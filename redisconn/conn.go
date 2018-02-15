package redisconn

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redis"
)

const (
	DoAsking      = 1
	DoTransaction = 2

	connDisconnected = 0
	connConnecting   = 1
	connConnected    = 2
	connClosed       = 3

	defaultIOTimeout = 1 * time.Second
)

type Opts struct {
	// DB - database number
	DB int
	// Password for AUTH
	Password string
	// IOTimeout - timeout on read/write to socket.
	// If IOTimeout == 0, then it is set to 1 second
	// If IOTimeout < 0, then timeout is disabled
	IOTimeout time.Duration
	// DialTimeout is timeout for net.Dialer
	// If it is <= 0 or >= IOTimeout, then IOTimeout
	// If IOTimeout is disabled, then 5 seconds used (but without affect on ReconnectPause)
	DialTimeout time.Duration
	// ReconnectPause is a pause after failed connection attempt before next one.
	// If ReconnectPause < 0, then no reconnection will be performed.
	// If ReconnectPause == 0, then DialTimeout * 2 is used
	ReconnectPause time.Duration
	// TCPKeepAlive - KeepAlive parameter for net.Dialer
	// default is IOTimeout / 3
	TCPKeepAlive time.Duration
	// Handle is returned with Connection.Handle()
	Handle interface{}
	// Concurrency - number for shards. Default is runtime.GOMAXPROCS(-1)*4
	Concurrency uint32
	// Logger
	Logger Logger
	// Async - do not establish connection immediately
	Async bool
}

type Connection struct {
	ctx      context.Context
	cancel   context.CancelFunc
	state    uint32
	closeErr error

	addr  string
	c     net.Conn
	mutex sync.Mutex

	shardid    uint32
	shard      []connShard
	dirtyShard chan uint32

	firstConn chan struct{}
	opts      Opts
}

type oneconn struct {
	c       net.Conn
	futures chan []future
	control chan struct{}
	err     error
	erronce sync.Once
}

type connShard struct {
	sync.Mutex
	buf     []byte
	futures []future
	_pad    [16]uint64
}

func Connect(ctx context.Context, addr string, opts Opts) (conn *Connection, err error) {
	if ctx == nil {
		return nil, redis.NewErr(redis.ErrKindOpts, redis.ErrContextIsNil)
	}
	if addr == "" {
		return nil, redis.NewErr(redis.ErrKindOpts, redis.ErrNoAddressProvided)
	}
	conn = &Connection{
		addr: addr,
		opts: opts,
	}
	conn.ctx, conn.cancel = context.WithCancel(ctx)

	maxprocs := uint32(runtime.GOMAXPROCS(-1))
	if opts.Concurrency == 0 || opts.Concurrency > maxprocs*128 {
		conn.opts.Concurrency = maxprocs
	}

	conn.shard = make([]connShard, conn.opts.Concurrency)
	conn.dirtyShard = make(chan uint32, conn.opts.Concurrency*2)

	if conn.opts.IOTimeout == 0 {
		conn.opts.IOTimeout = defaultIOTimeout
	} else if conn.opts.IOTimeout < 0 {
		conn.opts.IOTimeout = 0
	}

	if conn.opts.DialTimeout <= 0 || conn.opts.DialTimeout > conn.opts.IOTimeout {
		conn.opts.DialTimeout = conn.opts.IOTimeout
	}

	if conn.opts.ReconnectPause == 0 {
		conn.opts.ReconnectPause = conn.opts.DialTimeout * 2
	}

	if conn.opts.TCPKeepAlive == 0 {
		conn.opts.TCPKeepAlive = conn.opts.IOTimeout / 3
	}
	if conn.opts.TCPKeepAlive < 0 {
		conn.opts.TCPKeepAlive = 0
	}

	if conn.opts.Logger == nil {
		conn.opts.Logger = defaultLogger{}
	}

	if !conn.opts.Async {
		if err = conn.createConnection(false, nil); err != nil {
			if opts.ReconnectPause < 0 {
				return nil, err
			}
			if cer, ok := err.(*redis.Error); ok && cer.Code == redis.ErrAuth {
				return nil, err
			}
		}
	}

	if conn.opts.Async || err != nil {
		var ch chan struct{}
		if conn.opts.Async {
			ch = make(chan struct{})
		}
		go func() {
			conn.mutex.Lock()
			defer conn.mutex.Unlock()
			conn.createConnection(true, ch)
		}()
		// in async mode we are still waiting for state to set to connConnecting
		// so that Send will put requests into queue
		if conn.opts.Async {
			<-ch
		}
	}

	go conn.control()

	return conn, nil
}

// Context of this connection
func (conn *Connection) Ctx() context.Context {
	return conn.ctx
}

// Connection is certainly connected now
func (conn *Connection) ConnectedNow() bool {
	return atomic.LoadUint32(&conn.state) == connConnected
}

// MayBeConnected: connection either connected or connecting
func (conn *Connection) MayBeConnected() bool {
	s := atomic.LoadUint32(&conn.state)
	return s == connConnected || s == connConnecting
}

// Close connection forever
func (conn *Connection) Close() {
	conn.cancel()
}

// Remote is address of Redis socket
func (conn *Connection) RemoteAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.RemoteAddr().String()
}

// LocalAddr is outgoing socket addr
func (conn *Connection) LocalAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.LocalAddr().String()
}

func (conn *Connection) Addr() string {
	return conn.addr
}

// Handle returns user specified handle from Opts
func (conn *Connection) Handle() interface{} {
	return conn.opts.Handle
}

func (conn *Connection) Ping() error {
	res := redis.Sync{conn}.Do("PING")
	if err := redis.AsError(res); err != nil {
		return err
	}
	if str, ok := res.(string); !ok || str != "PONG" {
		return conn.err(redis.ErrKindResponse, redis.ErrPing).With("response", res)
	}
	return nil
}

func (conn *Connection) getShard() (uint32, *connShard) {
	shardn := atomic.AddUint32(&conn.shardid, 1) % conn.opts.Concurrency
	return shardn, &conn.shard[shardn]
}

type dumbcb struct{}

func (d dumbcb) Cancelled() bool             { return false }
func (d dumbcb) Resolve(interface{}, uint64) {}

var dumb dumbcb

func (conn *Connection) Send(req Request, cb Future, n uint64) {
	conn.SendAsk(req, cb, n, false)
}

func (conn *Connection) SendAsk(req Request, cb Future, n uint64, asking bool) {
	if cb == nil {
		cb = dumb
	}
	if err := conn.doSend(req, cb, n, asking); err != nil {
		cb.Resolve(err.With("connection", conn), n)
	}
}

func (conn *Connection) doSend(req Request, cb Future, n uint64, asking bool) *redis.Error {
	if cb.Cancelled() {
		return conn.err(redis.ErrKindRequest, redis.ErrRequestCancelled)
	}

	shardn, shard := conn.getShard()
	shard.Lock()
	defer shard.Unlock()

	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		return redis.NewErrWrap(redis.ErrKindContext, redis.ErrContextClosed, conn.ctx.Err())
	case connDisconnected:
		return redis.NewErr(redis.ErrKindConnection, redis.ErrNotConnected)
	}
	buf := shard.buf
	futures := shard.futures
	var err *redis.Error
	if asking {
		buf = append(buf, askingReq...)
		futures = append(futures, future{dumb, 0, 0, Request{}})
	}
	if buf, err = redis.AppendRequest(buf, req); err != nil {
		return err
	}
	futures = append(futures, future{cb, n, nownano(), req})
	if len(shard.buf) == 0 {
		conn.dirtyShard <- shardn
	}
	shard.buf = buf
	shard.futures = futures
	return nil
}

func (conn *Connection) SendMany(requests []Request, cb Future, start uint64) {
	for i := 0; i < len(requests); i += 16 {
		j := i + 16
		if j > len(requests) {
			j = len(requests)
		}
		conn.SendBatch(requests[i:j], cb, start+uint64(i))
	}
}

func (conn *Connection) SendBatch(requests []Request, cb Future, start uint64) {
	conn.SendBatchFlags(requests, cb, start, 0)
}

func (conn *Connection) SendBatchFlags(requests []Request, cb Future, start uint64, flags int) {
	commonerr, pos, err := conn.doSendBatch(requests, cb, start, flags)
	if commonerr != nil {
		for i := 0; i < len(requests); i++ {
			if i != pos {
				cb.Resolve(commonerr, start+uint64(i))
			} else {
				cb.Resolve(err, start+uint64(i))
			}
		}
	}
}

func (conn *Connection) doSendBatch(requests []Request, cb Future, start uint64, flags int) (error, int, error) {
	if len(requests) == 0 {
		return nil, 0, nil
	}

	if cb.Cancelled() {
		err := conn.err(redis.ErrKindRequest, redis.ErrRequestCancelled)
		return err, -1, nil
	}

	shardn, shard := conn.getShard()
	shard.Lock()
	defer shard.Unlock()

	var err *redis.Error
	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		err = conn.err(redis.ErrKindContext, redis.ErrContextClosed).Wrap(conn.ctx.Err())
		return err, -1, nil
	case connDisconnected:
		err = conn.err(redis.ErrKindConnection, redis.ErrNotConnected)
		return err, -1, nil
	}

	buf := shard.buf
	futures := shard.futures
	if flags&DoAsking != 0 {
		buf = append(buf, askingReq...)
		futures = append(futures, future{dumb, 0, 0, Request{}})
	}
	if flags&DoTransaction != 0 {
		buf = append(buf, multiReq...)
		futures = append(futures, future{dumb, 0, 0, Request{}})
	}

	now := nownano()

	for i, req := range requests {
		buf, err = redis.AppendRequest(buf, req)
		if err != nil {
			err = err.With("connection", conn).With("request", requests[i])
			commonerr := conn.err(redis.ErrKindRequest, redis.ErrBatchFormat).
				Wrap(err).
				With("requests", requests).
				With("request", requests[i])
			return commonerr, i, err
		}
		futures = append(futures, future{cb, start + uint64(i), now, req})
	}
	if flags&DoTransaction != 0 {
		buf = append(buf, execReq...)
		futures = append(futures, future{cb, start + uint64(len(requests)), now, Request{"EXEC", nil}})
	}

	if len(shard.buf) == 0 {
		conn.dirtyShard <- shardn
	}
	shard.buf = buf
	shard.futures = futures
	return nil, -1, nil
}

type wrapped struct {
	Future
	f func(res interface{}, n uint64)
}

func (cw wrapped) Resolve(res interface{}, n uint64) {
	cw.f(res, n)
}

func (conn *Connection) SendTransaction(reqs []Request, cb Future, off uint64) {
	if cb.Cancelled() {
		cb.Resolve(conn.err(redis.ErrKindRequest, redis.ErrRequestCancelled), off)
		return
	}
	l := uint64(len(reqs))
	resolve := func(res interface{}, n uint64) {
		if n == l {
			cb.Resolve(res, off)
		}
	}
	conn.SendBatchFlags(reqs, wrapped{cb, resolve}, 0, DoTransaction)
}

func (conn *Connection) String() string {
	return fmt.Sprintf("*redisconn.Connection{addr: %s}", conn.addr)
}

/********** private api **************/

func (conn *Connection) report(event LogKind, v ...interface{}) {
	conn.opts.Logger.Report(event, conn, v...)
}

func (conn *Connection) lockShards() {
	for i := range conn.shard {
		conn.shard[i].Lock()
	}
}

func (conn *Connection) unlockShards() {
	for i := range conn.shard {
		conn.shard[i].Unlock()
	}
}

func (conn *Connection) dial() error {
	var connection net.Conn
	var err error
	network := "tcp"
	address := conn.addr
	timeout := conn.opts.DialTimeout
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	if address[0] == '.' || address[0] == '/' {
		network = "unix"
	} else if address[0:7] == "unix://" {
		network = "unix"
		address = address[7:]
	} else if address[0:6] == "tcp://" {
		network = "tcp"
		address = address[6:]
	}
	dialer := net.Dialer{
		Timeout:       timeout,
		DualStack:     true,
		FallbackDelay: timeout / 2,
		KeepAlive:     conn.opts.TCPKeepAlive,
	}
	connection, err = dialer.DialContext(conn.ctx, network, address)
	if err != nil {
		return redis.NewErrWrap(redis.ErrKindConnection, redis.ErrDial, err)
	}
	dc := newDeadlineIO(connection, conn.opts.IOTimeout)
	r := bufio.NewReaderSize(dc, 128*1024)
	w := bufio.NewWriterSize(dc, 128*1024)

	var req []byte
	if conn.opts.Password != "" {
		req = append(req, authReq...)
	}
	req = append(req, pingReq...)
	if conn.opts.DB != 0 {
		req, _ = redis.AppendRequest(req, Request{"SELECT", []interface{}{conn.opts.DB}})
	}
	if conn.opts.IOTimeout > 0 {
		connection.SetWriteDeadline(time.Now().Add(conn.opts.IOTimeout))
	}
	if _, err = dc.Write(req); err != nil {
		connection.Close()
		return redis.NewErrWrap(redis.ErrKindConnection, redis.ErrConnSetup, err)
	}
	connection.SetWriteDeadline(time.Time{})
	var res interface{}
	// Password response
	if conn.opts.Password != "" {
		res = redis.ReadResponse(r)
		if err := redis.AsRedisError(res); err != nil {
			connection.Close()
			if strings.Contains(err.Error(), "password") {
				return conn.err(redis.ErrKindConnection, redis.ErrAuth).Wrap(err)
			}
			return conn.err(redis.ErrKindConnection, redis.ErrConnSetup).Wrap(err)
		}
	}
	// PING Response
	res = redis.ReadResponse(r)
	if err = redis.AsError(res); err != nil {
		connection.Close()
		return redis.NewErrWrap(redis.ErrKindConnection, redis.ErrConnSetup, err)
	}
	if str, ok := res.(string); !ok || str != "PONG" {
		connection.Close()
		return conn.err(redis.ErrKindConnection, redis.ErrConnSetup).
			WithMsg("ping response mismatch").
			With("response", res)
	}
	// SELECT DB Response
	if conn.opts.DB != 0 {
		res = redis.ReadResponse(r)
		if err = redis.AsError(res); err != nil {
			connection.Close()
			return conn.err(redis.ErrKindConnection, redis.ErrConnSetup).Wrap(err)
		}
		if str, ok := res.(string); !ok || str != "OK" {
			connection.Close()
			return conn.err(redis.ErrKindConnection, redis.ErrConnSetup).
				WithMsg("SELECT db response mismatch").
				With("db", conn.opts.DB).With("response", res)
		}
	}

	conn.lockShards()
	conn.c = connection
	conn.unlockShards()

	one := &oneconn{
		c:       connection,
		futures: make(chan []future, conn.opts.Concurrency/2+1),
		control: make(chan struct{}),
	}

	go conn.writer(w, one)
	go conn.reader(r, one)

	return nil
}

func (conn *Connection) createConnection(reconnect bool, ch chan struct{}) error {
	var err error
	for conn.c == nil && atomic.LoadUint32(&conn.state) == connDisconnected {
		conn.report(LogConnecting)
		now := time.Now()
		// start accepting requests
		atomic.StoreUint32(&conn.state, connConnecting)
		if ch != nil {
			close(ch)
			ch = nil
		}
		err = conn.dial()
		if err == nil {
			atomic.StoreUint32(&conn.state, connConnected)
			conn.report(LogConnected,
				conn.c.LocalAddr().String(),
				conn.c.RemoteAddr().String())
			return nil
		}

		conn.report(LogConnectFailed, err)
		atomic.StoreUint32(&conn.state, connDisconnected)
		conn.lockShards()
		conn.dropShardFutures(err)
		conn.unlockShards()

		if !reconnect {
			return err
		}
		conn.mutex.Unlock()
		time.Sleep(now.Add(conn.opts.ReconnectPause).Sub(time.Now()))
		conn.mutex.Lock()
	}
	if ch != nil {
		close(ch)
	}
	if atomic.LoadUint32(&conn.state) == connClosed {
		err = conn.ctx.Err()
	}
	return err
}

func (conn *Connection) dropShardFutures(err error) {
Loop:
	for {
		select {
		case <-conn.dirtyShard:
		default:
			break Loop
		}
	}
	for i := range conn.shard {
		sh := &conn.shard[i]
		for _, fut := range sh.futures {
			conn.call(fut, err)
		}
		sh.buf = sh.buf[:0]
		sh.futures = sh.futures[:0]
	}
}

func (conn *Connection) closeConnection(neterr error, forever bool) error {
	if forever {
		atomic.StoreUint32(&conn.state, connClosed)
		conn.report(LogContextClosed)
	} else {
		atomic.StoreUint32(&conn.state, connDisconnected)
		conn.report(LogDisconnected, neterr)
	}

	var err error

	conn.lockShards()
	defer conn.unlockShards()
	if conn.c != nil {
		err = conn.c.Close()
		conn.c = nil
	}

	conn.dropShardFutures(neterr)
	return err
}

func (conn *Connection) control() {
	timeout := conn.opts.IOTimeout / 3
	if timeout <= 0 {
		timeout = time.Second
	}
	t := time.NewTicker(timeout)
	defer t.Stop()
	for {
		select {
		case <-conn.ctx.Done():
			conn.mutex.Lock()
			defer conn.mutex.Unlock()
			conn.closeErr = conn.err(redis.ErrKindContext, redis.ErrContextClosed).
				Wrap(conn.ctx.Err())
			conn.closeConnection(conn.closeErr, true)
			return
		case <-t.C:
		}
		if err := conn.Ping(); err != nil {
			if cer, ok := err.(*redis.Error); ok && cer.Code == redis.ErrPing {
				// that states about serious error in our code
				panic(err)
			}
		}
	}
}

func (one *oneconn) setErr(neterr error, conn *Connection) {
	one.erronce.Do(func() {
		close(one.control)
		rerr, ok := neterr.(*redis.Error)
		if !ok {
			rerr = redis.NewErrWrap(redis.ErrKindIO, redis.ErrIO, neterr)
		}
		one.err = rerr.With("connection", conn)
		go conn.reconnect(one.err, one.c)
	})
}

func (conn *Connection) reconnect(neterr error, c net.Conn) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if atomic.LoadUint32(&conn.state) == connClosed {
		return
	}
	if conn.opts.ReconnectPause < 0 {
		conn.Close()
		return
	}
	if conn.c == c {
		conn.closeConnection(neterr, false)
		conn.createConnection(true, nil)
	}
}

func (conn *Connection) writer(w *bufio.Writer, one *oneconn) {
	var shardn uint32
	var packet []byte
	var futures []future
	defer close(one.futures)
	round := 1023
	for {
		select {
		case shardn = <-conn.dirtyShard:
		case <-conn.ctx.Done():
			return
		case <-one.control:
			return
		default:
			if err := w.Flush(); err != nil {
				one.setErr(err, conn)
				return
			}
			select {
			case shardn = <-conn.dirtyShard:
			case <-conn.ctx.Done():
				return
			case <-one.control:
				return
			}
		}

		shard := &conn.shard[shardn]
		shard.Lock()
		packet, shard.buf = shard.buf, packet
		futures, shard.futures = shard.futures, futures
		shard.Unlock()

		if len(packet) == 0 {
			if len(futures) != 0 {
				panic("len(packet) == 0 && len(futures) != 0")
			}
			continue
		}

		select {
		case one.futures <- futures:
		default:
			err := w.Flush()
			one.futures <- futures
			if err != nil {
				one.setErr(err, conn)
				return
			}
		}

		l, err := w.Write(packet)
		if err != nil {
			one.setErr(err, conn)
			return
		}
		if l != len(packet) {
			panic("Wrong length written")
		}

		if round--; round == 0 {
			// occasionally free buffer
			round = 1023
			packet = nil
		} else {
			packet = packet[0:0]
		}
		capa := 1
		for ; capa < len(futures); capa *= 2 {
		}
		futures = make([]future, 0, capa)
	}
}

func (conn *Connection) reader(r *bufio.Reader, one *oneconn) {
	var futures []future
	var res interface{}
	var ok bool

	for {
		res = redis.ReadResponse(r)
		if rerr := redis.AsRedisError(res); rerr.HardError() {
			one.setErr(rerr, conn)
			break
		}
		if len(futures) == 0 {
			futures, ok = <-one.futures
			if !ok {
				break
			}
		}
		fut := futures[0]
		futures[0].Future = nil
		futures = futures[1:]
		conn.call(fut, res)
	}

	for _, fut := range futures {
		conn.call(fut, one.err)
	}
	for futures := range one.futures {
		for _, fut := range futures {
			conn.call(fut, one.err)
		}
	}
}

func (conn *Connection) err(kind redis.ErrorKind, code redis.ErrorCode) *redis.Error {
	return redis.NewErr(kind, code).With("connection", conn)
}
