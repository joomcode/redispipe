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

	re "github.com/joomcode/redispipe/rediserror"
	"github.com/joomcode/redispipe/resp"
)

const (
	connDisconnected = 0
	connConnecting   = 1
	connConnected    = 2
	connClosed       = 3

	defaultReconnectPause = 500 * time.Millisecond
	defaultKeepAlive      = 300 * time.Millisecond
	defaultIOTimeout      = 1 * time.Second
)

type Opts struct {
	// ReconnectPause is a pause after failed connection attempt before next one.
	// If ReconnectPause < 0, then no reconnection will be performed.
	// If ReconnectPause == 0, then default pause used (250ms)
	// ReconnectPause/2 is used as timeout for Dial
	ReconnectPause time.Duration
	// DialTimeout is timeout for net.Dialer
	// If not set, then ReconnectPause/2 is used (unless ReconnectPause < 0)
	DialTimeout time.Duration
	// DB - database number
	DB int
	// Password for AUTH
	Password string
	// Handle is returned with Connection.Handle()
	Handle interface{}
	// Concurrency - number for shards. Default is runtime.GOMAXPROCS(-1)*4
	Concurrency uint32
	// IOTimeout - timeout on read/write to socket.
	// If IOTimeout == 0, then it is set to 200 ms
	// If IOTimeout < 0, then timeout is disabled
	IOTimeout time.Duration
	// TCPKeepAlive - KeepAlive parameter for net.Dialer
	TCPKeepAlive time.Duration
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
		return nil, re.New(re.ErrKindOpts, re.ErrContextIsNil)
	}
	if addr == "" {
		return nil, re.New(re.ErrKindOpts, re.ErrNoAddressProvided)
	}
	conn = &Connection{
		addr: addr,
		opts: opts,
	}
	conn.ctx, conn.cancel = context.WithCancel(ctx)

	maxprocs := uint32(runtime.GOMAXPROCS(-1))
	if opts.Concurrency == 0 || opts.Concurrency > maxprocs*128 {
		conn.opts.Concurrency = maxprocs * 2
	}

	conn.shard = make([]connShard, conn.opts.Concurrency)
	conn.dirtyShard = make(chan uint32, conn.opts.Concurrency*2)

	if conn.opts.ReconnectPause == 0 {
		conn.opts.ReconnectPause = defaultReconnectPause
	}

	if conn.opts.TCPKeepAlive == 0 {
		conn.opts.TCPKeepAlive = defaultKeepAlive
	} else if conn.opts.TCPKeepAlive < 0 {
		conn.opts.TCPKeepAlive = 0
	}

	if conn.opts.IOTimeout == 0 {
		conn.opts.IOTimeout = defaultIOTimeout
	} else if conn.opts.IOTimeout < 0 {
		conn.opts.IOTimeout = 0
	}

	if conn.opts.Logger == nil {
		conn.opts.Logger = defaultLogger{}
	}

	if !conn.opts.Async {
		if err = conn.createConnection(false, nil); err != nil {
			if opts.ReconnectPause < 0 {
				return nil, err
			}
			if cer, ok := err.(*re.Error); ok && cer.Code == re.ErrAuth {
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
	var wg sync.WaitGroup
	var res interface{}

	wg.Add(1)
	conn.Send(Request{"PING", nil}, func(r interface{}, _ uint64) {
		res = r
		wg.Done()
	}, 0)
	wg.Wait()

	if err := resp.Error(res); err != nil {
		return err
	}
	if str, ok := res.(string); !ok || str != "PONG" {
		return re.New(re.ErrKindResponse, re.ErrPing).With("conn", conn).With("result", res)
	}
	return nil
}

func (conn *Connection) getShard() (uint32, *connShard) {
	shardn := atomic.AddUint32(&conn.shardid, 1) % conn.opts.Concurrency
	return shardn, &conn.shard[shardn]
}

var dumb = func(interface{}, uint64) {}

func (conn *Connection) Send(req Request, cb Callback, n uint64) {
	conn.SendAsk(req, cb, n, false)
}

func (conn *Connection) SendAsk(req Request, cb Callback, n uint64, asking bool) {
	shardn, shard := conn.getShard()
	if cb == nil {
		cb = dumb
	}
	shard.Lock()
	defer shard.Unlock()

	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		go cb(re.NewWrap(re.ErrKindContext, re.ErrContextClosed, conn.ctx.Err()).With("conn", conn), n)
		return
	case connDisconnected:
		go cb(re.New(re.ErrKindConnection, re.ErrNotConnected).With("conn", conn), n)
		return
	}
	buf := shard.buf
	futures := shard.futures
	var err *re.Error
	if asking {
		buf, _ = resp.AppendRequest(buf, Request{"ASKING", nil})
		futures = append(futures, future{dumb, 0})
	}
	if buf, err = resp.AppendRequest(buf, req); err != nil {
		go cb(err.With("conn", conn), n)
		return
	}
	futures = append(futures, future{cb, n})
	if len(shard.buf) == 0 {
		conn.dirtyShard <- shardn
	}
	shard.buf = buf
	shard.futures = futures
}

func (conn *Connection) SendBatch(requests []Request, cb Callback, start uint64) {
	conn.SendBatchAsk(requests, cb, start, false)
}

func (conn *Connection) SendBatchAsk(requests []Request, cb Callback, start uint64, asking bool) {
	if len(requests) == 0 {
		return
	}

	shardn, shard := conn.getShard()
	shard.Lock()
	defer shard.Unlock()

	var err *re.Error
	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		err = re.NewWrap(re.ErrKindContext, re.ErrContextClosed, conn.ctx.Err()).With("conn", conn)
		return
	case connDisconnected:
		err = re.New(re.ErrKindConnection, re.ErrNotConnected).With("conn", conn)
		return
	}
	if err != nil {
		go func(n int) {
			for i := 0; i < n; i++ {
				cb(err, start+uint64(i))
			}
		}(len(requests))
		return
	}
	buf := shard.buf
	futures := shard.futures
	if asking {
		buf, _ = resp.AppendRequest(buf, Request{"ASKING", nil})
		futures = append(futures, future{dumb, 0})
	}
	for i, req := range requests {
		buf, err = resp.AppendRequest(shard.buf, req)
		if err != nil {
			go func(i int, err *re.Error, n int) {
				var common_err error
				err = err.With("conn", conn).With("request", requests[i])
				common_err = re.NewWrap(re.ErrKindRequest, re.ErrBatchFormat, err).
					With("requests", requests).
					With("conn", conn).
					With("request", requests[i])
				for j := 0; j < n; j++ {
					if j == i {
						cb(err, start+uint64(i))
					} else {
						cb(common_err, start+uint64(j))
					}
				}
			}(i, err, len(requests))
			return
		}
		futures = append(futures, future{cb, start + uint64(i)})
	}

	if len(shard.buf) == 0 {
		conn.dirtyShard <- shardn
	}
	shard.buf = buf
	shard.futures = futures
	return
}

func (conn *Connection) SendTransaction(reqs []Request, cb Callback, off uint64) {
	if len(reqs) < 3 || reqs[0].Cmd != "MULTI" || reqs[len(reqs)-1].Cmd != "EXEC" {
		go cb(re.New(re.ErrKindRequest, re.ErrMalformedTransaction).
			With("conn", conn).
			With("requests", reqs), off)
		return
	}
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
	timeout := conn.opts.ReconnectPause / 2
	if timeout <= 0 {
		timeout = defaultReconnectPause / 2
	} else if timeout > 5*time.Second {
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
		return re.NewWrap(re.ErrKindConnection, re.ErrDial, err)
	}
	dc := newDeadlineIO(connection, conn.opts.IOTimeout)
	r := bufio.NewReaderSize(dc, 128*1024)
	w := bufio.NewWriterSize(dc, 128*1024)

	var req []byte
	if conn.opts.Password != "" {
		req, _ = resp.AppendRequest(req, Request{"AUTH", []interface{}{conn.opts.Password}})
	}
	req, _ = resp.AppendRequest(req, Request{"PING", nil})
	if conn.opts.DB != 0 {
		req, _ = resp.AppendRequest(req, Request{"SELECT", []interface{}{conn.opts.DB}})
	}
	if _, err = dc.Write(req); err != nil {
		connection.Close()
		return err
	}
	var res interface{}
	// Password response
	if conn.opts.Password != "" {
		res = resp.Read(r)
		if err := resp.RedisError(res); err != nil {
			connection.Close()
			if strings.Contains(err.Error(), "password") {
				return re.NewWrap(re.ErrKindConnection, re.ErrAuth, err).With("conn", conn)
			}
			return re.NewWrap(re.ErrKindConnection, re.ErrConnSetup, err).With("conn", conn)
		}
	}
	// PING Response
	res = resp.Read(r)
	if err = resp.Error(res); err != nil {
		connection.Close()
		return re.NewWrap(re.ErrKindConnection, re.ErrConnSetup, err)
	}
	if str, ok := res.(string); !ok || str != "PONG" {
		connection.Close()
		return re.NewMsg(re.ErrKindConnection, re.ErrConnSetup, "ping response mismatch").
			With("conn", conn).With("response", res)
	}
	// SELECT DB Response
	if conn.opts.DB != 0 {
		res = resp.Read(r)
		if err = resp.Error(res); err != nil {
			connection.Close()
			return re.NewWrap(re.ErrKindConnection, re.ErrConnSetup, err).With("conn", conn)
		}
		if str, ok := res.(string); !ok || str != "OK" {
			connection.Close()
			return re.NewMsg(re.ErrKindConnection, re.ErrConnSetup, "SELECT db response mismatch").
				With("conn", conn).
				With("db", conn.opts.DB).With("response", res)
		}
	}

	conn.lockShards()
	conn.c = connection
	conn.unlockShards()

	one := &oneconn{
		c:       connection,
		futures: make(chan []future, conn.opts.Concurrency*8),
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
			fut.Call(err)
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
			conn.closeErr = re.NewWrap(re.ErrKindContext, re.ErrContextClosed, conn.ctx.Err()).
				With("conn", conn)
			conn.closeConnection(conn.closeErr, true)
			return
		case <-t.C:
		}
		if err := conn.Ping(); err != nil {
			if cer, ok := err.(*re.Error); ok && cer.Code == re.ErrPing {
				// that states about serious error in our code
				panic(err)
			}
		}
	}
}

func (one *oneconn) setErr(neterr error, conn *Connection) {
	one.erronce.Do(func() {
		close(one.control)
		one.err = neterr
	})
	go conn.reconnect(neterr, one)
}

func (conn *Connection) reconnect(neterr error, one *oneconn) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if atomic.LoadUint32(&conn.state) == connClosed {
		return
	}
	if conn.opts.ReconnectPause < 0 {
		conn.Close()
		return
	}
	if conn.c == one.c {
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
			runtime.Gosched()
			if len(conn.dirtyShard) == 0 {
				if err := w.Flush(); err != nil {
					one.setErr(err, conn)
					return
				}
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
			if err := w.Flush(); err != nil {
				one.futures <- futures
				one.setErr(err, conn)
				return
			}
			one.futures <- futures
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
Outter:
	for futures = range one.futures {
		for i, fut := range futures {
			res = resp.Read(r)
			futures[i].Callback = nil
			if rerr := resp.RedisError(res); rerr.HardError() {
				one.setErr(rerr.With("conn", conn), conn)
				fut.Call(one.err)
				break Outter
			}
			fut.Call(res)
		}
		futures = nil
	}
	for _, fut := range futures {
		fut.Call(one.err)
	}
	for futures := range one.futures {
		for _, fut := range futures {
			fut.Call(one.err)
		}
	}
}
