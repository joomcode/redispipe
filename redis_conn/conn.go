package redis_conn

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/resp"
)

type ConnLogKind int

const (
	connDisconnected = 0
	connConnecting   = 1
	connConnected    = 2
	connClosed       = 3

	defaultReconnectPause = 500 * time.Millisecond
	defaultKeepAlive      = 300 * time.Millisecond
	defaultIOTimeout      = 1 * time.Second

	LogConnecting ConnLogKind = iota
	LogConnected
	LogConnectFailed
	LogDisconnected
	LogContextClosed
)

type Logger interface {
	Report(event ConnLogKind, conn *Connection, v ...interface{})
}

type defaultLogger struct{}

func (d defaultLogger) Report(event ConnLogKind, conn *Connection, v ...interface{}) {
	switch event {
	case LogConnecting:
		log.Printf("redis: connecting to %s", conn.Addr())
	case LogConnected:
		localAddr := v[0].(string)
		remoteAddr := v[1].(string)
		log.Printf("redis: connected to %s (localAddr: %s, remote addr: %s)",
			conn.Addr(), localAddr, remoteAddr)
	case LogConnectFailed:
		err := v[0].(error)
		log.Printf("redis: connection to %s failed: %s", conn.Addr(), err.Error())
	case LogDisconnected:
		err := v[0].(error)
		log.Printf("redis: connection to %s broken: %s", conn.Addr(), err.Error())
	case LogContextClosed:
		log.Printf("redis: connect to %s explicitly closed", conn.Addr())
	default:
		args := append([]interface{}{"redis: unexpected event:"}, event, conn, v)
		log.Print(args...)
	}
}

type Opts struct {
	// ReconnectPause is a pause after failed connection attempt before next one.
	// If ReconnectPause < 0, then no reconnection will be performed.
	// If ReconnectPause == 0, then default pause used (250ms)
	// ReconnectPause/2 is used as timeout for Dial
	ReconnectPause time.Duration
	// DialTimeout is timeout for net.Dialer
	// If not set, then ReconnectPause/2 is used (unless ReconnectPause < 0)
	DialTimeout time.Duration
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

	opts Opts
}

type oneconn struct {
	c       net.Conn
	futures chan []*Future
	err     error
	erronce sync.Once
}

type connShard struct {
	sync.Mutex
	buf     []byte
	futures []*Future
	_pad    [16]uint64
}

func Connect(ctx context.Context, addr string, opts Opts) (conn *Connection, err error) {
	if ctx == nil {
		return nil, &ConnError{Code: ErrContextIsNil, Msg: "Context should not be nil"}
	}
	conn = &Connection{
		addr: addr,
		opts: opts,
	}
	conn.ctx, conn.cancel = context.WithCancel(ctx)

	maxprocs := uint32(runtime.GOMAXPROCS(-1))
	if opts.Concurrency == 0 || opts.Concurrency > maxprocs*128 {
		opts.Concurrency = maxprocs * 4
	}
	i := uint32(1)
	for ; i < opts.Concurrency; i *= 2 {
	}
	conn.opts.Concurrency = i

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

	if err = conn.createConnection(false); err != nil {
		if opts.ReconnectPause < 0 {
			return nil, err
		}
		if cer, ok := err.(*ConnError); ok && cer.Code == ErrAuth {
			return nil, err
		}
		go func() {
			conn.mutex.Lock()
			defer conn.mutex.Unlock()
			conn.createConnection(true)
		}()
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
	req := conn.Send(Request{"PING", nil})
	<-req.Done()
	if req.Err != nil {
		return req.Err
	}
	if str, ok := req.Result.(string); !ok || str != "PONG" {
		return &ConnError{Code: ErrPing, Msg: fmt.Sprintf("Ping response mismatch: %+v", str)}
	}
	return nil
}

func (conn *Connection) Send(req Request) *Future {
	shardid := atomic.AddUint32(&conn.shardid, 1) & (conn.opts.Concurrency - 1)
	shard := &conn.shard[shardid]

	res := &Future{}

	shard.Lock()
	defer shard.Unlock()

	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		res.Err = &ConnError{Code: ErrContextClosed, Wrap: conn.ctx.Err()}
	case connDisconnected:
		res.Err = &ConnError{Code: ErrDisconnected, Msg: "connection is broken at the moment"}
	}
	if res.Err != nil {
		res.wait = closedChan
		return res
	}
	var buf []byte
	var err error
	if buf, err = resp.AppendRequest(shard.buf, req.Cmd, req.Args); err != nil {
		res.Err = &ConnError{Code: ErrArgumentType, Wrap: err}
		res.wait = closedChan
		return res
	}
	res.wait = make(chan struct{})

	if len(shard.buf) == 0 {
		conn.dirtyShard <- shardid
	}
	shard.buf = buf
	shard.futures = append(shard.futures, res)
	return res
}

func (conn *Connection) SendBatch(requests []Request) []*Future {
	if len(requests) == 0 {
		return nil
	}
	shardid := atomic.AddUint32(&conn.shardid, 1) & (conn.opts.Concurrency - 1)
	shard := &conn.shard[shardid]

	resflat := make([]Future, len(requests))
	results := make([]*Future, len(requests))
	for i := range results {
		results[i] = &resflat[i]
	}

	shard.Lock()
	defer shard.Unlock()

	var err error
	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		err = &ConnError{Code: ErrContextClosed, Wrap: conn.ctx.Err()}
	case connDisconnected:
		err = &ConnError{Code: ErrDisconnected, Msg: "connection is broken at the moment"}
	}
	if err != nil {
		for _, res := range results {
			res.Err = err
			res.wait = closedChan
		}
		return results
	}
	buf := shard.buf
	for i, req := range requests {
		if buf, err = resp.AppendRequest(shard.buf, req.Cmd, req.Args); err != nil {
			results[i].Err = &ConnError{Code: ErrArgumentType, Wrap: err}
			err = &ConnError{Code: ErrBatchFailed, Msg: fmt.Sprintf("encoding of %d command %+v failed", i, req)}
			break
		}
	}
	if err != nil {
		for _, res := range results {
			if res.Err == nil {
				res.Err = err
			}
			res.wait = closedChan
		}
		return results
	}
	for _, res := range results {
		res.wait = make(chan struct{})
	}

	if len(shard.buf) == 0 {
		conn.dirtyShard <- shardid
	}
	shard.buf = buf
	shard.futures = append(shard.futures, results...)
	return results
}

/********** private api **************/

func (conn *Connection) report(event ConnLogKind, v ...interface{}) {
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
		return &ConnError{Code: ErrDial, Wrap: err}
	}
	dc := newDeadlineIO(connection, conn.opts.IOTimeout)
	r := bufio.NewReaderSize(dc, 128*1024)
	w := bufio.NewWriterSize(dc, 128*1024)
	if conn.opts.Password != "" {
		req, _ := resp.AppendRequest(nil, "AUTH", []interface{}{conn.opts.Password})
		if _, err = dc.Write(req); err != nil {
			connection.Close()
			return err
		}
		var res interface{}
		if res, err = resp.Read(r); err != nil {
			connection.Close()
			return err
		}
		if err, ok := res.(error); ok {
			connection.Close()
			if strings.Contains(err.Error(), "password") {
				return &ConnError{Code: ErrAuth, Msg: err.Error()}
			}
			return err
		}
	}

	conn.lockShards()
	conn.c = connection
	conn.unlockShards()

	one := &oneconn{
		c:       connection,
		futures: make(chan []*Future, conn.opts.Concurrency*4),
	}

	go conn.writer(w, one)
	go conn.reader(r, one)

	return nil
}

func (conn *Connection) createConnection(reconnect bool) error {
	var err error
	for conn.c == nil && atomic.LoadUint32(&conn.state) == connDisconnected {
		conn.report(LogConnecting)
		now := time.Now()
		// start accepting requests
		atomic.StoreUint32(&conn.state, connConnecting)
		err = conn.dial()
		if err == nil {
			conn.report(LogConnected,
				conn.c.LocalAddr().String(),
				conn.c.RemoteAddr().String())
			atomic.StoreUint32(&conn.state, connConnected)
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
		for _, r := range sh.futures {
			r.Result = nil
			r.Err = err
			close(r.wait)
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
			conn.closeErr = &ConnError{Code: ErrContextClosed, Wrap: conn.ctx.Err()}
			conn.closeConnection(conn.closeErr, true)
			return
		case <-t.C:
		}
		if err := conn.Ping(); err != nil {
			if cer, ok := err.(*ConnError); ok && cer.Code == ErrPing {
				conn.closeConnection(err, false)
			}
		}
	}
}

func (conn *Connection) reconnect(neterr error, one *oneconn) {
	one.erronce.Do(func() {
		if atomic.LoadUint32(&conn.state) == connClosed {
			one.err = conn.closeErr
		}
		one.err = neterr
	})

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
		conn.createConnection(true)
	}
}

func (conn *Connection) writer(w *bufio.Writer, one *oneconn) {
	var shardn uint32
	var packet []byte
	var futures []*Future
	defer close(one.futures)
	round := conn.opts.Concurrency*4 - 1
	for {
		select {
		case shardn = <-conn.dirtyShard:
		default:
			runtime.Gosched()
			if len(conn.dirtyShard) == 0 {
				if err := w.Flush(); err != nil {
					conn.reconnect(err, one)
					return
				}
			}
			select {
			case shardn = <-conn.dirtyShard:
			case <-conn.ctx.Done():
				return
			}
		}

		shard := &conn.shard[shardn]
		shard.Lock()
		if conn.c != one.c {
			select {
			case <-conn.ctx.Done():
				// connection closed
			default:
				conn.dirtyShard <- shardn
				return
			}
		}
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
				conn.reconnect(err, one)
				return
			}
			one.futures <- futures
		}

		l, err := w.Write(packet)
		if err != nil {
			conn.reconnect(err, one)
			return
		}
		if l != len(packet) {
			panic("Wrong length written")
		}

		if round--; round == 0 {
			// occasionally free buffer
			round = conn.opts.Concurrency*4 - 1
			packet = nil
		} else {
			packet = packet[0:0]
		}
		capa := 1
		for ; capa < len(futures); capa *= 2 {
		}
		futures = make([]*Future, 0, capa)
	}
}

func (conn *Connection) reader(r *bufio.Reader, one *oneconn) {
	var futures []*Future
	var err error
Outter:
	for futures = range one.futures {
		for i, req := range futures {
			req.Result, err = resp.Read(r)
			futures[i] = nil
			if err != nil {
				if ioerr, ok := err.(resp.IOError); ok {
					err = &ConnError{Code: ErrIO, Wrap: ioerr}
				} else {
					err = &ConnError{Code: ErrResponse, Wrap: err}
				}
				conn.reconnect(err, one)
				req.Err = one.err
				close(req.wait)
				break Outter
			}
			close(req.wait)
		}
		futures = nil
	}
	for _, req := range futures {
		if req != nil {
			req.Err = one.err
			close(req.wait)
		}
	}
	for futures := range one.futures {
		for _, req := range futures {
			req.Err = one.err
			close(req.wait)
		}
	}
}
