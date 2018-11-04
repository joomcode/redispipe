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

	defaultIOTimeout  = 1 * time.Second
	defaultWritePause = 10 * time.Microsecond
)

// Opts - options for Connection
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
	// WritePause - write loop pauses for this time to collect more requests.
	// Default is 10microseconds. Set < 0 to disable.
	// It is not wise to set it larger than 100 microseconds.
	WritePause time.Duration
	// Logger
	Logger Logger
	// Async - do not establish connection immediately
	Async bool
}

// Connection represents single connection to single redis instance.
// Underlying net.Conn is re-established as necessary.
type Connection struct {
	ctx    context.Context
	cancel context.CancelFunc
	state  uint32

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
	futpool chan []future
}

type connShard struct {
	sync.Mutex
	futures []future
	_pad    [16]uint64
}

func Connect(ctx context.Context, addr string, opts Opts) (conn *Connection, err error) {
	if ctx == nil {
		return nil, redis.ErrContextIsNil.New()
	}
	if addr == "" {
		return nil, redis.ErrNoAddressProvided.New()
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

	if conn.opts.WritePause == 0 {
		conn.opts.WritePause = defaultWritePause
	}

	if conn.opts.Logger == nil {
		conn.opts.Logger = DefaultLogger{}
	}

	if !conn.opts.Async {
		if err = conn.createConnection(false, nil); err != nil {
			if opts.ReconnectPause < 0 {
				return nil, err
			}
			if cer, ok := err.(*redis.Error); ok && cer.KindOf(redis.ErrAuth) {
				return nil, err
			}
		}
	}

	if conn.opts.Async || err != nil {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			conn.mutex.Lock()
			defer conn.mutex.Unlock()
			conn.createConnection(true, &wg)
		}()
		// in async mode we are still waiting for state to set to connConnecting
		// so that Send will put requests into queue
		if conn.opts.Async {
			wg.Wait()
		}
	}

	go conn.control()

	return conn, nil
}

// Ctx returns context of this connection
func (conn *Connection) Ctx() context.Context {
	return conn.ctx
}

// ConnectedNow answers if connection is certainly connected at the moment
func (conn *Connection) ConnectedNow() bool {
	return atomic.LoadUint32(&conn.state) == connConnected
}

// MayBeConnected answers if connection either connected or connecting at the moment.
// Ie it returns false if connection is disconnected at the moment, and reconnection is not started yet.
func (conn *Connection) MayBeConnected() bool {
	s := atomic.LoadUint32(&conn.state)
	return s == connConnected || s == connConnecting
}

// Close closes connection forever
func (conn *Connection) Close() {
	conn.cancel()
}

// RemoteAddr is address of Redis socket
// Attention: do not call this method from Logger.Report, because it could lead to deadlock!
func (conn *Connection) RemoteAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.RemoteAddr().String()
}

// LocalAddr is outgoing socket addr
// Attention: do not call this method from Logger.Report, because it could lead to deadlock!
func (conn *Connection) LocalAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.LocalAddr().String()
}

// Addr retuns configurred address
func (conn *Connection) Addr() string {
	return conn.addr
}

// Handle returns user specified handle from Opts
func (conn *Connection) Handle() interface{} {
	return conn.opts.Handle
}

// Ping sends ping request synchronously
func (conn *Connection) Ping() error {
	res := redis.Sync{conn}.Do("PING")
	if err := redis.AsError(res); err != nil {
		return err
	}
	if str, ok := res.(string); !ok || str != "PONG" {
		return conn.err(redis.ErrPing).With(redis.EKResponse, res)
	}
	return nil
}

// choose next "shard" to send query to
func (conn *Connection) getShard() (uint32, *connShard) {
	shardn := atomic.AddUint32(&conn.shardid, 1) % conn.opts.Concurrency
	return shardn, &conn.shard[shardn]
}

// dumb redis.Future implementation
type dumbcb struct{}

func (d dumbcb) Cancelled() bool             { return false }
func (d dumbcb) Resolve(interface{}, uint64) {}

var dumb dumbcb

// Send implements redis.Sender.Send
// It sends request asynchronously. At some moment in a future it will call cb.Resolve(result, n)
// But if cb is cancelled, then cb.Resolve will be called immediately.
func (conn *Connection) Send(req Request, cb Future, n uint64) {
	conn.SendAsk(req, cb, n, false)
}

// SendAsk is a helper method for redis-cluster client implementation.
// If asking==true, it will send request with ASKING request sent before.
func (conn *Connection) SendAsk(req Request, cb Future, n uint64, asking bool) {
	if cb == nil {
		cb = &dumb
	}
	if err := conn.doSend(req, cb, n, asking); err != nil {
		cb.Resolve(err, n)
	}
}

func (conn *Connection) doSend(req Request, cb Future, n uint64, asking bool) *redis.Error {
	if cb != nil && cb.Cancelled() {
		return conn.err(redis.ErrRequestCancelled)
	}

	// Since we do not pack request here, we need to be sure it could be packed
	if err := redis.CheckArgs(req); err != nil {
		return err.With(EKConnection, conn)
	}

	shardn, shard := conn.getShard()
	shard.Lock()
	defer shard.Unlock()

	// we need to check conn.state first
	// since we do not lock connection itself, we need to use atomics.
	// Note: we do not check for connConnecting, ie we will try to send request after connection established.
	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		return conn.err(redis.ErrContextClosed).Wrap(conn.ctx.Err())
	case connDisconnected:
		return conn.err(redis.ErrNotConnected)
	}
	futures := shard.futures
	if asking {
		// send ASKING request before actual
		futures = append(futures, future{&dumb, 0, 0, Request{"ASKING", nil}})
	}
	futures = append(futures, future{cb, n, nownano(), req})

	// should notify writer about this shard having queries.
	// Since we are under shard lock, it is safe to send notification before assigning futures.
	if len(shard.futures) == 0 {
		conn.dirtyShard <- shardn
	}
	shard.futures = futures
	return nil
}

// SendMany implements redis.Sender.SendMany
// Sends several requests asynchronously. Fills with cb.Resolve(res, n), cb.Resolve(res, n+1), ... etc.
// Note: it could resolve requests in arbitrary order.
func (conn *Connection) SendMany(requests []Request, cb Future, start uint64) {
	// split requests by chunks of 16 to not block shards for a long time.
	// Also it could help a bit to save pipeline with writer loop.
	for i := 0; i < len(requests); i += 16 {
		j := i + 16
		if j > len(requests) {
			j = len(requests)
		}
		conn.SendBatch(requests[i:j], cb, start+uint64(i))
	}
}

// SendBatch sends several requests in preserved order.
// They will be serialized to network in the order passed.
func (conn *Connection) SendBatch(requests []Request, cb Future, start uint64) {
	conn.SendBatchFlags(requests, cb, start, 0)
}

// SendBatchFlags sends several requests in preserved order with addition ASKING, MULTI+EXEC commands.
// If flag&DoAsking != 0 , then "ASKING" command is prepended.
// If flag&DoTransaction != 0, then "MULTI" command is prepended, and "EXEC" command appended.
// Note: cb.Resolve will be also called with start+len(requests) index with result of EXEC command.
// It is mostly helper method for SendTransaction for single connect and cluster implementations.
//
// Note: since it is used for transaction, single wrong argument in single request
// will result in error for all commands in a batch.
func (conn *Connection) SendBatchFlags(requests []Request, cb Future, start uint64, flags int) {
	var err *redis.Error
	var commonerr *redis.Error
	errpos := -1
	// check arguments of all commands. If single request is malformed, then all requests will be aborted.
	for i, req := range requests {
		if err = redis.CheckArgs(req); err != nil {
			err = err.With(EKConnection, conn).With(redis.EKRequest, requests[i])
			commonerr = conn.err(redis.ErrBatchFormat).
				Wrap(err).
				With(redis.EKRequests, requests).
				With(redis.EKRequest, requests[i])
			errpos = i
			break
		}
	}
	if cb == nil {
		cb = &dumb
	}
	if commonerr == nil {
		commonerr = conn.doSendBatch(requests, cb, start, flags)
	}
	if commonerr != nil {
		for i := 0; i < len(requests); i++ {
			if i != errpos {
				cb.Resolve(commonerr, start+uint64(i))
			} else {
				cb.Resolve(err, start+uint64(i))
			}
		}
		if flags&DoTransaction != 0 {
			// resolve EXEC request as well
			cb.Resolve(commonerr, start+uint64(len(requests)))
		}
	}
}

func (conn *Connection) doSendBatch(requests []Request, cb Future, start uint64, flags int) *redis.Error {
	if len(requests) == 0 {
		if flags&DoTransaction != 0 {
			cb.Resolve([]interface{}{}, start)
		}
		return nil
	}

	if cb != nil && cb.Cancelled() {
		return conn.err(redis.ErrRequestCancelled)
	}

	shardn, shard := conn.getShard()
	shard.Lock()
	defer shard.Unlock()

	// we need to check conn.state first
	// since we do not lock connection itself, we need to use atomics.
	// Note: we do not check for connConnecting, ie we will try to send request after connection established.
	switch atomic.LoadUint32(&conn.state) {
	case connClosed:
		return conn.err(redis.ErrContextClosed).Wrap(conn.ctx.Err())
	case connDisconnected:
		return conn.err(redis.ErrNotConnected)
	}

	futures := shard.futures
	if flags&DoAsking != 0 {
		// send ASKING request before actual
		futures = append(futures, future{&dumb, 0, 0, Request{"ASKING", nil}})
	}
	if flags&DoTransaction != 0 {
		// send MULTI request for transaction start
		futures = append(futures, future{&dumb, 0, 0, Request{"MULTI", nil}})
	}

	now := nownano()

	for i, req := range requests {
		futures = append(futures, future{cb, start + uint64(i), now, req})
	}

	if flags&DoTransaction != 0 {
		// send EXEC request for transaction end
		futures = append(futures, future{cb, start + uint64(len(requests)), now, Request{"EXEC", nil}})
	}

	// should notify writer about this shard having queries
	// Since we are under shard lock, it is safe to send notification before assigning futures.
	if len(shard.futures) == 0 {
		conn.dirtyShard <- shardn
	}
	shard.futures = futures
	return nil
}

// wrapped preserves Cancelled method of wrapped future, but redefines Resolve to react only on result of EXEC.
type transactionFuture struct {
	Future
	l   int
	off uint64
}

func (cw transactionFuture) Resolve(res interface{}, n uint64) {
	if n == uint64(cw.l) {
		cw.Future.Resolve(res, cw.off)
	}
}

// SendTransaction implements redis.Sender.SendTransaction
func (conn *Connection) SendTransaction(reqs []Request, cb Future, off uint64) {
	if cb.Cancelled() {
		cb.Resolve(conn.err(redis.ErrRequestCancelled), off)
		return
	}
	conn.SendBatchFlags(reqs, transactionFuture{cb, len(reqs), off}, 0, DoTransaction)
}

// String implements fmt.Stringer
func (conn *Connection) String() string {
	return fmt.Sprintf("*redisconn.Connection{addr: %s}", conn.addr)
}

/********** private api **************/

// lock all shards to prevent creation of new requests.
// Under this lock, all already sent requests are revoked.
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

// setup connection to redis
func (conn *Connection) dial() error {
	var connection net.Conn
	var err error

	// detect network and actual address
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

	// dial to redis
	dialer := net.Dialer{
		Timeout:       timeout,
		DualStack:     true,
		FallbackDelay: timeout / 2,
		KeepAlive:     conn.opts.TCPKeepAlive,
	}
	connection, err = dialer.DialContext(conn.ctx, network, address)
	if err != nil {
		return conn.err(redis.ErrDial).Wrap(err)
	}

	dc := newDeadlineIO(connection, conn.opts.IOTimeout)
	r := bufio.NewReaderSize(dc, 128*1024)

	// Password request
	var req []byte
	if conn.opts.Password != "" {
		req, _ = redis.AppendRequest(req, redis.Req("AUTH", conn.opts.Password))
	}
	// Ping request
	req = append(req, pingReq...)
	// Select request
	if conn.opts.DB != 0 {
		req, _ = redis.AppendRequest(req, redis.Req("SELECT", conn.opts.DB))
	}
	// Force timeout
	if conn.opts.IOTimeout > 0 {
		connection.SetWriteDeadline(time.Now().Add(conn.opts.IOTimeout))
	}
	if _, err = dc.Write(req); err != nil {
		connection.Close()
		return conn.err(redis.ErrConnSetup).Wrap(err)
	}
	// Disarm timeout
	connection.SetWriteDeadline(time.Time{})

	var res interface{}
	// Password response
	if conn.opts.Password != "" {
		res = redis.ReadResponse(r)
		if err := redis.AsRedisError(res); err != nil {
			connection.Close()
			if strings.Contains(err.Error(), "password") {
				return conn.err(redis.ErrAuth).Wrap(err)
			}
			return conn.err(redis.ErrConnSetup).Wrap(err)
		}
	}
	// PING Response
	res = redis.ReadResponse(r)
	if err = redis.AsError(res); err != nil {
		connection.Close()
		return conn.err(redis.ErrConnSetup).Wrap(err)
	}
	if str, ok := res.(string); !ok || str != "PONG" {
		connection.Close()
		return conn.err(redis.ErrConnSetup).
			WithMsg("ping response mismatch").
			With(redis.EKResponse, res)
	}
	// SELECT DB Response
	if conn.opts.DB != 0 {
		res = redis.ReadResponse(r)
		if err = redis.AsError(res); err != nil {
			connection.Close()
			return conn.err(redis.ErrConnSetup).Wrap(err)
		}
		if str, ok := res.(string); !ok || str != "OK" {
			connection.Close()
			return conn.err(redis.ErrConnSetup).
				WithMsg("SELECT db response mismatch").
				With(EKDb, conn.opts.DB).With(redis.EKResponse, res)
		}
	}

	conn.c = connection

	one := &oneconn{
		c: connection,
		// We intentionally limit futures channel capacity:
		// this way we will force to write some first request eagerly to network,
		// and pause until first response returns.
		// During this time, many new request will be buffered, and then we will
		// be switching to steady state pipelining: new requests will be written
		// with the same speed responses will arrive.
		futures: make(chan []future, conn.opts.Concurrency/2+1),
		control: make(chan struct{}),
		futpool: make(chan []future, conn.opts.Concurrency),
	}

	go conn.writer(one)
	go conn.reader(r, one)

	return nil
}

func (conn *Connection) createConnection(reconnect bool, wg *sync.WaitGroup) error {
	var err error
	for conn.c == nil && atomic.LoadUint32(&conn.state) == connDisconnected {
		conn.report(LogConnecting)
		now := time.Now()
		// start accepting requests
		atomic.StoreUint32(&conn.state, connConnecting)
		if wg != nil {
			wg.Done()
			wg = nil
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
		// stop accepting request
		atomic.StoreUint32(&conn.state, connDisconnected)
		// revoke accumulated requests
		conn.lockShards()
		conn.dropShardFutures(err)
		conn.unlockShards()

		// If you doesn't use reconnection, quit
		if !reconnect {
			return err
		}
		conn.mutex.Unlock()
		// do not spend CPU on useless attempts
		time.Sleep(now.Add(conn.opts.ReconnectPause).Sub(time.Now()))
		conn.mutex.Lock()
	}
	if wg != nil {
		wg.Done()
	}
	if atomic.LoadUint32(&conn.state) == connClosed {
		err = conn.ctx.Err()
	}
	return err
}

// dropShardFutures revokes all accumulated requests
// Should be called with all shards locked.
func (conn *Connection) dropShardFutures(err error) {
	// first, empty dirtyShard queue.
	// since shards are locked at the moment, it has finite work to be done.
	for ok := true; ok; {
		select {
		case _, ok = <-conn.dirtyShard:
		default:
			ok = false
		}
	}
	// then Resolve all future with error
	for i := range conn.shard {
		sh := &conn.shard[i]
		for _, fut := range sh.futures {
			conn.resolve(fut, err)
		}
		sh.futures = nil
	}
}

func (conn *Connection) closeConnection(neterr error, forever bool) {
	if forever {
		atomic.StoreUint32(&conn.state, connClosed)
		conn.report(LogContextClosed)
	} else {
		atomic.StoreUint32(&conn.state, connDisconnected)
		conn.report(LogDisconnected, neterr)
	}

	if conn.c != nil {
		conn.c.Close()
		conn.c = nil
	}

	conn.lockShards()
	defer conn.unlockShards()
	if forever {
		// have to close dirtyShard under shards lock
		close(conn.dirtyShard)
	}

	conn.dropShardFutures(neterr)
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
			closeErr := conn.err(redis.ErrContextClosed).Wrap(conn.ctx.Err())
			conn.closeConnection(closeErr, true)
			return
		case <-t.C:
		}
		// send PING at least 3 times per IO timeout, therefore read deadline will not be exceeded
		if err := conn.Ping(); err != nil {
			// I really don't know what to do here :-(
		}
	}
}

// setErr is called by either read or write loop in case of error
func (one *oneconn) setErr(neterr error, conn *Connection) {
	// lets sure error is set only once
	one.erronce.Do(func() {
		// notify writer to stop writting
		close(one.control)
		rerr, ok := neterr.(*redis.Error)
		if !ok {
			rerr = conn.err(redis.ErrIO).Wrap(neterr)
		}
		one.err = rerr.WithNewKey(EKConnection, conn)
		// and try to reconnect asynchronously
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

// writer is a core writer loop. It is part of oneconn pair.
// It doesn't write requests immediately to network, but throttles itself to accumulate more requests.
// It is root of good pipelined performance: trade latency for throughtput.
func (conn *Connection) writer(one *oneconn) {
	var shardn uint32
	var packet []byte
	var futures []future
	var ok bool

	defer func() {
		// on method exit send last futures to read loop.
		// Read loop will revoke these requests with error.
		if len(futures) != 0 {
			one.futures <- futures
		}
		// And inform read loop that our reader-writer pair is dying.
		close(one.futures)
	}()

	round := 1023
	write := func() bool {
		if _, err := one.c.Write(packet); err != nil {
			one.setErr(err, conn)
			return false
		}
		// every 1023 writes check our buffer.
		// If it is too large, then lets GC to free it.
		if round--; round == 0 {
			round = 1023
			if cap(packet) > 128*1024 {
				packet = nil
			}
		}
		// otherwise, reuse buffer
		packet = packet[:0]
		return true
	}

BigLoop:
	// wait for dirtyShard or close of our reader-writer pair.
	select {
	case shardn, ok = <-conn.dirtyShard:
		if !ok {
			// user closed connection
			return
		}
	case <-one.control:
		// this reader-writer pair is obsolete
		return
	}

	if conn.opts.WritePause > 0 {
		// lets sleep a bit to accumulate more requests
		time.Sleep(conn.opts.WritePause)
	}

	for {
		shard := &conn.shard[shardn]
		shard.Lock()
		// fetch requests from shard, and replace it with empty buffer with non-zero capacity
		futures, shard.futures = shard.futures, futures
		shard.Unlock()

		// serialize requests
		for _, fut := range futures {
			var err error
			if packet, err = redis.AppendRequest(packet, fut.req); err != nil {
				// since we checked arguments in doSend and doSendBatch, error here is a signal of programmer error.
				// lets just panic and die.
				panic(err)
			}
		}

		if len(futures) == 0 {
			// There are multiple ways to come here, and most of them are through dropShardFutures.
			// Lets just ignore them.
			goto control
		}

		select {
		case one.futures <- futures:
			// Write buffer if it is large enough
			if len(packet) > 64*1024 && !write() {
				return
			}
		default:
			// Reader doesn't fetches requests because we didn't write for a long time.
			// After we wrote requests, reader will read answers, and then will fetch requests for answers.
			if !write() {
				return
			}
			// It blocks here. It is ok, because requests are buffered at this moment.
			one.futures <- futures
		}

		select {
		// reuse request buffer
		case futures = <-one.futpool:
		default:
			// or allocate new one
			futures = make([]future, 0, len(futures)*2)
		}

	control:
		select {
		case <-one.control:
			// this reader-writer pair is obsolete
			return
		default:
		}

		select {
		case shardn, ok = <-conn.dirtyShard:
			if !ok {
				// user closed connection
				return
			}
		default:
			// no new requests were buffered. Flush the buffer and go to blocking select.
			if len(packet) != 0 && !write() {
				return
			}
			goto BigLoop
		}
	}
}

func (conn *Connection) reader(r *bufio.Reader, one *oneconn) {
	var futures []future
	var i int
	var res interface{}
	var ok bool

	for {
		// try to read response from buffered socket.
		// Here is IOTimeout handled as well (through deadlineIO wrapper around socket).
		res = redis.ReadResponse(r)
		if rerr := redis.AsRedisError(res); rerr != nil {
			if redis.HardError(rerr) {
				// it is not redis-sended error, then close connection
				// (most probably, it is already closed. But also it could be timeout).
				one.setErr(rerr, conn)
				break
			} else {
				// otherwise, resolve future with this error.
				res = rerr.WithNewKey(EKConnection, conn)
			}
		}
		if i == len(futures) {
			// this batch of requests exhausted,
			// lets recycle it
			i = 0
			select {
			case one.futpool <- futures[:0]:
			default:
			}
			// and fetch next one.
			futures, ok = <-one.futures
			if !ok {
				break
			}
		}
		// fetch request corresponding to answer
		fut := futures[i]
		futures[i] = future{}
		i++
		// and resolve it
		conn.resolve(fut, res)
	}

	// oops, connection is broken.
	// Should resolve already fetched requests with error.
	for _, fut := range futures[i:] {
		conn.resolve(fut, one.err)
	}
	// And should resolve all remaining requests as well
	// (looping until writer closes channel).
	for futures := range one.futures {
		for _, fut := range futures {
			conn.resolve(fut, one.err)
		}
	}
}

// create error with connection as an attribute.
func (conn *Connection) err(kind redis.ErrorKind) *redis.Error {
	return kind.New().With(EKConnection, conn)
}
