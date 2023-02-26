// Package redisdumb contains dumbest implementation of redis.Sender
package redisdumb

import (
	"bufio"
	"crypto/tls"
	"net"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
	"github.com/joomcode/redispipe/redisconn"
)

// ConnType - type of connection (simple server, or cluster aware).
type ConnType int

const (
	// TypeSimple (default) is for connection to single server.
	TypeSimple ConnType = 0
	// TypeCluster is for connection which awares for cluster redirects.
	TypeCluster ConnType = 1
)

// DefaultTimeout is default timeout.
var DefaultTimeout time.Duration = 5 * time.Second

// Conn is a simplest blocking implementation of redis.Sender.
type Conn struct {
	Addr       string
	TlsAddr    string
	C          net.Conn
	R          *bufio.Reader
	Timeout    time.Duration
	Type       ConnType
	TLSEnabled bool
	TLSConfig  tls.Config
}

// Do issues command to servers.
// It handles reconnection and redirection (if Conn.Type==TypeCluster).
func (c *Conn) Do(cmd string, args ...interface{}) interface{} {
	timeout := c.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	try := 1
	if c.C != nil {
		try = 2
	}
	var req []byte
	var err error
	var asking bool
	for i := 0; i < try; i++ {
		if c.C == nil {
			if c.TLSEnabled {
				dialer := net.Dialer{
					Timeout: timeout,
				}
				tlsDialer := tls.Dialer{NetDialer: &dialer, Config: &c.TLSConfig}
				c.C, err = tlsDialer.Dial("tcp", c.TlsAddr)
			} else {
				c.C, err = net.DialTimeout("tcp", c.Addr, timeout)
			}
			if err != nil {
				return redisconn.ErrDial.WrapWithNoMessage(err)
			}
			c.R = bufio.NewReader(c.C)
		}
		if asking {
			c.Do("ASKING")
		}
		c.C.SetDeadline(time.Now().Add(timeout))
		req, err = redis.AppendRequest(nil, redis.Request{cmd, args})
		if err == nil {
			if _, err = c.C.Write(req); err == nil {
				res := redis.ReadResponse(c.R)
				rerr := redis.AsErrorx(res)
				if rerr == nil {
					return res
				}
				err = rerr
				if c.Type == TypeCluster && rerr.HasTrait(redis.ErrTraitClusterMove) {
					asking = rerr.IsOfType(redis.ErrAsk)
					v, _ := rerr.Property(redis.EKMovedTo)
					c.Addr = v.(string)
					if try < 5 {
						try++
					}
				}
			} else {
				err = redis.ErrIO.WrapWithNoMessage(err)
			}
		}
		c.C.Close()
		c.C = nil
	}
	return err
}

// Send implements redis.Sender.Send
func (c *Conn) Send(r redis.Request, cb redis.Future, n uint64) {
	res := c.Do(r.Cmd, r.Args...)
	cb.Resolve(res, n)
}

// SendMany implements redis.Sender.SendMany.
// Note, it does it in a dumb way: commands are executed sequentially.
func (c *Conn) SendMany(reqs []redis.Request, cb redis.Future, n uint64) {
	for i, r := range reqs {
		res := c.Do(r.Cmd, r.Args...)
		cb.Resolve(res, n+uint64(i))
	}
}

// SendTransaction implements redis.Sender.SendTransaction.
func (c *Conn) SendTransaction(reqs []redis.Request, cb redis.Future, n uint64) {
	if c.Type == TypeCluster {
		// first, redirect ourself to right master
		key, ok := redisclusterutil.BatchKey(reqs)
		if !ok {
			cb.Resolve(redis.ErrNoSlotKey.New("no key to determine slot"), n)
			return
		}
		c.Do("TYPE", key)
		// ok, now we are at right shard, I hope.
	}
	res := c.Do("MULTI")
	if err := redis.AsError(res); err != nil {
		cb.Resolve(res, n)
		return
	}
	for _, r := range reqs {
		res = c.Do(r.Cmd, r.Args...)
		if err := redis.AsErrorx(res); !err.IsOfType(redis.ErrResult) {
			cb.Resolve(res, n)
			return
		}
	}
	res = c.Do("EXEC")
	cb.Resolve(res, n)
}

// EachShard implements redis.Sender.EachShard.
func (c *Conn) EachShard(f func(redis.Sender, error) bool) {
	// TODO: correctly iterate through cluster
	switch c.Type {
	case TypeSimple:
		f(c, nil)
	case TypeCluster:
		nodeInfos, err := redisclusterutil.ParseClusterNodes(c.Do("CLUSTER NODES"))
		if err != nil {
			f(c, err)
		}
		for _, nodeInfo := range nodeInfos {
			if nodeInfo.AddrValid() && nodeInfo.IsMaster() {
				con := &Conn{Addr: nodeInfo.Addr}
				if !f(con, nil) {
					return
				}
			}
		}
	}
}

// Scanner implements redis.Scanner
type Scanner struct {
	redis.ScannerBase
	c *Conn
}

// Next implements redis.Scanner.Next
func (s *Scanner) Next(cb redis.Future) {
	if s.Err != nil {
		cb.Resolve(s.Err, 0)
		return
	}
	if s.IterLast() {
		cb.Resolve(nil, 0)
		return
	}
	s.DoNext(cb, s.c)
}

// Scanner implements redis.Sender.Scanner
func (c *Conn) Scanner(opts redis.ScanOpts) redis.Scanner {
	return &Scanner{
		ScannerBase: redis.ScannerBase{ScanOpts: opts},
		c:           c,
	}
}

// Close closes connection (implements redis.Sender.Close)
func (c *Conn) Close() {
	if c.C != nil {
		c.C.Close()
		c.C = nil
	}
}

// Do is shortcut for issuing single command to redis by address.
func Do(addr string, cmd string, args ...interface{}) interface{} {
	conn, err := net.DialTimeout("tcp", addr, DefaultTimeout)
	if err != nil {
		return redisconn.ErrDial.WrapWithNoMessage(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(DefaultTimeout))
	req, rerr := redis.AppendRequest(nil, redis.Request{cmd, args})
	if rerr != nil {
		return rerr
	}
	if _, err = conn.Write(req); err != nil {
		return redis.ErrIO.WrapWithNoMessage(err)
	}
	res := redis.ReadResponse(bufio.NewReader(conn))
	return res
}
