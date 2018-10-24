package redisdumb

import (
	"bufio"
	"net"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
)

type ConnType int

const (
	TypeSimple  ConnType = 0
	TypeCluster ConnType = 1
)

var DefaultTimeout time.Duration = 5 * time.Second

type Conn struct {
	Addr    string
	C       net.Conn
	R       *bufio.Reader
	Timeout time.Duration
	Type    ConnType
}

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
			c.C, err = net.DialTimeout("tcp", c.Addr, timeout)
			if err != nil {
				return redis.NewErr(redis.ErrKindIO, redis.ErrDial).Wrap(err)
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
				if rerr := redis.AsRedisError(res); rerr == nil {
					return res
				} else {
					err = rerr
					if c.Type == TypeCluster && (rerr.Code == redis.ErrAsk || rerr.Code == redis.ErrMoved) {
						asking = rerr.Code == redis.ErrAsk
						c.Addr = rerr.Get("movedto").(string)
						if try < 5 {
							try++
						}
					}
				}
			} else {
				err = redis.NewErr(redis.ErrKindIO, redis.ErrIO).Wrap(err)
			}
		}
		c.C.Close()
		c.C = nil
	}
	return err
}

func (c *Conn) Send(r redis.Request, cb redis.Future, n uint64) {
	res := c.Do(r.Cmd, r.Args...)
	cb.Resolve(res, n)
}

func (c *Conn) SendMany(reqs []redis.Request, cb redis.Future, n uint64) {
	for i, r := range reqs {
		res := c.Do(r.Cmd, r.Args...)
		cb.Resolve(res, n+uint64(i))
	}
}

func (c *Conn) SendTransaction(reqs []redis.Request, cb redis.Future, n uint64) {
	if c.Type == TypeCluster {
		// first, redirect ourself to right master
		key, ok := redisclusterutil.BatchKey(reqs)
		if !ok {
			cb.Resolve(redis.NewErr(redis.ErrKindRequest, redis.ErrNoSlotKey), n)
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
		if err := redis.AsRedisError(res); redis.HardError(err) {
			cb.Resolve(res, n)
			return
		}
	}
	res = c.Do("EXEC")
	cb.Resolve(res, n)
}

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
			if nodeInfo.IsMaster() {
				con := &Conn{Addr: nodeInfo.Addr}
				if !f(con, nil) {
					return
				}
			}
		}
	}
}

type Scanner struct {
	redis.ScannerBase
	c *Conn
}

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

func (c *Conn) Scanner(opts redis.ScanOpts) redis.Scanner {
	return &Scanner{
		ScannerBase: redis.ScannerBase{ScanOpts: opts},
		c:           c,
	}
}

func (c *Conn) Close() {
	if c.C != nil {
		c.C.Close()
		c.C = nil
	}
}

func Do(addr string, cmd string, args ...interface{}) interface{} {
	conn, err := net.DialTimeout("tcp", addr, DefaultTimeout)
	if err != nil {
		return redis.NewErr(redis.ErrKindIO, redis.ErrDial).Wrap(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(DefaultTimeout))
	req, rerr := redis.AppendRequest(nil, redis.Request{cmd, args})
	if rerr != nil {
		return rerr
	}
	if _, err = conn.Write(req); err != nil {
		return redis.NewErr(redis.ErrKindIO, redis.ErrIO).Wrap(err)
	}
	res := redis.ReadResponse(bufio.NewReader(conn))
	return res
}
