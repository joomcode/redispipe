package redisconn

import (
	"github.com/joomcode/redispipe/redis"
)

type Scanner struct {
	redis.ScanOpts

	c  *Connection
	it []byte
	cb func([]string, error)
}

func (s *Scanner) Next(cb func(keys []string, err error)) {
	if len(s.it) == 1 && s.it[0] == '0' {
		cb(nil, redis.ScanEOF)
		return
	}
	s.cb = cb
	s.c.CallScan(s.ScanOpts, s.it, s.set)
}

func (s *Scanner) set(it []byte, keys []string, err error) {
	s.it = it
	s.cb(keys, err)
}

func (s *Connection) CallScan(opts redis.ScanOpts, it []byte, cb func([]byte, []string, error)) {
	set := func(res interface{}, _ uint64) { cb(redis.ScanResponse(res)) }
	s.Send(opts.Request(it), set, 0)
}

func (c *Connection) Scanner(opts redis.ScanOpts) redis.Scanner {
	return &Scanner{
		ScanOpts: opts,
		c:        c,
		it:       nil,
	}
}
