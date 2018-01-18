package redisconn

import (
	"github.com/joomcode/redispipe/rediswrap"
	"github.com/joomcode/redispipe/resp"
)

type Scanner struct {
	rediswrap.ScanOpts

	c  *Connection
	it []byte
	cb func([]string, error)
}

func (s *Scanner) Next(cb func(keys []string, err error)) {
	if len(s.it) == 1 && s.it[0] == '0' {
		cb(nil, rediswrap.ScanEOF)
		return
	}
	s.cb = cb
	s.c.CallScan(s.ScanOpts, s.it, s.set)
}

func (s *Scanner) set(it []byte, keys []string, err error) {
	s.it = it
	s.cb(keys, err)
}

func (s *Connection) CallScan(opts rediswrap.ScanOpts, it []byte, cb func([]byte, []string, error)) {
	set := func(res interface{}, _ uint64) { cb(resp.ScanResponse(res)) }
	s.Send(opts.Request(it), set, 0)
}

func (c *Connection) Scanner(opts rediswrap.ScanOpts) rediswrap.Scanner {
	return &Scanner{
		ScanOpts: opts,
		c:        c,
		it:       nil,
	}
}
