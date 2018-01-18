package rediscluster

import (
	"github.com/joomcode/redispipe/redis"
)

type Scanner struct {
	redis.ScanOpts

	err   error
	c     *Cluster
	addrs []string
	it    []byte
	cb    func([]string, error)
}

func (c *Cluster) Scanner(opts redis.ScanOpts) redis.Scanner {
	masters := c.getMasterMap()
	addrs := make([]string, 0, len(masters))
	for addr := range masters {
		addrs = append(addrs, addr)
	}
	if len(addrs) == 0 {
		return &Scanner{
			err: c.err(redis.ErrKindCluster, redis.ErrClusterConfigEmpty),
		}
	}

	return &Scanner{
		ScanOpts: opts,

		c:     c,
		addrs: addrs,
	}
}

func (s *Scanner) Next(cb func(keys []string, err error)) {
	if s.err != nil {
		cb(nil, s.err)
		return
	}
	if len(s.it) == 1 && s.it[0] == '0' {
		s.addrs = s.addrs[1:]
		s.it = nil
	}
	if len(s.addrs) == 0 && s.it == nil {
		cb(nil, redis.ScanEOF)
		return
	}
	conn := s.c.connForAddress(s.addrs[0])
	if conn == nil {
		s.err = s.c.err(redis.ErrKindConnection, redis.ErrNotConnected).
			With("address", s.addrs[0])
		cb(nil, s.err)
		return
	}
	s.cb = cb
	conn.CallScan(s.ScanOpts, s.it, s.set)
}

func (s *Scanner) set(it []byte, keys []string, err error) {
	cb := s.cb
	s.cb = nil
	s.it = it
	s.err = err
	cb(keys, err)
}
