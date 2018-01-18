package rediscluster

import (
	"github.com/joomcode/redispipe/redis"
)

type Scanner struct {
	redis.ScanOpts

	err   error
	c     *Cluster
	s     redis.Scanner
	addrs []string
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
	if len(s.addrs) == 0 && s.s == nil {
		cb(nil, redis.ScanEOF)
		return
	}
	if s.s == nil {
		conn := s.c.connForAddress(s.addrs[0])
		s.addrs = s.addrs[1:]
		if conn == nil {
			cb(nil, s.c.err(redis.ErrKindConnection, redis.ErrNotConnected).
				With("address", s.addrs[0]))
			return
		}
		s.s = conn.Scanner(s.ScanOpts)
	}
	s.cb = cb
	s.s.Next(s.set)
}

func (s *Scanner) set(keys []string, err error) {
	cb := s.cb
	s.cb = nil
	if err != nil && err != redis.ScanEOF {
		s.err = err
		cb(keys, err)
	} else if keys == nil {
		s.s = nil
		s.Next(cb)
	} else {
		s.err = err
		cb(keys, err)
	}
}
