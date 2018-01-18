package rediscluster

import (
	"fmt"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediswrap"
)

type Scanner struct {
	rediswrap.ScanOpts

	err   error
	c     *Cluster
	s     rediswrap.Scanner
	addrs []string
	cb    func([]string, error)
}

func (c *Cluster) Scanner(opts rediswrap.ScanOpts) rediswrap.Scanner {
	masters := c.getMasterMap()
	addrs := make([]string, 0, len(masters))
	for addr := range masters {
		addrs = append(addrs, addr)
	}
	fmt.Println("addrs", addrs)
	if len(addrs) == 0 {
		return &Scanner{
			err: redis.New(redis.ErrKindCluster, redis.ErrClusterConfigEmpty).With("cluster", c),
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
		cb(nil, rediswrap.ScanEOF)
		return
	}
	if s.s == nil {
		conn := s.c.connForAddress(s.addrs[0])
		s.addrs = s.addrs[1:]
		if conn == nil {
			cb(nil, redis.New(redis.ErrKindConnection, redis.ErrNotConnected).
				With("cluster", s.c).With("addr", s.addrs[0]))
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
	if err != nil && err != rediswrap.ScanEOF {
		cb(keys, err)
	} else if keys == nil {
		s.s = nil
		s.Next(cb)
	} else {
		cb(keys, err)
	}
}
