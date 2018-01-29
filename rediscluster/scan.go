package rediscluster

import (
	"github.com/joomcode/redispipe/redis"
)

type Scanner struct {
	redis.ScannerBase

	c     *Cluster
	addrs []string
}

func (c *Cluster) Scanner(opts redis.ScanOpts) redis.Scanner {
	var addrs []string

	if opts.Cmd == "" || opts.Cmd == "SCAN" {
		cfg := c.getConfig()
		addrs = make([]string, 0, len(cfg.masters))
		for addr := range cfg.masters {
			addrs = append(addrs, addr)
		}
		if len(addrs) == 0 {
			s := &Scanner{}
			s.Err = c.err(redis.ErrKindCluster, redis.ErrClusterConfigEmpty)
			return s
		}
	} else {
		// other commands operates on single key
		key := opts.Key
		slot := Slot(key)
		shard := c.getConfig().slot2shard(slot)
		addrs = shard.addr[:1]
	}

	return &Scanner{
		ScannerBase: redis.ScannerBase{ScanOpts: opts},

		c:     c,
		addrs: addrs,
	}
}

func (s *Scanner) Next(cb redis.Future) {
	if s.Err != nil {
		cb.Resolve(s.Err, 0)
		return
	}
	if s.IterLast() {
		s.addrs = s.addrs[1:]
		s.Iter = nil
	}
	if len(s.addrs) == 0 && s.Iter == nil {
		cb.Resolve(nil, 0)
		return
	}
	conn := s.c.connForAddress(s.addrs[0])
	if conn == nil {
		s.Err = s.c.err(redis.ErrKindConnection, redis.ErrNotConnected).
			With("address", s.addrs[0])
		cb.Resolve(s.Err, 0)
		return
	}
	s.DoNext(cb, conn)
}
