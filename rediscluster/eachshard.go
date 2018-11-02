package rediscluster

import (
	"github.com/joomcode/redispipe/redis"
)

// EachShard implements redis.Sender.EachShard
func (c *Cluster) EachShard(cb func(redis.Sender, error) bool) {
	cfg := c.getConfig()
	for _, shard := range cfg.shards {
		node := cfg.nodes[shard.addr[0]]
		if node == nil {
			cb(nil, c.err(redis.ErrDial))
			return
		}
		conn := node.getConn(c.opts.ConnHostPolicy, preferConnected, nil)
		if conn == nil {
			cb(nil, c.err(redis.ErrDial))
			return
		}
		if !cb(conn, nil) {
			return
		}
	}
}
