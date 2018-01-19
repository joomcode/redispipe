package rediscluster

import (
	"github.com/joomcode/redispipe/redis"
)

func (c *Cluster) EachShard(cb func(redis.Sender, error) bool) {
	masters := c.getMasterMap()
	shards := c.getShardMap()
	nodes := c.getNodeMap()
	for _, shno := range masters {
		shard := shards[shno]
		if shard == nil {
			cb(nil, c.err(redis.ErrKindConnection, redis.ErrDial))
			return
		}
		node := nodes[shard.addr[0]]
		if node == nil {
			cb(nil, c.err(redis.ErrKindConnection, redis.ErrDial))
			return
		}
		conn := node.getConn(c.opts.ConnHostPolicy, needConnected)
		if conn == nil {
			conn = node.getConn(c.opts.ConnHostPolicy, mayBeConnected)
		}
		if conn == nil {
			cb(nil, c.err(redis.ErrKindConnection, redis.ErrDial))
			return
		}
		if cb(conn, nil) {
			return
		}
	}
	cb(nil, nil)
}
