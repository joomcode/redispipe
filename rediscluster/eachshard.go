package rediscluster

import "github.com/joomcode/redispipe/redisconn"

func (c *Cluster) EachShard(cb func(c *redisconn.Connection) bool) {
	masters := c.getMasterMap()
	shards := c.getShardMap()
	nodes := c.getNodeMap()
	for _, shno := range masters {
		shard := shards[shno]
		if shard == nil {
			cb(nil)
			return
		}
		node := nodes[shard.addr[0]]
		if node == nil {
			cb(nil)
			return
		}
		conn := node.getConn(c.opts.ConnHostPolicy, needConnected)
		if conn == nil {
			conn = node.getConn(c.opts.ConnHostPolicy, mayBeConnected)
		}
		if cb(conn) {
			return
		}
		if conn == nil {
			return
		}
	}
}
