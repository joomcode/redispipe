package redisconn

func (c *Connection) EachShard(cb func(c *Connection)) {
	cb(c)
}
