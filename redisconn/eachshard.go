package redisconn

func (c *Connection) EachShard(cb func(*Connection, error)) {
	cb(c, nil)
	cb(nil, nil)
}
