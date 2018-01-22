package redisconn

import "github.com/joomcode/redispipe/redis"

func (c *Connection) EachShard(cb func(redis.Sender, error) bool) {
	if cb(c, nil) {
		return
	}
	cb(nil, nil)
}
