package redisconn

import "github.com/joomcode/redispipe/redis"

type Request = redis.Request
type Callback = redis.Callback

type future struct {
	Callback
	N uint64
}

func (f future) Call(res interface{}) {
	if f.Callback != nil {
		f.Callback(res, f.N)
	}
}
