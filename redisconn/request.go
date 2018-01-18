package redisconn

import "github.com/joomcode/redispipe/redis"

type Request = redis.Request
type Future = redis.Future

type future struct {
	Future
	N uint64
}

func (f future) call(res interface{}) {
	if f.Future != nil {
		f.Future.Resolve(res, f.N)
	}
}
