package redisconn

import "github.com/joomcode/redispipe/rediswrap"

type Request = rediswrap.Request
type Callback = rediswrap.Callback

type future struct {
	Callback
	N uint64
}

func (f future) Call(res interface{}, err error) {
	if f.Callback != nil {
		f.Callback(res, err, f.N)
	}
}
