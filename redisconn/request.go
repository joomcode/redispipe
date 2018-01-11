package redisconn

import "github.com/joomcode/redispipe/rediswrap"

type Request = rediswrap.Request
type Callback = rediswrap.Callback

type Future struct {
	Callback
	N uint64
}

func (f Future) Call(res interface{}, err error) {
	if f.Callback != nil {
		f.Callback(res, err, f.N)
	}
}
