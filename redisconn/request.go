package redisconn

import (
	"github.com/joomcode/redispipe/rediswrap"
	"github.com/joomcode/redispipe/resp"
)

type Request = resp.Request
type Callback = rediswrap.Callback

type future struct {
	Callback
	N uint64
}

func (f future) Call(res interface{}) {
	if f.Callback != nil {
		f.Callback(res, f.N)
	}
}
