package rediswrap

import "github.com/joomcode/redispipe/resp"

type Request = resp.Request

type Callback func(res interface{}, n uint64)

type Sender interface {
	Send(r Request, cb Callback, n uint64)
	SendTransaction(r []Request, cb Callback, start uint64)
}

type SendBatcher interface {
	SendBatch(r []Request, cb Callback, n uint64)
}
