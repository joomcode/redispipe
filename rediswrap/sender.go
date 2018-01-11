package rediswrap

type Request struct {
	Cmd  string
	Args []interface{}
}

type Callback func(res interface{}, err error, n uint64)

type Sender interface {
	Send(r Request, cb Callback, n uint64)
	SendBatch(r []Request, cb Callback, start uint64)
}
