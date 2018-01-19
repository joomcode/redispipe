package redis

func Req(cmd string, args ...interface{}) Request {
	return Request{cmd, args}
}

type Request struct {
	Cmd  string
	Args []interface{}
}

func (req Request) Key() (string, bool) {
	if req.Cmd == "RANDOMKEY" {
		return "RANDOMKEY", false
	}
	n := 0
	if req.Cmd == "EVAL" || req.Cmd == "EVALSHA" || req.Cmd == "BITOP" {
		n = 1
	}
	if len(req.Args) <= n {
		return "", false
	}
	return ArgToString(req.Args[n])
}

type Cancelling interface {
	Cancelled() bool
}

type Future interface {
	Resolve(res interface{}, n uint64)
	Cancelled() bool
}

type FutureWrapped struct {
	Cancelling
	Func func(res interface{}, n uint64)
}

func (cw FutureWrapped) Resolve(res interface{}, n uint64) {
	cw.Func(res, n)
}

func WrapFuture(act Cancelling, f func(res interface{}, n uint64)) Future {
	return FutureWrapped{act, f}
}

type FuncFuture func(res interface{}, n uint64)

func (f FuncFuture) Cancelled() bool                   { return false }
func (f FuncFuture) Resolve(res interface{}, n uint64) { f(res, n) }
