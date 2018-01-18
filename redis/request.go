package redis

func Req(cmd string, args ...interface{}) Request {
	return Request{cmd, args}
}

type Request struct {
	Cmd  string
	Args []interface{}
}

type Activer interface {
	Active() bool
}

type Future interface {
	Resolve(res interface{}, n uint64)
	Active() bool
}

type FutureWrapped struct {
	Activer
	Func func(res interface{}, n uint64)
}

func (cw FutureWrapped) Resolve(res interface{}, n uint64) {
	cw.Func(res, n)
}

func WrapFuture(act Activer, f func(res interface{}, n uint64)) Future {
	return FutureWrapped{act, f}
}

type FuncFuture func(res interface{}, n uint64)

func (f FuncFuture) Active() bool                      { return true }
func (f FuncFuture) Resolve(res interface{}, n uint64) { f(res, n) }
