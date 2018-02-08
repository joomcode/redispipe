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
	var n int
	switch req.Cmd {
	case "EVAL", "EVALSHA":
		n = 2
	case "BITOP":
		n = 1
	default:
		n = 0
	}
	if len(req.Args) <= n {
		return "", false
	}
	return ArgToString(req.Args[n])
}

type Future interface {
	Resolve(res interface{}, n uint64)
	Cancelled() bool
}

type FuncFuture func(res interface{}, n uint64)

func (f FuncFuture) Cancelled() bool                   { return false }
func (f FuncFuture) Resolve(res interface{}, n uint64) { f(res, n) }
