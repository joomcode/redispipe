package redis

// Req - convenient wrapper to create Request.
func Req(cmd string, args ...interface{}) Request {
	return Request{cmd, args}
}

// Request represents request to be passed to redis.
type Request struct {
	Cmd  string
	Args []interface{}
}

// Key returns first field of request that should be used as a key for redis cluster.
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

// Future is interface accepted by Sender to signal request completion.
type Future interface {
	// Resolve is called by sender to pass result (or error) for particular request.
	// Single future could be used for accepting multiple results.
	// n argument is used then to distinguish request this result is for.
	Resolve(res interface{}, n uint64)
	// Cancelled method could inform sender that request is abandoned.
	// It is called usually before sending request, and if Cancelled returns true,
	// then Sender calls Resolve with ErrRequestCancelled error.
	Cancelled() bool
}

// FuncFuture simple wrapper that makes Future from function.
type FuncFuture func(res interface{}, n uint64)

// Cancelled implements Future.Cancelled (always false)
func (f FuncFuture) Cancelled() bool { return false }

// Resolve implements Future.Resolve (by calling wrapped function).
func (f FuncFuture) Resolve(res interface{}, n uint64) { f(res, n) }
