package redis

import (
	"fmt"
	"strings"
)

// Req - convenient wrapper to create Request.
func Req(cmd string, args ...interface{}) Request {
	return Request{cmd, args}
}

// Request represents request to be passed to redis.
type Request struct {
	// Cmd is a redis command to be sent.
	// It could contain single space, then it will be split, and last part will be serialized as an argument.
	Cmd  string
	Args []interface{}
}

func (r Request) String() string {
	args := r.Args
	if len(args) > 5 {
		args = args[:5]
	}
	argss := make([]string, 0, 1+len(args))
	for _, arg := range args {
		argStr := fmt.Sprintf("%v", arg)
		if len(argStr) > 32 {
			argStr = argStr[:32] + "..."
		}
		argss = append(argss, argStr)
	}
	if len(r.Args) > 5 {
		argss = append(argss, "...")
	}
	return fmt.Sprintf("Req(%q, %q)", r.Cmd, argss)
}

// CommandName returns primary redis command name.
// Cmd could contain single space (see Request.Cmd); only part before space is returned.
func (r Request) CommandName() string {
	if i := strings.IndexByte(r.Cmd, ' '); i >= 0 {
		return r.Cmd[:i]
	}
	return r.Cmd
}

// Key returns first field of request that should be used as a key for redis cluster.
func (r Request) Key() (string, bool) {
	if r.Cmd == "RANDOMKEY" {
		return "RANDOMKEY", false
	}
	switch r.CommandName() {
	case "XREAD", "XREADGROUP":
		return xreadKey(r.Args)
	}
	var n int
	switch r.Cmd {
	case "EVAL", "EVALSHA":
		n = 2
	case "BITOP":
		n = 1
	default:
		n = 0
	}
	if len(r.Args) <= n {
		return "", false
	}
	return ArgToString(r.Args[n])
}

func xreadKey(args []interface{}) (string, bool) {
	for i, arg := range args {
		s, ok := ArgToString(arg)
		if !ok || !strings.EqualFold(s, "STREAMS") {
			continue
		}
		if i+1 >= len(args) {
			return "", false
		}
		return ArgToString(args[i+1])
	}
	return "", false
}

// Future is interface accepted by Sender to signal request completion.
type Future interface {
	// Resolve is called by sender to pass result (or error) for particular request.
	// Single future could be used for accepting multiple results.
	// n argument is used then to distinguish request this result is for.
	Resolve(res interface{}, n uint64)
	// Cancelled method could inform sender that request is abandoned.
	// It is called usually before sending request, and if Cancelled returns non-nil error,
	// then Sender calls Resolve with ErrRequestCancelled error wrapped around returned error.
	Cancelled() error
}

// FuncFuture simple wrapper that makes Future from function.
type FuncFuture func(res interface{}, n uint64)

// Cancelled implements Future.Cancelled (always false)
func (f FuncFuture) Cancelled() error { return nil }

// Resolve implements Future.Resolve (by calling wrapped function).
func (f FuncFuture) Resolve(res interface{}, n uint64) { f(res, n) }
