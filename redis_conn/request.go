package redis_conn

type Request struct {
	Cmd  string
	Args []interface{}
}

type Future struct {
	Result interface{}
	Err    error
	wait   chan struct{}
}

func (r *Future) Done() <-chan struct{} {
	return r.wait
}

var closedChan = make(chan struct{})

func init() {
	close(closedChan)
}
