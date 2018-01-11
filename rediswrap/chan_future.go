package rediswrap

import "sync/atomic"

type ChanFuture struct {
	r    Result
	wait chan struct{}
}

func (f ChanFuture) Value() interface{} {
	<-f.wait
	return f.r.Value()
}

func (f ChanFuture) Error() error {
	<-f.wait
	return f.r.Error()
}

func (f ChanFuture) AnyError() error {
	<-f.wait
	return f.r.AnyError()
}

func (f ChanFuture) Done() <-chan struct{} {
	return f.wait
}

func (f *ChanFuture) set(res interface{}, err error, _ uint64) {
	f.r.val, f.r.err = res, err
	close(f.wait)
}

type ChanFutures []ChanFuture

func (f ChanFutures) set(res interface{}, err error, i uint64) {
	f[i].set(res, err, i)
}

type BatchChanFuture struct {
	r    []Result
	cnt  uint32
	wait chan struct{}
}

func (f *BatchChanFuture) Results() []Result {
	<-f.wait
	return f.r
}

func (f BatchChanFuture) Done() <-chan struct{} {
	return f.wait
}

func (f BatchChanFuture) set(res interface{}, err error, i uint64) {
	r := &f.r[i]
	r.val, r.err = res, err
	if atomic.AddUint32(&f.cnt, 1) == uint32(len(f.r)) {
		close(f.wait)
	}
}

type ChanFutured struct {
	S Sender
}

func (s ChanFutured) Send(r Request) *ChanFuture {
	f := &ChanFuture{wait: make(chan struct{})}
	s.S.Send(r, Callback(f.set), 0)
	return f
}

func (s ChanFutured) SendMany(r []Request) ChanFutures {
	futures := make(ChanFutures, len(r))
	for i := range futures {
		futures[i].wait = make(chan struct{})
	}
	s.S.SendBatch(r, futures.set, 0)
	return futures
}

func (s ChanFutured) SendBatch(r []Request) *BatchChanFuture {
	future := &BatchChanFuture{
		r:    make([]Result, len(r)),
		wait: make(chan struct{}),
	}
	s.S.SendBatch(r, future.set, 0)
	return future
}
