package rediswrap

import "github.com/joomcode/redispipe/resp"

type ChanFutured struct {
	S Sender
}

func (s ChanFutured) Send(r Request) *ChanFuture {
	f := &ChanFuture{wait: make(chan struct{})}
	s.S.Send(r, Callback(f.set), 0)
	return f
}

func (s ChanFutured) SendMany(reqs []Request) ChanFutures {
	futures := make(ChanFutures, len(reqs))
	for i := range futures {
		futures[i] = &ChanFuture{wait: make(chan struct{})}
	}
	if batcher, ok := s.S.(SendBatcher); ok {
		batcher.SendBatch(reqs, futures.set, 0)
	} else {
		cb := futures.set
		for i, req := range reqs {
			s.S.Send(req, cb, uint64(i))
		}
	}
	return futures
}

func (s ChanFutured) SendTransaction(r []Request) *ChanTransaction {
	future := &ChanTransaction{
		n:    len(r),
		wait: make(chan struct{}),
	}
	s.S.SendTransaction(r, future.set, 0)
	return future
}

type ChanFuture struct {
	r    interface{}
	wait chan struct{}
}

func (f ChanFuture) Value() interface{} {
	<-f.wait
	return f.r
}

func (f ChanFuture) Done() <-chan struct{} {
	return f.wait
}

func (f *ChanFuture) set(res interface{}, _ uint64) {
	f.r = res
	close(f.wait)
}

type ChanFutures []*ChanFuture

func (f ChanFutures) set(res interface{}, i uint64) {
	f[i].set(res, i)
}

type ChanTransaction struct {
	r    []interface{}
	n    int
	wait chan struct{}
}

func (f *ChanTransaction) Results() []interface{} {
	<-f.wait
	return f.r
}

func (f ChanTransaction) Done() <-chan struct{} {
	return f.wait
}

func (f ChanTransaction) set(res interface{}, i uint64) {
	f.r = resp.TransactionResponse(res, f.n)
	close(f.wait)
}
