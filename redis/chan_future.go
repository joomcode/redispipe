package redis

type ChanFutured struct {
	S Sender
}

func (s ChanFutured) Send(r Request) *ChanFuture {
	f := &ChanFuture{wait: make(chan struct{})}
	s.S.Send(r, Future(f), 0)
	return f
}

func (s ChanFutured) SendMany(reqs []Request) ChanFutures {
	futures := make(ChanFutures, len(reqs))
	for i := range futures {
		futures[i] = &ChanFuture{wait: make(chan struct{})}
	}
	s.S.SendMany(reqs, futures, 0)
	return futures
}

func (s ChanFutured) SendTransaction(r []Request) *ChanTransaction {
	future := &ChanTransaction{
		ChanFuture: ChanFuture{wait: make(chan struct{})},
	}
	s.S.SendTransaction(r, future, 0)
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

func (f *ChanFuture) Resolve(res interface{}, _ uint64) {
	f.r = res
	close(f.wait)
}

func (f *ChanFuture) Active() bool {
	return true
}

type ChanFutures []*ChanFuture

func (f ChanFutures) Active() bool {
	return true
}

func (f ChanFutures) Resolve(res interface{}, i uint64) {
	f[i].Resolve(res, i)
}

type ChanTransaction struct {
	ChanFuture
}

func (f *ChanTransaction) Results() ([]interface{}, error) {
	<-f.wait
	return TransactionResponse(f.r)
}

func (f ChanTransaction) Done() <-chan struct{} {
	return f.wait
}
