package redis

import (
	"context"
	"sync/atomic"
)

type SyncCtx struct {
	S Sender
}

func (s SyncCtx) Do(ctx context.Context, cmd string, args ...interface{}) interface{} {
	return s.Send(ctx, Request{cmd, args})
}

func (s SyncCtx) Send(ctx context.Context, r Request) interface{} {
	res := ctxRes{active: newActive(ctx)}

	s.S.Send(r, &res, 0)

	select {
	case <-ctx.Done():
		return NewErr(ErrKindRequest, ErrRequestCancelled)
	case <-res.ch:
		return res.r
	}
}

func (s SyncCtx) SendMany(ctx context.Context, reqs []Request) interface{} {
	res := ctxBatch{
		active: newActive(ctx),
		r:      make([]interface{}, len(reqs)),
		o:      make([]uint32, len(reqs)),
		cnt:    0,
	}

	s.S.SendMany(reqs, &res, 0)

	select {
	case <-ctx.Done():
		err := NewErr(ErrKindRequest, ErrRequestCancelled)
		for i := range res.o {
			res.Resolve(err, uint64(i))
		}
		<-res.ch
	case <-res.ch:
	}
	return res.r
}

func (s SyncCtx) SendTransaction(ctx context.Context, reqs []Request) ([]interface{}, error) {
	res := ctxRes{active: newActive(ctx)}

	s.S.SendTransaction(reqs, &res, 0)

	var r interface{}
	select {
	case <-ctx.Done():
		r = NewErr(ErrKindRequest, ErrRequestCancelled)
	case <-res.ch:
		r = res.r
	}

	return TransactionResponse(r)
}

func (s SyncCtx) Scanner(ctx context.Context, opts ScanOpts) SyncCtxIterator {
	return SyncCtxIterator{ctx, s.S.Scanner(opts)}
}

type active struct {
	ctx context.Context
	ch  chan struct{}
}

func newActive(ctx context.Context) active {
	return active{ctx, make(chan struct{})}
}

func (c active) Cancelled() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}

func (c active) done() {
	close(c.ch)
}

type ctxRes struct {
	active
	r interface{}
}

func (c *ctxRes) Resolve(r interface{}, _ uint64) {
	c.r = r
	c.done()
}

type ctxBatch struct {
	active
	r   []interface{}
	o   []uint32
	cnt uint32
}

func (s *ctxBatch) Resolve(res interface{}, i uint64) {
	if atomic.CompareAndSwapUint32(&s.o[i], 0, 1) {
		s.r[i] = res
		if int(atomic.AddUint32(&s.cnt, 1)) == len(s.r) {
			s.done()
		}
	}
}

type SyncCtxIterator struct {
	ctx context.Context
	s   Scanner
}

type syncCtxScanRes struct {
	active
	keys []string
	err  error
}

func (r *syncCtxScanRes) Resolve(keys []string, err error) {
	r.keys = keys
	r.err = err
	r.done()
}

func (s SyncCtxIterator) Next() ([]string, error) {
	res := syncCtxScanRes{active: newActive(s.ctx)}
	select {
	case <-s.ctx.Done():
		return nil, NewErr(ErrKindRequest, ErrRequestCancelled)
	case <-res.ch:
		return res.keys, res.err
	}
}
