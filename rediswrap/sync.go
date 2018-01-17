package rediswrap

import (
	"sync"

	"github.com/joomcode/redispipe/resp"
)

type Sync struct {
	S Sender
}

func (s Sync) Send(r Request) interface{} {
	var res syncRes
	res.Add(1)
	s.S.Send(r, res.set, 0)
	res.Wait()
	return res.r
}

func (s Sync) SendMany(reqs []Request) []interface{} {
	res := syncBatch{
		r: make([]interface{}, len(reqs)),
	}
	res.Add(len(reqs))
	if batcher, ok := s.S.(SendBatcher); ok {
		batcher.SendBatch(reqs, res.set, 0)
	} else {
		cb := res.set
		for i, req := range reqs {
			s.S.Send(req, cb, uint64(i))
		}
	}
	res.Wait()
	return res.r
}

func (s Sync) SendTransaction(reqs []Request) []interface{} {
	var res syncRes
	res.Add(1)
	s.S.SendTransaction(reqs, res.set, 0)
	res.Wait()
	return resp.TransactionResponse(res.r, len(reqs))
}

type syncRes struct {
	r interface{}
	sync.WaitGroup
}

func (s *syncRes) set(res interface{}, _ uint64) {
	s.r = res
	s.Done()
}

type syncBatch struct {
	r []interface{}
	sync.WaitGroup
}

func (s *syncBatch) set(res interface{}, i uint64) {
	s.r[i] = res
	s.Done()
}
