package rediswrap

import "sync"

type Sync struct {
	S Sender
}

type syncRes struct {
	r interface{}
	sync.WaitGroup
}

func (s *syncRes) set(res interface{}, _ uint64) {
	s.r = res
	s.Done()
}

func (s Sync) Send(r Request) interface{} {
	var res syncRes
	res.Add(1)
	s.S.Send(r, res.set, 0)
	res.Wait()
	return res.r
}

type syncBatch struct {
	r []interface{}
	sync.WaitGroup
}

func (s *syncBatch) set(res interface{}, i uint64) {
	s.r[i] = res
	s.Done()
}

func (s Sync) SendBatch(r []Request) []interface{} {
	res := syncBatch{
		r: make([]interface{}, len(r)),
	}
	res.Add(len(r))
	s.S.SendBatch(r, res.set, 0)
	res.Wait()
	return res.r
}
