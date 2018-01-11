package rediswrap

import "sync"

type Sync struct {
	S Sender
}

type syncRes struct {
	r Result
	sync.WaitGroup
}

func (s *syncRes) set(res interface{}, err error, _ uint64) {
	s.r.val, s.r.err = res, err
	s.Done()
}

func (s Sync) Send(r Request) Result {
	var res syncRes
	res.Add(1)
	s.S.Send(r, res.set, 0)
	res.Wait()
	return res.r
}

type syncBatch struct {
	r []Result
	sync.WaitGroup
}

func (s *syncBatch) set(res interface{}, err error, i uint64) {
	el := &s.r[i]
	el.val, el.err = res, err
	s.Done()
}

func (s Sync) SendBatch(r []Request) []Result {
	res := syncBatch{
		r: make([]Result, len(r)),
	}
	res.Add(len(r))
	s.S.SendBatch(r, res.set, 0)
	res.Wait()
	return res.r
}
