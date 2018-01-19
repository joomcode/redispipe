package redis

import (
	"sync"
)

type Sync struct {
	S Sender
}

func (s Sync) Do(cmd string, args ...interface{}) interface{} {
	return s.Send(Request{cmd, args})
}

func (s Sync) Send(r Request) interface{} {
	var res syncRes
	res.Add(1)
	s.S.Send(r, &res, 0)
	res.Wait()
	return res.r
}

func (s Sync) SendMany(reqs []Request) []interface{} {
	res := syncBatch{
		r: make([]interface{}, len(reqs)),
	}
	res.Add(len(reqs))
	s.S.SendMany(reqs, &res, 0)
	res.Wait()
	return res.r
}

func (s Sync) SendTransaction(reqs []Request) ([]interface{}, error) {
	var res syncRes
	res.Add(1)
	s.S.SendTransaction(reqs, &res, 0)
	res.Wait()
	return TransactionResponse(res.r)
}

func (s Sync) Scanner(opts ScanOpts) SyncIterator {
	return SyncIterator{s.S.Scanner(opts)}
}

type syncRes struct {
	r interface{}
	sync.WaitGroup
}

func (s *syncRes) Cancelled() bool {
	return false
}

func (s *syncRes) Resolve(res interface{}, _ uint64) {
	s.r = res
	s.Done()
}

type syncBatch struct {
	r []interface{}
	sync.WaitGroup
}

func (s *syncBatch) Cancelled() bool {
	return false
}

func (s *syncBatch) Resolve(res interface{}, i uint64) {
	s.r[i] = res
	s.Done()
}

type SyncIterator struct {
	s Scanner
}

func (s SyncIterator) Next() ([]string, error) {
	var res syncRes
	res.Add(1)
	s.s.Next(&res)
	res.Wait()
	if err := AsError(res.r); err != nil {
		return nil, err
	} else if res.r == nil {
		return nil, ScanEOF
	} else {
		return res.r.([]string), nil
	}
}
