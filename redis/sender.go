package redis

import (
	"errors"
)

type Sender interface {
	Send(r Request, cb Future, n uint64)
	SendMany(r []Request, cb Future, n uint64)
	SendTransaction(r []Request, cb Future, start uint64)
	Scanner(opts ScanOpts) Scanner
	EachShard(func(Sender, error))
	Close()
}

type Scanner interface {
	Next(Future)
}

var ScanEOF = errors.New("Iteration finished")

// tools for scanning

type ScanOpts struct {
	Cmd   string
	Key   string
	Match string
	Count int
}

func (s ScanOpts) Request(it []byte) Request {
	if it == nil {
		it = []byte("0")
	}
	args := []interface{}{it}
	if s.Cmd == "" {
		s.Cmd = "SCAN"
	}
	if s.Cmd != "SCAN" {
		args = append(args, s.Key)
	}
	if s.Match != "" {
		args = append(args, "MATCH", s.Match)
	}
	if s.Count > 0 {
		args = append(args, "COUNT", s.Count)
	}
	return Request{s.Cmd, args}
}

type ScannerBase struct {
	ScanOpts
	Iter []byte
	Err  error
	cb   Future
}

func (s *ScannerBase) DoNext(cb Future, snd Sender) {
	s.cb = cb
	snd.Send(s.ScanOpts.Request(s.Iter), s, 0)
}

func (s *ScannerBase) IterLast() bool {
	return len(s.Iter) == 1 && s.Iter[0] == '0'
}

func (s *ScannerBase) Cancelled() bool {
	return s.cb.Cancelled()
}

func (s *ScannerBase) Resolve(res interface{}, _ uint64) {
	var keys []string
	s.Iter, keys, s.Err = ScanResponse(res)
	cb := s.cb
	s.cb = nil
	if s.Err != nil {
		cb.Resolve(s.Err, 0)
	} else {
		cb.Resolve(keys, 0)
	}
}
