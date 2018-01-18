package rediswrap

import (
	"errors"

	"github.com/joomcode/redispipe/resp"
)

type Request = resp.Request

type Callback func(res interface{}, n uint64)

type Sender interface {
	Send(r Request, cb Callback, n uint64)
	SendTransaction(r []Request, cb Callback, start uint64)
	Scanner(opts ScanOpts) Scanner
}

type SendBatcher interface {
	SendBatch(r []Request, cb Callback, n uint64)
}

type Scanner interface {
	Next(func(keys []string, err error))
}

var ScanEOF = errors.New("Iteration finished")

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
