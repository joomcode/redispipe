package rediscluster

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/joomcode/redispipe/rediswrap"
	"github.com/joomcode/redispipe/resp"
)

type MasterOnly struct {
	*Cluster
}

var _ rediswrap.Sender = MasterOnly{}

type MasterAndReplica struct {
	*Cluster
}

var _ rediswrap.Sender = MasterAndReplica{}

func reqSlot(req Request) (uint16, bool) {
	n := 0
	if req.Cmd == "EVAL" || req.Cmd == "EVALSHA" {
		n = 1
	}
	if len(req.Args) <= n {
		return 0, false
	}
	key, ok := resp.ArgToString(req.Args[1])
	return Slot(key), ok
}

func batchSlot(reqs []Request) (uint16, bool) {
	var slot uint16
	var set bool
	for _, req := range reqs {
		s, ok := reqSlot(req)
		if !ok {
			continue
		}
		if !set {
			slot = s
		} else if slot != s {
			return 0, false
		}
	}
	return slot, set
}

func (m MasterOnly) Send(req Request, cb Callback, off uint64) {
	slot, ok := reqSlot(req)
	if !ok {
		go cb(nil, m.err(ErrNoSlotKey).
			WithMsg(fmt.Sprintf("no slot key in request %+v", req)), off)
		return
	}

	shardn, addrs := m.shardForSlot(slot)
	callback := func(res interface{}, err error, off uint64) {
		if err != nil {
			cb(res, err, off)
			return
		}
		if err, ok := res.(resp.ResponseError); ok {
			moved := strings.HasPrefix(err.Msg, "MOVED ")
			ask := strings.HasPrefix(err.Msg, "ASK ")
			if !moved && !ask {
				cb(res, err, off)
				return
			}
			p := strings.Split(err.Msg, " ")
			slotn, erra := strconv.Atoi(p[1])
			if erra != nil || slotn != slot {
				cb(nil, c.err(ErrWrongRedirection).WithMsg(err.Msg))
				return
			}
		}
	}
	if m.SendToAddress(addrs[0], req, callback, off) {
		return
	}
}

func (m MasterOnly) SendBatch(reqs []Request, cb Callback, off uint64) {
}

func (m MasterAndReplica) Send(req Request, cb Callback, n uint64) {
}

func (m MasterAndReplica) SendBatch(reqs []Request, cb Callback, off uint64) {
}
