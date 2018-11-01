package redisclusterutil

import (
	"math/rand"

	"github.com/joomcode/redispipe/redis"
)

// ReqSlot returns slot number targeted by this command.
func ReqSlot(req redis.Request) (uint16, bool) {
	key, ok := req.Key()
	if key == "RANDOMKEY" && !ok {
		return uint16(rand.Intn(NumSlots)), true
	}
	return Slot(key), ok
}

// BatchSlot returns slot common for all requests in batch (if there is such common slot).
func BatchSlot(reqs []redis.Request) (uint16, bool) {
	var slot uint16
	var set bool
	for _, req := range reqs {
		s, ok := ReqSlot(req)
		if !ok {
			continue
		}
		if !set {
			slot = s
			set = true
		} else if slot != s {
			return 0, false
		}
	}
	return slot, set
}

// BatchKey returns first key from a batch that is targeted to common slot.
func BatchKey(reqs []redis.Request) (string, bool) {
	var key string
	var slot uint16
	var set bool
	for _, req := range reqs {
		k, ok := req.Key()
		if !ok {
			continue
		}
		s := Slot(k)
		if !set {
			key, slot = k, s
			set = true
		} else if slot != s {
			return "", false
		}
	}
	return key, set
}
