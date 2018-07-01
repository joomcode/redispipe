package redisclusterutil

import (
	"strconv"

	"github.com/joomcode/redispipe/redis"
)

const MasterOnlyKey = "CLUSTER_SELF:MASTER_ONLY"

func RequestMasterOnly(c redis.Sender, key string) (set map[uint16]struct{}, valid bool, err error) {
	if key == "" {
		key = MasterOnlyKey
	}
	resp := redis.Sync{c}.Do("SMEMBERS", key)
	return ParseMasterOnly(resp)
}

func ParseMasterOnly(resp interface{}) (set map[uint16]struct{}, valid bool, err error) {
	if err := redis.AsError(resp); err != nil {
		return nil, false, err
	}
	if slots, ok := resp.([]interface{}); ok {
		valid = true
		if len(slots) > 0 {
			set = make(map[uint16]struct{})
		}
		for _, sl := range slots {
			if b, ok := sl.([]byte); ok {
				var slot int
				slot, err = strconv.Atoi(string(b))
				if err != nil {
					return nil, false, err
				}
				set[uint16(slot)] = struct{}{}
			}
		}
	} else if resp == nil {
		valid = true
	}
	return set, valid, nil
}

func SetMasterOnly(c redis.Sender, key string, slots []uint16) error {
	if key == "" {
		key = MasterOnlyKey
	}
	args := append(make([]interface{}, 0, len(slots)+1), key)
	for _, slot := range slots {
		args = append(args, slot)
	}
	resp := redis.Sync{c}.Do("SADD", args...)
	return redis.AsError(resp)
}

func UnsetMasterOnly(c redis.Sender, key string, slots []uint16) error {
	if key == "" {
		key = MasterOnlyKey
	}
	args := append(make([]interface{}, 0, len(slots)+1), key)
	for _, slot := range slots {
		args = append(args, slot)
	}
	resp := redis.Sync{c}.Do("SREM", args...)
	return redis.AsError(resp)
}
