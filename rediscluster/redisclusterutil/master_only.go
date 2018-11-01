package redisclusterutil

import (
	"strconv"

	"github.com/joomcode/redispipe/redis"
)

// MasterOnlyKey is a key of redis's SET which contains slots number.
// Cluster connector's main loop will switch these slots to MasterOnly mode on configuration refreshing.
// When slot migrates, slave redis instance behaves stupidly: they do not know about migration, and therefore
// they doesn't response with "ASKING" error for migrated key, and doesn't response to "ASKING" command on new shard.
// MasterOnlyKey is used within custom cluster migration utility to correctly migrate slot.
const MasterOnlyKey = "CLUSTER_SELF:MASTER_ONLY"

// RequestMasterOnly fetches content of key as a int set, and returns it as a map.
// If key is empty (""), then MasterOnlyKey is used.
func RequestMasterOnly(c redis.Sender, key string) (set map[uint16]struct{}, valid bool, err error) {
	if key == "" {
		key = MasterOnlyKey
	}
	resp := redis.Sync{c}.Do("SMEMBERS", key)
	return ParseMasterOnly(resp)
}

// ParseMasterOnly parses content of MasterOnlyKey.
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

// SetMasterOnly sets MasterOnlyKey to contain specified slots.
// It is used before slot migration.
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

// UnsetMasterOnly unsets slots from MasterOnlyKey.
// It is used after slot migration.
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
