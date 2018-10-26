package rediscluster

import (
	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
)

// Request is an alias for redis.Request
type Request = redis.Request

// Future is an alias for redis.Future
type Future = redis.Future

const NumSlots = redisclusterutil.NumSlots

// Slot is a "shortcut" for redisclusterutil.Slot
func Slot(key string) uint16 { return redisclusterutil.Slot(key) }
