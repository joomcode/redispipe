package rediscluster

import (
	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
)

type Request = redis.Request
type Future = redis.Future

const NumSlots = redisclusterutil.NumSlots

func Slot(key string) uint16 { return redisclusterutil.Slot(key) }
