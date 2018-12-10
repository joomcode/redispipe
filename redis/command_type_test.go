package redis_test

import (
	"strings"
	"testing"

	"github.com/joomcode/redispipe/redis"

	"github.com/stretchr/testify/assert"
)

func TestCommandType(t *testing.T) {
	assert.True(t, redis.ReplicaSafe("GET"))
	assert.True(t, redis.ReplicaSafe("Get"))
	assert.True(t, redis.ReplicaSafe("get"))
	assert.False(t, redis.ReplicaSafe("SET"))
	assert.False(t, redis.ReplicaSafe("Set"))
	assert.False(t, redis.ReplicaSafe("set"))

	assert.True(t, redis.Blocking("BLPOP"))
	assert.True(t, redis.Blocking("Blpop"))
	assert.True(t, redis.Blocking("blpop"))
	assert.False(t, redis.Blocking("LPOP"))
	assert.False(t, redis.Blocking("Lpop"))
	assert.False(t, redis.Blocking("lpop"))

	assert.True(t, redis.Dangerous("SUBSCRIBE"))
	assert.True(t, redis.Dangerous("Subscribe"))
	assert.True(t, redis.Dangerous("subscribe"))
	assert.False(t, redis.Dangerous("PUBLISH"))
	assert.False(t, redis.Dangerous("Publish"))
	assert.False(t, redis.Dangerous("publish"))
}

var sum int

func BenchmarkCommandType(b *testing.B) {
	var cmds = strings.Split("PING ECHO DUMP MEMORY EXISTS GET GETRANGE RANDOMKEY KEYS TYPE TTL PTTL "+
		"BITCOUNT BITPOS GETBIT "+
		"GEOHASH GEOPOS GEODIST GEORADIUS_RO GEORADIUSBYMEMBER_RO "+
		"HEXISTS HGET HGETALL HKEYS HLEN HMGET HSTRLEN HVALS "+
		"LINDEX LLEN LRANGE "+
		"PFCOUNT "+
		"SCARD SDIFF SINTER SISMEMBER SMEMBERS SRANDMEMBER STRLEN SUNION "+
		"ZCARD ZCOUNT ZLEXCOUNT ZRANGE ZRANGEBYLEX ZREVRANGEBYLEX "+
		"ZRANGEBYSCORE ZRANK ZREVRANGE ZREVRANGEBYSCORE ZREVRANK ZSCORE "+
		"XPENDING XREVRANGE XREAD XLEN ", " ")[:3]

	for i := 0; i < b.N; i++ {
		for _, cmd := range cmds {
			if redis.ReplicaSafe(cmd) {
				sum++
			}
			if redis.Blocking(cmd) {
				sum++
			}
		}
	}
}
