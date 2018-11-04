package redisconn

import "github.com/joomcode/redispipe/redis"

var (
	// EKConnection - key for connection that handled request.
	EKConnection = redis.NewErrorKey("connection")
	// EKDb - db number to select.
	EKDb = redis.NewErrorKey("db")
)
