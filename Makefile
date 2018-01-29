test: testcluster testconn testredis

testredis:
	go test ./redis

testconn:
	go test ./redisconn

testcluster:
	go test -tags debugredis ./rediscluster

