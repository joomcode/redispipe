test: testcluster testconn testredis

testredis:
	go test ./redis

testconn:
	go test -count 1 ./redisconn

testcluster:
	go test -count 1 -tags debugredis ./rediscluster

