test: testcluster testconn testredis

testredis:
	go test ./redis

testconn:
	killall redis-server || true
	rm ./rediscluster/redis_test_* || true
	go test -count 1 ./redisconn

testcluster:
	killall redis-server || true
	rm ./rediscluster/redis_test_* || true
	go test -count 1 -tags debugredis ./rediscluster

bench: benchconn benchcluster

benchconn:
	go test -count 1 -run FooBar -bench . -benchmem ./redisconn

benchcluster:
	go test -count 1 -tags debugredis -run FooBar -bench . -benchmem ./rediscluster
