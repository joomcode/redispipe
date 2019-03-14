REDIS_ARCHIVE ?= https://github.com/joomcode/redis/archive/
REDIS_VERSION ?= 5.0.3-fixes

test: testcluster testconn testredis

redis-server/redis-server:
	@echo "Building redis-$(REDIS_VERSION)..."
	test ! -e redis-server && wget -nv -c $(REDIS_ARCHIVE)/$(REDIS_VERSION).tar.gz -O - | tar -xzC .
	cd redis-$(REDIS_VERSION) && make -j 4
	mkdir redis-server
	mv redis-$(REDIS_VERSION)/src/redis-server redis-server
	rm redis-$(REDIS_VERSION) -rf

testredis:
	go test ./redis

testconn: redis-server/redis-server
	killall redis-server || true
	rm ./redisconn/redis_test_* -r || true
	PATH=`realpath .`/redis-server/:${PATH} go test -count 1 ./redisconn

testcluster: redis-server/redis-server
	killall redis-server || true
	rm ./rediscluster/redis_test_* -r || true
	PATH=`realpath .`/redis-server/:${PATH} go test -count 1 -tags debugredis ./rediscluster

bench: benchconn benchcluster

benchconn:
	go test -count 1 -run FooBar -bench . -benchmem ./redisconn

benchcluster:
	go test -count 1 -tags debugredis -run FooBar -bench . -benchmem ./rediscluster
