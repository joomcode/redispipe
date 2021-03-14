REDIS_ARCHIVE ?= https://github.com/joomcode/redis/archive
REDIS_VERSION ?= debian-5.0.7-fixes

test: testcluster testconn testredis

/tmp/redis-server/redis-server:
	@echo "Building redis-$(REDIS_VERSION)..."
	wget -nv -c $(REDIS_ARCHIVE)/$(REDIS_VERSION).tar.gz -O - | tar -xzC .
	cd redis-$(REDIS_VERSION) && make -j 4 USE_JEMALLOC=no
	if [ ! -e /tmp/redis-server ] ; then mkdir /tmp/redis-server ; fi
	mv redis-$(REDIS_VERSION)/src/redis-server /tmp/redis-server
	rm redis-$(REDIS_VERSION) -rf

testredis: /tmp/redis-server/redis-server
	PATH=/tmp/redis-server/:${PATH} go test ./redis

testconn: /tmp/redis-server/redis-server
	killall redis-server || true
	rm ./redisconn/redis_test_* -r || true
	PATH=/tmp/redis-server/:${PATH} go test -count 1 ./redisconn

testcluster: /tmp/redis-server/redis-server
	killall redis-server || true
	rm ./rediscluster/redis_test_* -r || true
	PATH=/tmp/redis-server/:${PATH} go test -count 1 -tags debugredis ./rediscluster

bench: benchconn benchcluster

benchconn: /tmp/redis-server/redis-server
	PATH=/tmp/redis-server/:${PATH} ; cd ./redisconn/bench ; go test -count 1 -run FooBar -bench . -benchmem .

benchcluster: /tmp/redis-server/redis-server
	PATH=/tmp/redis-server/:${PATH} ; cd ./rediscluster/bench ; go test -count 1 -tags debugredis -run FooBar -bench . -benchmem .

clean:
	rm -r */redis_test_*
