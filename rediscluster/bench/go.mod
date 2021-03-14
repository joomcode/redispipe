module github.com/joomcode/redispipe/rediscluster/bench

go 1.16

replace github.com/joomcode/redispipe => ../..

require (
	github.com/joomcode/redispipe v0.0.0-00010101000000-000000000000
	github.com/mediocregopher/radix/v3 v3.7.0
	github.com/wuxibin89/redis-go-cluster v1.0.1-0.20161207023922-222d81891f1d
)
