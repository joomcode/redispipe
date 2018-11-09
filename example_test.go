package redispipe_test

import (
	"context"
	"fmt"
	"log"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/redisconn"
)

const databaseno = 0
const password = ""

var myhandle interface{} = nil

func Example_usage() {
	ctx := context.Background()
	cluster := false

	SingleRedis := func(ctx context.Context) (redis.Sender, error) {
		opts := redisconn.Opts{
			DB:       databaseno,
			Password: password,
			Logger:   redisconn.NoopLogger{}, // shut up logging. Could be your custom implementation.
			Handle:   myhandle,               // custom data, useful for custom logging
			// Other parameters (usually, no need to change)
			// IOTimeout, DialTimeout, ReconnectTimeout, TCPKeepAlive, Concurrency, WritePause, Async
		}
		conn, err := redisconn.Connect(ctx, "127.0.0.1:6379", opts)
		return conn, err
	}

	ClusterRedis := func(ctx context.Context) (redis.Sender, error) {
		opts := rediscluster.Opts{
			HostOpts: redisconn.Opts{
				// No DB
				Password: password,
				// Usually, no need for special logger
			},
			Name:   "mycluster",               // name of a cluster
			Logger: rediscluster.NoopLogger{}, // shut up logging. Could be your custom implementation.
			Handle: myhandle,                  // custom data, useful for custom logging
			// Other parameters (usually, no need to change):
			// ConnsPerHost, ConnHostPolicy, CheckInterval, MovedRetries, WaitToMigrate, RoundRobinSeed,
		}
		addresses := []string{"127.0.0.1:20001"} // one or more of cluster addresses
		cluster, err := rediscluster.NewCluster(ctx, addresses, opts)
		return cluster, err
	}

	var sender redis.Sender
	var err error
	if cluster {
		sender, err = ClusterRedis(ctx)
	} else {
		sender, err = SingleRedis(ctx)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer sender.Close()

	sync := redis.SyncCtx{sender} // wrapper for synchronous api

	res := sync.Do(ctx, "SET", "key", "ho")
	if err := redis.AsError(res); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("result: %q\n", res)

	res = sync.Do(ctx, "GET", "key")
	if err := redis.AsError(res); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("result: %q\n", res)

	res = sync.Send(ctx, redis.Req("HMSET", "hashkey", "field1", "val1", "field2", "val2"))
	if err := redis.AsError(res); err != nil {
		log.Fatal(err)
	}

	res = sync.Send(ctx, redis.Req("HMGET", "hashkey", "field1", "field2", "field3"))
	if err := redis.AsError(res); err != nil {
		log.Fatal(err)
	}
	for i, v := range res.([]interface{}) {
		fmt.Printf("%d: %T %q\n", i, v, v)
	}

	res = sync.Send(ctx, redis.Req("HMGET", "key", "field1"))
	if err := redis.AsError(res); err != nil {
		if rerr := redis.AsRedisError(res); rerr != nil && rerr.KindOf(redis.ErrResult) {
			fmt.Printf("expected error: %v\n", rerr)
		} else {
			fmt.Printf("unexpected error: %v\n", err)
		}
	} else {
		fmt.Printf("unexpected missed error\n")
	}

	results := sync.SendMany(ctx, []redis.Request{
		redis.Req("GET", "key"),
		redis.Req("HMGET", "hashkey", "field1", "field3"),
	})
	// results is []interface{}, each element is result for corresponding request
	for i, res := range results {
		fmt.Printf("result[%d]: %T %q\n", i, res, res)
	}

	results, err = sync.SendTransaction(ctx, []redis.Request{
		redis.Req("SET", "a{x}", "b"),
		redis.Req("SET", "b{x}", 0),
		redis.Req("INCRBY", "b{x}", 3),
	})
	if err != nil {
		log.Fatal(err)
	}
	for i, res := range results {
		fmt.Printf("tresult[%d]: %T %q\n", i, res, res)
	}

	// Output:
	// result: "OK"
	// result: "ho"
	// 0: []uint8 "val1"
	// 1: []uint8 "val2"
	// 2: <nil> %!q(<nil>)
	// expected error: WRONGTYPE Operation against a key holding the wrong kind of value (ErrResult {connection: *redisconn.Connection{addr: 127.0.0.1:6379}})
	// result[0]: []uint8 "ho"
	// result[1]: []interface {} ["val1" <nil>]
	// tresult[0]: string "OK"
	// tresult[1]: string "OK"
	// tresult[2]: int64 '\x03'
}
