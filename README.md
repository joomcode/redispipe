# RedisPipe

RedisPipe – is a client for redis that uses "implicit pipelining" for highest performance.

[![Build Status](https://travis-ci.com/joomcode/redispipe.svg?branch=master)](https://travis-ci.com/joomcode/redispipe)
[![GoDoc](https://godoc.org/github.com/joomcode/redispipe?status.svg)](https://godoc.org/github.com/joomcode/redispipe)
[![Report Card](https://goreportcard.com/badge/github.com/joomcode/redispipe)](https://goreportcard.com/report/github.com/joomcode/redispipe)

- [Highlights](#highlights)
- [Introduction](#introduction)
- [Performance](#performance)
- [Limitations](#limitations)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Highlights
- scalable: the more throughput you try to get, the more efficient it is.
- cares about redis: redis needs less CPU to perform same throughput.
- thread-safe: no need to lock around connection, no need to "return to pool", etc.
- pipelining is implicit.
- transactions are supported (but without `WATCH`).
- hook for custom logging.
- hook for request timing reporting.

## Introduction

https://redis.io/topics/pipelining

Pipelining improves the maximum throughput that redis can serve, and reduces CPU usage both on
redis server and on the client side. Mostly it comes from saving system CPU consumption.

But it is not always possible to use pipelining explicitly: usually there are dozens of
concurrent goroutines, each sends just one request at a time. To handle the usual workload,
pipelining has to be implicit.

"Implicit pipelining" is used in many drivers for other languages:
- https://github.com/NodeRedis/node_redis , https://github.com/h0x91b/redis-fast-driver ,
  and probably, other nodejs clients,
- https://github.com/andrew-bn/RedisBoost - C# connector,
- some C/C++ clients,
- all Dart clients ,
- some Erlang and Elixir clients,
- https://github.com/informatikr/hedis - Haskel client.
- http://aredis.sourceforge.net/ - Java client explicitly made for transparent pipelining,
- https://github.com/lettuce-io/lettuce-core - Java client capable for transparent pipelining,
- https://github.com/aio-libs/aioredis - Python's async connector, and some of other async
  python clients
- Ruby's EventMachine related connectors,
- etc

At the moment this connector were created there was no such connector for Golang.
All known Golang redis connectors use a connection-per-request model with a connection pool,
and provide only explicit pipelining.

This connector was created as implicitly pipelined from the ground up to achieve maximum performance
in a highly concurrent environment. It writes all requests to single connection to redis, and
continuously reads answers from another goroutine.

Note that it trades a bit of latency for throughput, and therefore could be not optimal for
low-concurrent low-request-per-second usage. Write loop latency is configurable as `WritePause`
parameter in connection options, and could be disabled at all, or increased to higher values
(150µs is the value used in production, 50µs is default value, -1 disables write pause). Implicit
runtime latency for switching goroutines still remains, however, and could not be removed.

## Performance

### Single redis

```
goos: linux
goarch: amd64
pkg: github.com/joomcode/redispipe/rediscluster
cpu: Intel(R) Core(TM) i7-10750H CPU @ 2.60GHz
BenchmarkSerialGetSet/radix_pause0-12              17691             63132 ns/op              68 B/op          4 allocs/op
BenchmarkSerialGetSet/redigo-12            19519             60064 ns/op             239 B/op         13 allocs/op
BenchmarkSerialGetSet/redispipe-12           504           2661790 ns/op             290 B/op         12 allocs/op
BenchmarkSerialGetSet/redispipe_pause0-12                  13669             84925 ns/op             208 B/op         12 allocs/op
BenchmarkParallelGetSet/radix-12                          621036              1817 ns/op              78 B/op          4 allocs/op
BenchmarkParallelGetSet/redigo-12                           7466            153584 ns/op            4008 B/op         20 allocs/op
BenchmarkParallelGetSet/redispipe-12                      665428              1599 ns/op             231 B/op         12 allocs/op
```

You can see a couple of things:
- first, redispipe has highest performance in Parallel benchmarks,
- second, redispipe has lower performance for single-threaded cases.

That is true: redispipe trades latency for throughput. Every single request has additional
latency for hidden batching in a connector. But thanks to batching, more requests can be sent
to redis and answered by redis in an interval of time.

`SerialGetSet/redispipe_pause0` shows single-threaded results with disabled additional latency
for "batching" (`WritePause: -1`). This way redispipe is quite close to other connectors in
performance, though there is still small overhead of internal design. But I would not recommend
disable batching (unless your use case is single threaded), because it increases CPU usage under
highly concurrent load both on client and on redis-server.

To be honestly, github.com/mediocregopher/radix/v3 is also able to perform implicit pipelining
and does it by default. Therefore it is almost as fast as redispipe in ParallelGetSet.
SerialGetSet is tested with disabled pipelining, because otherwise it will be as slow as
redispipe without pause0.

### Cluster

```
go test -count 1 -tags=debugredis -run FooBar -bench . -benchmem -benchtime 5s ./rediscluster
goos: linux
goarch: amd64
pkg: github.com/joomcode/redispipe/rediscluster
BenchmarkSerialGetSet/radixv2-8           200000    53585 ns/op   1007 B/op   31 allocs/op
BenchmarkSerialGetSet/redigo-8            200000    40705 ns/op    246 B/op   12 allocs/op
BenchmarkSerialGetSet/redispipe-8          30000   279838 ns/op    220 B/op   12 allocs/op
BenchmarkSerialGetSet/redispipe_pause0-8  200000    56356 ns/op    216 B/op   12 allocs/op
BenchmarkParallelGetSet/radixv2-8        1000000     9245 ns/op   1268 B/op   32 allocs/op
BenchmarkParallelGetSet/redigo-8         1000000     6886 ns/op    399 B/op   13 allocs/op
BenchmarkParallelGetSet/redispipe-8      5000000     1636 ns/op    219 B/op   12 allocs/op
```

With cluster configuration, internal cluster meta-info management adds additional overhead
inside of the Go process. And redispipe/rediscluster attempts to provide almost lockless cluster
info handling on the way of request execution.

While `redigo` is almost as fast in Parallel tests, it also happens to be limited by Redis's CPU
usage (three redis processes eats whole 3 cpu cores). It uses a huge number of connections,
and it is not trivial to recognize non-default setting that should be set to achieve this result
(both KeepAlive and AliveTime should be set as high as 128).
( [github.com/chasex/redis-go-cluster](https://github.com/chasex/redis-go-cluster) is used).

Each Redis uses less than 60% CPU core when `redispipe` is used, despite serving more requests.

### Practice

In practice, performance gain is lesser, because your application does other useful work aside
from sending requests to Redis. But gain is still noticeable. At our setup, we have around 10-15%
less CPU usage on Redis (ie 50%CPU->35%CPU), and 5-10% improvement on the client side.
`WritePause` is usually set to higher value (150µs) than default.

## Limitations

- by default, it is not allowed to send blocking calls, because it will block the whole pipeline:
  `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BZPOPMIN`, `BZPOPMAX`, `XREAD`, `XREADGROUP`, `SAVE`.
  However, you could set `ScriptMode: true` option to enable these commands.
  `ScriptMode: true` also turns default `WritePause` to -1 (meaning it practically disables forced
  batching).
- `WATCH` is also forbidden by default: it is useless and even harmful when concurrent goroutines
  use the same connection.
  It is also allowed with `ScriptMode: true`, but you should be sure you use connection only
  from a single goroutine.
- `SUBSCRIBE` and `PSUBSCRIBE` commands are forbidden. They switch connection work mode to a
  completely different mode of communication, therefore it could not be combined with regular
  commands. This connector doesn't implement subscribing mode.

## Installation

- Single connection: `go get github.com/joomcode/redispipe/redisconn`
- Cluster connection: `go get github.com/joomcode/redispipe/rediscluster`

## Usage

Both `redisconn.Connect` and `rediscluster.NewCluster` creates implementations of `redis.Sender`.
`redis.Sender` provides asynchronous api for sending request/requests/transactions. That api
accepts `redis.Future` interface implementations as an argument and fullfills it asynchronously.
Usually you don't need to provide your own `redis.Future` implementation, but rather use
synchronous wrappers.

To use convenient synchronous api, one should wrap "sender" with one of wrappers:
- `redis.Sync{sender}` - provides simple synchronouse api
- `redis.SyncCtx{sender}` - provides same api, but all methods accepts `context.Context`, and
  methods returns immediately if that context is closed.
- `redis.ChanFutured{sender}` - provides api with future through channel closing.

Types accepted as command arguments: `nil`, `[]byte`, `string`, `int` (and all other integer types),
`float64`, `float32`, `bool`. All arguments are converted to redis bulk strings as usual (ie
string and bytes - as is; numbers - in decimal notation). `bool` converted as "0/1",
`nil` converted to empty string.

In difference to other redis packages, no custom types are used for request results. Results
are de-serialized into plain go types and are returned as `interface{}`:

redis        | go
-------------|-------
plain string | `string`
bulk string  | `[]byte`
integer      | `int64`
array        | `[]interface{}`
error        | `error` (`*errorx.Error`)

IO, connection, and other errors are not returned separately, but as result (and has same
`*errorx.Error` underlying type).

```go
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
		if rerr := redis.AsErrorx(res); rerr != nil && rerr.IsOfType(redis.ErrResult) {
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
```

## Contributing

- Ask questions in [Issues](https://github.com/joomcode/redispipe/issues)
- Ask questions on [StackOverflow](https://stackoverflow.com/questions/ask?tags=go+redis).
- Report about bugs using github [Issues](https://github.com/joomcode/redispipe/issues),
- Request new features or report about intentions to implement feature using github
[Issues](https://github.com/joomcode/redispipe/issues),
- Send [pull requests](https://github.com/joomcode/redispipe/pulls) to fix reported bugs or
to implement discussed features.
- Be kind.
- Be lenient to our misunderstanding of your problem and our unwillingness to bloat library.

## License

[MIT License](https://github.com/joomcode/redispipe/blob/master/LICENSE)
