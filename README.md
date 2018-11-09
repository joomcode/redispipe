# redispipe

redispipe - is client for redis that uses "implicit pipelining" for highest performance
of "caching" usage scenarios.

https://redis.io/topics/pipelining

Pipelining improves maximum throughput that redis can serve, and reduces CPU usage both on
redis server and on client. That is why pipelining were considered as a critical feature
for our infrastructure.

But it is not always possible to use pipelining explicitly: not always you have bunch of
commands you ready to send. Usually there are dozen of concurrent goroutines, each sends just
one request at a time. To handle usual workload, pipelining have to be implicit. 

All known Golang redis connectors use connection-per-request working model with pool of
connection, and provide only explicit pipelining. It worked far from optimally under our load.

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

But there were no such connector for Golang.

This connector were created as implicitly pipelined from ground to achieve maximum performance
in a highly concurrent environment. It writes all requests to single connection to redis, and
continuously reads answers in other goroutine.

## Performance

### Single redis

```
go test -count 1 -run FooBar -bench . -benchmem -benchtime 5s ./redisconn
goos: linux
goarch: amd64
pkg: github.com/joomcode/redispipe/redisconn
BenchmarkSerialGetSet/radixv2-8             200000    33950 ns/op    256 B/op    11 allocs/op
BenchmarkSerialGetSet/redigo-8              200000    33143 ns/op     86 B/op     5 allocs/op
BenchmarkSerialGetSet/redispipe-8            30000   222391 ns/op    169 B/op     8 allocs/op
BenchmarkSerialGetSet/redispipe_pause0-8    200000    46704 ns/op    168 B/op     8 allocs/op
BenchmarkParallelGetSet/radixv2-8          1000000    10549 ns/op    254 B/op    11 allocs/op
BenchmarkParallelGetSet/redigo-8           1000000    10465 ns/op    119 B/op     6 allocs/op
BenchmarkParallelGetSet/redispipe-8        3000000     2788 ns/op    168 B/op     8 allocs/op
```

You can see couple of things:
- first, redispipe has highest performance in Parallel benchmarks both for single connection
and for cluster,
- second, redispipe has lower performance for single-threaded-single-redis case.

That is true: redispipe trades latency for throughput. Every single request has additional
latency for hidden batching in a connector. But thanks to batching, more requests can be sent
to redis and answered by redis in an interval of time.

`SerialGetSet/redispipe_pause0` shows single-threaded results with disabled "batching".
This way redispipe is quite close to other connectors in performance, though there is still
small overhead of internal design. But I would not recommend disable batching (unless your use case
is single threaded), because it increases CPU usage under high concurrent load both on client
and on redisd.

Parallel benchmark for single redis has Redis CPU usage as a bottleneck for both `radix.v2` and
`redigo` (ie Redis eats whole CPU core). But with redispipe Redis consumes only 75% of a core
despite it could serve 3 times more requests. It clearly shows how usage of implicitly
pipelined connector helps to get much more RPS from single Redis server.

### Cluster

```
go test -count 1 -tags=debugredis -run FooBar -bench . -benchmem -benchtime 5s ./rediscluster
goos: linux
goarch: amd64
pkg: github.com/joomcode/redispipe/rediscluster
BenchmarkSerialGetSet/radixv2-8           100000    56445 ns/op   1008 B/op   31 allocs/op
BenchmarkSerialGetSet/redigo-8            200000    43533 ns/op    246 B/op   12 allocs/op
BenchmarkSerialGetSet/redispipe-8          30000   305684 ns/op    220 B/op   12 allocs/op
BenchmarkSerialGetSet/redispipe_pause0-8  100000    59486 ns/op    217 B/op   12 allocs/op
BenchmarkParallelGetSet/radixv2-8        1000000     7643 ns/op   1010 B/op   31 allocs/op
BenchmarkParallelGetSet/redigo-8         1000000     5616 ns/op    248 B/op   13 allocs/op
BenchmarkParallelGetSet/redispipe-8      2000000     3818 ns/op    217 B/op   12 allocs/op
```


With cluster configuration, internal cluster meta-info management adds additional overhead
inside of Go process. And redispipe/rediscluster attempts to provide almost lockless cluster's
info handling on the way of request execution.

While `redigo` is almost as fast in Parallel test, in fact it also were limited by Redis's CPU
usage (three redis processes eats whole 3 cpu cores), and it uses huge number of connections.
I spent some noticable time to recognize both KeepAlive and AliveTime should be set (and
KeepAlive should be set as high as 128) to make `redis-go-cluster` (cluster wrapper for `redigo`)
so fast.

Each Redis uses less than 60% CPU core when `redispipe` is used, despite serving more requests.
Go process is also uses less CPU with `redispipe` than with `redigo`. And no hidden knowledge
for such good results.

( github.com/chasex/redis-go-cluster  is used for cluster test of redigo ).

### Practice

In practice, performance gain is lesser, because your application do other useful work aside
of sending requests to Redis. But gain is still noticeable. At our setup, we have around 10-15%
lesser CPU usage on Redis (ie 50%CPU->35%CPU), and 5-10% improvement on client side. 

## Capabilities

- fast
- thread-safe: no need to lock around connection, no need to "return to pool", etc
- Pipelining is implicit,
- transactions supported (but without `WATCH`)
- hook for request timing reporting.

## Limitations

- while it allows you to send blocking calls, thou shalt not, because it will block whole pipeline:
  `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BZPOPMIN`, `BZPOPMAX`, `XREAD`, `XREADGROUP`, `SAVE` - you'd better
  not call this commands.
- `WATCH` command is useless and harmful, because arbitrary commands from concurrent goroutines
  could be injected between `WATCH` and `MULTI`.
- `PUB/SUB` is not supported. `PUB/SUB` switches connection work mode to completely different,
  therefore it could not be combined with regular commands, and should spawn new connection
  instead.
  
## Installation

- Single connection: `go get github.com/joomcode/redispipe/redisconn`
- Cluster connection: `go get github.com/joomcode/redispipe/rediscluster`
- Common package, installed as dependency - 'github.com/joomcode/redispipe/redis'

## Usage

Both `redisconn.Connect` and `rediscluster.NewCluster` creates implementations of `redis.Sender`.
`redis.Sender` provides asynchronous api for sending request/requests/transactions. That api
accepts `redis.Future` as an argument and fullfills it asynchronously.

To use convenient synchronous api, one should wrap "sender" with one of wrappers:
- `redis.Sync{sender}` - provides simple synchronouse api
- `redis.SyncCtx{sender}` - provides same api, but all methods accepts `context.Context`, and
  methods returns immediately if that context is closed.
- `redis.ChanFutured{sender}` - provides api with future through channel closing.

Types accepted as command arguments: `nil`, `[]byte`, `string`, `int` (and all other integer types),
`float64`, `float32`, `bool`. All arguments are converted to redis bulk strings as usual (ie
string and bytes - as id; numbers - in decimal notation). `bool` converted as "0/1",
`nil` converted to empty string.

In difference to other redis packages, no custom types are used for request results. Results
are de-serialized into plain go types and are returned as `interface{}`:

redis        | go
-------------|-------
plain string | `string`
bulk string  | `[]byte`
integer      | `int`
array        | `[]interface{}`
error        | `error` (`*redis.Error`)

IO, connection, and other errors are not returned separately, but as result (and has same
`*redis.Error` underlying type).

```go
package foo

import "log"
import "context"

import "github.com/joomcode/redispipe/redis"
import "github.com/joomcode/redispipe/redisconn"
import "github.com/joomcode/redispipe/rediscluster"

func SingleRedis(ctx context.Context) (redis.Sender, error) {
    opts := redisconn.Opts{
    	DB: databaseno,
    	Password: password,
    	Logger: redisconn.NoopLogger{}, // shut up logging. Could be your custom implementation.
    	Handle: myhandle, // custom data, useful for custom logging
    	/*
    	Other parameters (usually, no need to change)
    	IOTimeout, DialTimeout, ReconnectTimeout, TCPKeepAlive, Concurrency, WritePause, Async
    	*/
    }
    conn, err := redisconn.Connect(ctx, "127.0.0.1:6379", opts)
    return conn, err
} 

func ClusterRedis(ctx context.Context) (redis.Sender, error) {
	opts := rediscluster.Opts{
		HostOpts: redisconn.Opts{
			// No DB
			Password: parssword,
			// Usually, no need for special logger
		},
		Name: "mycluster", // name of a cluster
		Logger: rediscluster.NoopLogger{}, // shut up logging. Could be your custom implementation.
		Handle: myhandle, // custom data, useful for custom logging
		/*
		Other parameters (usually, no need to change):
		ConnsPerHost, ConnHostPolicy, CheckInterval, MovedRetries, WaitToMigrate, RoundRobinSeed,
		*/
	}
	addresses := []string{"127.0.0.1:20001"} // one or more of cluster addresses
	cluster, err := rediscluster.NewCluster(ctx, addresses, opts)
	return cluster, err
}

func Usage(ctx context.Context, cluster bool) {
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
	
	res := sync.Do(ctx, "GET", "key")
	if err := redis.AsError(res); err != nil {
		log.Fatal(err)
	}
	if res != nil {
        log.Printf("result: %q", res.([]byte)) // 
	} else {
		log.Printf("not found")
	}
	
	res = sync.Send(ctx, redis.Req("HMGET", "hashkey", "field1", "field2"))
	if err := redis.AsError(res); err != nil {
		log.Fatal(err)
	}
	for i, v := range res.([]interface{}) {
		if v != nil {
            log.Printf("%d: %q", i, v.([]byte))
		} else {
            log.Printf("%d: not found", i)
		}
	}
	
	results := sync.SendMany(ctx, []redis.Request{
		redis.Req("GET", "key"),
		redis.Req("HMGET", "hashkey", "field1", "field2"),
	})
	// results is []interface{}, each element is result for corresponding request
	if err := redis.AsError(results[0]); err != nil {
		log.Fatal(err)
	}
	if res != nil {
        log.Printf("result[0]: %q", res.([]byte)) // 
	} else {
		log.Printf("result[0]: not found")
	}
	
	if err := redis.AsError(results[1]); err != nil {
		log.Fatal(err)
	}
	for i, v := range results[1].([]interface{}) {
		if v != nil {
            log.Printf("results[1]: %d: %q", i, v.([]byte))
		} else {
            log.Printf("results[1]: %d: not found", i)
		}
	}
	
	results, err = sync.SendTransaction(ctx, []redis.Request{
		redis.Req("SET", "a{x}", "b"),
		redis.Req("INCR", "b{x}", 1),
	})
	if err != nil {
		log.Fatal(err)
	}
	// results[0] - results of SET
	// results[1] - results of INCR
}
```

## Contributing

You're welcome!!!

## License

[MIT License](https://github.com/joomcode/redispipe/blob/master/LICENSE)
