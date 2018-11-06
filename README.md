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

```
go test -count 1 -run FooBar -bench . -benchmem -benchtime 5s ./redisconn
goos: linux
goarch: amd64
pkg: github.com/joomcode/redispipe/redisconn
BenchmarkSerialGetSet/radixv2-8                   200000             33950 ns/op             256 B/op         11 allocs/op
BenchmarkSerialGetSet/redigo-8                    200000             33143 ns/op              86 B/op          5 allocs/op
BenchmarkSerialGetSet/redispipe-8                  30000            222391 ns/op             169 B/op          8 allocs/op
BenchmarkSerialGetSet/redispipe_pause0-8                  200000             46704 ns/op             168 B/op          8 allocs/op
BenchmarkParallelGetSet/radixv2-8                        1000000             10549 ns/op             254 B/op         11 allocs/op
BenchmarkParallelGetSet/redigo-8                         1000000             10465 ns/op             119 B/op          6 allocs/op
BenchmarkParallelGetSet/redispipe-8                      3000000              2788 ns/op             168 B/op          8 allocs/op

go test -count 1 -tags=debugredis -run FooBar -bench . -benchmem -benchtime 5s ./rediscluster
goos: linux
goarch: amd64
pkg: github.com/joomcode/redispipe/rediscluster
BenchmarkSerialGetSet/radixv2-8                   200000             55940 ns/op            1007 B/op         31 allocs/op
BenchmarkSerialGetSet/redigo-8                     50000           1403153 ns/op           19418 B/op         63 allocs/op
BenchmarkSerialGetSet/redispipe-8                  30000            306860 ns/op             238 B/op         12 allocs/op
BenchmarkSerialGetSet/redispipe_pause0-8                  100000             59531 ns/op             222 B/op         12 allocs/op
BenchmarkParallelGetSet/radixv2-8                         100000             55503 ns/op            1763 B/op         34 allocs/op
BenchmarkParallelGetSet/redigo-8                           10000            714388 ns/op           19489 B/op         63 allocs/op
BenchmarkParallelGetSet/redispipe-8                      2000000              3815 ns/op             217 B/op         12 allocs/op
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

Parallel benchmark for single redis has Redis CPU usage as a bottleneck for both radix.v2 and
redigo. But with redispipe Redis consumes only 75% of a core despite it could serve 3 times more
requests. It clearly shows how usage of implicitly pipelined connector helps to get much more RPS
from single Redis server.

With cluster configuration, internal cluster meta-info management adds additional overhead inside
of Go process. And redispipe/rediscluster attempts to provide almost lockless cluster's info
handling on the way of request execution.

(Note: for redigo I used github.com/chasex/redis-go-cluster , and I don't know why it behave so
poor.)