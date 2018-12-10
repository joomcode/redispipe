/*
Package redispipe - high throughput Redis connector with implicit pipelining.

https://redis.io/topics/pipelining

Pipelining improves maximum throughput that redis can serve, and reduces CPU usage both on
redis server and on client. Mostly it comes from saving system CPU consumption.

But it is not always possible to use pipelining explicitly: usually there are dozen of
concurrent goroutines, each sends just one request at a time. To handle usual workload,
pipelining have to be implicit.

All known Golang redis connectors use connection-per-request working model with connection pool,
and provide only explicit pipelining. It worked far from optimal under highly concurrent load.

This connector was created as implicitly pipelined from ground to achieve maximum performance
in a highly concurrent environment. It writes all requests to single connection to redis, and
continuously reads answers from in other goroutine.

Note that it trades a bit of latency for throughput, and therefore could be not optimal for
non-concurrent usage.

Capabilities

- fast,

- thread-safe: no need to lock around connection, no need to "return to pool", etc,

- pipelining is implicit,

- transactions supported (but without WATCH),

- hook for custom logging,

- hook for request timing reporting.

Limitations

- by default, it is not allowed to send blocking calls, because it will block whole pipeline:
`BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BZPOPMIN`, `BZPOPMAX`, `XREAD`, `XREADGROUP`, `SAVE`.
However, you could set `ScriptMode: true` option to enable this commands.
`ScriptMode: true` also turns default `WritePause` to -1 (ie almost disables forced batching).

- `WATCH` is also forbidden by default: it is useless and harmful when concurrent goroutines
uses same connection.
It is also allowed with `ScriptMode: true`, but you should be sure you use connection only
from single goroutine.

- `SUBSCRIBE` and `PSUBSCRIBE` commands are forbidden. They switches connection work mode to
completely different mode of communication, therefore it could not be combined with regular
commands. This connector doesn't implement subscribing mode.

Structure

- root package is empty

- common functionality is in redis subpackage

- singe connection is in redisconn subpackage

- cluster support is in rediscluster subpackage

Usage

Both redisconn.Connect and rediscluster.NewCluster creates implementations of redis.Sender.
redis.Sender provides asynchronous api for sending request/requests/transactions. That api
accepts redis.Future interface implementations as an argument and fullfills it asynchronously.
Usually you don't need to provide your own redis.Future implementation, but rather use
synchronous wrappers.

To use convenient synchronous api, one should wrap "sender" with one of wrappers:

- redis.Sync{sender} - provides simple synchronouse api,

- redis.SyncCtx{sender} - provides same api, but all methods accepts context.Context, and
methods returns immediately if that context is closed,

- redis.ChanFutured{sender} - provides api with future through channel closing.

Types accepted as command arguments: nil, []byte, string, int (and all other integer types),
float64, float32, bool. All arguments are converted to redis bulk strings as usual (ie
string and bytes - as is; numbers - in decimal notation). bool converted as "0/1",
nil converted to empty string.

In difference to other redis packages, no custom types are used for request results. Results
are de-serialized into plain go types and are returned as interface{}:

  redis        | go
  -------------|-------
  plain string | string
  bulk string  | []byte
  integer      | int64
  array        | []interface{}
  error        | error (*errorx.Error)

IO, connection, and other errors are not returned separately but as result (and has same
*errorx.Error underlying type).
*/
package redispipe
