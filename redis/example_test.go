package redis_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/joomcode/redispipe/testbed"

	"github.com/joomcode/redispipe/redisconn"

	"github.com/joomcode/redispipe/redis"
)

func ExampleAppendRequest() {
	req, err := redis.AppendRequest(nil, redis.Req("GET", "one"))
	fmt.Printf("%q\n%v\n", req, err)
	req, err = redis.AppendRequest(req, redis.Req("INCRBY", "cnt", 5))
	fmt.Printf("%q\n%v\n", req, err)
	req, err = redis.AppendRequest(req, redis.Req("SENDFOO", time.Second))
	fmt.Printf("%q\n%v\n", req, err)

	// Output:
	// "*2\r\n$3\r\nGET\r\n$3\r\none\r\n"
	// <nil>
	// "*2\r\n$3\r\nGET\r\n$3\r\none\r\n*3\r\n$6\r\nINCRBY\r\n$3\r\ncnt\r\n$1\r\n5\r\n"
	// <nil>
	// "*2\r\n$3\r\nGET\r\n$3\r\none\r\n*3\r\n$6\r\nINCRBY\r\n$3\r\ncnt\r\n$1\r\n5\r\n"
	// command argument type not supported (ErrRequest/ErrArgumentType {request: {SENDFOO [1s]}, argpos: 0, val: 1s})
}

func ExampleAsError() {
	vals := []interface{}{
		nil,
		1,
		"hello",
		errors.New("high"),
		redis.ErrResult.NewMsg("goodbye"),
	}

	for _, v := range vals {
		fmt.Printf("%T %v => %T %v\n", v, v, redis.AsError(v), redis.AsError(v))
	}

	// Output:
	// <nil> <nil> => <nil> <nil>
	// int 1 => <nil> <nil>
	// string hello => <nil> <nil>
	// *errors.errorString high => *errors.errorString high
	// *redis.Error goodbye (ErrResult) => *redis.Error goodbye (ErrResult)
}

func ExampleError_keys() {
	var e *redis.Error

	e = redis.ErrResult.New()
	fmt.Println(e)
	fmt.Printf("msg: %q\n", e.Get(redis.EKMessage))

	e = redis.ErrResult.NewMsg("no message 1")
	fmt.Println(e)
	fmt.Printf("msg: %q\n", e.GetByName("message"))

	e = redis.ErrResult.NewWrap(errors.New("something wrong")).WithMsg("no message 2")
	fmt.Println(e)
	fmt.Printf("msg: %q\n", e.Get(redis.EKMessage))
	fmt.Printf("cause: %q\n", e.GetByName("cause"))

	eKey := redis.NewErrorKey("myattr")
	e = e.With(eKey, []interface{}{"my value", 3})
	fmt.Println(e)
	fmt.Printf("myattr: %q == %q\n", e.GetByName("myattr"), e.Get(eKey))

	// Output:
	// regular redis error (ErrResult)
	// msg: %!q(<nil>)
	// no message 1 (ErrResult)
	// msg: "no message 1"
	// no message 2 (ErrResult {cause: something wrong})
	// msg: "no message 2"
	// cause: "something wrong"
	// no message 2 (ErrResult {myattr: [my value 3], cause: something wrong})
	// myattr: ["my value" '\x03'] == ["my value" '\x03']
}

func ExampleScanner() {
	defer runServer(46231)()
	ctx := context.Background()
	conn, _ := redisconn.Connect(ctx, "127.0.0.1:46231", redisconn.Opts{
		Logger: redisconn.NoopLogger{},
	})
	sync := redis.Sync{conn}
	sync.Do("SET", "key1", "val1")
	sync.Do("SET", "key2", "val2")
	scan := sync.Scanner(redis.ScanOpts{Match: "key*"})
	for {
		keys, err := scan.Next()
		if err != nil {
			if err != redis.ScanEOF {
				log.Fatal(err)
			}
			break
		}
		for _, key := range keys {
			fmt.Println(key)
		}
	}

	// Unordered output:
	// key1
	// key2
}

func ExampleSync() {
	defer runServer(46231)()
	ctx := context.Background()
	conn, _ := redisconn.Connect(ctx, "127.0.0.1:46231", redisconn.Opts{
		Logger: redisconn.NoopLogger{},
	})
	sync := redis.Sync{conn}

	res := sync.Do("SET", "key1", "1")
	fmt.Println(res)

	res = sync.Send(redis.Req("SET", "key2", "2"))
	fmt.Println(res)

	ress := sync.SendMany([]redis.Request{
		redis.Req("GET", "key1"),
		redis.Req("GET", "key2"),
	})
	fmt.Printf("%q\n", ress)

	res = sync.Do("HSET", "key1", "field1", "val1")
	fmt.Println(redis.AsError(res))

	ress, err := sync.SendTransaction([]redis.Request{
		redis.Req("INCR", "key1"),
		redis.Req("INCRBY", "key2", -1),
		redis.Req("GET", "key1"),
		redis.Req("GET", "key2"),
	})
	fmt.Println(err)
	fmt.Printf("%q\n", ress)

	// Output:
	// OK
	// OK
	// ["1" "2"]
	// WRONGTYPE Operation against a key holding the wrong kind of value (ErrResult {connection: *redisconn.Connection{addr: 127.0.0.1:46231}})
	// <nil>
	// ['\x02' '\x01' "2" "1"]
}

func runServer(port int) func() {
	testbed.InitDir(".")
	s := testbed.Server{Port: uint16(port)}
	s.Start()
	return func() {
		s.Stop()
		testbed.RmDir()
	}
}
