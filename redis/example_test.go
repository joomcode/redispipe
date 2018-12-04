package redis_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
	"github.com/joomcode/redispipe/testbed"
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
	// redispipe.request.argument_type: wrong argument 0
}

func ExampleAsError() {
	vals := []interface{}{
		nil,
		1,
		"hello",
		errors.New("high"),
		redis.ErrResult.New("goodbye"),
	}

	for _, v := range vals {
		fmt.Printf("%T %v => %T %v\n", v, v, redis.AsError(v), redis.AsError(v))
	}

	// Output:
	// <nil> <nil> => <nil> <nil>
	// int 1 => <nil> <nil>
	// string hello => <nil> <nil>
	// *errors.errorString high => *errors.errorString high
	// *errorx.Error redispipe.result: goodbye => *errorx.Error redispipe.result: goodbye
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
	// redispipe.result: WRONGTYPE Operation against a key holding the wrong kind of value
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
