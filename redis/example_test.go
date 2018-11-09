package redis_test

import (
	"errors"
	"fmt"
	"time"

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
	// <nil> <nil> <nil> <nil>
	// int 1 <nil> <nil>
	// string hello <nil> <nil>
	// *errors.errorString high *errors.errorString high
	// *redis.Error goodbye (ErrResult) *redis.Error goodbye (ErrResult)
}
