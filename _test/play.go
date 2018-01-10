package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/joomcode/redispipe/redis_conn"
)

type Req = redis_conn.Request

func main() {
	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	opts := redis_conn.Opts{
		IOTimeout:   1 * time.Second,
		Concurrency: 32,
	}
	conn, err := redis_conn.Connect(ctx, "localhost:6379", opts)
	check(err)

	start := time.Now()
	var wg sync.WaitGroup
	N, K := 400, 400
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			for j := 0; j < K; j++ {
				fut := conn.Send(Req{"GET", []interface{}{"asdf"}})
				<-fut.Done()
				if fut.Err != nil {
					panic(fut.Err)
				}
				if !bytes.Equal(fut.Result.([]byte), []byte{49}) {
					panic("mismatch")
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("%d*%d: %s\n", N, K, time.Now().Sub(start))
}
