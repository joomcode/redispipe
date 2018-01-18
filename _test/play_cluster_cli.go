package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/redisconn"
)

type Req = redis.Request

func main() {
	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()

	connopts := redisconn.Opts{
		IOTimeout: 1 * time.Second,
	}
	clustopts := rediscluster.Opts{
		HostOpts:      connopts,
		ConnsPerHost:  2,
		Name:          "default",
		CheckInterval: time.Second,
	}
	addrs := []string{"127.0.0.1:30001", "127.0.0.1:30002"}
	cluster, err := rediscluster.NewCluster(ctx, addrs, clustopts)
	check(err)
	synccluster := redis.Sync{cluster}

	in := bufio.NewReader(os.Stdin)
	for {
		line, _, err := in.ReadLine()
		if err != nil {
			break
		}
		parts := bytes.Split(line, []byte(" "))
		args := make([]interface{}, 0, len(parts))
		for _, s := range parts {
			if len(s) > 0 {
				args = append(args, s)
			}
		}
		if len(args) == 0 {
			continue
		}
		r := synccluster.Send(Req{string(args[0].([]byte)), args[1:]})
		if err := redis.AsError(r); err != nil {
			fmt.Println("error:", err)
		} else {
			fmt.Println("result:", r)
		}
	}
}
