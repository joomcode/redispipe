package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
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
		cmds := bytes.Split(line, []byte(";"))
		reqs := []Req{}
		for _, cmd := range cmds {
			parts := bytes.Split(cmd, []byte(" "))
			args := make([]interface{}, 0, len(parts))
			for _, s := range parts {
				if len(s) > 0 {
					args = append(args, s)
				}
			}
			if len(args) == 0 {
				continue
			}
			reqs = append(reqs, Req{string(args[0].([]byte)), args[1:]})
		}
		switch l := len(reqs); l {
		case 0:
			continue
		case 1:
			r := synccluster.Send(reqs[0])
			if err := redis.AsError(r); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println("result:", r)
			}
		default:
			if strings.ToUpper(reqs[0].Cmd) == "MULTI" &&
				strings.ToUpper(reqs[l-1].Cmd) == "EXEC" {
				res, err := synccluster.SendTransaction(reqs)
				fmt.Printf("results: %+v\nerror: %v\n", res, err)
			} else {
				res := synccluster.SendMany(reqs)
				fmt.Printf("results: %+v\n", res)
			}
		}
	}
}
