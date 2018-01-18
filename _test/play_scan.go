package main

import (
	"context"
	"fmt"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/redisconn"
)

func main() {
	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	ctx, _ := context.WithTimeout(context.Background(), 10000*time.Second)
	connopts := redisconn.Opts{
		IOTimeout: 1 * time.Second,
	}
	conn, err := redisconn.Connect(ctx, "localhost:6379", connopts)
	check(err)
	syncconn := redis.Sync{conn}

	scan(syncconn.Scanner(redis.ScanOpts{Match: "x*", Count: 100}))

	// cluster

	clustopts := rediscluster.Opts{
		HostOpts:      connopts,
		ConnsPerHost:  2,
		Name:          "default",
		CheckInterval: time.Second,
		ForceInterval: 10 * time.Millisecond,
		Logger:        &rediscluster.SkippingLogger{},
	}
	addrs := []string{"127.0.0.1:30001", "127.0.0.1:30002"}
	cluster, err := rediscluster.NewCluster(ctx, addrs, clustopts)
	check(err)
	synccluster := redis.Sync{cluster.WithPolicy(rediscluster.PreferReplica)}

	scan(synccluster.Scanner(redis.ScanOpts{Match: "x*", Count: 100}))
}

func scan(scanner redis.SyncIterator) {
	all := []string{}
	for {
		keys, err := scanner.Next()
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(keys)
		all = append(all, keys...)
	}
	uniq := make(map[string]bool)
	for _, key := range all {
		uniq[key] = true
	}
	fmt.Println("Total keys ", len(all), " uniq keys ", len(uniq))
}
