package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster"
)

var sleep = flag.Duration("sleep", 50*time.Millisecond, "sleep between batches")
var addr = flag.String("addr", "", "address of one of cluster instances (required)")
var match = flag.String("match", "", "match expression to delete (required)")

func main() {
	flag.Parse()
	if *match == "" {
		log.Fatal("Match argument should be specified and not empty")
	}
	if *addr == "" {
		log.Fatal("Redis address should be specified and not empty")
	}
	con, err := rediscluster.NewCluster(
		context.Background(),
		[]string{*addr},
		rediscluster.Opts{},
	)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	con.EachShard(func(sh redis.Sender, err error) bool {
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sync := redis.Sync{sh}
			iter := sync.Scanner(redis.ScanOpts{
				Match: *match,
				Count: 1000,
			})
			for {
				keys, err := iter.Next()
				if err != nil {
					if err == redis.ScanEOF {
						break
					}
					log.Fatal(err)
				}
				if len(keys) != 0 {
					reqs := make([]redis.Request, len(keys))
					for i, key := range keys {
						reqs[i] = redis.Req("DEL", key)
					}
					sync.SendMany(reqs)
					fmt.Printf("%q\n", keys[0])
				}
				if *sleep > 0 {
					time.Sleep(*sleep)
				}
			}
		}()
		return true
	})
	wg.Wait()
}
