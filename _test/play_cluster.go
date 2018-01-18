package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/redisconn"
)

type Req = redis.Request

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	ctx, _ := context.WithTimeout(context.Background(), 10000*time.Second)
	connopts := redisconn.Opts{
		IOTimeout: 1 * time.Second,
	}
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

	N, K := 800, 80000
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			for j := 0; j < K; j++ {
				res := synccluster.Send(Req{"GET", []interface{}{i*K + j}})
				if err := redis.AsError(res); err != nil {
					if rand.Intn(30000) == 0 {
						log.Println(err, i, j)
					}
				} else {
					if rand.Intn(300000) == 0 {
						log.Println("OK")
					}
				}
				//if !bytes.Equal(fut.Result.([]byte), []byte{49}) {
				//panic("mismatch")
				//}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("%d*%d: %s\n", N, K, time.Now().Sub(start))

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}
