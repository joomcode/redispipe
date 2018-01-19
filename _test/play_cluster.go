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
	"strconv"
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
		HostOpts:     connopts,
		ConnsPerHost: 2,
		Name:         "default",
		Logger:       &rediscluster.SkippingLogger{},
	}
	addrs := []string{"127.0.0.1:30001", "127.0.0.1:30002"}
	cluster, err := rediscluster.NewCluster(ctx, addrs, clustopts)
	check(err)

	//synccluster := redis.SyncCtx{cluster.WithPolicy(rediscluster.PreferReplica)}
	synccluster := redis.SyncCtx{cluster}
	//reqctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	reqctx := context.TODO()

	N, K := 800, 80000
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			rnd := rand.New(rand.NewSource(rand.Int63()))
			for j := 0; j < K; j++ {
				v := rnd.Intn(10000)
				var res interface{}
				if rnd.Intn(10) == 0 {
					res = synccluster.Do(reqctx, "SET", v, v)
				} else {
					res = synccluster.Do(reqctx, "GET", v)
				}
				if err := redis.AsError(res); err != nil {
					if rand.Intn(30000) == 0 {
						log.Println(err, i, j)
					}
				} else {
					if res != nil {
						if str, ok := res.(string); ok {
							if str != "OK" {
								log.Println("string", str)
							}
							continue
						}
						rr, err := strconv.Atoi(string(res.([]byte)))
						if rr != v {
							log.Println(err, i, j, rr, v)
							continue
						}
					}
					if rand.Intn(300000) == 0 {
						log.Println("OK")
					}
				}
				//if !bytes.Equal(fut.Result.([]byte), []byte{49}) {
				//panic("mismatch")
				//}
			}
			wg.Done()
		}(i)
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
