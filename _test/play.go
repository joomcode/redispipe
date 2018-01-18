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
	"github.com/joomcode/redispipe/redisconn"
)

type Req = redis.Request

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
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

	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	ctx, _ := context.WithTimeout(context.Background(), 10000*time.Second)
	opts := redisconn.Opts{
		IOTimeout: 1 * time.Second,
	}
	conn, err := redisconn.Connect(ctx, "localhost:6379", opts)
	check(err)
	syncconn := redis.Sync{conn}

	start := time.Now()
	N, K := 800, 8000
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			for j := 0; j < K; j++ {
				res := syncconn.Send(Req{"GET", []interface{}{"asdf"}})
				if err := redis.AsError(res); err != nil {
					if rand.Intn(300) == 0 {
						log.Println(err)
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
