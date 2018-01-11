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

	"github.com/joomcode/redispipe/redis_conn"
)

type Req = redis_conn.Request

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
	opts := redis_conn.Opts{
		IOTimeout: 1 * time.Second,
	}
	conn, err := redis_conn.Connect(ctx, "localhost:6379", opts)
	check(err)

	start := time.Now()
	var wg sync.WaitGroup
	N, K := 800, 800
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			for j := 0; j < K; j++ {
				fut := conn.Send(Req{"GET", []interface{}{"asdf"}})
				<-fut.Done()
				if fut.Err != nil {
					if rand.Intn(300000) == 0 {
						log.Println(fut.Err)
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
