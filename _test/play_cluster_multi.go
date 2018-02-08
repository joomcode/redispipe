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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/redisconn"
)

type Req = redis.Request

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

type deciticker struct {
	cur uint32
}

func (d *deciticker) Current() uint32 {
	return atomic.LoadUint32(&d.cur)
}

var cur deciticker

func init() {
	go func() {
		for range time.NewTicker(100 * time.Millisecond).C {
			atomic.AddUint32(&cur.cur, 1)
		}
	}()
}

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
		HostOpts:       connopts,
		ConnsPerHost:   2,
		Name:           "default",
		RoundRobinSeed: &cur,
	}
	addrs := []string{"127.0.0.1:30001", "127.0.0.1:30002"}
	cluster, err := rediscluster.NewCluster(ctx, addrs, clustopts)
	check(err)

	//synccluster := redis.Sync{cluster.WithPolicy(rediscluster.MasterAndSlaves)}
	//synccluster := redis.Sync{cluster}
	synccluster := redis.SyncCtx{cluster.WithPolicy(rediscluster.MasterAndSlaves)}
	//synccluster := redis.SyncCtx{cluster}
	//reqctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	reqctx := context.TODO()

	N, K := 800, 18000
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			rnd := rand.New(rand.NewSource(rand.Int63()))
			for j := 0; j < K; j++ {
				v := rnd.Intn(10000)
				if rnd.Intn(10) == 0 {
					vv := strings.Repeat(strconv.Itoa(v), 100)
					res := synccluster.Do(reqctx, "SET", v, vv)
					if str, ok := res.(string); !ok || str != "OK" {
						log.Println("SET ERR", str)
						continue
					}
				} else {
					res := synccluster.SendMany(reqctx, []redis.Request{
						redis.Req("GET", v),
						redis.Req("GET", v+1),
						redis.Req("GET", v+2),
						redis.Req("GET", v+3),
						redis.Req("GET", v+4),
						redis.Req("GET", v+5),
					})
					ch := func(r interface{}, v int) {
						if vv, ok := res[0].([]byte); !ok {
							log.Println("Get ", v, vv)
							//} else if rr, err := strconv.Atoi(string(vv)); err != nil || rr != v {
							//	log.Println("Get ", v, vv)
						}
					}
					for i, r := range res {
						ch(r, v+i)
					}
				}
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
