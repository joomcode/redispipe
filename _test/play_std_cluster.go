package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/cluster"
)

func main() {
	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	p, err := cluster.NewWithOpts(cluster.Opts{
		Addr:     "localhost:30001",
		PoolSize: 4,
		Mode:     cluster.ModeMasterAndSlaves,
	})
	check(err)
	defer p.Close()

	start := time.Now()
	var wg sync.WaitGroup
	N, K := 800, 8000
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			for j := 0; j < K; j++ {
				_, _ = p.Cmd("GET", i*K+j).Str()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("%d*%d: %s\n", N, K, time.Now().Sub(start))
}
