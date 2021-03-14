package bench

import (
	"context"
	"math/rand"
	"runtime"
	"strconv"
	"sync/atomic"
	. "testing"
	"time"

	"github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/testbed"
	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"

	redigo "github.com/wuxibin89/redis-go-cluster"
	radix "github.com/mediocregopher/radix/v3"
)

func benchCluster(port int) func() {
	testbed.InitDir(".")
	cl := testbed.NewCluster(uint16(port))
	cl.Start()
	return func() {
		cl.Stop()
		testbed.RmDir()
	}
}

func BenchmarkSerialGetSet(b *B) {
	defer benchCluster(45000)()
	rng := rand.New(rand.NewSource(1))
	b.Run("radix_pause0", func(b *B) {
		rdxv2, err := radix.NewCluster(
			[]string{"127.0.0.1:45000"},
			radix.ClusterPoolFunc(func(network, addr string) (radix.Client, error) {
				return radix.NewPool(network, addr, 4,
					radix.PoolPipelineWindow(0, 0))
			}),
		)
		if err != nil {
			b.Fatal(err)
			return
		}
		defer rdxv2.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := "foo" + strconv.Itoa(rng.Intn(65536))
			if err := rdxv2.Do(radix.Cmd(nil, "SET", key, "bar")); err != nil {
				b.Fatal(err)
			}
			if err := rdxv2.Do(radix.Cmd(nil, "GET", key)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("redigo", func(b *B) {
		red := newRedigo()
		defer red.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := "foo" + strconv.Itoa(rng.Intn(65536))
			if _, err := red.Do("SET", key, "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(red.Do("GET", key)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("redispipe", func(b *B) {
		pipe, err := rediscluster.NewCluster(context.Background(), []string{"127.0.0.1:45000"}, rediscluster.Opts{
			Logger: rediscluster.NoopLogger{},
			HostOpts: redisconn.Opts{
				Logger: redisconn.NoopLogger{},
			},
		})
		defer pipe.Close()
		if err != nil {
			b.Fatal(err)
		}
		sync := redis.Sync{pipe}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := "foo" + strconv.Itoa(rng.Intn(65536))
			if res := sync.Do("SET", key, "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", key); redis.AsError(res) != nil {
				b.Fatal(res)
			}
		}
	})

	b.Run("redispipe_pause0", func(b *B) {
		pipe, err := rediscluster.NewCluster(context.Background(), []string{"127.0.0.1:45000"}, rediscluster.Opts{
			Logger: rediscluster.NoopLogger{},
			HostOpts: redisconn.Opts{
				Logger:     redisconn.NoopLogger{},
				WritePause: -1,
			},
		})
		defer pipe.Close()
		if err != nil {
			b.Fatal(err)
		}
		sync := redis.Sync{pipe}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := "foo" + strconv.Itoa(rng.Intn(65536))
			if res := sync.Do("SET", key, "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", key); redis.AsError(res) != nil {
				b.Fatal(res)
			}
		}
	})
}

func BenchmarkParallelGetSet(b *B) {
	defer benchCluster(45000)()
	parallel := runtime.GOMAXPROCS(0) * 8
	i := uint32(1)

	do := func(b *B, fn func(*rand.Rand)) {
		b.SetParallelism(parallel)
		b.RunParallel(func(pb *PB) {
			rng := rand.New(rand.NewSource(int64(atomic.AddUint32(&i, 1))))
			for pb.Next() {
				fn(rng)
			}
		})
	}

	b.Run("radix", func(b *B) {
		rdx2, err := radix.NewCluster([]string{"127.0.0.1:45000"})
		defer rdx2.Close()
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		do(b, func(rng *rand.Rand) {
			key := "foo" + strconv.Itoa(rng.Intn(65536))
			if err := rdx2.Do(radix.Cmd(nil, "SET", key, "bar")); err != nil {
				b.Fatal(err)
			}
			if err := rdx2.Do(radix.Cmd(nil, "GET", key)); err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("redigo", func(b *B) {
		red := newRedigo()
		defer red.Close()
		b.ResetTimer()
		do(b, func(rng *rand.Rand) {
			key := "foo" + strconv.Itoa(rng.Intn(65536))
			if _, err := red.Do("SET", key, "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(red.Do("GET", key)); err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("redispipe", func(b *B) {
		pipe, err := rediscluster.NewCluster(context.Background(), []string{"127.0.0.1:45000"}, rediscluster.Opts{
			Logger: rediscluster.NoopLogger{},
			HostOpts: redisconn.Opts{
				Logger: redisconn.NoopLogger{},
			},
		})
		defer pipe.Close()
		if err != nil {
			b.Fatal(err)
		}
		sync := redis.Sync{pipe}
		b.ResetTimer()
		do(b, func(rng *rand.Rand) {
			key := "foo" + strconv.Itoa(rng.Intn(65536))
			if res := sync.Do("SET", key, "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", key); redis.AsError(res) != nil {
				b.Fatal(err)
			}
		})
	})
}

func newRedigo() *redigo.Cluster {
	c, err := redigo.NewCluster(&redigo.Options{
		StartNodes:  []string{"127.0.0.1:45000"},
		ConnTimeout: time.Minute,
		KeepAlive:   128,
		AliveTime:   time.Minute,
	})
	if err != nil {
		panic(err)
	}
	return c
}
