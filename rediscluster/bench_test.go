package rediscluster_test

import (
	"context"
	"runtime"
	. "testing"
	"time"

	"github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/testbed"

	"github.com/joomcode/redispipe/redis"

	redigo "github.com/chasex/redis-go-cluster"
	"github.com/joomcode/redispipe/redisconn"

	radixv2cluster "github.com/mediocregopher/radix.v2/cluster"
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
	b.Run("radixv2", func(b *B) {
		rdxv2, err := radixv2cluster.New("127.0.0.1:45000")
		if err != nil {
			b.Fatal(err)
			return
		}
		defer rdxv2.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := rdxv2.Cmd("SET", "foo", "bar").Err; err != nil {
				b.Fatal(err)
			}
			if err := rdxv2.Cmd("GET", "foo").Err; err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("redigo", func(b *B) {
		red := newRedigo()
		defer red.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := red.Do("SET", "foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(red.Do("GET", "foo")); err != nil {
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
			if res := sync.Do("SET", "foo", "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", "foo"); redis.AsError(res) != nil {
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
			if res := sync.Do("SET", "foo", "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", "foo"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
		}
	})
}

func BenchmarkParallelGetSet(b *B) {
	defer benchCluster(45000)()
	parallel := runtime.GOMAXPROCS(0)

	do := func(b *B, fn func()) {
		b.SetParallelism(parallel)
		b.RunParallel(func(pb *PB) {
			for pb.Next() {
				fn()
			}
		})
	}

	b.Run("radixv2", func(b *B) {
		rdx2, err := radixv2cluster.New("127.0.0.1:45000")
		defer rdx2.Close()
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		do(b, func() {
			if rdx2.Cmd("SET", "foo", "bar").Err != nil {
				b.Fatal(err)
			}
			if rdx2.Cmd("GET", "foo").Err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("redigo", func(b *B) {
		red := newRedigo()
		defer red.Close()
		b.ResetTimer()
		do(b, func() {
			if _, err := red.Do("SET", "foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(red.Do("GET", "foo")); err != nil {
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
		do(b, func() {
			if res := sync.Do("SET", "foo", "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", "foo"); redis.AsError(res) != nil {
				b.Fatal(err)
			}
		})
	})
}

func newRedigo() *redigo.Cluster {
	c, err := redigo.NewCluster(&redigo.Options{
		StartNodes:  []string{"127.0.0.1:45000"},
		ConnTimeout: time.Minute,
	})
	if err != nil {
		panic(err)
	}
	return c
}
