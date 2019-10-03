package redisconn_test

import (
	"context"
	"runtime"
	. "testing"

	pascal "github.com/pascaldekloe/redis"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/testbed"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/joomcode/redispipe/redisconn"

	radixv2pool "github.com/mediocregopher/radix.v2/pool"
	radixv2 "github.com/mediocregopher/radix.v2/redis"
)

func benchServer(port int) func() {
	testbed.InitDir(".")
	s := testbed.Server{Port: uint16(port)}
	s.Start()
	return func() {
		s.Stop()
		testbed.RmDir()
	}
}

func BenchmarkSerialGetSet(b *B) {
	defer benchServer(45678)()

	b.Run("pascal", func(b *B) {
		client := pascal.NewClient("127.0.0.1:45678", 0, 0)
		defer client.Terminate()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.SETString("foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := client.GET("foo"); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("radixv2", func(b *B) {
		rdxv2, err := radixv2.Dial("tcp", "127.0.0.1:45678")
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
		pipe, err := redisconn.Connect(context.Background(), "127.0.0.1:45678", redisconn.Opts{
			Logger: redisconn.NoopLogger{},
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
		pipe, err := redisconn.Connect(context.Background(), "127.0.0.1:45678", redisconn.Opts{
			Logger:     redisconn.NoopLogger{},
			WritePause: -1,
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
	defer benchServer(45678)()
	parallel := runtime.GOMAXPROCS(0) * 8

	do := func(b *B, fn func()) {
		b.SetParallelism(parallel)
		b.RunParallel(func(pb *PB) {
			for pb.Next() {
				fn()
			}
		})
	}

	b.Run("pascal", func(b *B) {
		client := pascal.NewClient("127.0.0.1:45678", 0, 0)
		defer client.Terminate()
		b.ResetTimer()
		do(b, func() {
			if err := client.SETString("foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := client.GET("foo"); err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("radixv2", func(b *B) {
		rdx2, err := radixv2pool.New("tcp", "127.0.0.1:45678", parallel)
		if err != nil {
			b.Fatal(err)
		}
		defer rdx2.Empty()
		b.ResetTimer()
		do(b, func() {
			conn, err := rdx2.Get()
			if err != nil {
				b.Fatal(err)
			}
			defer rdx2.Put(conn)
			if conn.Cmd("SET", "foo", "bar").Err != nil {
				b.Fatal(err)
			}
			if conn.Cmd("GET", "foo").Err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("redigo", func(b *B) {
		red := &redigo.Pool{
			MaxIdle: parallel,
			Dial: func() (redigo.Conn, error) {
				return newRedigo(), nil
			},
		}
		defer red.Close()
		b.ResetTimer()
		do(b, func() {
			conn := red.Get()
			defer conn.Close()
			if _, err := conn.Do("SET", "foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(conn.Do("GET", "foo")); err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("redispipe", func(b *B) {
		pipe, err := redisconn.Connect(context.Background(), "127.0.0.1:45678", redisconn.Opts{
			Logger: redisconn.NoopLogger{},
		})
		if err != nil {
			b.Fatal(err)
		}
		defer pipe.Close()
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

func newRedigo() redigo.Conn {
	c, err := redigo.Dial("tcp", "127.0.0.1:45678")
	if err != nil {
		panic(err)
	}
	return c
}
