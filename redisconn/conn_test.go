package redisconn_test

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/joomcode/redispipe/redis"
	. "github.com/joomcode/redispipe/redisconn"
	"github.com/joomcode/redispipe/testbed"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type Suite struct {
	suite.Suite
	s testbed.Server

	ctx       context.Context
	ctxcancel func()
}

func (s *Suite) SetupSuite() {
	testbed.InitDir(".")
	s.s.Port = 45678
	s.s.Start()
}

func (s *Suite) SetupTest() {
	s.s.Start()
	s.ctx, s.ctxcancel = context.WithTimeout(context.Background(), 55*time.Second)
}

func (s *Suite) TearDownTest() {
	s.ctxcancel()
	s.ctx, s.ctxcancel = nil, nil
}

func (s *Suite) TearDownSuite() {
	s.s.Stop()
	testbed.RmDir()
}

func (s *Suite) r() *require.Assertions {
	return s.Require()
}

func (s *Suite) AsError(v interface{}) *redis.Error {
	s.r().IsType((*redis.Error)(nil), v)
	return v.(*redis.Error)
}

var defopts = Opts{
	IOTimeout: 10 * time.Millisecond,
}

func (s *Suite) ping(conn *Connection, timeout time.Duration) interface{} {
	start := time.Now()
	res := redis.Sync{conn}.Do("PING")
	done := time.Now()
	if timeout == 0 {
		timeout = defopts.IOTimeout
	}
	s.r().WithinDuration(start, done, timeout*5/4)
	return res
}

func (s *Suite) goodPing(conn *Connection, timeout time.Duration) {
	s.Equal("PONG", s.ping(conn, timeout))
}

func (s *Suite) badPing(conn *Connection, kind redis.ErrorKind, timeout time.Duration) {
	res := s.ping(conn, timeout)
	rerr := s.AsError(res)
	s.Equal(kind, rerr.Kind())
}

func (s *Suite) waitReconnect(conn *Connection) {
	start := time.Now()
	for {
		at := time.Now()
		res := redis.Sync{conn}.Do("PING")
		done := time.Now()
		s.r().WithinDuration(at, done, defopts.IOTimeout*3/2)
		if rerr := redis.AsRedisError(res); rerr != nil {
			s.Equal(redis.ErrNotConnected, rerr.Kind())
			s.r().WithinDuration(start, at, defopts.IOTimeout*2)
		} else {
			s.Equal("PONG", res)
			s.r().WithinDuration(start, at, defopts.IOTimeout*3)
			break
		}
		runtime.Gosched()
	}
}

func TestConn(t *testing.T) {
	suite.Run(t, new(Suite))
}

func (s *Suite) TestConnects() {
	conn, err := Connect(s.ctx, s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()
	s.goodPing(conn, 0)
}

func (s *Suite) TestStopped_DoesntConnectWithNegativeReconnectPause() {
	s.s.Stop()
	opts := defopts
	opts.ReconnectPause = -1
	_, err := Connect(s.ctx, s.s.Addr(), opts)
	s.r().NotNil(err)
	rerr := s.AsError(err)
	s.Equal(redis.ErrDial, rerr.Kind())
}

func (s *Suite) TestStopped_Reconnects() {
	s.s.Stop()

	conn, err := Connect(s.ctx, s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	s.badPing(conn, redis.ErrNotConnected, 0)

	s.s.Start()
	s.waitReconnect(conn)

	s.s.Stop()
	time.Sleep(1 * time.Millisecond)
	s.badPing(conn, redis.ErrNotConnected, 0)

	s.s.Start()
	s.waitReconnect(conn)
}

func (s *Suite) TestStopped_Reconnects2() {
	conn, err := Connect(s.ctx, s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	s.goodPing(conn, 0)

	s.s.Stop()
	time.Sleep(1 * time.Millisecond)
	s.badPing(conn, redis.ErrNotConnected, 0)

	s.s.Start()
	s.waitReconnect(conn)

	s.s.Stop()
	time.Sleep(1 * time.Millisecond)
	s.badPing(conn, redis.ErrNotConnected, 0)

	s.s.Start()
	s.waitReconnect(conn)
}

func (s *Suite) TestTimeout() {
	conn, err := Connect(s.ctx, s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	s.goodPing(conn, 0)

	s.s.Pause()
	events := 0
	start := time.Now()
	for events != 7 {
		res := s.ping(conn, 0)
		rerr := s.AsError(res)
		switch rerr.Kind() {
		case redis.ErrIO:
			events |= 1
		case redis.ErrConnSetup:
			events |= 2
		case redis.ErrNotConnected:
			events |= 4
		}
		s.r().WithinDuration(start, time.Now(), defopts.IOTimeout*10)
	}

	s.s.Resume()
	s.waitReconnect(conn)
}

func (s *Suite) TestTransaction() {
	conn, err := Connect(s.ctx, s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	sconn := redis.SyncCtx{conn}

	// transaction just works
	res, err := sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("PING"),
		redis.Req("PING", "asdf"),
	})
	s.Nil(err)
	if s.IsType([]interface{}{}, res) && s.Len(res, 2) {
		s.r().Equal("PONG", res[0])
		s.r().Equal([]byte("asdf"), res[1])
	}

	s.s.DoSure("SET", "tran:x", 1)

	// transaction daesn't execute in case of wrong command
	res, err = sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("INCR", "tran:x"),
		redis.Req("PANG"),
	})
	s.NotNil(err)
	rerr := s.AsError(err)
	s.Equal(redis.ErrResult, rerr.Kind())
	s.True(strings.HasPrefix(rerr.Msg(), "EXECABORT"))

	s.Equal([]byte("1"), s.s.DoSure("GET", "tran:x"))

	// transaction is executed partially (that is redis's behavior):
	// - first command executed well
	// - second command returns with error.
	res, err = sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("INCR", "tran:x"),
		redis.Req("HSET", "tran:x", "y", "1"),
	})
	s.Nil(err)
	if s.IsType([]interface{}{}, res) && s.Len(res, 2) {
		s.r().Equal(int64(2), res[0])
		rerr := s.AsError(res[1])
		s.Equal(redis.ErrResult, rerr.Kind())
		s.True(strings.HasPrefix(rerr.Msg(), "WRONGTYPE"))
	}

	s.Equal([]byte("2"), s.s.DoSure("GET", "tran:x"))
}

func (s *Suite) TestScan() {
	conn, err := Connect(s.ctx, s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	sconn := redis.SyncCtx{conn}
	for i := 0; i < 1000; i++ {
		sconn.Do(s.ctx, "SET", "scan:"+strconv.Itoa(i), i)
	}

	allkeys := make(map[string]struct{}, 1000)
	for scanner := sconn.Scanner(s.ctx, redis.ScanOpts{Match: "scan:*"}); ; {
		keys, err := scanner.Next()
		if err != nil {
			s.Equal(redis.ScanEOF, err)
			break
		}
		for _, key := range keys {
			_, ok := allkeys[key]
			s.False(ok)
			allkeys[key] = struct{}{}
		}
	}
	s.Len(allkeys, 1000)
}

// stress test for "good case" when redis works without issues.
func (s *Suite) TestAllReturns_Good() {
	conn, err := Connect(context.Background(), s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	const N = 200
	const K = 200
	ch := make(chan struct{}, N)

	sconn := redis.SyncCtx{conn}
	for i := 0; i < N; i++ {
		go func(i int) {
			for j := 0; j < K; j++ {
				sij := strconv.Itoa(i*N + j)
				res := sconn.Do(s.ctx, "PING", sij)
				if !s.IsType([]byte{}, res) || !s.Equal(sij, string(res.([]byte))) {
					return
				}
				ress := sconn.SendMany(s.ctx, []redis.Request{
					redis.Req("PING", "a"+sij),
					redis.Req("PING", "b"+sij),
				})
				if !s.IsType([]byte{}, ress[0]) || !s.Equal("a"+sij, string(ress[0].([]byte))) {
					return
				}
				if !s.IsType([]byte{}, ress[1]) || !s.Equal("b"+sij, string(ress[1].([]byte))) {
					return
				}
			}
			ch <- struct{}{}
		}(i)
	}

	cnt := 0
Loop:
	for cnt < N {
		select {
		case <-s.ctx.Done():
			break Loop
		case <-ch:
			cnt++
		}
	}
	s.Equal(N, cnt, "Not all goroutines finished")
}

// stress test for "bad case" when redis occasionally stops and stalls.
func (s *Suite) TestAllReturns_Bad() {
	conn, err := Connect(context.Background(), s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	const N = 200
	fin := make(chan struct{})
	goods := make([]chan bool, N)
	checks := make(chan bool, N)
	finch := make(chan struct{}, N)

	sconn := redis.SyncCtx{conn}
	ctx := s.ctx
	for i := 0; i < N; i++ {
		goods[i] = make(chan bool, 1)
		go func(i int) {
			check, good := true, true
		Loop:
			for j := 0; ; j++ {
				select {
				case good = <-goods[i]:
					check = true
				case <-fin:
					break Loop
				case <-ctx.Done():
					break Loop
				default:
				}
				sij := strconv.Itoa(i*N + j)
				res := sconn.Do(ctx, "PING", sij)
				ress := sconn.SendMany(ctx, []redis.Request{
					redis.Req("PING", "a"+sij),
					redis.Req("PING", "b"+sij),
				})
				if check && good {
					ok := s.IsType([]byte{}, res) && s.Equal(sij, string(res.([]byte)))
					ok = ok && s.IsType([]byte{}, ress[0]) && s.Equal("a"+sij, string(ress[0].([]byte)))
					ok = ok && s.IsType([]byte{}, ress[1]) && s.Equal("b"+sij, string(ress[1].([]byte)))
					checks <- ok
				} else if check && !good {
					ok := s.IsType((*redis.Error)(nil), res)
					ok = ok && s.IsType((*redis.Error)(nil), ress[0])
					ok = ok && s.IsType((*redis.Error)(nil), ress[1])
					checks <- ok
				}
				check = false
				runtime.Gosched()
			}
			finch <- struct{}{}
		}(i)
	}

	isAllGood := true
	sendgoods := func(need bool) bool {
		for i := 0; i < N; i++ {
			select {
			case <-s.ctx.Done():
				isAllGood = false
				return false
			case goods[i] <- need:
			}
		}
		return true
	}
	allgood := func() bool {
		ok := true
		for i := 0; i < N; i++ {
			select {
			case <-s.ctx.Done():
				isAllGood = false
				return false
			case cur := <-checks:
				ok = ok && cur
			}
		}
		isAllGood = ok
		return ok
	}

	time.Sleep(defopts.IOTimeout * 2)
	for k := 0; k < 10; k++ {
		if !allgood() {
			break
		}

		// kill redis: OS will report about disconnect
		s.s.Stop()
		time.Sleep(defopts.IOTimeout * 3)
		if !sendgoods(false) || !allgood() {
			break
		}

		s.s.Start()
		time.Sleep(defopts.IOTimeout * 2)
		if !sendgoods(true) || !allgood() {
			break
		}

		// stop redis: connection is stalled as when network looses packets.
		s.s.Pause()
		time.Sleep(defopts.IOTimeout * 2)
		if !sendgoods(false) || !allgood() {
			break
		}

		s.s.Resume()
		time.Sleep(defopts.IOTimeout * 2)
		if !sendgoods(true) {
			break
		}
	}

	if isAllGood {
		s.True(allgood())
	}

	close(fin)

	cnt := 0
Loop:
	for cnt < N {
		select {
		case <-s.ctx.Done():
			break Loop
		case <-finch:
			cnt++
		}
	}
	s.Equal(N, cnt, "Not all goroutines finished")
}
