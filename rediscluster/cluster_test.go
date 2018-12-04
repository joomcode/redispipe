package rediscluster_test

import (
	"context"
	"log"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joomcode/errorx"

	"github.com/joomcode/redispipe/redis"
	. "github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
	"github.com/joomcode/redispipe/redisconn"
	"github.com/joomcode/redispipe/testbed"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const NumSlots = redisclusterutil.NumSlots

type Suite struct {
	suite.Suite
	cl   *testbed.Cluster
	keys []string

	ctx       context.Context
	ctxcancel func()
}

func (s *Suite) SetupSuite() {
	testbed.InitDir(".")
	s.cl = testbed.NewCluster(43210)
	s.keys = make([]string, NumSlots)
	cnt := 0
	for i := 0; cnt < NumSlots; i++ {
		key := "x" + strconv.Itoa(i)
		slot := redisclusterutil.Slot(key)
		if s.keys[slot] == "" {
			s.keys[slot] = key
			cnt++
		}
	}
}

func (s *Suite) SetupTest() {
	s.cl.Start()
	s.ctx, s.ctxcancel = context.WithTimeout(context.Background(), 1000*time.Second)
	DebugDisable = false
	DebugEventsReset()
}

func (s *Suite) TearDownTest() {
	s.ctxcancel()
	s.ctx, s.ctxcancel = nil, nil
}

func (s *Suite) TearDownSuite() {
	s.cl.Stop()
	testbed.RmDir()
}

func (s *Suite) r() *require.Assertions {
	return s.Require()
}

func (s *Suite) AsError(v interface{}) *errorx.Error {
	if v == nil {
		return nil
	}
	s.r().IsType((*errorx.Error)(nil), v)
	return v.(*errorx.Error)
}

var defopts = redisconn.Opts{
	IOTimeout: 50 * time.Millisecond,
}

var clustopts = Opts{
	HostOpts:      defopts,
	Name:          "default",
	CheckInterval: 200 * time.Millisecond,

	ConnHostPolicy: ConnHostRoundRobin,
}

var longcheckopts = Opts{
	HostOpts:      defopts,
	Name:          "default",
	CheckInterval: 1 * time.Second,
}

func TestCluster(t *testing.T) {
	suite.Run(t, new(Suite))
}

func (s *Suite) TestConnectDisconnected() {
	s.cl.Stop()

	_, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().NotNil(err)
}

func slotkey(prefix, slot string, suffix ...string) string {
	if len(suffix) == 0 {
		return prefix + "{" + slot + "}"
	} else {
		return prefix + "{" + slot + "}" + strings.Join(suffix, ":")
	}
}

func slot2node(slot int) int {
	switch true {
	case slot < 5500:
		return 0
	case slot < 11000:
		return 1
	default:
		return 2
	}
}

func (s *Suite) slotnode(slot int) *testbed.Node {
	return &s.cl.Node[slot2node(slot)]
}

func (s *Suite) TestBasicOps() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().Nil(err)
	defer cl.Close()
	scl := redis.SyncCtx{cl}

	for _, key := range s.keys {
		s.Equal("OK", scl.Do(s.ctx, "SET", slotkey("basic", key), key))
	}
	for i, key := range s.keys {
		s.Equal([]byte(key), s.slotnode(i).Do("GET", slotkey("basic", key)))
		s.slotnode(i).DoSure("SET", slotkey("basic", key), key+"y")
	}
	for _, key := range s.keys {
		s.Equal([]byte(key+"y"), scl.Do(s.ctx, "GET", slotkey("basic", key)))
	}
}

func (s *Suite) Test_justToCover() {
	cl, err := NewCluster(nil, nil, clustopts)
	s.r().Nil(cl)
	s.r().Error(err)

	cl, err = NewCluster(s.ctx, nil, clustopts)
	s.r().Nil(cl)
	s.r().Error(err)

	opts := clustopts
	opts.CheckInterval = 0
	opts.MovedRetries = 11
	opts.WaitToMigrate = time.Microsecond
	cl, err = NewCluster(s.ctx, []string{"never-known-lost-my-host.badubadu.duba:43210"}, opts)
	s.r().Nil(cl)
	s.r().Error(err)

	opts.CheckInterval = 11 * time.Minute
	opts.MovedRetries = 1
	opts.WaitToMigrate = time.Second
	cl, err = NewCluster(s.ctx, []string{"127.0.0.1:43200"}, opts)
	s.r().Nil(cl)
	s.r().Error(err)

	opts = clustopts
	opts.ConnsPerHost = 1
	opts.Handle = new(struct{})
	cl, err = NewCluster(s.ctx, []string{"127.0.0.1:43210"}, opts)
	s.r().Nil(err)
	defer cl.Close()

	s.r().NotEqual(s.ctx, cl.Ctx())
	s.r().NotNil(cl.Ctx())
	s.r().Regexp("Cluster.*default", cl.String())
	s.r().Equal(opts.Handle, cl.Handle())

	var cnc cancelledFuture
	cl.SendTransaction(nil, &cnc, 0)
	s.r().Equal(1, cnc.cnt)
	s.r().Error(redis.AsError(cnc.res))
	cl.Send(redis.Req("GET", "a"), &cnc, 0)
	s.r().Equal(2, cnc.cnt)
	s.r().Error(redis.AsError(cnc.res))

	var results interface{}
	fut := redis.FuncFuture(func(res interface{}, n uint64) { results = res })
	cl.SendTransaction(nil, fut, 0)
	s.r().Equal([]interface{}{}, results)

	cl.SendTransaction([]redis.Request{redis.Req("GET", "A"), redis.Req("GET", "a")}, fut, 0)
	s.r().Error(redis.AsError(results))

	res := redis.Sync{cl}.Send(redis.Req("SADD", "tocoverset", "a", "b", "c"))
	s.r().NoError(redis.AsError(res))
	var allkeys []string
	scanner := redis.Sync{cl}.Scanner(redis.ScanOpts{Cmd: "SSCAN", Key: "tocoverset"})
	for {
		keys, err := scanner.Next()
		if err != nil {
			s.r().Equal(redis.ScanEOF, err)
			break
		}
		allkeys = append(allkeys, keys...)
	}
	sort.Strings(allkeys)
	s.r().Equal([]string{"a", "b", "c"}, allkeys)
}

type cancelledFuture struct {
	cnt int
	res interface{}
}

func (c *cancelledFuture) Cancelled() bool {
	return true
}

func (c *cancelledFuture) Resolve(res interface{}, n uint64) {
	c.res = res
	c.cnt++
}

func (s *Suite) TestSendMany() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().Nil(err)
	defer cl.Close()
	scl := redis.SyncCtx{cl}

	reqs := make([]redis.Request, NumSlots)
	for i, key := range s.keys {
		reqs[i] = redis.Req("SET", slotkey("many", key), key)
	}
	ress := scl.SendMany(s.ctx, reqs)
	for _, res := range ress {
		s.Equal("OK", res)
	}

	for i, key := range s.keys {
		s.Equal([]byte(key), s.slotnode(i).Do("GET", slotkey("many", key)))
		s.slotnode(i).DoSure("SET", slotkey("many", key), key+"y")
	}

	for i, key := range s.keys {
		reqs[i] = redis.Req("GET", slotkey("many", key))
	}
	ress = scl.SendMany(s.ctx, reqs)
	for i, res := range ress {
		s.Equal([]byte(s.keys[i]+"y"), res)
	}
}

func (s *Suite) TestTransactionNormal() {
	// copy fo connection test
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl}

	key := s.keys[0]
	key1 := slotkey("trans", key, "1")
	key2 := slotkey("trans", key, "2")
	s.cl.Node[0].DoSure("SET", key1, "1")
	s.cl.Node[0].DoSure("SET", key2, "2")

	res, err := sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("GET", key1),
		redis.Req("GET", key2),
	})
	s.Nil(err)
	if s.IsType([]interface{}{}, res) && s.Len(res, 2) {
		s.r().Equal([]byte("1"), res[0])
		s.r().Equal([]byte("2"), res[1])
	}

	res, err = sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("INCR", key1),
		redis.Req("PANG"),
	})
	s.NotNil(err)
	rerr := s.AsError(err)
	s.True(rerr.IsOfType(redis.ErrExecAbort))

	s.Equal([]byte("1"), s.cl.Node[0].DoSure("GET", key1))

	res, err = sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("INCR", key1),
		redis.Req("HSET", key2, "y", "1"),
	})
	s.Nil(err)
	if s.IsType([]interface{}{}, res) && s.Len(res, 2) {
		s.r().Equal(int64(2), res[0])
		rerr := s.AsError(res[1])
		s.True(rerr.IsOfType(redis.ErrResult))
		s.True(strings.HasPrefix(rerr.Message(), "WRONGTYPE"))
	}

	s.Equal([]byte("2"), s.cl.Node[0].DoSure("GET", key1))
}

func (s *Suite) TestScan() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl}

	reqs := make([]redis.Request, 0, NumSlots*4)
	for i := 0; i < NumSlots*4; i++ {
		reqs = append(reqs, redis.Req("SET", slotkey("scan:", s.keys[i/4], strconv.Itoa(i%4)), "1"))
	}
	res := sconn.SendMany(s.ctx, reqs)
	s.r().Nil(redis.AsError(res))

	allkeys := make(map[string]struct{}, len(reqs))
	for scanner := sconn.Scanner(s.ctx, redis.ScanOpts{Match: "scan:*", Count: 1000}); ; {
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
	s.Len(allkeys, len(reqs), "length doesn't match", len(allkeys), len(reqs))
}

type alwaysZero struct{}

func (a alwaysZero) Current() uint32 {
	return 0
}

func (s *Suite) TestFallbackToSlaveStop() {
	opts := longcheckopts
	opts.RoundRobinSeed = alwaysZero{}
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, opts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl.WithPolicy(MasterAndSlaves)}

	key := slotkey("toslave", s.keys[1], "stop")
	sconn.Do(s.ctx, "SET", key, "1")

	s.cl.Node[0].Stop()
	// test read from replica
	s.Equal([]byte("1"), sconn.Do(s.ctx, "GET", key))

	// wait replica becomes master
	s.cl.WaitClusterOk()
	time.Sleep(longcheckopts.CheckInterval)

	s.Equal("OK", sconn.Do(s.ctx, "SET", key, "1"))

	// return master
	s.cl.Node[0].Start()
	s.cl.WaitClusterOk()
	s.cl.Node[3].Stop()
	s.cl.WaitClusterOk()
	s.cl.Node[3].Start()
}

func (s *Suite) TestFallbackToSlaveTimeout() {
	opts := longcheckopts
	opts.RoundRobinSeed = alwaysZero{}
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, opts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl.WithPolicy(PreferSlaves)}

	key := slotkey("toslave", s.keys[1], "timeout")
	sconn.Do(s.ctx, "SET", key, "1")

	s.cl.Node[0].Pause()
	// test read from replica
	s.Equal([]byte("1"), sconn.Do(s.ctx, "GET", key))
	s.Contains(DebugEvents(), "retry")

	// wait replica becomes master
	s.cl.WaitClusterOk()
	time.Sleep(longcheckopts.CheckInterval)

	s.Equal("OK", sconn.Do(s.ctx, "SET", key, "1"))

	// return master
	s.cl.Node[0].Resume()
	s.cl.WaitClusterOk()
	s.cl.Node[3].Stop()
	s.cl.WaitClusterOk()
	s.cl.Node[3].Start()

}

func (s *Suite) TestGetMoved() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, longcheckopts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl}

	key := slotkey("moved", s.keys[10999], "get")
	s.r().Equal("OK", sconn.Do(s.ctx, "SET", key, key))

	s.cl.MoveSlot(10999, 1, 2)

	s.Equal([]byte(key), sconn.Do(s.ctx, "GET", key))
	s.Contains(DebugEvents(), "moved")

	s.cl.MoveSlot(10999, 2, 1)
}

func (s *Suite) TestSetMoved() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, longcheckopts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl}

	key := slotkey("moved", s.keys[10998], "set")
	s.r().Equal("OK", sconn.Do(s.ctx, "SET", key, key))

	s.cl.MoveSlot(10998, 1, 2)
	defer s.cl.MoveSlot(10998, 2, 1)

	s.r().Equal("OK", sconn.Do(s.ctx, "SET", key, key+"!"))
	s.Contains(DebugEvents(), "moved")

	s.Equal([]byte(key+"!"), s.cl.Node[2].Do("GET", key))

}

func (s *Suite) TestMasterOnly() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().Nil(err)
	defer cl.Close()

	for i := 0; i < 10; i++ {
		func() {
			DebugEventsReset()

			err = redisclusterutil.SetMasterOnly(cl, "", []uint16{1, 2})
			s.r().Nil(err)
			time.Sleep(clustopts.CheckInterval * 2)
			s.Contains(DebugEvents(), "automatic masteronly")

			err = redisclusterutil.UnsetMasterOnly(cl, "", []uint16{1, 2})
			s.r().Nil(err)
			DebugEventsReset()
			time.Sleep(clustopts.CheckInterval * 2)
			s.NotContains(DebugEvents(), "automatic masteronly")
		}()
	}
}

func (s *Suite) TestAsk() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, longcheckopts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl}

	s.cl.InitMoveSlot(10997, 1, 2)
	defer s.cl.CancelMoveSlot(10997)

	key := slotkey("ask", s.keys[10997])
	s.r().Equal("OK", sconn.Do(s.ctx, "SET", key, key))
	s.Equal([]byte(key), sconn.Do(s.ctx, "GET", key))
	s.Contains(DebugEvents(), "asking")

	// recheck that redis responses with correct errors
	rerr := s.AsError(s.cl.Node[2].Do("GET", key))
	s.True(rerr.IsOfType(redis.ErrMoved))

	rerr = s.AsError(s.cl.Node[1].Do("GET", key))
	s.True(rerr.IsOfType(redis.ErrAsk))

	s.Equal(int64(1), sconn.Do(s.ctx, "DEL", key))

}

func (s *Suite) TestAskTransaction() {
	opts := longcheckopts
	opts.MovedRetries = 4

	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, opts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl}

	key1 := slotkey("asktran", s.keys[10996], "1")
	key2 := slotkey("asktran", s.keys[10996], "2")
	key3 := slotkey("asktran", s.keys[10996], "3")
	//key4 := slotkey("asktran", s.keys[10996], "4")

	s.cl.InitMoveSlot(10996, 1, 2)
	defer s.cl.CancelMoveSlot(10996)

	sconn.Do(s.ctx, "SET", key1, "3")
	sconn.Do(s.ctx, "SET", key2, "3")

	DebugEventsReset()
	// if all keys are in new shard, then redis allows transaction to execute
	// on new shard.
	res, err := sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("SET", key1, "1"),
		redis.Req("SET", key2, "2"),
	})
	s.Nil(err)
	s.Equal([]interface{}{"OK", "OK"}, res)
	s.Contains(DebugEvents(), "transaction asking")

	s.Equal([]byte("1"), sconn.Do(s.ctx, "GET", key1))

	DebugEventsReset()
	// if some keys are absent in new shard, then redis returns TRYAGAIN error
	res, err = sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("SET", key2, "1"),
		redis.Req("SET", key3, "2"),
	})
	s.True(s.AsError(err).IsOfType(redis.ErrTryAgain))
	s.Contains(DebugEvents(), "transaction asking")
	s.Contains(DebugEvents(), "transaction tryagain")

	// lets add key3 to make transaction happy
	var wg sync.WaitGroup
	wg.Add(1)
	time.AfterFunc(5*time.Millisecond, func() {
		defer wg.Done()
		sconn.Do(s.ctx, "SET", key3, "3")
	})

	DebugEventsReset()
	res, err = sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("SET", key2, "1"),
		redis.Req("SET", key3, "2"),
	})
	s.Nil(err)

	s.Equal([]byte("1"), sconn.Do(s.ctx, "GET", key2))
	s.Equal([]byte("2"), sconn.Do(s.ctx, "GET", key3))
	s.Contains(DebugEvents(), "transaction asking")
	s.Contains(DebugEvents(), "transaction tryagain")

	wg.Wait()
}

func (s *Suite) TestMovedTransaction() {
	opts := longcheckopts
	opts.MovedRetries = 4

	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, opts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl}

	key1 := slotkey("movetran", s.keys[10995], "1")
	key2 := slotkey("movetran", s.keys[10995], "2")

	s.cl.MoveSlot(10995, 1, 2)
	defer s.cl.MoveSlot(10995, 2, 1)

	res, err := sconn.SendTransaction(s.ctx, []redis.Request{
		redis.Req("SET", key1, "2"),
		redis.Req("SET", key2, "3"),
	})
	s.Nil(err)
	s.Equal([]interface{}{"OK", "OK"}, res)

	s.Equal([]byte("2"), sconn.Do(s.ctx, "GET", key1))
	s.Equal([]byte("3"), sconn.Do(s.ctx, "GET", key2))
	s.Equal([]string{"transaction moved"}, DebugEvents())
}

func (s *Suite) fillMany(sconn redis.SyncCtx, prefix string) {
	// prepare
	reqs := make([]redis.Request, NumSlots)
	for i, key := range s.keys {
		reqs[i] = redis.Req("SET", slotkey(prefix, key), key)
	}
	ress := sconn.SendMany(s.ctx, reqs)
	for _, res := range ress {
		s.r().True("OK" == res)
	}
	time.Sleep(10 * time.Millisecond)
}

func (s *Suite) TestAllReturns_Good() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl.WithPolicy(MasterAndSlaves)}

	s.fillMany(sconn, "allgood")

	const N = 400
	const K = 400
	ch := make(chan struct{}, N)

	for i := 0; i < N; i++ {
		go func(i int) {
			for j := 0; j < K; j++ {
				skey := s.keys[(i*N+j)*127%NumSlots]
				key := slotkey("allgood", skey)
				res := sconn.Do(s.ctx, "GET", key)
				if !s.Equal([]byte(skey), res) {
					return
				}

				keya := slotkey("allgood", skey, "a")
				keyb := slotkey("allgood", skey, "b")

				z := i*53 + j*51
				reverse := (z^z>>8)&1 == 0
				if reverse {
					keya, keyb = keyb, keya
				}

				reqs := []redis.Request{
					redis.Req("SET", keya, keyb),
					redis.Req("GET", keyb),
				}
				ress := sconn.SendMany(s.ctx, reqs)

				if !s.Equal("OK", ress[0]) {
					return
				}
				if ress[1] != nil && !s.Equal([]byte(keya), ress[1]) {
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
	s.Equal([]string(nil), DebugEvents())
}

func (s *Suite) TestAllReturns_GoodMoving() {
	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl}

	s.fillMany(sconn, "allgoodmove")
	log.Println("Starting seventh")
	s.cl.StartSeventhNode()
	log.Println("Started seventh")
	defer s.cl.StopSeventhNode()

	const N = 400
	ch := make(chan struct{}, N)
	var good uint32
	var bad uint32
	var stop uint32
	ctx := s.ctx

	for i := 0; i < N; i++ {
		go func(i int) {
			defer func() { ch <- struct{}{} }()
			for j := 0; atomic.LoadUint32(&stop) == 0; j++ {
				skey := s.keys[(i*N+j)*127%NumSlots]
				key := slotkey("allgoodmove", skey)
				res := sconn.Do(ctx, "GET", key)
				if !s.Equal([]byte(skey), res) {
					log.Println("Res ", res)
					atomic.AddUint32(&bad, 1)
				}

				keya := slotkey("allgoodmove", skey, "a")
				keyb := slotkey("allgoodmove", skey, "b")

				z := i*53 + j*51
				reverse := (z^z>>8)&1 == 0
				if reverse {
					keya, keyb = keyb, keya
				}

				reqs := []redis.Request{
					redis.Req("SET", keya, keyb),
					redis.Req("GET", keyb),
				}
				ress := sconn.SendMany(ctx, reqs)

				if !s.Equal("OK", ress[0]) {
					log.Println("Ress[0] ", ress[0])
					atomic.AddUint32(&bad, 1)
				}
				if ress[1] != nil && !s.Equal([]byte(keya), ress[1]) {
					log.Println("Ress[1] ", ress[1])
					atomic.AddUint32(&bad, 1)
				}
			}
			atomic.AddUint32(&good, 1)
		}(i)
	}

	s.cl.MoveSlot(1, 0, 6)
	for i := 0; i < 10; i++ {
		s.cl.MoveSlot(2, 0, 6)
		s.cl.MoveSlot(2, 6, 0)
	}
	s.cl.MoveSlot(1, 6, 0)
	atomic.StoreUint32(&stop, 1)

	cnt := 0
Loop:
	for cnt < N {
		select {
		case <-ctx.Done():
			break Loop
		case <-ch:
			cnt++
		}
	}
	s.Equal(N, cnt, "Not all goroutines finished")
	s.Equal(N, int(good))
	s.Equal(0, int(bad))
	s.Contains(DebugEvents(), "moved")
	// s.Contains(DebugEvents(), "addNode") // could not be reliably triggered :-(
	// s.Contains(DebugEvents(), "asking") // could not be reliably triggered either :-(
}

func (s *Suite) TestAllReturns_Bad() {
	s.ctxcancel()
	s.ctx, s.ctxcancel = context.WithTimeout(context.Background(), 10*time.Minute)
	DebugDisable = true

	cl, err := NewCluster(s.ctx, []string{"127.0.0.1:43210"}, clustopts)
	s.r().Nil(err)
	defer cl.Close()

	sconn := redis.SyncCtx{cl.WithPolicy(MasterAndSlaves)}

	s.fillMany(sconn, "allbad")

	const N = 200
	fin := make(chan struct{})
	goods := make([]chan bool, N)
	checks := make(chan bool, N)
	finch := make(chan struct{}, N)

	for i := 0; i < N; i++ {
		goods[i] = make(chan bool, 1)
		go func(i int) {
			check := true
		Loop:
			for j := 0; ; j++ {
				select {
				case <-goods[i]:
					check = true
				case <-fin:
					break Loop
				case <-s.ctx.Done():
					break Loop
				default:
				}

				skey := s.keys[(i*N+j)*127%NumSlots]
				key := slotkey("allbad", skey)
				res := sconn.Do(s.ctx, "GET", key)

				keya := slotkey("allbad", skey, "a")
				keyb := slotkey("allbad", skey, "b")
				z := i*53 + j*51
				z ^= z >> 8
				reverse := z&1 == 0
				transact := z&2 == 0
				if reverse {
					keya, keyb = keyb, keya
				}
				reqs := []redis.Request{
					redis.Req("SET", keya, keyb),
					redis.Req("GET", keyb),
				}
				var ress []interface{}
				var err error
				if !transact {
					ress = sconn.SendMany(s.ctx, reqs)
				} else {
					ress, err = sconn.SendTransaction(s.ctx, reqs)
				}
				if check {
					ok := s.Equal([]byte(skey), res)
					ok = ok && err == nil
					ok = ok && s.Equal("OK", ress[0])
					if ress[1] != nil {
						ok = ok && s.Equal([]byte(keya), ress[1])
					}
					checks <- ok
				}
				check = false
				runtime.Gosched()
			}
			finch <- struct{}{}
		}(i)
	}

	isAllGood := true
	sendgoods := func() bool {
		for i := 0; i < N; i++ {
			select {
			case <-s.ctx.Done():
				isAllGood = false
				return false
			case goods[i] <- true:
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
	for k := 0; k < 3*len(s.cl.Node); k++ {
		n := k % len(s.cl.Node)
		node := &s.cl.Node[n]

		if !allgood() {
			break
		}

		node.Stop()
		s.cl.WaitClusterOk()
		time.Sleep(clustopts.CheckInterval * 2)
		if !sendgoods() || !allgood() {
			break
		}

		node.Start()
		s.cl.WaitClusterOk()
		time.Sleep(clustopts.CheckInterval * 2)
		if !sendgoods() || !allgood() {
			break
		}

		node.Pause()
		s.cl.WaitClusterOk()
		time.Sleep(clustopts.CheckInterval * 2)
		if !sendgoods() || !allgood() {
			break
		}

		node.Resume()
		s.cl.WaitClusterOk()
		time.Sleep(clustopts.CheckInterval * 2)
		if !sendgoods() {
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
