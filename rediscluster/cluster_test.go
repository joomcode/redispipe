package rediscluster_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/joomcode/redispipe/redis"
	. "github.com/joomcode/redispipe/rediscluster"
	"github.com/joomcode/redispipe/redisconn"
	"github.com/joomcode/redispipe/testbed"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

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
		slot := Slot(key)
		if s.keys[slot] == "" {
			s.keys[slot] = key
			cnt++
		}
	}
}

func (s *Suite) SetupTest() {
	s.cl.Start()
	s.ctx, s.ctxcancel = context.WithTimeout(context.Background(), 5*time.Second)
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

func (s *Suite) AsError(v interface{}) *redis.Error {
	s.r().IsType((*redis.Error)(nil), v)
	return v.(*redis.Error)
}

var defopts = redisconn.Opts{
	IOTimeout: 10 * time.Millisecond,
}

var clustopts = Opts{
	HostOpts: defopts,
	Name:     "default",
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
