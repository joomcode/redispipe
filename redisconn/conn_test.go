package redisconn_test

import (
	"context"
	"runtime"
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
}

func (s *Suite) SetupSuite() {
	testbed.InitDir(".")
	s.s.Port = 45678
	s.s.Start()
}

func (s *Suite) SetupTest() {
	s.s.Start()
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

func (s *Suite) goodPing(conn *Connection) {
	req := redis.Sync{conn}.Do("PING")
	s.Equal("PONG", req)
}

func (s *Suite) badPing(conn *Connection, kind redis.ErrorKind, code redis.ErrorCode) {
	req := redis.Sync{conn}.Do("PING")
	s.r().IsType((*redis.Error)(nil), req)
	rerr := s.AsError(req)
	s.Equal(kind, rerr.Kind)
	s.Equal(code, rerr.Code)
}

func (s *Suite) waitReconnect(conn *Connection) {
	now := time.Now()
	for {
		at := time.Now()
		res := redis.Sync{conn}.Do("PING")
		if rerr := redis.AsRedisError(res); rerr != nil {
			s.Equal(redis.ErrKindConnection, rerr.Kind)
			s.Equal(redis.ErrNotConnected, rerr.Code)
			s.r().WithinDuration(now, at, defopts.IOTimeout*2)
		} else {
			s.Equal("PONG", res)
			s.r().WithinDuration(now, at, defopts.IOTimeout*3)
			break
		}
		runtime.Gosched()
	}
}

func TestConn(t *testing.T) {
	suite.Run(t, new(Suite))
}

func (s *Suite) TestConnects() {
	conn, err := Connect(context.TODO(), s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()
	s.goodPing(conn)
}

func (s *Suite) TestStopped_DoesntConnectWithNegativeReconnectPause() {
	s.s.Stop()
	opts := defopts
	opts.ReconnectPause = -1
	_, err := Connect(context.TODO(), s.s.Addr(), opts)
	s.r().NotNil(err)
	rerr := s.AsError(err)
	s.Equal(redis.ErrKindConnection, rerr.Kind)
	s.Equal(redis.ErrDial, rerr.Code)
}

func (s *Suite) TestStopped_Reconnects() {
	s.s.Stop()

	conn, err := Connect(context.TODO(), s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	s.badPing(conn, redis.ErrKindConnection, redis.ErrNotConnected)

	s.s.Start()
	s.waitReconnect(conn)

	s.s.Stop()
	s.badPing(conn, redis.ErrKindConnection, redis.ErrNotConnected)

	s.s.Start()
	s.waitReconnect(conn)
}

func (s *Suite) TestStopped_Reconnects2() {
	conn, err := Connect(context.TODO(), s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	s.goodPing(conn)

	s.s.Stop()
	s.badPing(conn, redis.ErrKindConnection, redis.ErrNotConnected)

	s.s.Start()
	s.waitReconnect(conn)

	s.s.Stop()
	s.badPing(conn, redis.ErrKindConnection, redis.ErrNotConnected)

	s.s.Start()
	s.waitReconnect(conn)
}

func (s *Suite) TestTimeout() {
	conn, err := Connect(context.TODO(), s.s.Addr(), defopts)
	s.r().Nil(err)
	defer conn.Close()

	s.goodPing(conn)

	s.s.Pause()
	s.badPing(conn, redis.ErrKindIO, redis.ErrIO)
	s.badPing(conn, redis.ErrKindConnection, redis.ErrNotConnected)

	s.s.Resume()
	s.waitReconnect(conn)
}
