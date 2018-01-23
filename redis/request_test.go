package redis_test

import (
	"testing"

	. "github.com/joomcode/redispipe/redis"
	"github.com/stretchr/testify/assert"
)

func TestRequestKey(t *testing.T) {
	var k string
	var ok bool

	k, ok = Req("GET", 1).Key()
	assert.Equal(t, "1", k)
	assert.True(t, ok)

	k, ok = Req("GET").Key()
	assert.False(t, ok)

	k, ok = Req("SET", 1, 2).Key()
	assert.Equal(t, "1", k)
	assert.True(t, ok)

	k, ok = Req("RANDOMKEY").Key()
	assert.Equal(t, "RANDOMKEY", k)
	assert.False(t, ok)

	k, ok = Req("EVAL", 1, 2, 3).Key()
	assert.Equal(t, "2", k)
	assert.True(t, ok)

	k, ok = Req("EVALSHA", 1, 2, 3).Key()
	assert.Equal(t, "2", k)
	assert.True(t, ok)

	k, ok = Req("BITOP", "AND", 1, 2).Key()
	assert.Equal(t, "1", k)
	assert.True(t, ok)
}

func TestArgToString(t *testing.T) {
	var k string
	var ok bool

	k, ok = ArgToString(int(0))
	assert.Equal(t, "0", k)
	assert.True(t, ok)

	k, ok = ArgToString(uint(1))
	assert.Equal(t, "1", k)
	assert.True(t, ok)

	k, ok = ArgToString(int8(6))
	assert.Equal(t, "6", k)
	assert.True(t, ok)

	k, ok = ArgToString(int8(-31))
	assert.Equal(t, "-31", k)
	assert.True(t, ok)

	k, ok = ArgToString(uint8(156))
	assert.Equal(t, "156", k)
	assert.True(t, ok)

	k, ok = ArgToString(int16(781))
	assert.Equal(t, "781", k)
	assert.True(t, ok)

	k, ok = ArgToString(int16(-3906))
	assert.Equal(t, "-3906", k)
	assert.True(t, ok)

	k, ok = ArgToString(uint16(19351))
	assert.Equal(t, "19351", k)
	assert.True(t, ok)

	k, ok = ArgToString(int32(97656))
	assert.Equal(t, "97656", k)
	assert.True(t, ok)

	k, ok = ArgToString(int32(-488281))
	assert.Equal(t, "-488281", k)
	assert.True(t, ok)

	k, ok = ArgToString(uint32(2441406))
	assert.Equal(t, "2441406", k)
	assert.True(t, ok)

	k, ok = ArgToString(int64(12207031))
	assert.Equal(t, "12207031", k)
	assert.True(t, ok)

	k, ok = ArgToString(int64(-61035156))
	assert.Equal(t, "-61035156", k)
	assert.True(t, ok)

	k, ok = ArgToString(uint64(305175781))
	assert.Equal(t, "305175781", k)
	assert.True(t, ok)

	k, ok = ArgToString(int64(9223372036854775807))
	assert.Equal(t, "9223372036854775807", k)
	assert.True(t, ok)

	k, ok = ArgToString(int64(-9223372036854775808))
	assert.Equal(t, "-9223372036854775808", k)
	assert.True(t, ok)

	k, ok = ArgToString(uint64(18446744073709551615))
	assert.Equal(t, "18446744073709551615", k)
	assert.True(t, ok)

	k, ok = ArgToString(float32(0.0))
	assert.Equal(t, "0", k)
	assert.True(t, ok)

	k, ok = ArgToString(float32(0.25))
	assert.Equal(t, "0.25", k)
	assert.True(t, ok)

	k, ok = ArgToString(float32(-10000.25))
	assert.Equal(t, "-10000.25", k)
	assert.True(t, ok)

	k, ok = ArgToString(float64(0.0))
	assert.Equal(t, "0", k)
	assert.True(t, ok)

	k, ok = ArgToString(float64(0.25))
	assert.Equal(t, "0.25", k)
	assert.True(t, ok)

	k, ok = ArgToString(float64(-10000.25))
	assert.Equal(t, "-10000.25", k)
	assert.True(t, ok)

	k, ok = ArgToString(true)
	assert.Equal(t, "1", k)
	assert.True(t, ok)

	k, ok = ArgToString(false)
	assert.Equal(t, "0", k)
	assert.True(t, ok)

	k, ok = ArgToString(nil)
	assert.Equal(t, "", k)
	assert.True(t, ok)

	k, ok = ArgToString("asdf")
	assert.Equal(t, "asdf", k)
	assert.True(t, ok)

	k, ok = ArgToString([]byte("asdf"))
	assert.Equal(t, "asdf", k)
	assert.True(t, ok)

	k, ok = ArgToString(make(chan int))
	assert.Equal(t, "", k)
	assert.False(t, ok)
}

func TestAppendRequestArgument(t *testing.T) {
	var k []byte
	var err *Error

	k, err = AppendRequest(nil, Req("CMD", int(0)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$1\r\n0\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", uint(1)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$1\r\n1\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int8(6)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$1\r\n6\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int8(-31)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$3\r\n-31\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", uint8(156)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$3\r\n156\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int16(781)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$3\r\n781\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int16(-3906)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$5\r\n-3906\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", uint16(19351)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$5\r\n19351\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int32(97656)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$5\r\n97656\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int32(-488281)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$7\r\n-488281\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", uint32(2441406)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$7\r\n2441406\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int64(12207031)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$8\r\n12207031\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int64(-61035156)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$9\r\n-61035156\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", uint64(305175781)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$9\r\n305175781\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int64(9223372036854775807)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$19\r\n9223372036854775807\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", int64(-9223372036854775808)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$20\r\n-9223372036854775808\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", uint64(18446744073709551615)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$20\r\n18446744073709551615\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", float32(0.0)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$1\r\n0\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", float32(0.25)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$4\r\n0.25\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", float32(-10000.25)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$9\r\n-10000.25\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", float64(0.0)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$1\r\n0\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", float64(0.25)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$4\r\n0.25\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", float64(-10000.25)))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$9\r\n-10000.25\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", true))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$1\r\n1\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", false))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$1\r\n0\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", nil))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$0\r\n\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", "asdf"))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$4\r\nasdf\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", []byte("asdf")))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$4\r\nasdf\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", make(chan int)))
	assert.Nil(t, k)
	assert.NotNil(t, err)
	assert.Equal(t, uint32(ErrKindRequest), err.Kind)
	assert.Equal(t, uint32(ErrArgumentType), err.Code)
}
