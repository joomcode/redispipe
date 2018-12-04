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

	k, ok = Req("EVAL", "return KEY[1]", 1, 2, 3).Key()
	assert.Equal(t, "2", k)
	assert.True(t, ok)

	k, ok = Req("EVALSHA", "1234abcdef", 1, 2, 3).Key()
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

	k, ok = ArgToString(uint(0))
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
	var err error

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

	k, err = AppendRequest(nil, Req("CMD", ""))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$0\r\n\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", "asdf"))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$4\r\nasdf\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", "abcdefghijklmnopqrstuvwxyz"))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$26\r\nabcdefghijklmnopqrstuvwxyz\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", []byte("asdf")))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$4\r\nasdf\r\n"), k)
	assert.Nil(t, err)

	big := make([]byte, 12345)
	k, err = AppendRequest(nil, Req("CMD", big))
	res := []byte("*2\r\n$3\r\nCMD\r\n$12345\r\n")
	res = append(append(res, big...), "\r\n"...)
	assert.Equal(t, res, k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", make(chan int)))
	assert.Len(t, k, 0)
	assert.NotNil(t, err)
	rerr := AsErrorx(err)
	assert.True(t, rerr.IsOfType(ErrArgumentType))
}

func TestAppendRequestCmdAndArgcount(t *testing.T) {
	var k []byte
	var err error

	k, err = AppendRequest(nil, Req("CMD", "hi"))
	assert.Equal(t, []byte("*2\r\n$3\r\nCMD\r\n$2\r\nhi\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", "hi", "ho"))
	assert.Equal(t, []byte("*3\r\n$3\r\nCMD\r\n$2\r\nhi\r\n$2\r\nho\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD", "hi", "ho", "hu"))
	assert.Equal(t, []byte("*4\r\n$3\r\nCMD\r\n$2\r\nhi\r\n$2\r\nho\r\n$2\r\nhu\r\n"), k)
	assert.Nil(t, err)

	// split by first space
	k, err = AppendRequest(nil, Req("CMD ONE", "hi"))
	assert.Equal(t, []byte("*3\r\n$3\r\nCMD\r\n$3\r\nONE\r\n$2\r\nhi\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD ONE", "hi", "ho"))
	assert.Equal(t, []byte("*4\r\n$3\r\nCMD\r\n$3\r\nONE\r\n$2\r\nhi\r\n$2\r\nho\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD ONE", "hi", "ho", "hu"))
	assert.Equal(t, []byte("*5\r\n$3\r\nCMD\r\n$3\r\nONE\r\n$2\r\nhi\r\n$2\r\nho\r\n$2\r\nhu\r\n"), k)
	assert.Nil(t, err)

	// no split by second space
	k, err = AppendRequest(nil, Req("CMD ONE TWO", "hi"))
	assert.Equal(t, []byte("*3\r\n$3\r\nCMD\r\n$7\r\nONE TWO\r\n$2\r\nhi\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD ONE TWO", "hi", "ho"))
	assert.Equal(t, []byte("*4\r\n$3\r\nCMD\r\n$7\r\nONE TWO\r\n$2\r\nhi\r\n$2\r\nho\r\n"), k)
	assert.Nil(t, err)

	k, err = AppendRequest(nil, Req("CMD ONE TWO", "hi", "ho", "hu"))
	assert.Equal(t, []byte("*5\r\n$3\r\nCMD\r\n$7\r\nONE TWO\r\n$2\r\nhi\r\n$2\r\nho\r\n$2\r\nhu\r\n"), k)
	assert.Nil(t, err)
}
