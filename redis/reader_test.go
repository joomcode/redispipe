package redis_test

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	"github.com/joomcode/errorx"

	. "github.com/joomcode/redispipe/redis"
	"github.com/stretchr/testify/assert"
)

func lines2bufio(lines ...string) *bufio.Reader {
	buf := strings.Join(lines, "")
	return bufio.NewReader(strings.NewReader(buf))
}

func readLines(lines ...string) interface{} {
	r, _ := ReadResponse(lines2bufio(lines...))
	return r
}

func checkErrType(t *testing.T, res interface{}, kind *errorx.Type) bool {
	if assert.IsType(t, (*errorx.Error)(nil), res) {
		err := res.(*errorx.Error)
		return assert.True(t, err.IsOfType(kind))
	}
	return false
}

func TestReadResponse_IOAndFormatErrors(t *testing.T) {
	var res interface{}

	res = readLines("")
	checkErrType(t, res, ErrIO)

	res = readLines("\n")
	checkErrType(t, res, ErrHeaderlineEmpty)

	res = readLines("\r\n")
	checkErrType(t, res, ErrHeaderlineEmpty)

	res = readLines("$\r\n")
	checkErrType(t, res, ErrIntegerParsing)

	res = readLines("/\r\n")
	checkErrType(t, res, ErrUnknownHeaderType)

	res = readLines("+" + strings.Repeat("A", 1024*1024) + "\r\n")
	checkErrType(t, res, ErrHeaderlineTooLarge)

	res = readLines(":\r\n")
	checkErrType(t, res, ErrIntegerParsing)

	res = readLines(":1.1\r\n")
	checkErrType(t, res, ErrIntegerParsing)

	res = readLines(":a\r\n")
	checkErrType(t, res, ErrIntegerParsing)

	res = readLines("$a\r\n")
	checkErrType(t, res, ErrIntegerParsing)

	res = readLines("*a\r\n")
	checkErrType(t, res, ErrIntegerParsing)

	res = readLines("$0\r\n")
	checkErrType(t, res, ErrIO)

	res = readLines("$1\r\n")
	checkErrType(t, res, ErrIO)

	res = readLines("$1\r\na")
	checkErrType(t, res, ErrIO)

	res = readLines("$1\r\nabc")
	checkErrType(t, res, ErrNoFinalRN)

	res = readLines("*1\r\n")
	checkErrType(t, res, ErrIO)

	res = readLines("*1\r\n$1\r\n")
	checkErrType(t, res, ErrIO)

	res = readLines("*1\r\n$1\r\nabc")
	checkErrType(t, res, ErrNoFinalRN)

	res = readLines("-MOVED 1234\r\n")
	checkErrType(t, res, ErrResponseFormat)

	res = readLines("-MOVED asdf 1.1.1.1:3456\r\n")
	checkErrType(t, res, ErrIntegerParsing)

	res = readLines("-ASK 1234\r\n")
	checkErrType(t, res, ErrResponseFormat)

	res = readLines("-ASK asdf 1.1.1.1:3456\r\n")
	checkErrType(t, res, ErrIntegerParsing)
}

func TestReadResponse_Correct(t *testing.T) {
	var res interface{}

	res = readLines("+\r\n")
	assert.Equal(t, "", res)

	res = readLines("+asdf\r\n")
	assert.Equal(t, "asdf", res)

	res = readLines("-\r\n")
	if checkErrType(t, res, ErrResult) {
		assert.Equal(t, "", res.(*errorx.Error).Message())
	}

	res = readLines("-asdf\r\n")
	if checkErrType(t, res, ErrResult) {
		assert.Equal(t, "asdf", res.(*errorx.Error).Message())
	}

	res = readLines("-MOVED 1234 1.1.1.1:3456\r\n")
	if checkErrType(t, res, ErrMoved) {
		err := res.(*errorx.Error)
		assert.Equal(t, "MOVED 1234 1.1.1.1:3456", err.Message())
		v, _ := err.Property(EKMovedTo)
		assert.Equal(t, "1.1.1.1:3456", v)
		v, _ = err.Property(EKSlot)
		assert.Equal(t, int64(1234), v)
	}

	res = readLines("-ASK 1234 1.1.1.1:3456\r\n")
	if checkErrType(t, res, ErrAsk) {
		err := res.(*errorx.Error)
		assert.Equal(t, "ASK 1234 1.1.1.1:3456", err.Message())
		v, _ := err.Property(EKMovedTo)
		assert.Equal(t, "1.1.1.1:3456", v)
		v, _ = err.Property(EKSlot)
		assert.Equal(t, int64(1234), v)
	}

	res = readLines("-LOADING\r\n")
	if checkErrType(t, res, ErrLoading) {
		err := res.(*errorx.Error)
		assert.Equal(t, "LOADING", err.Message())
	}

	for i := -1000; i <= 1000; i++ {
		res = readLines(fmt.Sprintf(":%d\r\n", i))
		assert.Equal(t, int64(i), res)
	}

	res = readLines(":9223372036854775807\r\n")
	assert.Equal(t, int64(9223372036854775807), res)

	res = readLines(":-9223372036854775808\r\n")
	assert.Equal(t, int64(-9223372036854775808), res)

	res = readLines("$0\r\n", "\r\n")
	assert.Equal(t, []byte(""), res)
	assert.Equal(t, len(res.([]byte)), cap(res.([]byte)))

	res = readLines("$1\r\n", "a\r\n")
	assert.Equal(t, []byte("a"), res)
	assert.Equal(t, len(res.([]byte)), cap(res.([]byte)))

	res = readLines("$4\r\n", "asdf\r\n")
	assert.Equal(t, []byte("asdf"), res)
	assert.Equal(t, len(res.([]byte)), cap(res.([]byte)))

	big := strings.Repeat("a", 1024*1024)
	res = readLines(fmt.Sprintf("$%d\r\n", len(big)), big, "\r\n")
	assert.Equal(t, []byte(big), res)
	assert.Equal(t, len(res.([]byte)), cap(res.([]byte)))

	res = readLines("*0\r\n")
	assert.Equal(t, []interface{}{}, res)

	res = readLines("*1\r\n", "+OK\r\n")
	assert.Equal(t, []interface{}{"OK"}, res)

	res = readLines("*2\r\n", "+OK\r\n", "*2\r\n", ":1\r\n", "+OK\r\n")
	assert.Equal(t, []interface{}{"OK", []interface{}{int64(1), "OK"}}, res)

	res = readLines("$-1\r\n")
	assert.Nil(t, res)

	res = readLines("*-1\r\n")
	assert.Nil(t, res)
}
