package redis_test

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"

	. "github.com/joomcode/redispipe/redis"
	"github.com/stretchr/testify/assert"
)

func lines2bufio(lines ...string) *bufio.Reader {
	buf := []byte(strings.Join(lines, ""))
	return bufio.NewReader(bytes.NewReader(buf))
}

func readLines(lines ...string) interface{} {
	return ReadResponse(lines2bufio(lines...))
}

func checkErr(t *testing.T, res interface{}, kind ErrorKind, code ErrorCode) bool {
	if assert.IsType(t, (*Error)(nil), res) {
		err := res.(*Error)
		ok := assert.Equal(t, kind, err.Kind)
		ok = ok && assert.Equal(t, code, err.Code)
		return ok
	}
	return false
}

func TestReadResponse_IOAndFormatErrors(t *testing.T) {
	var res interface{}

	res = readLines("")
	checkErr(t, res, ErrKindIO, ErrIO)

	res = readLines("\n")
	checkErr(t, res, ErrKindResponse, ErrHeaderlineEmpty)

	res = readLines("\r\n")
	checkErr(t, res, ErrKindResponse, ErrHeaderlineEmpty)

	res = readLines("$\r\n")
	checkErr(t, res, ErrKindResponse, ErrIntegerParsing)

	res = readLines("/\r\n")
	checkErr(t, res, ErrKindResponse, ErrUnknownHeaderType)

	res = readLines("+" + strings.Repeat("A", 1024*1024) + "\r\n")
	checkErr(t, res, ErrKindResponse, ErrHeaderlineTooLarge)

	res = readLines(":\r\n")
	checkErr(t, res, ErrKindResponse, ErrIntegerParsing)

	res = readLines(":1.1\r\n")
	checkErr(t, res, ErrKindResponse, ErrIntegerParsing)

	res = readLines(":a\r\n")
	checkErr(t, res, ErrKindResponse, ErrIntegerParsing)

	res = readLines("$a\r\n")
	checkErr(t, res, ErrKindResponse, ErrIntegerParsing)

	res = readLines("*a\r\n")
	checkErr(t, res, ErrKindResponse, ErrIntegerParsing)

	res = readLines("$0\r\n")
	checkErr(t, res, ErrKindIO, ErrIO)

	res = readLines("$1\r\n")
	checkErr(t, res, ErrKindIO, ErrIO)

	res = readLines("$1\r\na")
	checkErr(t, res, ErrKindIO, ErrIO)

	res = readLines("$1\r\nabc")
	checkErr(t, res, ErrKindResponse, ErrNoFinalRN)

	res = readLines("*1\r\n")
	checkErr(t, res, ErrKindIO, ErrIO)

	res = readLines("*1\r\n$1\r\n")
	checkErr(t, res, ErrKindIO, ErrIO)

	res = readLines("*1\r\n$1\r\nabc")
	checkErr(t, res, ErrKindResponse, ErrNoFinalRN)

	res = readLines("-MOVED 1234\r\n")
	checkErr(t, res, ErrKindResponse, ErrResponseFormat)

	res = readLines("-MOVED asdf 1.1.1.1:3456\r\n")
	checkErr(t, res, ErrKindResponse, ErrResponseFormat)

	res = readLines("-ASK 1234\r\n")
	checkErr(t, res, ErrKindResponse, ErrResponseFormat)

	res = readLines("-ASK asdf 1.1.1.1:3456\r\n")
	checkErr(t, res, ErrKindResponse, ErrResponseFormat)
}

func TestReadResponse_Correct(t *testing.T) {
	var res interface{}

	res = readLines("+\r\n")
	assert.Equal(t, "", res)

	res = readLines("+asdf\r\n")
	assert.Equal(t, "asdf", res)

	res = readLines("-\r\n")
	if checkErr(t, res, ErrKindResult, ErrResult) {
		assert.Equal(t, "", res.(*Error).Msg())
	}

	res = readLines("-asdf\r\n")
	if checkErr(t, res, ErrKindResult, ErrResult) {
		assert.Equal(t, "asdf", res.(*Error).Msg())
	}

	res = readLines("-MOVED 1234 1.1.1.1:3456\r\n")
	if checkErr(t, res, ErrKindResult, ErrMoved) {
		err := res.(*Error)
		assert.Equal(t, "MOVED 1234 1.1.1.1:3456", err.Msg())
		assert.Equal(t, "1.1.1.1:3456", err.Get("movedto"))
		assert.Equal(t, int64(1234), err.Get("slot"))
	}

	res = readLines("-ASK 1234 1.1.1.1:3456\r\n")
	if checkErr(t, res, ErrKindResult, ErrAsk) {
		err := res.(*Error)
		assert.Equal(t, "ASK 1234 1.1.1.1:3456", err.Msg())
		assert.Equal(t, "1.1.1.1:3456", err.Get("movedto"))
		assert.Equal(t, int64(1234), err.Get("slot"))
	}

	res = readLines("-LOADING\r\n")
	if checkErr(t, res, ErrKindResult, ErrLoading) {
		err := res.(*Error)
		assert.Equal(t, "LOADING", err.Msg())
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
