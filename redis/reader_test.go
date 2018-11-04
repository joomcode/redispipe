package redis_test

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	. "github.com/joomcode/redispipe/redis"
	"github.com/stretchr/testify/assert"
)

func lines2bufio(lines ...string) *bufio.Reader {
	buf := strings.Join(lines, "")
	return bufio.NewReader(strings.NewReader(buf))
}

func readLines(lines ...string) interface{} {
	return ReadResponse(lines2bufio(lines...))
}

func checkErr(t *testing.T, res interface{}, kind ErrorKind) bool {
	if assert.IsType(t, (*Error)(nil), res) {
		err := res.(*Error)
		return assert.Equal(t, kind, err.Kind())
	}
	return false
}

func TestReadResponse_IOAndFormatErrors(t *testing.T) {
	var res interface{}

	res = readLines("")
	checkErr(t, res, ErrIO)

	res = readLines("\n")
	checkErr(t, res, ErrHeaderlineEmpty)

	res = readLines("\r\n")
	checkErr(t, res, ErrHeaderlineEmpty)

	res = readLines("$\r\n")
	checkErr(t, res, ErrIntegerParsing)

	res = readLines("/\r\n")
	checkErr(t, res, ErrUnknownHeaderType)

	res = readLines("+" + strings.Repeat("A", 1024*1024) + "\r\n")
	checkErr(t, res, ErrHeaderlineTooLarge)

	res = readLines(":\r\n")
	checkErr(t, res, ErrIntegerParsing)

	res = readLines(":1.1\r\n")
	checkErr(t, res, ErrIntegerParsing)

	res = readLines(":a\r\n")
	checkErr(t, res, ErrIntegerParsing)

	res = readLines("$a\r\n")
	checkErr(t, res, ErrIntegerParsing)

	res = readLines("*a\r\n")
	checkErr(t, res, ErrIntegerParsing)

	res = readLines("$0\r\n")
	checkErr(t, res, ErrIO)

	res = readLines("$1\r\n")
	checkErr(t, res, ErrIO)

	res = readLines("$1\r\na")
	checkErr(t, res, ErrIO)

	res = readLines("$1\r\nabc")
	checkErr(t, res, ErrNoFinalRN)

	res = readLines("*1\r\n")
	checkErr(t, res, ErrIO)

	res = readLines("*1\r\n$1\r\n")
	checkErr(t, res, ErrIO)

	res = readLines("*1\r\n$1\r\nabc")
	checkErr(t, res, ErrNoFinalRN)

	res = readLines("-MOVED 1234\r\n")
	checkErr(t, res, ErrResponseFormat)

	res = readLines("-MOVED asdf 1.1.1.1:3456\r\n")
	checkErr(t, res, ErrIntegerParsing)

	res = readLines("-ASK 1234\r\n")
	checkErr(t, res, ErrResponseFormat)

	res = readLines("-ASK asdf 1.1.1.1:3456\r\n")
	checkErr(t, res, ErrIntegerParsing)
}

func TestReadResponse_Correct(t *testing.T) {
	var res interface{}

	res = readLines("+\r\n")
	assert.Equal(t, "", res)

	res = readLines("+asdf\r\n")
	assert.Equal(t, "asdf", res)

	res = readLines("-\r\n")
	if checkErr(t, res, ErrResult) {
		assert.Equal(t, "", res.(*Error).Msg())
	}

	res = readLines("-asdf\r\n")
	if checkErr(t, res, ErrResult) {
		assert.Equal(t, "asdf", res.(*Error).Msg())
	}

	res = readLines("-MOVED 1234 1.1.1.1:3456\r\n")
	if checkErr(t, res, ErrMoved) {
		err := res.(*Error)
		assert.Equal(t, "MOVED 1234 1.1.1.1:3456", err.Msg())
		assert.Equal(t, "1.1.1.1:3456", err.Get(EKMovedTo))
		assert.Equal(t, int64(1234), err.Get(EKSlot))
	}

	res = readLines("-ASK 1234 1.1.1.1:3456\r\n")
	if checkErr(t, res, ErrAsk) {
		err := res.(*Error)
		assert.Equal(t, "ASK 1234 1.1.1.1:3456", err.Msg())
		assert.Equal(t, "1.1.1.1:3456", err.Get(EKMovedTo))
		assert.Equal(t, int64(1234), err.Get(EKSlot))
	}

	res = readLines("-LOADING\r\n")
	if checkErr(t, res, ErrLoading) {
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
