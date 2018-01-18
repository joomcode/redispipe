package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/joomcode/redispipe/redis"
)

func Error(v interface{}) error {
	e, _ := v.(error)
	return e
}

func RedisError(v interface{}) *redis.Error {
	e, _ := v.(*redis.Error)
	if e == nil {
		if _, ok := v.(error); ok {
			panic(fmt.Errorf("result should be either *rediserror.Error, or not error at all, but got %#v", v))
		}
	}
	return e
}

func Read(b *bufio.Reader) interface{} {
	line, isPrefix, err := b.ReadLine()
	if err != nil {
		return redis.NewErrWrap(redis.ErrKindIO, redis.ErrIO, err)
	}

	if isPrefix {
		return redis.NewErr(redis.ErrKindResponse, redis.ErrHeaderlineTooLarge).With("line", line)
	}

	if len(line) == 0 {
		return redis.NewErr(redis.ErrKindResponse, redis.ErrHeaderlineEmpty)
	}

	var v int64
	switch line[0] {
	case '+':
		return string(line[1:])
	case '-':
		// detect MOVED and ASK
		txt := string(line[1:])
		moved := strings.HasPrefix(txt, "MOVED ")
		ask := strings.HasPrefix(txt, "ASK ")
		if moved || ask {
			parts := bytes.Split(line, []byte(" "))
			if len(parts) < 3 {
				return redis.NewErr(redis.ErrKindResponse, redis.ErrResponseFormat).With("line", line)
			}
			slot, err := parseInt(parts[1])
			if err != nil {
				return redis.NewErr(redis.ErrKindResponse, redis.ErrResponseFormat).With("line", line)
			}
			if moved {
				return redis.NewErrMsg(redis.ErrKindResult, redis.ErrMoved, txt).
					With("addr", string(parts[2])).With("slot", slot)
			} else {
				return redis.NewErrMsg(redis.ErrKindResult, redis.ErrAsk, txt).
					With("addr", string(parts[2])).With("slot", slot)
			}
		}
		if strings.HasPrefix(txt, "LOADING") {
			return redis.NewErrMsg(redis.ErrKindResult, redis.ErrLoading, txt)
		}
		return redis.NewErrMsg(redis.ErrKindResult, redis.ErrResult, txt)
	case ':':
		if v, err = parseInt(line[1:]); err != nil {
			return err
		} else {
			return v
		}
	case '$':
		if v, err = parseInt(line[1:]); err != nil {
			return err
		}
		if v < 0 {
			return nil
		}
		buf := make([]byte, v+2, v+2)
		if _, err = io.ReadFull(b, buf); err != nil {
			return redis.NewErrWrap(redis.ErrKindIO, redis.ErrIO, err)
		}
		if buf[v] != '\r' || buf[v+1] != '\n' {
			return redis.NewErr(redis.ErrKindResponse, redis.ErrNoFinalRN)
		}
		return buf[:v:v]
	case '*':
		if v, err = parseInt(line[1:]); err != nil {
			return err
		}
		if v < 0 {
			return nil
		}
		result := make([]interface{}, v, v)
		for i := int64(0); i < v; i++ {
			result[i] = Read(b)
			if e, ok := result[i].(*redis.Error); ok && e.Kind == redis.ErrKindIO {
				return e
			}
		}
		return result
	default:
		return redis.NewErr(redis.ErrKindResponse, redis.ErrUnknownHeaderType)
	}
}

func parseInt(buf []byte) (int64, error) {
	neg := buf[0] == '-'
	if neg {
		buf = buf[1:]
	}
	v := int64(0)
	for _, b := range buf {
		if b < '0' || b > '9' {
			return 0, redis.NewErr(redis.ErrKindResponse, redis.ErrIntegerParsing)
		}
		v *= 10
		v += int64(b - '0')
	}
	if neg {
		v = -v
	}
	return v, nil
}
