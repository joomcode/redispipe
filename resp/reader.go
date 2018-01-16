package resp

import (
	"bufio"
	"bytes"
	"io"
	"strings"

	re "github.com/joomcode/redispipe/rediserror"
)

func Error(v interface{}) error {
	e, _ := v.(error)
	return e
}

func RedisError(v interface{}) *re.Error {
	e, _ := v.(*re.Error)
	if e == nil {
		if _, ok := v.(error); ok {
			panic("result should be either *rediserror.Error, or not error at all")
		}
	}
	return e
}

func Read(b *bufio.Reader) interface{} {
	line, isPrefix, err := b.ReadLine()
	if err != nil {
		return re.NewWrap(re.ErrKindIO, re.ErrIO, err)
	}

	if isPrefix {
		return re.New(re.ErrKindResponse, re.ErrHeaderlineTooLarge).With("line", line)
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
				return re.New(re.ErrKindResponse, re.ErrResponseFormat).With("line", line)
			}
			slot, err := parseInt(parts[1])
			if err != nil {
				return re.New(re.ErrKindResponse, re.ErrResponseFormat).With("line", line)
			}
			if moved {
				return re.NewMsg(re.ErrKindResult, re.ErrMoved, txt).
					With("addr", string(parts[2])).With("slot", slot)
			} else {
				return re.NewMsg(re.ErrKindResult, re.ErrAsk, txt).
					With("addr", string(parts[2])).With("slot", slot)
			}
		}
		if strings.HasPrefix(txt, "LOADING") {
			return re.NewMsg(re.ErrKindResult, re.ErrLoading, txt)
		}
		return re.NewMsg(re.ErrKindResult, re.ErrResult, txt)
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
			return re.NewWrap(re.ErrKindIO, re.ErrIO, err)
		}
		if buf[v] != '\r' || buf[v+1] != '\n' {
			return re.NewMsg(re.ErrKindResponse, re.ErrNoFinalRN, "no final \\r\\n")
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
			if e, ok := result[i].(*re.Error); ok && e.Kind == re.ErrKindIO {
				return e
			}
		}
		return result
	default:
		return re.New(re.ErrKindResponse, re.ErrUnknownHeaderType)
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
			return 0, re.New(re.ErrKindResponse, re.ErrIntegerParsing)
		}
		v *= 10
		v += int64(b - '0')
	}
	if neg {
		v = -v
	}
	return v, nil
}
