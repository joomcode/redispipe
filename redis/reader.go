package redis

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

// ReadResponse reads single RESP answer from bufio.Reader
func ReadResponse(b *bufio.Reader) interface{} {
	line, isPrefix, err := b.ReadLine()
	if err != nil {
		return NewErrWrap(ErrKindIO, ErrIO, err)
	}

	if isPrefix {
		return NewErr(ErrKindResponse, ErrHeaderlineTooLarge).With("line", line)
	}

	if len(line) == 0 {
		return NewErr(ErrKindResponse, ErrHeaderlineEmpty)
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
				return NewErr(ErrKindResponse, ErrResponseFormat).With("line", line)
			}
			slot, err := parseInt(parts[1])
			if err != nil {
				return NewErr(ErrKindResponse, ErrResponseFormat).With("line", line)
			}
			if moved {
				return NewErrMsg(ErrKindResult, ErrMoved, txt).
					With("movedto", string(parts[2])).With("slot", slot)
			} else {
				return NewErrMsg(ErrKindResult, ErrAsk, txt).
					With("movedto", string(parts[2])).With("slot", slot)
			}
		}
		if strings.HasPrefix(txt, "LOADING") {
			return NewErrMsg(ErrKindResult, ErrLoading, txt)
		}
		return NewErrMsg(ErrKindResult, ErrResult, txt)
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
			return NewErrWrap(ErrKindIO, ErrIO, err)
		}
		if buf[v] != '\r' || buf[v+1] != '\n' {
			return NewErr(ErrKindResponse, ErrNoFinalRN)
		}
		return buf[:v:v]
	case '*':
		if v, err = parseInt(line[1:]); err != nil {
			return err
		}
		if v < 0 {
			return nil
		}
		result := make([]interface{}, v)
		for i := int64(0); i < v; i++ {
			result[i] = ReadResponse(b)
			if e, ok := result[i].(*Error); ok && e.Kind != ErrKindResult {
				return e
			}
		}
		return result
	default:
		return NewErr(ErrKindResponse, ErrUnknownHeaderType)
	}
}

func parseInt(buf []byte) (int64, error) {
	if len(buf) == 0 {
		return 0, NewErr(ErrKindResponse, ErrIntegerParsing)
	}

	neg := buf[0] == '-'
	if neg {
		buf = buf[1:]
	}
	v := int64(0)
	for _, b := range buf {
		if b < '0' || b > '9' {
			return 0, NewErr(ErrKindResponse, ErrIntegerParsing)
		}
		v *= 10
		v += int64(b - '0')
	}
	if neg {
		v = -v
	}
	return v, nil
}
