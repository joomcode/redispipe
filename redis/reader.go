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
		return ErrIO.NewWrap(err)
	}

	if isPrefix {
		return ErrHeaderlineTooLarge.New().With(EKLine, line)
	}

	if len(line) == 0 {
		return ErrHeaderlineEmpty.New()
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
				return ErrResponseFormat.New().With(EKLine, line)
			}
			slot, err := parseInt(parts[1])
			if err != nil {
				return err.With(EKLine, line)
			}
			if moved {
				return ErrMoved.NewMsg(txt).With(EKMovedTo, string(parts[2])).With(EKSlot, slot)
			} else {
				return ErrAsk.NewMsg(txt).With(EKMovedTo, string(parts[2])).With(EKSlot, slot)
			}
		}
		if strings.HasPrefix(txt, "LOADING") {
			return ErrLoading.NewMsg(txt)
		}
		return ErrResult.NewMsg(txt)
	case ':':
		if v, err := parseInt(line[1:]); err != nil {
			return err.With(EKLine, line)
		} else {
			return v
		}
	case '$':
		var rerr *Error
		if v, rerr = parseInt(line[1:]); rerr != nil {
			return rerr.With(EKLine, line)
		}
		if v < 0 {
			return nil
		}
		buf := make([]byte, v+2, v+2)
		if _, err = io.ReadFull(b, buf); err != nil {
			return ErrIO.NewWrap(err)
		}
		if buf[v] != '\r' || buf[v+1] != '\n' {
			return ErrNoFinalRN.New()
		}
		return buf[:v:v]
	case '*':
		var rerr *Error
		if v, rerr = parseInt(line[1:]); rerr != nil {
			return rerr.With(EKLine, line)
		}
		if v < 0 {
			return nil
		}
		result := make([]interface{}, v)
		for i := int64(0); i < v; i++ {
			result[i] = ReadResponse(b)
			if e, ok := result[i].(*Error); ok && !e.KindOf(ErrResult) {
				return e
			}
		}
		return result
	default:
		return ErrUnknownHeaderType.New()
	}
}

func parseInt(buf []byte) (int64, *Error) {
	if len(buf) == 0 {
		return 0, ErrIntegerParsing.New()
	}

	neg := buf[0] == '-'
	if neg {
		buf = buf[1:]
	}
	v := int64(0)
	for _, b := range buf {
		if b < '0' || b > '9' {
			return 0, ErrIntegerParsing.New()
		}
		v *= 10
		v += int64(b - '0')
	}
	if neg {
		v = -v
	}
	return v, nil
}
