package resp

import (
	"bufio"
	"errors"
	"io"
)

func Read(b *bufio.Reader) (interface{}, error) {
	line, isPrefix, err := b.ReadLine()
	if err != nil {
		return nil, IOError{err}
	}

	if isPrefix {
		return nil, ErrHeaderlineTooLarge
	}

	var v int64
	switch line[0] {
	case '+':
		return string(line[1:]), nil
	case '-':
		return errors.New(string(line[1:])), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		if v, err = parseInt(line[1:]); err != nil {
			return nil, err
		}
		if v < 0 {
			return nil, nil
		}
		buf := make([]byte, v+2, v+2)
		if _, err = io.ReadFull(b, buf); err != nil {
			return nil, IOError{err}
		}
		if buf[v] != '\r' || buf[v+1] != '\n' {
			return nil, ErrNoFinalRN
		}
		return buf[:v:v], err
	case '*':
		if v, err = parseInt(line[1:]); err != nil {
			return nil, err
		}
		if v < 0 {
			return nil, nil
		}
		result := make([]interface{}, v, v)
		for i := int64(0); i < v; i++ {
			result[i], err = Read(b)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	default:
		return nil, ErrUnknownHeaderType
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
			return 0, ErrIntegerParsing
		}
		v *= 10
		v += int64(b - '0')
	}
	if neg {
		v = -v
	}
	return v, nil
}
