package resp

import (
	"fmt"
	"strconv"
)

func AppendRequest(buf []byte, cmd string, args []interface{}) ([]byte, error) {
	buf = appendHead(buf, '*', int64(len(args)+1))
	buf = appendHead(buf, '$', int64(len(cmd)))
	buf = append(buf, cmd...)
	buf = append(buf, '\r', '\n')
	for _, val := range args {
		switch v := val.(type) {
		case string:
			buf = appendHead(buf, '$', int64(len(v)))
			buf = append(buf, v...)
		case []byte:
			buf = appendHead(buf, '$', int64(len(v)))
			buf = append(buf, v...)
		case int:
			buf = appendBulkInt(buf, int64(v))
		case uint:
			buf = appendBulkInt(buf, int64(v))
		case int64:
			buf = appendBulkInt(buf, int64(v))
		case uint64:
			buf = appendBulkInt(buf, int64(v))
		case int32:
			buf = appendBulkInt(buf, int64(v))
		case uint32:
			buf = appendBulkInt(buf, int64(v))
		case int8:
			buf = appendBulkInt(buf, int64(v))
		case uint8:
			buf = appendBulkInt(buf, int64(v))
		case int16:
			buf = appendBulkInt(buf, int64(v))
		case uint16:
			buf = appendBulkInt(buf, int64(v))
		case float32:
			str := strconv.FormatFloat(float64(v), 'f', -1, 32)
			buf = appendHead(buf, '$', int64(len(str)))
			buf = append(buf, str...)
		case float64:
			str := strconv.FormatFloat(v, 'f', -1, 64)
			buf = appendHead(buf, '$', int64(len(str)))
			buf = append(buf, str...)
		default:
			return nil, fmt.Errorf("resp.AppendRequest() couldn't handle type %t", val)
		}
		buf = append(buf, '\r', '\n')
	}
	return buf, nil
}

func appendInt(b []byte, i int64) []byte {
	var u uint
	if i == 0 {
		b = append(b, '0', '\r', '\n')
		return b
	}
	if i > 0 {
		u = uint(i)
	} else {
		b = append(b, '-')
		u = uint(-i)
	}
	digits := [18]byte{}
	p := 18
	for u > 0 {
		n := u / 10
		p--
		digits[p] = byte(u-n*10) + '0'
		u = n
	}
	return append(b, digits[p:]...)
}

func appendHead(b []byte, t byte, i int64) []byte {
	b = append(b, t)
	b = appendInt(b, i)
	return append(b, '\r', '\n')
}

func appendBulkInt(b []byte, i int64) []byte {
	if i >= -99999999 && i <= 999999999 {
		b = append(b, '$', '0', '\r', '\n')
	} else {
		b = append(b, '$', '1', '0', '\r', '\n')
	}
	l := len(b)
	b = appendInt(b, i)
	li := len(b) - l
	if li < 10 {
		b[l-3] = byte(li) + '0'
	} else {
		b[l-3] = byte(li) - 10 + '0'
	}
	return b
}
