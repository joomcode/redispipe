package resp

import (
	"strconv"

	re "github.com/joomcode/redispipe/rediserror"
)

type Request struct {
	Cmd  string
	Args []interface{}
}

func AppendRequest(buf []byte, req Request) ([]byte, *re.Error) {
	buf = appendHead(buf, '*', int64(len(req.Args)+1))
	buf = appendHead(buf, '$', int64(len(req.Cmd)))
	buf = append(buf, req.Cmd...)
	buf = append(buf, '\r', '\n')
	for _, val := range req.Args {
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
		case bool:
			if v {
				buf = append(buf, "$1\r\n1"...)
			} else {
				buf = append(buf, "$1\r\n0"...)
			}
		case float32:
			str := strconv.FormatFloat(float64(v), 'f', -1, 32)
			buf = appendHead(buf, '$', int64(len(str)))
			buf = append(buf, str...)
		case float64:
			str := strconv.FormatFloat(v, 'f', -1, 64)
			buf = appendHead(buf, '$', int64(len(str)))
			buf = append(buf, str...)
		default:
			return nil, re.NewMsg(re.ErrKindRequest, re.ErrArgumentType,
				"resp.AppendRequest() couldn't handle type").With("val", val).With("request", req)
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

// returns string representataion of an argument.
// Used in cluster to determine cluster slot.
// Have to be in sync with AppendRequest
func ArgToString(arg interface{}) (string, bool) {
	var bufarr [20]byte
	var buf []byte
	switch v := arg.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case int:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case uint:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case int64:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case uint64:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case int32:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case uint32:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case int8:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case uint8:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case int16:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case uint16:
		buf = appendInt(bufarr[:0], int64(v))
		return string(buf), true
	case bool:
		if v {
			return "1", true
		} else {
			return "0", true
		}
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	default:
		return "", false
	}
}
