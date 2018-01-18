package redis

import (
	"fmt"
	"sort"
	"strconv"
)

func AsError(v interface{}) error {
	e, _ := v.(error)
	return e
}

func AsRedisError(v interface{}) *Error {
	e, _ := v.(*Error)
	if e == nil {
		if _, ok := v.(error); ok {
			panic(fmt.Errorf("result should be either *rediserror.Error, or not error at all, but got %#v", v))
		}
	}
	return e
}

// parse response of Scan command
func ScanResponse(res interface{}) ([]byte, []string, error) {
	if err := AsError(res); err != nil {
		return nil, nil, err
	}
	var ok bool
	var arr []interface{}
	var it []byte
	var keys []interface{}
	var strs []string
	if arr, ok = res.([]interface{}); !ok {
		goto wrong
	}
	if it, ok = arr[0].([]byte); !ok {
		goto wrong
	}
	if keys, ok = arr[1].([]interface{}); !ok {
		goto wrong
	}
	strs = make([]string, len(keys))
	for i, k := range keys {
		var b []byte
		if b, ok = k.([]byte); !ok {
			goto wrong
		}
		strs[i] = string(b)
	}
	return it, strs, nil

wrong:
	return nil, nil, NewErr(ErrKindResponse, ErrResponseUnexpected).With("response", res)
}

// parse response of EXEC command
func TransactionResponse(res interface{}) ([]interface{}, error) {
	if arr, ok := res.([]interface{}); ok {
		return arr, nil
	}
	if res == nil {
		res = NewErr(ErrKindResult, ErrExecEmpty)
	}
	if _, ok := res.(error); !ok {
		res = NewErr(ErrKindResponse, ErrResponseUnexpected).With("response", res)
	}
	return nil, res.(error)
}

type SlotsRange struct {
	From  int
	To    int
	Addrs []string
}

func ParseSlotsInfo(res interface{}) ([]SlotsRange, error) {
	const NumSlots = 1 << 14
	if err := AsError(res); err != nil {
		return nil, err
	}

	errf := func(f string, args ...interface{}) ([]SlotsRange, error) {
		msg := fmt.Sprintf(f, args...)
		err := NewErrMsg(ErrKindResponse, ErrResponseUnexpected, msg)
		return nil, err
	}

	var rawranges []interface{}
	var ok bool
	if rawranges, ok = res.([]interface{}); !ok {
		return errf("type is not array: %+v", res)
	}

	ranges := make([]SlotsRange, len(rawranges))
	for i, rawelem := range rawranges {
		var rawrange []interface{}
		var ok bool
		var i64 int64
		r := SlotsRange{}
		if rawrange, ok = rawelem.([]interface{}); !ok || len(rawrange) < 3 {
			return errf("format mismatch: res[%d]=%+v", i, rawelem)
		}
		if i64, ok = rawrange[0].(int64); !ok || i64 < 0 || i64 >= NumSlots {
			return errf("format mismatch: res[%d][0]=%+v", i, rawrange[0])
		}
		r.From = int(i64)
		if i64, ok = rawrange[1].(int64); !ok || i64 < 0 || i64 >= NumSlots {
			return errf("format mismatch: res[%d][1]=%+v", i, rawrange[1])
		}
		r.To = int(i64)
		if r.From > r.To {
			return errf("range wrong: res[%d]=%+v (%+v)", i, rawrange)
		}
		for j := 2; j < len(rawrange); j++ {
			rawaddr, ok := rawrange[j].([]interface{})
			if !ok || len(rawaddr) < 2 {
				return errf("address format mismatch: res[%d][%d] = %+v",
					i, j, rawrange[j])
			}
			host, ok := rawaddr[0].([]byte)
			port, ok2 := rawaddr[1].(int64)
			if !ok || !ok2 || port <= 0 || port+10000 > 65535 {
				return errf("address format mismatch: res[%d][%d] = %+v",
					i, j, rawaddr)
			}
			r.Addrs = append(r.Addrs, string(host)+":"+strconv.Itoa(int(port)))
		}
		sort.Strings(r.Addrs[1:])
		ranges[i] = r
	}
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].From < ranges[j].From
	})
	return ranges, nil
}
