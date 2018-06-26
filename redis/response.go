package redis

import (
	"fmt"
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
