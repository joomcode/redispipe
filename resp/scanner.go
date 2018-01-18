package resp

import (
	"github.com/joomcode/redispipe/redis"
)

func ScanResponse(res interface{}) ([]byte, []string, error) {
	if err := Error(res); err != nil {
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
	return nil, nil, redis.New(redis.ErrKindResponse, redis.ErrResponseFormat).With("response", res)
}
