package resp

import "github.com/joomcode/redispipe/redis"

func TransactionResponse(res interface{}, n int) []interface{} {
	if arr, ok := res.([]interface{}); ok {
		return arr
	}
	if res == nil {
		res = redis.New(redis.ErrKindResult, redis.ErrExecEmpty)
	}
	arr := make([]interface{}, n)
	for i := range arr {
		arr[i] = res
	}
	return arr
}
