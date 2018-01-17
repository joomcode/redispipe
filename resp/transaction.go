package resp

import re "github.com/joomcode/redispipe/rediserror"

func TransactionResponse(res interface{}, n int) []interface{} {
	if arr, ok := res.([]interface{}); ok {
		return arr
	}
	if res == nil {
		res = re.New(re.ErrKindResult, re.ErrExecEmpty)
	}
	arr := make([]interface{}, n)
	for i := range arr {
		arr[i] = res
	}
	return arr
}
