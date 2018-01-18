package redisconn

const (
	authReq   = "*1\r\n$4\r\nAUTH\r\n"
	pingReq   = "*1\r\n$4\r\nPING\r\n"
	selectReq = "*1\r\n$4\r\nSELECT\r\n"
	askingReq = "*1\r\n$6\r\nASKING\r\n"
	multiReq  = "*1\r\n$5\r\nMULTI\r\n"
	execReq   = "*1\r\n$4\r\nEXEC\r\n"
)
