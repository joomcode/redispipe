package resp

const (
	AuthReq   = "*1\r\n$4\r\nAUTH\r\n"
	PingReq   = "*1\r\n$4\r\nPING\r\n"
	SelectReq = "*1\r\n$4\r\nSELECT\r\n"
	AskingReq = "*1\r\n$6\r\nASKING\r\n"
	MultiReq  = "*1\r\n$5\r\nMULTI\r\n"
	ExecReq   = "*1\r\n$4\r\nEXEC\r\n"
)
