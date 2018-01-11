package redis_conn

import "fmt"

const (
	ErrContextIsNil = iota + 1
	ErrContextClosed
	ErrDisconnected
	ErrDial
	ErrIO
	ErrAuth
	ErrPing
	ErrArgumentType
	ErrResponse
	ErrBatchFailed
)

type ConnError struct {
	Code int
	Msg  string
	Wrap error
}

func (e ConnError) Error() string {
	var typ string
	switch e.Code {
	case ErrContextIsNil:
		typ = "ErrContextIsNil"
	case ErrContextClosed:
		typ = "ErrContextClosed"
	case ErrDisconnected:
		typ = "ErrDisconnected"
	case ErrDial:
		typ = "ErrDial"
	case ErrIO:
		typ = "ErrIO"
	case ErrAuth:
		typ = "ErrAuth"
	case ErrPing:
		typ = "ErrPing"
	case ErrArgumentType:
		typ = "ErrArgumentType"
	case ErrResponse:
		typ = "ErrResponse"
	case ErrBatchFailed:
		typ = "ErrBatchFailed"
	default:
		typ = fmt.Sprintf("ErrUnknown%d", e.Code)
	}
	if e.Msg != "" {
		return fmt.Sprintf("%s (%s)", e.Msg, typ)
	} else {
		return fmt.Sprintf("%s (%s)", e.Wrap.Error(), typ)
	}
}
