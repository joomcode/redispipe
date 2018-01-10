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
	case ErrDial:
		typ = "ErrDial"
	case ErrIO:
		typ = "ErrIO"
	case ErrAuth:
		typ = "ErrAuth"
	case ErrPing:
		typ = "ErrPing"
	}
	if e.Msg != "" {
		return fmt.Sprintf("%s (%s)", e.Msg, typ)
	} else {
		return fmt.Sprintf("%s (%s)", e.Wrap.Error(), typ)
	}
}
