package rediscluster

import "fmt"

const (
	ErrNoAddressProvided = iota + 1
	ErrNoAliveFound
	ErrContextIsNil
	ErrClusterSlots
	ErrNoSlotKey
	ErrWrongRedirection
)

type Error struct {
	Code    int
	Msg     string
	Wrap    error
	Cluster *Cluster
}

func (e Error) Error() string {
	var typ string
	switch e.Code {
	default:
		typ = fmt.Sprintf("ErrUnknown%d", e.Code)
	}
	if e.Msg != "" {
		return fmt.Sprintf("%s (%s)", e.Msg, typ)
	} else {
		return fmt.Sprintf("%s (%s)", e.Wrap.Error(), typ)
	}
}

func (e *Error) WithMsg(s string) *Error {
	e.Msg = s
	return e
}

func (e *Error) WithWrap(err error) *Error {
	e.Wrap = err
	return e
}

func (c *Cluster) err(code int) *Error {
	return &Error{Cluster: c, Code: code}
}
