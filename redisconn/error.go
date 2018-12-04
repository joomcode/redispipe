package redisconn

import (
	"github.com/joomcode/errorx"
)

var (
	// EKConnection - key for connection that handled request.
	EKConnection = errorx.RegisterProperty("connection")
	// EKDb - db number to select.
	EKDb = errorx.RegisterProperty("db")
)

func withNewProperty(err *errorx.Error, p errorx.Property, v interface{}) *errorx.Error {
	_, ok := err.Property(p)
	if ok {
		return err
	}
	return err.WithProperty(p, v)
}
