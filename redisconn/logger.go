package redisconn

import "log"

type LogKind int

const (
	LogConnecting LogKind = iota
	LogConnected
	LogConnectFailed
	LogDisconnected
	LogContextClosed
	LogMAX
)

type Logger interface {
	Report(event LogKind, conn *Connection, v ...interface{})
}

type defaultLogger struct{}

func (d defaultLogger) Report(event LogKind, conn *Connection, v ...interface{}) {
	switch event {
	case LogConnecting:
		log.Printf("redis: connecting to %s", conn.Addr())
	case LogConnected:
		localAddr := v[0].(string)
		remoteAddr := v[1].(string)
		log.Printf("redis: connected to %s (localAddr: %s, remote addr: %s)",
			conn.Addr(), localAddr, remoteAddr)
	case LogConnectFailed:
		err := v[0].(error)
		log.Printf("redis: connection to %s failed: %s", conn.Addr(), err.Error())
	case LogDisconnected:
		err := v[0].(error)
		log.Printf("redis: connection to %s broken: %s", conn.Addr(), err.Error())
	case LogContextClosed:
		log.Printf("redis: connect to %s explicitly closed", conn.Addr())
	default:
		args := []interface{}{"redis: unexpected event:", event, conn}
		args = append(args, v...)
		log.Print(args...)
	}
}
