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

// Logger is a type for custom event and stat reporter.
type Logger interface {
	// Report will be called when some events happens during connection's lifetime.
	// Default implementation just prints this information using standard log package.
	Report(event LogKind, conn *Connection, v ...interface{})
	// ReqStat is called after request receives it's answer with request/result information
	// and time spend to fulfill request.
	// Default implementation is no-op.
	ReqStat(conn *Connection, req Request, res interface{}, nanos int64)
}

func (conn *Connection) report(event LogKind, v ...interface{}) {
	conn.opts.Logger.Report(event, conn, v...)
}

// DefaultLogger is default implementation of Logger
type DefaultLogger struct{}

// Report implements Logger.Report
func (d DefaultLogger) Report(event LogKind, conn *Connection, v ...interface{}) {
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

// ReqStat implements Logger.ReqStat
func (d DefaultLogger) ReqStat(conn *Connection, req Request, res interface{}, nanos int64) {
	// noop
}

// Noop implementation of Logger
// Useful in tests
type NoopLogger struct{}

// Report implements Logger.Report
func (d NoopLogger) Report(event LogKind, conn *Connection, v ...interface{}) {}

// ReqStat implements Logger.ReqStat
func (d NoopLogger) ReqStat(conn *Connection, req Request, res interface{}, nanos int64) {}
