package rediscluster

import (
	"fmt"
	"log"

	"github.com/joomcode/redispipe/redisconn"
)

type LogKind int

const (
	LogHostEvent LogKind = iota
	LogClusterSlotsError
	LogForceReload
	LogContextClosed
	LogMAX
)

// Logger is used for loggin cluster-related events and requests statistic.
type Logger interface {
	// Report will be called when some events happens during cluster's lifetime.
	// Default implementation just prints this information using standard log package.
	Report(event LogKind, c *Cluster, v ...interface{})
	// ReqStat is called after request receives it's answer with request/result information
	// and time spend to fulfill request.
	// Default implementation is no-op.
	ReqStat(c *Cluster, conn *redisconn.Connection, req Request, res interface{}, nanos int64)
}

func (c *Cluster) report(event LogKind, v ...interface{}) {
	c.opts.Logger.Report(event, c, v...)
}

// DefaultLogger is a default Logger implementation
type DefaultLogger struct{}

// Report implements Logger.Report.
func (d DefaultLogger) Report(event LogKind, cluster *Cluster, v ...interface{}) {
	switch event {
	case LogHostEvent:
		event := v[0].(redisconn.LogKind)
		conn := v[1].(*redisconn.Connection)
		switch event {
		case redisconn.LogConnecting:
			log.Printf("rediscluster %s: connecting to %s", cluster.Name(), conn.Addr())
		case redisconn.LogConnected:
			localAddr := v[2].(string)
			remoteAddr := v[3].(string)
			log.Printf("rediscluster %s: connected to %s (localAddr: %s, remote addr: %s)",
				cluster.Name(), conn.Addr(), localAddr, remoteAddr)
		case redisconn.LogConnectFailed:
			err := v[2].(error)
			log.Printf("rediscluster %s: connection to %s failed: %s",
				cluster.Name(), conn.Addr(), err.Error())
		case redisconn.LogDisconnected:
			err := v[2].(error)
			log.Printf("rediscluster %s: connection to %s broken: %s",
				cluster.Name(), conn.Addr(), err.Error())
		case redisconn.LogContextClosed:
			log.Printf("rediscluster %s: connect to %s explicitly closed", cluster.Name(), conn.Addr())
		default:
			args := []interface{}{fmt.Sprintf("rediscluster %s: unexpected connection event:", cluster.Name()), event, conn}
			args = append(args, v[2:])
			log.Print(args...)
		}
	case LogClusterSlotsError:
		if len(v) > 0 {
			conn := v[0].(*redisconn.Connection)
			err := v[1].(error)
			log.Printf("rediscluster %s: 'CLUSTER SLOTS' request to %s failed: %s",
				cluster.Name(), conn.Addr(), err.Error())
		} else {
			log.Printf("rediscluster %s: no alive nodes to request 'CLUSTER SLOTS'",
				cluster.Name())
		}
	case LogForceReload:
		log.Printf("rediscluster %s: force reloading slots", cluster.Name())
	case LogContextClosed:
		log.Printf("rediscluster %s: shutting down", cluster.Name())
	}
}

func (d DefaultLogger) ReqStat(c *Cluster, conn *redisconn.Connection, req Request, res interface{}, nanos int64) {
	// noop
}

// defaultConnLogger implements redisconn.Logger to log individual connection events in context of cluster.
type defaultConnLogger struct {
	*Cluster
}

// Report implements redisconn.Logger.Report
func (d defaultConnLogger) Report(event redisconn.LogKind, conn *redisconn.Connection, v ...interface{}) {
	args := []interface{}{event, conn}
	args = append(args, v...)
	d.Cluster.opts.Logger.Report(LogHostEvent, d.Cluster, args...)
}

// Report implements redisconn.Logger.ReqStat
func (d defaultConnLogger) ReqStat(conn *redisconn.Connection, req Request, res interface{}, nanos int64) {
	d.Cluster.opts.Logger.ReqStat(d.Cluster, conn, req, res, nanos)
}
