package rediscluster

import (
	"log"

	"github.com/joomcode/redispipe/redisconn"
)

type LogKind int

// Logger is used for loggin cluster-related events and requests statistic.
type Logger interface {
	// Report will be called when some events happens during cluster's lifetime.
	// Default implementation just prints this information using standard log package.
	Report(c *Cluster, event LogEvent)
	// ReqStat is called after request receives it's answer with request/result information
	// and time spend to fulfill request.
	// Default implementation is no-op.
	ReqStat(c *Cluster, conn *redisconn.Connection, req Request, res interface{}, nanos int64)
}

func (c *Cluster) report(event LogEvent) {
	c.opts.Logger.Report(c, event)
}

// LogEvent is a sumtype for events to be logged.
type LogEvent interface {
	logEvent()
}

// LogHostEvent is a wrapper for per-connection event
type LogHostEvent struct {
	Conn  *redisconn.Connection // Connection which triggers event.
	Event redisconn.LogEvent
}

// LogClusterSlotsError is logged when CLUSTER SLOTS failed.
type LogClusterSlotsError struct {
	Conn  *redisconn.Connection // Connection which were used for CLUSTER SLOTS
	Error error                 // observed error
}

// LogSlotRangeError is logged when no host were able to respond to CLUSTER SLOTS.
type LogSlotRangeError struct{}

// LogContextClosed is logged when cluster's context is closed.
type LogContextClosed struct{ Error error }

func (LogHostEvent) logEvent()         {}
func (LogClusterSlotsError) logEvent() {}
func (LogSlotRangeError) logEvent()    {}
func (LogContextClosed) logEvent()     {}

// DefaultLogger is a default Logger implementation
type DefaultLogger struct{}

// Report implements Logger.Report.
func (d DefaultLogger) Report(cluster *Cluster, event LogEvent) {
	switch ev := event.(type) {
	case LogHostEvent:
		switch cev := ev.Event.(type) {
		case redisconn.LogConnecting:
			log.Printf("rediscluster %s: connecting to %s", cluster.Name(), ev.Conn.Addr())
		case redisconn.LogConnected:
			log.Printf("rediscluster %s: connected to %s (localAddr: %s, remAddr: %s)",
				cluster.Name(), ev.Conn.Addr(), cev.LocalAddr, cev.RemoteAddr)
		case redisconn.LogConnectFailed:
			log.Printf("rediscluster %s: connection to %s failed: %s",
				cluster.Name(), ev.Conn.Addr(), cev.Error.Error())
		case redisconn.LogDisconnected:
			log.Printf("rediscluster %s: connection to %s broken (localAddr: %s, remAddr: %s): %s",
				cluster.Name(), ev.Conn.Addr(), cev.LocalAddr, cev.RemoteAddr, cev.Error.Error())
		case redisconn.LogContextClosed:
			log.Printf("rediscluster %s: connect to %s explicitly closed: %s",
				cluster.Name(), ev.Conn.Addr(), cev.Error.Error())
		default:
			log.Printf("rediscluster %s: unexpected connection event for %s: %s",
				cluster.Name(), ev.Conn.Addr(), event)
		}
	case LogClusterSlotsError:
		log.Printf("rediscluster %s: 'CLUSTER SLOTS' request to %s failed: %s",
			cluster.Name(), ev.Conn.Addr(), ev.Error.Error())
	case LogSlotRangeError:
		log.Printf("rediscluster %s: no alive nodes to request 'CLUSTER SLOTS'",
			cluster.Name())
	case LogContextClosed:
		log.Printf("rediscluster %s: shutting down (%s)", cluster.Name(), ev.Error)
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
func (d defaultConnLogger) Report(conn *redisconn.Connection, event redisconn.LogEvent) {
	d.Cluster.opts.Logger.Report(d.Cluster, LogHostEvent{Conn: conn, Event: event})
}

// Report implements redisconn.Logger.ReqStat
func (d defaultConnLogger) ReqStat(conn *redisconn.Connection, req Request, res interface{}, nanos int64) {
	d.Cluster.opts.Logger.ReqStat(d.Cluster, conn, req, res, nanos)
}

// NoopLogger implements Logger with no logging at all.
type NoopLogger struct{}

// Report implements Logger.Report
func (d NoopLogger) Report(conn *Cluster, event LogEvent) {}

// ReqStat implements Logger.ReqStat
func (d NoopLogger) ReqStat(c *Cluster, conn *redisconn.Connection, req Request, res interface{}, nanos int64) {
}
