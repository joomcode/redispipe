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
	LogForceCompaction
	LogContextClosed
)

type Logger interface {
	Report(event LogKind, conn *Cluster, v ...interface{})
}

type defaultLogger struct{}

func (d defaultLogger) Report(event LogKind, cluster *Cluster, v ...interface{}) {
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
			err := v[3].(error)
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
	case LogForceCompaction:
		log.Printf("rediscluster %s: force shards compaction", cluster.Name())
	case LogContextClosed:
		log.Printf("rediscluster %s: shutting down", cluster.Name())
	}
}

type defaultConnLogger struct {
	*Cluster
}

func (d defaultConnLogger) Report(event redisconn.LogKind, conn *redisconn.Connection, v ...interface{}) {
	args := []interface{}{event, conn}
	args = append(args, v...)
	d.Cluster.opts.Logger.Report(LogHostEvent, d.Cluster, args...)
}
