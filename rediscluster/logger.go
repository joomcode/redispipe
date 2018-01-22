package rediscluster

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

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

type Logger interface {
	Report(event LogKind, c *Cluster, v ...interface{})
	ReqStat(c *Cluster, conn *redisconn.Connection, req Request, res interface{}, nanos int64)
}

func (c *Cluster) report(event LogKind, v ...interface{}) {
	c.opts.Logger.Report(event, c, v...)
}

type DefaultLogger struct{}

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

type SkippingLogger struct {
	DefaultLogger
	ts     int64
	rnd    uint32
	counts [int(LogMAX) + int(redisconn.LogMAX)]uint32
}

func (s *SkippingLogger) Report(event LogKind, cluster *Cluster, v ...interface{}) {
	n := uint32(event)
	if event == LogHostEvent {
		n = uint32(LogMAX) + uint32(v[0].(redisconn.LogKind))
	}
	ts := time.Now().UnixNano()
	oldts := atomic.LoadInt64(&s.ts)
	if ts >= oldts && atomic.CompareAndSwapInt64(&s.ts, oldts, ts+1e8) && oldts != 0 {
		del := uint32(2)
		dist := (oldts - ts) / 1e8
		if dist > 5 {
			del = 33
		} else {
			for ; dist > 0; dist-- {
				del *= 2
			}
		}
		for i := range s.counts {
			atomic.StoreUint32(&s.counts[i], atomic.LoadUint32(&s.counts[i])/del)
		}
	}
	rnd := atomic.AddUint32(&s.rnd, 1)
	k := atomic.LoadUint32(&s.counts[n])
	if k >= 32 || rnd&(1<<k-1) != 0 {
		return
	}
	atomic.AddUint32(&s.counts[n], 1)
	s.DefaultLogger.Report(event, cluster, v...)
}

type defaultConnLogger struct {
	*Cluster
}

func (d defaultConnLogger) Report(event redisconn.LogKind, conn *redisconn.Connection, v ...interface{}) {
	args := []interface{}{event, conn}
	args = append(args, v...)
	d.Cluster.opts.Logger.Report(LogHostEvent, d.Cluster, args...)
}

func (d defaultConnLogger) ReqStat(conn *redisconn.Connection, req Request, res interface{}, nanos int64) {
	d.Cluster.opts.Logger.ReqStat(d.Cluster, conn, req, res, nanos)
}
