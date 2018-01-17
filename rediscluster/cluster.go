package rediscluster

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redisconn"
	re "github.com/joomcode/redispipe/rediserror"
	"github.com/joomcode/redispipe/resp"
)

type ConnHostPolicyEnum int8
type MasterReplicaPolicyEnum int8

const (
	ConnHostPreferFirst ConnHostPolicyEnum = iota
	ConnHostRoundRobin
)

const (
	MasterOnly MasterReplicaPolicyEnum = iota
	MasterAndReplica
	PreferReplica
	ForceMasterAndReplica
	ForcePreferReplica
)

const (
	defaultCheckInterval = 30 * time.Second
	defaultForceInterval = 100 * time.Millisecond
	defaultWaitToMigrate = 1 * time.Millisecond

	needConnected = iota
	mayBeConnected
)

type Opts struct {
	// HostOpts - per host options
	// Note that HostOpts.Handle will be overwritten to ClusterHandle{ cluster.opts.Handle, conn.address}
	HostOpts redisconn.Opts
	// ConnsPerHost - how many connections are established to each host
	// if ConnsPerHost <= 1 then ConnsPerHost = 1
	ConnsPerHost int
	// ConnHostPolicy - either prefer to send to first connection until it is disconnected, or
	//					send to all connections in round robin maner
	// default: ConnHostPreferFirst
	ConnHostPolicy ConnHostPolicyEnum
	// Handle is returned with Cluster.Handle()
	// Also it is part of per-connection handle
	Handle interface{}
	// Name
	Name string
	// Check interval - default cluster configuration reloading interval
	// default: 30 seconds, min: 1 second, max: 10 minutes
	CheckInterval time.Duration
	// Force interval - short interval for forcing reloading cluster configuration
	// default: 100 milliseconds, min: 10 milliseconds, max: 1 second
	ForceInterval time.Duration
	// MovedRetries - follow MOVED|ASK redirections this number of times
	// default: 2, min: 1, max: 10
	MovedRetries int
	// WaitToMigrate - wait this time if not all transaction keys were migrating
	// default: 1 millisecond, min: 100 microseconds, max: 100 milliseconds
	WaitToMigrate time.Duration
	// Logger
	Logger Logger
}

type shard struct {
	rr   uint32
	good uint32
	addr []string
}
type shardMap map[uint16]*shard
type masterMap map[string]uint16
type nodeMap map[string]*node

type Cluster struct {
	ctx    context.Context
	cancel context.CancelFunc

	m sync.Mutex

	forceReload uint32
	version     uint32

	shardMap  atomic.Value // map[uint32][]string
	masterMap atomic.Value // map[string]uint32
	nextShard uint16

	nodeMap atomic.Value // map[string]*host

	// map of slot to shard
	slotMap []uint32

	opts Opts
}

type node struct {
	addr    string
	rr      uint32
	version uint32
	opts    redisconn.Opts
	conns   []*redisconn.Connection
}

func NewCluster(ctx context.Context, init_addrs []string, opts Opts) (*Cluster, error) {
	if ctx == nil {
		return nil, re.New(re.ErrKindOpts, re.ErrContextIsNil)
	}
	if len(init_addrs) == 0 {
		return nil, re.New(re.ErrKindOpts, re.ErrNoAddressProvided)
	}
	cluster := &Cluster{
		opts: opts,
	}
	cluster.ctx, cluster.cancel = context.WithCancel(ctx)

	if cluster.opts.HostOpts.Logger == nil {
		cluster.opts.HostOpts.Logger = defaultConnLogger{cluster}
	}
	if cluster.opts.Logger == nil {
		cluster.opts.Logger = DefaultLogger{}
	}

	if cluster.opts.ConnsPerHost <= 1 {
		cluster.opts.ConnsPerHost = 1
	}

	if cluster.opts.CheckInterval <= 0 {
		cluster.opts.CheckInterval = defaultCheckInterval
	} else if cluster.opts.CheckInterval < time.Second {
		cluster.opts.CheckInterval = time.Second
	} else if cluster.opts.CheckInterval > 10*time.Minute {
		cluster.opts.CheckInterval = 10 * time.Minute
	}

	if cluster.opts.ForceInterval <= 0 {
		cluster.opts.ForceInterval = defaultForceInterval
	} else if cluster.opts.ForceInterval < 10*time.Millisecond {
		cluster.opts.ForceInterval = 10 * time.Millisecond
	} else if cluster.opts.ForceInterval > time.Second {
		cluster.opts.ForceInterval = time.Second
	}

	if cluster.opts.MovedRetries <= 0 {
		cluster.opts.MovedRetries = 2
	} else if cluster.opts.MovedRetries > 10 {
		cluster.opts.MovedRetries = 10
	}

	if cluster.opts.WaitToMigrate <= 0 {
		cluster.opts.WaitToMigrate = defaultWaitToMigrate
	} else if cluster.opts.WaitToMigrate < 100*time.Microsecond {
		cluster.opts.WaitToMigrate = 100 * time.Microsecond
	} else if cluster.opts.WaitToMigrate > 100*time.Millisecond {
		cluster.opts.WaitToMigrate = 100 * time.Millisecond
	}

	nodes := make(nodeMap)
	shards := make(shardMap)
	masters := make(masterMap)
	cluster.nodeMap.Store(nodes)
	cluster.shardMap.Store(shards)
	cluster.masterMap.Store(masters)

	cluster.slotMap = make([]uint32, NumSlots/2)

	for _, addr := range init_addrs {
		if _, ok := masters[addr]; !ok {
			nodes[addr] = cluster.newNode(addr)
		}
	}

	if err := cluster.reloadMapping(); err != nil {
		cluster.cancel()
		return nil, err
	}

	go cluster.checker()

	return cluster, nil
}

func (c *Cluster) String() string {
	return fmt.Sprintf("*rediscluster.Cluster{Name: %s}", c.opts.Name)
}

func (c *Cluster) Name() string {
	return c.opts.Name
}

func (c *Cluster) Handle() interface{} {
	return c.opts.Handle
}

func (c *Cluster) checker() {
	t := time.NewTicker(c.opts.CheckInterval)
	f := time.NewTicker(c.opts.ForceInterval)
	defer t.Stop()
	defer f.Stop()

Loop:
	for {
		select {
		case <-c.ctx.Done():
			c.report(LogContextClosed)
			return
		case <-t.C:
		case <-f.C:
			if atomic.LoadUint32(&c.forceReload) == 0 {
				continue Loop
			}
			c.report(LogForceReload)
			atomic.StoreUint32(&c.forceReload, 0)
		}

		c.reloadMapping()
	}
}

func (c *Cluster) reloadMapping() error {
	slotsRanges, err := c.SlotRanges()
	if err == nil {
		c.updateMappings(slotsRanges)
	}
	return err
}

func (c *Cluster) forceReloading() {
	atomic.StoreUint32(&c.forceReload, 1)
}

func reqSlot(req Request) (uint16, bool) {
	n := 0
	if req.Cmd == "RANDOMKEY" {
		return uint16(rand.Intn(NumSlots)), true
	}
	if req.Cmd == "EVAL" || req.Cmd == "EVALSHA" || req.Cmd == "BITOP" {
		n = 1
	}
	if len(req.Args) <= n {
		return 0, false
	}
	key, ok := resp.ArgToString(req.Args[n])
	return Slot(key), ok
}

func batchSlot(reqs []Request) (uint16, bool) {
	var slot uint16
	var set bool
	for _, req := range reqs {
		s, ok := reqSlot(req)
		if !ok {
			continue
		}
		if !set {
			slot = s
		} else if slot != s {
			return 0, false
		}
	}
	return slot, set
}

var readonly = func() map[string]bool {
	cmds := "BITCOUNT BITPOS DUMP EXISTS GEOHASH GEOPOS GEODIST " +
		"GEORADIUS GEORADIUSBYMEMBER GET GETBIT GETRANGE " +
		"HEXISTS HGET HGETALL HKEYS HLEN HMGET HSTRLEN HVALS " +
		"KEYS LINDEX LLEN LRANGE PFCOUNT RANDOMKEY SCARD SDIFF " +
		"SINTER SISMEMBER SMEMBERS SRANDMEMBER STRLEN SUNION " +
		"ZCARD ZCOUNT ZLEXCOUNT ZRANGE ZRANGEBYLEX ZREVRANGEBYLEX " +
		"ZRANGEBYSCORE ZRANK ZREVRANGE ZREVRANGEBYSCORE ZREVRANK " +
		"SCAN SSCAN HSCAN ZSCAN"
	ro := make(map[string]bool)
	for _, str := range strings.Split(cmds, " ") {
		ro[str] = true
	}
	return ro
}()

func fixPolicy(req Request, policy MasterReplicaPolicyEnum) MasterReplicaPolicyEnum {
	switch policy {
	case MasterOnly:
		return MasterOnly
	case ForceMasterAndReplica:
		return MasterAndReplica
	case ForcePreferReplica:
		return PreferReplica
	}
	if readonly[req.Cmd] {
		return policy
	}
	return MasterOnly
}

func (c *Cluster) Send(req Request, cb Callback, off uint64) {
	c.SendWithPolicy(MasterOnly, req, cb, off)
}

func (c *Cluster) SendWithPolicy(policy MasterReplicaPolicyEnum, req Request, cb Callback, off uint64) {
	slot, ok := reqSlot(req)
	if !ok {
		c.forceReloading()
		re.Do(func() {
			cb(re.New(re.ErrKindRequest, re.ErrNoSlotKey).With("request", req), off)
		})
		return
	}

	policy = fixPolicy(req, policy)

	conn, err := c.connForSlot(slot, policy)
	if err != nil {
		re.Do(func() { cb(err, off) })
		return
	}

	request := &request{
		c:      c,
		req:    req,
		cb:     cb,
		off:    off,
		slot:   slot,
		policy: policy,
	}
	conn.Send(req, request.set, 0)
}

type request struct {
	c   *Cluster
	req Request
	cb  Callback

	off    uint64
	slot   uint16
	policy MasterReplicaPolicyEnum

	lastErrIsHard bool
	try           uint8
}

func (r *request) set(res interface{}, _ uint64) {
	err := resp.RedisError(res)
	if err == nil {
		r.cb(res, r.off)
		return
	}

	// do not retry if cluster is closed
	select {
	case <-r.c.ctx.Done():
		r.cb(res, r.off)
		return
	default:
	}
	err = err.With("cluster", r.c)

	switch err.Kind {
	case re.ErrKindIO:
		if r.policy == MasterOnly {
			// It is not safe to retry read-write operation
			r.cb(res, r.off)
			return
		}
		fallthrough
	case re.ErrKindConnection, re.ErrKindContext:
		r.c.forceReloading()
		// It is safe to retry readonly requests, and if request were
		// not sent at all.
		if r.lastErrIsHard {
			// on second try do no "smart" things,
			// cause it is likely cluster is in unstable state
			r.cb(res, r.off)
			return
		}
		r.lastErrIsHard = true
		conn, err := r.c.connForSlot(r.slot, r.policy)
		if err != nil {
			r.cb(err, r.off)
		} else {
			conn.Send(r.req, r.set, 0)
		}
	case re.ErrKindResult:
		if err.Code == re.ErrMoved || err.Code == re.ErrLoading {
			r.c.forceReloading()
		}
		if (err.Code == re.ErrMoved || err.Code == re.ErrAsk) && int(r.try) < r.c.opts.MovedRetries {
			r.try++
			r.lastErrIsHard = false
			addr := err.Data.Get("addr").(string)
			conn := r.c.connForAddress(addr)
			if conn != nil {
				conn.SendAsk(r.req, r.set, 0, err.Code == re.ErrAsk)
				return
			}
			re.Do(func() {
				conn, cerr := r.c.newConn(addr)
				if cerr != nil {
					r.cb(cerr, r.off)
					return
				}
				conn.SendAsk(r.req, r.set, 0, err.Code == re.ErrAsk)
			})
			return
		}
		fallthrough
	default:
		r.cb(res, r.off)
	}
}

func (c *Cluster) SendTransaction(reqs []Request, cb Callback, off uint64) {
	if len(reqs) == 0 {
		return
	}
	slot, ok := batchSlot(reqs)
	if !ok {
		err := re.New(re.ErrKindRequest, re.ErrNoSlotKey).
			With("cluster", c).
			With("requests", reqs)
		re.Do(func() { cb(err, off) })
		return
	}

	conn, err := c.connForSlot(slot, MasterOnly)

	if err != nil {
		// ? no known alive connection for slot
		re.Do(func() { cb(err, off) })
		return
	}

	t := &transaction{c: c, reqs: reqs, cb: cb, off: off, slot: slot}
	t.send(conn, false)
}

type transaction struct {
	c    *Cluster
	reqs []Request
	cb   Callback
	off  uint64

	r []interface{}

	lastErrIsHard bool
	slot          uint16
	try           uint8
}

func (t *transaction) send(conn *redisconn.Connection, ask bool) {
	t.r = make([]interface{}, len(t.reqs)+1)
	flags := redisconn.DoTransaction
	if ask {
		flags |= redisconn.DoAsking
	}
	conn.SendBatchFlags(t.reqs, t.set, 0, flags)
}

func (t *transaction) set(res interface{}, n uint64) {
	t.r[n] = res
	if int(n) != len(t.reqs) {
		return
	}

	execres := t.r[len(t.reqs)-1]

	select {
	case <-t.c.ctx.Done():
		t.cb(execres, t.off)
		return
	default:
	}

	err := resp.RedisError(execres)
	if err == nil {
		t.cb(execres, t.off)
		return
	}
	err = err.With("cluster", t.c)

	switch err.Kind {
	case re.ErrKindIO:
		// redis treats all transactions as read-write, and it is not safe
		// to retry
		t.cb(execres, t.off)
		return
	case re.ErrKindConnection, re.ErrKindContext:
		t.c.forceReloading()
		if t.lastErrIsHard {
			t.cb(execres, t.off)
			return
		}
		t.lastErrIsHard = true

		conn, err := t.c.connForSlot(t.slot, MasterOnly)
		if err != nil {
			t.cb(err, t.off)
		} else {
			t.send(conn, false)
		}
	case re.ErrKindResult:
		var moved string
		allmoved := true
		moving := false
		asking := false
		if err.Code == re.ErrMoved {
			// we occasionally sent transaction to slave
			moved = err.Data.Get("addr").(string)
			moving = true
		} else if strings.HasPrefix(err.Text, "EXECABORT") {
			// check if all partial responses were ASK or MOVED
			responses := t.r[1 : len(t.r)-1]
			for _, r := range responses {
				err := resp.RedisError(r)
				if err == nil || err.Code != re.ErrMoved || err.Code != re.ErrAsk {
					allmoved = false
					break
				}
				moved = err.Data.Get("addr").(string)
				if err.Code == re.ErrMoved {
					moving = true
				} else if err.Code == re.ErrAsk {
					asking = true
				}
			}
		}
		if moved != "" && moving != asking && int(t.try) < t.c.opts.MovedRetries {
			t.try++
			if moving {
				t.c.forceReloading()
			}
			if !allmoved {
				if asking {
					// lets wait a bit for migrating keys
					time.AfterFunc(t.c.opts.WaitToMigrate, func() {
						t.sendMoved(moved, asking)
					})
				} else {
					// shit... wtf?
					t.cb(res, t.off)
				}
				return
			}
			t.sendMoved(moved, asking)
			return
		}
		fallthrough
	default:
		t.cb(execres, t.off)
		return
	}
}

func (t *transaction) sendMoved(addr string, asking bool) {
	conn := t.c.connForAddress(addr)
	if conn != nil {
		t.send(conn, asking)
		return
	}
	re.Do(func() {
		conn, cerr := t.c.newConn(addr)
		if cerr != nil {
			t.cb(cerr, t.off)
			return
		}
		t.send(conn, asking)
	})
	return
}

func (c *Cluster) newConn(addr string) (*redisconn.Connection, error) {
	node := c.addNode(addr)
	conn := node.getConn(c.opts.ConnHostPolicy, mayBeConnected)
	if conn == nil {
		err := re.New(re.ErrKindConnection, re.ErrDial)
		err = err.With("cluster", c).With("addr", addr)
		return nil, err
	}
	return conn, nil
}
