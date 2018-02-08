package rediscluster

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
)

type ConnHostPolicyEnum int8
type ReplicaPolicyEnum int8

const (
	ConnHostPreferFirst ConnHostPolicyEnum = iota
	ConnHostRoundRobin
)

const (
	MasterOnly ReplicaPolicyEnum = iota
	MasterAndSlaves
	PreferSlaves
	ForceMasterAndSlaves
	ForcePreferSlaves
)

const (
	defaultCheckInterval = 5 * time.Second
	defaultForceInterval = 100 * time.Millisecond
	defaultWaitToMigrate = 20 * time.Millisecond

	needConnected = iota
	mayBeConnected
	preferConnected
)

type Opts struct {
	// HostOpts - per host options
	// Note that HostOpts.Handle will be overwritten to ClusterHandle{ cluster.opts.Handle, conn.address}
	HostOpts redisconn.Opts
	// ConnsPerHost - how many connections are established to each host
	// if ConnsPerHost < 1 then ConnsPerHost = 2
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
	// default: 5 seconds, min: 100 millisecond, max: 10 minutes
	CheckInterval time.Duration
	// Force interval - short interval for forcing reloading cluster configuration
	// default: 100 milliseconds, min: 10 milliseconds, max: 1 second
	ForceInterval time.Duration
	// MovedRetries - follow MOVED|ASK redirections this number of times
	// default: 3, min: 1, max: 10
	MovedRetries int
	// WaitToMigrate - wait this time if not all transaction keys were migrating
	// default: 20 millisecond, min: 100 microseconds, max: 100 milliseconds
	WaitToMigrate time.Duration
	// Logger
	Logger Logger

	// RoundRobinSeed - used to choose between master and replica.
	// Best implementation should return same number during 100 milliseconds, ie
	// uint32(time.Now().UnixNano()/int64(100*time.Millisecond))
	RoundRobinSeed interface {
		Current() uint32
	}
}

type Cluster struct {
	ctx    context.Context
	cancel context.CancelFunc

	opts Opts

	m sync.Mutex

	config    *clusterConfig
	prevNodes nodeMap

	externalForceMasterOnly map[uint16]struct{}

	nodeWait struct {
		sync.Mutex
		promises map[string]*[]connThen
	}

	forceReload chan struct{}
	commands    chan clusterCommand

	// for requests, waiting for retry
	wheelm    sync.Mutex
	wheelt    *time.Timer
	waitwheel [2][]func()
}

type clusterConfig struct {
	shards  shardMap
	masters masterMap
	nodes   nodeMap

	slots [NumSlots / 2]uint32
}

type shard struct {
	rr   uint32
	good uint32
	addr []string
}
type shardMap map[uint16]*shard
type masterMap map[string]uint16
type nodeMap map[string]*node

type node struct {
	addr   string
	rr     uint32
	refcnt uint32
	opts   redisconn.Opts
	conns  []*redisconn.Connection
}

type clusterCommand struct {
	cmd  string
	slot uint16
	addr string
}

func NewCluster(ctx context.Context, init_addrs []string, opts Opts) (*Cluster, error) {
	if ctx == nil {
		return nil, redis.NewErr(redis.ErrKindOpts, redis.ErrContextIsNil)
	}
	if len(init_addrs) == 0 {
		return nil, redis.NewErr(redis.ErrKindOpts, redis.ErrNoAddressProvided)
	}
	cluster := &Cluster{
		opts: opts,

		commands:    make(chan clusterCommand, 4),
		forceReload: make(chan struct{}, 1),
		wheelt:      time.NewTimer(time.Hour),
	}
	cluster.ctx, cluster.cancel = context.WithCancel(ctx)
	cluster.wheelt.Stop()

	if cluster.opts.HostOpts.Logger == nil {
		cluster.opts.HostOpts.Logger = defaultConnLogger{cluster}
	}
	if cluster.opts.Logger == nil {
		cluster.opts.Logger = DefaultLogger{}
	}
	if cluster.opts.RoundRobinSeed == nil {
		cluster.opts.RoundRobinSeed = new(defaultRoundRobinSeed)
	}

	if cluster.opts.ConnsPerHost < 1 {
		cluster.opts.ConnsPerHost = 2
	}

	if cluster.opts.CheckInterval <= 0 {
		cluster.opts.CheckInterval = defaultCheckInterval
	} else if cluster.opts.CheckInterval < 100*time.Millisecond {
		cluster.opts.CheckInterval = 100 * time.Millisecond
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
		cluster.opts.MovedRetries = 3
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

	cluster.config = &clusterConfig{
		nodes:   make(nodeMap),
		shards:  make(shardMap),
		masters: make(masterMap),
	}

	var err error
	for _, addr := range init_addrs {
		if _, ok := cluster.config.masters[addr]; !ok {
			cluster.config.nodes[addr], err = cluster.newNode(addr, true)
			// since we connecting asynchronously, it can be only configuration error
			if err != nil {
				cluster.cancel()
				return nil, err
			}
		}
	}

	// case if no nodes are accessible is handled here
	if err := cluster.reloadMapping(); err != nil {
		cluster.cancel()
		return nil, err
	}

	go cluster.checker()

	return cluster, nil
}

// Set slots, that will be migrating, to force them MasterOnly
func (c *Cluster) SetSlotsForcedMasterOnly(slots []uint16) {
	var efm map[uint16]struct{}
	if len(slots) > 0 {
		efm = make(map[uint16]struct{}, len(slots))
		for _, slot := range slots {
			efm[slot] = struct{}{}
		}
	}

	c.m.Lock()
	defer c.m.Unlock()
	if len(slots) == 0 {
		if c.externalForceMasterOnly == nil {
			return
		}
	} else if len(efm) == len(c.externalForceMasterOnly) {
		equal := true
		for slot, _ := range efm {
			if _, ok := c.externalForceMasterOnly[slot]; !ok {
				equal = false
				break
			}
		}
		if equal {
			return
		}
	}
	c.externalForceMasterOnly = efm
	c.ForceReloading()
}

// Context of this connection
func (c *Cluster) Ctx() context.Context {
	return c.ctx
}

func (c *Cluster) Close() {
	c.cancel()
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
	defer t.Stop()

	forceReload := c.forceReload
	ft := time.NewTimer(time.Hour)
	ft.Stop()

	for {
		select {
		case <-c.ctx.Done():
			c.report(LogContextClosed)
			return
		case <-c.wheelt.C:
			c.callbackMigrate()
			continue
		case cmd := <-c.commands:
			c.execCommand(cmd)
			continue
		case <-ft.C:
			forceReload = c.forceReload
		case <-forceReload:
			forceReload = nil
			ft.Reset(c.opts.ForceInterval)
		case <-t.C:
		}

		c.reloadMapping()
	}
}

func (c *Cluster) reloadMapping() error {
	nodes, slotsRanges, err := c.nodesAndSlotRanges()
	if err == nil {
		c.updateMappings(nodes, slotsRanges)
	}
	return err
}

func (c *Cluster) addWaitToMigrate(f func()) {
	c.wheelm.Lock()
	if len(c.waitwheel[0]) == 0 && len(c.waitwheel[1]) == 0 {
		c.wheelt.Reset(c.opts.WaitToMigrate / 2)
	}
	c.waitwheel[1] = append(c.waitwheel[1], f)
	c.wheelm.Unlock()
}

func (c *Cluster) callbackMigrate() {
	c.wheelm.Lock()
	funcs := c.waitwheel[0]
	c.waitwheel[0], c.waitwheel[1] = c.waitwheel[1], nil
	if len(c.waitwheel[0]) != 0 {
		c.wheelt.Reset(c.opts.WaitToMigrate / 2)
	}
	defer c.wheelm.Unlock()

	for _, f := range funcs {
		f()
	}
}

func (c *Cluster) sendCommand(cmd string, slot uint16, addr string) {
	if cmd == "asking" {
		if c.getConfig().slotIsAsking(slot) {
			return
		}
	}
	select {
	case c.commands <- clusterCommand{cmd, slot, addr}:
	default:
	}
}

func (c *Cluster) ForceReloading() {
	select {
	case c.forceReload <- struct{}{}:
	default:
	}
}

func (c *Cluster) execCommand(cmd clusterCommand) {
	config := c.getConfig()
	switch cmd.cmd {
	case "moved":
		addrshard, ok := config.masters[cmd.addr]
		if !ok {
			return
		}
		slotshard := config.slot2shardno(cmd.slot)
		if addrshard == slotshard {
			return
		}
		c.m.Lock()
		defer c.m.Unlock()
		config = c.getConfig()
		addrshard, ok = config.masters[cmd.addr]
		if !ok {
			return
		}
		slotshard = config.slot2shardno(cmd.slot)
		if addrshard == slotshard {
			return
		}
		config.slotSetShard(cmd.slot, addrshard)
	case "asking":
		config.slotMarkAsking(cmd.slot)
	}
}

func reqSlot(req Request) (uint16, bool) {
	key, ok := req.Key()
	if key == "RANDOMKEY" && !ok {
		return uint16(rand.Intn(NumSlots)), true
	}
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
			set = true
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
		"SCAN SSCAN HSCAN ZSCAN PING ECHO"
	ro := make(map[string]bool)
	for _, str := range strings.Split(cmds, " ") {
		ro[str] = true
	}
	return ro
}()

func (c *Cluster) fixPolicy(slot uint16, req Request, policy ReplicaPolicyEnum) ReplicaPolicyEnum {
	if c.getConfig().slotIsAsking(slot) {
		return MasterOnly
	}
	switch policy {
	case MasterOnly:
		return MasterOnly
	case ForceMasterAndSlaves:
		return MasterAndSlaves
	case ForcePreferSlaves:
		return PreferSlaves
	}
	if readonly[req.Cmd] {
		return policy
	}
	return MasterOnly
}

func (c *Cluster) Send(req Request, cb Future, off uint64) {
	c.SendWithPolicy(MasterOnly, req, cb, off)
}

func (c *Cluster) SendWithPolicy(policy ReplicaPolicyEnum, req Request, cb Future, off uint64) {
	slot, ok := reqSlot(req)
	if !ok {
		cb.Resolve(redis.NewErr(redis.ErrKindRequest, redis.ErrNoSlotKey).With("request", req), off)
		return
	}

	policy = c.fixPolicy(slot, req, policy)

	conn, err := c.connForSlot(slot, policy, nil)
	if err != nil {
		cb.Resolve(err, off)
		return
	}

	request := &request{
		c:      c,
		req:    req,
		cb:     cb,
		off:    off,
		slot:   slot,
		policy: policy,

		lastconn: conn,
	}
	conn.Send(req, request, 0)
}

func (c *Cluster) SendMany(reqs []Request, cb Future, off uint64) {
	for i, req := range reqs {
		c.Send(req, cb, off+uint64(i))
	}
}

type request struct {
	c   *Cluster
	req Request
	cb  Future
	off uint64

	lastconn *redisconn.Connection
	seen     []*redisconn.Connection

	slot   uint16
	policy ReplicaPolicyEnum

	hardErrs uint8
	redir    uint8
}

func (r *request) Cancelled() bool {
	return r.cb.Cancelled()
}

func (r *request) Resolve(res interface{}, _ uint64) {
	err := redis.AsRedisError(res)
	if err == nil {
		r.cb.Resolve(res, r.off)
		return
	}

	// do not retry if cluster is closed
	select {
	case <-r.c.ctx.Done():
		r.cb.Resolve(res, r.off)
		return
	default:
	}
	// or if request is not active already
	if r.cb.Cancelled() {
		r.cb.Resolve(redis.NewErr(redis.ErrKindRequest, redis.ErrRequestCancelled), r.off)
		return
	}

	err = err.With("cluster", r.c)

	switch err.Kind {
	case redis.ErrKindIO:
		if r.policy == MasterOnly {
			// It is not safe to retry read-write operation
			r.cb.Resolve(res, r.off)
			return
		}
		fallthrough
	case redis.ErrKindConnection, redis.ErrKindContext:
		r.c.ForceReloading()
		// It is safe to retry readonly requests, and if request were
		// not sent at all.
		tries := r.c.opts.ConnsPerHost
		if r.policy != MasterOnly {
			tries *= 2
		}
		if int(r.hardErrs) >= tries {
			// on second try do no "smart" things,
			// cause it is likely cluster is in unstable state
			r.cb.Resolve(res, r.off)
			return
		}
		DebugEvent("retry")
		r.hardErrs++
		r.seen = append(r.seen, r.lastconn)
		conn, err := r.c.connForSlot(r.slot, r.policy, r.seen)
		if err != nil {
			r.cb.Resolve(err, r.off)
		} else {
			r.lastconn = conn
			conn.Send(r.req, r, 0)
		}
	case redis.ErrKindResult:
		if err.Code == redis.ErrLoading {
			r.c.ForceReloading()
		}
		if (err.Code == redis.ErrMoved || err.Code == redis.ErrAsk) && int(r.redir) < r.c.opts.MovedRetries {
			r.redir++
			r.hardErrs = 0
			r.seen = nil
			addr := err.Get("movedto").(string)
			if err.Code == redis.ErrMoved {
				DebugEvent("moved")
				r.c.sendCommand("moved", r.slot, addr)
			} else {
				DebugEvent("asking")
				r.c.sendCommand("asking", r.slot, "")
			}
			r.c.ensureConnForAddress(addr, func(conn *redisconn.Connection, cerr error) {
				if cerr != nil {
					r.cb.Resolve(cerr, r.off)
				} else {
					r.lastconn = conn
					conn.SendAsk(r.req, r, 0, err.Code == redis.ErrAsk)
				}
			})
			return
		}
		fallthrough
	default:
		r.cb.Resolve(res, r.off)
	}
}

func (c *Cluster) SendTransaction(reqs []Request, cb Future, off uint64) {
	if len(reqs) == 0 {
		return
	}
	slot, ok := batchSlot(reqs)
	if !ok {
		err := c.err(redis.ErrKindRequest, redis.ErrNoSlotKey).
			With("requests", reqs)
		cb.Resolve(err, off)
		return
	}

	conn, err := c.connForSlot(slot, MasterOnly, nil)

	if err != nil {
		// ? no known alive connection for slot
		cb.Resolve(err, off)
		return
	}

	t := &transaction{
		c:    c,
		reqs: reqs,
		cb:   cb,
		off:  off,
		slot: slot,

		lastconn: conn,
	}
	t.send(conn, false)
}

type transaction struct {
	c    *Cluster
	reqs []Request
	cb   Future
	off  uint64

	res []interface{}

	lastconn *redisconn.Connection
	seen     []*redisconn.Connection

	hardErrs uint8
	redir    uint8
	slot     uint16
	asked    bool
}

func (t *transaction) send(conn *redisconn.Connection, ask bool) {
	t.res = make([]interface{}, len(t.reqs)+1)
	flags := redisconn.DoTransaction
	if ask {
		t.asked = true
		flags |= redisconn.DoAsking
	}
	conn.SendBatchFlags(t.reqs, t, 0, flags)
}

func (t *transaction) Cancelled() bool {
	return t.cb.Cancelled()
}

func (t *transaction) Resolve(res interface{}, n uint64) {
	t.res[n] = res
	if int(n) != len(t.reqs) {
		return
	}

	execres := t.res[len(t.reqs)]
	// do not retry if cluster is closed
	select {
	case <-t.c.ctx.Done():
		t.cb.Resolve(execres, t.off)
		return
	default:
	}
	// or if request is not active already
	if t.cb.Cancelled() {
		t.cb.Resolve(redis.NewErr(redis.ErrKindRequest, redis.ErrRequestCancelled), t.off)
		return
	}

	err := redis.AsRedisError(execres)
	if err == nil {
		t.cb.Resolve(execres, t.off)
		return
	}
	err = err.With("cluster", t.c)

	switch err.Kind {
	case redis.ErrKindIO:
		// redis treats all transactions as read-write, and it is not safe
		// to retry
		t.cb.Resolve(execres, t.off)
		return
	case redis.ErrKindConnection, redis.ErrKindContext:
		t.c.ForceReloading()
		if int(t.hardErrs) >= t.c.opts.ConnsPerHost {
			// on second try do no "smart" things,
			// cause it is likely cluster is in unstable state
			t.cb.Resolve(res, t.off)
			return
		}
		t.hardErrs++

		t.seen = append(t.seen, t.lastconn)
		conn, err := t.c.connForSlot(t.slot, MasterOnly, t.seen)
		if err != nil {
			t.cb.Resolve(err, t.off)
		} else {
			t.lastconn = conn
			t.send(conn, false)
		}
	case redis.ErrKindResult:
		var moved string
		allmoved := true
		moving := false
		asking := false
		if err.Code == redis.ErrMoved {
			// we occasionally sent transaction to slave
			moved = err.Get("movedto").(string)
			moving = true
		} else if strings.HasPrefix(err.Msg(), "EXECABORT") {
			// check if all partial responses were ASK or MOVED
			responses := t.res[:len(t.res)-1]
			for _, r := range responses {
				err := redis.AsRedisError(r)
				if err == nil || (err.Code != redis.ErrMoved && err.Code != redis.ErrAsk) {
					allmoved = false
					break
				}
				moved = err.Get("movedto").(string)
				if err.Code == redis.ErrMoved {
					moving = true
				} else if err.Code == redis.ErrAsk {
					asking = true
				}
			}
		} else if strings.HasPrefix(err.Msg(), "TRYAGAIN") && int(t.redir) < t.c.opts.MovedRetries {
			t.redir++
			t.hardErrs = 0
			t.seen = nil
			t.c.addWaitToMigrate(func() { t.send(t.lastconn, t.asked) })
			DebugEvent("transaction tryagain")
			return
		}
		if moved != "" && moving != asking && int(t.redir) < t.c.opts.MovedRetries {
			t.redir++
			t.hardErrs = 0
			t.seen = nil
			if moving {
				t.c.sendCommand("moved", t.slot, moved)
				DebugEvent("transaction moved")
			} else {
				t.c.sendCommand("asking", t.slot, "")
				DebugEvent("transaction asking")
			}
			if !allmoved {
				if asking {
					// lets wait a bit for migrating keys
					t.c.addWaitToMigrate(func() {
						t.sendMoved(moved, asking)
					})
				} else {
					// shit... wtf?
					t.cb.Resolve(res, t.off)
				}
				return
			}
			t.sendMoved(moved, asking)
			return
		}
		fallthrough
	default:
		t.cb.Resolve(execres, t.off)
		return
	}
}

func (t *transaction) sendMoved(addr string, asking bool) {
	t.c.ensureConnForAddress(addr, func(conn *redisconn.Connection, cerr error) {
		if cerr != nil {
			t.cb.Resolve(cerr, t.off)
		} else {
			t.lastconn = conn
			t.send(conn, asking)
		}
	})
}

func (c *Cluster) err(kind redis.ErrorKind, code redis.ErrorCode) *redis.Error {
	return redis.NewErr(kind, code).With("cluster", c)
}
