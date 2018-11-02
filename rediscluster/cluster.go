package rediscluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
	"github.com/joomcode/redispipe/redisconn"
)

type ConnHostPolicyEnum int8
type ReplicaPolicyEnum int8

const (
	// ConnHostPreferFirst means "always prefer first connection among established to redis instance"
	ConnHostPreferFirst ConnHostPolicyEnum = iota
	// ConnHostRoundRobin means "spread requests among connections established to redis instance"
	ConnHostRoundRobin
)

const (
	// MasterOnly means request should be executed on master
	MasterOnly ReplicaPolicyEnum = iota
	// MasterAndSlave means request could be executed on slave,
	// and every host in replica set has same probability for query execution.
	// Write requests still goes to master.
	MasterAndSlaves
	// PreferSlaves means request could be executed on slave,
	// but replica has 3 times more probability to handle request.
	// Write requests still goes to master.
	PreferSlaves
	// ForceMasterAndSlaves - override "writeness" of command and allow to send it to replica.
	// Since we could not analize Lua code, all "EVAL/EVALSHA" commands are considered as "writing".
	// Also, list of "readonly" commands is hardcoded, and could miss one you need.
	// In this case you may use one of ForceMasterAndSlaves, ForcePreferSlaves or ForceMasterWithFallback.
	ForceMasterAndSlaves
	ForcePreferSlaves
)

const (
	defaultCheckInterval = 5 * time.Second
	defaultWaitToMigrate = 20 * time.Millisecond

	forceInterval = 100 * time.Millisecond

	needConnected = iota
	mayBeConnected
	preferConnected
)

// Opts is a options for Cluster
type Opts struct {
	// HostOpts - per host options
	// Note that HostOpts.Handle will be overwritten to ClusterHandle{ cluster.opts.Handle, conn.address}
	HostOpts redisconn.Opts
	// ConnsPerHost - how many connections are established to each host
	// if ConnsPerHost < 1 then ConnsPerHost = 2
	ConnsPerHost int
	// ConnHostPolicy - either prefer to send to first connection until it is disconnected, or
	//					send to all connections in round robin maner.
	// default: ConnHostPreferFirst
	ConnHostPolicy ConnHostPolicyEnum
	// Handle is returned with Cluster.Handle()
	// Also it is part of per-connection handle
	Handle interface{}
	// Name of a cluster.
	Name string
	// Check interval - default cluster configuration reloading interval
	// default: 5 seconds, min: 100 millisecond, max: 10 minutes
	// Note, that MOVE and ASK redis errors will force configuration reloading,
	// therefore there is not need to make it very frequent.
	CheckInterval time.Duration
	// MovedRetries - follow MOVED|ASK redirections this number of times
	// default: 3, min: 1, max: 10
	MovedRetries int
	// WaitToMigrate - wait this time if not all transaction keys were migrated
	// from one shard to another and then repeat transaction.
	// default: 20 millisecond, min: 100 microseconds, max: 100 milliseconds
	WaitToMigrate time.Duration
	// Logger used for logging cluster events and account request stats
	Logger Logger

	// RoundRobinSeed - used to choose between master and replica.
	// Best implementation should return same number during 50-100 milliseconds, ie
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
	prevNodes nodeMap // connections from previous cluster configuration. Probably, could be reused.

	internallyForceMasterOnly map[uint16]struct{}

	nodeWait struct {
		sync.Mutex
		promises map[string]*[]connThen
	}

	forceReload chan struct{}
	commands    chan clusterCommand
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
		return nil, redis.ErrContextIsNil.New()
	}
	if len(init_addrs) == 0 {
		return nil, redis.ErrNoAddressProvided.New()
	}
	cluster := &Cluster{
		opts: opts,

		commands:    make(chan clusterCommand, 4),
		forceReload: make(chan struct{}, 1),
	}
	cluster.ctx, cluster.cancel = context.WithCancel(ctx)

	if cluster.opts.HostOpts.Logger == nil {
		cluster.opts.HostOpts.Logger = defaultConnLogger{cluster}
	}
	if cluster.opts.Logger == nil {
		cluster.opts.Logger = DefaultLogger{}
	}
	if cluster.opts.RoundRobinSeed == nil {
		cluster.opts.RoundRobinSeed = DefaultRoundRobinSeed()
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

	config := &clusterConfig{
		nodes:   make(nodeMap),
		shards:  make(shardMap),
		masters: make(masterMap),
	}
	cluster.storeConfig(config)

	cluster.nodeWait.promises = make(map[string]*[]connThen, 1)

	var err error
	for _, addr := range init_addrs {
		// If redis hosts are mentioned by names, couple of connections will be established and closed shortly.
		// Lets resolve them to ip addresses.
		addr, err = redisclusterutil.Resolve(addr)
		if err != nil {
			return nil, redis.ErrAddressNotResolved.NewWrap(err)
		}
		if _, ok := config.masters[addr]; !ok {
			config.nodes[addr], err = cluster.newNode(addr, true)
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

	go cluster.control()

	return cluster, nil
}

// Ctx returns context associated with this connection
func (c *Cluster) Ctx() context.Context {
	return c.ctx
}

// Close this cluster handler (by cancelling its context)
func (c *Cluster) Close() {
	c.cancel()
}

// String implements fmt.Stringer
func (c *Cluster) String() string {
	return fmt.Sprintf("*rediscluster.Cluster{Name: %s}", c.opts.Name)
}

// Name returns configured name.
func (c *Cluster) Name() string {
	return c.opts.Name
}

// Handle returns configured handle.
func (c *Cluster) Handle() interface{} {
	return c.opts.Handle
}

func (c *Cluster) control() {
	t := time.NewTicker(c.opts.CheckInterval)
	defer t.Stop()

	forceReload := c.forceReload
	ft := time.NewTimer(time.Hour)
	ft.Stop()

	// main control loop
	for {
		select {
		case <-c.ctx.Done():
			// cluster closed, exit control loop
			c.report(LogContextClosed)
			return
		case cmd := <-c.commands:
			// execute some asynchronous "cluster-wide" actions
			c.execCommand(cmd)
			continue
		case <-forceReload:
			// forced mapping reload
			forceReload = nil
			ft.Reset(forceInterval)
			c.reloadMapping()
		case <-ft.C:
			// allow force reloading again
			forceReload = c.forceReload
			continue
		case <-t.C:
			// regular mapping reload
			c.reloadMapping()
		}
	}
}

func (c *Cluster) reloadMapping() error {
	nodes, err := c.slotRangesAndInternalMasterOnly()
	if err == nil {
		c.updateMappings(nodes)
	}
	return err
}

// addWaitToMigrate schedules some actions to be executed after WaitToMigrate interval.
// It is used when transaction touches several keys, part of which was already migrated, and part wasn't.
func (c *Cluster) addWaitToMigrate(f func()) {
	time.AfterFunc(c.opts.WaitToMigrate, f)
}

// sendCommand queues some cluster aware actions for execution in control loop.
func (c *Cluster) sendCommand(cmd string, slot uint16, addr string) {
	if cmd == "asking" {
		// do not spam about asking slot if we already knows about.
		if c.getConfig().slotIsAsking(slot) {
			return
		}
	}

	// send command non-blocking manner to
	// - not block user queries
	// - not spam control loop with many-many same commands.
	//   Some of commands will be queued, and executed, and stream of same commands will stop.
	//   Then other commands will have a chance to be executed.
	select {
	case c.commands <- clusterCommand{cmd, slot, addr}:
	default:
	}
}

// ForceReloading forces reloading of cluster slot mapping.
// It is non-blocking call, and it's effect is throttled: reloading is called at most 10 times a second.
func (c *Cluster) ForceReloading() {
	select {
	case c.forceReload <- struct{}{}:
	default:
	}
}

// execCommand executes "cluster-wide" actions
func (c *Cluster) execCommand(cmd clusterCommand) {
	config := c.getConfig()
	switch cmd.cmd {
	case "moved":
		// remap slot to other shard without reloading of whole mapping.
		// first search shard for address
		addrshard, ok := config.masters[cmd.addr]
		if !ok {
			// Shard corresponding to address is not installed yet.
			// Wait a bit, and remap slot on other "moved" command later.
			return
		}
		slotshard := config.slot2shardno(cmd.slot)
		if addrshard == slotshard {
			// slot were already remapped
			return
		}
		c.m.Lock()
		defer c.m.Unlock()
		// ok, repeat it under lock
		config = c.getConfig()
		addrshard, ok = config.masters[cmd.addr]
		if !ok {
			return
		}
		slotshard = config.slot2shardno(cmd.slot)
		if addrshard == slotshard {
			return
		}
		// ok, we need to remap slot
		config.slotSetShard(cmd.slot, addrshard)
	case "asking":
		// mark slot as asking, therefore, it is switched to MasterOnly mode.
		config.slotMarkAsking(cmd.slot)
	}
}

// readonly - it is mostly "safe to run on replica" commands, therefore "scan" is not included, because its could differ
// between master and replica.
var readonly = func() map[string]bool {
	cmds := "PING ECHO DUMP MEMORY EXISTS GET GETRANGE RANDOMKEY KEYS TYPE TTL PTTL " +
		"BITCOUNT BITPOS GETBIT " +
		"GEOHASH GEOPOS GEODIST GEORADIUS_RO GEORADIUSBYMEMBER_RO " +
		"HEXISTS HGET HGETALL HKEYS HLEN HMGET HSTRLEN HVALS " +
		"LINDEX LLEN LRANGE " +
		"PFCOUNT " +
		"SCARD SDIFF SINTER SISMEMBER SMEMBERS SRANDMEMBER STRLEN SUNION " +
		"ZCARD ZCOUNT ZLEXCOUNT ZRANGE ZRANGEBYLEX ZREVRANGEBYLEX " +
		"ZRANGEBYSCORE ZRANK ZREVRANGE ZREVRANGEBYSCORE ZREVRANK ZSCORE " +
		"XPENDING XREVRANGE XREAD XLEN "
	ro := make(map[string]bool)
	for _, str := range strings.Split(cmds, " ") {
		ro[str] = true
	}
	return ro
}()

// fixPolicy correct current policy according to command 'write-ness' or forced mode.
func (c *Cluster) fixPolicy(slot uint16, req Request, policy ReplicaPolicyEnum) ReplicaPolicyEnum {
	// If slot is "asking" we could not use slaves.
	// This is actual limitation of redis-cluster implementation:
	// slaves doesn't know about slot movements until movements finished.
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

// Send implements redis.Sender.Send
// It sends request to correct shard (accordingly to know cluster configuration),
// handles MOVED and ASKING redirections and performs suitable retries.
func (c *Cluster) Send(req Request, cb Future, off uint64) {
	c.SendWithPolicy(MasterOnly, req, cb, off)
}

// SendWithPolicy allows to choose master/replica policy for individual requests.
// You can also call cluster.WithPolicy() to obtain redis.Sender with predefined policy.
func (c *Cluster) SendWithPolicy(policy ReplicaPolicyEnum, req Request, cb Future, off uint64) {
	slot, ok := redisclusterutil.ReqSlot(req)
	if !ok {
		// Probably, redis-cluster is not configured properly yet, or it is broken at the moment.
		err := c.err(redis.ErrNoSlotKey).With("request", req)
		cb.Resolve(err, off)
		return
	}

	policy = c.fixPolicy(slot, req, policy)

	conn, err := c.connForSlot(slot, policy, nil)
	if err != nil {
		cb.Resolve(err.(*redis.Error).With("request", req), off)
		return
	}

	r := requestPool.Get().(*request)
	*r = request{
		c:      c,
		req:    req,
		cb:     cb,
		off:    off,
		slot:   slot,
		policy: policy,

		// can retry if it is readonly command or if user forced to use slaves
		// (and then user is sure that command is readonly, for example, complex
		// readonly lua script.)
		mayRetry: policy != MasterOnly || readonly[req.Cmd],

		lastconn: conn,
	}
	conn.Send(req, r, 0)
}

// SendMany implements redis.Sender.SendMany
// Each request will be handled as if it were sent with Send method.
func (c *Cluster) SendMany(reqs []Request, cb Future, off uint64) {
	for i, req := range reqs {
		c.Send(req, cb, off+uint64(i))
	}
}

// request is a handle for single request sent to cluster.
// It implements redis.Future in a way it will try to retry itself on other suitable hosts, if it is possible.
type request struct {
	c   *Cluster
	req Request
	cb  Future
	off uint64

	lastconn *redisconn.Connection   // last connection used for this request
	seen     []*redisconn.Connection // all connection tried for this request so far

	slot   uint16
	policy ReplicaPolicyEnum

	mayRetry bool
	hardErrs uint8
	redir    uint8
}

var requestPool = sync.Pool{New: func() interface{} { return &request{} }}

func (r *request) resolve(res interface{}) {
	if err := redis.AsRedisError(res); err != nil {
		res = err.With("request", r.req).With("cluster", r.c)
	}
	r.cb.Resolve(res, r.off)
	*r = request{}
	requestPool.Put(r)
}

// Cancelled implements redis.Future.Cancelled.
// It proxies call to original request.
func (r *request) Cancelled() bool {
	return r.cb.Cancelled()
}

// Resolve implements redis.Future.Resolve.
// If request resolved with network error, and its master-replica policy allows for retry,
// another request attempt will be invoked here.
func (r *request) Resolve(res interface{}, _ uint64) {
	err := redis.AsRedisError(res)
	if err == nil {
		// if there is no error, resolve
		r.resolve(res)
		return
	}

	// do not retry if cluster is closed
	select {
	case <-r.c.ctx.Done():
		r.resolve(r.off)
		return
	default:
	}
	// or if request is not active already
	if r.cb.Cancelled() {
		r.resolve(r.c.err(redis.ErrRequestCancelled))
		return
	}

	kind := err.Kind()
	switch {
	case kind.KindOf(redis.ErrIO):
		if !r.mayRetry {
			// It is not safe to retry read-write operation
			r.resolve(err)
			return
		}
		fallthrough
	case kind.KindOf(redis.ErrConnection), kind.KindOf(redis.ErrContextClosed):
		// It is safe to retry readonly requests, and if request were not sent at all.

		r.c.ForceReloading() // Something is happen with cluster. Lets know actual information asap.

		// We could try at least connections to same host (of policy is MasterOnly)
		retries := r.c.opts.ConnsPerHost
		if r.mayRetry {
			// If policy is not MasterOnly, then try some of replica's as well,
			// Even with MasterOnly policy and single connection we may try to send it after reconnect.
			retries *= 2
		}
		if int(r.hardErrs) >= retries {
			// It looks like cluster is in unstable state.
			// Resolve with error.
			r.resolve(err)
			return
		}
		DebugEvent("retry")
		r.hardErrs++
		r.seen = append(r.seen, r.lastconn)
		conn, err := r.c.connForSlot(r.slot, r.policy, r.seen)
		if err != nil {
			r.resolve(err)
			return
		} else {
			r.lastconn = conn
			conn.Send(r.req, r, 0)
		}
	case kind.KindOf(redis.ErrResult):
		if kind == redis.ErrLoading {
			// Some host is not started properly yet. Lets learn actual cluster state asap.
			r.c.ForceReloading()
		}
		if (kind == redis.ErrMoved || kind == redis.ErrAsk) && int(r.redir) < r.c.opts.MovedRetries {
			// Slot is moving or were moved.
			r.redir++
			r.hardErrs = 0 // reset hardErrors because we are going to another physical shard.
			r.seen = nil
			addr := err.Get("movedto").(string)
			if kind == redis.ErrMoved {
				DebugEvent("moved")
				r.c.sendCommand("moved", r.slot, addr)
			} else {
				DebugEvent("asking")
				r.c.sendCommand("asking", r.slot, "")
			}
			// Send query to other address.
			// This address could be new, ie not listed in known cluster configuration,
			// therefore, connection is not established at the moment. In this case,
			// callback will be called after connection established.
			r.c.ensureConnForAddress(addr, func(conn *redisconn.Connection, cerr error) {
				if cerr != nil {
					r.resolve(cerr)
				} else {
					r.lastconn = conn
					conn.SendAsk(r.req, r, 0, kind == redis.ErrAsk)
				}
			})
			return
		}
		fallthrough
	default:
		// All other errors: just resolve.
		r.resolve(err)
	}
}

// SendTransaction implements redis.Sender.SendTransaction.
// It analyses commands keys, and send whole transaction to suitable shard.
// It redirects whole transaction on MOVED/ASKING requests, and waits a bit
// if not all keys in transaction were moved.
func (c *Cluster) SendTransaction(reqs []Request, cb Future, off uint64) {
	if len(reqs) == 0 {
		if cb != nil {
			cb.Resolve([]interface{}{}, off)
		}
		return
	}
	slot, ok := redisclusterutil.BatchSlot(reqs)
	if !ok {
		err := c.err(redis.ErrNoSlotKey).With("requests", reqs)
		cb.Resolve(err, off)
		return
	}

	conn, err := c.connForSlot(slot, MasterOnly, nil)

	if err != nil {
		// ? no known alive connection for slot
		cb.Resolve(err.(*redis.Error).With("requests", reqs), off)
		return
	}

	t := transactionPool.Get().(*transaction)
	*t = transaction{
		c:    c,
		reqs: reqs,
		cb:   cb,
		off:  off,
		slot: slot,

		lastconn: conn,
	}
	t.send(conn, false)
}

// handle for transaction as whole
type transaction struct {
	c    *Cluster
	reqs []Request
	cb   Future
	off  uint64

	res []interface{}

	lastconn *redisconn.Connection   // last connection used for this request
	seen     []*redisconn.Connection // all connections tried for this request

	hardErrs uint8
	redir    uint8
	slot     uint16
	asked    bool
}

var transactionPool = sync.Pool{New: func() interface{} { return &transaction{} }}

func (t *transaction) resolve(res interface{}) {
	if err := redis.AsRedisError(res); err != nil {
		res = err.With("requests", t.reqs).With("cluster", t.c)
	}
	t.cb.Resolve(res, t.off)
	*t = transaction{}
	transactionPool.Put(t)
}

// send transaction to connection
func (t *transaction) send(conn *redisconn.Connection, ask bool) {
	t.res = make([]interface{}, len(t.reqs)+1)
	flags := redisconn.DoTransaction
	if ask {
		t.asked = true
		flags |= redisconn.DoAsking
	}
	conn.SendBatchFlags(t.reqs, t, 0, flags)
}

// Cancelled implements redis.Future.Cancelled.
// It proxies call to original Future.
func (t *transaction) Cancelled() bool {
	return t.cb.Cancelled()
}

// Resolve implements redis.Future.Resolve
// It handles retry in case of broken connection.
func (t *transaction) Resolve(res interface{}, n uint64) {
	t.res[n] = res
	if int(n) != len(t.reqs) { // it is not response to EXEC.
		return
	}

	err := redis.AsRedisError(res)
	if err == nil {
		t.resolve(res)
		return
	}

	// do not retry if cluster is closed
	select {
	case <-t.c.ctx.Done():
		t.resolve(res)
		return
	default:
	}
	// or if request is not active already
	if t.cb.Cancelled() {
		t.resolve(t.c.err(redis.ErrRequestCancelled))
		return
	}

	kind := err.Kind()
	switch {
	case kind.KindOf(redis.ErrIO):
		// redis treats all transactions as read-write, and it is not safe
		// to retry
		t.resolve(err)
		return
	case kind.KindOf(redis.ErrConnection), kind.KindOf(redis.ErrContextClosed):
		// Transaction were not sent at all.
		// It is safe to retry transaction.
		t.c.ForceReloading()
		if int(t.hardErrs) >= t.c.opts.ConnsPerHost {
			// Look like cluster is in unstable state.
			t.resolve(err)
			return
		}
		t.hardErrs++

		t.seen = append(t.seen, t.lastconn)
		conn, err := t.c.connForSlot(t.slot, MasterOnly, t.seen)
		if err != nil {
			t.resolve(err.(*redis.Error).With("requests", t.reqs))
			return
		} else {
			t.lastconn = conn
			t.send(conn, false)
		}
	case kind.KindOf(redis.ErrResult):
		var moved string
		allmoved := true // all keys were moved
		moving := false  // has moving keys
		asking := false  // has asking keys
		if kind == redis.ErrMoved {
			// we occasionally sent transaction to slave
			moved = err.Get("movedto").(string)
			moving = true
		} else if strings.HasPrefix(err.Msg(), "EXECABORT") {
			// check if all partial responses were ASK or MOVED
			responses := t.res[:len(t.res)-1]
			for _, r := range responses {
				err := redis.AsRedisError(r)
				if err == nil {
					allmoved = false
					break
				}
				emoved := err.KindOf(redis.ErrMoved)
				eask := err.KindOf(redis.ErrAsk)
				if !emoved && !eask {
					allmoved = false
					break
				}
				moved = err.Get("movedto").(string)
				if emoved {
					moving = true
				} else if eask {
					asking = true
				}
			}
		} else if strings.HasPrefix(err.Msg(), "TRYAGAIN") && int(t.redir) < t.c.opts.MovedRetries {
			// Redis informs, that some, but not all, keys were migrated.
			// Lets wait a bit for migration finalization.
			t.redir++
			t.hardErrs = 0
			t.seen = nil
			t.c.addWaitToMigrate(func() { t.send(t.lastconn, t.asked) })
			DebugEvent("transaction tryagain")
			return
		}
		if moved != "" && moving != asking && int(t.redir) < t.c.opts.MovedRetries {
			// all keys are either moved (in this case, migration were finished),
			// or asking (migration is in progress, but all keys were migrated).
			t.redir++
			t.hardErrs = 0
			t.seen = nil
			if moving {
				t.c.sendCommand("moved", t.slot, moved) // remap slot to other address
				DebugEvent("transaction moved")
			} else {
				t.c.sendCommand("asking", t.slot, "") // mark slot as MasterOnly
				DebugEvent("transaction asking")
			}
			if !allmoved { // not all requests were moved, and redis didn't return TRYAGAIN.
				if asking {
					// lets wait a bit for migrating keys
					t.c.addWaitToMigrate(func() {
						t.sendMoved(moved, asking)
					})
				} else {
					// shit... wtf?
					// this should not happen, and I don't know how to handle it in better way.
					t.resolve(err)
				}
				return
			}
			// send transaction to other address.
			t.sendMoved(moved, asking)
			return
		}
		fallthrough
	default:
		// all other kinds of error
		t.resolve(err)
		return
	}
}

func (t *transaction) sendMoved(addr string, asking bool) {
	// Send query to other address.
	t.c.ensureConnForAddress(addr, func(conn *redisconn.Connection, cerr error) {
		if cerr != nil {
			t.resolve(cerr)
		} else {
			t.lastconn = conn
			t.send(conn, asking)
		}
	})
}

func (c *Cluster) err(kind redis.ErrorKind) *redis.Error {
	return kind.New().With("cluster", c)
}
