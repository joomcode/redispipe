package rediscluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redisconn"
)

type ConnHostPolicyEnum int

const (
	ConnHostPreferFirst ConnHostPolicyEnum = iota
	ConnHostRoundRobin
)

type shardOnExistEnum int

const (
	shardReplaceShorter shardOnExistEnum = iota
	shardReplaceAlways
)

const (
	defaultCheckInterval = 5 * time.Second
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
	ConnHostPolicy ConnHostPolicyEnum
	// Handle is returned with Cluster.Handle()
	// Also it is part of per-connection handle
	Handle interface{}
	// Name
	Name string
	// Check interval
	CheckInterval time.Duration
	// Logger
	Logger Logger
}

type shardMap map[uint32][]string
type masterMap map[string]uint32
type nodeMap map[string]*node

type Cluster struct {
	ctx    context.Context
	cancel context.CancelFunc

	m sync.Mutex

	shardMap  atomic.Value // map[uint32][]string
	masterMap atomic.Value // map[string]uint32
	nextShard uint32

	nodeMap atomic.Value // map[string]*host

	// map of slot to shard
	slotMap []uint32

	opts Opts
}

type node struct {
	addr  string
	rr    uint32
	known uint32
	opts  redisconn.Opts
	conns []*redisconn.Connection
}

func NewCluster(ctx context.Context, init_addrs []string, opts Opts) (*Cluster, error) {
	if ctx == nil {
		return nil, &Error{Code: ErrContextIsNil, Msg: "Context should not be nil"}
	}
	if len(init_addrs) == 0 {
		return nil, &Error{Code: ErrNoAddressProvided, Msg: "no initial addresses given"}
	}
	cluster := &Cluster{
		opts: opts,
	}
	cluster.ctx, cluster.cancel = context.WithCancel(ctx)

	if cluster.opts.HostOpts.Logger == nil {
		cluster.opts.HostOpts.Logger = defaultConnLogger{cluster}
	}
	if cluster.opts.Logger == nil {
		cluster.opts.Logger = defaultLogger{}
	}

	if cluster.opts.CheckInterval <= 0 {
		cluster.opts.CheckInterval = 5 * time.Second
	} else if cluster.opts.CheckInterval < time.Second {
		cluster.opts.CheckInterval = time.Second
	} else if cluster.opts.CheckInterval > 10*time.Minute {
		cluster.opts.CheckInterval = 10 * time.Minute
	}

	nodes := make(nodeMap)
	shards := make(shardMap)
	masters := make(masterMap)
	cluster.nodeMap.Store(nodes)
	cluster.shardMap.Store(shards)
	cluster.masterMap.Store(masters)

	for _, addr := range init_addrs {
		if _, ok := masters[addr]; !ok {
			nodes[addr] = cluster.newNode(addr)
			masters[addr] = cluster.nextShard
			shards[cluster.nextShard] = []string{addr}
			cluster.nextShard++
		}
	}

	cluster.slotMap = make([]uint32, NumSlots)
	for i := range cluster.slotMap {
		cluster.slotMap[i] = uint32(i) % cluster.nextShard
	}

	go cluster.checker()

	return cluster, nil
}

func (c *Cluster) Name() string {
	return c.opts.Name
}

func (c *Cluster) Handle() interface{} {
	return c.opts.Handle
}

func (c *Cluster) checker() {
	t := time.NewTicker(c.opts.CheckInterval)
	for {
		select {
		case <-c.ctx.Done():
			c.opts.Logger.Report(LogContextClosed, c)
			return
		case <-t.C:
		}

		// first, fetch info about cluster slots
		slotsRanges, err := c.SlotRanges()
		if err == nil {
			c.updateMappings(slotsRanges)
		}
	}
}

func (c *Cluster) shardForSlot(slot uint16) (uint32, []string) {
	// HERE SHOULD BE SPIN LOOP
	shardn := atomic.LoadUint32(&c.slotMap[slot])
	return shardn, c.getShardMap()[shardn]
}

func (c *Cluster) SendToAddress(addr string, req Request, cb Callback, off uint64) bool {
	node := c.getNode(addr)
	if node == nil {
		return false
	}

	if len(node.conns) == 1 {
		if node.conns[0].MayBeConnected() {
			node.conns[0].Send(req, cb, off)
			return true
		}
		return false
	}

	switch c.opts.ConnHostPolicy {
	case ConnHostPreferFirst:
		for _, conn := range node.conns {
			if conn.MayBeConnected() {
				conn.Send(req, cb, off)
				return true
			}
		}
	case ConnHostRoundRobin:
		n := int(atomic.AddUint32(&node.rr, 1))
		l := len(node.conns)
		for i := 0; i < l; i++ {
			conn := node.conns[(i+n)%l]
			if conn.MayBeConnected() {
				conn.Send(req, cb, off)
				return true
			}
		}
	default:
		panic("unknown ConnHostPolicy")
	}
	return false
}
