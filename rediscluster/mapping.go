package rediscluster

import (
	"sync/atomic"
	"unsafe"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
)

func (c *Cluster) storeConfig(cfg *clusterConfig) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&c.config)), unsafe.Pointer(cfg))
}

func (c *Cluster) getConfig() *clusterConfig {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&c.config)))
	return (*clusterConfig)(ptr)
}

type ClusterHandle struct {
	Handle  interface{}
	Address string
	N       int
}

func (c *Cluster) newNode(addr string, initial bool) (*node, error) {
	node := &node{
		opts:   c.opts.HostOpts,
		addr:   addr,
		refcnt: 1,
	}
	node.opts.Async = true
	node.conns = make([]*redisconn.Connection, c.opts.ConnsPerHost)
	for i := range node.conns {
		node.opts.Handle = ClusterHandle{c.opts.Handle, addr, i}
		var err error
		node.conns[i], err = redisconn.Connect(c.ctx, addr, node.opts)
		if err != nil {
			if initial {
				return nil, err
			}
			// Since we are connected in async mode, there are should no be
			// errors. If there is error, it is configuration error.
			// There could no be configuration error after start.
			panic(err)
		}
	}
	return node, nil
}

type connThen func(conn *redisconn.Connection, err error)

func (c *Cluster) ensureConnForAddress(addr string, then connThen) {
	if conn := c.connForAddress(addr); conn != nil {
		then(conn, nil)
		return
	}

	c.nodeWait.Lock()
	defer c.nodeWait.Unlock()

	if c.nodeWait.promises == nil {
		c.nodeWait.promises = make(map[string]*[]connThen, 1)
	}

	if future, ok := c.nodeWait.promises[addr]; ok {
		*future = append(*future, then)
		return
	}

	future := &[]connThen{then}
	c.nodeWait.promises[addr] = future
	go func() {
		node := c.addNode(addr)
		var err error
		conn := node.getConn(c.opts.ConnHostPolicy, mayBeConnected, nil)
		if conn == nil {
			err = c.err(redis.ErrKindConnection, redis.ErrDial).With("address", addr)
		}
		c.nodeWait.Lock()
		delete(c.nodeWait.promises, addr)
		c.nodeWait.Unlock()
		for _, cb := range *future {
			cb(conn, err)
		}
	}()
}

func (c *Cluster) addNode(addr string) *node {
	var node *node
	var ok bool
	if node, ok = c.getConfig().nodes[addr]; ok {
		return node
	}

	c.m.Lock()
	defer c.m.Unlock()

	oldConf := c.getConfig()
	if node, ok = oldConf.nodes[addr]; ok {
		return node
	}

	newConf := *oldConf
	newConf.nodes = make(nodeMap, len(oldConf.nodes)+1)
	for a, node := range oldConf.nodes {
		newConf.nodes[a] = node
	}

	if node, ok = c.prevNodes[addr]; ok {
		atomic.AddUint32(&node.refcnt, 1)
	} else {
		node, _ = c.newNode(addr, false)
	}
	newConf.nodes[addr] = node

	c.storeConfig(&newConf)

	return node
}

func (cfg *clusterConfig) slot2shardno(slot uint16) uint16 {
	sh32 := atomic.LoadUint32(&cfg.slots[slot/2])
	sh16 := uint16((sh32 >> (16 * (slot & 1))) & 0x3fff)
	return sh16
}

func (cfg *clusterConfig) slotSetShard(slot, shard uint16) {
	sh32 := atomic.LoadUint32(&cfg.slots[slot/2])
	sh32 &^= 0xffff << (16 * (slot & 1))
	sh32 |= uint32(shard) << (16 * (slot & 1))
	atomic.StoreUint32(&cfg.slots[slot/2], sh32)
}

func (cfg *clusterConfig) slotMarkAsking(slot uint16) {
	sh32 := atomic.LoadUint32(&cfg.slots[slot/2])
	flag := uint32(MasterOnlyFlag << (16 * (slot & 1)))
	if sh32&flag == 0 {
		sh32 |= flag
		atomic.StoreUint32(&cfg.slots[slot/2], sh32)
	}
}

func (cfg *clusterConfig) slotIsAsking(slot uint16) bool {
	sh32 := atomic.LoadUint32(&cfg.slots[slot/2])
	flag := uint32(MasterOnlyFlag << (16 * (slot & 1)))
	return sh32&flag != 0
}

func (cfg *clusterConfig) slot2shard(slot uint16) *shard {
	sh16 := cfg.slot2shardno(slot)
	shard := cfg.shards[sh16]
	return shard
}

type defaultRoundRobinSeed uint32

func (d *defaultRoundRobinSeed) Current() uint32 {
	return atomic.AddUint32((*uint32)(d), 1)
}

func (c *Cluster) connForSlot(slot uint16, policy ReplicaPolicyEnum, seen []*redisconn.Connection) (*redisconn.Connection, error) {
	// We are not synchronizing by locks, so we need to spin until we have
	// consistent configuration, ie for shard number we have a shard in a shardmap
	// and a node in a nodemap.
	var conn *redisconn.Connection
	cfg := c.getConfig()
	shard := cfg.slot2shard(slot)
	nodes := cfg.nodes

	if shard == nil {
		return nil, c.err(redis.ErrKindCluster, redis.ErrClusterConfigEmpty).
			With("slot", slot)
	}

	var addr string
	switch policy {
	case MasterOnly:
		addr = shard.addr[0]
		node := nodes[addr]
		if node == nil {
			break /*switch*/
		}
		conn = node.getConn(c.opts.ConnHostPolicy, preferConnected, seen)
	case MasterAndSlaves, PreferSlaves:
		n, a := uint32(len(shard.addr))*3, uint32(0)
		if policy == PreferSlaves {
			n, a = n-2, 2
		}
		off := c.opts.RoundRobinSeed.Current()
		for _, needState := range []int{needConnected, mayBeConnected} {
			mask := atomic.LoadUint32(&shard.good)
			for mask != 0 && conn == nil {
				k := (nextRng(&off, n) + a) / 3
				if mask&(1<<k) == 0 {
					// replica isn't healthy, or already viewed
					continue
				}
				mask &^= 1 << k
				addr = shard.addr[k]
				node := nodes[addr]
				if node == nil {
					continue
				}
				conn = node.getConn(c.opts.ConnHostPolicy, needState, seen)
			}
			if conn != nil {
				break
			}
		}
	default:
		panic("unknown policy")
	}
	if conn == nil {
		c.ForceReloading()
		return nil, c.err(redis.ErrKindConnection, redis.ErrDial).
			With("slot", slot).With("policy", policy)
	}
	return conn, nil
}

func (c *Cluster) connForAddress(addr string) *redisconn.Connection {
	node := c.getConfig().nodes[addr]
	if node == nil {
		return nil
	}

	return node.getConn(c.opts.ConnHostPolicy, preferConnected, nil)
}

func connHealthy(c *redisconn.Connection, needState int) bool {
	if needState == needConnected {
		return c.ConnectedNow()
	} else if needState == mayBeConnected {
		return c.MayBeConnected()
	} else {
		panic("unknown needState")
	}
}

func isSeen(conn *redisconn.Connection, seen []*redisconn.Connection) bool {
	for _, p := range seen {
		if conn == p {
			return true
		}
	}
	return false
}

func (n *node) getConn(policy ConnHostPolicyEnum, needState int, seen []*redisconn.Connection) *redisconn.Connection {
	if len(n.conns) == 1 {
		if isSeen(n.conns[0], seen) {
			return nil
		}
		if connHealthy(n.conns[0], needState) {
			return n.conns[0]
		}
		return nil
	}
	if needState == preferConnected {
		conn := n.getConnConcreteNeed(policy, needConnected, seen)
		if conn == nil {
			conn = n.getConnConcreteNeed(policy, mayBeConnected, seen)
		}
		return conn
	}
	return n.getConnConcreteNeed(policy, needState, seen)
}

func (n *node) getConnConcreteNeed(policy ConnHostPolicyEnum, needState int, seen []*redisconn.Connection) *redisconn.Connection {
	switch policy {
	case ConnHostPreferFirst:
		for _, conn := range n.conns {
			if isSeen(conn, seen) {
				continue
			}
			if connHealthy(conn, needState) {
				return conn
			}
		}
	case ConnHostRoundRobin:
		off := atomic.AddUint32(&n.rr, 1)
		l := uint32(len(n.conns))
		mask := uint32(1)<<uint(l) - 1
		for mask != 0 {
			k := nextRng(&off, l)
			if mask&(1<<k) == 0 {
				continue
			}
			mask &^= 1 << k
			conn := n.conns[k]
			if isSeen(conn, seen) {
				continue
			}
			if connHealthy(conn, needState) {
				return conn
			}
		}
	default:
		panic("unknown ConnHostPolicy")
	}
	return nil
}

func nextRng(state *uint32, mod uint32) uint32 {
	v := *state
	*state = v*0x12345 + 1
	return (v ^ v>>16) % mod
}
