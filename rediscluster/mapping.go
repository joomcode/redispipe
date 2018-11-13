package rediscluster

import (
	"sync/atomic"
	"unsafe"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
)

// storeConfig atomically stores config
func (c *Cluster) storeConfig(cfg *clusterConfig) {
	p := (*unsafe.Pointer)(unsafe.Pointer(&c.config))
	atomic.StorePointer(p, unsafe.Pointer(cfg))
}

// getConfig loads config atomically
func (c *Cluster) getConfig() *clusterConfig {
	p := (*unsafe.Pointer)(unsafe.Pointer(&c.config))
	return (*clusterConfig)(atomic.LoadPointer(p))
}

// ClusterHandle is used to wrap cluster's handle and set it as connection's handle.
// You can use it in connection's logging.
type ClusterHandle struct {
	Handle  interface{}
	Address string
	N       int
}

// newNode creates handle for a connection, that will be established in a future.
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

// Call callback with connection to specified address.
// If connection is already established, callback will be called immediately.
// Otherwise, callback will be called after connection established.
func (c *Cluster) ensureConnForAddress(addr string, then connThen) {
	node := c.getConfig().nodes[addr]
	if node != nil {
		// there is node for address, so call callback now.
		conn := node.getConn(c.opts.ConnHostPolicy, preferConnected, nil)
		if conn != nil {
			then(conn, nil)
		} else {
			err := c.err(redis.ErrDial).With(EKAddress, addr)
			then(nil, err)
		}
		return
	}

	c.nodeWait.Lock()
	defer c.nodeWait.Unlock()

	if future, ok := c.nodeWait.promises[addr]; ok {
		// there are already queued callback.
		// It means, goroutine with connection establishing is already run.
		// Add our callback to queue, and exit.
		*future = append(*future, then)
		return
	}

	// initiate queue for this address
	future := &[]connThen{then}
	c.nodeWait.promises[addr] = future

	go func() {
		node := c.addNode(addr)
		var err error
		conn := node.getConn(c.opts.ConnHostPolicy, mayBeConnected, nil)
		if conn == nil {
			err = c.err(redis.ErrDial).With(EKAddress, addr)
		}
		c.nodeWait.Lock()
		delete(c.nodeWait.promises, addr)
		c.nodeWait.Unlock()
		// since we deleted from promises under lock, no one could append to *future any more.
		// lets run callbacks.
		for _, cb := range *future {
			cb(conn, err)
		}
	}()
}

// addNode creates host handle and adds it to cluster configuration.
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
		// someone could already create same node
		return node
	}

	// we could not update configuration in-place (threadsafety, bla-bla-bla).
	// So we have to copy configuration and node map.
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
	pos, off := slot/2, 16*(slot&1)
	sh32 := atomic.LoadUint32(&cfg.slots[pos])
	sh16 := uint16((sh32 >> off) & 0x3fff)
	return sh16
}

// slotSetShard sets slot2shard mapping
func (cfg *clusterConfig) slotSetShard(slot, shard uint16) {
	pos, off := slot/2, 16*(slot&1)
	sh32 := atomic.LoadUint32(&cfg.slots[pos])
	if uint16((sh32>>off)&0x3fff) == shard {
		return
	}
	sh32 &^= 0xffff << off
	sh32 |= uint32(shard) << off
	// yep, we doesn't do any synchronization here.
	// If we lost update now, it will be naturally retried with other MOVED redis response.
	atomic.StoreUint32(&cfg.slots[pos], sh32)
}

func (cfg *clusterConfig) slotMarkAsking(slot uint16) {
	pos, off := slot/2, 16*(slot&1)
	sh32 := atomic.LoadUint32(&cfg.slots[pos])
	flag := uint32(masterOnlyFlag << off)
	if sh32&flag == 0 {
		sh32 |= flag
		// Again: no synchronization, because any updates will be retried with redis responses.
		atomic.StoreUint32(&cfg.slots[pos], sh32)
	}
}

func (cfg *clusterConfig) slotIsAsking(slot uint16) bool {
	pos, off := slot/2, 16*(slot&1)
	sh32 := atomic.LoadUint32(&cfg.slots[pos])
	flag := uint32(masterOnlyFlag << off)
	return sh32&flag != 0
}

func (cfg *clusterConfig) slot2shard(slot uint16) *shard {
	sh16 := cfg.slot2shardno(slot)
	shard := cfg.shards[sh16]
	return shard
}

// connForSlot returns established connection for slot, if it exists.
func (c *Cluster) connForSlot(slot uint16, policy ReplicaPolicyEnum, seen []*redisconn.Connection) (*redisconn.Connection, error) {
	var conn *redisconn.Connection
	cfg := c.getConfig()
	shard := cfg.slot2shard(slot)
	nodes := cfg.nodes

	if shard == nil {
		return nil, c.err(ErrClusterConfigEmpty).With(redis.EKSlot, slot)
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
			// with PreferSlaves policy, slaves are three times more preferred than master.
			// that is why master's partition is reduced from 3 to 1.
			n, a = n-2, 2
		}
		off := c.opts.RoundRobinSeed.Current()
		// First, we try already established connections.
		// If no one found, then connections thar are connecting at the moment are tried.
		for _, needState := range []int{needConnected, mayBeConnected} {
			mask := atomic.LoadUint32(&shard.good) // load health information
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
					// it is strange a bit, but lets ignore
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
		return nil, c.err(redis.ErrDial).With(redis.EKSlot, slot).With(EKPolicy, policy)
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

// getConn returns connection with desired "health", but without already seen(used) connections.
func (n *node) getConn(policy ConnHostPolicyEnum, liveness int, seen []*redisconn.Connection) *redisconn.Connection {
	if len(n.conns) == 1 {
		if isSeen(n.conns[0], seen) {
			return nil
		}
		if connHealthy(n.conns[0], liveness) {
			return n.conns[0]
		}
		return nil
	}
	if liveness == preferConnected {
		conn := n.getConnConcreteNeed(policy, needConnected, seen)
		if conn == nil {
			conn = n.getConnConcreteNeed(policy, mayBeConnected, seen)
		}
		return conn
	}
	return n.getConnConcreteNeed(policy, liveness, seen)
}

func (n *node) getConnConcreteNeed(policy ConnHostPolicyEnum, liveness int, seen []*redisconn.Connection) *redisconn.Connection {
	switch policy {
	case ConnHostPreferFirst:
		for _, conn := range n.conns {
			if isSeen(conn, seen) {
				continue
			}
			if connHealthy(conn, liveness) {
				return conn
			}
		}
	case ConnHostRoundRobin:
		off := atomic.AddUint32(&n.rr, 1)
		l := uint32(len(n.conns))
		mask := uint32(1)<<l - 1
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
			if connHealthy(conn, liveness) {
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
