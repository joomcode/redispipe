package rediscluster

import (
	"runtime"
	"sync/atomic"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
)

func (c *Cluster) getNodeMap() nodeMap {
	return c.nodeMap.Load().(nodeMap)
}

func (c *Cluster) getShardMap() shardMap {
	return c.shardMap.Load().(shardMap)
}

func (c *Cluster) getMasterMap() masterMap {
	return c.masterMap.Load().(masterMap)
}

func (c *Cluster) getNode(addr string) *node {
	node := c.getNodeMap()[addr]
	if node != nil {
		node.copyVersion(c)
	}
	return node
}

type ClusterHandle struct {
	Handle  interface{}
	Address string
	N       int
}

func (c *Cluster) newNode(addr string, initial bool) (*node, error) {
	node := &node{
		opts:    c.opts.HostOpts,
		addr:    addr,
		version: atomic.LoadUint32(&c.version),
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
	addrs := c.getNodeMap()
	if node, ok := addrs[addr]; ok {
		node.copyVersion(c)
		return node
	}

	c.m.Lock()
	defer c.m.Unlock()

	addrs = c.getNodeMap()
	if node, ok := addrs[addr]; ok {
		node.copyVersion(c)
		return node
	}

	atomic.AddUint32(&c.version, 1)

	new := make(nodeMap, len(addrs))
	for a, node := range addrs {
		new[a] = node
	}
	node, _ := c.newNode(addr, false)
	new[addr] = node

	c.nodeMap.Store(new)

	return node
}

func (c *Cluster) slot2shardno(slot uint16) uint16 {
	sh32 := atomic.LoadUint32(&c.slotMap[slot/2])
	sh16 := uint16((sh32 >> (16 * (slot & 1))) & 0x3fff)
	return sh16
}

func (c *Cluster) slotSetShard(slot, shard uint16) {
	sh32 := atomic.LoadUint32(&c.slotMap[slot/2])
	sh32 &^= 0xffff << (16 * (slot & 1))
	sh32 |= uint32(shard) << (16 * (slot & 1))
	atomic.StoreUint32(&c.slotMap[slot/2], sh32)
}

func (c *Cluster) slotMarkAsking(slot uint16) {
	sh32 := atomic.LoadUint32(&c.slotMap[slot/2])
	flag := uint32(0x4000 << (16 * (slot & 1)))
	if sh32&flag == 0 {
		sh32 |= flag
		atomic.StoreUint32(&c.slotMap[slot/2], sh32)
	}
}

func (c *Cluster) slotIsAsking(slot uint16) bool {
	sh32 := atomic.LoadUint32(&c.slotMap[slot/2])
	flag := uint32(0x4000 << (16 * (slot & 1)))
	return sh32&flag != 0
}

func (c *Cluster) slot2shard(slot uint16) *shard {
	for {
		sh16 := c.slot2shardno(slot)
		shards := c.getShardMap()
		shard := shards[sh16]
		if shard != nil {
			return shard
		}
		runtime.Gosched()
	}
}

func (c *Cluster) connForSlot(slot uint16, policy ReplicaPolicyEnum, seen []*redisconn.Connection) (*redisconn.Connection, error) {
	// We are not synchronizing by locks, so we need to spin until we have
	// consistent configuration, ie for shard number we have a shard in a shardmap
	// and a node in a nodemap.
	var conn *redisconn.Connection
Loop:
	for {
		shard := c.slot2shard(slot)
		nodes := c.getNodeMap()
		var addr string
		switch policy {
		case MasterOnly:
			addr = shard.addr[0]
			node := nodes[addr]
			if node == nil {
				break /*switch*/
			}
			conn = node.getConn(c.opts.ConnHostPolicy, needConnected, seen)
			if conn == nil {
				conn = node.getConn(c.opts.ConnHostPolicy, mayBeConnected, seen)
			}
			break Loop
		case MasterAndSlaves, PreferSlaves:
			n, a := uint32(len(shard.addr))*3, uint32(0)
			if policy == PreferSlaves {
				n, a = n-2, 2
			}
			off := atomic.AddUint32(&shard.rr, 1)
			hadall := true
			for _, needState := range []int{needConnected, mayBeConnected} {
				mask := atomic.LoadUint32(&shard.good)
				for mask != 0 {
					k := (nextRng(&off, n) + a) / 3
					if mask&(1<<k) == 0 {
						// replica isn't healthy, or already viewed
						continue
					}
					mask &^= 1 << k
					addr = shard.addr[k]
					node := nodes[addr]
					if node == nil {
						hadall = false
						continue
					}
					conn = node.getConn(c.opts.ConnHostPolicy, needState, seen)
					if conn != nil {
						break Loop
					}
				}
			}
			if hadall {
				break Loop
			}
		default:
			panic("unknown policy")
		}
		runtime.Gosched()
	}
	if conn == nil {
		c.ForceReloading()
		return nil, c.err(redis.ErrKindConnection, redis.ErrDial).
			With("slot", slot).With("policy", policy)
	}
	return conn, nil
}

func (c *Cluster) connForAddress(addr string) *redisconn.Connection {
	node := c.getNode(addr)
	if node == nil {
		return nil
	}

	conn := node.getConn(c.opts.ConnHostPolicy, needConnected, nil)
	if conn == nil {
		conn = node.getConn(c.opts.ConnHostPolicy, mayBeConnected, nil)
	}
	return conn
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

func (n *node) copyVersion(c *Cluster) {
	cver := atomic.LoadUint32(&c.version)
	nver := atomic.LoadUint32(&n.version)
	if nver != cver {
		atomic.StoreUint32(&n.version, cver)
	}
}

func (n *node) isOlder(cver uint32) bool {
	nver := atomic.LoadUint32(&n.version)
	return (nver-cver)&0x80000000 != 0
}

func nextRng(state *uint32, mod uint32) uint32 {
	v := *state
	*state = v*0x12345 + 1
	return (v ^ v>>16) % mod
}
