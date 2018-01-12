package rediscluster

import (
	"sync/atomic"
	"time"

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
	return c.getNodeMap()[addr]
}

type ClusterHandle struct {
	Handle  interface{}
	Address string
	N       int
}

func (c *Cluster) newNode(addr string) *node {
	node := &node{
		opts:  c.opts.HostOpts,
		addr:  addr,
		known: 1,
	}
	node.opts.Async = true
	node.conns = make([]*redisconn.Connection, c.opts.ConnsPerHost)
	for i := range node.conns {
		node.opts.Handle = ClusterHandle{c.opts.Handle, addr, i}
		var err error
		node.conns[i], err = redisconn.Connect(c.ctx, addr, node.opts)
		if err != nil {
			// since we are connected in async mode, there are should no be
			// errors. If there is error, it is configuration error.
			panic(err)
		}
	}
	return node
}

func (c *Cluster) replaceNodes(addrs []string, mayclose chan struct{}) {
	old := c.getNodeMap()

	new := make(nodeMap, len(old))
	for _, addr := range addrs {
		if node, ok := old[addr]; ok {
			new[addr] = node
		}
	}

	for _, addr := range addrs {
		if _, ok := new[addr]; ok {
			continue
		}
		new[addr] = c.newNode(addr)
	}
	c.nodeMap.Store(new)

	go func() {
		<-mayclose
		// Concervatively close unknown nodes
		time.Sleep(1)
		for addr, node := range old {
			if _, ok := new[addr]; ok {
				continue
			}
			for _, conn := range node.conns {
				conn.Close()
			}
		}
	}()
}

func (c *Cluster) addNode(addr string) *node {
	addrs := c.getNodeMap()
	if node, ok := addrs[addr]; ok {
		return node
	}

	c.m.Lock()
	defer c.m.Unlock()

	addrs = c.getNodeMap()
	if node, ok := addrs[addr]; ok {
		return node
	}

	new := make(nodeMap, len(addrs))
	for a, node := range addrs {
		new[a] = node
	}
	node := c.newNode(addr)
	new[addr] = node

	c.nodeMap.Store(new)

	return node
}

// add shard after MOVED response
// returns shardnum, and was it updated or not
func (c *Cluster) addShard(addr string, slot uint16) uint32 {
	oldMasters := c.getMasterMap()

	if shardn, ok := oldMasters[addr]; ok {
		if atomic.LoadUint32(&c.slotMap[slot]) != shardn {
			c.m.Lock()
			defer c.m.Unlock()
			atomic.StoreUint32(&c.slotMap[slot], shardn)
		}
		return shardn
	}

	c.m.Lock()
	defer c.m.Unlock()

	oldMasters = c.getMasterMap()

	if shardn, ok := oldMasters[addr]; ok {
		atomic.StoreUint32(&c.slotMap[slot], shardn)
		return shardn
	}

	oldShards := c.getShardMap()
	newMasters := make(masterMap, len(oldMasters)+1)
	newShards := make(shardMap, len(oldShards)+1)

	for a, n := range oldMasters {
		newMasters[a] = n
	}
	for n, a := range oldShards {
		newShards[n] = a
	}

	var shardn uint32
	for {
		shardn = atomic.AddUint32(&c.nextShard, 1) - 1
		if _, ok := oldShards[shardn]; !ok {
			break
		}
	}

	newMasters[addr] = shardn
	newShards[shardn] = []string{addr}

	c.shardMap.Store(newShards)
	c.masterMap.Store(newMasters)

	atomic.StoreUint32(&c.slotMap[slot], shardn)

	return shardn
}
