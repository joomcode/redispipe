package rediscluster

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redis"
)

func (c *Cluster) SlotRanges() ([]redis.SlotsRange, error) {
	nodes := c.getNodeMap()
	for _, searchConnected := range []bool{true, false} {
		for _, node := range nodes {
			for i := len(node.conns) - 1; i >= 0; i-- {
				conn := node.conns[i]
				var maySend bool
				if searchConnected {
					maySend = conn.ConnectedNow()
				} else {
					maySend = conn.MayBeConnected()
				}
				if maySend {
					res := redis.Sync{conn}.Do("CLUSTER SLOTS")
					slotsres, err := redis.ParseSlotsInfo(res)
					if err == nil {
						return slotsres, nil
					}
					c.report(LogClusterSlotsError, conn, err)
				}
			}
		}
	}
	c.report(LogClusterSlotsError)
	return nil, c.err(redis.ErrKindCluster, redis.ErrClusterSlots)
}

func (c *Cluster) updateMappings(ranges []redis.SlotsRange) {
	shards := make(map[string][]string)
	for _, r := range ranges {
		shards[r.Addrs[0]] = r.Addrs
	}

	uniqaddrs := make(map[string]struct{})
	for _, s := range shards {
		for _, a := range s {
			uniqaddrs[a] = struct{}{}
		}
	}

	c.m.Lock()
	defer c.m.Unlock()

	mayclose := make(chan struct{})
	defer close(mayclose)

	version := atomic.AddUint32(&c.version, 1)

	oldNodes := c.getNodeMap()
	tmpNodes := make(nodeMap, len(oldNodes))

	for a, n := range oldNodes {
		tmpNodes[a] = n
	}
	for addr := range uniqaddrs {
		if node, ok := oldNodes[addr]; ok {
			node.copyVersion(c)
		} else {
			node, _ = c.newNode(addr, false)
			tmpNodes[addr] = node
		}
	}

	oldShards := c.getShardMap()
	newShards := make(shardMap, len(oldShards))
	tmpShards := make(shardMap, len(oldShards))

	oldMasters := c.getMasterMap()
	newMasters := make(masterMap, len(oldMasters))

	for shardn, addrs := range oldShards {
		tmpShards[shardn] = addrs
	}

	var random uint16
	for master, addrs := range shards {
		var sh *shard
		shardn, ok := func() (uint16, bool) {
			var ok bool
			var oldnum uint16
			oldnum, ok = oldMasters[master]
			if !ok {
				return 0, false
			}
			sh, ok = oldShards[oldnum]
			if !ok {
				return 0, false
			}
			if len(addrs) != len(sh.addr) {
				return 0, false
			}
			for i, addr := range addrs {
				if sh.addr[i] != addr {
					return 0, false
				}
			}
			return oldnum, ok
		}()
		if !ok {
			for {
				shardn = c.nextShard
				c.nextShard++
				if _, ok := tmpShards[shardn]; !ok {
					break
				}
			}
			sh = &shard{addr: addrs, good: (uint32(1) << uint(len(addrs))) - 1}
			tmpShards[shardn] = sh
		}
		newMasters[master] = shardn
		newShards[shardn] = sh
		random = shardn
	}

	c.nodeWait.Lock()
	c.nodeWait.promises = nil
	c.nodeWait.Unlock()

	c.nodeMap.Store(tmpNodes)
	c.shardMap.Store(tmpShards)
	c.masterMap.Store(newMasters)

	go c.setConnRoles(newShards)

	var sh uint32
	for i := 0; i < NumSlots; i++ {
		var cur uint32
		if len(ranges) != 0 && i > ranges[0].To {
			ranges = ranges[1:]
		}
		if len(ranges) == 0 || i < ranges[0].From {
			cur = uint32(random)
		} else {
			cur = uint32(newMasters[ranges[0].Addrs[0]])
		}
		if i&1 == 0 {
			sh = cur
		} else {
			sh |= cur << 16
			atomic.StoreUint32(&c.slotMap[i/2], sh)
		}
	}

	time.AfterFunc(time.Millisecond, func() {
		newNodes := make(nodeMap, len(tmpNodes))
		delNodes := make([]*node, 0, len(tmpNodes))
		c.m.Lock()
		if c.version == version {
			c.shardMap.Store(newShards)
		}
		tmpNodes := c.getNodeMap()
		for a, n := range tmpNodes {
			if n.isOlder(version) {
				delNodes = append(delNodes, n)
			} else {
				newNodes[a] = n
			}
		}
		c.nodeMap.Store(newNodes)
		c.m.Unlock()
		for _, n := range delNodes {
			for _, conn := range n.conns {
				conn.Close()
			}
		}
	})
}

func (s *shard) setReplicaInfo(res interface{}, n uint64) {
	haserr := false
	if err := redis.AsError(res); err != nil {
		haserr = true
	} else if n&1 == 0 {
		str, ok := res.(string)
		haserr = !(ok && str == "OK")
	} else if buf, ok := res.([]byte); !ok {
		haserr = true
	} else if bytes.Contains(buf, []byte("master_link_status:down")) || bytes.Contains(buf, []byte("loading:1")) {
		haserr = true
	}
	for {
		oldstate := atomic.LoadUint32(&s.good)
		newstate := oldstate
		if haserr {
			newstate &^= 1 << (n / 2)
		} else {
			newstate |= 1 << (n / 2)
		}
		if newstate == oldstate {
			break
		}
		if atomic.CompareAndSwapUint32(&s.good, oldstate, newstate) {
			break
		}
	}
}

func (c *Cluster) setConnRoles(shards shardMap) {
	for _, sh := range shards {
		for i, addr := range sh.addr {
			node := c.getNode(addr)
			if node == nil {
				continue
			}
			for _, conn := range node.conns {
				if i == 0 {
					conn.Send(Request{"READWRITE", nil}, nil, 0)
				} else {
					conn.SendBatch([]Request{{"READONLY", nil}, Request{"INFO", nil}},
						redis.FuncFuture(sh.setReplicaInfo), uint64(i*2))
				}
			}
		}
	}
}
