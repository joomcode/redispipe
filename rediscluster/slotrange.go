package rediscluster

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
)

const masterOnlyFlag = 0x4000

func (c *Cluster) slotRangesAndInternalMasterOnly() ([]redisclusterutil.SlotsRange, error) {
	nodes := c.getConfig().nodes

	var ranges []redisclusterutil.SlotsRange
	var err error
Outter:
	for _, node := range nodes {
		for _, conn := range node.conns {
			resp := redis.Sync{conn}.Do("CLUSTER SLOTS")
			ranges, err = redisclusterutil.ParseSlotsInfo(resp)
			if err == nil {
				break Outter
			}
			c.report(LogClusterSlotsError{Conn: conn, Error: err})
			continue
		}
	}
	if err != nil {
		c.report(LogSlotRangeError{})
		return nil, c.err(ErrClusterSlots)
	}

	// look for reminder about future migrations
	internalForce, internalForceSet, _ := redisclusterutil.RequestMasterOnly(c, "")
	c.m.Lock()
	if internalForceSet {
		c.internallyForceMasterOnly = internalForce
	}
	c.m.Unlock()

	return ranges, nil
}

func (c *Cluster) updateMappings(slotRanges []redisclusterutil.SlotsRange) {
	shards := make(map[string][]string)
	for _, r := range slotRanges {
		shards[r.Addrs[0]] = r.Addrs
	}

	addrs := make(map[string]struct{})
	for _, rng := range slotRanges {
		for _, addr := range rng.Addrs {
			addrs[addr] = struct{}{}
		}
	}

	c.m.Lock()
	defer c.m.Unlock()

	oldConfig := c.getConfig()
	oldNodes := c.prevNodes
	c.prevNodes = oldConfig.nodes

	newConfig := *oldConfig
	newConfig.nodes = make(nodeMap, len(c.prevNodes))

	for addr := range addrs {
		if node, ok := c.prevNodes[addr]; ok {
			atomic.AddUint32(&node.refcnt, 1)
			newConfig.nodes[addr] = node
		} else if node, ok := oldNodes[addr]; ok {
			atomic.AddUint32(&node.refcnt, 1)
			newConfig.nodes[addr] = node
		} else {
			node, _ = c.newNode(addr, false)
			newConfig.nodes[addr] = node
		}
	}

	newConfig.shards = make(shardMap, len(oldConfig.shards))
	newConfig.masters = make(masterMap, len(oldConfig.masters))

	var random uint16
	for master, addrs := range shards {
		shardno := uint16(len(newConfig.shards))

		oldshard := func() *shard {
			var ok bool
			var oldnum uint16
			oldnum, ok = oldConfig.masters[master]
			if !ok {
				return nil
			}
			sh, ok := oldConfig.shards[oldnum]
			if !ok {
				return nil
			}
			if len(addrs) != len(sh.addr) {
				return nil
			}
			for i, addr := range addrs {
				if sh.addr[i] != addr {
					return nil
				}
			}
			return sh
		}()

		if oldshard != nil {
			newConfig.shards[shardno] = oldshard
		} else {
			shard := &shard{
				addr:        addrs,
				good:        (uint32(1) << uint(len(addrs))) - 1,
				pingWeights: make([]uint32, len(addrs)),
			}
			newConfig.shards[shardno] = shard
			for i := range shard.pingWeights {
				shard.pingWeights[i] = 1
			}
		}
		newConfig.masters[addrs[0]] = shardno
		random = shardno
	}

	c.nodeWait.Lock()
	c.nodeWait.promises = make(map[string]*[]connThen, 1)
	c.nodeWait.Unlock()

	go newConfig.setConnRoles()

	var sh uint32
	for i := 0; i < redisclusterutil.NumSlots; i++ {
		var cur uint32
		if len(slotRanges) != 0 && i > slotRanges[0].To {
			slotRanges = slotRanges[1:]
		}
		if len(slotRanges) == 0 || i < slotRanges[0].From {
			cur = uint32(random)
		} else {
			cur = uint32(newConfig.masters[slotRanges[0].Addrs[0]])
		}
		if _, ok := c.internallyForceMasterOnly[uint16(i)]; ok {
			cur |= masterOnlyFlag
			DebugEvent("automatic masteronly")
		}
		if i&1 == 0 {
			sh = cur
		} else {
			sh |= cur << 16
			newConfig.slots[i/2] = sh
		}
	}

	c.storeConfig(&newConfig)

	time.AfterFunc(3*time.Millisecond, func() {
		for _, node := range oldNodes {
			if atomic.AddUint32(&node.refcnt, ^uint32(0)) != 0 {
				continue
			}
			for _, conn := range node.conns {
				conn.Close()
			}
		}
	})
	time.AfterFunc(8*time.Millisecond, func() {
		for _, node := range newConfig.nodes {
			node.updatePingLatency()
		}
		for _, shard := range newConfig.shards {
			sumLatency := uint32(0)
			for _, addr := range shard.addr {
				node := newConfig.nodes[addr]
				sumLatency += atomic.LoadUint32(&node.ping)
			}
			for i, addr := range shard.addr {
				node := newConfig.nodes[addr]
				weight := sumLatency / atomic.LoadUint32(&node.ping)
				atomic.StoreUint32(&shard.pingWeights[i], weight)
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

func (cfg *clusterConfig) setConnRoles() {
	for _, sh := range cfg.shards {
		for i, addr := range sh.addr {
			node := cfg.nodes[addr]
			if node == nil {
				continue
			}
			for _, conn := range node.conns {
				if i == 0 {
					conn.Send(Request{"READWRITE", nil}, nil, 0)
				} else {
					conn.SendBatch([]Request{{"READONLY", nil}, {"INFO", nil}},
						redis.FuncFuture(sh.setReplicaInfo), uint64(i*2))
				}
			}
		}
	}
}
