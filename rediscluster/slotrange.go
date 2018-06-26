package rediscluster

import (
	"bytes"
	"sync/atomic"
	"time"

	"sync"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
)

const MasterOnlyFlag = 0x4000

type nodesAndMigrating struct {
	addrs      map[string]struct{}
	migrating  map[uint16]struct{}
	slotRanges []redis.SlotsRange
}

func (c *Cluster) nodesAndSlotRanges() (nodesAndMigrating, error) {
	nodes := c.getConfig().nodes

	conns := make([]*redisconn.Connection, 0, len(nodes)*2)
	for _, node := range nodes {
		for _, conn := range node.conns {
			conns = append(conns, conn)
		}
	}

	responses := make([]interface{}, len(conns))
	var wg sync.WaitGroup
	wg.Add(len(conns))
	fut := redis.FuncFuture(func(res interface{}, i uint64) {
		responses[i] = res
		wg.Done()
	})

	for i, conn := range conns {
		conn.Send(redis.Req("CLUSTER NODES"), fut, uint64(i))
	}
	wg.Wait()

	type infosCnt struct {
		infos redis.ClusterInstanceInfos
		cnt   int
	}

	allInfos := make(map[uint64]infosCnt)
	for i, resp := range responses {
		infos, err := redis.ParseClusterInfo(resp)
		if err != nil {
			c.report(LogClusterSlotsError, conns[i], err)
			continue
		}
		hsh := infos.HashSum()
		ic := allInfos[hsh]
		ic.infos = infos
		ic.cnt++
		allInfos[hsh] = ic
	}
	if len(allInfos) == 0 {
		c.report(LogClusterSlotsError)
		return nodesAndMigrating{}, c.err(redis.ErrKindCluster, redis.ErrClusterSlots)
	}

	addrMap := make(map[string]struct{})
	moveMap := make(map[uint16]struct{})
	mostCommonMap := infosCnt{nil, -1}
	for _, ic := range allInfos {
		ic.infos.CollectAddressesAndMigrations(addrMap, moveMap)
		if ic.cnt > mostCommonMap.cnt {
			mostCommonMap = ic
		}
	}
	return nodesAndMigrating{
		addrs:      addrMap,
		migrating:  moveMap,
		slotRanges: mostCommonMap.infos.SlotsRanges(),
	}, nil
}

func (c *Cluster) updateMappings(nandm nodesAndMigrating) {
	shards := make(map[string][]string)
	for _, r := range nandm.slotRanges {
		shards[r.Addrs[0]] = r.Addrs
	}

	c.m.Lock()
	defer c.m.Unlock()

	oldConfig := c.getConfig()
	oldNodes := c.prevNodes
	c.prevNodes = oldConfig.nodes

	newConfig := *oldConfig
	newConfig.nodes = make(nodeMap, len(c.prevNodes))

	for addr := range nandm.addrs {
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
			newConfig.shards[shardno] = &shard{
				addr: addrs,
				good: (uint32(1) << uint(len(addrs))) - 1,
			}
		}
		newConfig.masters[addrs[0]] = shardno
		random = shardno
	}

	c.nodeWait.Lock()
	c.nodeWait.promises = nil
	c.nodeWait.Unlock()

	go newConfig.setConnRoles()

	var sh uint32
	ranges := nandm.slotRanges
	for i := 0; i < NumSlots; i++ {
		var cur uint32
		if len(ranges) != 0 && i > ranges[0].To {
			ranges = ranges[1:]
		}
		if len(ranges) == 0 || i < ranges[0].From {
			cur = uint32(random)
		} else {
			cur = uint32(newConfig.masters[ranges[0].Addrs[0]])
		}
		if _, ok := nandm.migrating[uint16(cur)]; ok {
			cur |= MasterOnlyFlag
		}
		if _, ok := c.externalForceMasterOnly[uint16(cur)]; ok {
			cur |= MasterOnlyFlag
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
					conn.SendBatch([]Request{{"READONLY", nil}, Request{"INFO", nil}},
						redis.FuncFuture(sh.setReplicaInfo), uint64(i*2))
				}
			}
		}
	}
}
