package rediscluster

import (
	"bytes"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
)

const masterOnlyFlag = 0x4000

func (c *Cluster) slotRangesAndInternalMasterOnly() ([]redisclusterutil.SlotsRange, map[string]struct{}, error) {
	nodes := c.getConfig().nodes

	var ranges []redisclusterutil.SlotsRange
	var failedMasters map[string]struct{}
	var err error
Outter:
	for _, node := range nodes {
		for _, conn := range node.conns {
			resp := redis.Sync{conn}.Do("CLUSTER SLOTS")
			ranges, err = redisclusterutil.ParseSlotsInfo(resp)
			if err == nil {
				failedMasters = failedMastersOf(redis.Sync{conn}.Do("CLUSTER NODES"))
				break Outter
			}
			c.report(LogClusterSlotsError{Conn: conn, Error: err})
			continue
		}
	}
	if err != nil {
		c.report(LogSlotRangeError{})
		return nil, nil, c.err(ErrClusterSlots)
	}

	// look for reminder about future migrations
	internalForce, internalForceSet, _ := redisclusterutil.RequestMasterOnly(c, "")
	c.m.Lock()
	if internalForceSet {
		c.internallyForceMasterOnly = internalForce
	}
	c.m.Unlock()

	return ranges, failedMasters, nil
}

// failedMastersOf lists the masters the cluster agrees are down. A node's own
// suspicion (fail?) does not count. Without a readable CLUSTER NODES nothing counts,
// and the link-down tolerance alone decides.
func failedMastersOf(res interface{}) map[string]struct{} {
	infos, err := redisclusterutil.ParseClusterNodes(res)
	if err != nil {
		return nil
	}
	failed := map[string]struct{}{}
	for i := range infos {
		ii := &infos[i]
		if ii.IsMaster() && ii.Fail && !ii.PFail && ii.HasAddr() {
			failed[ii.Addr] = struct{}{}
		}
	}
	return failed
}

func (c *Cluster) updateMappings(slotRanges []redisclusterutil.SlotsRange, failedMasters map[string]struct{}) {
	shards := make(map[string][]string)
	for _, r := range slotRanges {
		shards[r.Addrs[0]] = r.Addrs
	}

	addrs := make(map[string]struct{})
	addrHostnames := make(map[string]string)
	for _, rng := range slotRanges {
		for _, addr := range rng.Addrs {
			addrs[addr] = struct{}{}
		}
		for addr, hostname := range rng.AddrHostnames {
			addrHostnames[addr] = hostname
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
			// For new nodes, addr is exposed with IP.
			// To make TLS work, try to get the hostname from addrHostnames.
			// This way, adding a discovered node behaves the same as in NewCluster.
			hostname := addr
			if h, ok := addrHostnames[addr]; ok {
				hostname = h
			}
			node, _ = c.newNode(hostname, addr, false)
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

		sh := oldshard
		if sh == nil {
			sh = &shard{
				addr:        addrs,
				good:        (uint32(1) << uint(len(addrs))) - 1,
				pingWeights: make([]uint32, len(addrs)),
			}
			for i := range sh.pingWeights {
				sh.pingWeights[i] = 1
			}
		}
		masterFailed := uint32(0)
		if _, ok := failedMasters[master]; ok {
			masterFailed = 1
		}
		atomic.StoreUint32(&sh.masterFailed, masterFailed)
		newConfig.shards[shardno] = sh
		newConfig.masters[addrs[0]] = shardno
		random = shardno
	}

	c.nodeWait.Lock()
	c.nodeWait.promises = make(map[string]*[]connThen, 1)
	c.nodeWait.Unlock()

	go newConfig.setConnRoles(c.opts.ReplicaLinkDownTolerance)

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
			minLatencyID := 0
			minLatency := uint32(math.MaxUint32)

			for i, addr := range shard.addr {
				node := newConfig.nodes[addr]
				pingLatency := atomic.LoadUint32(&node.ping)
				if pingLatency < minLatency {
					minLatency = pingLatency
					minLatencyID = i
				}

				sumLatency += pingLatency
			}
			for i, addr := range shard.addr {
				node := newConfig.nodes[addr]

				weight := sumLatency / atomic.LoadUint32(&node.ping)
				if atomic.LoadUint32(&c.forceMinLatencyReplica) == enabled && i == minLatencyID {
					const alwaysPrefer = 1_000_000
					weight = alwaysPrefer
				}

				atomic.StoreUint32(&shard.pingWeights[i], weight)
			}
		}
	})
}

func (s *shard) replicaInfoFuture(tolerance time.Duration) redis.FuncFuture {
	return func(res interface{}, n uint64) {
		s.setReplicaInfo(res, n, tolerance)
	}
}

func (s *shard) setReplicaInfo(res interface{}, n uint64, tolerance time.Duration) {
	haserr := false
	if err := redis.AsError(res); err != nil {
		haserr = true
	} else if n&1 == 0 {
		str, ok := res.(string)
		haserr = !(ok && str == "OK")
	} else if buf, ok := res.([]byte); !ok {
		haserr = true
	} else {
		haserr = !replicaHealthy(buf, tolerance, atomic.LoadUint32(&s.masterFailed) != 0)
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

// replicaHealthy tells whether INFO output describes a replica worth reading from.
// master_link_down_since_seconds is -1 for a replica that has never synced since it
// started, and its dataset is then anything from empty to the RDB it booted from.
// A replica cut off from a master the cluster has declared failed cannot fall further
// behind: nobody accepts writes for the shard until a new master is elected.
func replicaHealthy(info []byte, tolerance time.Duration, masterFailed bool) bool {
	if bytes.Contains(info, []byte("loading:1")) {
		return false
	}
	if !bytes.Contains(info, []byte("master_link_status:down")) {
		return true
	}
	since, ok := infoInt(info, "master_link_down_since_seconds")
	if !ok || since < 0 {
		return false
	}
	return masterFailed || time.Duration(since)*time.Second < tolerance
}

func infoInt(info []byte, field string) (int64, bool) {
	key := []byte("\n" + field + ":")
	i := bytes.Index(info, key)
	if i < 0 {
		return 0, false
	}
	value := info[i+len(key):]
	if end := bytes.IndexAny(value, "\r\n"); end >= 0 {
		value = value[:end]
	}
	v, err := strconv.ParseInt(string(value), 10, 64)
	return v, err == nil
}

func (cfg *clusterConfig) setConnRoles(tolerance time.Duration) {
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
						sh.replicaInfoFuture(tolerance), uint64(i*2))
				}
			}
		}
	}
}
