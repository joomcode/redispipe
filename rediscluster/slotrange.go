package rediscluster

import (
	"bytes"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redis"
)

func (c *Cluster) nodesAndSlotRanges() (nodesAndMigrating, []redis.SlotsRange, error) {
	nodes := c.getConfig().nodes
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
					ress := redis.Sync{conn}.SendMany([]Request{
						redis.Req("CLUSTER NODES"),
						redis.Req("CLUSTER SLOTS"),
					})
					nandm, err := parseNodes(ress[0])
					if err != nil {
						c.report(LogClusterSlotsError, conn, err)
						continue
					}
					slotsres, err := redis.ParseSlotsInfo(ress[1])
					if err != nil {
						c.report(LogClusterSlotsError, conn, err)
						continue
					}
					return nandm, slotsres, nil
				}
			}
		}
	}
	c.report(LogClusterSlotsError)
	return nodesAndMigrating{}, nil, c.err(redis.ErrKindCluster, redis.ErrClusterSlots)
}

type nodesAndMigrating struct {
	addrs     []string
	migrating map[uint16]struct{}
}

func parseNodes(res interface{}) (nodesAndMigrating, error) {
	if err := redis.AsError(res); err != nil {
		return nodesAndMigrating{}, err
	}
	buf, ok := res.([]byte)
	if !ok {
		return nodesAndMigrating{}, redis.NewErrMsg(redis.ErrKindResponse, redis.ErrResponseUnexpected, "CLUSTER NODES returns not string").With("response", res)
	}

	lines := bytes.Split(buf, []byte("\n"))
	result := nodesAndMigrating{
		addrs:     make([]string, 0, len(lines)),
		migrating: make(map[uint16]struct{}),
	}

	for _, line := range lines {
		parts := bytes.Split(line, []byte(" "))
		if len(parts) >= 7 {
			addr := bytes.Split(parts[1], []byte("@"))[0]
			result.addrs = append(result.addrs, string(addr))
		} else if len(parts) > 1 || len(parts) == 1 && len(parts[0]) > 0 {
			return nodesAndMigrating{}, redis.NewErrMsg(redis.ErrKindResponse, redis.ErrResponseUnexpected, "CLUSTER NODES return is in unknown format").With("result", string(buf)).With("line", string(line)).With("parts", parts)
		}

		for i := 8; i < len(parts); i++ {
			p := parts[i]
			if p[0] != '[' {
				continue
			}
			t := bytes.IndexByte(p, '-')
			if t == -1 {
				// should we report error here ???
				continue
			}
			slot, err := strconv.Atoi(string(p[1:t]))
			if err != nil {
				// should we report error here too ???
				continue
			}
			result.migrating[uint16(slot)] = struct{}{}
		}
	}

	if len(result.migrating) == 0 {
		// to speedup lookup a bit
		result.migrating = nil
	}
	return result, nil
}

func (c *Cluster) updateMappings(nandm nodesAndMigrating, ranges []redis.SlotsRange) {
	shards := make(map[string][]string)
	for _, r := range ranges {
		shards[r.Addrs[0]] = r.Addrs
	}

	uniqaddrs := make(map[string]struct{})
	for _, addr := range nandm.addrs {
		uniqaddrs[addr] = struct{}{}
	}
	for _, s := range shards {
		for _, a := range s {
			uniqaddrs[a] = struct{}{}
		}
	}

	c.m.Lock()
	defer c.m.Unlock()

	oldConfig := c.getConfig()
	oldNodes := c.prevNodes
	c.prevNodes = oldConfig.nodes

	newConfig := *oldConfig
	newConfig.nodes = make(nodeMap, len(c.prevNodes))

	for addr := range uniqaddrs {
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
			cur |= 0x4000
		}
		if _, ok := c.externalForceMasterOnly[uint16(cur)]; ok {
			cur |= 0x4000
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
