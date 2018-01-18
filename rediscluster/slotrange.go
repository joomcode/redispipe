package rediscluster

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
	"github.com/joomcode/redispipe/resp"
)

type SlotsRange struct {
	from  int
	to    int
	addrs []string
}

func syncSend(c *redisconn.Connection, r Request) interface{} {
	var wg sync.WaitGroup
	var res interface{}
	wg.Add(1)
	c.Send(r, func(r interface{}, _ uint64) {
		res = r
		wg.Done()
	}, 0)
	wg.Wait()
	return res
}

func (c *Cluster) SlotRanges() ([]SlotsRange, error) {
	nodes := c.getNodeMap()
	for _, node := range nodes {
		for _, conn := range node.conns {
			res := syncSend(conn, Request{"CLUSTER SLOTS", nil})
			slotsres, err := ParseSlotsInfo(res, c)
			if err == nil {
				return slotsres, nil
			}
			c.report(LogClusterSlotsError, conn, err)
		}
	}
	c.report(LogClusterSlotsError)
	return nil, redis.New(redis.ErrKindCluster, redis.ErrClusterSlots).With("cluster", c)
}

func ParseSlotsInfo(res interface{}, cl *Cluster) ([]SlotsRange, error) {
	if err := resp.Error(res); err != nil {
		return nil, err
	}

	errf := func(f string, args ...interface{}) ([]SlotsRange, error) {
		return nil, redis.NewMsg(redis.ErrKindResponse, redis.ErrResponseFormat,
			fmt.Sprintf(f, args...)).With("response", res)
	}

	var rawranges []interface{}
	var ok bool
	if rawranges, ok = res.([]interface{}); !ok {
		return errf("type is not array: %+v", res)
	}

	ranges := make([]SlotsRange, len(rawranges))
	for i, rawelem := range rawranges {
		var rawrange []interface{}
		var ok bool
		var i64 int64
		r := SlotsRange{}
		if rawrange, ok = rawelem.([]interface{}); !ok || len(rawrange) < 3 {
			return errf("format mismatch: res[%d]=%+v", i, rawelem)
		}
		if i64, ok = rawrange[0].(int64); !ok || i64 < 0 || i64 >= NumSlots {
			return errf("format mismatch: res[%d][0]=%+v", i, rawrange[0])
		}
		r.from = int(i64)
		if i64, ok = rawrange[1].(int64); !ok || i64 < 0 || i64 >= NumSlots {
			return errf("format mismatch: res[%d][1]=%+v", i, rawrange[1])
		}
		r.to = int(i64)
		if r.from > r.to {
			return errf("range wrong: res[%d]=%+v (%+v)", i, rawrange)
		}
		for j := 2; j < len(rawrange); j++ {
			rawaddr, ok := rawrange[j].([]interface{})
			if !ok || len(rawaddr) < 2 {
				return errf("address format mismatch: res[%d][%d] = %+v",
					i, j, rawrange[j])
			}
			host, ok := rawaddr[0].([]byte)
			port, ok2 := rawaddr[1].(int64)
			if !ok || !ok2 || port <= 0 || port+10000 > 65535 {
				return errf("address format mismatch: res[%d][%d] = %+v",
					i, j, rawaddr)
			}
			r.addrs = append(r.addrs, string(host)+":"+strconv.Itoa(int(port)))
		}
		sort.Strings(r.addrs[1:])
		ranges[i] = r
	}
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].from < ranges[j].from
	})
	return ranges, nil
}

func (c *Cluster) updateMappings(ranges []SlotsRange) {
	shards := make(map[string][]string)
	for _, r := range ranges {
		shards[r.addrs[0]] = r.addrs
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
			node = c.newNode(addr)
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

	c.nodeMap.Store(tmpNodes)
	c.shardMap.Store(tmpShards)
	c.masterMap.Store(newMasters)
	fmt.Println("store masters", newMasters)

	go c.setConnRoles(newShards)

	var sh uint32
	for i := 0; i < NumSlots; i++ {
		var cur uint32
		if len(ranges) != 0 && i > ranges[0].to {
			ranges = ranges[1:]
		}
		if len(ranges) == 0 || i < ranges[0].from {
			cur = uint32(random)
		} else {
			cur = uint32(newMasters[ranges[0].addrs[0]])
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
	if err := resp.Error(res); err != nil {
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
					conn.SendBatch([]Request{{"READONLY", nil}, Request{"INFO", nil}}, sh.setReplicaInfo, uint64(i*2))
				}
			}
		}
	}
}
