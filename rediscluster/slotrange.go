package rediscluster

import (
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/joomcode/redispipe/rediswrap"
)

type SlotsRange struct {
	from  int
	to    int
	addrs []string
}

func (c *Cluster) SlotRanges() ([]SlotsRange, error) {
	nodes := c.getNodeMap()
	for _, node := range nodes {
		for _, conn := range node.conns {
			sync := rediswrap.Sync{conn}
			res := sync.Send(Request{"CLUSTER SLOTS", nil})
			slotsres, err := ParseSlotsInfo(res, c)
			if err == nil {
				return slotsres, nil
			}
			c.opts.Logger.Report(LogClusterSlotsError, c, conn, err)
		}
	}
	c.opts.Logger.Report(LogClusterSlotsError, c)
	return nil, c.err(ErrClusterSlots).WithMsg("Couldn't retreive slots map")
}

func ParseSlotsInfo(res Result, cl *Cluster) ([]SlotsRange, error) {
	if err := res.AnyError(); err != nil {
		return nil, cl.err(ErrClusterSlots).WithWrap(err)
	}

	errf := func(f string, args ...interface{}) ([]SlotsRange, error) {
		return nil, cl.err(ErrClusterSlots).
			WithMsg(fmt.Sprintf("CLUSTER SLOTS "+f, args...))
	}

	var rawranges []interface{}
	var ok bool
	if rawranges, ok = res.Value().([]interface{}); !ok {
		return errf("type is not array: %+v", res.Value())
	}

	ranges := make([]SlotsRange, len(rawranges))
	for i, rawelem := range rawranges {
		var rawrange []interface{}
		var i64 int64
		r := SlotsRange{}
		if rawrange, ok := rawelem.([]interface{}); !ok || len(rawrange) < 3 {
			return errf("format mismatch: res[%d]=%+v (%+v)", i, rawelem, rawranges)
		}
		if i64, ok := rawrange[0].(int64); !ok || i64 < 0 || i64 >= NumSlots {
			return errf("format mismatch: res[%d][0]=%+v (%+v)", i, rawrange[0], rawranges)
		}
		r.from = int(i64)
		if i64, ok := rawrange[1].(int64); !ok || i64 < 0 || i64 >= NumSlots {
			return errf("format mismatch: res[%d][1]=%+v (%+v)", i, rawrange[1], rawranges)
		}
		r.to = int(i64)
		if r.from > r.to {
			return errf("range wrong: res[%d]=%+v (%+v)", i, rawrange, rawranges)
		}
		for j := 2; j < len(rawrange); j++ {
			rawaddr, ok := rawrange[j].([]interface{})
			if !ok || len(rawaddr) < 2 {
				return errf("address format mismatch: res[%d][%d] = %+v (%+v)",
					i, j, rawrange[j])
			}
			host, ok := rawaddr[0].(string)
			port, ok2 := rawaddr[1].(int64)
			if !ok || !ok2 || port <= 0 || port+10000 > 65535 {
				return errf("address format mismatch: res[%d][%d] = %+v (%+v)",
					i, j, rawaddr)
			}
			r.addrs = append(r.addrs, host+":"+strconv.Itoa(int(port)))
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

	oldNodes := c.getNodeMap()
	tmpNodes := make(nodeMap, len(oldNodes))
	newNodes := make(nodeMap, len(uniqaddrs))
	var delNodes []*node

	for a, n := range oldNodes {
		tmpNodes[a] = n
	}
	for addr := range uniqaddrs {
		if node, ok := oldNodes[addr]; ok {
			newNodes[addr] = node
		} else {
			node = c.newNode(addr)
			tmpNodes[addr] = node
			newNodes[addr] = node
		}
	}
	for a, n := range oldNodes {
		if _, ok := newNodes[a]; !ok {
			delNodes = append(delNodes, n)
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

	var random uint32
	for master, addrs := range shards {
		shardn, ok := func() (uint32, bool) {
			oldnum, ok := oldMasters[master]
			if !ok {
				return 0, false
			}
			oldaddrs, ok := oldShards[oldnum]
			if !ok {
				return 0, false
			}
			if len(addrs) != len(oldaddrs) {
				return 0, false
			}
			for i, addr := range addrs {
				if oldaddrs[i] != addr {
					return 0, false
				}
			}
			return oldnum, ok
		}()
		if !ok {
			for {
				shardn = atomic.AddUint32(&c.nextShard, 1) - 1
				if _, ok := tmpShards[shardn]; !ok {
					break
				}
			}
			tmpShards[shardn] = addrs
		}
		newMasters[master] = shardn
		newShards[shardn] = addrs
		random = shardn
	}

	c.nodeMap.Store(tmpNodes)
	c.shardMap.Store(tmpShards)
	c.masterMap.Store(newMasters)

	go c.setConnRoles(newShards)

	prevslot := 0
	for _, r := range ranges {
		for i := prevslot; i < r.from; i++ {
			atomic.StoreUint32(&c.slotMap[i], random)
		}
		shardn := newMasters[r.addrs[0]]
		for i := r.from; i <= r.to; i++ {
			atomic.StoreUint32(&c.slotMap[i], shardn)
		}
		prevslot = r.to + 1
	}
	for i := prevslot; i < NumSlots; i++ {
		atomic.StoreUint32(&c.slotMap[i], random)
	}

	c.shardMap.Store(newShards)
	c.nodeMap.Store(newNodes)

	go func() {
		time.Sleep(1)
		c.m.Lock()
		if c.getShardMap() == tmpShards {
		}
		if c.getNodeMap() == tmpNodes {
		}
	}()
}

func (c *Cluster) setConnRoles(shards shardMap) {
	for _, addrs := range shards {
		for i, addr := range addrs {
			node := c.getNode(addr)
			if node == nil {
				continue
			}
			for _, conn := range node.conns {
				if i == 0 {
					conn.Send(Request{"READWRITE", nil}, nil, 0)
				} else {
					conn.Send(Request{"READONLY", nil}, nil, 0)
				}
			}
		}
	}
}
