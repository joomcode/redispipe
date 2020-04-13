package redisclusterutil

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"github.com/joomcode/redispipe/redis"
)

// SlotMoving is a flag about direction of slot migration.
type SlotMoving byte

const (
	// SlotMigrating indicates slot is migrating from this instance.
	SlotMigrating SlotMoving = 1
	// SlotImporting indicates slot is importing into this instance.
	SlotImporting SlotMoving = 2
)

// SlotsRange represents slice of slots
type SlotsRange struct {
	From  int
	To    int
	Addrs []string // addresses of hosts hosting this range of slots. First address is a master, and other are slaves.
}

// ParseSlotsInfo parses result of CLUSTER SLOTS command
func ParseSlotsInfo(res interface{}) ([]SlotsRange, error) {
	const NumSlots = 1 << 14
	if err := redis.AsError(res); err != nil {
		return nil, err
	}

	errf := func(f string, args ...interface{}) ([]SlotsRange, error) {
		msg := fmt.Sprintf(f, args...)
		err := redis.ErrResponseUnexpected.New(msg)
		return nil, err
	}

	var rawranges []interface{}
	var ok bool
	if rawranges, ok = res.([]interface{}); !ok {
		return errf("type is not array: %+v", res)
	}
	if len(rawranges) == 0 {
		return errf("host doesn't know about slots (probably it is not in cluster)")
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
		r.From = int(i64)
		if i64, ok = rawrange[1].(int64); !ok || i64 < 0 || i64 >= NumSlots {
			return errf("format mismatch: res[%d][1]=%+v", i, rawrange[1])
		}
		r.To = int(i64)
		if r.From > r.To {
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
			r.Addrs = append(r.Addrs, string(host)+":"+strconv.Itoa(int(port)))
		}
		sort.Strings(r.Addrs[1:])
		ranges[i] = r
	}
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].From < ranges[j].From
	})
	return ranges, nil
}

// InstanceInfo represents line of CLUSTER NODES result.
type InstanceInfo struct {
	Uuid   string
	Addr   string
	IP     string
	Port   int
	Port2  int
	Fail   bool
	MySelf bool
	// NoAddr means that node were missed due to misconfiguration.
	// More probably, redis instance with other UUID were started on the same port.
	NoAddr    bool
	Handshake bool
	SlaveOf   string
	Slots     [][2]uint16
	Migrating []SlotMigration
}

// InstanceInfos represents CLUSTER NODES result
type InstanceInfos []InstanceInfo

// SlotMigration represents one migrating slot.
type SlotMigration struct {
	Number uint16
	Moving SlotMoving
	Peer   string
}

// HasAddr returns true if it is addressless instance (replaced with instance with other UUID),
// it will have no port
func (ii *InstanceInfo) HasAddr() bool {
	return !ii.NoAddr && ii.Port != 0
}

// AddrValid returns true if instance is successfully configure.
// Note that it could differ from HasAddr in some corner cases.
func (ii *InstanceInfo) AddrValid() bool {
	return ii.IP != "" && ii.Port != 0
}

// IsMaster returns if this instance is master
func (ii *InstanceInfo) IsMaster() bool {
	return ii.SlaveOf == ""
}

// HashSum calculates signature of cluster configuration.
// It assumes, configuration were sorted in some way.
// If configuration fetched from all hosts has same signature, then cluster is in stable state.
func (iis InstanceInfos) HashSum() uint64 {
	hsh := fnv.New64a()
	for _, ii := range iis {
		if !ii.AddrValid() && len(ii.Slots) == 0 { // looks like redis-cli also ignores hosts without slots
			continue
		}
		if ii.Handshake {
			continue
		}
		fmt.Fprintf(hsh, "%s\t%s\t%d\t%v\t%s", ii.Uuid, ii.Addr, ii.Port2, ii.Fail, ii.SlaveOf)
		for _, slots := range ii.Slots {
			fmt.Fprintf(hsh, "\t%d-%d", slots[0], slots[1])
		}
		hsh.Write([]byte("\n"))
	}
	return hsh.Sum64()
}

// CollectAddressesAndMigrations collects all node's addresses and all slot migrations.
func (iis InstanceInfos) CollectAddressesAndMigrations(addrs map[string]struct{}, migrating map[uint16]struct{}) {
	for _, ii := range iis {
		if ii.AddrValid() {
			addrs[ii.Addr] = struct{}{}
		}
		if migrating != nil {
			for _, m := range ii.Migrating {
				migrating[m.Number] = struct{}{}
			}
		}
	}
}

// SlotsRanges returns sorted SlotsRange-s made from slots information of cluster configuration.
func (iis InstanceInfos) SlotsRanges() []SlotsRange {
	uuid2addrs := make(map[string][]string)
	for _, ii := range iis {
		if !ii.AddrValid() {
			continue
		}
		if ii.IsMaster() {
			uuid2addrs[ii.Uuid] = append([]string{ii.Addr}, uuid2addrs[ii.Uuid]...)
		} else {
			uuid2addrs[ii.SlaveOf] = append(uuid2addrs[ii.SlaveOf], ii.Addr)
		}
	}
	ranges := make([]SlotsRange, 0, 16)
	for _, ii := range iis {
		if !ii.AddrValid() || !ii.IsMaster() || len(ii.Slots) == 0 {
			continue
		}
		for _, slots := range ii.Slots {
			ranges = append(ranges, SlotsRange{
				From:  int(slots[0]),
				To:    int(slots[1]),
				Addrs: uuid2addrs[ii.Uuid],
			})
		}
	}
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].From < ranges[j].From
	})
	return ranges
}

// MySelf returns info line for the host information were collected from.
func (iis InstanceInfos) MySelf() *InstanceInfo {
	for _, ii := range iis {
		if ii.MySelf {
			return &ii
		}
	}
	return nil
}

// MergeWith merges sorted cluster information, giving preference to myself lines.
// It could be used to obtain "union of all cluster configuration visions" in custom tools managing cluster.
func (iis InstanceInfos) MergeWith(other InstanceInfos) InstanceInfos {
	// assume they are sorted by uuid
	// common case : they are same
	if len(iis) == len(other) {
		res := make(InstanceInfos, len(iis))
		copy(res, iis)
		for i := range res {
			if res[i].Uuid != other[i].Uuid {
				goto RealMerge
			}
			if !res[i].MySelf && other[i].MySelf {
				res[i] = other[i]
			}
		}
		return res
	}
RealMerge:
	res := make(InstanceInfos, 0, len(iis))
	i, j := 0, 0
	for i < len(iis) && j < len(other) {
		if iis[i].Uuid == other[j].Uuid {
			if !other[j].MySelf {
				res = append(res, iis[i])
			} else {
				res = append(res, other[j])
			}
			i++
			j++
		} else if iis[i].Uuid < other[j].Uuid {
			res = append(res, iis[i])
			i++
		} else {
			res = append(res, other[j])
			j++
		}
	}
	if i < len(iis) {
		res = append(res, iis[i:]...)
	}
	if j < len(other) {
		res = append(res, iis[j:]...)
	}
	return res
}

// Hosts returns set of instance addresses.
func (iis InstanceInfos) Hosts() []string {
	res := make([]string, 0, len(iis))
	for i := range iis {
		if iis[i].AddrValid() {
			res = append(res, iis[i].Addr)
		}
	}
	return res
}

// ParseClusterNodes parses result of CLUSTER NODES command.
func ParseClusterNodes(res interface{}) (InstanceInfos, error) {
	var err error
	if err = redis.AsError(res); err != nil {
		return nil, err
	}

	errf := func(f string, args ...interface{}) (InstanceInfos, error) {
		msg := fmt.Sprintf(f, args...)
		err := redis.ErrResponseUnexpected.New(msg)
		return nil, err
	}

	infob, ok := res.([]byte)
	if !ok {
		return errf("type is not []bytes, but %t", res)
	}
	info := string(infob)
	lines := strings.Split(info, "\n")
	infos := InstanceInfos{}
	for _, line := range lines {
		if len(line) < 16 {
			continue
		}
		parts := strings.Split(line, " ")
		ipp := strings.Split(parts[1], "@")
		addrparts := strings.Split(ipp[0], ":")
		if len(ipp) != 2 || len(addrparts) != 2 {
			return errf("ip-port is not in 'ip:port@port2' format, but %q", line)
		}
		node := InstanceInfo{
			Uuid: parts[0],
			Addr: ipp[0],
		}
		node.IP = addrparts[0]
		node.Port, _ = strconv.Atoi(addrparts[1])
		node.Port2, _ = strconv.Atoi(ipp[1])

		node.Fail = strings.Contains(parts[2], "fail")
		if strings.Contains(parts[2], "slave") {
			node.SlaveOf = parts[3]
		}
		node.NoAddr = strings.Contains(parts[2], "noaddr")
		node.MySelf = strings.Contains(parts[2], "myself")
		node.Handshake = strings.Contains(parts[2], "handshake")

		for _, slot := range parts[8:] {
			if slot[0] == '[' {
				var uuid string
				var slotn int
				dir := SlotImporting

				if ix := strings.Index(slot, "-<-"); ix != -1 {
					slotn, err = strconv.Atoi(slot[1:ix])
					if err != nil {
						return errf("slot number is not an integer: %q", slot[1:ix])
					}
					uuid = slot[ix+3 : len(slot)-1]
				} else if ix = strings.Index(slot, "->-"); ix != -1 {
					slotn, err = strconv.Atoi(slot[1:ix])
					if err != nil {
						return errf("slot number is not an integer: %q", slot[1:ix])
					}
					uuid = slot[ix+3 : len(slot)-1]
					dir = SlotMigrating
				}
				migrating := SlotMigration{
					Number: uint16(slotn),
					Moving: dir,
					Peer:   uuid,
				}
				node.Migrating = append(node.Migrating, migrating)
			} else if ix := strings.IndexByte(slot, '-'); ix != -1 {
				from, err := strconv.Atoi(slot[:ix])
				if err != nil {
					return errf("slot number is not an integer: %q", slot)
				}
				to, err := strconv.Atoi(slot[ix+1:])
				if err != nil {
					return errf("slot number is not an integer: %q", slot)
				}
				node.Slots = append(node.Slots, [2]uint16{uint16(from), uint16(to)})
			} else {
				slotn, err := strconv.Atoi(slot)
				if err != nil {
					return errf("slot number is not an integer: %q", slot)
				}
				node.Slots = append(node.Slots, [2]uint16{uint16(slotn), uint16(slotn)})
			}
		}
		infos = append(infos, node)
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Uuid < infos[j].Uuid
	})
	return infos, nil
}
