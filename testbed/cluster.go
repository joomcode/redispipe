package testbed

import (
	"bytes"
	"log"
	"time"

	"github.com/joomcode/redispipe/rediscluster/redisclusterutil"
)

// Node is wrapper for Server with its NodeId
type Node struct {
	Server
	NodeId []byte
}

// Cluster is a tool for starting/stopping redis cluster for tests.
type Cluster struct {
	Node []Node
}

// NewCluster instantiate cluster of 6 nodes (3 masters and 3 slaves).
// Master are on ports startport, startport+1, startport+2,
// and slaves are on ports startport+3, startport+4, startport+5
func NewCluster(startport uint16) *Cluster {
	cl := &Cluster{}
	cl.Node = make([]Node, 6)
	for i := range cl.Node {
		cl.Node[i].Port = startport + uint16(i)
		cl.Node[i].Args = []string{
			"--cluster-enabled", "yes",
			"--cluster-config-file", "node-" + cl.Node[i].PortStr() + ".conf",
			"--cluster-node-timeout", "200",
			"--cluster-slave-validity-factor", "1000",
			"--slave-serve-stale-data", "yes",
			"--cluster-require-full-coverage", "no",
		}
		cl.Node[i].Start()
		cl.Node[i].SetupNodeId()
		cl.Node[i].DoSure("CLUSTER SET-CONFIG-EPOCH", i+1)
	}
	for i := 0; i < 5; i++ {
		for j := i + 1; j < 6; j++ {
			cl.Node[i].DoSure("CLUSTER MEET", "127.0.0.1", cl.Node[j].Port)
		}
	}
	time.Sleep(1 * time.Second)
	cl.Node[0].AddSlots(0, 5499)
	cl.Node[1].AddSlots(5500, 10999)
	cl.Node[2].AddSlots(11000, redisclusterutil.NumSlots-1)
	cl.Node[3].DoSure("CLUSTER REPLICATE", cl.Node[0].NodeId)
	cl.Node[4].DoSure("CLUSTER REPLICATE", cl.Node[1].NodeId)
	cl.Node[5].DoSure("CLUSTER REPLICATE", cl.Node[2].NodeId)
	cl.WaitClusterOk()

	return cl
}

// Stop stops all cluster's servers
func (cl *Cluster) Stop() {
	for i := range cl.Node {
		func() {
			defer recover()
			cl.Node[i].Stop()
		}()
	}
}

// Start starts all cluster's servers
func (cl *Cluster) Start() {
	for i := range cl.Node {
		cl.Node[i].Start()
	}
	cl.WaitClusterOk()
}

// WaitClusterOk wait for cluster configuration to be stable.
func (cl *Cluster) WaitClusterOk() {
	i := 0
	for !cl.ClusterOk() {
		if i++; i == 10 {
			cl.AttemptFailover()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ClusterOk checks cluster configuration.
func (cl *Cluster) ClusterOk() bool {
	stopped := []int{}
	for i := range cl.Node {
		if !cl.Node[i].RunningNow() {
			stopped = append(stopped, i)
		}
	}
	var hashsum uint64
	for i := range cl.Node {
		if !cl.Node[i].RunningNow() {
			continue
		}
		res := cl.Node[i].Do("CLUSTER INFO")
		buf, ok := res.([]byte)
		if !ok {
			return false
		}
		if !bytes.Contains(buf, []byte("cluster_state:ok")) {
			return false
		}
		res = cl.Node[i].Do("INFO REPLICATION")
		buf, ok = res.([]byte)
		if !ok {
			return false
		}
		if !bytes.Contains(buf, []byte("role:master")) &&
			!bytes.Contains(buf, []byte("master_link_status:up")) {
			return false
		}
		res = cl.Node[i].Do("CLUSTER NODES")
		buf, ok = res.([]byte)
		if !ok {
			return false
		}
		masters := 0
		for _, line := range bytes.Split(buf, []byte("\n")) {
			hasStopped := false
			for _, j := range stopped {
				if bytes.HasPrefix(line, cl.Node[j].NodeId) {
					if !bytes.Contains(line, []byte("fail ")) {
						return false
					}
					hasStopped = true
				}
			}
			if !hasStopped && bytes.Contains(line, []byte("master")) && !bytes.Contains(line, []byte("fail")) {
				masters++
			}
		}
		if masters != 3+(len(cl.Node)-6) {
			return false
		}
		infos, _ := redisclusterutil.ParseClusterNodes(res)
		hash := infos.HashSum()
		if hash != hashsum && hashsum != 0 {
			return false
		}
		hashsum = hash
	}
	return true
}

// AttemptFailover tries to issue CLUSTER FAILOVER FORCE to slaves of falled masters.
// This is work around replication bug present in Redis till 4.0.9 (including)
func (cl *Cluster) AttemptFailover() {
	for i := range cl.Node[:6] {
		if !cl.Node[i].RunningNow() {
			slave := (i + 3) % 6
			log.Printf("FORCE FAILVER %d=>%d", cl.Node[i].Port, cl.Node[slave].Port)
			cl.Node[slave].Do("CLUSTER FAILOVER", "FORCE")
		}
	}
}

// InitMoveSlot issues start for slot migration.
func (cl *Cluster) InitMoveSlot(slot, from, to int) {
	cl.Node[to].DoSure("CLUSTER SETSLOT", slot, "IMPORTING", cl.Node[from].NodeId)
	cl.Node[from].DoSure("CLUSTER SETSLOT", slot, "MIGRATING", cl.Node[to].NodeId)
}

// CancelMoveSlot resets slot migration.
func (cl *Cluster) CancelMoveSlot(slot int) {
	for i := 0; i < 3; i++ {
		cl.Node[i].DoSure("CLUSTER SETSLOT", slot, "STABLE")
	}
}

// FinishMoveSlot finalizes slot migration
func (cl *Cluster) FinishMoveSlot(slot, from, to int) {
	cl.Node[to].Do("CLUSTER SETSLOT", slot, "NODE", cl.Node[to].NodeId)
	cl.Node[from].Do("CLUSTER SETSLOT", slot, "NODE", cl.Node[to].NodeId)
	cl.Node[to].Do("CLUSTER BUMPEPOCH", "BROADCAST") // proprietary extention
	cl.Node[to].Do("CLUSTER BUMPEPOCH")
}

// MoveSlot moves slot's keys from host to host.
func (cl *Cluster) MoveSlot(slot, from, to int) {
	cl.InitMoveSlot(slot, from, to)
	for {
		keysi := cl.Node[from].DoSure("CLUSTER GETKEYSINSLOT", slot, 100)
		keys := keysi.([]interface{})
		if len(keys) == 0 {
			break
		}
		args := []interface{}{"127.0.0.1", cl.Node[to].Port, nil, 0, 5000, "REPLACE", "KEYS"}
		args = append(args, keys...)
		cl.Node[from].DoSure("MIGRATE", args...)
	}
	cl.FinishMoveSlot(slot, from, to)

	cl.WaitClusterOk()
}

// StartSeventhNode start additional node
func (cl *Cluster) StartSeventhNode() {
	cl.Node = append(cl.Node, Node{})
	cl.Node[6].Port = cl.Node[0].Port + 6
	cl.Node[6].Args = []string{
		"--cluster-enabled", "yes",
		"--cluster-config-file", "node-" + cl.Node[6].PortStr() + ".conf",
		"--cluster-node-timeout", "200",
		"--cluster-slave-validity-factor", "1000",
		"--slave-serve-stale-data", "yes",
		"--cluster-require-full-coverage", "no",
	}
	cl.Node[6].Start()
	cl.Node[6].SetupNodeId()
	cl.Node[6].DoSure("CLUSTER SET-CONFIG-EPOCH", 0)
	for i := 0; i < 6; i++ {
		cl.Node[i].DoSure("CLUSTER MEET", "127.0.0.1", cl.Node[6].Port)
	}
	time.Sleep(1 * time.Second)
	cl.WaitClusterOk()
}

// StopSeventhNode stops additional node
func (cl *Cluster) StopSeventhNode() {
	cl.Node[6].Stop()
	cl.Node = cl.Node[:6]
}

// SetupNodeId learns nodeid of this node
func (n *Node) SetupNodeId() {
	res := n.Do("CLUSTER NODES")
	lines := bytes.Split(res.([]byte), []byte{'\n'})
	for _, line := range lines {
		if bytes.Contains(line, []byte("myself")) {
			n.NodeId = bytes.Split(line, []byte(" "))[0]
			break
		}
	}
}

// AddSlots issues CLUSTER ADDSLOTS command.
func (n *Node) AddSlots(from, to int) {
	var args = []interface{}{}
	for i := from; i <= to; i++ {
		args = append(args, i)
	}
	n.DoSure("CLUSTER ADDSLOTS", args...)
}
