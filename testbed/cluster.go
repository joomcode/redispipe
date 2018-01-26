package testbed

import (
	"bytes"
	"log"
	"time"

	"github.com/joomcode/redispipe/rediscluster"
)

type Node struct {
	Server
	NodeId []byte
}

type Cluster struct {
	Node [6]Node
}

func NewCluster(startport uint16) *Cluster {
	cl := &Cluster{}
	for i := range cl.Node {
		cl.Node[i].Port = startport + uint16(i)
		cl.Node[i].Args = []string{
			"--cluster-enabled", "yes",
			"--cluster-config-file", "node-" + cl.Node[i].PortStr() + ".conf",
			"--cluster-node-timeout", "100",
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
	cl.Node[2].AddSlots(11000, rediscluster.NumSlots-1)
	cl.Node[3].DoSure("CLUSTER REPLICATE", cl.Node[0].NodeId)
	cl.Node[4].DoSure("CLUSTER REPLICATE", cl.Node[1].NodeId)
	cl.Node[5].DoSure("CLUSTER REPLICATE", cl.Node[2].NodeId)
	cl.WaitClusterOk()

	return cl
}

func (cl *Cluster) Stop() {
	for i := range cl.Node {
		func() {
			defer recover()
			cl.Node[i].Stop()
		}()
	}
}

func (cl *Cluster) Start() {
	for i := range cl.Node {
		cl.Node[i].Start()
	}
	cl.WaitClusterOk()
}

func (cl *Cluster) WaitClusterOk() {
	i := 0
	for !cl.ClusterOk() {
		if i++; i == 10 {
			cl.AttemptFailover()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (cl *Cluster) ClusterOk() bool {
	stopped := []int{}
	for i := range cl.Node {
		if !cl.Node[i].RunningNow() {
			stopped = append(stopped, i)
		}
	}
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
		if masters != 3 {
			return false
		}
	}
	return true
}

func (cl *Cluster) AttemptFailover() {
	for i := range cl.Node {
		if !cl.Node[i].RunningNow() {
			slave := (i + 3) % 6
			log.Printf("FORCE FAILVER %d=>%d", cl.Node[i].Port, cl.Node[slave].Port)
			cl.Node[slave].Do("CLUSTER FAILOVER", "FORCE")
		}
	}
}

func (cl *Cluster) InitMoveSlot(slot, from, to int) {
	cl.Node[to].DoSure("CLUSTER SETSLOT", slot, "IMPORTING", cl.Node[from].NodeId)
	cl.Node[from].DoSure("CLUSTER SETSLOT", slot, "MIGRATING", cl.Node[to].NodeId)
}

func (cl *Cluster) CancelMoveSlot(slot int) {
	for i := 0; i < 3; i++ {
		cl.Node[i].DoSure("CLUSTER SETSLOT", slot, "STABLE")
	}
}

func (cl *Cluster) FinishMoveSlot(slot, to int) {
	for i := 0; i < 3; i++ {
		cl.Node[i].DoSure("CLUSTER SETSLOT", slot, "NODE", cl.Node[to].NodeId)
	}
}

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
	cl.FinishMoveSlot(slot, to)

	cl.WaitClusterOk()
}

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

func (n *Node) AddSlots(from, to int) {
	var args = []interface{}{}
	for i := from; i <= to; i++ {
		args = append(args, i)
	}
	n.DoSure("CLUSTER ADDSLOTS", args...)
}
