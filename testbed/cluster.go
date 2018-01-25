package testbed

import (
	"bytes"
	"time"

	"github.com/joomcode/redispipe/rediscluster"
)

type Node struct {
	Server
	NodeId string
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
			"--cluster-node-timeout", "1000",
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
	time.Sleep(1 * time.Second)

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
	for !cl.ClusterOk() {
		time.Sleep(1 * time.Millisecond)
	}
}

func (cl *Cluster) ClusterOk() bool {
	for i := range cl.Node {
		res := cl.Node[i].Do("CLUSTER INFO")
		buf, ok := res.([]byte)
		if !ok {
			return false
		}
		if !bytes.Contains(buf, []byte("cluster_state:ok")) {
			return false
		}
	}
	return true
}

func (n *Node) SetupNodeId() {
	res := n.Do("CLUSTER NODES")
	lines := bytes.Split(res.([]byte), []byte{'\n'})
	for _, line := range lines {
		if bytes.Contains(line, []byte("myself")) {
			n.NodeId = string(bytes.Split(line, []byte(" "))[0])
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
