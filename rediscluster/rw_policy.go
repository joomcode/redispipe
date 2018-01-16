package rediscluster

type Policeman struct {
	*Cluster
	Policy MasterReplicaPolicyEnum
}

func (p Policeman) Send(req Request, cb Callback, off uint64) {
	p.Cluster.SendWithPolicy(p.Policy, req, cb, off)
}

func (c *Cluster) WithPolicy(policy MasterReplicaPolicyEnum) Policeman {
	return Policeman{c, policy}
}
