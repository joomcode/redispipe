package rediscluster

type Policeman struct {
	*Cluster
	Policy MasterReplicaPolicyEnum
}

func (p Policeman) Send(req Request, cb Future, off uint64) {
	p.Cluster.SendWithPolicy(p.Policy, req, cb, off)
}

func (p Policeman) SendMany(reqs []Request, cb Future, off uint64) {
	for i, req := range reqs {
		p.Cluster.SendWithPolicy(p.Policy, req, cb, off+uint64(i))
	}
}

func (c *Cluster) WithPolicy(policy MasterReplicaPolicyEnum) Policeman {
	return Policeman{c, policy}
}
