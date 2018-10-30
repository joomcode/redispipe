package rediscluster

// Policeman wraps Cluster and change default policy for Send and SendMany methods.
// Policeman implements redis.Sender.
type Policeman struct {
	*Cluster
	// Policy is default policy for Send and SendMany
	Policy ReplicaPolicyEnum
}

// Send implements redis.Sender.Send
// It calls Cluster.SendWithPolicy with specified default policy.
func (p Policeman) Send(req Request, cb Future, off uint64) {
	p.Cluster.SendWithPolicy(p.Policy, req, cb, off)
}

// SendMany implements redis.Sender.SendMany
// It sends requests with specified default policy.
func (p Policeman) SendMany(reqs []Request, cb Future, off uint64) {
	for i, req := range reqs {
		p.Cluster.SendWithPolicy(p.Policy, req, cb, off+uint64(i))
	}
}

// WithPolicy returns Policeman with specified policy.
func (c *Cluster) WithPolicy(policy ReplicaPolicyEnum) Policeman {
	return Policeman{c, policy}
}
