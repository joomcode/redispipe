/*
Package rediscluster implements a connector for redis cluster.

Cluster automatically learns and periodically refreshes cluster configuration.
It could send requests to slaves (if a corresponding policy is used), and could retry
read requests within replicaset and write requests with connections to the same master host
(if it is known that requests were not sent).

It reacts on set CLUSTER_SELF:MASTER_ONLY stored in the cluster itself to force master-only
policy on some slots. It is used by proprietary tool for correct and fast cluster
rebalancing.
*/
package rediscluster
