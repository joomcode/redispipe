/*
Package rediscluster implements connector for redis cluster.

Cluster automatically learns and periodically refreshes cluster configuration.
It could send requests to slaves (if corresponding policy is used), and could retry
read requests within replicaset and write requests with connections to same master host
(if it is known that request were not sent).

It reacts on set CLUSTER_SELF:MASTER_ONLY stored in cluster itself to force master-only
policy on some slots. It is used by proprietary tool for correct and fast cluster
rebalancing.
*/
package rediscluster
