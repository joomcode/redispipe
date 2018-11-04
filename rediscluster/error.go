package rediscluster

import "github.com/joomcode/redispipe/redis"

var (
	// ErrCluster - some cluster related errors.
	ErrCluster = redis.NewErrorKind("ErrCluster", "cluster related error")
	// ErrClusterSlots - fetching slots configuration failed
	ErrClusterSlots = ErrCluster.SubKind("ErrClusterSlots", "could not retrieve slots from redis")
	// ErrAddressNotResolved - address could not be resolved
	// Cluster resolves named hosts specified as start points. If this resolution fails, this error returned.
	ErrAddressNotResolved = ErrCluster.SubKind("ErrAddressNotResolved", "address is not resolved.")
	// ErrClusterConfigEmpty - no addresses found in config.
	ErrClusterConfigEmpty = ErrCluster.SubKind("ErrClusterConfigEmpty", "cluster configuration is emptry.")
)

var (
	// EKCluster - cluster for error
	EKCluster = redis.NewErrorKey("cluster")
	// EKAddress - set when no alive connection found for address
	EKAddress = redis.NewErrorKey("address")
	// EKPolicy - policy used to choose between master and replicas.
	EKPolicy = redis.NewErrorKey("policy")
)
