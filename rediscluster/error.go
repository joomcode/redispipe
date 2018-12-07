package rediscluster

import (
	"github.com/joomcode/errorx"
	"github.com/joomcode/redispipe/redis"
)

var (
	// ErrCluster - some cluster related errors.
	ErrCluster = redis.Errors.NewSubNamespace("cluster")
	// ErrClusterSlots - fetching slots configuration failed
	ErrClusterSlots = ErrCluster.NewType("retreive_slots")
	// ErrAddressNotResolved - address could not be resolved
	// Cluster resolves named hosts specified as start points. If this resolution fails, this error returned.
	ErrAddressNotResolved = ErrCluster.NewType("resolve_address")
	// ErrClusterConfigEmpty - no addresses found in config.
	ErrClusterConfigEmpty = ErrCluster.NewType("config_empty")
	// ErrNoAliveConnection - no alive connection to shard
	ErrNoAliveConnection = ErrCluster.NewType("no_alive_connection", redis.ErrTraitConnectivity)
)

var (
	// EKCluster - cluster for error
	EKCluster = errorx.RegisterProperty("cluster")
	// EKClusterName - cluster name
	EKClusterName = errorx.RegisterPrintableProperty("clusterName")
	// EKPolicy - policy used to choose between master and replicas.
	EKPolicy = errorx.RegisterPrintableProperty("policy")
)

func withNewProperty(err *errorx.Error, p errorx.Property, v interface{}) *errorx.Error {
	_, ok := err.Property(p)
	if ok {
		return err
	}
	return err.WithProperty(p, v)
}

func movedTo(err *errorx.Error) string {
	a, ok := err.Property(redis.EKMovedTo)
	if !ok {
		return ""
	}
	return a.(string)
}
