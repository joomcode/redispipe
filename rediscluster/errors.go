package rediscluster

const (
	ErrNoAddressProvided = iota + 1
	ErrNoAliveFound
	ErrContextIsNil
	ErrClusterSlots
	ErrNoSlotKey
	ErrWrongRedirection
	ErrNoAliveHost

	ErrMalformedTransaction
)
