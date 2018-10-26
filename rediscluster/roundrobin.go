package rediscluster

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// RoundRobinSeed is the source of decision which replica to use for each particular request when
// replica-policy is MasterAndSlaves or PreferSlaves.
type RoundRobinSeed interface {
	// Current returns "deterministic random" value used for choosing replica.
	Current() uint32
}

// FairRoundRobinSeed implements RoundRobinSeed by returning new value every time using atomic increment.
// It doesn't works well in practice because it reduces pipeline efficiency.
// It is presented only as example.
type FairRoundRobinSeed struct{ v uint32 }

// Current implements RoundRobinSeed.Current method.
func (d *FairRoundRobinSeed) Current() uint32 {
	return atomic.AddUint32(&d.v, 1)
}

// TimedRoundRobindSeed is implementation of RoundRobinSeed.
// It runs goroutine which periodically stores new random value,
// and returns this value between this updates.
// It improves pipeline efficiency, and it is used as default implementation.
type TimedRoundRobinSeed struct {
	v    uint32
	stop uint32
}

// NewTimedRoundRobinSeed returns TimedRoundRobinSeed which updates its value every `interval`.
func NewTimedRoundRobinSeed(interval time.Duration) *TimedRoundRobinSeed {
	rr := &TimedRoundRobinSeed{}
	go func() {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		t := time.NewTicker(interval)
		defer t.Stop()
		for atomic.LoadUint32(&rr.stop) == 0 {
			<-t.C
			atomic.StoreUint32(&rr.v, rnd.Uint32())
		}
	}()
	return rr
}

// Current is implementation of RoundRobinSeed.Current.
// It returns same value during `interval` period.
func (rr *TimedRoundRobinSeed) Current() uint32 {
	return atomic.LoadUint32(&rr.v)
}

// Stop signals value changing goroutine to quit.
func (rr *TimedRoundRobinSeed) Stop() {
	atomic.StoreUint32(&rr.stop, 1)
}

var defaultSeed *TimedRoundRobinSeed
var defaultSeedOnce sync.Once

// DefaultRoundRobinSeed returns singleton of TimedRoundRobinSeed with random interval between 45ms and 100ms.
// Random interval used to
func DefaultRoundRobinSeed() *TimedRoundRobinSeed {
	defaultSeedOnce.Do(func() {
		v := uint64(time.Now().UnixNano())
		v ^= (v<<40 | v>>24) ^ (v<<15 | v>>49)
		v ^= v >> 1
		defaultSeed = NewTimedRoundRobinSeed(time.Duration(45000+time.Now().UnixNano()%55000) * time.Microsecond)
	})
	return defaultSeed
}
