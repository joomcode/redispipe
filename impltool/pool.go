package impltool

import "sync/atomic"

const shardN = 16
const capa = 2048

var shardn uint32
var chans []chan func()

func init() {
	chans = make([]chan func(), shardN)
	for i := range chans {
		ch := make(chan func(), capa)
		chans[i] = ch
		go worker(ch)
	}

}

func worker(ch chan func()) {
	for f := range ch {
		f()
	}
}

func Go(f func()) {
	i := atomic.AddUint32(&shardn, 1)
	select {
	case chans[i%shardN] <- f:
	default:
		m := NextRng(&i, shardN)
		n := NextRng(&i, shardN-1)
		select {
		case chans[m%shardN] <- f:
		case chans[(m+1+n)%shardN] <- f:
		}
	}
}
