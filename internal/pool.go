package internal

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
		i = i*0x12345 + 1
		j := i ^ i>>16
		i = i*0x12345 + 1
		i ^= i >> 16
		select {
		case chans[i%shardN] <- f:
		case chans[j%shardN] <- f:
		}
	}
}
