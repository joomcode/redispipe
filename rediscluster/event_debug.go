// +build debugredis

package rediscluster

import "sync"

var DebugMtx sync.Mutex
var DebugEvents = []string{}

func DebugEvent(ev string) {
	DebugMtx.Lock()
	DebugEvents = append(DebugEvents, ev)
	DebugMtx.Unlock()
}
