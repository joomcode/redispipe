// +build debugredis

package rediscluster

import "sync"

var DebugMtx sync.Mutex
var DebugEvents = []string{}
var DebugDisable = false

func DebugEvent(ev string) {
	if !DebugDisable {
		DebugMtx.Lock()
		DebugEvents = append(DebugEvents, ev)
		DebugMtx.Unlock()
	}
}
