// +build debugredis

package rediscluster

import "sync"

var DebugMtx sync.Mutex
var debugEvents = []string{}
var DebugDisable = false

func DebugEvent(ev string) {
	if !DebugDisable {
		DebugMtx.Lock()
		debugEvents = append(debugEvents, ev)
		DebugMtx.Unlock()
	}
}

func DebugEvents() []string {
	DebugMtx.Lock()
	defer DebugMtx.Unlock()
	return debugEvents
}

func DebugEventsReset() {
	DebugMtx.Lock()
	defer DebugMtx.Unlock()
	debugEvents = nil
}
