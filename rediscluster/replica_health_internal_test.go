package rediscluster

import (
	"strings"
	"testing"
	"time"
)

func TestReplicaHealthy(t *testing.T) {
	info := func(lines ...string) []byte {
		return []byte("# Replication\r\nrole:slave\r\n" + strings.Join(lines, "\r\n") + "\r\n# Persistence\r\nloading:0\r\n")
	}
	for _, tc := range []struct {
		name              string
		info              []byte
		linkDownTolerance time.Duration
		want              bool
	}{
		{name: "link up", info: info("master_link_status:up"), linkDownTolerance: time.Minute, want: true},
		{name: "link down for a while", info: info("master_link_status:down", "master_link_down_since_seconds:3"), linkDownTolerance: time.Minute, want: true},
		{name: "link down for the whole linkDownTolerance", info: info("master_link_status:down", "master_link_down_since_seconds:60"), linkDownTolerance: time.Minute, want: false},
		{name: "never synced since start", info: info("master_link_status:down", "master_link_down_since_seconds:-1"), linkDownTolerance: time.Minute, want: false},
		{name: "link down without duration", info: info("master_link_status:down"), linkDownTolerance: time.Minute, want: false},
		{name: "negative tolerance", info: info("master_link_status:down", "master_link_down_since_seconds:0"), linkDownTolerance: -1, want: false},
		{name: "loading", info: []byte("# Replication\r\nmaster_link_status:up\r\n# Persistence\r\nloading:1\r\n"), linkDownTolerance: time.Minute, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := replicaHealthy(tc.info, tc.linkDownTolerance); got != tc.want {
				t.Errorf("replicaHealthy(%q, %v) = %v, want %v", tc.info, tc.linkDownTolerance, got, tc.want)
			}
		})
	}
}
