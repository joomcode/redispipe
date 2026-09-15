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
		name         string
		info         []byte
		tolerance    time.Duration
		masterFailed bool
		want         bool
	}{
		{name: "link up", info: info("master_link_status:up"), tolerance: time.Minute, want: true},
		{name: "link down for a while", info: info("master_link_status:down", "master_link_down_since_seconds:3"), tolerance: time.Minute, want: true},
		{name: "link down for the whole tolerance", info: info("master_link_status:down", "master_link_down_since_seconds:60"), tolerance: time.Minute, want: false},
		{name: "never synced since start", info: info("master_link_status:down", "master_link_down_since_seconds:-1"), tolerance: time.Minute, want: false},
		{name: "link down without duration", info: info("master_link_status:down"), tolerance: time.Minute, want: false},
		{name: "negative tolerance", info: info("master_link_status:down", "master_link_down_since_seconds:0"), tolerance: -1, want: false},
		{name: "loading", info: []byte("# Replication\r\nmaster_link_status:up\r\n# Persistence\r\nloading:1\r\n"), tolerance: time.Minute, want: false},
		{name: "master failed, link down beyond tolerance", info: info("master_link_status:down", "master_link_down_since_seconds:3600"), tolerance: time.Minute, masterFailed: true, want: true},
		{name: "master failed, negative tolerance", info: info("master_link_status:down", "master_link_down_since_seconds:5"), tolerance: -1, masterFailed: true, want: true},
		{name: "master failed, never synced since start", info: info("master_link_status:down", "master_link_down_since_seconds:-1"), tolerance: time.Minute, masterFailed: true, want: false},
		{name: "master failed, loading", info: []byte("# Replication\r\nmaster_link_status:down\r\nmaster_link_down_since_seconds:5\r\n# Persistence\r\nloading:1\r\n"), tolerance: time.Minute, masterFailed: true, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := replicaHealthy(tc.info, tc.tolerance, tc.masterFailed); got != tc.want {
				t.Errorf("replicaHealthy(%q, %v, %v) = %v, want %v", tc.info, tc.tolerance, tc.masterFailed, got, tc.want)
			}
		})
	}
}
