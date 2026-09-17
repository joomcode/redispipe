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
		{name: "async loading is not loading", info: []byte("# Replication\r\nmaster_link_status:up\r\n# Persistence\r\nloading:0\r\nasync_loading:1\r\n"), linkDownTolerance: time.Minute, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := replicaHealthy(tc.info, tc.linkDownTolerance); got != tc.want {
				t.Errorf("replicaHealthy(%q, %v) = %v, want %v", tc.info, tc.linkDownTolerance, got, tc.want)
			}
		})
	}
}

func TestInfoField(t *testing.T) {
	info := []byte("loading:0\r\nasync_loading:1\r\nmaster_link_status:down\r\nmaster_link_down_since_seconds:-1\r\n")
	for _, tc := range []struct {
		field string
		want  string
		ok    bool
	}{
		{field: "loading", want: "0", ok: true},
		{field: "async_loading", want: "1", ok: true},
		{field: "master_link_status", want: "down", ok: true},
		{field: "master_link_down_since_seconds", want: "-1", ok: true},
		{field: "link_status", ok: false},
		{field: "role", ok: false},
	} {
		got, ok := infoField(info, tc.field)
		if ok != tc.ok || string(got) != tc.want {
			t.Errorf("infoField(%q) = %q, %v; want %q, %v", tc.field, got, ok, tc.want, tc.ok)
		}
	}
}
