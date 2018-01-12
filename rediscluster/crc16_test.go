package rediscluster_test

// copied from github.com/mediocregopher/radix.v2/cluster/crc16.go

import (
	"testing"

	. "github.com/joomcode/redispipe/rediscluster"
)

func TestCRC16(t *testing.T) {
	if c := CRC16([]byte("123456789")); c != 0x31c3 {
		t.Fatalf("checksum came out to %x not %x", c, 0x31c3)
	}
}
