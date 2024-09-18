package redisclusterutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSlotsInfo(t *testing.T) {
	clusterSlotsResponse := []interface{}{
		[]interface{}{
			int64(0),
			int64(5460),
			[]interface{}{
				[]byte("127.0.0.1"),
				int64(30001),
				[]byte("09dbe9720cda62f7865eabc5fd8857c5d2678366"),
				[]interface{}{
					"hostname",
					"host-1.redis.example.com",
				},
			},
			[]interface{}{
				[]byte("127.0.0.1"),
				int64(30004),
				[]byte("821d8ca00d7ccf931ed3ffc7e3db0599d2271abf"),
				[]interface{}{
					"hostname",
					"host-2.redis.example.com",
				},
			},
		},
		[]interface{}{
			int64(5461),
			int64(10922),
			[]interface{}{
				[]byte("127.0.0.1"),
				int64(30002),
				[]byte("c9d93d9f2c0c524ff34cc11838c2003d8c29e013"),
				[]interface{}{
					"hostname",
					"host-3.redis.example.com",
				},
			},
			[]interface{}{
				[]byte("127.0.0.1"),
				int64(30005),
				[]byte("faadb3eb99009de4ab72ad6b6ed87634c7ee410f"),
				[]interface{}{
					"hostname",
					"host-4.redis.example.com",
				},
			},
		},
		[]interface{}{
			int64(10923),
			int64(16383),
			[]interface{}{
				[]byte("192.168.11.131"),
				int64(30003),
				[]byte("044ec91f325b7595e76dbcb18cc688b6a5b434a1"),
				[]interface{}{
					"hostname",
					"host-5.redis.example.com",
				},
			},
			[]interface{}{
				[]byte("127.0.0.1"),
				int64(30006),
				[]byte("58e6e48d41228013e5d9c1c37c5060693925e97e"),
				[]interface{}{
					"hostname",
					"host-6.redis.example.com",
				},
			},
		},
	}

	expectedSlots := []SlotsRange{
		{
			From: 0,
			To:   5460,
			Addrs: []string{
				"127.0.0.1:30001",
				"127.0.0.1:30004",
			},
		},
		{
			From: 5461,
			To:   10922,
			Addrs: []string{
				"127.0.0.1:30002",
				"127.0.0.1:30005",
			},
		},
		{
			From: 10923,
			To:   16383,
			Addrs: []string{
				"192.168.11.131:30003",
				"127.0.0.1:30006",
			},
		},
	}

	slots, err := ParseSlotsInfo(clusterSlotsResponse)
	require.NoError(t, err)

	assert.Equal(t, expectedSlots, slots)
}

func TestParseSlotsInfo_EmptyAddress(t *testing.T) {
	clusterSlotsResponse := []interface{}{
		[]interface{}{
			int64(0),
			int64(5460),
			[]interface{}{
				[]byte("127.0.0.1"),
				int64(30001),
				[]byte("09dbe9720cda62f7865eabc5fd8857c5d2678366"),
				[]interface{}{
					"hostname",
					"host-1.redis.example.com",
				},
			},
			[]interface{}{
				[]interface{}{},
				int64(0),
				[]byte("821d8ca00d7ccf931ed3ffc7e3db0599d2271abf"),
				[]interface{}{
					"hostname",
					"host-2.redis.example.com",
				},
			},
		},
		[]interface{}{
			int64(5461),
			int64(10922),
			[]interface{}{
				[]interface{}{},
				int64(0),
				[]byte("c9d93d9f2c0c524ff34cc11838c2003d8c29e013"),
				[]interface{}{
					"hostname",
					"host-3.redis.example.com",
				},
			},
			[]interface{}{
				[]byte("127.0.0.1"),
				int64(30005),
				[]byte("faadb3eb99009de4ab72ad6b6ed87634c7ee410f"),
				[]interface{}{
					"hostname",
					"host-4.redis.example.com",
				},
			},
		},
	}

	expectedSlots := []SlotsRange{
		{
			From: 0,
			To:   5460,
			Addrs: []string{
				"127.0.0.1:30001",
			},
		},
		{
			From: 5461,
			To:   10922,
			Addrs: []string{
				"127.0.0.1:30005",
			},
		},
	}

	slots, err := ParseSlotsInfo(clusterSlotsResponse)
	require.NoError(t, err)

	assert.Equal(t, expectedSlots, slots)
}
