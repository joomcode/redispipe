package redisconn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDialTimeoutAboveIOTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := Connect(ctx, "127.0.0.1:1", Opts{
		IOTimeout:   200 * time.Millisecond,
		DialTimeout: 3 * time.Second,
		AsyncDial:   true,
	})
	require.NoError(t, err)
	defer conn.Close()

	require.Equal(t, 3*time.Second, conn.opts.DialTimeout)
}

func TestDialTimeoutDefaultsToIOTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := Connect(ctx, "127.0.0.1:1", Opts{
		IOTimeout: 200 * time.Millisecond,
		AsyncDial: true,
	})
	require.NoError(t, err)
	defer conn.Close()

	require.Equal(t, 200*time.Millisecond, conn.opts.DialTimeout)
}
