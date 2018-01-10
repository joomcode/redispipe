package redis_conn

import (
	"io"
	"net"
	"time"
)

type deadlineIO struct {
	to time.Duration
	c  net.Conn
}

func newDeadlineIO(c net.Conn, to time.Duration) io.ReadWriter {
	if to > 0 {
		return &deadlineIO{c: c, to: to}
	}
	return c
}

func (d *deadlineIO) Write(b []byte) (int, error) {
	d.c.SetWriteDeadline(time.Now().Add(d.to))
	return d.c.Write(b)
}

func (d *deadlineIO) Read(b []byte) (int, error) {
	d.c.SetReadDeadline(time.Now().Add(d.to))
	return d.c.Read(b)
}
