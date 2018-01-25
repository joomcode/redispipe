package testbed

import (
	"bufio"
	"net"
	"time"

	. "github.com/joomcode/redispipe/redis"
)

type Conn struct {
	Addr string
	C    net.Conn
	R    *bufio.Reader
}

func (c *Conn) Do(cmd string, args ...interface{}) interface{} {
	try := 1
	if c.C != nil {
		try = 2
	}
	var rerr *Error
	var req []byte
	var err error
	for i := 0; i < try; i++ {
		if c.C == nil {
			c.C, err = net.DialTimeout("tcp", c.Addr, 100*time.Millisecond)
			if err != nil {
				return NewErr(ErrKindIO, ErrDial).Wrap(err)
			}
			c.R = bufio.NewReader(c.C)
		}
		c.C.SetDeadline(time.Now().Add(1 * time.Second))
		req, rerr = AppendRequest(nil, Request{cmd, args})
		if rerr == nil {
			if _, err = c.C.Write(req); err == nil {
				res := ReadResponse(c.R)
				if rerr = AsRedisError(res); rerr == nil {
					return res
				}
			} else {
				rerr = NewErr(ErrKindIO, ErrIO).Wrap(err)
			}
		}
		c.C = nil
	}
	return rerr
}

func Do(addr string, cmd string, args ...interface{}) interface{} {
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		return NewErr(ErrKindIO, ErrDial).Wrap(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(1 * time.Second))
	req, rerr := AppendRequest(nil, Request{cmd, args})
	if rerr != nil {
		return rerr
	}
	if _, err = conn.Write(req); err != nil {
		return NewErr(ErrKindIO, ErrIO).Wrap(err)
	}
	res := ReadResponse(bufio.NewReader(conn))
	return res
}
