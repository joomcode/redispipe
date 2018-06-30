package redisdumb

import (
	"bufio"
	"net"
	"time"

	. "github.com/joomcode/redispipe/redis"
)

type ConnType int

const (
	TypeSimple  ConnType = 0
	TypeCluster ConnType = 1
)

var DefaultTimeout time.Duration = 5 * time.Second

type Conn struct {
	Addr    string
	C       net.Conn
	R       *bufio.Reader
	Timeout time.Duration
	Type    ConnType
}

func (c *Conn) Do(cmd string, args ...interface{}) interface{} {
	timeout := c.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	try := 1
	if c.C != nil {
		try = 2
	}
	var rerr *Error
	var req []byte
	var err error
	var asking bool
	for i := 0; i < try; i++ {
		if c.C == nil {
			c.C, err = net.DialTimeout("tcp", c.Addr, timeout)
			if err != nil {
				return NewErr(ErrKindIO, ErrDial).Wrap(err)
			}
			c.R = bufio.NewReader(c.C)
		}
		if asking {
			c.Do("ASKING")
		}
		c.C.SetDeadline(time.Now().Add(timeout))
		req, rerr = AppendRequest(nil, Request{cmd, args})
		if rerr == nil {
			if _, err = c.C.Write(req); err == nil {
				res := ReadResponse(c.R)
				if rerr = AsRedisError(res); rerr == nil {
					return res
				} else if c.Type == TypeCluster && (rerr.Code == ErrAsk || rerr.Code == ErrMoved) {
					asking = rerr.Code == ErrAsk
					c.Addr = rerr.Get("movedto").(string)
					if try < 5 {
						try++
					}
				}
			} else {
				rerr = NewErr(ErrKindIO, ErrIO).Wrap(err)
			}
		}
		c.C.Close()
		c.C = nil
	}
	return rerr
}

func (c *Conn) Close() {
	if c.C != nil {
		c.C.Close()
		c.C = nil
	}
}

func Do(addr string, cmd string, args ...interface{}) interface{} {
	conn, err := net.DialTimeout("tcp", addr, DefaultTimeout)
	if err != nil {
		return NewErr(ErrKindIO, ErrDial).Wrap(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(DefaultTimeout))
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
