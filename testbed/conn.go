package testbed

import (
	"bufio"
	"net"
	"time"

	. "github.com/joomcode/redispipe/redis"
)

func Do(addr string, cmd string, args ...interface{}) (interface{}, error) {
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		return nil, NewErr(ErrKindIO, ErrDial).Wrap(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(1 * time.Second))
	req, rerr := AppendRequest(nil, Request{cmd, args})
	if rerr != nil {
		return nil, rerr
	}
	if _, err = conn.Write(req); err != nil {
		return nil, NewErr(ErrKindIO, ErrDial).Wrap(err)
	}
	res := ReadResponse(bufio.NewReader(conn))
	return res, AsError(res)
}
