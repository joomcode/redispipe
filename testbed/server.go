package testbed

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

var Binary = func() string { p, _ := exec.LookPath("redis-server"); return p }()
var Dir = ""

func InitDir(base string) {
	if Dir == "" {
		var err error
		Dir, err = ioutil.TempDir(base, "redis_test_")
		if err != nil {
			panic(err)
		}
	}
}

func RmDir() {
	if err := os.RemoveAll(Dir); err != nil {
		panic(err)
	}
	Dir = ""
}

type Server struct {
	Port   uint16
	Args   []string
	Cmd    *exec.Cmd
	Paused bool
}

func (s *Server) PortStr() string {
	return strconv.Itoa(int(s.Port))
}

func (s *Server) Addr() string {
	return "127.0.0.1:" + s.PortStr()
}

func (s *Server) Start() {
	if s.Cmd != nil {
		return
	}
	s.Paused = false
	port := s.PortStr()
	args := append([]string{
		"--bind", "127.0.0.1",
		"--port", port,
		//"--logfile", port + ".log",
	}, s.Args...)
	var err error
	s.Cmd = exec.Command(Binary, args...)
	s.Cmd.Dir = Dir

	_stdout, _ := s.Cmd.StdoutPipe()
	stdout := bufio.NewReader(_stdout)

	err = s.Cmd.Start()
	if err != nil {
		s.Cmd = nil
		panic(err)
	}
	for {
		l, isPrefix, err := stdout.ReadLine()
		if err != nil {
			panic(err)
		}
		if isPrefix {
			panic("logline too long")
		}
		if bytes.Contains(l, []byte("Ready to accept connections")) {
			break
		}
	}
	go func() {
		for {
			_, _, err := stdout.ReadLine()
			if err != nil {
				break
			}
		}
	}()
}

func (s *Server) Pause() {
	if s.Paused {
		return
	}
	if err := s.Cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		panic(err)
	}
	s.Paused = true
}

func (s *Server) Resume() {
	if !s.Paused {
		return
	}
	if err := s.Cmd.Process.Signal(syscall.SIGCONT); err != nil {
		panic(err)
	}
	s.Paused = false
}

func (s *Server) Stop() {
	if s.Paused {
		s.Resume()
	}
	if s.Cmd == nil {
		return
	}
	p := s.Cmd
	s.Cmd = nil
	if err := p.Process.Kill(); err != nil {
		panic(err)
	}
	p.Wait()
}

func (s *Server) Do(cmd string, args ...interface{}) (interface{}, error) {
	return Do(s.Addr(), cmd, args...)
}
