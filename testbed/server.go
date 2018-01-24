package testbed

import (
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
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
	os.RemoveAll(Dir)
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

func (s *Server) Start() error {
	if s.Cmd != nil {
		return nil
	}
	s.Paused = false
	port := s.PortStr()
	args := append([]string{
		"--bind", "127.0.0.1",
		"--port", port,
		"--logfile", port + ".log",
	}, s.Args...)
	var err error
	s.Cmd = exec.Command(Binary, args...)
	s.Cmd.Dir = Dir
	/*
		stdout, _ := s.Cmd.StdoutPipe()
		stderr, _ := s.Cmd.StderrPipe()
		go func() {
			buf, _ := ioutil.ReadAll(stdout)
			fmt.Printf("stdout: %q\n", buf)
		}()
		go func() {
			buf, _ := ioutil.ReadAll(stderr)
			fmt.Printf("stderr: %q\n", buf)
		}()
	*/
	err = s.Cmd.Start()
	if err != nil {
		s.Cmd = nil
		return err
	}
	time.Sleep(10 * time.Millisecond)
	return nil
}

func (s *Server) Pause() error {
	if s.Paused {
		return nil
	}
	if err := s.Cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		return err
	}
	s.Paused = true
	return nil
}

func (s *Server) Resume() error {
	if !s.Paused {
		return nil
	}
	if err := s.Cmd.Process.Signal(syscall.SIGCONT); err != nil {
		return err
	}
	s.Paused = false
	return nil
}

func (s *Server) Stop() error {
	if s.Paused {
		s.Resume()
	}
	if s.Cmd == nil {
		return nil
	}
	defer time.Sleep(10 * time.Millisecond)
	p := s.Cmd
	s.Cmd = nil
	defer p.Wait()
	return p.Process.Kill()
}

func (s *Server) Do(cmd string, args ...interface{}) (interface{}, error) {
	return Do(s.Addr(), cmd, args...)
}
