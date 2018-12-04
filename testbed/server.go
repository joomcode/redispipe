package testbed

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/joomcode/redispipe/redisdumb"
)

// Server is a handle for running redis-server.
type Server struct {
	Port   uint16
	Args   []string
	Cmd    *exec.Cmd
	Paused bool
	Conn   redisdumb.Conn
}

// PortStr returns server's port as a string
func (s *Server) PortStr() string {
	return strconv.Itoa(int(s.Port))
}

// Addr - address + port
func (s *Server) Addr() string {
	return "127.0.0.1:" + s.PortStr()
}

// Start starts redis and waits for its initialization.
func (s *Server) Start() {
	if s.Cmd != nil {
		return
	}
	s.Paused = false
	port := s.PortStr()
	args := append([]string{
		"--bind", "127.0.0.1",
		"--port", port,
		"--dbfilename", "dump-" + port + ".rdb",
	}, s.Args...)
	var err error
	s.Cmd = exec.Command(Binary, args...)
	s.Cmd.Dir = Dir

	_stdout, _ := s.Cmd.StdoutPipe()
	logfile, err := os.Create(filepath.Join(s.Cmd.Dir, "log-"+port+".log"))
	if err != nil {
		panic(err)
	}
	_tee := io.TeeReader(_stdout, logfile)
	stdout := bufio.NewReader(_tee)

	err = s.Cmd.Start()
	if err != nil {
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
		if bytes.Contains(l, []byte("eady to accept connections")) {
			break
		}
	}
	go func() {
		defer logfile.Close()
		for {
			_, _, err := stdout.ReadLine()
			if err != nil {
				break
			}
		}
	}()
	s.Conn.Addr = s.Addr()
}

// Running returns true if server should be running at the moment.
func (s *Server) Running() bool {
	return s.Cmd != nil
}

// RunningNow returns true if server should be running and it is not paused (with SIGSTOP).
func (s *Server) RunningNow() bool {
	return s.Cmd != nil && !s.Paused
}

// Pause pauses server with SIGSTOP.
func (s *Server) Pause() {
	if s.Paused {
		return
	}
	if err := s.Cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		panic(err)
	}
	s.Paused = true
}

// Resume resumes server with SIGCONT.
func (s *Server) Resume() {
	if !s.Paused {
		return
	}
	if err := s.Cmd.Process.Signal(syscall.SIGCONT); err != nil {
		panic(err)
	}
	s.Paused = false
}

// Stop kills server.
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

// Do executes command on server.
func (s *Server) Do(cmd string, args ...interface{}) interface{} {
	return s.Conn.Do(cmd, args...)
}

// DoSure executes command and panics if it returns error.
func (s *Server) DoSure(cmd string, args ...interface{}) interface{} {
	r := s.Do(cmd, args...)
	if err, ok := r.(error); ok {
		panic(err)
	}
	return r
}
