// Package testbed is a tool for running redis-server for tests.
package testbed

import (
	"io/ioutil"
	"os"
	"os/exec"
)

// Binary is a path to redis-server
var Binary = func() string { p, _ := exec.LookPath("redis-server"); return p }()

// Dir is temporary directory where redis will run.
var Dir = ""
var tlsCluster = os.Getenv("TLS_ENABLED") == "ENABLED"

// InitDir initiates Dir with temporary directory in base.
func InitDir(base string) {
	if Dir == "" {
		var err error
		Dir, err = ioutil.TempDir(base, "redis_test_")
		if err != nil {
			panic(err)
		}
	}
}

// RmDir removes temporary directory.
func RmDir() {
	if Dir == "" {
		return
	}
	if err := os.RemoveAll(Dir); err != nil {
		panic(err)
	}
	Dir = ""
}
