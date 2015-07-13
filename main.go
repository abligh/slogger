package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"time"
)

/*
 * TODO:
 *
 * + Sharding and shard index
 * + SSL and client certificate handling
 * + Merkle thread
 */

func killPrevious() {
	// A hack to kill previous instances
	pid := os.Getpid()
	exec.Command("sh", "-c", "pgrep slogger | fgrep -v '^"+fmt.Sprintf("%d", pid)+"$' | xargs kill").Run()
	time.Sleep(500 * time.Millisecond)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	killPrevious()
	buildJsonMap()
	initFieldProperties()
	db := newDatabase()
	go syslogServerStart(db)
	httpServerStart(db)
}
