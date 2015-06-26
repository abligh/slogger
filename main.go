package main

import (
	"fmt"
	"os"
	"os/exec"
	"time"
)

func killPrevious() {
	// A hack to kill previous instances
	pid := os.Getpid()
	exec.Command("sh", "-c", "pgrep slogger | fgrep -v '^"+fmt.Sprintf("%d", pid)+"$' | xargs kill").Run()
	time.Sleep(500 * time.Millisecond)
}

func main() {
	killPrevious()
	db := newDatabase()
	go syslogServerStart(db)
	httpServerStart(db)
}
