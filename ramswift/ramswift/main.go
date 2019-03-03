package main

import (
	"log"
	"os"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/ramswift"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("no .conf file specified")
	}

	doneChan := make(chan bool, 1) // Must be buffered to avoid race

	// indicate that ramswift is running in a separate process
	ramswift.StandAloneMode = true

	go ramswift.Daemon(os.Args[1], os.Args[2:], nil, doneChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	_ = <-doneChan
}
