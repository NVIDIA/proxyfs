// The proxyfsd program is the main ProxyFS dæmon and is named accordingly.
package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/proxyfsd"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("no .conf file specified")
	}

	errChan := make(chan error, 1) // Must be buffered to avoid race
	var wg sync.WaitGroup

	go proxyfsd.Daemon(os.Args[1], os.Args[2:], nil, errChan, &wg, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	err := <-errChan

	wg.Wait() // wait for services to go Down()

	if nil != err {
		fmt.Fprintf(os.Stderr, "proxyfsd/Daemon(): exited with error: %v\n", err) // Can't use logger.*() as it's not currently "up"
		os.Exit(1)                                                                // Exit with non-success status that can be checked from scripts
	}
}
