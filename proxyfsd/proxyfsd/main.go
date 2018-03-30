// The proxyfsd program is the main ProxyFS dæmon and is named accordingly.
package main

import (
	"fmt"
	"log"
	"log/syslog"
	"os"
	"sync"

	"github.com/swiftstack/ProxyFS/proxyfsd"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("no .conf file specified")
	}

	errChan := make(chan error, 1) // Must be buffered to avoid race
	var wg sync.WaitGroup

	syslogger, err := syslog.Dial("", "", syslog.LOG_DAEMON, "proxyfsd")
	if err != nil {
		fmt.Fprintf(os.Stderr, "proxyfsd/Daemon(): syslog.Dial() failed: %v\n", err)
		syslogger = nil
	} else {
		syslogger.Info(fmt.Sprintf("starting up: calling Daemon()"))
	}

	// empty signal list (final argument) means "catch all signals" its possible to catch
	go proxyfsd.Daemon(os.Args[1], os.Args[2:], nil, errChan, &wg, os.Args)

	err = <-errChan

	if syslogger != nil {
		if err == nil {
			syslogger.Info(fmt.Sprintf("shutting down: Daemon() finished"))
		} else {
			syslogger.Err(fmt.Sprintf("shutting down: Daemon() returned error: %v", err))
		}
	}

	wg.Wait() // wait for services to go Down()

	if nil != err {
		fmt.Fprintf(os.Stderr, "proxyfsd: Daemon(): returned error: %v\n", err) // Can't use logger.*() as it's not currently "up"
		os.Exit(1)                                                              // Exit with non-success status that can be checked from scripts
	}
}
