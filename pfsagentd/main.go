package main

import (
	"log"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
)

func main() {
	var (
		confMap    conf.ConfMap
		err        error
		signalChan chan os.Signal
	)

	if len(os.Args) < 2 {
		log.Fatalf("no .conf file specified")
	}

	// Arm signal handler used to indicate termination and wait on it
	//
	// Note: signalled chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	// Parse arguments (at this point, logging goes only to the console)

	globals.logFile = nil
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true

	confMap, err = conf.MakeConfMapFromFile(os.Args[1])
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	err = confMap.UpdateFromStrings(os.Args[2:])
	if nil != err {
		log.Fatalf("failed to apply config overrides: %v", err)
	}

	// Initialize globals

	initializeGlobals(confMap)

	// Perform mount via ProxyFS

	doMountProxyFS()

	// Start serving FUSE mount point

	performMountFUSE()

	// Await any of specified signals

	_ = <-signalChan

	// Stop serving FUSE mount point

	performUnmountFUSE()

	// Flush all dirty fileInode's

	emptyFileInodeDirtyList()

	// Uninitialize globals

	uninitializeGlobals()
}
