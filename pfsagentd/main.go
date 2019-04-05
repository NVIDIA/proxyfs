package main

import (
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
)

func main() {
	var (
		args       []string
		confMap    conf.ConfMap
		err        error
		signalChan chan os.Signal
	)

	// Setup signal catcher for clean shutdown

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, unix.SIGHUP, unix.SIGINT, unix.SIGTERM)

	// Parse arguments (at this point, logging goes only to the console)

	globals.logFile = nil
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true

	args = os.Args[1:]

	if 0 == len(args) {
		logFatalf("no .conf file specified")
	}

	confMap, err = conf.MakeConfMapFromFile(args[0])
	if nil != err {
		logFatalf("failed to load config: %v", err)
	}

	err = confMap.UpdateFromStrings(args[1:])
	if nil != err {
		logFatalf("failed to load config overrides: %v", err)
	}

	// Initialize globals

	initializeGlobals(confMap)

	// Perform mount via ProxyFS

	doMountProxyFS()

	// Start serving FUSE mount point

	performMountFUSE()

	// Await SIGHUP, SIGINT, or SIGTERM

	_ = <-signalChan

	// Perform clean shutdown

	performUnmountFUSE()

	// Uninitialize globals

	uninitializeGlobals()
}
