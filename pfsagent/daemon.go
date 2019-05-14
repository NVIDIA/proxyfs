package pfsagent

import (
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/swiftstack/ProxyFS/conf"
)

// Daemon is used to launch a daemonized instance of package pfsagent.
//
func Daemon(confFile string, confStrings []string, signalHandlerIsArmedWG *sync.WaitGroup, doneChan chan bool, signals ...os.Signal) {
	var (
		confMap    conf.ConfMap
		err        error
		signalChan chan os.Signal
	)

	// Arm signal handler used to indicate termination and wait on it
	//
	// Note: signalled chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, signals...)

	if nil != signalHandlerIsArmedWG {
		signalHandlerIsArmedWG.Done()
	}

	// Parse arguments (at this point, logging goes only to the console)

	globals.logFile = nil
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true

	// Compute confMap

	confMap, err = conf.MakeConfMapFromFile(confFile)
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	err = confMap.UpdateFromStrings(confStrings)
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

	// Perform clean shutdown

	performUnmountFUSE()

	// Uninitialize globals

	uninitializeGlobals()

	// Indicate we are done before exiting

	doneChan <- true
}
