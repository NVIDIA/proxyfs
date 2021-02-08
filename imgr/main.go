// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/NVIDIA/proxyfs/conf"
)

func main() {
	var (
		confMap        conf.ConfMap
		err            error
		signalChan     chan os.Signal
		signalReceived os.Signal
	)

	if len(os.Args) < 2 {
		log.Fatalf("no .conf file specified")
	}

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

	// Arm signal handler used to indicate termination and wait on it
	//
	// Note: signalled chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	// Initialize globals

	initializeGlobals(confMap)

	// Indicate we are UP

	logInfof("UP")

	// Await any of specified signals or fission exit

	for {
		signalReceived = <-signalChan
		logInfof("Received signal: %v", signalReceived)
		if unix.SIGHUP == signalReceived {
			logSIGHUP()
		} else {
			break
		}
	}

	// Indicate we are DOWN

	logInfof("DOWN")

	// Uninitialize globals

	uninitializeGlobals()
}
