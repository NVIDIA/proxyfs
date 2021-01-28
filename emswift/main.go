// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/emswift/emswiftpkg"
)

func main() {
	var (
		confMap    conf.ConfMap
		err        error
		signalChan chan os.Signal
	)

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "no .conf file specified\n")
		os.Exit(1)
	}

	confMap, err = conf.MakeConfMapFromFile(os.Args[1])
	if nil != err {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	err = confMap.UpdateFromStrings(os.Args[2:])
	if nil != err {
		fmt.Fprintf(os.Stderr, "failed to apply config overrides: %v\n", err)
		os.Exit(1)
	}

	// Start Swift Emulation

	err = emswiftpkg.Start(confMap)
	if nil != err {
		fmt.Fprintf(os.Stderr, "emswiftpkg.Start(confMap) failed: %v\n", err)
		os.Exit(1)
	}

	// Arm signal handler used to indicate termination & wait on it
	//
	// Note: signal'd chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	_ = <-signalChan

	// Stop Swift Emulation

	err = emswiftpkg.Stop()
	if nil != err {
		fmt.Fprintf(os.Stderr, "emswiftpkg.Stop() failed: %v\n", err)
		os.Exit(1)
	}
}
