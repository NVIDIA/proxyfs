// Copyright (c) 2015-2022, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Program ickpt provides a command-line wrapper around package ickptpkg APIs.
//
// The program requires a single argument that is a path to a package config
// formatted configuration to load. Optionally, overrides the the config may
// be passed as additional arguments in the form <section_name>.<option_name>=<value>.
//
package main

import (
	"fmt"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/ickpt/ickptpkg"
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

	// Start

	err = ickptpkg.Start(confMap)
	if nil != err {
		fmt.Fprintf(os.Stderr, "ickptpkg.Start(confMap) failed: %v\n", err)
		os.Exit(1)
	}

	// Arm signal handler used to indicate termination & wait on it
	//
	// Note: signal'd chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	<-signalChan

	// Stop

	err = ickptpkg.Stop()
	if nil != err {
		fmt.Fprintf(os.Stderr, "ickptpkg.Stop() failed: %v\n", err)
		os.Exit(1)
	}
}
