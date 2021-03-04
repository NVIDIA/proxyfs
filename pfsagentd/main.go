// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/NVIDIA/proxyfs/conf"
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

	// Parse arguments (at this point, logging goes only to the console)

	globals.logFile = nil
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true

	// if this process is run setuid then ascend to full daemon
	if syscall.Getgid() != syscall.Getegid() {
		egid := syscall.Getegid()
		err = syscall.Setresgid(egid, egid, egid)
		if nil != err {
			log.Fatalf("gid %d  egid %d  syscall.Setresgid(egid, egid, egid)) failed: %v",
				syscall.Getegid(), egid, err)
		}
	}
	if syscall.Getuid() != syscall.Geteuid() {
		euid := syscall.Geteuid()
		err = syscall.Setresuid(euid, euid, euid)
		if nil != err {
			log.Fatalf("uid %d  euid %d  syscall.Setresuid(euid, euid, euid)) failed: %v",
				syscall.Geteuid(), euid, err)
		}
	}

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

	// Trigger initial auth plug-in invocation

	updateAuthTokenAndStorageURL()

	// Perform mount via ProxyFS

	doMountProxyFS()

	// Start serving FUSE mount point

	performMountFUSE()

	// Start serving HTTP

	serveHTTP()

	// Await any of specified signals or fission exit

	select {
	case _ = <-signalChan:
		// Normal termination due to one of the above registered signals
	case err = <-globals.fissionErrChan:
		// Unexpected exit of fission.Volume
		logFatalf("unexpected error from package fission: %v", err)
	}

	// Stop serving HTTP

	unserveHTTP()

	// Stop serving FUSE mount point

	performUnmountFUSE()

	// Flush all dirty fileInode's

	emptyFileInodeDirtyListAndLogSegmentChan()

	// Perform unmount via ProxyFS

	doUnmountProxyFS()

	// Terminate authPlugIn

	stopAuthPlugIn()

	// Uninitialize globals

	uninitializeGlobals()
}
