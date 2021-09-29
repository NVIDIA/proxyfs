// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package icllientpkg implements a client to package imgrpkg for the purpose
// of presenting a single ProxyFS volume via FUSE.
//
// To configure an iclientpkg instance, Start() is called passing, as the sole
// argument, a package conf ConfMap. Here is a sample .conf file:
//
//  [ICLIENT]
//  VolumeName:               testvol
//  MountPointDirPath:        /mnt
//  FUSEAllowOther:           true
//  FUSEMaxBackground:        1000
//  FUSECongestionThreshhold: 0
//  FUSEMaxWrite:             131076
//  AuthPlugInPath:           iauth-swift.so
//  AuthPlugInEnvName:        # Only used if not defining AuthPlugInEnvValue here
//  AuthPlugInEnvValue:       {"AuthURL":"http://swift:8080/auth/v1.0"\u002C"AuthUser":"test:tester"\u002C"AuthKey":"testing"\u002C"Account":"AUTH_test"\u002C"Container":"con"}
//  SwiftRetryDelay:          100ms
//  SwiftRetryExpBackoff:     2
//  SwiftRetryLimit:          4
//  SwiftTimeout:             10m
//  SwiftConnectionPoolSize:  128
//  RetryRPCPublicIPAddr:     imgr
//  RetryRPCPort:             32356
//  RetryRPCDeadlineIO:       60s
//  RetryRPCKeepAlivePeriod:  60s
//  RetryRPCCACertFilePath:   # Defaults to /dev/null
//  LogFilePath:              iclient.log
//  LogToConsole:             true
//  TraceEnabled:             false
//  FUSELogEnabled:           false
//  HTTPServerIPAddr:         # Defaults to 0.0.0.0 (i.e. all interfaces)
//  HTTPServerPort:           # Defaults to disabling the embedded HTTP Server
//
// Most of the config keys are required and must have values. One set of exceptions
// are the HTTPServer{IPAddr|Port} keys that, if not present (or HTTPServerPort is
// zero) will disable the embedded HTTP Server. Another set of exceptions are the
// AuthPlungInEnv{Name|Value} keys of which exactly one must be present.
//
// The embedded HTTP Server (at URL http://<HTTPServerIPAddr>:<HTTPServerPort>)
// responds to the following:
//
//  GET /config
//
// This will return a JSON document that matches the conf.ConfMap used to
// launch this package.
//
//  GET /leases
//
// This will display the state of every lease.
//
//  POST /leases/demote
//
// This will trigger the demotion of any Exclusive Leases held.
//
//  POST /leases/release
//
// This will trigger the release of any {Exclusive|Shared} Leases held.
//
//  GET /stats
//
// This will return a raw bucketstats dump.
//
//  GET /version
//
package iclientpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

// Start is called to start serving.
//
func Start(confMap conf.ConfMap, fissionErrChan chan error) (err error) {
	err = start(confMap, fissionErrChan)
	return
}

// Stop is called to stop serving.
//
func Stop() (err error) {
	err = stop()
	return
}

// Signal is called to interrupt the server for performing operations such as log rotation.
//
func Signal() (err error) {
	err = signal()
	return
}

// LogFatalf is a wrapper around the internal logFatalf() func called by iclient/main.go::main().
//
func LogFatalf(format string, args ...interface{}) {
	logFatalf(format, args...)
}

// LogWarnf is a wrapper around the internal logWarnf() func called by iclient/main.go::main().
//
func LogWarnf(format string, args ...interface{}) {
	logWarnf(format, args...)
}

// LogInfof is a wrapper around the internal logInfof() func called by iclient/main.go::main().
//
func LogInfof(format string, args ...interface{}) {
	logInfof(format, args...)
}
