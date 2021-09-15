// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package icllientpkg implements a client to package imgrpkg for the purpose
// of presenting a single ProxyFS volume via FUSE.
//
// To configure an iclientpkg instance, Start() is called passing, as the sole
// argument, a package conf ConfMap. Here is a sample .conf file:
//
//  [ICLIENT]
//  VolumeName:              testvol
//  MountPointDirPath:       /mnt
//  AllowOther:              true
//  PlugInPath:              iauth-swift.so
//  PlugInEnvName:           SwiftAuthBlob
//  PlugInEnvValue:          {"AuthURL":"http://swift:8080/auth/v1.0"\u002C"AuthUser":"test:tester"\u002C"AuthKey":"testing"\u002C"Account":"AUTH_test"\u002C"Container":"con"}
//  RetryRPCPublicIPAddr:    imgr
//  RetryRPCPort:            32356
//  RetryRPCDeadlineIO:      60s
//  RetryRPCKeepAlivePeriod: 60s
//  RetryRPCCACertFilePath:  # Defaults to /dev/null
//  LogFilePath:             iclient.log
//  LogToConsole:            true
//  TraceEnabled:            false
//
package iclientpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

// Start is called to start serving.
//
func Start(confMap conf.ConfMap) (err error) {
	err = start(confMap)
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

// LogWarnf is a wrapper around the internal logWarnf() func called by imgr/main.go::main().
//
func LogWarnf(format string, args ...interface{}) {
	logWarnf(format, args...)
}

// LogInfof is a wrapper around the internal logInfof() func called by imgr/main.go::main().
//
func LogInfof(format string, args ...interface{}) {
	logInfof(format, args...)
}
