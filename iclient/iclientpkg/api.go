// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

// Start is called to start serving.
//
func Start(confMap conf.ConfMap) (err error) {
	err = nil // TODO: err = start(confMap)
	return
}

// Stop is called to stop serving.
//
func Stop() (err error) {
	err = nil // TODO: err = stop()
	return
}

// Signal is called to interrupt the server for performing operations such as log rotation.
//
func Signal() (err error) {
	err = nil // TODO: err = signal()
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
