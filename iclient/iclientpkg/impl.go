// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

func start(confMap conf.ConfMap, fissionErrChan chan error) (err error) {
	err = initializeGlobals(confMap, fissionErrChan)
	if nil != err {
		return
	}

	// TODO

	err = performMountFUSE()
	if nil != err {
		return
	}

	// TODO

	err = startHTTPServer()
	if nil != err {
		return
	}

	return
}

func stop() (err error) {
	err = stopHTTPServer()
	if nil != err {
		return
	}

	// TODO

	err = performUnmountFUSE()
	if nil != err {
		return
	}

	// TODO

	err = uninitializeGlobals()

	return
}

func signal() (err error) {
	logSIGHUP()

	err = nil
	return
}
