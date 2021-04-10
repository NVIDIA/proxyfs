// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

func start(confMap conf.ConfMap) (err error) {
	err = initializeGlobals(confMap)
	if nil != err {
		return
	}

	err = startSwiftClient()
	if nil != err {
		return
	}

	err = startVolumeManagement()
	if nil != err {
		return
	}

	err = startRetryRPCServer()
	if nil != err {
		return
	}

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

	err = stopRetryRPCServer()
	if nil != err {
		return
	}

	err = stopVolumeManagement()
	if nil != err {
		return
	}

	err = stopSwiftClient()
	if nil != err {
		return
	}

	err = uninitializeGlobals()

	return
}

func signal() (err error) {
	logSIGHUP()

	err = nil
	return
}
