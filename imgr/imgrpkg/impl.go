// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

func start(confMap conf.ConfMap) (err error) {
	err = initializeGlobals(confMap)

	return
}

func stop() (err error) {
	err = uninitializeGlobals()

	return
}

func signal() (err error) {
	logSIGHUP()

	err = nil
	return
}
