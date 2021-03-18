// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iswiftpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

// Start is called to start serving the NoAuth Swift Proxy Port and,
// optionally, the Auth Swift Proxy Port
//
func Start(confMap conf.ConfMap) (err error) {
	err = start(confMap)
	return
}

// Stop is called to stop serving
//
func Stop() (err error) {
	err = stop()
	return
}

func ForceReAuth() {
	forceReAuth()
}
