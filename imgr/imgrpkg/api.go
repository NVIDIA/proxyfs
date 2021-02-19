// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

// Start is called to start serving
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

// Signal is called to interrupt the server for performing operations such as log rotation
//
func Signal() (err error) {
	err = signal()
	return
}
