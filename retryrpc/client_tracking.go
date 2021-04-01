// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package retryrpc

// This file contains functions in the server which
// keep track of clients.

func (ci *clientInfo) isEmpty() bool {
	return len(ci.completedRequest) == 0
}

func (ci *clientInfo) completedCnt() int {
	return len(ci.completedRequest)
}
