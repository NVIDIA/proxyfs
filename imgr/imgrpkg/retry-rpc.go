// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

type retryRPCServerStruct struct{}

var retryRPCServer *retryRPCServerStruct

func startRetryRPCServer() (err error) {
	retryRPCServer = &retryRPCServerStruct{}

	return nil // TODO
}

func stopRetryRPCServer() (err error) {
	retryRPCServer = nil

	return nil // TODO
}
