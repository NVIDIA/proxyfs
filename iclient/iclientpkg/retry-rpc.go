// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"github.com/NVIDIA/proxyfs/retryrpc"
)

func openRetryRPC() (err error) {
	globals.retryRPCClientConfig = &retryrpc.ClientConfig{
		DNSOrIPAddr:              globals.config.RetryRPCPublicIPAddr,
		Port:                     int(globals.config.RetryRPCPort),
		RootCAx509CertificatePEM: globals.retryRPCCACertPEM,
		Callbacks:                globals,
		DeadlineIO:               globals.config.RetryRPCDeadlineIO,
		KeepAlivePeriod:          globals.config.RetryRPCKeepAlivePeriod,
	}

	globals.retryRPCClient, err = retryrpc.NewClient(globals.retryRPCClientConfig)

	return // err as set by call to retryrpc.NewClient() is sufficient
}

func closeRetryRPC() (err error) {
	globals.retryRPCClient.Close()

	globals.retryRPCClientConfig = nil
	globals.retryRPCClient = nil

	err = nil
	return
}
