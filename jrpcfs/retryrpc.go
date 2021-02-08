// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

import (
	"time"

	"github.com/NVIDIA/proxyfs/logger"
	"github.com/NVIDIA/proxyfs/retryrpc"
)

func retryRPCServerUp(jserver *Server, publicIPAddr string, retryRPCPort uint16,
	retryRPCTTLCompleted time.Duration, retryRPCAckTrim time.Duration,
	retryRPCDeadlineIO time.Duration, retryRPCKeepAlivePeriod time.Duration) {

	var err error

	if globals.retryRPCPort == 0 {
		return
	}

	// Create a new RetryRPC Server.
	retryConfig := &retryrpc.ServerConfig{LongTrim: retryRPCTTLCompleted, ShortTrim: retryRPCAckTrim, IPAddr: publicIPAddr,
		Port: int(retryRPCPort), DeadlineIO: retryRPCDeadlineIO, KeepAlivePeriod: retryRPCKeepAlivePeriod}

	rrSvr := retryrpc.NewServer(retryConfig)

	// Register jrpcsfs methods with the retryrpc server
	err = rrSvr.Register(jserver)
	if err != nil {
		logger.ErrorfWithError(err, "failed to register Retry RPC handler")
		return
	}

	// Start the retryrpc server listener
	startErr := rrSvr.Start()
	if startErr != nil {
		logger.ErrorfWithError(startErr, "retryrpc.Start() failed with err: %v", startErr)
		return
	}

	globals.connLock.Lock()
	globals.retryrpcSvr = rrSvr
	globals.rootCAx509CertificatePEM = rrSvr.Creds.RootCAx509CertificatePEM
	globals.connLock.Unlock()

	// Tell retryrpc server to start accepting requests
	rrSvr.Run()
}

func retryRPCServerDown() {
	if globals.retryRPCPort == 0 {
		return
	}

	globals.connLock.Lock()
	rrSvr := globals.retryrpcSvr
	globals.connLock.Unlock()
	rrSvr.Close()
}
