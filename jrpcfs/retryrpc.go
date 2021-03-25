// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

import (
	"crypto/tls"
	"fmt"

	"github.com/NVIDIA/proxyfs/logger"
	"github.com/NVIDIA/proxyfs/retryrpc"
)

func retryRPCServerUp(jserver *Server) {

	var err error

	if globals.retryRPCPort == 0 {
		return
	}

	// Create a new RetryRPC Server.
	retryConfig := &retryrpc.ServerConfig{LongTrim: globals.retryRPCTTLCompleted, ShortTrim: globals.retryRPCAckTrim,
		IPAddr: globals.publicIPAddr, Port: int(globals.retryRPCPort), DeadlineIO: globals.retryRPCDeadlineIO,
		KeepAlivePeriod: globals.retryRPCKeepAlivePeriod}

	// Retrive credentials from globals if they are valid.
	//
	// If the credentials are invalid, create self-signed certifcate and log message
	if globals.rootCAx509CertificatePEM == nil || globals.retryRPCCertPEM == nil || globals.retryRPCKeyPEM == nil {
		var (
			constructErr error
		)
		retryConfig.ServerCreds, constructErr = retryrpc.ConstructCreds(globals.publicIPAddr)
		if constructErr != nil {
			logger.PanicfWithError(constructErr, "Unable to create ServerCreds")
		}
		logger.Infof("Invalid credentials passed retryrpc - creating self-signed. Passed rootCAx509CertificatePEM: %v retryRPCCertPEM: %v retryRPCKeyPEM: %v",
			globals.rootCAx509CertificatePEM, globals.retryRPCCertPEM, globals.retryRPCKeyPEM)
	} else {
		svrCert, tmpErr := tls.X509KeyPair(globals.retryRPCCertPEM, globals.retryRPCKeyPEM)
		if tmpErr != nil {
			pMsg := fmt.Errorf("tls.X509KeyPair failed with error: %v", tmpErr)
			panic(pMsg)

		}
		retryConfig.ServerCreds = &retryrpc.ServerCreds{RootCAx509CertificatePEM: globals.rootCAx509CertificatePEM, ServerTLSCertificate: svrCert}
	}

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
