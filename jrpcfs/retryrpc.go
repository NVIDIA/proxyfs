package jrpcfs

import (
	"time"

	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/retryrpc"
)

func retryRPCServerUp(jserver *Server, publicIPAddr string, retryRPCPort uint16, retryRPCTTLCompleted time.Duration) {
	var err error

	if 0 == globals.retryRPCPort {
		return
	}

	// Create a new RetryRPC Server.
	rrSvr := retryrpc.NewServer(retryRPCTTLCompleted, publicIPAddr, int(retryRPCPort))

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
	if 0 == globals.retryRPCPort {
		return
	}

	globals.connLock.Lock()
	rrSvr := globals.retryrpcSvr
	globals.connLock.Unlock()
	rrSvr.Close()
}
