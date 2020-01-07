package jrpcfs

import (
	"strconv"
	"time"

	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/retryrpc"
)

func retryRPCServerUp(jserver *Server, ipAddr string, portString string, completedTTL time.Duration) {
	var err error

	port, _ := strconv.Atoi(portString)

	// Create a new RetryRPC Server.
	rrSvr := retryrpc.NewServer(completedTTL, ipAddr, port)

	// Register jrpcsfs methods with the retryrpc server
	err = rrSvr.Register(jserver)
	if err != nil {
		logger.ErrorfWithError(err, "failed to register Retry RPC handler")
		return
	}
	globals.connLock.Lock()
	globals.retryrpcSvr = rrSvr
	globals.connLock.Unlock()

	// Start the retryrpc server listener
	_, listErr := rrSvr.Start()
	if listErr != nil {
		logger.ErrorfWithError(listErr, "net.Listen %s:%s failed", ipAddr, portString)
		return
	}

	// Tell retryrpc server to start accepting requests
	rrSvr.Run()
}

func retryRPCServerDown() {
	globals.connLock.Lock()
	rrSvr := globals.retryrpcSvr
	globals.connLock.Unlock()
	rrSvr.Close()
}
