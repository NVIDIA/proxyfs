package main

import (
	"bazil.org/fuse"

	"github.com/swiftstack/ProxyFS/utils"
)

// This file is a temporary implementation of handleWriteRequest()

func handleWriteRequestTODO(request *fuse.WriteRequest) {
	logInfof("TODO: handleWriteRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}
