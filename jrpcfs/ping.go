// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

// Simple ping for testing the RPC layer
import (
	"fmt"

	"github.com/NVIDIA/proxyfs/logger"
)

func (s *Server) RpcPing(in *PingReq, reply *PingReply) (err error) {
	enterGate()
	defer leaveGate()

	if globals.dataPathLogging {
		flog := logger.TraceEnter("in.", in)
		defer func() { flog.TraceExitErr("reply.", err, reply) }()
	}

	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return nil
}
