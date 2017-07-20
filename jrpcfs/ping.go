package jrpcfs

// Simple ping for testing the RPC layer
import (
	"fmt"

	"github.com/swiftstack/ProxyFS/logger"
)

func (s *Server) RpcPing(in *PingReq, reply *PingReply) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	if globals.dataPathLogging {
		flog := logger.TraceEnter("in.", in)
		defer func() { flog.TraceExitErr("reply.", err, reply) }()
	}

	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return nil
}
