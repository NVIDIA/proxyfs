package rpctest

// Simple ping for testing the RPC layer
import (
	"fmt"

	"github.com/swiftstack/ProxyFS/blunder"
)

func encodeErrno(e *error) {
	if *e != nil {
		*e = fmt.Errorf("errno: %d", blunder.Errno(*e))
	}
}

// RpcPing simply does a len on the message path and returns the result
func (s *Server) RpcPing(in *PingReq, reply *PingReply) (err error) {

	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return nil
}

// RpcPingWithError returns an error
func (s *Server) RpcPingWithError(in *PingReq, reply *PingReply) (err error) {
	err = blunder.AddError(err, blunder.NotFoundError)
	encodeErrno(&err)
	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return err
}
