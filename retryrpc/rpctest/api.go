package rpctest

// rcptest is a set of RPC methods used to test retryrpc

import ()

// PingReq is the request object for RpcPing
type PingReq struct {
	Message string
}

// PingReply is the response object for RpcPutLocation
type PingReply struct {
	Message string
}

type Server struct{}

// NewServer creates the Server object and returns the pointer
func NewServer() *Server {
	s := Server{}

	return &s
}
