package jrpcfs

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRpcBypass(t *testing.T) {
	s := &Server{}

	testBasicRpcBypass(t, s)
}

// testMarshal will marshal the RPC request into the RpcBypass request
// TODO - move to API???
func testBypassMarshal(requestMethod string, request interface{}) (requestID uint64, bypassReq *BypassReq, marshalErr error) {
	var (
		dummyRequestID uint64 = 1
		jsonReq        []byte
	)

	// Convert the tunneled request to JSON and save in bypass request
	jsonReq, marshalErr = json.Marshal(request)

	// TODO - need mount ID???
	bypassReq = &BypassReq{
		JSONrpc: "2.0",
		Method:  requestMethod,
		ID:      dummyRequestID,
		Params:  jsonReq,
	}

	return
}

// testBasicRpcBypass tests the bypass RPC
func testBasicRpcBypass(t *testing.T, server *Server) {
	assert := assert.New(t)

	// Create a RpcPing request and tunnel via RpcBypass
	pingReply := &PingReply{}
	_, bypassReq, marshalErr := testBypassMarshal("Server.RpcPing", pingReply)
	if nil != marshalErr {
		/*
			logFatalf("unable to marshal request (jrpcMethod=%s jrpcParam=%v): %#v", jrpcSwiftProxyBypassRequest.jrpcMethod, jrpcSwiftProxyBypassRequest.jrpcParam, marshalErr)
		*/
	}

	response := BypassReply{}
	err := server.RpcBypass(bypassReq, &response)

	// TODO - verify the results with assert

	assert.Nil(err)
}
