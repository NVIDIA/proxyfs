package jrpcfs

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRpcBypass(t *testing.T) {
	s := &Server{}

	testBasicRpcBypass(t, s)
}

// TODO - move to API???
func bypassMarshalReq(requestMethod string, ID uint64, tunnelReq interface{}) (bypassReq *BypassReq, err error) {

	// Create request struct and convert tunnelReq to JSON
	// TODO - need mount ID???
	bypassReq = &BypassReq{
		JSONrpc: "2.0",
		Method:  requestMethod,
		ID:      ID,
	}

	// Convert the tunneled request to JSON and save in bypass Params
	bypassReq.Params, err = json.Marshal(tunnelReq)

	return
}

// testBasicRpcBypass tests the bypass RPC
func testBasicRpcBypass(t *testing.T, server *Server) {
	var (
		dummyRequestID uint64 = 1
	)

	assert := assert.New(t)

	// Create a RpcPing request
	pingReq := &PingReq{
		Message: "TestMessage",
	}

	// Create bypass request
	bypassReq, marshalErr := bypassMarshalReq("Server.RpcPing", dummyRequestID, pingReq)
	assert.Nil(marshalErr)

	// Send the bypass request
	bypassReply := BypassReply{}
	err := server.RpcBypass(bypassReq, &bypassReply)
	assert.Nil(err)

	// Unmarshal reply to bypass request
	var pingReply PingReply
	err = json.Unmarshal(bypassReply.Result, &pingReply)
	assert.Nil(err)

	// Verify the results with assert
	x := fmt.Sprintf("pong %d bytes", len(pingReq.Message))
	assert.Equal(x, pingReply.Message)
}
