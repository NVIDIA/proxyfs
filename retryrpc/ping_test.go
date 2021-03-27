// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package retryrpc

import (
	"bytes"
	"fmt"

	"github.com/NVIDIA/proxyfs/blunder"
)

// TestPingServer is a struct with pointer receivers implementing RpcTestPing*()
type TestPingServer struct{}

// TestPingReq is the request object for RpcTestPing*()
type TestPingReq struct {
	Message string
}

// TestPingReply is the response object for RpcTestPing*()
type TestPingReply struct {
	Message string
}

// Simple ping for testing the RPC layer
func testEncodeErrno(e *error) {
	if *e != nil {
		*e = fmt.Errorf("errno: %d", blunder.Errno(*e))
	}
}

// RpcTestPing simply does a len on the message path and returns the result
func (s *TestPingServer) RpcTestPing(in *TestPingReq, reply *TestPingReply) (err error) {
	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return nil
}

var largeStr string = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"

// RpcTestPingLarge simply does a len on the message path and returns the result
// along with a larger buffer.
func (s *TestPingServer) RpcTestPingLarge(in *TestPingReq, reply *TestPingReply) (err error) {
	buf := bytes.Buffer{}
	p := fmt.Sprintf("pong %d bytes", len(in.Message))
	buf.WriteString(p)
	for i := 0; i < 1000; i++ {
		buf.WriteString(largeStr)
	}
	reply.Message = fmt.Sprintf("%v", buf.String())
	return nil
}

// RpcTestPingWithError returns an error
func (s *TestPingServer) RpcTestPingWithError(in *TestPingReq, reply *TestPingReply) (err error) {
	err = blunder.AddError(err, blunder.NotFoundError)
	testEncodeErrno(&err)
	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return err
}

// RpcTestPingWithClientID simply does a len on the message path and returns the clientID & result
func (s *TestPingServer) RpcTestPingWithClientID(clientID uint64, in *TestPingReq, reply *TestPingReply) (err error) {
	reply.Message = fmt.Sprintf("Client ID: %v pong %d bytes", clientID, len(in.Message))
	return nil
}

// RpcTestPingWithInvalidClientID is not a valid RPC
// Note: Currently unused
func (s *TestPingServer) RpcTestPingWithInvalidClientID(clientID int, in *TestPingReq, reply *TestPingReply) (err error) {
	err = blunder.AddError(err, blunder.NotFoundError)
	testEncodeErrno(&err)
	reply.Message = fmt.Sprintf("client ID: %v pong %d bytes", clientID, len(in.Message))
	return err
}
