// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"

	"golang.org/x/sys/unix"
)

// PerfPingServer is a struct with pointer receivers implementing RpcPerfPing*()
type PerfPingServer struct{}

// PerfPingReq is the request object for RpcPerfPing*()
type PerfPingReq struct {
	Message string
}

// PerfPingReply is the response object for RpcPerfPing*()
type PerfPingReply struct {
	Message string
}

// RpcTestPing simply does a len on the message path and returns the result
func (s *PerfPingServer) RpcPerfPing(in *PerfPingReq, reply *PerfPingReply) (err error) {
	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return nil
}

var largeStr string = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"

// RpcPerfPingLarge simply does a len on the message path and returns the result
// along with a larger buffer.
func (s *PerfPingServer) RpcPerfPingLarge(in *PerfPingReq, reply *PerfPingReply) (err error) {
	buf := bytes.Buffer{}
	p := fmt.Sprintf("pong %d bytes", len(in.Message))
	buf.WriteString(p)
	for i := 0; i < 1000; i++ {
		buf.WriteString(largeStr)
	}
	reply.Message = fmt.Sprintf("%v", buf.String())
	return nil
}

// RpcPerfPingWithError returns an error
func (s *PerfPingServer) RpcPerfPingWithError(in *PerfPingReq, reply *PerfPingReply) (err error) {
	err = fmt.Errorf("errno: %v", unix.EIO)
	reply.Message = fmt.Sprintf("pong %d bytes", len(in.Message))
	return err
}

// RpcPerfPingWithClientID simply does a len on the message path and returns the clientID & result
func (s *PerfPingServer) RpcPerfPingWithClientID(clientID uint64, in *PerfPingReq, reply *PerfPingReply) (err error) {
	reply.Message = fmt.Sprintf("Client ID: %v pong %d bytes", clientID, len(in.Message))
	return nil
}
