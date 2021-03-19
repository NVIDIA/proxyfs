// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package retryrpc

import (
	"testing"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/retryrpc/rpctest"
	"github.com/stretchr/testify/assert"
)

// Test basic retryrpc primitives
//
// This unit test exists here since it uses jrpcfs which would be a
// circular dependency if the test was in retryrpc.
func TestRetryRPC(t *testing.T) {

	testRegister(t)
	testServer(t)
	testBtree(t)
	testStatsAndBucketstats(t)
}

func getNewServer(lt time.Duration, dontStartTrimmers bool) (rrSvr *Server, ip string, p int) {
	var (
		ipaddr = "127.0.0.1"
		port   = 24456
	)
	config := &ServerConfig{LongTrim: lt, ShortTrim: 100 * time.Millisecond, IPAddr: "127.0.0.1",
		Port: 24456, DeadlineIO: 5 * time.Second, dontStartTrimmers: dontStartTrimmers}

	// Create a new RetryRPC Server.  Completed request will live on
	// completedRequests for 10 seconds.
	rrSvr = NewServer(config)
	ip = ipaddr
	p = port
	return
}

// Test register function finds all expected functions
func testRegister(t *testing.T) {
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	rrSvr, _, _ := getNewServer(10*time.Second, false)
	assert.NotNil(rrSvr)

	// Register the Server - sets up the methods supported by the
	// server
	err := rrSvr.Register(myJrpcfs)
	assert.Nil(err)

	// Make sure we discovered the correct functions
	assert.Equal(4, len(rrSvr.svrMap))
}

// Test basic Server creation and deletion
func testServer(t *testing.T) {
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	rrSvr, ipaddr, port := getNewServer(10*time.Second, false)
	assert.NotNil(rrSvr)

	// Register the Server - sets up the methods supported by the
	// server
	err := rrSvr.Register(myJrpcfs)
	assert.Nil(err)

	// Start listening for requests on the ipaddr/port
	startErr := rrSvr.Start()
	assert.Nil(startErr, "startErr is not nil")

	// Tell server to start accepting and processing requests
	rrSvr.Run()

	// Now - setup a client to send requests to the server
	clientConfig := &ClientConfig{MyUniqueID: "client 1", IPAddr: ipaddr, Port: port, RootCAx509CertificatePEM: rrSvr.Creds.RootCAx509CertificatePEM,
		Callbacks: nil, DeadlineIO: 5 * time.Second}
	rrClnt, newErr := NewClient(clientConfig)
	assert.NotNil(rrClnt)
	assert.Nil(newErr)

	// Send an RPC which should return success
	pingRequest := &rpctest.PingReq{Message: "Ping Me!"}
	pingReply := &rpctest.PingReply{}
	sendErr := rrClnt.Send("RpcPing", pingRequest, pingReply)
	assert.Nil(sendErr)
	assert.Equal("pong 8 bytes", pingReply.Message)
	assert.Equal(1, rrSvr.CompletedCnt())

	// Send an RPC which expects the client ID and which should return success
	pingRequest = &rpctest.PingReq{Message: "Ping Me!"}
	pingReply = &rpctest.PingReply{}
	sendErr = rrClnt.Send("RpcPingWithClientID", pingRequest, pingReply)
	assert.Nil(sendErr)
	assert.Equal("Client ID: unqClnt-1 pong 8 bytes", pingReply.Message)
	assert.Equal(2, rrSvr.CompletedCnt())

	// Send an RPC which should return an error
	pingRequest = &rpctest.PingReq{Message: "Ping Me!"}
	pingReply = &rpctest.PingReply{}
	sendErr = rrClnt.Send("RpcPingWithError", pingRequest, pingReply)
	assert.NotNil(sendErr)

	assert.Equal(3, rrSvr.CompletedCnt())

	// Send an RPC which should return an error
	pingRequest = &rpctest.PingReq{Message: "Ping Me!"}
	pingReply = &rpctest.PingReply{}
	sendErr = rrClnt.Send("RpcInvalidMethod", pingRequest, pingReply)
	assert.NotNil(sendErr)

	assert.Equal(4, rrSvr.CompletedCnt())

	// Stop the client before exiting
	rrClnt.Close()

	// Stop the server before exiting
	rrSvr.Close()
}

func testBtree(t *testing.T) {
	assert := assert.New(t)

	rrSvr, ipaddr, port := getNewServer(10*time.Second, false)
	assert.NotNil(rrSvr)

	// Setup a client - we only will be targeting the btree
	clientConfig := &ClientConfig{MyUniqueID: "client 1", IPAddr: ipaddr, Port: port, RootCAx509CertificatePEM: rrSvr.Creds.RootCAx509CertificatePEM,
		Callbacks: nil, DeadlineIO: 5 * time.Second}
	client, newErr := NewClient(clientConfig)
	assert.NotNil(client)
	assert.Nil(newErr)

	// Simulate requests completing out of order
	client.updateHighestConsecutiveNum(requestID(10))
	client.updateHighestConsecutiveNum(requestID(5))
	client.updateHighestConsecutiveNum(requestID(11))

	client.setHighestConsecutive()
	assert.Equal(requestID(0), client.highestConsecutive)

	// Now fillin first gap
	client.updateHighestConsecutiveNum(requestID(4))
	client.updateHighestConsecutiveNum(requestID(3))
	client.updateHighestConsecutiveNum(requestID(2))
	client.updateHighestConsecutiveNum(requestID(1))
	assert.Equal(int(3), client.bt.Len())

	client.setHighestConsecutive()
	assert.Equal(int(3), client.bt.Len())
	assert.Equal(requestID(5), client.highestConsecutive)

	// Now fillin next set of gaps
	client.updateHighestConsecutiveNum(requestID(6))
	client.updateHighestConsecutiveNum(requestID(7))
	client.updateHighestConsecutiveNum(requestID(8))
	client.updateHighestConsecutiveNum(requestID(9))
	assert.Equal(int(1), client.bt.Len())

	client.setHighestConsecutive()
	assert.Equal(int(1), client.bt.Len())
	assert.Equal(requestID(11), client.highestConsecutive)
}

// Per pfsagent statistics
type clientStats struct {
	AddCompleted           bucketstats.Total           // Number added to completed list
	RmCompleted            bucketstats.Total           // Number removed from completed list
	RPCLenUsec             bucketstats.BucketLog2Round // Average times of RPCs
	LongestRPCMethod       string                      // Method of longest RPC
	ReplySize              bucketstats.BucketLog2Round // Largest RPC reply size completed
	LargestReplySizeMethod string                      // Method of largest RPC reply size completed
	RPCcompleted           bucketstats.Total           // Number of RPCs which completed - incremented after call returns
	RPCretried             bucketstats.Total           // Number of RPCs which were just pulled from completed list
	RPCattempted           bucketstats.Total           // Number of RPCs attempted - may be completed or in process
	RPCinprocess           bucketstats.Total           // Number of RPCs presently calling RPC - decremented when completed
}

// Test use of bucketstats package
func testStatsAndBucketstats(t *testing.T) {
	var (
		myClient1       clientStats
		myUniqueClient1 = "1111111"

		myClient2       clientStats
		myUniqueClient2 = "2222222"
	)

	// Register from bucketstats from pfsagent #1
	bucketstats.Register("proxyfs.retryrpc", myUniqueClient1, &myClient1)

	// Register from bucketstats from pfsagent #2
	bucketstats.Register("proxyfs.retryrpc", myUniqueClient2, &myClient2)

	// Completed list stats
	myClient1.AddCompleted.Add(1)
	myClient1.RmCompleted.Add(1)

	// RPC counts
	myClient1.RPCcompleted.Add(1)
	myClient1.RPCretried.Add(1)
	myClient1.RPCattempted.Add(1)
	myClient1.RPCinprocess.Add(1)

	// Track duration of all RPCs in a graph
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	myClient1.RPCLenUsec.Add(uint64(time.Since(start) / time.Microsecond))
	myClient1.ReplySize.Add(8192)

	// Example of pfsagent #2
	myClient2.RPCcompleted.Add(1)
	myClient2.RPCretried.Add(1)
	myClient2.RPCattempted.Add(1)
	myClient2.RPCinprocess.Add(1)

	// Dump stats
	/* DEBUG ONLY -
	fmt.Printf("pfsagent #1: %s\n", bucketstats.SprintStats(bucketstats.StatFormatParsable1, "proxyfs.retryrpc", myUniqueClient1))
	fmt.Printf("pfsagent #2: %s\n", bucketstats.SprintStats(bucketstats.StatFormatParsable1, "proxyfs.retryrpc", myUniqueClient2))
	*/

	// Unregister clients from bucketstats
	bucketstats.UnRegister("proxyfs.retryrpc", myUniqueClient1)
	bucketstats.UnRegister("proxyfs.retryrpc", myUniqueClient2)
}
