package retryrpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/retryrpc/rpctest"
)

// Test basic retryrpc primitives
//
// This unit test exists here since it uses jrpcfs which would be a
// circular dependency if the test was in retryrpc.
func TestRetryRPC(t *testing.T) {

	//	testServer(t)
	testBtree(t)
}

type MyType struct {
	field1 int
}

type MyRequest struct {
	Field1 int
}

type MyResponse struct {
	Error error
}

func (m *MyType) ExportedFunction(request MyRequest, response *MyResponse) (err error) {
	request.Field1 = 1
	return
}

func (m *MyType) unexportedFunction(i int) {
	m.field1 = i
}

func getNewServer() (rrSvr *Server, ip string, p int) {
	var (
		ipaddr = "127.0.0.1"
		port   = 24456
	)

	// Create a new RetryRPC Server.  Completed request will live on
	// completedRequests for 10 seconds.
	rrSvr = NewServer(10*time.Second, 100*time.Millisecond, ipaddr, port)
	ip = ipaddr
	p = port
	return
}

// Test basic Server creation and deletion
func testServer(t *testing.T) {
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	rrSvr, ipaddr, port := getNewServer()
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
	rrClnt, newErr := NewClient("client 1", ipaddr, port, rrSvr.Creds.RootCAx509CertificatePEM, nil)
	assert.NotNil(rrClnt)
	assert.Nil(newErr)

	// Send an RPC which should return success
	pingRequest := &rpctest.PingReq{Message: "Ping Me!"}
	pingReply := &rpctest.PingReply{}
	sendErr := rrClnt.Send("RpcPing", pingRequest, pingReply)
	assert.Nil(sendErr)
	assert.Equal("pong 8 bytes", pingReply.Message)

	assert.Equal(0, rrSvr.PendingCnt())
	assert.Equal(1, rrSvr.CompletedCnt())

	// Send an RPC which should return an error
	pingRequest = &rpctest.PingReq{Message: "Ping Me!"}
	pingReply = &rpctest.PingReply{}
	sendErr = rrClnt.Send("RpcPingWithError", pingRequest, pingReply)
	assert.NotNil(sendErr)

	assert.Equal(0, rrSvr.PendingCnt())
	assert.Equal(2, rrSvr.CompletedCnt())

	// TODO - TODO - TODO....
	// Verify that the server has seen the updated
	// highestReplySeen

	// Send an RPC which should return an error
	pingRequest = &rpctest.PingReq{Message: "Ping Me!"}
	pingReply = &rpctest.PingReply{}
	sendErr = rrClnt.Send("RpcInvalidMethod", pingRequest, pingReply)
	assert.NotNil(sendErr)

	assert.Equal(0, rrSvr.PendingCnt())
	assert.Equal(3, rrSvr.CompletedCnt())

	// Stop the client before exiting
	rrClnt.Close()

	// Stop the server before exiting
	rrSvr.Close()
}

func testBtree(t *testing.T) {
	assert := assert.New(t)

	rrSvr, ipaddr, port := getNewServer()
	assert.NotNil(rrSvr)

	// Setup a client - we only will be targeting the btree
	client, newErr := NewClient("client 1", ipaddr, port, rrSvr.Creds.RootCAx509CertificatePEM, nil)
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
