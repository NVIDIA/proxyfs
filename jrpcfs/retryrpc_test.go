package jrpcfs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/retryrpc"
)

// Test basic retryrpc primitives
//
// This unit test exists here since it uses jrpcfs which would be a
// circular dependency if the test was in retryrpc.
func TestRetryRPC(t *testing.T) {

	testServer(t)
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

// Test basic Server creation and deletion
func testServer(t *testing.T) {
	var (
		ipaddr = "127.0.0.1"
		port   = 24456
	)
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new jrpcfs server - needed for calling
	// jrcpfs RPCs
	myJrpcfs := NewServer()

	// Create a new RetryRPC Server.  Completed request will live on
	// completedRequests for 5 seconds.
	rrSvr := retryrpc.NewServer(5*time.Second, ipaddr, port)
	assert.NotNil(rrSvr)

	// Register the Server - sets up the methods supported by the
	// server
	err := rrSvr.Register(myJrpcfs)
	assert.Nil(err)

	// Start listening for requests on the ipaddr/port
	listener, lisErr := rrSvr.Start()
	assert.NotNil(listener, "Listener should not be nil")
	assert.Nil(lisErr, "lisErr is not nil")

	// Tell server to start accepting and processing requests
	rrSvr.Run()

	// Now - setup a client to send requests to the server
	rrClnt := retryrpc.NewClient("client 1")
	assert.NotNil(rrClnt)

	// Have client connect to server
	rrClnt.Dial(ipaddr, port)

	pingRequest := &PingReq{Message: "Ping Me!"}
	pingReply := &PingReply{}
	sendErr := rrClnt.Send("RpcPing", pingRequest, pingReply)
	assert.Nil(sendErr)
	assert.Equal("pong 8 bytes", pingReply.Message)

	assert.Equal(0, rrSvr.PendingCnt())
	assert.Equal(1, rrSvr.CompletedCnt())

	// Stop the client before exiting
	rrClnt.Close()

	// Stop the server before exiting
	rrSvr.Close()
}
