package retryrpc

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/retryrpc/rpctest"
)

// Test Upcall() functionality
func TestUpCall(t *testing.T) {
	testUpCall(t)
}

// TODO - change stress test to send upcalls???   upcalls are
// best effort and not queued? correct?

type MyClient struct {
	// TODO - have either a counter or WG to signal that saw message...
}

func (cb *MyClient) Interrupt(payload []byte) {
	fmt.Printf("CLIENT - Interrupt() payload: %s\n", payload)
	return
}

// Test getting an upcallbasic Server creation and deletion
func testUpCall(t *testing.T) {
	var myUniqueClientID string = "client 1"

	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	rrSvr, ipaddr, port := getNewServer(10 * time.Second)
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
	cb := &MyClient{}
	rrClnt, newErr := NewClient(myUniqueClientID, ipaddr, port, rrSvr.Creds.RootCAx509CertificatePEM, cb)
	assert.NotNil(rrClnt)
	assert.Nil(newErr)

	// Send an RPC which should return success
	pingRequest := &rpctest.PingReq{Message: "Ping Me!"}
	pingReply := &rpctest.PingReply{}
	sendErr := rrClnt.Send("RpcPing", pingRequest, pingReply)
	assert.Nil(sendErr)
	assert.Equal("pong 8 bytes", pingReply.Message)
	assert.Equal(1, rrSvr.CompletedCnt())

	// Send an upcall() from the server to the client and
	// verify that client method is called.
	var msg1 []byte = []byte("server msg back to client")
	rrSvr.SendCallback(myUniqueClientID, msg1)

	// TODO - add test to verify the callback is called....
	// make part of retryrpc_test.go?
	// TODO - block until WG/broadcast in cb....
	time.Sleep(100 * time.Millisecond)

	// Stop the client before exiting
	rrClnt.Close()

	// Stop the server before exiting
	rrSvr.Close()
}
