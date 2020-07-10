package retryrpc

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/retryrpc/rpctest"
)

// Test Upcall() functionality
func TestUpCall(t *testing.T) {
	testUpCall(t)
}

type MyClient struct {
	sync.Mutex
	cond        *sync.Cond // Signal that received Interrupt() callback
	sawCallback bool       // True if Interrupt() was called
}

func (cb *MyClient) Interrupt(payload []byte) {
	cb.Lock()
	cb.sawCallback = true
	cb.cond.Broadcast()
	cb.Unlock()
	return
}

// Test getting an upcallbasic Server creation and deletion
func testUpCall(t *testing.T) {
	var myUniqueClientID string = "my upcall client 1"

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
	cb := &MyClient{}
	cb.cond = sync.NewCond(&cb.Mutex)

	clientConfig := &ClientConfig{MyUniqueID: myUniqueClientID, IPAddr: ipaddr, Port: port,
		RootCAx509CertificatePEM: rrSvr.Creds.RootCAx509CertificatePEM, Callbacks: cb,
		DeadlineIO: 5 * time.Second}
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

	// Send an upcall() from the server to the client and
	// verify that client method is called.
	var msg1 []byte = []byte("server msg back to client")
	rrSvr.SendCallback(myUniqueClientID, msg1)

	// Wait until Interrupt() was called
	cb.Lock()
	for cb.sawCallback != true {
		cb.cond.Wait()
	}
	cb.Unlock()

	// Dump stats
	/* DEBUG ONLY -
	fmt.Printf("pfsagent dump stats: %s\n", bucketstats.SprintStats(bucketstats.StatFormatParsable1, "proxyfs.retryrpc", rrClnt.GetStatsGroupName()))
	*/

	// Stop the client before exiting
	rrClnt.Close()

	// Stop the server before exiting
	rrSvr.Close()
}
