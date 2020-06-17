package retryrpc

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	/* DEBUG for pprof
	 */
	_ "net/http/pprof"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/retryrpc/rpctest"
)

func TestStress(t *testing.T) {

	/*
	 * DEBUG - used to debug memory leaks
	 * Run " go tool pprof  http://localhost:12123/debug/pprof/heap"
	 * to look at memory inuse
	 */
	// Start the ws that listens for pprof requests
	go http.ListenAndServe("localhost:12123", nil)

	testLoop(t)
	testLoopClientAckTrim(t)
	testLoopTTLTrim(t)
	testSendLargeRPC(t)
}

func testLoop(t *testing.T) {
	var (
		agentCount = 15
		sendCount  = 250
	)
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	rrSvr, ipAddr, port := getNewServer(65*time.Second, false)
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

	// Start up the agents
	parallelAgentSenders(t, rrSvr, ipAddr, port, agentCount, "RpcPing", sendCount, rrSvr.Creds.RootCAx509CertificatePEM)

	rrSvr.Close()
}

// testLoopClientAckTrim tests that we are correctly trimming messages
// based on the shorter term trimmer.   The shorter term trimmer relies
// on the client code saying "this is the highest consecutive sqn we have
// seen".   Then the server can throw away messages up to and including the
// highest consecutive sqn.
func testLoopClientAckTrim(t *testing.T) {
	var (
		agentCount = 15
		sendCount  = 250
	)
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	whenTTL := 10 * time.Millisecond
	rrSvr, ipAddr, port := getNewServer(whenTTL, true)
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

	// Start up the agents
	parallelAgentSenders(t, rrSvr, ipAddr, port, agentCount, "RpcPing", sendCount, rrSvr.Creds.RootCAx509CertificatePEM)

	// Now for both trimmers to run
	tm := time.Now()

	// First the 100ms trimmer - this will leave 1 entry on completed request queue
	// for each agent since there is no remaining client request to say it is completed.
	//
	// We need the TTL timer to clean up the last entry
	rrSvr.trimCompleted(tm, false)
	assert.Equal(agentCount, cntNotTrimmed(rrSvr), "Should have agentCount messages remaining")

	// Make sure the queue messages will be old enough to be trimmed
	time.Sleep(whenTTL)

	// Now the TTL timer to cleanup the last
	tmTTL := time.Now()
	rrSvr.trimCompleted(tmTTL, true)

	// All messages should be trimmed at this point
	assert.Equal(0, cntNotTrimmed(rrSvr), "Still have incomplete messages")

	/*
		 *  DEBUG - allows user to use pprof to check for memory leaks
		// The caller of this test will block and we can check for memory leaks with pprof
		fmt.Printf("\n=========== SLEEP 5 minutes ===================\n")
		time.Sleep(5 * time.Minute)
	*/

	rrSvr.Close()
}

func testLoopTTLTrim(t *testing.T) {
	var (
		agentCount = 15
		sendCount  = 250
	)
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	whenTTL := 10 * time.Millisecond
	rrSvr, ipAddr, port := getNewServer(whenTTL, true)
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

	// Start up the agents
	parallelAgentSenders(t, rrSvr, ipAddr, port, agentCount, "RpcPing", sendCount, rrSvr.Creds.RootCAx509CertificatePEM)

	// Use the TTL trimmer to remove all messages after guaranteeing we are
	// past time when they should be removed
	time.Sleep(whenTTL)
	tmTTL := time.Now()
	rrSvr.trimCompleted(tmTTL, true)

	assert.Equal(0, cntNotTrimmed(rrSvr), "Still have incomplete messages")

	/*
		 * DEBUG - all time for pprof tool to be used for tracking down memory leaks
		// The caller of this test will block and we can check for memory leaks with pprof
		fmt.Printf("\n=========== SLEEP 5 minutes ===================\n")
		time.Sleep(5 * time.Minute)
	*/

	rrSvr.Close()
}

func testSendLargeRPC(t *testing.T) {
	var (
		agentCount = 15
		sendCount  = 250
	)
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	whenTTL := 10 * time.Millisecond
	rrSvr, ipAddr, port := getNewServer(whenTTL, true)
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

	// Start up the agents
	parallelAgentSenders(t, rrSvr, ipAddr, port, agentCount, "RpcPingLarge", sendCount, rrSvr.Creds.RootCAx509CertificatePEM)

	// Now for both trimmers to run
	tm := time.Now()

	// First the 100ms trimmer - this will leave 1 entry on completed request queue
	// for each agent since there is no remaining client request to say it is completed.
	//
	// We need the TTL timer to clean up the last entry
	rrSvr.trimCompleted(tm, false)
	assert.Equal(agentCount, cntNotTrimmed(rrSvr), "Should have agentCount messages remaining")

	// Make sure the queue messages will be old enough to be trimmed
	time.Sleep(whenTTL)

	// Now the TTL timer to cleanup the last
	tmTTL := time.Now()
	rrSvr.trimCompleted(tmTTL, true)

	/*
		 * DEBUG - sleep for a time for pprof tool to be used for tracking down memory leaks
		// The caller of this test will block and we can check for memory leaks with pprof
		fmt.Printf("\n=========== SLEEP 5 minutes ===================\n")
		time.Sleep(5 * time.Minute)
	*/

	// All messages should be trimmed at this point
	assert.Equal(0, cntNotTrimmed(rrSvr), "Still have incomplete messages")

	rrSvr.Close()
}

// testLoopClientAckTrim tests that we are correctly trimming messages

func cntNotTrimmed(server *Server) (numItems int) {
	server.Lock()
	for _, ci := range server.perClientInfo {
		ci.Lock()
		if len(ci.completedRequest) != 0 {
			numItems += len(ci.completedRequest)
		} else {
			if ci.completedRequestLRU.Len() != 0 {
				numItems += ci.completedRequestLRU.Len()
			}
		}
		ci.Unlock()
	}
	server.Unlock()

	return
}

func ping(t *testing.T, client *Client, i int, agentID uint64, assert *assert.Assertions) {
	// Send a ping RPC and print the results
	msg := fmt.Sprintf("Ping Me - %v", i)
	pingRequest := &rpctest.PingReq{Message: msg}
	pingReply := &rpctest.PingReply{}
	expectedReply := fmt.Sprintf("pong %d bytes", len(msg))
	err := client.Send("RpcPing", pingRequest, pingReply)
	assert.Nil(err, "client.Send() returned an error")
	if expectedReply != pingReply.Message {
		fmt.Printf("		 client - AGENTID: %v\n", agentID)
		fmt.Printf("         client.Send(RpcPing) reply '%+v'\n", pingReply)
		fmt.Printf("         client.Send(RpcPing) expected '%s' but received '%s'\n", expectedReply, pingReply.Message)
		fmt.Printf("         client.Send(RpcPing) SENT: msg '%v' but received '%s'\n", msg, pingReply.Message)
		fmt.Printf("         client.Send(RpcPing) len(pingRequest.Message): '%d' i: %v\n", len(pingRequest.Message), i)
	}
	assert.Equal(expectedReply, pingReply.Message, "Received different output then expected")
}

// pingLarge responds to the RPC with a large packet
func pingLarge(t *testing.T, client *Client, i int, agentID uint64, assert *assert.Assertions) {
	// Send a ping RPC and print the results
	msg := fmt.Sprintf("Ping Me - %v", i)
	pingRequest := &rpctest.PingReq{Message: msg}
	pingReply := &rpctest.PingReply{}
	err := client.Send("RpcPing", pingRequest, pingReply)
	assert.Nil(err, "client.Send() returned an error")
}

func sendIt(t *testing.T, client *Client, i int, sendWg *sync.WaitGroup, agentID uint64, method string) {
	assert := assert.New(t)
	defer sendWg.Done()

	switch method {
	case "RpcPing":
		ping(t, client, i, agentID, assert)
		break
	case "RpcPingLarge":
		pingLarge(t, client, i, agentID, assert)
		break
	}
}

type stressMyClient struct {
	sync.Mutex
	cond         *sync.Cond // Signal that received Interrupt() callback
	sawCallback  bool       // True if Interrupt() was called
	interruptCnt int        // Count of Interrupt() calls received (best effort)
}

func (cb *stressMyClient) Interrupt(payload []byte) {
	cb.Lock()
	cb.sawCallback = true
	cb.interruptCnt++
	cb.cond.Broadcast()
	cb.Unlock()
	return
}

// Represents a pfsagent - sepearate client
func pfsagent(t *testing.T, rrSvr *Server, ipAddr string, port int, agentID uint64, method string,
	agentWg *sync.WaitGroup, sendCnt int, rootCAx509CertificatePEM []byte) {
	defer agentWg.Done()

	cb := &stressMyClient{}
	cb.cond = sync.NewCond(&cb.Mutex)
	clientID := fmt.Sprintf("client - %v", agentID)
	clientConfig := &ClientConfig{MyUniqueID: clientID, IPAddr: ipAddr, Port: port,
		RootCAx509CertificatePEM: rootCAx509CertificatePEM, Callbacks: cb, DeadlineIO: 5 * time.Second}
	client, err := NewClient(clientConfig)
	if err != nil {
		fmt.Printf("Dial() failed with err: %v\n", err)
		return
	}
	defer client.Close()

	var sendWg sync.WaitGroup

	var z, r int
	var msg1 []byte = []byte("server msg back to client")
	for i := 0; i < sendCnt; i++ {

		if i == sendCnt-1 {
			// Give server time to process messages.   This last
			// call to pfsagent() should approximately send the
			// highestConsecutive set to sendCnt - 1.
			time.Sleep(10 * time.Second)
		}

		z = (z + i) * 10

		sendWg.Add(1)
		go func(z int) {
			sendIt(t, client, z, &sendWg, agentID, method)
			rrSvr.SendCallback(clientID, msg1)
		}(z)

		// Occasionally drop the connection to the server to
		// simulate retransmits
		r = i % 10
		if r == 0 && (i != 0) {
			rrSvr.CloseClientConn()
		}
	}
	sendWg.Wait()
}

// Start a bunch of "pfsagents" in parallel
func parallelAgentSenders(t *testing.T, rrSrv *Server, ipAddr string, port int, agentCnt int,
	method string, sendCnt int, rootCAx509CertificatePEM []byte) {

	var agentWg sync.WaitGroup

	// Figure out random seed for runs
	r := rand.New(rand.NewSource(99))
	clientSeed := r.Uint64()

	// Start parallel pfsagents - each agent doing sendCnt parallel sends
	var agentID uint64
	for i := 0; i < agentCnt; i++ {
		agentID = clientSeed + uint64(i)

		agentWg.Add(1)
		go pfsagent(t, rrSrv, ipAddr, port, agentID, method, &agentWg, sendCnt, rootCAx509CertificatePEM)
	}
	agentWg.Wait()
}
