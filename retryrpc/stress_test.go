package retryrpc

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/retryrpc/rpctest"
)

func TestStress(t *testing.T) {

	testLoop(t)
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

	rrSvr, ipAddr, port := getNewServer(65 * time.Second)
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
	parallelAgentSenders(t, rrSvr, ipAddr, port, agentCount, sendCount, rrSvr.Creds.RootCAx509CertificatePEM)

	rrSvr.Close()
}

func sendIt(t *testing.T, client *Client, i int, sendWg *sync.WaitGroup, agentID uint64) {
	assert := assert.New(t)
	defer sendWg.Done()

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
func pfsagent(t *testing.T, rrSvr *Server, ipAddr string, port int, agentID uint64, agentWg *sync.WaitGroup,
	sendCnt int, rootCAx509CertificatePEM []byte) {
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
		z = (z + i) * 10

		sendWg.Add(1)
		go func(z int) {
			sendIt(t, client, z, &sendWg, agentID)
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
	sendCnt int, rootCAx509CertificatePEM []byte) {

	var agentWg sync.WaitGroup

	// Figure out random seed for runs
	r := rand.New(rand.NewSource(99))
	clientSeed := r.Uint64()

	// Start parallel pfsagents - each agent doing sendCnt parallel sends
	var agentID uint64
	for i := 0; i < agentCnt; i++ {
		agentID = clientSeed + uint64(i)

		agentWg.Add(1)
		go pfsagent(t, rrSrv, ipAddr, port, agentID, &agentWg, sendCnt, rootCAx509CertificatePEM)
	}
	agentWg.Wait()
}
