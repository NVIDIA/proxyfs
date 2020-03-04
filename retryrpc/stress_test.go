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
		//agentCount = 10
		agentCount = 1
		sendCount  = 15
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

// Represents a pfsagent - sepearate client
func pfsagent(t *testing.T, rrSvr *Server, ipAddr string, port int, agentID uint64, agentWg *sync.WaitGroup,
	sendCnt int, rootCAx509CertificatePEM []byte) {
	defer agentWg.Done()

	clientID := fmt.Sprintf("client - %v", agentID)
	fmt.Printf("pfsagent() - calling NewClient - clientID: %v\n", clientID)
	client, err := NewClient(clientID, ipAddr, port, rootCAx509CertificatePEM)
	if err != nil {
		fmt.Printf("Dial() failed with err: %v\n", err)
		return
	}
	defer client.Close()

	var sendWg sync.WaitGroup

	// var z, r int
	var z int
	for i := 0; i < sendCnt; i++ {
		z = (z + i) * 10

		sendWg.Add(1)
		go sendIt(t, client, z, &sendWg, agentID)

		// Occasionally drop the connection to the server to
		// simulate retransmits
		/****** - March 3
		if i == (sendCnt - 1) {
			rrSvr.CloseClientConn()
		}
		******/
		if i == 0 {
			rrSvr.CloseClientConn()
		}
		/*******
			r = i % 10
			if r == 0 && (i != 0) {
				rrSvr.CloseClientConn()
			}
		********/
	}
	fmt.Printf("pfsagent: %v sentCnt: %v - now wait=========\n", agentID, sendCnt)
	sendWg.Wait()
	fmt.Printf("pfsagent: %v COMPLETED agentWG\n", agentID)
}

// Start a bunch of "pfsagents" in parallel
func parallelAgentSenders(t *testing.T, rrSrv *Server, ipAddr string, port int, agentCnt int,
	sendCnt int, rootCAx509CertificatePEM []byte) {

	var agentWg sync.WaitGroup

	// Figure out random seed for runs
	r := rand.New(rand.NewSource(99))
	clientSeed := r.Uint64()

	fmt.Printf("parallelAgentSenders - agentCnt: %v\n", agentCnt)

	// Start parallel pfsagents - each agent doing sendCnt parallel sends
	var agentID uint64
	for i := 0; i < agentCnt; i++ {
		agentID = clientSeed + uint64(i)

		agentWg.Add(1)
		go pfsagent(t, rrSrv, ipAddr, port, agentID, &agentWg, sendCnt, rootCAx509CertificatePEM)
	}
	agentWg.Wait()
	fmt.Printf("parallelAgentSenders() - after agentWg.Wait()-----\n")
}
