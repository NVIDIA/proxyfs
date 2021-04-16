// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
)

/*
func testLoop(t *testing.T, useTLS bool) {
	var (
		agentCount = 15
		sendCount  = 250
	)
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new TestPingServer - needed for calling RPCs
	myJrpcfs := &TestPingServer{}

	rrSvr := getNewServer(65*time.Second, false, useTLS)
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
	parallelAgentSenders(t, rrSvr, agentCount, "RpcPing", sendCount, useTLS)

	rrSvr.Close()
}
*/

/*
func ping(t *testing.T, client *Client, i int, agentID uint64, assert *assert.Assertions) {
	// Send a ping RPC and print the results
	msg := fmt.Sprintf("Ping Me - %v", i)
	pingRequest := &TestPingReq{Message: msg}
	pingReply := &TestPingReply{}
	expectedReply := fmt.Sprintf("pong %d bytes", len(msg))
	err := client.Send("RpcTestPing", pingRequest, pingReply)
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
	pingRequest := &TestPingReq{Message: msg}
	pingReply := &TestPingReply{}
	err := client.Send("RpcTestPingLarge", pingRequest, pingReply)
	assert.Nil(err, "client.Send() returned an error")
}

func sendIt(t *testing.T, client *Client, z int, sendCnt int, sendWg *sync.WaitGroup, prevWg *sync.WaitGroup, agentID uint64, method string, i int) {

	assert := assert.New(t)
	defer sendWg.Done()

	switch method {
	case "RpcPing":
		ping(t, client, z, agentID, assert)
		break
	case "RpcPingLarge":
		pingLarge(t, client, z, agentID, assert)
		break
	}

	// The last send is blocked until the previous send has completed.   This
	// is how we test the short trimmer.
	if i <= (sendCnt - 2) {
		prevWg.Done()
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
func pfsagent(t *testing.T, rrSvr *Server, agentID uint64, method string, agentWg *sync.WaitGroup, sendCnt int, useTLS bool) {
	var (
		clientConfig *ClientConfig
	)

	defer agentWg.Done()

	cb := &stressMyClient{}
	cb.cond = sync.NewCond(&cb.Mutex)
	if useTLS {
		clientConfig = &ClientConfig{
			IPAddr:                   testIPAddr,
			Port:                     testPort,
			RootCAx509CertificatePEM: testTLSCerts.caCertPEMBlock,
			Callbacks:                cb,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
		}
	} else {
		clientConfig = &ClientConfig{
			IPAddr:                   testIPAddr,
			Port:                     testPort,
			RootCAx509CertificatePEM: nil,
			Callbacks:                cb,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
		}
	}
	client, err := NewClient(clientConfig)
	if err != nil {
		fmt.Printf("Dial() failed with err: %v\n", err)
		return
	}
	defer client.Close()

	// WG to verify all messages sent
	var sendWg sync.WaitGroup

	// WG to verify all but the last send() has been sent and
	// received.   This is needed to test the consecutive sequence
	// trimmer is working.
	var prevWg sync.WaitGroup

	var z, r int
	var msg1 []byte = []byte("server msg back to client")
	for i := 0; i < sendCnt; i++ {

		z = (z + i) * 10

		if i == (sendCnt - 1) {
			// Give server time to process messages.   This last
			// call gets us closer to highestConsecutive set to sendCnt - 1.
			prevWg.Wait()

			// The highest consecutive number is updated in the background with
			// a goroutine when send() returns.
			//
			// Therefore, we loop waiting for it to hit (sendCnt - 1)
			for {
				var currentHighest requestID
				client.Lock()
				currentHighest = client.highestConsecutive
				client.Unlock()

				if int(currentHighest) == (sendCnt - 1) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			prevWg.Add(1)
		}

		sendWg.Add(1)
		go func(z int, i int) {
			sendIt(t, client, z, sendCnt, &sendWg, &prevWg, agentID, method, i)
			rrSvr.SendCallback(client.GetMyUniqueID(), msg1)
		}(z, i)

		// Occasionally drop the connection to the server to
		// simulate retransmits
		r = i % 10
		if r == 0 && (i != 0) {
			rrSvr.CloseClientConn()
		}
	}
	sendWg.Wait()
}
*/

// Start a bunch of "clients" in parallel sending RPCs
func parallelClientSenders(ipAddr string, port string, agentCnt int, method string, sendCnt int, useTLS bool) {

	/*
		var agentWg sync.WaitGroup

		// Figure out random seed for runs
		r := rand.New(rand.NewSource(99))
		clientSeed := r.Uint64()

			// Start parallel pfsagents - each agent doing sendCnt parallel sends
			var agentID uint64
			for i := 0; i < agentCnt; i++ {
				agentID = clientSeed + uint64(i)

				agentWg.Add(1)
				go pfsagent(t, rrSrv, agentID, method, &agentWg, sendCnt, useTLS)
			}
		agentWg.Wait()
	*/
}

type ClientSubcommand struct {
	fs       *flag.FlagSet
	clients  int    // Number of clients which will be sending messages
	ipAddr   string // IP Address of server
	messages int    // Number of messages to send
	port     int    // Port on which the server is listening
}

func NewClientCommand() *ClientSubcommand {
	cs := &ClientSubcommand{
		fs: flag.NewFlagSet("client", flag.ContinueOnError),
	}

	cs.fs.IntVar(&cs.clients, "clients", 0, "Number of clients which will be sending a subset of messages")
	cs.fs.StringVar(&cs.ipAddr, "ipaddr", "", "IP Address of server")
	cs.fs.IntVar(&cs.messages, "messages", 0, "Number of total messages to send")
	cs.fs.IntVar(&cs.port, "port", 0, "Port on which the server is listening")

	return cs
}

func (cs *ClientSubcommand) Init(args []string) error {
	return cs.fs.Parse(args)
}

func (cs *ClientSubcommand) Name() string {
	return cs.fs.Name()
}

func (cs *ClientSubcommand) Run() (err error) {
	if cs.ipAddr == "" {
		err = fmt.Errorf("IP Address cannot be blank")
		cs.fs.PrintDefaults()
		return
	}
	if cs.port == 0 {
		err = fmt.Errorf("Port cannot be 0")
		cs.fs.PrintDefaults()
		return
	}
	if cs.messages == 0 {
		err = fmt.Errorf("Messages cannot be 0")
		cs.fs.PrintDefaults()
		return
	}
	if cs.clients == 0 {
		err = fmt.Errorf("Clients cannot be 0")
		cs.fs.PrintDefaults()
		return
	}
	fmt.Printf("clients: %v messages: %v\n", cs.clients, cs.messages)

	// TODO -
	// pass count of messages, warm up like Craig said..... what other options?
	// how dump bucketstats??
	/*
		parallelClientSenders(ipAddr, port, count)
	*/
	return nil
}
