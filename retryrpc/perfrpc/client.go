// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/retryrpc"
)

type globalsStruct struct {
	cs       *ClientSubcommand
	tlsCerts *tlsCertsStruct
	tlsDir   string // Directory containing TLS info
	useTLS   bool
}

var globals globalsStruct

func ping(client *retryrpc.Client, method string, i int, agentID uint64) {
	// Send a ping RPC and print the results
	msg := fmt.Sprintf("Ping Me - %v", i)
	pingRequest := &PerfPingReq{Message: msg}
	pingReply := &PerfPingReply{}
	expectedReply := fmt.Sprintf("pong %d bytes", len(msg))
	err := client.Send(method, pingRequest, pingReply)
	if err != nil {
		panic(err)
	}
	if expectedReply != pingReply.Message {
		fmt.Printf("		 client - AGENTID: %v\n", agentID)
		fmt.Printf("         client.Send(RpcPing) reply '%+v'\n", pingReply)
		fmt.Printf("         client.Send(RpcPing) expected '%s' but received '%s'\n", expectedReply, pingReply.Message)
		fmt.Printf("         client.Send(RpcPing) SENT: msg '%v' but received '%s'\n", msg, pingReply.Message)
		fmt.Printf("         client.Send(RpcPing) len(pingRequest.Message): '%d' i: %v\n", len(pingRequest.Message), i)
	}
}

func sendIt(client *retryrpc.Client, i int, sendWg *sync.WaitGroup, agentID uint64, method string) {

	defer sendWg.Done()

	switch method {
	case "RpcPerfPing":
		ping(client, method, i, agentID)
		break
	default:
		err := fmt.Errorf("Invalid method: %v", method)
		panic(err)
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
func pfsagent(agentID uint64, method string, agentWG *sync.WaitGroup, warmUpCompleteWG *sync.WaitGroup, fence chan int) {
	var (
		clientConfig *retryrpc.ClientConfig
		ipAddrOrDNS  string
	)
	defer agentWG.Done()

	if globals.cs.dnsName != "" {
		ipAddrOrDNS = globals.cs.dnsName
	} else {
		ipAddrOrDNS = globals.cs.ipAddr
	}

	cb := &stressMyClient{}
	cb.cond = sync.NewCond(&cb.Mutex)
	if globals.useTLS {
		clientConfig = &retryrpc.ClientConfig{
			DNSOrIPAddr:              ipAddrOrDNS,
			Port:                     globals.cs.port,
			RootCAx509CertificatePEM: globals.tlsCerts.caCertPEMBlock,
			Callbacks:                cb,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
		}
	} else {
		clientConfig = &retryrpc.ClientConfig{
			DNSOrIPAddr:              ipAddrOrDNS,
			Port:                     globals.cs.port,
			RootCAx509CertificatePEM: nil,
			Callbacks:                cb,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
		}
	}
	client, err := retryrpc.NewClient(clientConfig)
	if err != nil {
		err := fmt.Errorf("Dial() failed with err: %v\n", err)
		panic(err)
	}
	defer client.Close()

	// Send messages to create connection and warm up client/server
	var warmUpWg sync.WaitGroup
	for i := 0; i < globals.cs.warmUpCnt; i++ {

		warmUpWg.Add(1)
		go func(i int) {
			sendIt(client, i, &warmUpWg, agentID, method)
		}(i)
	}
	warmUpWg.Wait()

	// Signal caller that this agent has completed warmup cycle
	warmUpCompleteWG.Done()

	// We have sent and received all our messages used to warm up client/server.
	// Block here until the channel is closed which signifies that all goroutines
	// should run the test
	<-fence

	// Now run the real test
	var sendWG sync.WaitGroup
	for i := 0; i < globals.cs.messages; i++ {

		sendWG.Add(1)
		go func(i int) {
			sendIt(client, i, &sendWG, agentID, method)
		}(i)
	}
	sendWG.Wait()
}

// Start a bunch of "clients" and send messages
//
// We first start all the goroutines ("clients") and let them send
// some messages to the server.    This allows us to "warmup" the
// clients so we are only measuring the steady state.
func parallelClientSenders(method string) (duration time.Duration) {
	var (
		agentWG          sync.WaitGroup
		warmUpCompleteWG sync.WaitGroup
		err              error
	)

	// Figure out random seed for runs
	r := rand.New(rand.NewSource(99))
	clientSeed := r.Uint64()

	globals.tlsCerts = &tlsCertsStruct{}
	tlsSetFileNames(globals.tlsCerts, globals.tlsDir)

	globals.tlsCerts.caCertPEMBlock, err = ioutil.ReadFile(globals.tlsCerts.caCertFile)
	if err != nil {
		panic(err)
	}

	// Start parallel pfsagents - each agent setup connection to server,
	// send RPCs to warm up client/server and then block on channel named fence
	var agentID uint64
	fence := make(chan int)
	for i := 0; i < globals.cs.clients; i++ {
		agentID = clientSeed + uint64(i)

		agentWG.Add(1)
		warmUpCompleteWG.Add(1)
		go pfsagent(agentID, method, &agentWG, &warmUpCompleteWG, fence)
	}

	// TODO --- bucketstats to do measurement??

	// Wait for all agents to warm up
	warmUpCompleteWG.Wait()

	// At this point, we know that the goroutines/clients have started,
	// established their connection to the server and sent some warm up
	// messages.   The goroutines then block on fence channel.
	//
	// We note the current time and then close fence channel.  This in
	// effect releases all of the goroutines at once.

	start := time.Now()
	close(fence)

	// Now wait for all messages to be sent
	agentWG.Wait()
	duration = time.Since(start)
	return
}

type ClientSubcommand struct {
	fs        *flag.FlagSet
	clients   int    // Number of clients which will be sending messages
	ipAddr    string // IP Address of server
	dnsName   string // DNS Name of server
	warmUpCnt int    // Number of messages to send to "warm up"
	messages  int    // Number of messages to send
	port      int    // Port on which the server is listening
	dbgport   string // Debug port for pprof webserver
	tlsDir    string // Directory to write TLS info
}

func NewClientCommand() *ClientSubcommand {
	cs := &ClientSubcommand{
		fs: flag.NewFlagSet("client", flag.ContinueOnError),
	}

	cs.fs.IntVar(&cs.clients, "clients", 0, "Number of clients which will be sending a subset of messages")
	cs.fs.StringVar(&cs.ipAddr, "ipaddr", "", "IP Address of server (optional)")
	cs.fs.StringVar(&cs.dnsName, "dnsname", "", "DNS Name of server (optional)")
	cs.fs.IntVar(&cs.messages, "messages", 0, "Number of total messages to send")
	cs.fs.IntVar(&cs.warmUpCnt, "warmupcnt", 0, "Number of total messages to send warm up client/server")
	cs.fs.IntVar(&cs.port, "port", 0, "Port on which the server is listening")
	cs.fs.StringVar(&cs.dbgport, "dbgport", "", "Debug port for pprof webserver (optional)")
	cs.fs.StringVar(&cs.tlsDir, "tlsdir", "", "Directory to write TLS info")

	return cs
}

func (cs *ClientSubcommand) Init(args []string) error {
	return cs.fs.Parse(args)
}

func (cs *ClientSubcommand) Name() string {
	return cs.fs.Name()
}

func (cs *ClientSubcommand) Run() (err error) {
	if cs.ipAddr == "" && cs.dnsName == "" {
		err = fmt.Errorf("Must pass either IP Address or DNS name")
		cs.fs.PrintDefaults()
		return
	}
	if cs.ipAddr != "" && cs.dnsName != "" {
		err = fmt.Errorf("Must pass only IP Address or DNS name")
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
	if cs.tlsDir == "" {
		err = fmt.Errorf("tlsdir cannot be empty")
		cs.fs.PrintDefaults()
		return
	}

	// Start debug webserver if we have a debug port
	if cs.dbgport != "" {
		hostPort := net.JoinHostPort("localhost", cs.dbgport)
		go http.ListenAndServe(hostPort, nil)
	}

	fmt.Printf("clients: %v messages: %v\n", cs.clients, cs.messages)

	// Use global structure to store settings and certificate instead of
	// passing on stack
	globals.cs = cs
	globals.useTLS = true // TODO - make option?
	globals.tlsDir = cs.tlsDir

	// Run the performance test
	duration := parallelClientSenders("RpcPerfPing")
	fmt.Printf("\n===== PERFRPC - Clients: %v Messages per Client: %v Total Messages: %v ---- Test Duration: %v\n",
		globals.cs.clients, globals.cs.messages, globals.cs.clients*globals.cs.messages, duration)
	return nil
}
