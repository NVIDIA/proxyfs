// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/retryrpc"
)

// TODO - move to top
type globalsStruct struct {
	cs       *ClientSubcommand
	tlsCerts *tlsCertsStruct
	tlsDir   string // Directory containing TLS info
	useTLS   bool
}

var globals globalsStruct

func ping(client *retryrpc.Client, i int, agentID uint64) {
	// Send a ping RPC and print the results
	msg := fmt.Sprintf("Ping Me - %v", i)
	pingRequest := &PerfPingReq{Message: msg}
	pingReply := &PerfPingReply{}
	expectedReply := fmt.Sprintf("pong %d bytes", len(msg))
	err := client.Send("RpcPerfPing", pingRequest, pingReply)
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
		ping(client, i, agentID)
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
func pfsagent(agentID uint64, method string, agentWg *sync.WaitGroup, clients int, useTLS bool) {
	var (
		clientConfig *retryrpc.ClientConfig
	)

	defer agentWg.Done()

	cb := &stressMyClient{}
	cb.cond = sync.NewCond(&cb.Mutex)
	if useTLS {
		clientConfig = &retryrpc.ClientConfig{
			IPAddr:                   globals.cs.ipAddr,
			Port:                     globals.cs.port,
			RootCAx509CertificatePEM: globals.tlsCerts.caCertPEMBlock,
			Callbacks:                cb,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
		}
	} else {
		clientConfig = &retryrpc.ClientConfig{
			IPAddr:                   globals.cs.ipAddr,
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

	// WG to verify all messages sent
	var sendWg sync.WaitGroup

	for i := 0; i < globals.cs.messages; i++ {

		sendWg.Add(1)
		go func(i int) {
			sendIt(client, i, &sendWg, agentID, method)
		}(i)
	}
	sendWg.Wait()
}

// Start a bunch of "clients" and send messages
func parallelClientSenders(method string) {
	var (
		agentWg sync.WaitGroup
		err     error
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

	// TODO - warm up with N number of clients before doing
	// performance run...... bucketstats to do measurement??

	// Start parallel pfsagents - each agent doing sendCnt parallel sends
	var agentID uint64
	for i := 0; i < globals.cs.clients; i++ {
		agentID = clientSeed + uint64(i)

		agentWg.Add(1)
		go pfsagent(agentID, method, &agentWg, globals.cs.messages, globals.useTLS)
	}
	agentWg.Wait()
}

type ClientSubcommand struct {
	fs       *flag.FlagSet
	clients  int    // Number of clients which will be sending messages
	ipAddr   string // IP Address of server
	messages int    // Number of messages to send
	port     int    // Port on which the server is listening
	tlsDir   string // Directory to write TLS info
}

func NewClientCommand() *ClientSubcommand {
	cs := &ClientSubcommand{
		fs: flag.NewFlagSet("client", flag.ContinueOnError),
	}

	cs.fs.IntVar(&cs.clients, "clients", 0, "Number of clients which will be sending a subset of messages")
	cs.fs.StringVar(&cs.ipAddr, "ipaddr", "", "IP Address of server")
	cs.fs.IntVar(&cs.messages, "messages", 0, "Number of total messages to send")
	cs.fs.IntVar(&cs.port, "port", 0, "Port on which the server is listening")
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
	if cs.tlsDir == "" {
		err = fmt.Errorf("tlsdir cannot be empty")
		cs.fs.PrintDefaults()
		return
	}
	fmt.Printf("clients: %v messages: %v\n", cs.clients, cs.messages)

	// Use global structure to store settings and certificate instead of
	// passing on stack
	globals.cs = cs
	globals.useTLS = true // TODO - make option?
	globals.tlsDir = cs.tlsDir

	parallelClientSenders("RpcPerfPing")
	return nil
}
