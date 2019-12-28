package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/retryrpc"
)

func sendIt(client *retryrpc.Client, i int, sendWg *sync.WaitGroup) {
	defer sendWg.Done()

	// Send a ping RPC and print the results
	msg := fmt.Sprintf("Ping Me - %v", i)
	pingRequest := &jrpcfs.PingReq{Message: msg}
	pingReply := &jrpcfs.PingReply{}
	expectedReply := fmt.Sprintf("pong %d bytes", len(msg))
	err := client.Send("RpcPing", pingRequest, pingReply)
	if err != nil {
		fmt.Printf("client.Send(RpcPing) failed with err: %v\n", err)
		os.Exit(-1)
	}
	if expectedReply != pingReply.Message {
		fmt.Printf("ERR ==== client: '%+v'\n", client)
		fmt.Printf("         client.Send(RpcPing) reply '%+v'\n", pingReply)
		fmt.Printf("         client.Send(RpcPing) expected '%s' but received '%s'\n", expectedReply, pingReply.Message)
		fmt.Printf("         client.Send(RpcPing) SENT: msg '%v' but received '%s'\n", msg, pingReply.Message)
		fmt.Printf("         client.Send(RpcPing) len(pingRequest.Message): '%d' i: %v\n", len(pingRequest.Message), i)
		os.Exit(-1)
	}
}

// Represents a pfsagent - sepearate client
func pfsagent(ipAddr string, retryRPCPortString string, aid uint64, agentWg *sync.WaitGroup,
	sendCnt int) {
	defer agentWg.Done()

	// 1. setup client and connect to proxyfsd
	// 2. loop doing RPCs in parallel
	clientID := fmt.Sprintf("client - %v", aid)
	client := retryrpc.NewClient(clientID)

	// Have client connect to server
	port, _ := strconv.Atoi(retryRPCPortString)
	err := client.Dial(ipAddr, port)
	if err != nil {
		fmt.Printf("Dial() failedd with err: %v\n", err)
		return
	}

	var sendWg sync.WaitGroup

	var z int
	for i := 0; i < sendCnt; i++ {
		z = (z + i) * 10

		sendWg.Add(1)
		go sendIt(client, z, &sendWg)
	}
	sendWg.Wait()

	client.Close()
}

func parallelAgentSenders(ipAddr string, retryRPCPortString string, agentCnt int,
	sendCnt int) {

	var agentWg sync.WaitGroup

	// Figure out random seed for runs
	r := rand.New(rand.NewSource(99))
	clientSeed := r.Uint64()

	// Start parallel pfsagents - each agent doing sendCnt parallel sends
	var aid uint64
	for i := 0; i < agentCnt; i++ {
		aid = clientSeed + uint64(i)

		agentWg.Add(1)
		go pfsagent(ipAddr, retryRPCPortString, aid, &agentWg, sendCnt)
	}
	agentWg.Wait()
}

func main() {
	var (
		args               []string
		err                error
		confMap            conf.ConfMap
		whoAmI             string
		ipAddr             string
		retryRPCPortString string
	)

	// Parse arguments

	args = os.Args[1:]

	// Read in the program's os.Arg[1]-specified (and required) .conf file
	if len(args) == 0 {
		log.Fatalf("no .conf file specified")
	}

	confMap, err = conf.MakeConfMapFromFile(args[0])
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	// Update confMap with any extra os.Args supplied
	err = confMap.UpdateFromStrings(args[1:])
	if nil != err {
		log.Fatalf("failed to load config overrides: %v", err)
	}

	// Process resultant confMap

	if nil != err {
		log.Fatal(err)
	}

	// Fetch IPAddr from config file
	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get Cluster.WhoAmI from config file")
		return
	}
	ipAddr, err = confMap.FetchOptionValueString("Peer:"+whoAmI, "PrivateIPAddr")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get %s.PrivateIPAddr from config file", whoAmI)
		return
	}

	retryRPCPortString, err = confMap.FetchOptionValueString("JSONRPCServer", "RetryRPCPort")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.RetryRPCPort from config file")
		return
	}

	sendCnt := 100
	agentCnt := 100
	parallelAgentSenders(ipAddr, retryRPCPortString, agentCnt, sendCnt)
}
