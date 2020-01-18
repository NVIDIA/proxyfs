package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

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
	} else {
		fmt.Printf("Worked----\n")
	}
}

// Represents a pfsagent - sepearate client
func pfsagent(ipAddr string, retryRPCPortString string, aid uint64, agentWg *sync.WaitGroup,
	sendCnt int) {
	defer agentWg.Done()

	// TODO - do mount request and get cert...

	// Have client connect to server
	port, _ := strconv.Atoi(retryRPCPortString)

	raw, readErr := ioutil.ReadFile("/tmp/cert.pem")
	if readErr != nil {
		fmt.Printf("readErr: %v\n", readErr)
	}

	// 1. setup client and connect to proxyfsd
	// 2. loop doing RPCs in parallel
	clientID := fmt.Sprintf("client - %v", aid)
	client, err := retryrpc.NewClient(clientID, ipAddr, port, raw)
	if err != nil {
		fmt.Printf("Dial() failed with err: %v\n", err)
		return
	}
	defer client.Close()

	for {
		var sendWg sync.WaitGroup

		var z int
		for i := 0; i < sendCnt; i++ {
			z = (z + i) * 10

			sendWg.Add(1)
			go sendIt(client, z, &sendWg)
		}
		sendWg.Wait()
	}
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
	time.Sleep(100 * time.Millisecond)
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
	ipAddr, err = confMap.FetchOptionValueString("Peer:"+whoAmI, "PublicIPAddr")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get %s. from config file", whoAmI)
		return
	}

	retryRPCPortString, err = confMap.FetchOptionValueString("JSONRPCServer", "RetryRPCPort")
	if nil != err {
		logger.ErrorfWithError(err, "failed to get JSONRPCServer.RetryRPCPort from config file")
		return
	}

	sendCnt := 100
	// agentCnt := 100
	agentCnt := 1
	parallelAgentSenders(ipAddr, retryRPCPortString, agentCnt, sendCnt)
}
