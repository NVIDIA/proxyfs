package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/retryrpc"
)

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

	// 1. setup client and connect to proxyfsd
	// 2. do one ping request and print results
	client := retryrpc.NewClient("client 1")

	// Have client connect to server
	port, _ := strconv.Atoi(retryRPCPortString)
	client.Dial(ipAddr, port)

	// Send a ping RPC and print the results
	pingRequest := &jrpcfs.PingReq{Message: "Ping Me!"}
	pingReply := &jrpcfs.PingReply{}
	err = client.Send("RpcPing", pingRequest, pingReply)
	if err != nil {
		fmt.Printf("client.Send(RpcPing) failed with err: %v\n", err)
		os.Exit(-1)
	}
	fmt.Printf("client.Send(RpcPing) returned reply: %+v\n", pingReply)
}
