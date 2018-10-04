package main

import (
	"flag"
	"fmt"
	"github.com/swiftstack/ProxyFS/consensus"
	"os"
	"time"
)

func setupConnection() (cs *consensus.Struct) {

	// TODO - endpoints should be an option or grab from configuration
	// file of etcd.conf
	endpoints := []string{"192.168.60.10:2379", "192.168.60.11:2379", "192.168.60.12:2379"}

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs, err := consensus.Register(endpoints, 2*time.Second)
	if err != nil {
		fmt.Printf("Register() returned err: %v\n", err)
		os.Exit(-1)
	}

	return cs
}

func teardownConnection(cs *consensus.Struct) {
	// NOTE: Currently this code is never hit
	//
	// Unregister from the etcd cluster
	cs.Unregister()
}

// listOp handles a listing
func listOp(node string, vg string) {
	cs := setupConnection()

	vgName, vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail, vgEnabled,
		vgVolumelist, nodesAlreadyDead, nodesOnline, nodesHb := cs.List()

	fmt.Printf("Dump everything for node: %v vg: %v\n", node, vg)
	fmt.Printf("vgName: %v\n", vgName)
	fmt.Printf("vgState: %v\n", vgState)
	fmt.Printf("vgNode: %v\n", vgNode)
	fmt.Printf("vgIpaddr: %v\n", vgIpaddr)
	fmt.Printf("vgNetmask: %v\n", vgNetmask)
	fmt.Printf("vgNic: %v\n", vgNic)
	fmt.Printf("vgAutofail: %v\n", vgAutofail)
	fmt.Printf("vgEnabled: %v\n", vgEnabled)
	fmt.Printf("vgVolumelist: %v\n", vgVolumelist)
	fmt.Printf("nodesAlreadyDead: %v\n", nodesAlreadyDead)
	fmt.Printf("nodesOnline: %v\n", nodesOnline)
	fmt.Printf("nodesHb: %v\n", nodesHb)

	teardownConnection(cs)
}

func main() {

	// Setup Subcommands
	listCommand := flag.NewFlagSet("list", flag.ExitOnError)
	offlineCommand := flag.NewFlagSet("offline", flag.ExitOnError)
	onlineCommand := flag.NewFlagSet("online", flag.ExitOnError)
	/*
		setCommand := flag.NewFlagSet("set", flag.ExitOnError)
		watchCommand := flag.NewFlagSet("watch", flag.ExitOnError)
	*/

	// List subcommand flag pointers
	listNodePtr := listCommand.String("node", "", "node to list - list all if empty")
	listVgPtr := listCommand.String("vg", "", "volume group to list - list all if empty")

	// Offline subcommand flag pointers
	offlineNodePtr := offlineCommand.String("node", "", "node to offline")
	offlineVgPtr := offlineCommand.String("vg", "", "volume group to offline")

	// Online subcommand flag pointers
	onlineNodePtr := onlineCommand.String("node", "", "node where to online VG (required)")
	onlineVgPtr := onlineCommand.String("vg", "", "volume group to online (required)")

	// Verify that a subcommand has been provided
	// os.Arg[0] is the main command
	// os.Arg[1] will be the subcommand
	if len(os.Args) < 2 {
		fmt.Println("subcommand is required")
		os.Exit(1)
	}

	// Switch on the subcommand
	// Parse the flags for appropriate FlagSet
	// FlagSet.Parse() requires a set of arguments to parse as input
	// os.Args[2:] will be all arguments starting after the subcommand at os.Args[1]
	switch os.Args[1] {
	case "list":
		listCommand.Parse(os.Args[2:])
	case "offline":
		offlineCommand.Parse(os.Args[2:])
	case "online":
		onlineCommand.Parse(os.Args[2:])
	default:
		// TODO - fix this error message
		fmt.Println("invalid subcommand")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Check which subcommand was Parsed for list
	if listCommand.Parsed() {
		if (*listNodePtr != "") && (*listVgPtr != "") {
			listCommand.PrintDefaults()
			os.Exit(1)
		}
		listOp(*listNodePtr, *listVgPtr)
	}

	// Check which subcommand was Parsed for offline
	if offlineCommand.Parsed() {

		fmt.Printf("offlineNodePtr is: %v offlineVgPtr: %v\n", *offlineNodePtr, *offlineVgPtr)

		if (*offlineNodePtr == "") && (*offlineVgPtr == "") {
			offlineCommand.PrintDefaults()
			os.Exit(1)
		}

		if (*offlineNodePtr != "") && (*offlineVgPtr != "") {
			offlineCommand.PrintDefaults()
			os.Exit(1)
		}

		if *offlineNodePtr != "" {
			fmt.Printf("Node ptr: %v\n", *offlineNodePtr)
			os.Exit(1)
		}
		if *offlineVgPtr != "" {
			fmt.Printf("volume group ptr: %v\n", *offlineVgPtr)
			os.Exit(1)
		}
		fmt.Printf("offline everything\n")
	}

	// Check which subcommand was Parsed for online
	if onlineCommand.Parsed() {

		fmt.Printf("onlineNodePtr is: %v onlineVgPtr: %v\n", *onlineNodePtr, *onlineVgPtr)

		if (*onlineNodePtr == "") || (*onlineVgPtr == "") {
			onlineCommand.PrintDefaults()
			os.Exit(1)
		}
	}
}
