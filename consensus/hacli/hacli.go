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

	// Setup to do administrative operations
	cs.CLI()

	return cs
}

func teardownConnection(cs *consensus.Struct) {

	// Unregister from the etcd cluster
	cs.Unregister()
}

func listNode(cs *consensus.Struct, node string, nodeState map[string]string,
	nodesHb map[string]time.Time) {

	if node != "" {
		fmt.Printf("Node: %v State: %v Last HB: %v\n", node, nodeState[node], nodesHb[node])
		return
	}
	fmt.Printf("Node Information\n")
	for n := range nodeState {
		fmt.Printf("\tNode: %v State: %v Last HB: %v\n", n, nodeState[n], nodesHb[n])
	}

}

func listVg(cs *consensus.Struct, vg string, vgState map[string]string,
	vgNode map[string]string, vgIpaddr map[string]string,
	vgNetmask map[string]string, vgNic map[string]string,
	vgAutofail map[string]bool, vgEnabled map[string]bool,
	vgVolumelist map[string]string) {

	fmt.Printf("\tVG: %v State: %v Node: %v Ipaddr: %v Netmask: %v Nic: %v\n\t\tAutofail: %v Enabled: %v\n",
		vg, vgState[vg], vgNode[vg], vgIpaddr[vg], vgNetmask[vg], vgNic[vg], vgAutofail[vg],
		vgEnabled[vg])
	fmt.Printf("\tVolumes:\n")
	for _, v := range vgVolumelist[vg] {
		fmt.Printf("\t\t%v\n", v)
	}
	fmt.Printf("\n")
}

// listOp handles a listing
func listOp(node string, vg string) {
	cs := setupConnection()

	vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail, vgEnabled,
		vgVolumelist, _, _, nodesHb, nodesState := cs.List()

	if vg != "" {
		if vgState[vg] != "" {
			listVg(cs, vg, vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail,
				vgEnabled, vgVolumelist)
		} else {
			fmt.Printf("vg: %v does not exist\n", vg)
		}
	} else if node != "" {
		if nodesState[node] != "" {
			listNode(cs, node, nodesState, nodesHb)
		} else {
			fmt.Printf("Node: %v does not exist\n", node)
		}
	} else {
		listNode(cs, node, nodesState, nodesHb)

		fmt.Printf("\nVolume Group Information\n")
		for n := range vgState {
			listVg(cs, n, vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail,
				vgEnabled, vgVolumelist)
		}
	}

	teardownConnection(cs)
}

func main() {

	// Setup Subcommands
	listCommand := flag.NewFlagSet("list", flag.ExitOnError)
	offlineCommand := flag.NewFlagSet("offline", flag.ExitOnError)
	onlineCommand := flag.NewFlagSet("online", flag.ExitOnError)
	/*
		  TODO - add watch and set commands to watch for changes for
		  example a VG, etc or to change the state of a VG in a unit
		  test.

			setCommand := flag.NewFlagSet("set", flag.ExitOnError)
			watchCommand := flag.NewFlagSet("watch", flag.ExitOnError)
	*/

	/*
		TODO - Have node STOP command instead of OFFLINE ???
		returns when node transitions to DEAD
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
		fmt.Println("list, offline or online subcommand is required")
		flag.PrintDefaults()
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
		fmt.Println("invalid subcommand - valid subcommands are list, offline or online")
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

		if (*offlineNodePtr == "") && (*offlineVgPtr == "") {
			offlineCommand.PrintDefaults()
			os.Exit(1)
		}

		if (*offlineNodePtr != "") && (*offlineVgPtr != "") {
			offlineCommand.PrintDefaults()
			os.Exit(1)
		}

		cs := setupConnection()

		// Offline the VG
		if *offlineVgPtr != "" {
			err := cs.CLIOfflineVg(*offlineVgPtr)
			if err != nil {
				fmt.Printf("Offline failed with error: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		}

		// Offline all VGs on the node and stop the node
		if *offlineNodePtr != "" {
			fmt.Printf("Node ptr: %v\n", *offlineNodePtr)
			err := cs.CLIOfflineNode(*offlineNodePtr)
			if err != nil {
				fmt.Printf("Offline failed with error: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}

	// Check which subcommand was Parsed for online
	if onlineCommand.Parsed() {

		fmt.Printf("onlineNodePtr is: %v onlineVgPtr: %v\n", *onlineNodePtr, *onlineVgPtr)
		// TODO - online the VG

		if (*onlineNodePtr == "") || (*onlineVgPtr == "") {
			onlineCommand.PrintDefaults()
			os.Exit(1)
		}
	}
}
