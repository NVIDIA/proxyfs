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

	fmt.Printf("\tVG: %v State: '%v' Node: '%v' Ipaddr: '%v' Netmask: '%v' Nic: '%v'\n",
		vg, vgState[vg], vgNode[vg], vgIpaddr[vg], vgNetmask[vg], vgNic[vg])
	fmt.Printf("\t\tAutofail: %v Enabled: %v\n", vgAutofail[vg], vgEnabled[vg])

	fmt.Printf("\tVolumes:\n")
	for _, v := range vgVolumelist[vg] {
		fmt.Printf("\t\t%v\n", v)
	}
	fmt.Printf("\n")
}

// listOp handles a listing
func listOp(nodeName string, vgName string) {

	cs := setupConnection()

	clusterInfo := cs.List()
	vgInfo := clusterInfo.VgInfo
	nodeInfo := clusterInfo.NodeInfo

	if vgName != "" {
		if vgInfo.VgState[vgName] != "" {
			listVg(cs, vgName, vgInfo.VgState, vgInfo.VgNode,
				vgInfo.VgIpAddr, vgInfo.VgNetmask, vgInfo.VgNic,
				vgInfo.VgAutofail, vgInfo.VgEnabled, vgInfo.VgVolumeList)
		} else {
			fmt.Printf("vg: %v does not exist\n", vgName)
		}
	} else if nodeName != "" {
		if nodeInfo.NodesState[nodeName] != "" {
			listNode(cs, nodeName, nodeInfo.NodesState, nodeInfo.NodesHb)
		} else {
			fmt.Printf("Node: %v does not exist\n", nodeName)
		}
	} else {
		listNode(cs, nodeName, nodeInfo.NodesState, nodeInfo.NodesHb)

		fmt.Printf("\nVolume Group Information\n")
		for n := range vgInfo.VgState {
			listVg(cs, n, vgInfo.VgState, vgInfo.VgNode,
				vgInfo.VgIpAddr, vgInfo.VgNetmask, vgInfo.VgNic,
				vgInfo.VgAutofail, vgInfo.VgEnabled, vgInfo.VgVolumeList)
		}
	}

	teardownConnection(cs)
}

func main() {

	// Setup Subcommands
	listCommand := flag.NewFlagSet("list", flag.ExitOnError)
	offlineCommand := flag.NewFlagSet("offline", flag.ExitOnError)
	onlineCommand := flag.NewFlagSet("online", flag.ExitOnError)
	stopCommand := flag.NewFlagSet("stop", flag.ExitOnError)
	/*
		  TODO - add watch commands to watch for changes for
		  example a VG, etc
			watchCommand := flag.NewFlagSet("watch", flag.ExitOnError)
	*/

	// List subcommand flag pointers
	listNodePtr := listCommand.String("node", "", "node to list - list all if empty")
	listVgPtr := listCommand.String("vg", "", "volume group to list - list all if empty")

	// Offline subcommand flag pointers
	offlineVgPtr := offlineCommand.String("vg", "", "volume group to offline (required)")

	// Online subcommand flag pointers
	onlineNodePtr := onlineCommand.String("node", "", "node where to online VG (required)")
	onlineVgPtr := onlineCommand.String("vg", "", "volume group to online (required)")

	// Stop subcommand
	stopNodePtr := stopCommand.String("node", "", "node to be stopped (required)")

	// Verify that a subcommand has been provided
	// os.Arg[0] is the main command
	// os.Arg[1] will be the subcommand
	if len(os.Args) < 2 {
		fmt.Println("list, offline, online or stop subcommand is required")
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
	case "stop":
		stopCommand.Parse(os.Args[2:])
	default:
		// TODO - fix this error message
		fmt.Println("invalid subcommand - valid subcommands are list, offline, online or stop")
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

		if *offlineVgPtr == "" {
			offlineCommand.PrintDefaults()
			os.Exit(1)
		}

		cs := setupConnection()

		// Offline the VG
		err := cs.CLIOfflineVg(*offlineVgPtr)
		if err != nil {
			fmt.Printf("offline failed with error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Check which subcommand was Parsed for online
	if onlineCommand.Parsed() {

		fmt.Printf("onlineNodePtr is: %v onlineVgPtr: %v\n", *onlineNodePtr, *onlineVgPtr)
		// TODO - online the VG

		if (*onlineNodePtr == "") || (*onlineVgPtr == "") {
			onlineCommand.PrintDefaults()
			os.Exit(1)
		}

		cs := setupConnection()

		// Online the VG
		err := cs.CLIOnlineVg(*onlineVgPtr, *onlineNodePtr)
		if err != nil {
			fmt.Printf("online failed with error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Check which subcommand was Parsed for stop
	if stopCommand.Parsed() {

		if *stopNodePtr == "" {
			stopCommand.PrintDefaults()
			os.Exit(1)
		}

		cs := setupConnection()

		// Stop the node and wait for it to reach DEAD state
		// Offline all VGs on the node and stop the node
		if *stopNodePtr != "" {
			fmt.Printf("Node ptr: %v\n", *stopNodePtr)
			err := cs.CLIStopNode(*stopNodePtr)
			if err != nil {
				fmt.Printf("Stop failed with error: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
}
