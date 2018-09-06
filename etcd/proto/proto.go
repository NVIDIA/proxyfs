package main

import (
	"fmt"
	"github.com/swiftstack/ProxyFS/consensus"
	"os"
	//"github.com/coreos/etcd/clientv3/namespace"
	//"sync"
	"time"
)

func main() {
	endpoints := []string{"192.168.60.10:2379", "192.168.60.11:2379", "192.168.60.12:2379"}
	hostName, _ := os.Hostname()

	fmt.Printf("LOCAL HOST: %v\n", hostName)

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs, err := consensus.Register(endpoints, 2*time.Second)
	if err != nil {
		fmt.Printf("Register() returned err: %v\n", err)
		os.Exit(-1)
	}

	// Start a watcher to watch for node state changes
	cs.StartAWatcher(consensus.NodeKeyStatePrefix())

	// Set state of local node to STARTING
	err = cs.SetNodeState(hostName, consensus.STARTING)
	if err != nil {
		// TODO - assume this means txn timed out, could not
		// get majority, etc....
		fmt.Printf("SetNodeState(STARTING) returned err: %v\n", err)
		os.Exit(-1)
	}

	// Watcher will start HB after getting STARTING state
	// Watcher also decides the state changes, etc....
	cs.WaitWatchers()

	// Unregister from the etcd cluster
	cs.Unregister()
}
