package main

import (
	"fmt"
	"github.com/swiftstack/ProxyFS/consensus"
	"os"
	"time"
)

func main() {
	endpoints := []string{"192.168.60.10:2379", "192.168.60.11:2379", "192.168.60.12:2379"}

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs, err := consensus.Register(endpoints, 2*time.Second)
	if err != nil {
		fmt.Printf("Register() returned err: %v\n", err)
		os.Exit(-1)
	}

	// Unregister from the etcd cluster
	cs.Unregister()
}
