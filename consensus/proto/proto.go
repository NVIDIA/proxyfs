package main

import (
	"fmt"
	"github.com/swiftstack/ProxyFS/consensus"
	"os"
	"time"
)

var (
	dummyBool    = false
	vgCreated    = false
	vgTestName1  = "myTestVg1"
	ipAddr1      = "192.168.60.20"
	vgTestName2  = "myTestVg2"
	ipAddr2      = "192.168.60.21"
	netMask      = "24"
	nic          = "enp0s8"
	autoFailover = true
	enabled      = true
)

func main() {
	endpoints := []string{"192.168.60.10:2379", "192.168.60.11:2379", "192.168.60.12:2379"}

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	hostName, _ := os.Hostname()
	cs, err := consensus.New(endpoints, hostName, 2*time.Second)
	if err != nil {
		fmt.Printf("Register() returned err: %v\n", err)
		os.Exit(-1)
	}

	// Become a Server in the cluster
	cs.Server()

	// Simulate proxyfsd being a long running daemon
	// by looping here.
	for {
		time.Sleep(5 * time.Second)

		// Create the VG if it does not exist but only do it
		// the first time.
		if !vgCreated {

			// TODO - Add a volume group.  If this fails it probably means
			// it already exists.
			// assume volumes are unique across VGs???
			_ = cs.AddVolumeGroup(vgTestName1, ipAddr1, netMask, nic, autoFailover, enabled)
			_ = cs.AddVolumeGroup(vgTestName2, ipAddr2, netMask, nic, autoFailover, enabled)

			// This code is never hit in this prototype.
			// Just eliminates a VS warning.
			if dummyBool {
				break
			}
		}
	}

	// NOTE: Currently this code is never hit
	//
	// Unregister from the etcd cluster
	cs.Close()
}
