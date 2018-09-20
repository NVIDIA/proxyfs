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
	vgTestName   = "myTestVg"
	ipAddr       = "192.168.60.20"
	netMask      = "1.1.1.1"
	nic          = "enp0s8"
	autoFailover = true
	enabled      = true
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
			_ = cs.AddVolumeGroup(vgTestName, ipAddr, netMask, nic, autoFailover, enabled)

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
	cs.Unregister()
}
