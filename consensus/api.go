package consensus

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"os"
	"sync"
	"time"
)

// TODO - use etcd namepspace

// Struct is the connection to our consensus protocol
type Struct struct {
	cli       *clientv3.Client // etcd client pointer
	kvc       clientv3.KV
	hostName  string         // hostname of the local host
	watcherWG sync.WaitGroup // WaitGroup to keep track of watchers outstanding
	HBTicker  *time.Ticker   // HB ticker for sending HB
	// and processing DEAD nodes.
}

// Register with the consensus protocol.  In our case this is etcd.
func Register(endpoints []string, timeout time.Duration) (cs *Struct, err error) {
	// Look for our current node ID and print it
	hostName, err := os.Hostname()
	if err != nil {
		return
	}
	cs = &Struct{hostName: hostName}

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs.cli, err = clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: timeout})

	cs.kvc = clientv3.NewKV(cs.cli)

	// Verify that our hostName is one of the members of the cluster
	resp, err := cs.cli.MemberList(context.Background())
	if err != nil {
		return
	}

	var inCluster = false
	for _, v := range resp.Members {
		if v.Name == cs.hostName {
			inCluster = true
			break
		}
	}
	if !inCluster {
		err = errors.New("Hostname is not a member of the cluster")
		return
	}

	// Start a watcher to watch for node state changes
	cs.startAWatcher(nodeKeyStatePrefix())

	// Start a watcher to watch for node heartbeats
	cs.startAWatcher(nodeKeyHbPrefix())

	// Start a watcher to watch for volume group changes
	cs.startAWatcher(vgKeyStatePrefix())

	// Set state of local node to STARTING
	err = cs.setNodeStateForced(hostName, STARTINGNS)
	if err != nil {
		// TODO - assume this means txn timed out, could not
		// get majority, etc.... what do we do?
		fmt.Printf("setInitialNodeState(STARTING) returned err: %v\n", err)
		os.Exit(-1)
	}

	// Watcher will start HB after getting STARTING state
	// Watcher also decides the state changes, etc....
	cs.waitWatchers()

	return
}

// AddVolumeGroup creates a new volume group.
func (cs *Struct) AddVolumeGroup() (err error) {
	err = cs.addVg()
	return
}

// RmVolumeGroup removes a volume group if it is OFFLINE
func (cs *Struct) RmVolumeGroup() (err error) {
	err = cs.rmVg()
	return
}

// Unregister from the consensus protocol
func (cs *Struct) Unregister() {
	cs.cli.Close()
	cs.cli = nil
}
