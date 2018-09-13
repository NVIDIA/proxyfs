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

	// TODO - add cs.SetInitalNodeState() or something
	// like that which ignores existing value using the
	// PUT semantics...
	// GET - first dump key/value...

	// Set state of local node to STARTING
	err = cs.setNodeStateForced(hostName, STARTING)
	if err != nil {
		// TODO - assume this means txn timed out, could not
		// get majority, etc....
		fmt.Printf("SetInitialNodeState(STARTING) returned err: %v\n", err)
		os.Exit(-1)
	}

	// Watcher will start HB after getting STARTING state
	// Watcher also decides the state changes, etc....
	cs.waitWatchers()

	return
}

// Unregister from the consensus protocol
func (cs *Struct) Unregister() {
	cs.cli.Close()
	cs.cli = nil
}

// TODO - must wrap with WithRequiredLeader
// TODO - review how compaction works with watchers,
//
// watcher is a goroutine which watches for events with the key prefix.
// For example, all node events have a key as returned by NodeKeyPrefix().
func (cs *Struct) watcher(keyPrefix string, swg *sync.WaitGroup) {

	switch keyPrefix {
	case nodeKeyStatePrefix():
		cs.nodeStateWatchEvents(swg)
	case nodeKeyHbPrefix():
		cs.nodeHbWatchEvents(swg)
	}
}

// StartAWatcher starts a goroutine to watch for changes
// to the given keys
func (cs *Struct) startAWatcher(prefixKey string) {
	// Keep track of how many watchers we have started so that we
	// can clean them up as needed.
	cs.watcherWG.Add(1)

	var startedWG sync.WaitGroup
	startedWG.Add(1)

	go cs.watcher(prefixKey, &startedWG)

	// Wait for watcher to start before returning
	startedWG.Wait()
}

// WaitWatchers waits for all watchers to return
func (cs *Struct) waitWatchers() {
	cs.watcherWG.Wait()
}
