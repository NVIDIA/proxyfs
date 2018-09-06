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

// NodeState represents the state of a node at a given point in time
type NodeState int

// NOTE: When updating NodeState be sure to also update String() below.
const (
	nilNodeState NodeState = iota
	STARTING               // STARTING means node has just booted
	ONLINE                 // ONLINE means the node is available to online VGs
	OFFLINE                // OFFLINE means the node gracefully shut down
	DEAD                   // DEAD means node appears to have left the cluster
	maxNodeState           // Must be last field!
)

func (state NodeState) String() string {
	return [...]string{"", "STARTING", "ONLINE", "OFFLINE", "DEAD"}[state]
}

// Struct is the connection to our consensus protocol
type Struct struct {
	cli       *clientv3.Client // etcd client pointer
	kvc       clientv3.KV
	hostName  string         // hostname of the local host
	watcherWG sync.WaitGroup // WaitGroup to keep track of watchers outstanding
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
	for _, v := range resp.Members {
		if v.Name == cs.hostName {
			return
		}
	}
	err = errors.New("Hostname is not a member of the cluster")
	return
}

// Unregister from the consensus protocol
func (cs *Struct) Unregister() {
	cs.cli.Close()
	cs.cli = nil
}

// NodeKeyStatePrefix returns a string containing the node prefix
func NodeKeyStatePrefix() string {
	return "NODE"
}

func makeNodeStateKey(n string) string {
	return NodeKeyStatePrefix() + n
}

// TODO - must wrap with WithRequiredLeader
// TODO - review how compaction works with watchers,
//
// watcher is a goroutine which watches for events with the key prefix.
// For example, all node events have a key as returned by NodeKeyPrefix().
func (cs *Struct) watcher(keyPrefix string, swg *sync.WaitGroup) {

	swg.Done() // The watcher is running!

	// TODO - probably switch based on keyPrefix for appropriate key
	// type!!!

	wch1 := cs.cli.Watch(context.Background(), keyPrefix, clientv3.WithPrefix())
	for wresp1 := range wch1 {
		fmt.Printf("watcher() wresp1: %v\n", wresp1)
		for _, ev := range wresp1.Events {
			// TODO - what key???
			fmt.Printf("Watcher for key: %v saw value: %v\n", ev.Kv.Key, string(ev.Kv.Value))
		}

		// TODO - node watcher only shutdown when local node is OFFLINE, then
		// decrement WaitGroup() and call cs.DONE()????
		// TODO - node watchers only shutdown when state is OFFLINING, then decrement WaitGroup()
		// ....
		// TODO - how notified when shutting down?
		cs.watcherWG.Done() // TODO - only do this when we are shutting down
		return
	}
}

// StartAWatcher starts a goroutine to watch for changes
// to the given keys
func (cs *Struct) StartAWatcher(prefixKey string) {
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
func (cs *Struct) WaitWatchers() {
	cs.watcherWG.Wait()
}

// SetNodeState updates the state of the node in etcd using a transaction.
func (cs *Struct) SetNodeState(nodeName string, state NodeState) (err error) {
	fmt.Printf("SetNodeState(%v, %v)\n", nodeName, state.String())
	if (state <= nilNodeState) || (state >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	// NOTE: "etcd does not ensure linearizability for watch operations. Users are
	// expected to verify the revision of watch responses to ensure correct ordering."
	// TODO - figure out what this means
	// TODO - review use of WithRequireLeader.  Seems important for a subcluster to
	//     recognize partitioned out of cluster.
	// TODO - figure out correct timeout value.  Pass as arg???

	nodeKey := makeNodeStateKey(nodeName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(nodeKey), "=", ""),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(nodeKey, state.String()),

	// the "Else" does not run
	).Else().Commit()
	cancel()

	return
}
