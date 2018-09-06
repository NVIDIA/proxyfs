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

// nodeStateWatchEvents creates a watcher based on node state
// changes.
func (cs *Struct) nodeStateWatchEvents(swg *sync.WaitGroup) {

	wch1 := cs.cli.Watch(context.Background(), NodeKeyStatePrefix(),
		clientv3.WithPrefix())
	swg.Done() // The watcher is running!
	for wresp1 := range wch1 {
		fmt.Printf("watcher() wresp1: %+v\n", wresp1)
		for _, ev := range wresp1.Events {
			fmt.Printf("Watcher for key: %v saw value: %v\n", string(ev.Kv.Key),
				string(ev.Kv.Value))
			if string(ev.Kv.Key) == makeNodeStateKey(cs.hostName) {
				fmt.Printf("Received own watch event for node\n")
				if string(ev.Kv.Value) == STARTING.String() {
					fmt.Printf("Received own watch event for node - now STARTING\n")
					cs.SetNodeState(cs.hostName, ONLINE)
					// TODO - start HB
					// TODO - detect if local node has been ejected from cluster
				}
			}
		}

		// TODO - node watcher only shutdown when local node is OFFLINE, then
		// decrement WaitGroup() and call cs.DONE()????
		// TODO - node watchers only shutdown when state is OFFLINING, then decrement WaitGroup()
		// ....
		// TODO - how notified when shutting down?
	}
}

// TODO - must wrap with WithRequiredLeader
// TODO - review how compaction works with watchers,
//
// watcher is a goroutine which watches for events with the key prefix.
// For example, all node events have a key as returned by NodeKeyPrefix().
func (cs *Struct) watcher(keyPrefix string, swg *sync.WaitGroup) {

	// TODO - probably switch based on keyPrefix for appropriate key
	// type!!!
	switch keyPrefix {
	case NodeKeyStatePrefix():
		cs.nodeStateWatchEvents(swg)
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

	// TODO ====>>>> figure out case where multiple states to update from...
	// STARTING, etc +++> STARTING....

	nodeKey := makeNodeStateKey(nodeName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(nodeKey), "!=", state.String()),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(nodeKey, state.String()),

	// the "Else" does not run
	).Else(
	// TODO - figure out what this should be
	//		clientv3.OpPut(nodeKey, state.String()),
	).Commit()
	cancel()

	return
}

// SetInitalNodeState ignores the existing node state value and
// sets it to the new state.   Generally, this is only needed for
// the STARTING and DEAD states.
// TODO - is there a better name for this???
func (cs *Struct) SetInitalNodeState(nodeName string, state NodeState) (err error) {
	fmt.Printf("SetInitialNodeState(%v, %v)\n", nodeName, state.String())
	if (state <= nilNodeState) || (state >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	nodeKey := makeNodeStateKey(nodeName)
	fmt.Printf("nodeKey: %v\n", nodeKey)

	if state == STARTING {
		// TODO - do I still need this or will I get an event regardless??
		// We will not get a watch event if the initial state is
		// already STARTING based on a previous run.  Therefore,
		// set state to "" and then to STARTING to guarantee we
		// get a watch event.
		_, err = cs.cli.Put(context.TODO(), nodeKey, "")
	}

	_, err = cs.cli.Put(context.TODO(), nodeKey, state.String())

	return
}
