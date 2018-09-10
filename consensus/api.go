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
	INITIAL      NodeState = iota
	STARTING               // STARTING means node has just booted
	ONLINE                 // ONLINE means the node is available to online VGs
	OFFLINE                // OFFLINE means the node gracefully shut down
	DEAD                   // DEAD means node appears to have left the cluster
	maxNodeState           // Must be last field!
)

func (state NodeState) String() string {
	return [...]string{"INITIAL", "STARTING", "ONLINE", "OFFLINE", "DEAD"}[state]
}

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

// startHBandMonitor() will start the HB timer to
// do txn(myNodeID, aliveTimeUTC) and will also look
// if any nodes are DEAD and we should do a failover
// TODO - also need stopHB function....
func (cs *Struct) startHBandMonitor() {
	// TODO - interval should be tunable
	cs.HBTicker = time.NewTicker(1 * time.Second)
	go func() {
		for range cs.HBTicker.C {
			fmt.Printf("Send a HB now\n")
		}
	}()
}

// TODO - decide whom should failover VGs
// in otherNodeEvents() when see went DEAD
func (cs *Struct) otherNodeEvents(ev *clientv3.Event) {
	fmt.Printf("Received own watch event for node\n")
	switch string(ev.Kv.Value) {
	case STARTING.String():
		// TODO - strip out NODE from name
		fmt.Printf("Node: %v went: %v\n", string(ev.Kv.Key), string(ev.Kv.Value))
	case DEAD.String():
		fmt.Printf("Node: %v went: %v\n", string(ev.Kv.Key), string(ev.Kv.Value))
		// TODO - figure out what VGs I should online if
		// any.... how prevent autofailback????
	case ONLINE.String():
		fmt.Printf("Node: %v went: %v\n", string(ev.Kv.Key), string(ev.Kv.Value))
	}
}

// TODO - move watchers to own file(s) and hide behind
// interface{}
func (cs *Struct) myNodeEvents(ev *clientv3.Event) {
	fmt.Printf("Received own watch event for node\n")
	switch string(ev.Kv.Value) {
	case STARTING.String():
		fmt.Printf("Received - now STARTING\n")
		cs.SetNodeState(cs.hostName, ONLINE)
	case DEAD.String():
		fmt.Printf("Received - now DEAD\n")
		fmt.Printf("Exiting proxyfsd\n")
		os.Exit(-1)
	case ONLINE.String():
		fmt.Printf("Received - now ONLINE\n")
		cs.startHBandMonitor()
	}
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
				cs.myNodeEvents(ev)
			} else {
				cs.otherNodeEvents(ev)
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

// oneKeyTxn is a helper function which creates a context and modifies the value of one key.
func (cs *Struct) oneKeyTxn(key string, ifValue string, thenValue string, elseValue string, timeout time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	_, err = cs.kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(key), "=", ifValue),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(key, thenValue),

	// the "Else" if "!="
	).Else(
		clientv3.OpPut(key, elseValue),
	).Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

// SetNodeState updates the state of the node in etcd using a transaction.
// TODO - this correct?  Probably should verify that we are doing a valid
// state transition and panic if not.
func (cs *Struct) SetNodeState(nodeName string, state NodeState) (err error) {
	fmt.Printf("SetNodeState(%v, %v)\n", nodeName, state.String())
	if (state <= INITIAL) || (state >= maxNodeState) {
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
	err = cs.oneKeyTxn(nodeKey, state.String(), state.String(), state.String(), 5*time.Second)
	return
}

// SetNodeStateForced ignores the existing node state value and
// sets it to the new state.   Generally, this is only needed for
// the STARTING and DEAD states.
func (cs *Struct) SetNodeStateForced(nodeName string, state NodeState) (err error) {
	fmt.Printf("SetInitialNodeState(%v, %v)\n", nodeName, state.String())
	if (state <= INITIAL) || (state >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	nodeKey := makeNodeStateKey(nodeName)
	fmt.Printf("nodeKey: %v\n", nodeKey)

	if state == STARTING {
		// We will not get a watch event if the current state stored
		// in etcd is already STARTING.  (This could happen if lose power
		// to whole cluster while in STARTING state).
		//
		// Therefore, set state to INITIAL and then to STARTING to
		// guarantee we get a watch event.
		//
		// Regardless of existing state, reset it to INITIAL
		_ = cs.oneKeyTxn(nodeKey, INITIAL.String(), INITIAL.String(), INITIAL.String(), 5*time.Second)
	}

	// Regardless of existing state - set to new state
	_ = cs.oneKeyTxn(nodeKey, INITIAL.String(), state.String(), state.String(), 5*time.Second)

	return
}
