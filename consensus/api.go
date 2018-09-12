package consensus

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"os"
	"strings"
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

// NodePrefix returns a string containing the node prefix
func NodePrefix() string {
	return "NODE"
}

// NodeKeyStatePrefix returns a string containing the node state prefix
func NodeKeyStatePrefix() string {
	return NodePrefix() + "STATE"
}

// NodeKeyHbPrefix returns a unique string for heartbeat key prefix
func NodeKeyHbPrefix() string {
	return NodePrefix() + "HB"
}

func makeNodeStateKey(n string) string {
	return NodeKeyStatePrefix() + n
}

func makeNodeHbKey(n string) string {
	return NodeKeyHbPrefix() + n
}

// makeNodeLists is a helper method to break the GET of node data into
// the lists we need.
func makeNodeLists(resp *clientv3.GetResponse) (nodesAlreadyDead []string, nodesOnline []string, nodesHb map[string]time.Time) {
	nodesAlreadyDead = make([]string, 0)
	nodesOnline = make([]string, 0)
	nodesHb = make(map[string]time.Time)
	for _, e := range resp.Kvs {
		if strings.HasPrefix(string(e.Key), NodeKeyStatePrefix()) {
			if string(e.Value) == DEAD.String() {
				node := strings.TrimPrefix(string(e.Key), NodeKeyStatePrefix())
				nodesAlreadyDead = append(nodesAlreadyDead, node)
			} else {
				if string(e.Value) == ONLINE.String() {
					node := strings.TrimPrefix(string(e.Key), NodeKeyStatePrefix())
					nodesOnline = append(nodesOnline, node)
				}
			}
		} else if strings.HasPrefix(string(e.Key), NodeKeyHbPrefix()) {
			node := strings.TrimPrefix(string(e.Key), NodeKeyHbPrefix())
			var sentTime time.Time
			err := sentTime.UnmarshalText(e.Value)
			if err != nil {
				fmt.Printf("UnmarshalTest failed with err: %v", err)
				os.Exit(-1)
			}
			nodesHb[node] = sentTime
		}
	}
	return
}

// markNodesDead takes the nodesNewlyDead and sets their state
// to DEAD IFF they are still in state ONLINE && hb time has not
// changed.
//
// TODO - is this correct???
//
// NOTE: We are updating the node states in multiple transactions.  I
// assume this is okay but want to revisit this.
func (cs *Struct) markNodesDead(nodesNewlyDead []string, nodesHb map[string]time.Time) {
	for _, n := range nodesNewlyDead {
		err := cs.SetNodeStateIfSame(n, DEAD, ONLINE, nodesHb[n])

		// If this errors out it probably just means another node already
		// beat us to the punch.  However, during early development we want
		// to know about the error.
		if err != nil {
			fmt.Printf("Marking node: %v DEAD failed with err: %v\n", n, err)
		}
	}

}

// checkForDeadNodes() looks for nodes no longer
// heartbeating and sets their state to DEAD.
//
// It then initiates failover of any VGs.
func (cs *Struct) checkForDeadNodes() {
	// First grab all node state information in one operation
	resp, err := cs.cli.Get(context.TODO(), NodePrefix(), clientv3.WithPrefix())
	if err != nil {
		fmt.Printf("GET node state failed with: %v\n", err)
		os.Exit(-1)
	}

	// Break the response out into list of already DEAD nodes and
	// nodes which are still marked ONLINE.
	//
	// Also retrieve the last HB values for each node.
	_, nodesOnline, nodesHb := makeNodeLists(resp)

	// Go thru list of nodeNotDeadState and verify HB is not past
	// interval.  If so, put on list nodesNewlyDead and then
	// do txn to mark them DEAD all in one transaction.
	nodesNewlyDead := make([]string, 0)
	timeNow := time.Now()
	for _, n := range nodesOnline {
		// TODO - this should use heartbeat interval and number of missed heartbeats
		nodeTime := nodesHb[n].Add(5 * time.Second)
		if nodeTime.Before(timeNow) {
			nodesNewlyDead = append(nodesNewlyDead, n)
		}
	}

	if len(nodesNewlyDead) == 0 {
		return
	}
	fmt.Printf("nodesNewlyDead: %+v\n", nodesNewlyDead)

	// Set newly dead nodes to DEAD in a series of separate
	// transactions.
	cs.markNodesDead(nodesNewlyDead, nodesHb)

	// TODO - initiate failover of VGs.
}

// sendHB sends a heartbeat by doing a txn() to update
// the local node's last heartbeat.
func (cs *Struct) sendHb() {
	nodeKey := makeNodeHbKey(cs.hostName)
	currentTime, err := time.Now().UTC().MarshalText()
	if err != nil {
		fmt.Printf("time.Now() returned err: %v\n", err)
		os.Exit(-1)
	}

	// TODO - update timeout of txn() to be multiple of leader election
	// time and/or heartbeat time...
	err = cs.oneKeyTxn(nodeKey, string(currentTime), string(currentTime),
		string(currentTime), 5*time.Second)
	return
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
			cs.sendHb()
			cs.checkForDeadNodes()
		}
	}()
}

// TODO - decide whom should failover VGs
// in otherNodeEvents() when see went DEAD
func (cs *Struct) otherNodeStateEvents(ev *clientv3.Event) {
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
func (cs *Struct) myNodeStateEvents(ev *clientv3.Event) {
	fmt.Printf("Received own watch event for node\n")
	switch string(ev.Kv.Value) {
	case STARTING.String():
		fmt.Printf("Received - now STARTING\n")
		cs.SetMyNodeState(cs.hostName, ONLINE)
	case DEAD.String():
		fmt.Printf("Received - now DEAD\n")
		fmt.Printf("Exiting proxyfsd - after stopping VIP\n")
		// TODO - Drop VIP here!!!
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
				cs.myNodeStateEvents(ev)
			} else {
				cs.otherNodeStateEvents(ev)
			}
		}

		// TODO - node watcher only shutdown when local node is OFFLINE, then
		// decrement WaitGroup() and call cs.DONE()????
		// TODO - how notified when shutting down?
	}
}

// nodeHbWatchEvents creates a watcher based on node heartbeats.
// TODO - figure out if node is dead
func (cs *Struct) nodeHbWatchEvents(swg *sync.WaitGroup) {

	wch1 := cs.cli.Watch(context.Background(), NodeKeyHbPrefix(),
		clientv3.WithPrefix())

	swg.Done() // The watcher is running!
	for wresp1 := range wch1 {
		for _, e := range wresp1.Events {
			// Heartbeat is for the local node.
			if string(e.Kv.Key) == makeNodeHbKey(cs.hostName) {
				// TODO - need to do anything in this case?
			} else {
				// TODO - probably not needed....
				var sentTime time.Time
				err := sentTime.UnmarshalText(e.Kv.Value)
				if err != nil {
					fmt.Printf("UnmarshalTest failed with err: %v", err)
					os.Exit(-1)
				}

				/* TODO - TODO -
				Do we even do anything with heartbeats?  Do we only care
				about a timer thread looking for nodes which missed correct
				number of heartbeats? should we have a separate thread for
				checking if expired hb?  should we overload sending thread
				or is that a hack?
				*/
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
	case NodeKeyHbPrefix():
		cs.nodeHbWatchEvents(swg)
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

		// TODO - should only allow transaction if local node state is
		// !DEAD.  What about other node states like STARTED?
		// If local is DEAD won't we die?  How guarantee that?

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

// SetNodeStateIfSame will only change the state of the node if the
// node state and heartbeat time have not changed.
// TODO - consolidate with existing functions if possible
func (cs *Struct) SetNodeStateIfSame(nodeName string, newState NodeState, existingState NodeState,
	hb time.Time) (err error) {
	fmt.Printf("SetNodeStateIfSame(%v, %v)\n", nodeName, newState.String())
	if (newState <= INITIAL) || (newState >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	nodeStateKey := makeNodeStateKey(nodeName)
	nodeHbKey := makeNodeHbKey(nodeName)
	existingHbTime, err := hb.MarshalText()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// TODO - should only allow transaction if local node state is
		// !DEAD.  What about other node states like STARTED?
		// If local is DEAD won't we die?  How guarantee that?

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(nodeStateKey), "=", existingState.String()),
			clientv3.Compare(clientv3.Value(nodeHbKey), "=", string(existingHbTime)),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(nodeStateKey, newState.String()),

	// the "Else" if "!="
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

// SetMyNodeState updates the state of the node in etcd using a transaction.
// TODO - this correct?  Probably should verify that we are doing a valid
// state transition and panic if not.
// TODO - review for cleanup
func (cs *Struct) SetMyNodeState(nodeName string, newState NodeState) (err error) {
	fmt.Printf("SetNodeState(%v, %v)\n", nodeName, newState.String())
	if (newState <= INITIAL) || (newState >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	nodeStateKey := makeNodeStateKey(nodeName)
	nodeHbKey := makeNodeHbKey(nodeName)
	currentTime, err := time.Now().UTC().MarshalText()
	if err != nil {
		fmt.Printf("time.Now() returned err: %v\n", err)
		os.Exit(-1)
	}

	// Update heartbeat time to close hole where see ONLINE state before
	// get a new heartbeat value.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// txn value comparisons are lexical
		If(
			clientv3.Compare(clientv3.Value(nodeStateKey), "=", INITIAL.String()),

		// the "Then" runs, since "" = "" for both keys
		).Then(
		clientv3.OpPut(nodeStateKey, newState.String()),
		clientv3.OpPut(nodeHbKey, string(currentTime)),

	// Regardless of current state - set new time.
	).Else(
		clientv3.OpPut(nodeStateKey, newState.String()),
		clientv3.OpPut(nodeHbKey, string(currentTime)),
	).Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

// SetNodeStateForced ignores the existing node state value and
// sets it to the new state.   Generally, this is only needed for
// the STARTING state.
func (cs *Struct) SetNodeStateForced(nodeName string, state NodeState) (err error) {
	fmt.Printf("SetInitialNodeState(%v, %v)\n", nodeName, state.String())

	// TODO - probaly should verify that node state transitions
	// are correct.
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
