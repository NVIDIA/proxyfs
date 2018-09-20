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

// NodeState represents the state of a node at a given point in time
type NodeState int

// NOTE: When updating NodeState be sure to also update String() below.
const (
	INITIALNS    NodeState = iota
	STARTINGNS             // STARTINGNS means node has just booted
	ONLINENS               // ONLINENS means the node is available to online VGs
	OFFLINENS              // OFFLINENS means the node gracefully shut down
	DEADNS                 // DEADNS means node appears to have left the cluster
	maxNodeState           // Must be last field!
)

func (state NodeState) String() string {
	return [...]string{"INITIAL", "STARTING", "ONLINE", "OFFLINE", "DEAD"}[state]
}

// NodePrefix returns a string containing the node prefix
func nodePrefix() string {
	return "NODE"
}

// NodeKeyStatePrefix returns a string containing the node state prefix
func nodeKeyStatePrefix() string {
	return nodePrefix() + "STATE:"
}

// NodeKeyHbPrefix returns a unique string for heartbeat key prefix
func nodeKeyHbPrefix() string {
	return nodePrefix() + "HB:"
}

func makeNodeStateKey(n string) string {
	return nodeKeyStatePrefix() + n
}

func makeNodeHbKey(n string) string {
	return nodeKeyHbPrefix() + n
}

// parseNodeResp is a helper method to break the GET of node data into
// the lists we need.
//
// NOTE: The resp given could have retrieved many objects including VGs
// so do not assume it only contains VGs.
func parseNodeResp(resp *clientv3.GetResponse) (nodesAlreadyDead []string,
	nodesOnline []string, nodesHb map[string]time.Time) {
	nodesAlreadyDead = make([]string, 0)
	nodesOnline = make([]string, 0)
	nodesHb = make(map[string]time.Time)
	for _, e := range resp.Kvs {
		if strings.HasPrefix(string(e.Key), nodeKeyStatePrefix()) {
			if string(e.Value) == DEADNS.String() {
				node := strings.TrimPrefix(string(e.Key), nodeKeyStatePrefix())
				nodesAlreadyDead = append(nodesAlreadyDead, node)
			} else {
				if string(e.Value) == ONLINENS.String() {
					node := strings.TrimPrefix(string(e.Key), nodeKeyStatePrefix())
					nodesOnline = append(nodesOnline, node)
				}
			}
		} else if strings.HasPrefix(string(e.Key), nodeKeyHbPrefix()) {
			node := strings.TrimPrefix(string(e.Key), nodeKeyHbPrefix())
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
// NOTE: We are updating the node states in multiple transactions.  I
// assume this is okay but want to revisit this.
func (cs *Struct) markNodesDead(nodesNewlyDead []string, nodesHb map[string]time.Time) {
	for _, n := range nodesNewlyDead {
		err := cs.setNodeStateIfSame(n, DEADNS, ONLINENS, nodesHb[n])

		// If this errors out it probably just means another node already
		// beat us to the punch.  However, during early development we want
		// to know about the error.
		if err != nil {
			fmt.Printf("Marking node: %v DEAD failed with err: %v\n", n, err)
			// TODO - Must remove node from nodesNewlyDead since other
			// routines will use this list to decide failover!!!
		}
	}

}

// getRevNodeState retrieves node state as of given revision
func (cs *Struct) getRevNodeState(revNeeded int64) (nodesAlreadyDead []string,
	nodesOnline []string, nodesHb map[string]time.Time) {

	// First grab all node state information in one operation
	resp, err := cs.cli.Get(context.TODO(), nodePrefix(), clientv3.WithPrefix(), clientv3.WithRev(revNeeded))
	if err != nil {
		fmt.Printf("GET node state failed with: %v\n", err)
		os.Exit(-1)
	}

	nodesAlreadyDead, nodesOnline, nodesHb = parseNodeResp(resp)
	return
}

// checkForDeadNodes() looks for nodes no longer
// heartbeating and sets their state to DEAD.
//
// It then initiates failover of any VGs.
func (cs *Struct) checkForDeadNodes() {
	// First grab all node state information in one operation
	resp, err := cs.cli.Get(context.TODO(), nodePrefix(), clientv3.WithPrefix())
	if err != nil {
		fmt.Printf("GET node state failed with: %v\n", err)
		os.Exit(-1)
	}

	// Break the response out into list of already DEAD nodes and
	// nodes which are still marked ONLINE.
	//
	// Also retrieve the last HB values for each node.
	_, nodesOnline, nodesHb := parseNodeResp(resp)

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

	// Initiate failover of VGs.
	cs.failoverVgs(nodesNewlyDead)
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
// if any nodes are DEAD and we should do a failover.
//
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

// We received a watch event for a node other than ourselves
//
// TODO - what about OFFLINE, etc events which are not implemented?
func (cs *Struct) otherNodeStateEvents(ev *clientv3.Event) {
	switch string(ev.Kv.Value) {
	case STARTINGNS.String():
		// TODO - strip out NODE from name
		fmt.Printf("Node: %v went: %v\n", string(ev.Kv.Key), string(ev.Kv.Value))
	case DEADNS.String():
		fmt.Printf("Node: %v went: %v\n", string(ev.Kv.Key), string(ev.Kv.Value))
		// TODO - figure out what VGs I should online if
		// any.... how prevent autofailback????
		nodesNewlyDead := make([]string, 1)
		nodesNewlyDead = append(nodesNewlyDead, string(ev.Kv.Key))
		cs.failoverVgs(nodesNewlyDead)
	case ONLINENS.String():
		fmt.Printf("Node: %v went: %v\n", string(ev.Kv.Key), string(ev.Kv.Value))
	}
}

// We received a watch event for the local node.
//
// TODO - hide watchers behind interface{}?
// TODO - what about OFFLINE, etc events which are not implemented?
func (cs *Struct) myNodeStateEvents(ev *clientv3.Event) {
	switch string(ev.Kv.Value) {
	case STARTINGNS.String():
		fmt.Printf("Received local - now STARTING\n")
		cs.setMyNodeState(cs.hostName, ONLINENS)
	case DEADNS.String():
		fmt.Printf("Received local - now DEAD\n")
		fmt.Printf("Exiting proxyfsd - after stopping VIP\n")
		cs.offlineVgs(true, ev.Kv.ModRevision)
		// TODO - Drop VIP here!!!
		os.Exit(-1)
	case ONLINENS.String():
		// TODO - implement ONLINE - how know to start VGs vs
		// avoid failback.  Probably only initiate online of
		// VGs which are not already started.....
		// TODO - should I pass the REVISION to the start*() functions?
		fmt.Printf("Received local - now ONLINE\n")
		cs.startHBandMonitor()
		cs.startVgs()
	case OFFLININGVS.String():
		// TODO - implement OFFLINING - txn(OFFLINEVS)
		// when done
		fmt.Printf("Received local - now OFFLINING\n")
		cs.offlineVgs(false, 0)
	}
}

// nodeStateWatchEvents creates a watcher based on node state
// changes.
func (cs *Struct) nodeStateWatchEvents(swg *sync.WaitGroup) {

	wch1 := cs.cli.Watch(context.Background(), nodeKeyStatePrefix(),
		clientv3.WithPrefix())

	swg.Done() // The watcher is running!
	for wresp1 := range wch1 {
		for _, ev := range wresp1.Events {
			if string(ev.Kv.Key) == makeNodeStateKey(cs.hostName) {
				cs.myNodeStateEvents(ev)
			} else {
				cs.otherNodeStateEvents(ev)
			}
		}

		// TODO - node watcher only shutdown when local node is OFFLINE
	}
}

// nodeHbWatchEvents creates a watcher based on node heartbeats.
func (cs *Struct) nodeHbWatchEvents(swg *sync.WaitGroup) {

	wch1 := cs.cli.Watch(context.Background(), nodeKeyHbPrefix(),
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
				Should this be where we do a liveliness check?
				*/
			}
		}

		// TODO - node watcher only shutdown when local node is OFFLINE
	}
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
func (cs *Struct) setNodeStateIfSame(nodeName string, newState NodeState, existingState NodeState,
	hb time.Time) (err error) {
	fmt.Printf("setNodeStateIfSame(%v, %v)\n", nodeName, newState.String())
	if (newState <= INITIALNS) || (newState >= maxNodeState) {
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
func (cs *Struct) setMyNodeState(nodeName string, newState NodeState) (err error) {
	fmt.Printf("setNodeState(%v, %v)\n", nodeName, newState.String())
	if (newState <= INITIALNS) || (newState >= maxNodeState) {
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
			clientv3.Compare(clientv3.Value(nodeStateKey), "=", INITIALNS.String()),

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
func (cs *Struct) setNodeStateForced(nodeName string, state NodeState) (err error) {
	fmt.Printf("setInitialNodeState(%v, %v)\n", nodeName, state.String())

	// TODO - probaly should verify that node state transitions
	// are correct.
	if (state <= INITIALNS) || (state >= maxNodeState) {
		err = errors.New("Invalid node state")
		return
	}

	nodeKey := makeNodeStateKey(nodeName)

	if state == STARTINGNS {
		// We will not get a watch event if the current state stored
		// in etcd is already STARTING.  (This could happen if lose power
		// to whole cluster while in STARTING state).
		//
		// Therefore, set state to INITIAL and then to STARTING to
		// guarantee we get a watch event.
		//
		// Regardless of existing state, reset it to INITIAL
		_ = cs.oneKeyTxn(nodeKey, INITIALNS.String(), INITIALNS.String(), INITIALNS.String(), 5*time.Second)
	}

	// Regardless of existing state - set to new state
	_ = cs.oneKeyTxn(nodeKey, INITIALNS.String(), state.String(), state.String(), 5*time.Second)

	return
}
