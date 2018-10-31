package consensus

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
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
	OFFLININGNS            // OFFLININGNS means the node gracefully shut down
	DEADNS                 // DEADNS means node appears to have left the cluster
	maxNodeState           // Must be last field!
)

func (state NodeState) String() string {
	return [...]string{"INITIAL", "STARTING", "ONLINE", "OFFLINING", "DEAD"}[state]
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
func parseNodeResp(resp *clientv3.GetResponse) (nodeInfo AllNodeInfo) {

	nodeInfo.NodesAlreadyDead = make([]string, 0)
	nodeInfo.NodesOnline = make([]string, 0)
	nodeInfo.NodesHb = make(map[string]time.Time)
	nodeInfo.NodesState = make(map[string]string)

	nodeInfo.RevNum = RevisionNumber(resp.Header.GetRevision())
	for _, e := range resp.Kvs {
		if strings.HasPrefix(string(e.Key), nodeKeyStatePrefix()) {
			node := strings.TrimPrefix(string(e.Key), nodeKeyStatePrefix())
			nodeInfo.NodesState[node] = string(e.Value)
			if string(e.Value) == DEADNS.String() {
				nodeInfo.NodesAlreadyDead = append(nodeInfo.NodesAlreadyDead, node)
			} else {
				if string(e.Value) == ONLINENS.String() {
					nodeInfo.NodesOnline = append(nodeInfo.NodesOnline, node)
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
			nodeInfo.NodesHb[node] = sentTime
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
func (cs *EtcdConn) markNodesDead(nodesNewlyDead []string, nodesHb map[string]time.Time) {
	if !cs.server {
		return
	}

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
func (cs *EtcdConn) getRevNodeState(revNeeded RevisionNumber) (nodeInfo AllNodeInfo) {

	// First grab all node state information in one operation
	resp, err := cs.cli.Get(context.TODO(), nodePrefix(), clientv3.WithPrefix(),
		clientv3.WithRev(int64(revNeeded)))
	if err != nil {
		fmt.Printf("GET node state failed with: %v\n", err)
		panic(fmt.Errorf("GET node state rev %d failed with: %v", revNeeded, err))
	}

	nodeInfo = parseNodeResp(resp)
	return
}

// checkForDeadNodes() looks for nodes no longer
// heartbeating and sets their state to DEAD.
//
// It then initiates failover of any VGs.
func (cs *EtcdConn) checkForDeadNodes() {
	if !cs.server {
		return
	}

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
	nodeInfo := parseNodeResp(resp)

	// Go thru list of nodeNotDeadState and verify HB is not past
	// interval.  If so, put on list nodesNewlyDead and then
	// do txn to mark them DEAD all in one transaction.
	nodesNewlyDead := make([]string, 0)
	timeNow := time.Now()
	for _, n := range nodeInfo.NodesOnline {

		// HBs are only sent while the node is in ONLINE or OFFLINING
		if (nodeInfo.NodesState[n] != ONLINENS.String()) &&
			(nodeInfo.NodesState[n] != OFFLININGNS.String()) {
			continue
		}

		// TODO - this should use heartbeat interval and number of missed heartbeats
		nodeTime := nodeInfo.NodesHb[n].Add(5 * time.Second)
		if nodeTime.Before(timeNow) {
			nodesNewlyDead = append(nodesNewlyDead, n)
		}
	}

	if len(nodesNewlyDead) == 0 {
		return
	}

	// Set newly dead nodes to DEAD in a series of separate
	// transactions.
	cs.markNodesDead(nodesNewlyDead, nodeInfo.NodesHb)

	// Initiate failover of VGs.
	cs.failoverVgs(nodesNewlyDead, nodeInfo.RevNum)
}

// sendHB sends a heartbeat by doing a txn() to update
// the local node's last heartbeat.
func (cs *EtcdConn) sendHb() {
	if !cs.server {
		return
	}

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
func (cs *EtcdConn) startHBandMonitor() {
	if !cs.server {
		return
	}

	// TODO - interval should be tunable
	cs.HBTicker = time.NewTicker(1 * time.Second)
	go func() {
		for range cs.HBTicker.C {

			var stopHB bool
			cs.Lock()
			stopHB = cs.stopHB
			cs.Unlock()

			if stopHB {
				// Shutting down - stop heartbeating
				cs.stopHBWG.Done()
				return
			}
			cs.sendHb()
			cs.checkForDeadNodes()
		}
	}()
}

// We received a watch event for a node other than ourselves
//
// TODO - what about OFFLINE, etc events which are not implemented?
func (cs *EtcdConn) otherNodeStateEvents(ev *clientv3.Event) {

	node := strings.TrimPrefix(string(ev.Kv.Key), nodeKeyStatePrefix())
	revNum := RevisionNumber(ev.Kv.ModRevision)

	switch string(ev.Kv.Value) {
	case STARTINGNS.String():
		// TODO - strip out NODE from name
		fmt.Printf("Node: %v went: %v\n", node, string(ev.Kv.Value))
	case DEADNS.String():
		fmt.Printf("Node: %v went: %v\n", node, string(ev.Kv.Value))

		nodesNewlyDead := make([]string, 1)
		nodesNewlyDead = append(nodesNewlyDead, string(ev.Kv.Key))
		if cs.server {
			cs.failoverVgs(nodesNewlyDead, revNum)
		} else {

			// The CLI shutdown a remote node - now signal CLI
			// that complete.
			if cs.stopNode && (cs.nodeName == node) {
				cs.cliWG.Done()
			}
		}

	case ONLINENS.String():
		fmt.Printf("Node: %v went: %v\n", node, string(ev.Kv.Value))
	case OFFLININGNS.String():
		fmt.Printf("Node: %v went: %v\n", node, string(ev.Kv.Value))
	}
}

// We received a watch event for the local node.
//
// TODO - hide watchers behind interface{}?
func (cs *EtcdConn) myNodeStateEvents(ev *clientv3.Event) {
	fmt.Printf("\nLocal Node - went: %v\n", string(ev.Kv.Value))
	revNum := RevisionNumber(ev.Kv.ModRevision)

	switch string(ev.Kv.Value) {
	case STARTINGNS.String():
		if cs.server {
			cs.clearMyVgs(revNum)
			cs.setNodeState(cs.hostName, ONLINENS)
		}
	case DEADNS.String():
		fmt.Printf("Exiting proxyfsd - after stopping VIP(s)\n")

		// There are several cases here:
		//
		// 1. "hacli stop" CLI process
		// 2. Server shutting down and called Close().  Want os.Exit(-1)
		// 3. Server shutting down from remote call of "hacli stop".
		//    Want os.Exit(-1).
		// 4. Unit test shutting server down
		if cs.server {
			cs.doAllVgOfflineBeforeDead(revNum)
			cs.stopHBWG.Add(1)

			cs.Lock()
			cs.stopHB = true
			cs.Unlock()

			// Wait HB goroutine to finish
			cs.stopHBWG.Wait()

			// Exit etcd - this will also cause the watchers to
			// exit.
			cs.cli.Close()

			if !cs.unitTest {
				os.Exit(-1)
			}
		} else {

			// We are in the CLI process.  The CLI blocks while waiting on
			// confirmation that the node has reached the DEAD state.
			if cs.stopNode && (cs.nodeName == cs.hostName) {
				cs.cliWG.Done()
			}
		}
	case ONLINENS.String():
		// TODO - implement ONLINE - how know to start VGs vs
		// avoid failback.  Probably only initiate online of
		// VGs which are not already started.....
		//
		// TODO - should I pass the REVISION to the start*() functions?
		if cs.server {
			cs.startHBandMonitor()
			cs.startVgs(revNum)
		}
	case OFFLININGNS.String():
		// Initiate offlining of VGs, when last VG goes
		// offline the watcher will transition the local node to
		// DEAD.
		if cs.server {
			numVgsOffline := cs.doAllVgOfflining(revNum)

			// If the node has no VGs to offline then transition
			// to DEAD.
			if numVgsOffline == 0 {
				cs.setNodeState(cs.hostName, DEADNS)
			}
		}
	}
}

// nodeStateWatchEvents creates a watcher based on node state
// changes.
func (cs *EtcdConn) nodeStateWatchEvents(swg *sync.WaitGroup) {

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
func (cs *EtcdConn) nodeHbWatchEvents(swg *sync.WaitGroup) {

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
