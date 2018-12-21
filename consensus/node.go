package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	mvccpb "go.etcd.io/etcd/mvcc/mvccpb"
	"os"
	"strings"
	"sync"
	"time"
)

// getRevNodeState retrieves node state as of given revision
func (cs *EtcdConn) getRevNodeState(revNeeded RevisionNumber) (allNodeState AllNodeInfo) {

	allNodeState.NodesAlreadyDead = make([]string, 0)
	allNodeState.NodesOnline = make([]string, 0)
	allNodeState.NodesHb = make(map[string]time.Time)
	allNodeState.NodesState = make(map[string]NodeState)

	allNodeInfo, _, err := cs.getAllNodeInfo(revNeeded)
	if err != nil {
		msg := fmt.Sprintf("getAllNodeInfo(%d) failed with: %v\n", revNeeded, err)
		fmt.Printf(msg)
		panic(msg)
	}

	for nodeName, nodeInfo := range allNodeInfo {
		allNodeState.NodesHb[nodeName] = nodeInfo.NodeHeartBeat
		allNodeState.NodesState[nodeName] = nodeInfo.NodeState
		allNodeState.RevNum = nodeInfo.RevNum
	}
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

	allNodeInfo, allNodeInfoCmp, err := cs.getAllNodeInfo(RevisionNumber(0))
	if err != nil {
		fmt.Printf("checkForDeadNodes: getAllNodeInfo node state failed with: %v\n", err)
		os.Exit(-1)
	}

	// Go thru list of nodes that are online and verify HB is not past
	// interval.  Also construct a list of all nodes that are DEAD.
	deadNodes := make([]string, 0)
	timeNow := time.Now()
	var revNum RevisionNumber
	for nodeName, nodeInfo := range allNodeInfo {

		// strictly speaking this only needs to be done once
		revNum = nodeInfo.RevNum

		if nodeInfo.NodeState == DEAD {
			deadNodes = append(deadNodes, nodeName)
		}

		// HBs are only sent while the node is in ONLINE or OFFLINING
		if (nodeInfo.NodeState != ONLINE) && (nodeInfo.NodeState != OFFLINING) {
			continue
		}

		// see if the heartbeat is recent enough
		//
		// TODO - this should use heartbeat interval and number of missed heartbeats
		// Do we need to worry about clock skeq here?  another node could be well
		// in the past.
		timeSince := timeNow.Sub(nodeInfo.NodeHeartBeat)
		if timeSince <= etcdUpdateTimeout {
			continue
		}

		// it seems that we need to mark the node dead!  .. but only if
		// nothing has changed in NodeInfo (use the conditional).
		//
		// if the transaction fails we'll catch this node the next time,
		// assuming the heartBeat is still not updated.
		nodeInfo.NodeState = DEAD
		ops, err := cs.putNodeInfo(nodeName, nodeInfo)

		txnResp, err := cs.updateEtcd(allNodeInfoCmp[nodeName], ops)
		if err != nil {
			fmt.Printf("checkForDeadNodes(): updateEtcd of %s failed with err %v\n",
				nodeName, err)
			continue
		}
		if !txnResp.Succeeded {
			fmt.Printf("checkForDeadNodes(): updateEtcd of %s conditionals not satisfied\n",
				nodeName)
		} else {
			fmt.Printf("checkForDeadNodes(): node %s marked DEAD\n",
				nodeName)
			deadNodes = append(deadNodes, nodeName)
		}
	}

	// Initiate failover of VGs.
	cs.failoverVgs(deadNodes, revNum)
}

// Update the heartbeat for this node based on the state it was in at revNum.
//
// The request can fail for various reasons, including if the database entry has
// been changed (if revNum is non-zero).
//
func (cs *EtcdConn) updateNodeHeartbeat(revNum RevisionNumber) (err error) {

	nodeName := cs.hostName
	if !cs.server {
		err = fmt.Errorf("updateNodeHeartbeat(%d): called by a non-server for node %s",
			revNum, nodeName)
		fmt.Printf("%s\n", err)
		panic(err)
		// return
	}

	nodeInfo, nodeInfoCmp, err := cs.getNodeInfo(nodeName, revNum)
	if err != nil {
		err = fmt.Errorf("updateNodeHeartbeat(%d): getNodeInfo() failed: %s", revNum, nodeName)
		fmt.Printf("%s\n", err)
		return
	}

	if nodeInfo.NodeState != ONLINE && nodeInfo.NodeState != OFFLINING {
		err = fmt.Errorf("updateNodeHeartbeat(%d): heartbeat requested but state is %s",
			revNum, nodeInfo.NodeState)
		fmt.Printf("%s\n", err)
		return
	}
	nodeInfo.NodeHeartBeat = time.Now().UTC()

	ops, err := cs.putNodeInfo(nodeName, nodeInfo)
	if err != nil {
		err = fmt.Errorf("updateNodeHeartbeat(%s, %d): putNodeInfo() failed: %s", nodeName, revNum, err)
		fmt.Printf("%s\n", err)
		return
	}

	txnResp, err := cs.updateEtcd(nodeInfoCmp, ops)
	if err != nil {
		err = fmt.Errorf("updateNodeHeartbeat(%s, %d): updateEtcd() failed: %s", nodeName, revNum, err)
		fmt.Printf("%s\n", err)
		return
	}

	if !txnResp.Succeeded {
		err = fmt.Errorf("updateNodeHeartbeat(%s, %d): txn didn't succeed", nodeName, revNum)
		fmt.Printf("%s\n", err)
	} else {
		err = fmt.Errorf("updateNodeHeartbeat(%s, %d): txn succeeded!\n", nodeName, revNum)
		fmt.Printf("%s\n", err)
	}
	return
}

// Create a brand new node which has never existed before!
//
// It takes a node name and creates the node with a state of INITIAL.  The
// request fails if the node exists (or has existed in the past?).  There are
// numerous other reasons it can fail ...
//
func (cs *EtcdConn) createNode(nodeName string) (err error) {

	nodeInfo, conds, err := cs.getNodeInfo(nodeName, RevisionNumber(0))
	if err != nil {
		err = fmt.Errorf("createNode() name '%s' getNodeInfo() failed: %s", nodeName, err)
		fmt.Printf("%s\n", err)
		return
	}
	if nodeInfo != nil {
		err = fmt.Errorf("createNode() name '%s'; name already exists!", nodeName)
		fmt.Printf("%s\n", err)
		return
	}

	nodeInfo = &NodeInfo{
		NodeInfoValue: NodeInfoValue{
			NodeState:     INITIAL,
			NodeHeartBeat: time.Now().UTC(),
		},
	}
	ops, err := cs.putNodeInfo(nodeName, nodeInfo)
	if err != nil {
		err = fmt.Errorf("createNode(%s): putNodeInfo() conditionals '%v' failed: %s",
			nodeName, conds, err)
		fmt.Printf("%s\n", err)
		return
	}

	txnResp, err := cs.updateEtcd(conds, ops)
	if err != nil {
		err = fmt.Errorf("createNode(%s): updateEtcd() conditionnals '%v' ops '%v' failed: %s",
			nodeName, conds, ops, err)
		fmt.Printf("%s\n", err)
		return
	}

	if !txnResp.Succeeded {
		err = fmt.Errorf("createNode(%s): conditionals '%v' not satisfied", nodeName, conds)
		fmt.Printf("%s\n", err)
	}

	return
}

// Make the requested change to the state of a node.
//
// It takes a node name, the new state for the node and a slice of conditionals
// (which may be nil).  The request fails if the conditionals do not evaluate to
// true, if requested change is illegal, such as an invalid state transition or
// if the NodeInfo has been modified since revNum (if revNum is non-zero).
//
func (cs *EtcdConn) updateNodeState(nodeName string, revNum RevisionNumber,
	newState NodeState, conditionals []clientv3.Cmp) (err error) {

	nodeInfo, nodeInfoCmp, err := cs.getNodeInfo(nodeName, revNum)
	if err != nil {
		err = fmt.Errorf("updateNodeState(node %s, newState %s): getNodeInfo() failed: %s",
			nodeName, newState, err)
		fmt.Printf("%s\n", err)
		return
	}
	if nodeInfo == nil {
		err = fmt.Errorf("updateNodeState(node %s, newState %s): node '%s' not in etcd",
			nodeName, newState, nodeName)
		fmt.Printf("%s\n", err)
		return
	}

	if conditionals == nil {
		conditionals = make([]clientv3.Cmp, 0)
	}
	conditionals = append(conditionals, nodeInfoCmp...)

	// validate the state transition
	switch newState {
	case STARTING:
		if nodeInfo.NodeState != DEAD && nodeInfo.NodeState != INITIAL {
			err = fmt.Errorf("updateNodeState(node %s, newState %s): NodeState %s is incompatible",
				nodeName, newState, nodeInfo.NodeState)
			fmt.Printf("%s\n", err)
			return
		}
		if nodeName != cs.hostName {
			err = fmt.Errorf("updateNodeState(node %s, newState %s): a node must set itself to %s",
				nodeName, newState, newState)
			fmt.Printf("%s\n", err)
			panic(err)
			// return
		}
	case ONLINE:
		if nodeInfo.NodeState != STARTING {
			err = fmt.Errorf("updateNodeState(node %s, newState %s): NodeState %s is incompatible",
				nodeName, newState, nodeInfo.NodeState)
			fmt.Printf("%s\n", err)
			return
		}
		if nodeName != cs.hostName {
			err = fmt.Errorf("updateNodeState(node %s, newState %s): a node must set itself to %s",
				nodeName, newState, newState)
			fmt.Printf("%s\n", err)
			panic(err)
			// return
		}

	case OFFLINING:
		fmt.Printf("updateNodeState() node %s  nodeInfo %v\n", nodeName, nodeInfo)
		if nodeInfo.NodeState != ONLINE {
			err = fmt.Errorf("updateNodeState(node %s, newState %s): NodeState %s is incompatible",
				nodeName, newState, nodeInfo.NodeState)
			fmt.Printf("%s\n", err)
			return
		}

	case DEAD:
		// always OK to transition to DEAD

	default:
		// this includes INITIAL and NoChange
		err = fmt.Errorf("updateNodeState(node %s, newState %s): invalid newState %v",
			nodeName, newState, newState)
		fmt.Printf("%s\n", err)
		panic(err)
	}

	nodeInfo.NodeState = newState
	ops, err := cs.putNodeInfo(nodeName, nodeInfo)
	if err != nil {
		err = fmt.Errorf("updateNodeState(node %s, newState %s): putNodeInfo() failed: %s",
			nodeName, newState, err)
		fmt.Printf("%s\n", err)
		return
	}

	txnResp, err := cs.updateEtcd(conditionals, ops)
	if err != nil {
		err = fmt.Errorf("updateNodeState(node %s, newState %s): updateEtcd() failed: %s",
			nodeName, newState, err)
		fmt.Printf("%s\n", err)
		return
	}

	if !txnResp.Succeeded {
		err = fmt.Errorf("updateNodeState(node %s, newState %s): conditionals '%v' not satisfied",
			nodeName, newState, conditionals)
		fmt.Printf("%s\n", err)
	}

	return
}

// startHbAndMonitor() will start the HB timer to
// do txn(myNodeID, aliveTimeUTC) and will also look
// if any nodes are DEAD and we should do a failover.
//
// TODO - also need stopHB function....
//
func (cs *EtcdConn) startHbAndMonitor() {
	if !cs.server {
		return
	}

	// the heartbeat is about to start ...
	cs.stopHBWG.Add(1)

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

			cs.updateNodeHeartbeat(RevisionNumber(0))
			cs.checkForDeadNodes()
		}
	}()
}

// We received a watch event for a node other than ourselves
//
// The cs.Lock is currently held.
//
// TODO - what about OFFLINE, etc events which are not implemented?
//
func (cs *EtcdConn) otherNodeStateEvent(revNum RevisionNumber, nodeName string,
	newNodeInfo *NodeInfo, nodeInfoCmp []clientv3.Cmp) {

	fmt.Printf("otherNodeStateEvents(): nodeName '%s'  newNodeInfo %v\n", nodeName, newNodeInfo)

	nodeState := newNodeInfo.NodeState
	switch nodeState {

	case STARTING:
		// add the node to the node map
		cs.nodeMap[nodeName] = newNodeInfo
		fmt.Printf("Node: %v went: %v\n", nodeName, nodeState)

	case DEAD:
		fmt.Printf("Node: %v went: %v\n", nodeName, nodeState)

		nodesNewlyDead := make([]string, 1)
		nodesNewlyDead = append(nodesNewlyDead, nodeName)
		if cs.server {
			cs.failoverVgs(nodesNewlyDead, revNum)
		} else {

			// The CLI shutdown a remote node - now signal CLI
			// that complete.
			if cs.stopNode && (cs.nodeName == nodeName) {
				cs.cliWG.Done()
			}
		}

	case ONLINE:
		fmt.Printf("Node: %v went: %v\n", nodeName, nodeState)

	case OFFLINING:
		fmt.Printf("Node: %v went: %v\n", nodeName, nodeState)
	}

	// update node state and revision numbers
	cs.nodeMap[nodeName].EtcdKeyHeader = newNodeInfo.EtcdKeyHeader
	cs.nodeMap[nodeName].NodeState = nodeState
}

// We received a watch event for the local node.
//
// The cs.Lock is currently held.
//
func (cs *EtcdConn) myNodeStateEvent(revNum RevisionNumber, nodeName string,
	newNodeInfo *NodeInfo, nodeInfoCmp []clientv3.Cmp) {

	switch newNodeInfo.NodeState {

	case STARTING:
		// This node is probably not in the map, so add it
		cs.nodeMap[nodeName] = newNodeInfo

		// TODO - implement ONLINE - how know to start VGs vs
		// avoid failback.  Probably only initiate online of
		// VGs which are not already started.....
		//
		// TODO - should I pass the REVISION to the start*() functions?
		if cs.server {
			cs.clearMyVgs(revNum)

			cs.startHbAndMonitor()
			cs.startVgs(revNum)

			cs.updateNodeState(cs.hostName, revNum, ONLINE, nil)
		}

	case ONLINE:

	case OFFLINING:
		// Initiate offlining of VGs, when last VG goes
		// offline the watcher will transition the local node to
		// DEAD.
		if cs.server {
			numVgsOffline := cs.doAllVgOfflining(revNum)

			// If the node has no VGs to offline then transition
			// to DEAD.
			if numVgsOffline == 0 {
				cs.updateNodeState(cs.hostName, revNum, DEAD, nil)
			}
		}

	case DEAD:
		fmt.Printf("Preparing to exit - stopping VIP(s)\n")

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
			fmt.Printf("Exiting\n")
		} else {

			// We are in the CLI process.  The CLI blocks while waiting on
			// confirmation that the node has reached the DEAD state.
			if cs.stopNode && (cs.nodeName == cs.hostName) {
				cs.cliWG.Done()
			}
		}
	}

	// update the new state and revision numbers in the map
	cs.nodeMap[nodeName].EtcdKeyHeader = newNodeInfo.EtcdKeyHeader
	cs.nodeMap[nodeName].NodeState = newNodeInfo.NodeState
}

// Start the goroutine(s) that watch for, and react to, node events
//
func (cs *EtcdConn) startNodeWatcher(stopChan chan interface{}, doneWg *sync.WaitGroup) {

	// watch for changes to any key starting with the node prefix;
	wch1 := cs.cli.Watch(context.Background(), getNodeStateKeyPrefix(),
		clientv3.WithPrefix(), clientv3.WithPrevKV())

	go cs.StartWatcher(wch1, nodeWatchResponse, doneWg, stopChan, nil)
}

// Something about one or more nodes changed.  React appropriately.
//
func nodeWatchResponse(cs *EtcdConn, response *clientv3.WatchResponse) (err error) {

	revNum := RevisionNumber(response.Header.Revision)
	for _, ev := range response.Events {

		nodeInfos, nodeInfoCmps, err2 := cs.unpackNodeInfo(revNum, []*mvccpb.KeyValue{ev.Kv})
		if err2 != nil {
			err = err2
			fmt.Printf("nodeWatchResponse: failed to unpack NodeInfo event(s) for '%s' KV '%v'\n",
				string(ev.Kv.Key), ev.Kv)
			return
		}
		if len(nodeInfos) != 1 {
			fmt.Printf("WARNING: nodeWatchResponse: NodeInfo event for '%s' has %d entries values '%v'\n",
				string(ev.Kv.Key), len(nodeInfos), ev.Kv)
		}

		for nodeName, newNodeInfo := range nodeInfos {

			cs.Lock()
			nodeInfo, ok := cs.nodeMap[nodeName]
			if !ok || newNodeInfo.NodeState != nodeInfo.NodeState {

				// a node changed state
				if nodeName == cs.hostName {
					cs.myNodeStateEvent(revNum, nodeName, newNodeInfo, nodeInfoCmps[nodeName])
				} else {
					cs.otherNodeStateEvent(revNum, nodeName, newNodeInfo, nodeInfoCmps[nodeName])
				}
			}

			// if the node still exists update the heartbeat and
			// revision numbers (whether they changed or not)
			nodeInfo, ok = cs.nodeMap[nodeName]
			if ok {
				nodeInfo.EtcdKeyHeader = newNodeInfo.EtcdKeyHeader
				nodeInfo.NodeHeartBeat = newNodeInfo.NodeHeartBeat
			}
			cs.Unlock()
		}
	}
	return
}

// Unpack a slice of NodeInfo's from the slice of KeyValue's received from an event,
// a Get request or a Range request.
//
// For each key a NodeInfo is returned and a slice of comparison struct
// (clientv3.Cmp) that can be used as conditionals in a transaction.  The
// comparison will evaluate to false if the VG has changed from this
// information.
//
// Note: if NodeInfo.CreateRevNum == 0 then the VG has been deleted and the
// NodeValue part of NodeInfo is zero values.
//
func (cs *EtcdConn) unpackNodeInfo(revNum RevisionNumber, etcdKVs []*mvccpb.KeyValue) (nodeInfos map[string]*NodeInfo,
	nodeInfoCmps map[string][]clientv3.Cmp, err error) {

	nodeInfos = make(map[string]*NodeInfo)
	nodeInfoCmps = make(map[string][]clientv3.Cmp)

	keyHeaders, values, modCmps, err := mapKeyValues(revNum, etcdKVs)
	if err != nil {
		fmt.Printf("unpackNodeInfo(): mapKeyValues for '%v' returned err: %s\n", etcdKVs, err)
		return
	}
	for key, value := range values {

		var nodeInfoValue NodeInfoValue
		if keyHeaders[key].CreateRevNum != 0 {
			err = json.Unmarshal(value, &nodeInfoValue)
			if err != nil {
				fmt.Printf("unpackNodeInfo(): Unmarshall of key '%s' header '%v' "+
					"value '%v' failed: %s\n",
					key, keyHeaders[key], string(values[key]), err)
				return
			}
		}

		nodeName := strings.TrimPrefix(key, getNodeStateKeyPrefix())
		info := NodeInfo{
			EtcdKeyHeader: *keyHeaders[key],
			NodeInfoValue: nodeInfoValue,
		}
		nodeInfos[nodeName] = &info
		nodeInfoCmps[nodeName] = []clientv3.Cmp{modCmps[key]}
	}

	return
}

// Fetch the node information for node "name" as of revNum, or as of the
// "current" revision number if revNum is 0.
//
// If there's no error, return the NodeInfo as well as a comparision operation
// that can be used as a conditional in a transaction to insure the info hasn't
// changed.  Note that its not an error if the VG doesn't exist -- instead nil
// is returned for nodeInfo and the comparison function can still be used as a
// conditional that it doesn't exist.
//
// Only one comparison function is returned, but we return it in a slice for
// convenience of the caller.
//
func (cs *EtcdConn) getNodeInfo(nodeName string, revNum RevisionNumber) (nodeInfo *NodeInfo,
	nodeInfoCmp []clientv3.Cmp, err error) {

	// create a context for the request
	ctx, cancel := context.WithTimeout(context.Background(), etcdQueryTimeout)
	defer cancel()

	var resp *clientv3.GetResponse

	nodeKey := makeNodeStateKey(nodeName)
	if revNum != 0 {
		resp, err = cs.cli.Get(ctx, nodeKey, clientv3.WithRev(int64(revNum)))
	} else {
		resp, err = cs.cli.Get(ctx, nodeKey)
	}
	if err != nil {
		return
	}

	if resp.OpResponse().Get().Count == int64(0) {
		nodeInfo = nil
		cmp := clientv3.Compare(clientv3.CreateRevision(nodeKey), "=", 0)
		nodeInfoCmp = []clientv3.Cmp{cmp}
		return
	}

	nodeInfos, nodeInfoCmps, err := cs.unpackNodeInfo(revNum, resp.OpResponse().Get().Kvs)
	if err != nil {
		fmt.Printf("getNodeInfo(): unpackNodeInfo of %v returned err: %s\n",
			resp.OpResponse().Get().Kvs, err)
		return
	}
	nodeInfo = nodeInfos[nodeName]
	nodeInfoCmp = nodeInfoCmps[nodeName]

	return
}

// Return a clientv3.Op "function" that can be added to a transaction's Then()
// clause to change the NodeInfo for a VolumeGroup to the passed Value.
//
// Typically the transaction will will include a conditional (a clientv3.Cmp
// "function") returned by getNodeInfo() for this same Volume Group to insure that
// the changes should be applied.
//
func (cs *EtcdConn) putNodeInfo(nodeName string, nodeInfo *NodeInfo) (operations []clientv3.Op, err error) {

	// extract the NodeInfoValue fields without the rest of NodeInfo
	nodeInfoValue := nodeInfo.NodeInfoValue

	// nodeInfoValueAsString, err := json.MarshalIndent(nodeInfoValue, "", "  ")
	nodeInfoValueAsString, err := json.Marshal(nodeInfoValue)
	if err != nil {
		fmt.Printf("putNodeInfo(): name '%s': json.MarshalIndent complained: %s\n",
			nodeName, err)
		return
	}

	nodeKey := makeNodeStateKey(nodeName)
	op := clientv3.OpPut(nodeKey, string(nodeInfoValueAsString))
	operations = []clientv3.Op{op}
	return
}

// Return a clientv3.Op "function" that can be added to a transaction's Then()
// clause to delete the Volume Group for the specified name.
//
// Typically the transaction will will include a conditional (a clientv3.Cmp
// "function") returned by getNodeInfo() for this same Volume Group to insure that
// the changes should be applied.
//
func (cs *EtcdConn) deleteNodeInfo(nodeName string) (op clientv3.Op, err error) {

	nodeKey := makeNodeStateKey(nodeName)
	op = clientv3.OpDelete(nodeKey)
	return
}

// getAllNodeInfo() gathers information about all the nodes and returns it as a
// map from node name to NodeInfo
//
// All data is taken from the same etcd global revision number.
//
func (cs *EtcdConn) getAllNodeInfo(revNum RevisionNumber) (allNodeInfo map[string]*NodeInfo,
	allNodeInfoCmp map[string][]clientv3.Cmp, err error) {

	// create a context for the request
	ctx, cancel := context.WithTimeout(context.Background(), etcdQueryTimeout)
	defer cancel()

	// First grab all VG state information in one operation
	var resp *clientv3.GetResponse
	if revNum != 0 {
		resp, err = cs.cli.Get(ctx, getNodeStateKeyPrefix(), clientv3.WithPrefix(),
			clientv3.WithRev(int64(revNum)))
	} else {
		resp, err = cs.cli.Get(ctx, getNodeStateKeyPrefix(), clientv3.WithPrefix())
	}
	if err != nil {
		fmt.Printf("Get Node state failed with: %v\n", err)
		os.Exit(-1)
	}

	allNodeInfo, allNodeInfoCmp, err = cs.unpackNodeInfo(revNum, resp.Kvs)
	if err != nil {
		fmt.Printf("getAllNodeInfo() unexpected error for from unpackNodeInfo(%v): %s\n",
			resp.Kvs, err)
		return
	}

	return
}
