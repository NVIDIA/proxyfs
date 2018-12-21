package consensus

import (
	"bytes"
	_ "container/list"
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	mvccpb "go.etcd.io/etcd/mvcc/mvccpb"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// whether callUpDownScript() is being called for an "up" or a "down"
//
type upDownOperation int

const (
	// upDownScript is the location of the script to bring VG up or down
	// TODO - change location of this script
	upDownScript                         = "/vagrant/src/github.com/swiftstack/ProxyFS/consensus/proto/vg_up_down.sh"
	upDownUnitTestScript                 = "./vg_up_down.sh"
	upOperation          upDownOperation = iota
	downOperation
)

// allVgsDown is called to test if all VGs on the local node are OFFLINE.
//
func (cs *EtcdConn) allVgsDown(revNum RevisionNumber) (allDown bool, compares []clientv3.Cmp) {

	compares = make([]clientv3.Cmp, 0)

	// If any VG has the node set to the local node then return false
	// (OFFLINE VG do not have a node)
	allVgInfo, allVgInfoCmp, err := cs.getAllVgInfo(revNum)
	if err != nil {
		fmt.Printf("allVgsDown(): getAllVgInfo() revNum %d failed: %s\n", revNum, err)
		return
	}

	for vgName, vgInfo := range allVgInfo {
		if vgInfo.VgNode == cs.hostName {
			allDown = false
			return
		}
		compares = append(compares, allVgInfoCmp[vgName]...)
	}
	allDown = true

	return
}

// stateChgEvent handles any event notification for a VG state change (the
// KeyValue, ev.Kv, must be for a Volume Group).
func (cs *EtcdConn) stateChgEvent(ev *clientv3.Event) {

	// Something about a VG has changed ... try to figure out what it was
	// and how to react
	revNum := RevisionNumber(ev.Kv.ModRevision)
	vgName := strings.TrimPrefix(string(ev.Kv.Key), getVgKeyPrefix())

	if ev.Kv.CreateRevision == 0 {
		// if the vg was deleted then there's nothing to do
		fmt.Printf("stateChgEvent(): vg '%s' deleted\n", vgName)
		return
	}

	// unpackVgInfo operates on a slice of KeyValue
	keyValues := []*mvccpb.KeyValue{ev.Kv}
	vgInfos, vgInfoCmps, err := cs.unpackVgInfo(revNum, keyValues)
	if err != nil {
		fmt.Printf("stateChgEvent() unexpected error for VG %s from unpackVgInfo(%v): %s\n",
			ev.Kv.Key, ev.Kv, err)
		return
	}

	vgInfo := vgInfos[vgName]
	vgInfoCmp := vgInfoCmps[vgName]
	fmt.Printf("stateChgEvent(): vg '%s' vgInfo %v  node '%s' state '%v'\n", vgName, vgInfo, vgInfo.VgNode, vgInfo.VgState)

	var (
		conditionals = make([]clientv3.Cmp, 0, 1)
		operations   = make([]clientv3.Op, 0, 1)
	)
	conditionals = append(conditionals, vgInfoCmp...)

	switch vgInfo.VgState {

	case INITIALVS:
		// A new VG was created.  If this node is a server then set the
		// VG to onlining on this node (if multiple nodes are up this is
		// a race to see which node wins).
		//
		// TODO: make a placement decision
		if !cs.server {
			return
		}
		vgInfo.VgState = ONLININGVS
		vgInfo.VgNode = cs.hostName
		putOps, err := cs.putVgInfo(vgName, vgInfo)
		if err != nil {
			fmt.Printf("Hmmm. putVgInfo(%s, %v) failed: %s\n",
				vgName, vgInfo, err)
			return
		}

		operations = append(operations, putOps...)

	case ONLININGVS:
		// If VG onlining on local host then start the online
		if vgInfo.VgNode != cs.hostName {
			return
		}
		if !cs.server {
			fmt.Printf("ERROR: VG '%s' is onlining on node %s but %s is not a server\n",
				vgName, vgInfo.VgNode, cs.hostName)
			return
		}

		// Assume there is only one thread on this node that handles
		// events for this volume so there is only one caller to
		// callUpDownScript() at a time.
		//
		// However, this will be run each time there is a state change
		// for this VG (i.e. if any field changes).
		fmt.Printf("stateChgEvent() - vgName: %s - LOCAL - ONLINING\n", vgName)

		err = cs.callUpDownScript(upOperation, vgName, vgInfo.VgIpAddr, vgInfo.VgNetmask, vgInfo.VgNic)
		if err != nil {
			fmt.Printf("WARNING: UpDownScript UP for VG %s IPaddr %s netmask %s nic %s failed: %s\n",
				vgName, vgInfo.VgIpAddr, vgInfo.VgNetmask, vgInfo.VgNic, err)

			// old code would set to failed at this point
			// vgInfo.VgState = FAILEDVS
			// putOp := cs.putVgInfo(vgName, *vgInfo)
			return
		}
		vgInfo.VgState = ONLINEVS
		putOps, err := cs.putVgInfo(vgName, vgInfo)
		if err != nil {
			fmt.Printf("Hmmm. putVgInfo(%s, %v) failed: %s\n",
				vgName, vgInfo, err)
			return
		}
		operations = append(operations, putOps...)

	case ONLINEVS:
		// the VG is now online, so there's no work to do ...
		return

	case OFFLININGVS:
		// A VG is offlining.  If its on this node and we're the server, do something ...
		if vgInfo.VgNode != cs.hostName {
			return
		}
		if !cs.server {
			fmt.Printf("ERROR: VG '%s' is offlining on node %s but %s is not a server\n",
				vgName, vgInfo.VgNode, cs.hostName)
			return
		}
		fmt.Printf("stateChgEvent() - vgName: %s - LOCAL - OFFLINING\n", vgName)

		err = cs.callUpDownScript(downOperation, vgName, vgInfo.VgIpAddr, vgInfo.VgNetmask, vgInfo.VgNic)
		if err != nil {
			fmt.Printf("WARNING: UpDownScript Down for VG %s IPaddr %s netmask %s nic %s failed: %s\n",
				vgName, vgInfo.VgIpAddr, vgInfo.VgNetmask, vgInfo.VgNic, err)
			// old code would set to failed at this point
			// vgInfo.VgState = FAILEDVS
			// putOp := cs.putVgInfo(vgName, *vgInfo)
			return
		}

		vgInfo.VgState = OFFLINEVS
		vgInfo.VgNode = ""
		putOps, err := cs.putVgInfo(vgName, vgInfo)
		if err != nil {
			fmt.Printf("Hmmm. putVgInfo(%s, %v) failed: %s\n",
				vgName, vgInfo, err)
			return
		}

		operations = append(operations, putOps...)

	case OFFLINEVS:
		// We have finished offlining this VG.
		//
		// If we are in the CLI then signal that we are done offlining this VG.
		if !cs.server {
			// Wakeup blocked CLI if waiting for this VG
			if cs.offlineVg && vgName == cs.vgName {
				cs.cliWG.Done()
			}
			return
		}

		// If the local node is in OFFLINING and all VGs are OFFLINE
		// then transition to DEAD, otherwise there's nothing to do.
		if cs.getRevNodeState(revNum).NodesState[cs.hostName] != OFFLINING {
			// nothing else to do
			return
		}

		allDown, compares := cs.allVgsDown(revNum)
		if !allDown {
			return
		}
		conditionals = append(conditionals, compares...)

		// All VGs are down - now transition the node to DEAD.
		// TODO: this should be a transaction OP that checks conditionals
		cs.updateNodeState(cs.hostName, revNum, DEAD, nil)
		return

	case FAILEDVS:
		// the volume group is now failed; there's nothing else to do
		return

	default:
		fmt.Printf("stateChgEvent(): vg '%s' has unknown state '%v'\n",
			vgName, vgInfo.VgState)
	}

	// update the shared state (or fail)
	txnResp, err := cs.updateEtcd(conditionals, operations)

	// TODO: should we retry on failure?
	fmt.Printf("stateChgEvent(): txnResp: %v err %v\n", txnResp, err)
}

// vgWatchEvents creates a watcher based on volume group
// changes.
func (cs *EtcdConn) vgWatchEvents(swg *sync.WaitGroup) {

	wch1 := cs.cli.Watch(context.Background(), getVgKeyPrefix(), clientv3.WithPrefix())

	swg.Done() // The watcher is running!
	for wresp1 := range wch1 {
		for _, ev := range wresp1.Events {

			// Something about a VG has changed
			cs.stateChgEvent(ev)
		}

		// TODO - watcher only shutdown when local node is OFFLINE
	}
}

// setVgsOfflineDeadNodes finds all VGs ONLINE on the newly
// DEAD node and marks the VG as OFFLINE
func (cs *EtcdConn) setVgsOfflineDeadNodes(newlyDeadNodes []string, revNum RevisionNumber) {

	if !cs.server {
		return
	}

	// Retrieve VG and node state
	allVgInfo, allVgCmp, err := cs.getAllVgInfo(revNum)
	if err != nil {
		fmt.Printf("setVgsOfflineDeadNodes(): getAllVgInfo() revNum %d failed: %s\n", revNum, err)
		return
	}

	for _, deadNode := range newlyDeadNodes {
		for vgName, vgInfo := range allVgInfo {

			// If VG was ONLINE on dead node - set to OFFLINE
			if vgInfo.VgNode == deadNode {

				// VG state should be ONLINE, ONLINING, or OFFLINING
				// and transitiosn straing to OFFLINE
				vgInfo.VgState = OFFLINEVS
				vgInfo.VgNode = ""
				putOps, err := cs.putVgInfo(vgName, vgInfo)
				if err != nil {
					fmt.Printf("setVgsOfflineDeadNodes() putVgInfo for vg: %v err: %v\n",
						vgName, err)
				}

				// update the shared state (or fail)
				txnResp, err := cs.updateEtcd(allVgCmp[vgName], putOps)

				// TODO: should we retry on failure?
				fmt.Printf("setVgsOfflineDeadNodes(): vgName %s txnResp: %v err %v\n",
					vgName, txnResp, err)
			}
		}
	}
}

// failoverVgs is called when nodes have died.  The remaining nodes
// are scored and VGs from the failed nodes are started if possible.
//
// TODO - how avoid overloading a node? need weight for a VG?  what
// about priority for a VG and high priority first?
// Don't we have to use the same revision of ETCD for all these decisions?
// TODO - how prevent autofailback from happening? Does it matter?
//
func (cs *EtcdConn) failoverVgs(deadNodes []string, revNum RevisionNumber) {

	if !cs.server {
		return
	}

	// Mark all VGs that were online on newlyDeadNodes as OFFLINE
	cs.setVgsOfflineDeadNodes(deadNodes, revNum)

	// TODO - startVgs() should be triggered from the watcher since
	// the state will not be updated to OFFLINE until then
	// Do we need a VG state FAILING_OVER to denote this?
	cs.startVgs(revNum)
}

// setVgOfflining transitions the VG to OFFLINING.
//
// This is called either when a node has transitioned to OFFLINING or the CLI
// wants to offline a VG.
//
// The watcher will see the transition and initiate the offline activities.
//
func (cs *EtcdConn) setVgOfflining(vgName string) (err error) {

	fmt.Printf("setVgOfflining() - vg: %v\n", vgName)

	vgInfo, vgInfoCmp, err := cs.getVgInfo(vgName, RevisionNumber(0))
	if err != nil {
		err = fmt.Errorf("setVgOfflining(%s) failed to get VgInfo: %s", vgName, err)
		return
	}
	if vgInfo.VgState == OFFLINEVS {
		err = fmt.Errorf("setVgOfflining(): VG %s is already", vgName)
		return
	}
	if vgInfo.VgState == FAILEDVS {
		err = fmt.Errorf("setVgOfflining(): VG %s is FAILED", vgName)
		return
	}

	// TODO: ONLININGVS --> OFFLINING is not a valid transation;
	// need to wait for onlining to finish before forcing this.
	if vgInfo.VgState == ONLININGVS {
		fmt.Printf("setVgOfflining(): VG %s state is %s; offlining anyway (bug)\n", vgName, ONLININGVS)
	}

	// TODO: OFFLININGVS --> OFFLINEVS is not going to make progress if the
	// node to be offlined is down unless another node steps up to the plate.
	// Fix this later.

	// create the operation to update this this Volume Group to OFFLINING
	vgInfo.VgState = OFFLININGVS
	putOperations, err := cs.putVgInfo(vgName, vgInfo)

	// update the shared state (or fail)
	txnResp, err := cs.updateEtcd(vgInfoCmp, putOperations)

	// TODO: should we retry on failure?
	fmt.Printf("setVgOfflining(): txnResp: %v err %v\n", txnResp, err)

	return
}

// setVgOnlining sets the vg VGSTATE to ONLININGVS and the VGNODE to the node.
//
// This transaction can fail if the node is no longer in the INITIALVS state
// or the VG is no longer in the OFFLINEVS state.
//
func (cs *EtcdConn) setVgOnlining(vgName string, node string) (err error) {

	fmt.Printf("setVgOnlining() - vg: %v\n", vgName)

	vgInfo, vgInfoCmp, err := cs.getVgInfo(vgName, RevisionNumber(0))
	if err != nil || vgInfo == nil {
		err = fmt.Errorf("setVgOnlining(%s) failed to get VgInfo: %s", vgName, err)
		return
	}

	// TODO: ONLININGVS --> ONLINEVS is not going to make progress if the
	// node to be offlined is down unless another node steps up to the plate.
	// Fix this later.
	if vgInfo.VgState == ONLINEVS || vgInfo.VgState == ONLININGVS {
		err = fmt.Errorf("setVgOnlining(): VG %s is already %s", vgName, vgInfo.VgState)
		return
	}
	if vgInfo.VgState == FAILEDVS {
		err = fmt.Errorf("setVgOnlining(): VG %s is FAILED", vgName)
		return
	}

	// create the operation to update this this Volume Group to ONLINING
	vgInfo.VgState = ONLININGVS
	vgInfo.VgNode = node
	putOperations, err := cs.putVgInfo(vgName, vgInfo)

	// update the shared state (or fail)
	txnResp, err := cs.updateEtcd(vgInfoCmp, putOperations)

	// TODO: should we retry on failure?
	fmt.Printf("setVgOnlining(): txnResp: %s err %s\n", cs.formatTxnResp(txnResp), err)

	return
}

func (cs *EtcdConn) setOps(operation upDownOperation) (realOp string, script string) {
	switch cs.unitTest {
	case false:
		script = upDownScript
		if operation == upOperation {
			realOp = "up"
		} else {
			realOp = "down"
		}
	case true:
		script = cs.swd + "/" + upDownUnitTestScript
		if operation == upOperation {
			realOp = "mockup"
		} else {
			realOp = "mockdown"
		}
	}

	return
}

func (cs *EtcdConn) callUpDownScript(operation upDownOperation, vgName string, ipAddr string,
	netMask string, nic string) (err error) {

	realOp, script := cs.setOps(operation)

	cmd := exec.Command(script, realOp, vgName, ipAddr, netMask, nic)

	cmd.Stdin = strings.NewReader("some input")
	var stderr bytes.Buffer
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	fmt.Printf("callUpDownScript() - operation: %v name: %v ipaddr: %v nic: %v\n",
		realOp, vgName, ipAddr, nic)
	fmt.Printf("command: %s %s %s %s %s %s\n", script, realOp, vgName, ipAddr, netMask, nic)
	fmt.Printf("STDOUT: %s  STDERR: %s\n", stdout.String(), stderr.String())
	return
}

// doAllVgOfflineBeforeDead is called before the local node dies.  The only
// thing to do is to drop all the VIPs and kill smbd, etc so that we do
// not interfer with remaining nodes being able to take over.
func (cs *EtcdConn) doAllVgOfflineBeforeDead(deadRevNum RevisionNumber) {

	// Retrieve VG and node state
	allVgInfo, _, err := cs.getAllVgInfo(deadRevNum)
	if err != nil {
		fmt.Printf("doAllVgOfflineBefore(): aborrting; getAllVgInfo(%d) failed: %s\n",
			deadRevNum, err)
		return
	}

	// Drop the VIPs and daemons before we die
	for vgName, vgInfo := range allVgInfo {
		if vgInfo.VgNode == cs.hostName {
			_ = cs.callUpDownScript(downOperation, vgName, vgInfo.VgIpAddr, vgInfo.VgNetmask, vgInfo.VgNic)
		}
	}
}

// doAllVgOfflining is called when the local node has transitioned to
// OFFLININGVS.
//
// Initiate the offlining of VGs on this node.
func (cs *EtcdConn) doAllVgOfflining(revNum RevisionNumber) (numVgsOfflinint int) {

	// Retrieve VG state
	allVgInfo, _, err := cs.getAllVgInfo(revNum)
	if err != nil {
		fmt.Printf("doAllVgOfflining(): aborrting; getAllVgInfo(%d) failed: %s\n",
			revNum, err)
		return
	}

	/* DEBUG CODE
	vgState, vgNode, vgIpAddr, vgNetmask, vgNic, vgAutofail, vgEnabled,
		vgVolumelist, nodesAlreadyDead, nodesOnline, nodesHb, nodesState := cs.gatherInfo(revNum)

	fmt.Printf("doAllVgOfflining() ---- vgState: %v vgNode: %v vgIpAddr: %v vgNetmask: %v\n",
		vgState, vgNode, vgIpAddr, vgNetmask)
	fmt.Printf("vgNic: %v vgAutofail: %v vgEnabled: %v vgVolumelist: %v\n",
		vgNic, vgAutofail, vgEnabled, vgVolumelist)
	fmt.Printf("nodesAlreadyDead: %v nodesOnline: %v nodesHb: %v nodesState: %v\n",
		nodesAlreadyDead, nodesOnline, nodesHb, nodesState)
	*/

	// Set all VGs to OFFLINING.

	// TODO - Must also reject online of volume locally if local node is
	// OFFLINING - at least prevent ONLINING locally.
	for vgName, vgInfo := range allVgInfo {
		if vgInfo.VgNode == cs.hostName {
			numVgsOfflinint++
			cs.setVgOfflining(vgName)
		}
	}
	return
}

// clearMyVgs is called when the local node enters the
// STARTING state.
//
// The node must clear the VIPs of any VGs presently ONLINE or
// ONLINING on the local node and mark those VGs OFFLINE.  This
// can happen if the local node is the first node up after all
// proxyfsd processes were killed.
func (cs *EtcdConn) clearMyVgs(revNum RevisionNumber) {
	if !cs.server {
		return
	}

	// Retrieve VG state
	allVgInfo, _, err := cs.getAllVgInfo(revNum)
	if err != nil {
		fmt.Printf("clearMyVgs(): aborrting; getAllVgInfo(%d) failed: %s\n",
			revNum, err)
		return
	}

	for vgName, vgInfo := range allVgInfo {
		if vgName == cs.hostName {
			_ = cs.callUpDownScript(downOperation, vgName,
				vgInfo.VgIpAddr, vgInfo.VgNetmask, vgInfo.VgNic)
			cs.setVgOfflining(vgName)

			// TODO - this is a race and duplicate effort
			// When the VG goes offlining we will call the up down script
			// again.  Also, the node call immediately calls set node state
			// to ONLINE which is incorrect.
			// This code should block until see change that VG is offline before
			// returning...
		}
	}

}

// startVgs is called when a node has come ONLINE.
//
// TODO - implement algorithm to spread the VGs more evenly and
// in a predictable manner.
func (cs *EtcdConn) startVgs(revNum RevisionNumber) {

	if !cs.server {
		return
	}

	// Retrieve VG and node state
	allVgInfo, _, err := cs.getAllVgInfo(revNum)
	if err != nil {
		fmt.Printf("doAllVgOfflining(): aborrting; getAllVgInfo(%d) failed: %s\n",
			revNum, err)
		return
	}

	/* Debugging code
	fmt.Printf("startVgs() ---- vgState: %v vgNode: %v vgIpAddr: %v vgNetmask: %v\n",
		clusterInfo.AllVgInfo.VgState, clusterInfo.AllVgInfo.VgNode, clusterInfo.AllVgInfo.VgIpAddr,
		clusterInfo.AllVgInfo.VgNetmask)
	fmt.Printf("vgNic: %v vgAutofail: %v vgEnabled: %v vgVolumelist: %v\n",
		clusterInfo.AllVgInfo.VgNic, clusterInfo.AllVgInfo.VgAutofail, clusterInfo.AllVgInfo.VgEnabled,
	        clusterInfo.AllVgInfo.VgVolumelist)
	fmt.Printf("nodesAlreadyDead: %v nodesOnline: %v nodesHb: %v\n",
		clusterInfo.AllNodeInfo.NodesAlreadyDead, clusterInfo.AllNodeInfo.NodesOnline, clusterInfo.AllNodeInfo.NodesHb)
	*/

	// Find all VGs which are in the INITIAL or OFFLINE state, have
	// vgEnabled and vgAutofailover true, and (try to) start them on this
	// node.  There is an implicit race condition between nodes following
	// the same algorithm here.
	for vgName, vgInfo := range allVgInfo {
		if (vgInfo.VgState == OFFLINEVS || vgInfo.VgState == INITIALVS) &&
			vgInfo.VgEnabled && vgInfo.VgAutofail {

			// Set state to ONLINING to initiate the ONLINE
			// Set the VG to online.  If the txn fails then leave it on the list
			// for the next loop.
			//
			// TODO - could this be an infinite loop
			// if all nodes are offlining?
			// TODO - this is executed in parallel on all online nodes and could
			// fail.  We need to figure out if racing nodes or failure...

			// and what if this fails?
			_ = cs.setVgOnlining(vgName, cs.hostName)
		}

	}

	// TODO - figure out which is least loaded node and start spreading the load
	// around...
	// For the initial prototype we just do round robin which does not take into
	// consideration the load of an node.  Could be case that one node is already
	// overloaded.
}

// addVg adds a volume group
//
// TODO - should we add create time, failover time, etc?
//
func (cs *EtcdConn) addVg(name string, ipAddr string, netMask string,
	nic string, autoFailover bool, enabled bool) (err error) {

	var (
		conditionals = make([]clientv3.Cmp, 0, 1)
		operations   = make([]clientv3.Op, 0, 1)
	)

	vgInfo, cmpVgInfoName, err := cs.getVgInfo(name, RevisionNumber(0))
	if err != nil {
		return
	}
	if vgInfo != nil {
		err = fmt.Errorf("VG name '%s' already exists", name)
		return
	}
	conditionals = append(conditionals, cmpVgInfoName...)

	// the vg does not exist (yet), so initialize the VgInfoValue
	// information it will have when created (skip other VgInfo fields)
	vgInfo = &VgInfo{
		VgInfoValue: VgInfoValue{
			VgState:      INITIALVS,
			VgNode:       "",
			VgIpAddr:     ipAddr,
			VgNetmask:    netMask,
			VgNic:        nic,
			VgEnabled:    enabled,
			VgAutofail:   autoFailover,
			VgVolumeList: "",
		},
	}

	// create the operation to add this Volume Group
	putOps, err := cs.putVgInfo(name, vgInfo)
	operations = append(operations, putOps...)

	// update the shared state (or fail)
	//txnResp, err := cs.updateEtcd(conditionals, operations)
	_, err = cs.updateEtcd(conditionals, operations)

	// TODO: should we retry on failure?
	//fmt.Printf("addVg(): txnResp: %v err %v\n", txnResp, err)

	return
}

// rmVg removes a VG if possible
func (cs *EtcdConn) rmVg(name string) (err error) {
	var (
		conditionals = make([]clientv3.Cmp, 0, 1)
		operations   = make([]clientv3.Op, 0, 1)
	)

	vgInfo, cmpVgInfoName, err := cs.getVgInfo(name, RevisionNumber(0))
	if err != nil {
		return
	}
	if vgInfo == nil {
		err = fmt.Errorf("VG name '%s' does not exist", name)
		return
	}
	// should probably require that the VG be empty as well ...
	if vgInfo.VgState != OFFLINEVS && vgInfo.VgState != FAILEDVS {
		// how will controller handle this?
		err = fmt.Errorf("VG name '%s' must be offline to delete", name)
		return
	}
	conditionals = append(conditionals, cmpVgInfoName...)

	// create the operation to delete this Volume Group
	deleteOp, err := cs.deleteVgInfo(name)
	operations = append(operations, deleteOp)

	// update the shared state (or fail)
	//txnResp, err := cs.updateEtcd(conditionals, operations)
	_, err = cs.updateEtcd(conditionals, operations)

	// TODO: should we retry on failure?
	//fmt.Printf("rmVg(): txnResp: %v err %v\n", txnResp, err)

	return
}

// Mark a volume group as failed (no matter what its current state is, as long as
// it exists).
//
func (cs *EtcdConn) markVgFailed(name string) (err error) {

	for {
		var (
			conditionals = make([]clientv3.Cmp, 0, 1)
			operations   = make([]clientv3.Op, 0, 1)
		)

		vgInfo, cmpVgInfoName, err := cs.getVgInfo(name, RevisionNumber(0))
		if err != nil {
			return err
		}
		if vgInfo == nil {
			err = fmt.Errorf("VG name '%s' does not exist", name)
			return err
		}

		// Delete this --craig
		fmt.Printf("markVgFailed(): vg '%s' state '%v' vgInfo '%v'r\n", name, vgInfo.VgState, vgInfo)
		conditionals = append(conditionals, cmpVgInfoName...)

		// mark the VG dead
		vgInfo.VgState = FAILEDVS
		putOp, err := cs.putVgInfo(name, vgInfo)
		operations = append(operations, putOp...)

		// update the shared state (or fail)
		txnResp, err := cs.updateEtcd(conditionals, operations)

		if err != nil {
			fmt.Printf("markVgFailed(): vg '%s' updateEtcd error: %s\n", name, err)
			return err
		}

		if txnResp.Succeeded {
			return nil
		}

		fmt.Printf("markVgFailed(): vg '%s' transaction failed; retrying\n", name)
	}
	// notreached
}

// Unpack a slice of VgInfo's from the slice of KeyValue's received from an event,
// a Get request or a Range request.
//
// For each key a VgInfo is returned and a slice of comparison struct
// (clientv3.Cmp) that can be used as conditionals in a transaction.  The
// comparison will evaluate to false if the VG has changed from this
// information.
//
// Note: if VgInfo.CreateRevNum == 0 then the VG has been deleted and the
// VgValue part of VgInfo is zero values.
//
func (cs *EtcdConn) unpackVgInfo(revNum RevisionNumber, etcdKVs []*mvccpb.KeyValue) (vgInfos map[string]*VgInfo,
	vgInfoCmps map[string][]clientv3.Cmp, err error) {

	vgInfos = make(map[string]*VgInfo)
	vgInfoCmps = make(map[string][]clientv3.Cmp)

	keyHeaders, values, modCmps, err := mapKeyValues(revNum, etcdKVs)
	if err != nil {
		fmt.Printf("unpackVgInfo(): unpackKeyValues of '%v' returned err: %s\n", etcdKVs, err)
		return
	}
	for key, value := range values {

		vgName := strings.TrimPrefix(key, getVgKeyPrefix())
		vgInfoCmps[vgName] = []clientv3.Cmp{modCmps[key]}

		if keyHeaders[key].CreateRevNum != 0 {
			var vgInfoValue VgInfoValue

			err = json.Unmarshal(value, &vgInfoValue)
			if err != nil {
				fmt.Printf("unpackVgInfo(): Unmarshall of key '%s' header '%v' "+
					"value '%v' failed: %s\n",
					key, keyHeaders[key], string(values[key]), err)
				return
			}

			info := VgInfo{
				EtcdKeyHeader: *keyHeaders[key],
				VgInfoValue:   vgInfoValue,
			}
			vgInfos[vgName] = &info
		}

	}

	return
}

// Fetch the volume group information for volume group "name" as of revNum, or
// as of the "current" revision number if revNum is 0.
//
// If there's no error, return the vgInfo as well as a comparision function that
// can be used as a conditional in a transaction to insure the info hasn't
// changed.  Note that its not an error if the VG doesn't exist -- instead nil
// is returned for vgInfo and the comparison function can still be used as a
// conditional that it doesn't exist.
//
// Only one comparison function is returned, but we return it in a slice for
// convenience of the caller.
//
func (cs *EtcdConn) getVgInfo(vgName string, revNum RevisionNumber) (vgInfo *VgInfo,
	vgInfoCmp []clientv3.Cmp, err error) {

	// create a context for the request
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp *clientv3.GetResponse

	vgKey := makeVgKey(vgName)
	if revNum != 0 {
		resp, err = cs.cli.Get(ctx, vgKey, clientv3.WithRev(int64(revNum)))
	} else {
		resp, err = cs.cli.Get(ctx, vgKey)
	}
	if err != nil {
		return
	}

	if resp.OpResponse().Get().Count == int64(0) {
		vgInfo = nil
		cmp := clientv3.Compare(clientv3.CreateRevision(vgKey), "=", 0)
		vgInfoCmp = []clientv3.Cmp{cmp}
		return
	}

	vgInfos, vgInfoCmps, err := cs.unpackVgInfo(revNum, resp.OpResponse().Get().Kvs)
	if err != nil {
		fmt.Printf("getVgInfo(): unpackVgInfo of %v returned err: %s\n",
			resp.OpResponse().Get().Kvs, err)
		return
	}
	vgInfo = vgInfos[vgName]
	vgInfoCmp = vgInfoCmps[vgName]

	return
}

// Return a clientv3.Op "function" that can be added to a transaction's Then()
// clause to change the VgInfo for a VolumeGroup to the passed Value.
//
// Typically the transaction will will include a conditional (a clientv3.Cmp
// "function") returned by getVgInfo() for this same Volume Group to insure that
// the changes should be applied.
//
func (cs *EtcdConn) putVgInfo(vgName string, vgInfo *VgInfo) (operations []clientv3.Op, err error) {

	// extract the VgInfoValue fields without the rest of VgInfo
	vgInfoValue := vgInfo.VgInfoValue

	// vgInfoValueAsString, err := json.MarshalIndent(vgInfoValue, "", "  ")
	vgInfoValueAsString, err := json.Marshal(vgInfoValue)
	if err != nil {
		fmt.Printf("putVgInfo(): name '%s': json.MarshalIndent complained: %s\n",
			vgName, err)
		return
	}

	vgKey := makeVgKey(vgName)
	op := clientv3.OpPut(vgKey, string(vgInfoValueAsString))
	operations = []clientv3.Op{op}
	return
}

// Return a clientv3.Op "function" that can be added to a transaction's Then()
// clause to delete the Volume Group for the specified name.
//
// Typically the transaction will will include a conditional (a clientv3.Cmp
// "function") returned by getVgInfo() for this same Volume Group to insure that
// the changes should be applied.
//
func (cs *EtcdConn) deleteVgInfo(vgName string) (op clientv3.Op, err error) {

	vgKey := makeVgKey(vgName)
	op = clientv3.OpDelete(vgKey)
	return
}

// getAllVgInfo() gathers all VG information and returns it as a map from VG
// name to VgInfo
//
// All data is taken from the same etcd global revision number.
//
func (cs *EtcdConn) getAllVgInfo(revNum RevisionNumber) (allVgInfo map[string]*VgInfo,
	allVgInfoCmp map[string][]clientv3.Cmp, err error) {

	// First grab all VG state information in one operation
	var resp *clientv3.GetResponse
	if revNum != 0 {
		resp, err = cs.cli.Get(context.TODO(), getVgKeyPrefix(), clientv3.WithPrefix(),
			clientv3.WithRev(int64(revNum)))
	} else {
		resp, err = cs.cli.Get(context.TODO(), getVgKeyPrefix(), clientv3.WithPrefix())
	}
	if err != nil {
		fmt.Printf("GET VG state failed with: %v\n", err)
		os.Exit(-1)
	}

	allVgInfo, allVgInfoCmp, err = cs.unpackVgInfo(revNum, resp.Kvs)
	if err != nil {
		fmt.Printf("getAllVgInfo() unexpected error for from unpackVgInfo(%v): %s\n",
			resp.Kvs, err)
		return
	}

	return
}