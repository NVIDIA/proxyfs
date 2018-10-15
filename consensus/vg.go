package consensus

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// VgState represents the state of a volume group at a given point in time
type VgState int

// NOTE: When updating NodeState be sure to also update String() below.
const (
	INITIALVS   VgState = iota
	ONLININGVS          // ONLINING means VG is starting to come online on the node in the volume list
	ONLINEVS            // ONLINE means VG is online on the node in the volume list
	OFFLININGVS         // OFFLINING means the VG is gracefully going offline
	OFFLINEVS           // OFFLINE means the VG is offline and volume list is empty
	FAILEDVS            // FAILED means the VG failed on the node in the volume list
	maxVgState          // Must be last field!
)

func (state VgState) String() string {
	return [...]string{"INITIAL", "ONLINING", "ONLINE", "OFFLINING", "OFFLINE", "FAILED"}[state]
}

const (
	// upDownScript is the location of the script to bring VG up or down
	// TODO - change location of this script
	upDownScript = "/vagrant/src/github.com/swiftstack/ProxyFS/consensus/proto/vg_up_down.sh"
	up           = "up"
	down         = "down"
)

const (
	vgStr             = "VG"
	vgNameStr         = "NAME"
	vgStateStr        = "STATE"
	vgNodeStr         = "NODE"
	vgIpaddrStr       = "IPADDR"
	vgNetmaskStr      = "NETMASK"
	vgNicStr          = "NIC"
	vgEnabledStr      = "ENABLED"
	vgAutofailoverStr = "AUTOFAILOVER"
	vgVolumelistStr   = "VOLUMELIST"
)

// vgPrefix returns a string containing the vg prefix
// used for all VG keys.
func vgPrefix() string {
	return vgStr
}

// vgKeyPrefix returns a string containing the VG prefix
// with the the individual key string appended.
func vgKeyPrefix(v string) string {
	return vgPrefix() + v + ":"
}

func makeVgNameKey(n string) string {
	return vgKeyPrefix(vgNameStr) + n
}

func makeVgStateKey(n string) string {
	return vgKeyPrefix(vgStateStr) + n
}

func makeVgNodeKey(n string) string {
	return vgKeyPrefix(vgNodeStr) + n
}

func makeVgIpaddrKey(n string) string {
	return vgKeyPrefix(vgIpaddrStr) + n
}

func makeVgNetmaskKey(n string) string {
	return vgKeyPrefix(vgNetmaskStr) + n
}

func makeVgNicKey(n string) string {
	return vgKeyPrefix(vgNicStr) + n
}

func makeVgAutoFailoverKey(n string) string {
	return vgKeyPrefix(vgAutofailoverStr) + n
}

func makeVgEnabledKey(n string) string {
	return vgKeyPrefix(vgEnabledStr) + n
}

func makeVgVolumeListKey(n string) string {
	return vgKeyPrefix(vgVolumelistStr) + n
}

func (cs *Struct) getVgAttrs(name string, rev int64) (state string, node string,
	ipaddr string, netmask string, nic string) {

	// First grab all VG state information in one operation using the revision.
	resp, err := cs.cli.Get(context.TODO(), vgPrefix(), clientv3.WithPrefix(), clientv3.WithRev(rev))
	if err != nil {
		fmt.Printf("GET VG state failed with: %v\n", err)
		os.Exit(-1)
	}

	// Break the response out into lists.
	vgState, vgNode, vgIpaddr, vgNetmask, vgNic, _, _, _ := cs.parseVgResp(resp)

	// Find the attributes needed
	state = vgState[name]
	node = vgNode[name]
	ipaddr = vgIpaddr[name]
	netmask = vgNetmask[name]
	nic = vgNic[name]
	return
}

// doVgOnline attempts to online the VG locallly.  Once
// done it will do a txn() to set the state either ONLINE
// or FAILED
func (cs *Struct) doVgOnline(name string, rev int64) {

	// Retrieve the VG attributes
	_, _, ipAddr, netMask, nic := cs.getVgAttrs(name, rev)

	// TODO - how long to timeout?
	// Execute up/down script and record state
	err := callUpDownScript(up, name, ipAddr, netMask, nic)
	if err != nil {
		fmt.Printf("doVgOnline() returned err: %v\n", err)
		cs.setVgFailed(name)
	}
	cs.setVgOnline(name)
}

// localHostEvent gets called when an event for the key "VGNODE" changes
// and the value equals the local node.
//
// At present we do not do anything when we get a notification of a VGNODE
// change and the node is the local node.
func (cs *Struct) localHostEvent(ev *clientv3.Event) {
	// Only do something if we are running in server as opposed to client
	if cs.server {

	}

}

// allVgsDownAndNodeOfflining is called to test if the local
// node is OFFLINING and all VGs on the local node are OFFLINE.
func (cs *Struct) allVgsDownAndNodeOfflining(rev int64) bool {

	_, vgNode, _, _, _, _, _, _, _, _, _, nodesState := cs.gatherInfo(true, rev)

	if nodesState[cs.hostName] != OFFLININGNS.String() {
		return false
	}

	// If any VG has the node set to the local node then return false
	for _, v := range vgNode {
		if v == cs.hostName {
			return false
		}

	}

	return true
}

// stateChgEvent handles any event notification for a VG state change
func (cs *Struct) stateChgEvent(ev *clientv3.Event) {

	// The state of a VG has changed
	vgName := strings.TrimPrefix(string(ev.Kv.Key), vgKeyPrefix(vgStateStr))
	vgState := string(ev.Kv.Value)
	rev := ev.Kv.ModRevision

	// Find node affected
	vgNodeKey := makeVgNodeKey(vgName)
	resp, _ := cs.cli.Get(context.TODO(), vgNodeKey, clientv3.WithRev(rev))
	node := string(resp.Kvs[0].Value)

	fmt.Printf("vgState now: %v for vgName: %v node: %v\n", vgState, vgName, node)

	switch vgState {
	case INITIALVS.String():
		// A new VG was created.  Online it.
		//
		// TOOD - should we have a lighter weight version of
		// startVgs() that just onlines one VG?
		if cs.server {
			cs.startVgs(rev)
		}

	case ONLININGVS.String():
		// If VG onlining on local host then start the online
		if (node == cs.hostName) && cs.server {
			go cs.doVgOnline(vgName, ev.Kv.ModRevision)
		}

	case OFFLININGVS.String():
		// A VG is offlining.
		if (node == cs.hostName) && cs.server {
			fmt.Printf("stateChgEvent() - vgName: %v - LOCAL - OFFLINING\n", vgName)
			cs.doVgOffline(vgName, ev.Kv.ModRevision)
		}

	case OFFLINEVS.String():
		// We have finished offlining this VG.
		//
		// If the local node is in OFFLINING and all VGs are OFFLINE then
		// transition to DEAD.
		//
		// If we are in the CLI then signal that we are done offlining this VG.
		if cs.server {
			if cs.allVgsDownAndNodeOfflining(ev.Kv.ModRevision) {

				// All VGs are down - now transition the
				// node to DEAD.
				cs.setNodeState(cs.hostName, DEADNS)
			}

		} else {
			// TODO - should offlineVg() really be offlineVG()?

			// Wakeup blocked CLI if waiting for this VG
			if cs.offlineVg && (vgName == cs.vgName) {
				cs.cliWG.Done()
			}
		}
	}
}

// vgWatchEvents creates a watcher based on volume group
// changes.
func (cs *Struct) vgWatchEvents(swg *sync.WaitGroup) {

	wch1 := cs.cli.Watch(context.Background(), vgPrefix(),
		clientv3.WithPrefix())

	swg.Done() // The watcher is running!
	for wresp1 := range wch1 {
		for _, ev := range wresp1.Events {

			// The node for a VG has changed
			if strings.HasPrefix(string(ev.Kv.Key), vgKeyPrefix(vgNodeStr)) {

				// The local node has the change
				if string(ev.Kv.Value) == cs.hostName {
					// Saw a VG event for VGNODE and it is the local host.
					cs.localHostEvent(ev)
				}
			} else if strings.HasPrefix(string(ev.Kv.Key), vgKeyPrefix(vgStateStr)) {
				// The state of a VG has changed
				cs.stateChgEvent(ev)
			}

			// TODO - any other VG watch events we want?
		}

		// TODO - watcher only shutdown when local node is OFFLINE
	}
}

func (cs *Struct) parseVgResp(resp *clientv3.GetResponse) (vgState map[string]string,
	vgNode map[string]string, vgIpaddr map[string]string, vgNetmask map[string]string,
	vgNic map[string]string, vgAutofail map[string]bool, vgEnabled map[string]bool,
	vgVolumelist map[string]string) {

	vgState = make(map[string]string)
	vgNode = make(map[string]string)
	vgIpaddr = make(map[string]string)
	vgNetmask = make(map[string]string)
	vgNic = make(map[string]string)
	vgAutofail = make(map[string]bool)
	vgEnabled = make(map[string]bool)
	vgVolumelist = make(map[string]string)

	for _, e := range resp.Kvs {
		if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgStateStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgStateStr))
			vgState[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgNodeStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgNodeStr))
			vgNode[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgIpaddrStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgIpaddrStr))
			vgIpaddr[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgNetmaskStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgNetmaskStr))
			vgNetmask[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgNicStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgNicStr))
			vgNic[n] = string(e.Value)
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgEnabledStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgEnabledStr))
			vgEnabled[n], _ = strconv.ParseBool(string(e.Value))
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgAutofailoverStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgAutofailoverStr))
			vgAutofail[n], _ = strconv.ParseBool(string(e.Value))
		} else if strings.HasPrefix(string(e.Key), vgKeyPrefix(vgVolumelistStr)) {
			n := strings.TrimPrefix(string(e.Key), vgKeyPrefix(vgVolumelistStr))
			vgVolumelist[n] = string(e.Value)
		}
	}

	return
}

// gatherInfo() gathers all VG information and node information and
// returns it broken out into maps.
//
// All data is taken from the same etcd global revision number.
func (cs *Struct) gatherInfo(revProvided bool, rev int64) (vgState map[string]string, vgNode map[string]string,
	vgIpaddr map[string]string, vgNetmask map[string]string, vgNic map[string]string,
	vgAutofail map[string]bool, vgEnabled map[string]bool, vgVolumelist map[string]string,
	nodesAlreadyDead []string, nodesOnline []string, nodesHb map[string]time.Time,
	nodesState map[string]string) {

	var (
		err  error
		resp *clientv3.GetResponse
	)

	// First grab all VG state information in one operation
	if revProvided {
		resp, err = cs.cli.Get(context.TODO(), vgPrefix(), clientv3.WithPrefix(),
			clientv3.WithRev(rev))
	} else {
		resp, err = cs.cli.Get(context.TODO(), vgPrefix(), clientv3.WithPrefix())
	}
	if err != nil {
		fmt.Printf("GET VG state failed with: %v\n", err)
		os.Exit(-1)
	}

	// Break the response out into list of already DEAD nodes and
	// nodes which are still marked ONLINE.
	//
	// Also retrieve the last HB values for each node.
	vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail, vgEnabled,
		vgVolumelist = cs.parseVgResp(resp)
	respRev := resp.Header.GetRevision()

	// Get the node state as of the same revision number
	nodesAlreadyDead, nodesOnline, nodesHb, nodesState = cs.getRevNodeState(respRev)

	return
}

// setVgsOfflineDeadNodes finds all VGs ONLINE on the newly
// DEAD node and marks the VG as OFFLINE
func (cs *Struct) setVgsOfflineDeadNodes(newlyDeadNodes []string, rev int64) {

	if !cs.server {
		return
	}

	// Retrieve VG and node state
	_, vgNode, _, _, _, _, _, _, _, _, _, _ := cs.gatherInfo(true, rev)

	for _, deadNode := range newlyDeadNodes {
		for name, node := range vgNode {

			// If VG was ONLINE on dead node - set to OFFLINE
			if node == deadNode {
				err := cs.setVgOffline(name)
				if err != nil {
					fmt.Printf("setVgsOfflineDeadNodes() failed for vg: %v err: %v\n",
						name, err)
				}
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
func (cs *Struct) failoverVgs(newlyDeadNodes []string, rev int64) {

	if !cs.server {
		return
	}

	// Mark all VGs that were online on newlyDeadNodes as OFFLINE
	cs.setVgsOfflineDeadNodes(newlyDeadNodes, rev)

	// TODO - startVgs() should be triggered from the watcher since
	// the state will not be updated to OFFLINE until then
	// Do we need a VG state FAILING_OVER to denote this?
	cs.startVgs(rev)
}

// parseVgOnlineInit returns a map of all VGs in either the
// ONLINE or INITIAL states
//
// This routine only adds the VG to the map if "autofailover==true" and
// "enabled=true"
func parseVgOfflineInit(vgState map[string]string, vgEnabled map[string]bool,
	vgAutofailover map[string]bool) (vgOfflineInit *list.List) {

	vgOfflineInit = list.New()
	for k, v := range vgState {
		if (vgEnabled[k] == false) || (vgAutofailover[k] == false) {
			continue
		}
		switch v {
		case INITIALVS.String():
			vgOfflineInit.PushBack(k)
		case OFFLINEVS.String():
			vgOfflineInit.PushBack(k)
		}

	}
	return
}

// setVgFailed sets the vg VGSTATE to FAILINGVS and leaves VGNODE as the
// node where it failed.
func (cs *Struct) setVgFailed(vg string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// Verify that the VG is still in ONLINING state
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", ONLININGVS.String()),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), FAILEDVS.String()),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// TODO - how handle error cases????

	return
}

// setVgOffline sets the vg VGSTATE to OFFLINEVS and clears VGNODE.
func (cs *Struct) setVgOffline(vg string) (err error) {
	var txnResp *clientv3.TxnResponse

	fmt.Printf("setVgOffline() - vg: %v\n", vg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is still in OFFLINING state
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", OFFLININGVS.String()),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), OFFLINEVS.String()),
		clientv3.OpPut(makeVgNodeKey(vg), ""),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	if txnResp.Succeeded {
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is still in ONLINE state - this can happen
		// if the node is DEAD.   We need to clear the VG state.
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", ONLINEVS.String()),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), OFFLINEVS.String()),
		clientv3.OpPut(makeVgNodeKey(vg), ""),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

// setVgOfflining transitions the VG to OFFLINING.
//
// This is called either when a node has transitioned to OFFLINING
// or the CLI wants to offline a VG.
//
// The watcher will see the transition and initiate the offline
// activities.
func (cs *Struct) setVgOfflining(vg string) (err error) {
	var txnResp *clientv3.TxnResponse

	fmt.Printf("setVgOfflining() - vg: %v\n", vg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is still in ONLINE state
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", ONLINEVS.String()),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), OFFLININGVS.String()),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// If the VG was in ONLINEVS and the txn succeeded then return now
	//
	// TODO - review all txn() code - should I be checking this at other
	// places? Probably missing some locations...
	if txnResp.Succeeded {
		return
	}

	// Earlier transaction failed - do next transaction assuming that state
	// was ONLINING
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is in ONLININGVS and node name is ""
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", ONLININGVS.String()),

		// "Then" set the values...
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), OFFLININGVS.String()),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// If the VG was in FAILEDVS and the txn succeeded then return now
	if txnResp.Succeeded {
		return
	}

	// Earlier transaction failed - do next transaction assuming that state
	// was FAILED
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is in FAILEDVS and node name is ""
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", FAILEDVS.String()),

		// "Then" set the values...
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), OFFLININGVS.String()),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	if !txnResp.Succeeded {
		err = errors.New("VG no longer in ONLINE, ONLINING or FAILED")
	}

	// TODO - how handle error cases????

	return
}

// setVgOnline sets the vg VGSTATE to ONLINEVS and leaves VGNODE as the
// node where it is online.
func (cs *Struct) setVgOnline(vg string) (err error) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// Verify that the VG is still ONLINING
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", ONLININGVS.String()),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), ONLINEVS.String()),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// TODO - how handle error cases????

	return
}

// setVgOnlining sets the vg VGSTATE to ONLININGVS and the VGNODE to the node.
//
// This transaction can fail if the node is no longer in the INITIALVS state
// or the VG is no longer in the OFFLINEVS state.
func (cs *Struct) setVgOnlining(vg string, node string) (err error) {
	var txnResp *clientv3.TxnResponse

	// Assuming that current state is INITIALVS - transition to ONLINING
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is in INITIALVS and node name is ""
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", INITIALVS.String()),
			clientv3.Compare(clientv3.Value(makeVgNodeKey(vg)), "=", ""),

		// "Then" set the values...
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), ONLININGVS.String()),
		clientv3.OpPut(makeVgNodeKey(vg), node),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	// If the VG was in INITIALVS and the txn succeeded then return now
	// TODO - review all txn() code - should I be checking this at other
	// places? Probably missing some locations...
	if txnResp.Succeeded {
		return
	}

	// Earlier transaction failed - do next transaction assuming that state
	// was OFFLINE
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	txnResp, err = cs.kvc.Txn(ctx).

		// Verify that the VG is in OFFLINEVS and node name is ""
		If(
			clientv3.Compare(clientv3.Value(makeVgStateKey(vg)), "=", OFFLINEVS.String()),
			clientv3.Compare(clientv3.Value(makeVgNodeKey(vg)), "=", ""),

		// "Then" set the values...
		).Then(
		clientv3.OpPut(makeVgStateKey(vg), ONLININGVS.String()),
		clientv3.OpPut(makeVgNodeKey(vg), node),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	if !txnResp.Succeeded {
		err = errors.New("VG no longer in INITIALVS or OFFLINEVS - possibly in ONLINING?")
		return
	}

	return
}

func callUpDownScript(operation string, vgName string, ipAddr string,
	netMask string, nic string) (err error) {

	cmd := exec.Command(upDownScript, operation, vgName, ipAddr, netMask, nic)
	cmd.Stdin = strings.NewReader("some input")
	var stderr bytes.Buffer
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err = cmd.Run()
	fmt.Printf("callUpDownScript() - operation: %v name: %v ipaddr: %v nic: %v OUT: %v STDERR: %v\n",
		operation, vgName, ipAddr, nic, out.String(), stderr.String())

	return
}

// getRevVgAttrs returns all VG attributes at the given etcd revision.
func (cs *Struct) getRevVgAttrs(rev int64) (vgState map[string]string,
	vgNode map[string]string, vgIpaddr map[string]string,
	vgNetmask map[string]string, vgNic map[string]string) {

	// First grab all VG state information at the given revision
	resp, err := cs.cli.Get(context.TODO(), vgPrefix(), clientv3.WithPrefix(),
		clientv3.WithRev(rev))
	if err != nil {
		fmt.Printf("GET VG state failed with: %v\n", err)
		os.Exit(-1)
	}

	// Break the response out into list of already DEAD nodes and
	// nodes which are still marked ONLINE.
	//
	// Also retrieve the last HB values for each node.
	vgState, vgNode, vgIpaddr, vgNetmask, vgNic, _, _, _ = cs.parseVgResp(resp)

	return
}

// doVgOffline offlines a volume group and then does a txn to
// set the state to OFFLINE.
//
// The actual offline is done in the background.
func (cs *Struct) doVgOffline(name string, rev int64) {

	_, _, vgIpaddr, vgNetmask, vgNic := cs.getRevVgAttrs(rev)

	// TODO - call this goroutine for each volume in the VG.  Need WG
	// and to handle case where volume added/removed while VG is offlining
	// and onlining.
	go func() {
		err := callUpDownScript(down, name, vgIpaddr[name], vgNetmask[name], vgNic[name])
		if err == nil {

			// TODO - unmount the volume from FS layer.

			// Do txn() to mark the volume offline.
			err = cs.setVgOffline(name)
			if err != nil {
				fmt.Printf("offlineVg(%v) setVgOffline() returned err: %v\n", name, err)
			}
		} else {
			fmt.Printf("offlineVg(%v) callUpDownScript() returned err: %v\n", name, err)
		}
	}()
}

// doAllVgOfflineBeforeDead is called before the local node dies.  The only
// thing to do is to drop all the VIPs and kill smbd, etc so that we do
// not interfer with remaining nodes being able to take over.
func (cs *Struct) doAllVgOfflineBeforeDead(deadRev int64) {

	// Retrieve VG and node state
	_, vgNode, vgIpaddr, vgNetmask, vgNic, _, _, _, _, _, _, _ := cs.gatherInfo(true, deadRev)

	// Drop the VIPs and daemons before we die
	for name, node := range vgNode {
		if node == cs.hostName {
			_ = callUpDownScript(down, name, vgIpaddr[name], vgNetmask[name],
				vgNic[name])
		}
	}
}

// doAllVgOfflining is called when the local node has transitioned to
// OFFLINING.
//
// Initiate the offlining of VGs on this node.
func (cs *Struct) doAllVgOfflining(rev int64) (numVgsOfflinint int) {

	// Retrieve VG and node state
	_, vgNode, _, _, _, _, _, _, _, _, _, _ := cs.gatherInfo(true, rev)

	/* DEBUG CODE
	vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail, vgEnabled,
		vgVolumelist, nodesAlreadyDead, nodesOnline, nodesHb, nodesState := cs.gatherInfo()

	fmt.Printf("doAllVgOfflining() ---- vgState: %v vgNode: %v vgIpaddr: %v vgNetmask: %v\n",
		vgState, vgNode, vgIpaddr, vgNetmask)
	fmt.Printf("vgNic: %v vgAutofail: %v vgEnabled: %v vgVolumelist: %v\n",
		vgNic, vgAutofail, vgEnabled, vgVolumelist)
	fmt.Printf("nodesAlreadyDead: %v nodesOnline: %v nodesHb: %v nodesState: %v\n",
		nodesAlreadyDead, nodesOnline, nodesHb, nodesState)
	*/

	// Set all VGs to OFFLINING.

	// TODO - Must also reject online of volume locally if local node is
	// OFFLINING - at least prevent ONLINING locally.
	for name, node := range vgNode {
		if node == cs.hostName {
			numVgsOfflinint++
			cs.setVgOfflining(name)
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
func (cs *Struct) clearMyVgs(rev int64) {
	if !cs.server {
		return
	}

	// Retrieve VG and node state
	_, vgNode, vgIpaddr, vgNetmask, vgNic, _, _, _, _, _, _, _ := cs.gatherInfo(true, rev)

	for name, node := range vgNode {
		if node == cs.hostName {
			_ = callUpDownScript(down, name, vgIpaddr[name], vgNetmask[name],
				vgNic[name])
			cs.setVgOffline(name)
		}
	}

}

// startVgs is called when a node has come ONLINE.
//
// TODO - implement algorithm to spread the VGs more evenly and
// in a predictable manner.
func (cs *Struct) startVgs(rev int64) {

	if !cs.server {
		return
	}

	// Retrieve VG and node state
	vgState, _, _, _, _, vgAutofail, vgEnabled, _, _, nodesOnline,
		_, _ := cs.gatherInfo(true, rev)

	/* Debugging code
	vgState, vgNode, vgIpaddr, vgNetmask, vgNic, vgAutofail, vgEnabled, vgVolumelist, nodesAlreadyDead,
		nodesOnline, nodesHb, _ := cs.gatherInfo(true, rev)

	fmt.Printf("startVgs() ---- vgState: %v vgNode: %v vgIpaddr: %v vgNetmask: %v\n",
		vgState, vgNode, vgIpaddr, vgNetmask)
	fmt.Printf("vgNic: %v vgAutofail: %v vgEnabled: %v vgVolumelist: %v\n",
		vgNic, vgAutofail, vgEnabled, vgVolumelist)
	fmt.Printf("nodesAlreadyDead: %v nodesOnline: %v nodesHb: %v\n",
		nodesAlreadyDead, nodesOnline, nodesHb)
	*/

	// Find VGs which are in the INITIAL or OFFLINE state
	vgsToStart := parseVgOfflineInit(vgState, vgEnabled, vgAutofail)

	cntVgsToStart := vgsToStart.Len()
	if cntVgsToStart == 0 {
		return
	}

	// Set state to ONLINING to initiate the ONLINE
	for vgsToStart.Len() > 0 {
		for _, node := range nodesOnline {
			e := vgsToStart.Front()
			if e == nil {
				// No more VGs to online
				break
			}

			// Set the VG to online.  If the txn fails then leave it on the list
			// for the next loop.
			//
			// TODO - could this be an infinite loop
			// if all nodes are offlining?
			// TODO - this is executed in parallel on all online nodes and could
			// fail.  We need to figure out if racing nodes or failure...
			_ = cs.setVgOnlining(e.Value.(string), node)
			vgsToStart.Remove(e)
		}

	}

	// TODO - figure out which is least loaded node and start spreading the load
	// around...
	// For the initial prototype we just do round robin which does not take into
	// consideration the load of an node.  Could be case that one node is already
	// overloaded.
}

// getVgState returns the state of a VG
func (cs *Struct) getVgState(name string) (state VgState) {
	stateKey := makeVgStateKey(name)
	resp, _ := cs.cli.Get(context.TODO(), stateKey)

	stateStr := string(resp.OpResponse().Get().Kvs[0].Value)

	switch stateStr {
	case INITIALVS.String():
		return INITIALVS
	case ONLININGVS.String():
		return ONLININGVS
	case ONLINEVS.String():
		return ONLINEVS
	case OFFLININGVS.String():
		return OFFLININGVS
	case OFFLINEVS.String():
		return OFFLINEVS
	case FAILEDVS.String():
		return FAILEDVS
	}

	return
}

func (cs *Struct) checkVgExist(vgKeys []string) (err error) {
	for _, v := range vgKeys {
		resp, _ := cs.cli.Get(context.TODO(), v)
		if resp.OpResponse().Get().Count > int64(0) {
			err = errors.New("VG already exists")
			return
		}
	}
	return
}

// calcVgKeys returns all possible key names representing a VG
func calcVgKeys(name string) (nameKey string, ipaddrKey string, netmaskKey string,
	nicKey string, autofailKey string, enabledKey string, stateKey string,
	nodeKey string, volumeListKey string, vgKeys []string) {

	vgKeys = make([]string, 0)
	nameKey = makeVgNameKey(name)
	vgKeys = append(vgKeys, nameKey)
	stateKey = makeVgStateKey(name)
	vgKeys = append(vgKeys, stateKey)
	nodeKey = makeVgNodeKey(name)
	vgKeys = append(vgKeys, nodeKey)
	ipaddrKey = makeVgIpaddrKey(name)
	vgKeys = append(vgKeys, ipaddrKey)
	netmaskKey = makeVgNetmaskKey(name)
	vgKeys = append(vgKeys, netmaskKey)
	nicKey = makeVgNicKey(name)
	vgKeys = append(vgKeys, nicKey)
	autofailKey = makeVgAutoFailoverKey(name)
	vgKeys = append(vgKeys, autofailKey)
	enabledKey = makeVgEnabledKey(name)
	vgKeys = append(vgKeys, enabledKey)
	volumeListKey = makeVgVolumeListKey(name)
	vgKeys = append(vgKeys, volumeListKey)

	return
}

// addVg adds a volume group
//
// TODO - should we add create time, failover time, etc?
func (cs *Struct) addVg(name string, ipAddr string, netMask string,
	nic string, autoFailover bool, enabled bool) (err error) {

	nameKey, ipaddrKey, netmaskKey, nicKey, autofailKey, enabledKey, stateKey,
		nodeKey, volumeListKey, vgKeys := calcVgKeys(name)

	err = cs.checkVgExist(vgKeys)
	if err != nil {
		return
	}

	// Verify that VG does not already exist which means check all
	// keys.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// Verify that the VG and it's attributes are not there.  If they are
		// the transaction will silently return.
		If(
			clientv3.Compare(clientv3.Version(nameKey), "=", 0),
			clientv3.Compare(clientv3.Version(stateKey), "=", 0),
			clientv3.Compare(clientv3.Version(nodeKey), "=", 0),
			clientv3.Compare(clientv3.Version(ipaddrKey), "=", 0),
			clientv3.Compare(clientv3.Version(netmaskKey), "=", 0),
			clientv3.Compare(clientv3.Version(nicKey), "=", 0),
			clientv3.Compare(clientv3.Version(autofailKey), "=", 0),
			clientv3.Compare(clientv3.Version(enabledKey), "=", 0),
			clientv3.Compare(clientv3.Version(volumeListKey), "=", 0),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpPut(nameKey, name),
		clientv3.OpPut(stateKey, INITIALVS.String()),
		clientv3.OpPut(nodeKey, ""),
		clientv3.OpPut(ipaddrKey, ipAddr),
		clientv3.OpPut(netmaskKey, netMask),
		clientv3.OpPut(nicKey, nic),
		clientv3.OpPut(autofailKey, strconv.FormatBool(autoFailover)),
		clientv3.OpPut(enabledKey, strconv.FormatBool(enabled)),
		clientv3.OpPut(volumeListKey, ""),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

// rmVg removes a VG if possible
func (cs *Struct) rmVg(name string) (err error) {

	nameKey, ipaddrKey, netmaskKey, nicKey, autofailKey, enabledKey, stateKey,
		nodeKey, volumeListKey, vgKeys := calcVgKeys(name)

	err = cs.checkVgExist(vgKeys)
	if err == nil {
		err = errors.New("VG does not exist")
		return
	}

	// Don't allow a remove of a VG if ONLINING or ONLINE
	state := cs.getVgState(name)
	if (state == ONLININGVS) || (state == ONLINEVS) {
		err = errors.New("VG is in ONLINING or ONLINE state")
		return
	}

	// Verify that VG does not already exist which means check all
	// keys.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// Verify that the VG and it's attributes are not there.  If they are
		// the transaction will silently return.
		If(
			clientv3.Compare(clientv3.Version(nameKey), "!=", 0),
			clientv3.Compare(clientv3.Version(stateKey), "!=", 0),
			clientv3.Compare(clientv3.Value(stateKey), "!=", ONLINEVS.String()),
			clientv3.Compare(clientv3.Value(stateKey), "!=", ONLININGVS.String()),
			clientv3.Compare(clientv3.Version(nodeKey), "!=", 0),
			clientv3.Compare(clientv3.Version(ipaddrKey), "!=", 0),
			clientv3.Compare(clientv3.Version(netmaskKey), "!=", 0),
			clientv3.Compare(clientv3.Version(nicKey), "!=", 0),
			clientv3.Compare(clientv3.Version(autofailKey), "!=", 0),
			clientv3.Compare(clientv3.Version(enabledKey), "!=", 0),
			clientv3.Compare(clientv3.Version(volumeListKey), "!=", 0),

		// "Then" create the keys with initial values
		).Then(
		clientv3.OpDelete(nameKey),
		clientv3.OpDelete(stateKey),
		clientv3.OpDelete(nodeKey),
		clientv3.OpDelete(ipaddrKey),
		clientv3.OpDelete(netmaskKey),
		clientv3.OpDelete(nicKey),
		clientv3.OpDelete(autofailKey),
		clientv3.OpDelete(enabledKey),
		clientv3.OpDelete(volumeListKey),

	// If failed - silently return
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return

}
