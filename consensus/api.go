package consensus

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"os"
	"sync"
	"time"
)

// TODO - use etcd namepspace.  At a mininum we need a proxyfsd
// namespace in case we coexist with other users.

// EtcdConn contains a connection to the etcd server and the status of any
// operations in progress
type EtcdConn struct {
	cli       *clientv3.Client // etcd client pointer
	kvc       clientv3.KV
	hostName  string         // hostname of the local host
	watcherWG sync.WaitGroup // WaitGroup to keep track of watchers outstanding
	HBTicker  *time.Ticker   // HB ticker for sending HB and processing DEAD nodes
	server    bool           // Is this instance a server?
	stopNode  bool           // CLI - are we offlining node?
	nodeName  string         // CLI - name of node for operation
	offlineVg bool           // CLI - are we offlining VG?
	onlineVg  bool           // CLI - are we onlining VG?
	vgName    string         // CLI - name of VG
	cliWG     sync.WaitGroup // CLI WG to signal when done

	unitTest bool   // TEST - Is this a unit test?  If so allow some operations.
	swd      string // TEST - Starting working directory
}

// RevisionNumber is a database revision number.  All values in the database with
// the same revision number appeared in the database with that value at the same
// point in time.
//
type RevisionNumber int64

// VgState represents the state of a volume group at a given point in time
type VgState string

// NOTE: When updating NodeState be sure to also update String() below.
const (
	INITIALVS   VgState = "INITIAL"   // INITIAL means just created
	ONLININGVS  VgState = "ONLINING"  // ONLINING means VG is starting to come online on the node in the volume list
	ONLINEVS    VgState = "ONLINE"    // ONLINE means VG is online on the node in the volume list
	OFFLININGVS VgState = "OFFLINING" // OFFLINING means the VG is gracefully going offline
	OFFLINEVS   VgState = "OFFLINE"   // OFFLINE means the VG is offline and volume list is empty
	FAILEDVS    VgState = "FAILED"    // FAILED means the VG failed on the node in the volume list
	maxVgState                        // Must be last field!
)

// VgInfoValue is the information associated with a volume group key in the etcd
// database (except its JSON encoded)
//
type VgInfoValue struct {
	VgState      VgState
	VgNode       string
	VgIpAddr     string
	VgNetmask    string
	VgNic        string
	VgEnabled    bool
	VgAutofail   bool
	VgVolumeList string
}

// VgInfo is information associated with a volume group as well as the revision
// number when the info was fetched and the last time the value was modified.

//
type VgInfo struct {
	CreateRevNum RevisionNumber
	ModRevNum    RevisionNumber
	RevNum       RevisionNumber
	VgInfoValue
}

// AllNodeInfo contains the state of the interesting keys in the shared database
// relevant to nodes as of a particular revision number (sequence number)
//
type AllNodeInfo struct {
	RevNum           RevisionNumber
	NodesHb          map[string]time.Time
	NodesState       map[string]string
	NodesAlreadyDead []string
	NodesOnline      []string
}

// ClusterInfo contains the state of the interesting keys in the shared database
// relevant to the cluster as of a particular revision number (sequence number).
// (The revision numbers is VgInfo and NodeInfo are the same as RevNum.)
//
type AllClusterInfo struct {
	RevNum      RevisionNumber
	AllNodeInfo AllNodeInfo
}

// New creates a new HA client.
func New(endpoints []string, hostName string, timeout time.Duration) (cs *EtcdConn, err error) {
	cs = &EtcdConn{hostName: hostName}

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs.cli, err = clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: timeout})

	cs.kvc = clientv3.NewKV(cs.cli)

	return
}

func (cs *EtcdConn) startWatchers() {

	// Start a watcher to watch for node state changes
	cs.startAWatcher(nodeKeyStatePrefix())

	// Start a watcher to watch for node heartbeats
	cs.startAWatcher(nodeKeyHbPrefix())

	// Start a watcher to watch for volume group changes
	cs.startAWatcher(getVgKeyPrefix())
}

// Server sets up to be a long running process in the consensus cluster
// driving longer term operations as opposed to CLI which is only doing
// short term operations before exiting.
func (cs *EtcdConn) Server() (err error) {

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

	cs.server = true

	cs.startWatchers()

	// Set state of local node to STARTING
	err = cs.setNodeStateForced(cs.hostName, STARTINGNS)
	if err != nil {
		// TODO - assume this means txn timed out, could not
		// get majority, etc.... what do we do?
		fmt.Printf("setInitialNodeState(STARTING) returned err: %v\n", err)
		os.Exit(-1)
	}

	// Watcher will start HB after getting STARTING state

	return
}

// CLI sets up to do CLI operations in the cluster.
func (cs *EtcdConn) CLI() (err error) {

	// paranoid...
	cs.server = false

	cs.startWatchers()

	return
}

// AddVolumeGroup creates a new volume group.
// TODO - verify valid input
func (cs *EtcdConn) AddVolumeGroup(name string, ipAddr string, netMask string,
	nic string, autoFailover bool, enabled bool) (err error) {

	err = cs.addVg(name, ipAddr, netMask, nic, autoFailover, enabled)
	return
}

// RmVolumeGroup removes a volume group if it is OFFLINE
func (cs *EtcdConn) RmVolumeGroup(name string) (err error) {
	err = cs.rmVg(name)
	return
}

// AddVolumeToVG adds a volume to an existing volume group
// TODO - implement this
func (cs *EtcdConn) AddVolumeToVG(vgName string, newVolume string) (err error) {
	// TODO - can you add volume to FAILED VG, need checks in other layers,
	// liveliness? does VG state change if volume is offline?
	return
}

// RmVolumeFromVG removes the volume from the VG
// TODO - implement this
func (cs *EtcdConn) RmVolumeFromVG(vgName string, volumeName string) (err error) {
	// TODO - can you you remove volume only if unmounted?  assume so since
	// VIP will change.   Will it also remove the volume?  Do we need a move
	// API to move from one VG to another VG?
	return
}

// CLIOfflineVg offlines the volume group and waits until it is offline
//
// This routine can only be called from CLI.
func (cs *EtcdConn) CLIOfflineVg(vgName string) (err error) {

	if cs.server {
		fmt.Printf("CLIOfflineVg() can only be called from CLI and not from server")
		os.Exit(1)
	}

	cs.offlineVg = true
	cs.vgName = vgName

	// TODO - What if VG is already OFFLINE?
	// need to verify state
	err = cs.setVgOfflining(vgName)
	if err != nil {
		fmt.Printf("CLIOfflineVg(): offline of VG %s failed: %s\n", vgName, err)
		return
	}

	cs.cliWG.Wait()
	// TODO: did it succeed?
	return
}

// CLIOnlineVg onlines the volume group on the node and waits until it is online
//
// This routine can only be called from CLI.
func (cs *EtcdConn) CLIOnlineVg(vgName string, node string) (err error) {

	if cs.server {
		fmt.Printf("CLIOnlineVg() can only be called from CLI and not from server")
		os.Exit(1)
	}

	cs.onlineVg = true
	cs.vgName = vgName

	err = cs.setVgOnlining(vgName, node)
	if err != nil {
		fmt.Printf("CLIOnlineVg(): online of VG %s failed: %s\n", vgName, err)
		return
	}

	cs.cliWG.Wait()
	// TODO: did it succeed?
	return
}

// CLIStopNode offlines all volume groups on a node and then stops the node.
//
// This routine can only be called from CLI.
func (cs *EtcdConn) CLIStopNode(name string) (err error) {

	// TODO - verify that valid node transition...
	// based on current state to proper transition
	// i.e. ONLINE->OFFLINING->DEAD

	if cs.server {
		fmt.Printf("OfflineNode() can only be called from CLI and not from server")
		os.Exit(1)
	}

	cs.stopNode = true
	cs.nodeName = name
	err = cs.setNodeState(name, OFFLININGNS)
	if err != nil {
		return err
	}
	cs.cliWG.Wait()

	return
}

// ListVG grabs info for all volume groups and returns it.
//
func (cs *EtcdConn) ListVg() (allVgInfo map[string]*VgInfo) {

	var err error
	allVgInfo, _, err = cs.getAllVgInfo(RevisionNumber(0))
	if err != nil {
		fmt.Printf("List() failed with: %s\n", err)
	}
	return
}

// ListVG grabs  for all nodes and returns it.
//
// Note that this uses the "old format".
//
func (cs *EtcdConn) ListNode() AllNodeInfo {

	return cs.getRevNodeState(RevisionNumber(0))
}

// Close will close client connection with HA
//
// TODO - this should probably set state OFFLINING
// to initiate OFFLINE and use waitgroup to see
// own OFFLINE before calling Close()
func (cs *EtcdConn) Close() {

	cs.cli.Close()

	cs.waitWatchers()

	cs.cli = nil
}

// SetTest sets the bool signalling if this is a unit
// test.  If it is a unit test then the location of
// of test scripts as well as operations allowed is
// different.
//
// NOTE: This should only be called from a unit test.
func (cs *EtcdConn) SetTest(flag bool) {
	cs.unitTest = flag
}

// SetTestSWD sets the starting working directory for use by
// test scripts.
//
// NOTE: This should only be called from a unit test.
func (cs *Struct) SetTestSWD(swd string) {
	cs.swd = swd
}
