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

const (
	etcdQueryTimeout  time.Duration = 5 * time.Second
	etcdUpdateTimeout time.Duration = 5 * time.Second
)

// TODO - use etcd namepspace.  At a mininum we need a proxyfsd
// namespace in case we coexist with other users.

// EtcdConn contains a connection to the etcd server and the status of any
// operations in progress
type EtcdConn struct {
	cli             *clientv3.Client // etcd client pointer
	kvc             clientv3.KV
	hostName        string               // hostname of the local host
	stopWatcherChan chan struct{}        // stop all the s
	watcherErrChan  chan error           // any errors returned by watchers
	watcherWG       sync.WaitGroup       // WaitGroup to keep track of watchers outstanding
	HBTicker        *time.Ticker         // HB ticker for sending HB and processing DEAD nodes
	stopHB          bool                 // True means local node DEAD and wants to kill HB routine
	stopHBWG        sync.WaitGroup       // WaitGroup to serialize shutdown of HB goroutine
	server          bool                 // Is this instance a server?
	stopNode        bool                 // CLI - are we offlining node?
	nodeName        string               // CLI - name of node for operation
	offlineVg       bool                 // CLI - are we offlining VG?
	onlineVg        bool                 // CLI - are we onlining VG?
	vgName          string               // CLI - name of VG
	cliWG           sync.WaitGroup       // CLI WG to signal when done
	vgMap           map[string]*VgInfo   // most recent info about each VG
	nodeMap         map[string]*NodeInfo // most recent info about each node
	sync.Mutex

	unitTest bool   // TEST - Is this a unit test?  If so allow some operations.
	swd      string // TEST - Starting working directory
}

// VgState represents the state of a volume group at a given point in time
type VgState string

const (
	INITIALVS   VgState = "INITIAL"   // INITIAL means just created
	ONLININGVS  VgState = "ONLINING"  // ONLINING means VG is starting to come online on the node in the volume list
	ONLINEVS    VgState = "ONLINE"    // ONLINE means VG is online on the node in the volume list
	OFFLININGVS VgState = "OFFLINING" // OFFLINING means the VG is gracefully going offline
	OFFLINEVS   VgState = "OFFLINE"   // OFFLINE means the VG is offline and volume list is empty
	FAILEDVS    VgState = "FAILED"    // FAILED means the VG failed on the node in the volume list
)

// VgInfoValue is the information associated with a volume group key in the etcd
// database (except its JSON encoded in the database)
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
	EtcdKeyHeader
	VgInfoValue
}

// VgState represents the state of a volume group at a given point in time
type NodeState string

const (
	INITIAL   NodeState = "INITIAL"   // INITIAL means just created
	STARTING  NodeState = "STARTING"  // STARTING means node is starting to come online
	ONLINE    NodeState = "ONLINE"    // ONLINE means node is available to online VGs
	OFFLINING NodeState = "OFFLINING" // OFFLINING means the node gracefully shut down
	DEAD      NodeState = "DEAD"      // DEAD means node appears to have left the cluster
)

// NodeInfoValue is the information associated with a node key in the etcd
// database (except its JSON encoded in the database)
//
type NodeInfoValue struct {
	NodeState     NodeState
	NodeHeartBeat time.Time
}

// NodeInfo is information associated with a node as well as the revision number
// when the info was fetched and the last time the value was modified.
//
type NodeInfo struct {
	EtcdKeyHeader
	NodeInfoValue
}

// New creates a new HA client.
func New(endpoints []string, hostName string, timeout time.Duration) (cs *EtcdConn, err error) {

	cs = &EtcdConn{hostName: hostName}

	// Create an etcd client - our current etcd setup does not listen on
	// localhost.  Therefore, we pass the IP addresses used by etcd.
	cs.cli, err = clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: timeout,
		AutoSyncInterval: 60 * time.Second})

	cs.kvc = clientv3.NewKV(cs.cli)

	// capacity of 16 means a maximum of 16 watchers ...
	cs.watcherErrChan = make(chan error, 16)
	cs.stopWatcherChan = make(chan struct{}, 1)

	cs.vgMap = make(map[string]*VgInfo)
	cs.nodeMap = make(map[string]*NodeInfo)
	fmt.Printf("(*consensus.New() opening new &EtcdConn\n")

	return
}

// Endpoints returns the current list of endpoints in use
func (cs *EtcdConn) Endpoints() []string {
	return cs.cli.Endpoints()
}

// Server sets up to be a long running process in the consensus cluster
// driving longer term operations as opposed to CLI which is only doing
// short term operations before exiting.
//
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

	// we're going to be a server! we're going to be a server!
	cs.server = true

	cs.startWatchers()

	// Set state of local node to STARTING
	err = cs.updateNodeState(cs.hostName, RevisionNumber(0), STARTING, nil)
	if err != nil {
		// TODO - assume this means txn timed out, could not
		// get majority, etc.... what do we do?
		fmt.Printf("setInitialNodeState(STARTING) returned err: %v\n", err)
		os.Exit(-1)
	}

	// Watcher will start HB after getting STARTING state
	// wait for the node to become ONLINE
	for {
		nodeInfo, _, err := cs.getNodeInfo(cs.hostName, RevisionNumber(0))
		if err != nil {
			fmt.Printf("getNodeInfo() for host '%s' failed: %s\n", cs.hostName, err)
		} else if nodeInfo.NodeState == ONLINE {
			break
		}
		sleepyTime, _ := time.ParseDuration("250ms")
		time.Sleep(sleepyTime)
	}

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

// Mark the volume group failed (a transition that can occur from any state).
func (cs *EtcdConn) MarkVolumeGroupFailed(name string) (err error) {
	err = cs.markVgFailed(name)
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
	err = cs.updateNodeState(name, RevisionNumber(0), OFFLINING, nil)
	if err != nil {
		return err
	}
	cs.cliWG.Wait()

	return
}

// ListVg grabs info for all volume groups and returns it.
//
func (cs *EtcdConn) ListVg() (allVgInfo map[string]*VgInfo) {

	var err error
	allVgInfo, _, err = cs.getAllVgInfo(RevisionNumber(0))
	if err != nil {
		fmt.Printf("List() failed with: %s\n", err)
	}
	return
}

// ListNode grabs  for all nodes and returns it.
//
func (cs *EtcdConn) ListNode() (allNodeInfo map[string]*NodeInfo, err error) {

	allNodeInfo, _, err = cs.getAllNodeInfo(RevisionNumber(0))
	return
}

// Close the connection
func (cs *EtcdConn) Close() {
	fmt.Printf("(*EtcdConn).Close(): called\n")

	// until, or if, we implement stopWatcher() functions for the several
	// watchers that each client creates, do not zero out the cs.cli
	// pointer they use for messages.
	cs.cli = nil
}

// Cause this local "node" to transition to OFFLINING and wait for another node
// to mark it DEAD.
func (cs *EtcdConn) OfflineNode() {

	cs.updateNodeState(cs.hostName, RevisionNumber(0), OFFLINING, nil)

	// When we receive our own DEAD notification, we
	// close the connection to etcd.   This causes
	// the watchers to exit and waitWatchers() blocks
	// until the watchers have exited.
	cs.waitWatchers()
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
func (cs *EtcdConn) SetTestSWD(swd string) {
	cs.swd = swd
}
