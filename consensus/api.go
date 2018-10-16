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

// Struct is the connection to our consensus protocol
type Struct struct {
	cli       *clientv3.Client // etcd client pointer
	kvc       clientv3.KV
	hostName  string         // hostname of the local host
	watcherWG sync.WaitGroup // WaitGroup to keep track of watchers outstanding
	HBTicker  *time.Ticker   // HB ticker for sending HB
	// and processing DEAD nodes.

	server bool // Is this instance a server?

	stopNode  bool           // CLI - are we offlining node?
	nodeName  string         // CLI - name of node for operation
	offlineVg bool           // CLI - are we offlining VG?
	onlineVg  bool           // CLI - are we onlining VG?
	vgName    string         // CLI - name of VG
	cliWG     sync.WaitGroup // CLI WG to signal when done
}

// A database revision number.  All values in the database with the same
// revision number appeared in the database with that value at the same point in
// time.
//
type RevisionNumber int64

// VgInfo contains the state of the interesting keys in the shared database
// relevant to Volume Groups as of a particular revision number (sequence number)
//
type VgInfo struct {
	RevNum       RevisionNumber
	VgState      map[string]string
	VgNode       map[string]string
	VgIpAddr     map[string]string
	VgNetmask    map[string]string
	VgNic        map[string]string
	VgEnabled    map[string]bool
	VgAutofail   map[string]bool
	VgVolumeList map[string]string
}

// NodeInfo contains the state of the interesting keys in the shared database
// relevant to nodes as of a particular revision number (sequence number)
//
type NodeInfo struct {
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
type ClusterInfo struct {
	RevNum   RevisionNumber
	NodeInfo NodeInfo
	VgInfo   VgInfo
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

	return
}

func (cs *Struct) startWatchers() {

	// Start a watcher to watch for node state changes
	cs.startAWatcher(nodeKeyStatePrefix())

	// Start a watcher to watch for node heartbeats
	cs.startAWatcher(nodeKeyHbPrefix())

	// Start a watcher to watch for volume group changes
	cs.startAWatcher(vgPrefix())
}

// Server sets up to be a long running process in the consensus cluster
// driving longer term operations as opposed to CLI which is only doing
// short term operations before exiting.
func (cs *Struct) Server() (err error) {

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
func (cs *Struct) CLI() (err error) {

	// paranoid...
	cs.server = false

	cs.startWatchers()

	return
}

// AddVolumeGroup creates a new volume group.
// TODO - verify valid input
func (cs *Struct) AddVolumeGroup(name string, ipAddr string, netMask string,
	nic string, autoFailover bool, enabled bool) (err error) {
	err = cs.addVg(name, ipAddr, netMask, nic, autoFailover, enabled)
	return
}

// RmVolumeGroup removes a volume group if it is OFFLINE
func (cs *Struct) RmVolumeGroup(name string) (err error) {
	err = cs.rmVg(name)
	return
}

// AddVolumeToVG adds a volume to an existing volume group
// TODO - implement this
func (cs *Struct) AddVolumeToVG(vgName string, newVolume string) (err error) {
	// TODO - can you add volume to FAILED VG, need checks in other layers,
	// liveliness? does VG state change if volume is offline?
	return
}

// RmVolumeFromVG removes the volume from the VG
// TODO - implement this
func (cs *Struct) RmVolumeFromVG(vgName string, volumeName string) (err error) {
	// TODO - can you you remove volume only if unmounted?  assume so since
	// VIP will change.   Will it also remove the volume?  Do we need a move
	// API to move from one VG to another VG?
	return
}

// CLIOfflineVg offlines the volume group and waits until it is offline
//
// This routine can only be called from CLI.
func (cs *Struct) CLIOfflineVg(name string) (err error) {

	if cs.server {
		fmt.Printf("CLIOfflineVg() can only be called from CLI and not from server")
		os.Exit(1)
	}

	cs.offlineVg = true
	cs.vgName = name

	// TODO - What if VG is already OFFLINE?
	// need to verify state
	err = cs.setVgOfflining(name)
	if err != nil {
		return err
	}

	cs.cliWG.Wait()
	return
}

// CLIOnlineVg onlines the volume group on the node and waits until it is online
//
// This routine can only be called from CLI.
func (cs *Struct) CLIOnlineVg(name string, node string) (err error) {

	if cs.server {
		fmt.Printf("CLIOnlineVg() can only be called from CLI and not from server")
		os.Exit(1)
	}

	cs.onlineVg = true
	cs.vgName = name

	// TODO - What if VG is already ONLINE?
	// need to verify state
	err = cs.setVgOnlining(name, node)
	if err != nil {
		return err
	}

	cs.cliWG.Wait()
	return
}

// CLIStopNode offlines all volume groups on a node and then stops the node.
//
// This routine can only be called from CLI.
func (cs *Struct) CLIStopNode(name string) (err error) {

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

// List grabs all VG and node state and returns it.
func (cs *Struct) List() (clusterInfo ClusterInfo) {

	clusterInfo = cs.gatherInfo(RevisionNumber(0))

	return
}

// Unregister from the consensus protocol
// TODO - this should probably set state OFFLINING
// to initiate OFFLINE and use waitgroup to see
// own OFFLINE before calling Close()
func (cs *Struct) Unregister() {

	cs.cli.Close()

	cs.waitWatchers()

	cs.cli = nil
}
