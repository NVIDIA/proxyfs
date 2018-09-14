package consensus

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"strconv"
	"sync"
	"time"
)

// VgState represents the state of a volume group at a given point in time
type VgState int

// NOTE: When updating NodeState be sure to also update String() below.
const (
	INITIALVS   VgState = iota
	ONLININGVS          // STARTING means node has just booted
	ONLINEVS            // ONLINE means the node is available to online VGs
	OFFLININGVS         // OFFLINE means the node gracefully shut down
	OFFLINEVS           // OFFLINE means the node gracefully shut down
	maxVgState          // Must be last field!
)

func (state VgState) String() string {
	return [...]string{"INITIAL", "ONLINING", "ONLINE", "OFFLINING", "OFFLINE"}[state]
}

// NodePrefix returns a string containing the node prefix
func vgPrefix() string {
	return "VG"
}

// NodeKeyStatePrefix returns a string containing the node state prefix
func vgKeyPrefix(v string) string {
	return vgPrefix() + v + ":"
}

func makeVgNameKey(n string) string {
	return vgKeyPrefix("NAME") + n
}

func makeVgStateKey(n string) string {
	return vgKeyPrefix("STATE") + n
}

func makeVgNodeKey(n string) string {
	return vgKeyPrefix("NODE") + n
}

func makeVgIpaddrKey(n string) string {
	return vgKeyPrefix("IPADDR") + n
}

func makeVgNetmaskKey(n string) string {
	return vgKeyPrefix("NETMASK") + n
}

func makeVgNicKey(n string) string {
	return vgKeyPrefix("NIC") + n
}

func makeVgAutoFailoverKey(n string) string {
	return vgKeyPrefix("AUTOFAILOVER") + n
}

func makeVgEnabledKey(n string) string {
	return vgKeyPrefix("ENABLED") + n
}

func makeVgVolumeListKey(n string) string {
	return vgKeyPrefix("VOLUMELIST") + n
}

// vgStateWatchEvents creates a watcher based on node state
// changes.
func (cs *Struct) vgWatchEvents(swg *sync.WaitGroup) {

	// TODO - TODO - figure out what keys we watch!!!
	wch1 := cs.cli.Watch(context.Background(), vgPrefix(),
		clientv3.WithPrefix())

	swg.Done() // The watcher is running!
	for wresp1 := range wch1 {
		fmt.Printf("vgStateWatchEvents() wresp1: %+v\n", wresp1)
		for _, ev := range wresp1.Events {
			fmt.Printf("vgStateWatchEvents for key: %v saw value: %v\n", string(ev.Kv.Key),
				string(ev.Kv.Value))
			// TODO
			fmt.Printf("HAVE VG EVENT - now what???")
		}

		// TODO - how shut this watcher down?
		// TODO - how notified when shutting down?
	}
}

// failoverVgs is called when nodes have died.  The remaining nodes
// are scored and VGs from the failed nodes are started if possible.
// TODO - how avoid overloading a node? need weight for a VG?  what
// about priority for a VG and high priority first?
// TODO - start webserver on different port - fork off script to start VIP
func (cs *Struct) failoverVgs(nodesNewlyDead []string) {
	// TODO - implement this
}

// startVgs is called when a node has come ONLINE.
// TODO - be sure that txn() asserts that node is online
// before onlining the VG!!!!
// TODO - start webserver on different port - fork off script to start VIP
func (cs *Struct) startVgs() {
	// TODO - implement this
	/*
		1. make list of VGs which need to be brought online since nodes asserts
		   dead or which have not been started
		2. score VGs and decide transitions?
		3. set VGs ONLINING and which node with a transaction and verify not
		   already ONLINE somewhere.   Mark failed first???
	*/

}

func (cs *Struct) addVg(name string, ipAddr string, netMask string,
	nic string, autoFailover bool, enabled bool) (err error) {

	// TODO - implement this
	/*
		1. make sure not a duplicate name - will txn fail if this is the case?
		2. verify all attributes
		3. verify that set to correct state
	*/
	nameKey := makeVgNameKey(name)
	stateKey := makeVgStateKey(name)
	nodeKey := makeVgNodeKey(name)
	ipaddrKey := makeVgIpaddrKey(name)
	netmaskKey := makeVgNetmaskKey(name)
	nicKey := makeVgNicKey(name)
	autofailKey := makeVgAutoFailoverKey(name)
	enabledKey := makeVgEnabledKey(name)
	volumeListKey := makeVgVolumeListKey(name)

	// TODO - should we add create time, failover time, etc?

	// Verify that VG does not already exist which means check all
	// keys.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = cs.kvc.Txn(ctx).

		// Verify that VG attributes are not there.....
		If(
			clientv3.Compare(clientv3.Version(nameKey), "=", 0),
			clientv3.Compare(clientv3.Version(stateKey), "=", 0),
			clientv3.Compare(clientv3.Version(nodeKey), "=", 0),
			clientv3.Compare(clientv3.Version(ipaddrKey), "=", 0),
			clientv3.Compare(clientv3.Version(netmaskKey), "=", 0),
			clientv3.Compare(clientv3.Version(nicKey), "=", 0),
			clientv3.Compare(clientv3.Version(autofailKey), "=", 0),
			clientv3.Compare(clientv3.Version(enabledKey), "=", 0),
			clientv3.Compare(clientv3.Version(volumeListKey), "0", 0),

		// "Then" create the keys with initial strings
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

	// If failed - just return error
	).Else().Commit()
	cancel() // NOTE: Difficult memory leak if you do not do this!

	return
}

func (cs *Struct) rmVg(name string) (err error) {
	// TODO - implement this
	/*
		1. make sure OFFLINE or FAILED...client
		2. make sure in one txn can remove all VG - function to
		   remove all attributes at once and to init all at once?
	*/
	return

}
