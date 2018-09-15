package consensus

import (
	"context"
	"errors"
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

	// TODO - figure out what keys we watch!!!
	wch1 := cs.cli.Watch(context.Background(), vgPrefix(),
		clientv3.WithPrefix())

	swg.Done() // The watcher is running!
	for wresp1 := range wch1 {
		for _, ev := range wresp1.Events {
			fmt.Printf("vgStateWatchEvents for key: %v saw value: %v\n", string(ev.Kv.Key),
				string(ev.Kv.Value))
			// TODO - what do with the VG watch events
		}

		// TODO - watcher only shutdown when local node is OFFLINE
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
