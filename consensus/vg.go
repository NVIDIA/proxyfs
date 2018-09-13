package consensus

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"sync"
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
func vgKeyStatePrefix() string {
	return vgPrefix() + "STATE"
}

func makeVgStateKey(n string) string {
	return vgKeyStatePrefix() + n
}

// vgStateWatchEvents creates a watcher based on node state
// changes.
func (cs *Struct) vgStateWatchEvents(swg *sync.WaitGroup) {

	wch1 := cs.cli.Watch(context.Background(), vgKeyStatePrefix(),
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
func (cs *Struct) startVgs(nodeStarted []string) {

}
