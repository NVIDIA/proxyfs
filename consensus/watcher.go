package consensus

import (
	"sync"
)

// TODO - use etcd namepspace

// TODO - must wrap with WithRequiredLeader
// TODO - review how compaction works with watchers,
//
// watcher is a goroutine which watches for events with the key prefix.
func (cs *EtcdConn) watcher(keyPrefix string, swg *sync.WaitGroup) {

	switch keyPrefix {
	case nodeKeyStatePrefix():
		cs.nodeStateWatchEvents(swg)
	case nodeKeyHbPrefix():
		cs.nodeHbWatchEvents(swg)
	case vgPrefix():
		cs.vgWatchEvents(swg)
	}
	cs.watcherWG.Done()
}

// StartAWatcher starts a goroutine to watch for changes
// to the given keys
func (cs *EtcdConn) startAWatcher(prefixKey string) {
	// Keep track of how many watchers we have started so that we
	// can clean them up as needed.
	cs.watcherWG.Add(1)

	var startedWG sync.WaitGroup
	startedWG.Add(1)

	go cs.watcher(prefixKey, &startedWG)

	// Wait for watcher to start before returning
	startedWG.Wait()
}

// WaitWatchers waits for all watchers to return
func (cs *EtcdConn) waitWatchers() {
	cs.watcherWG.Wait()
}
