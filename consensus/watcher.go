package consensus

import (
	"time"
)

// TODO - use etcd namepspace

// TODO - must wrap with WithRequiredLeader
// TODO - review how compaction works with watchers,
//
// watcher is a goroutine which watches for events with the key prefix.

func (cs *EtcdConn) startWatchers() {

	// start the node watcher(s)
	cs.startNodeWatcher(cs.stopWatcherChan, cs.watcherErrChan, &cs.watcherWG)

	// start a watcher to watch for volume group changes
	cs.startVgWatcher(cs.stopWatcherChan, cs.watcherErrChan, &cs.watcherWG)
}

func (cs *EtcdConn) stopWatchers() {

	// signal the watchers to stop
	cs.stopWatcherChan <- struct{}{}
}

// WaitWatchers waits for all watchers to return
func (cs *EtcdConn) waitWatchers() {

	cs.watcherWG.Wait()

	// drain any values already pushed on errChan
	for {
		select {
		case <-cs.watcherErrChan:
			// err is discarded
		default:
			time.Sleep(10 * time.Millisecond)
			// if there's no more err after 10 ms then its drained
			return
		}
	}

}
