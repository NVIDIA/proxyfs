// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package dlm

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/trackedlock"
)

// This struct is used by LLM to track a lock.
type localLockTrack struct {
	trackedlock.Mutex
	lockId       string // lock identity (must be unique)
	owners       uint64 // Count of threads which own lock
	waiters      uint64 // Count of threads which want to own the lock (either shared or exclusive)
	state        lockState
	exclOwner    CallerID
	listOfOwners []CallerID
	waitReqQ     *list.List               // List of requests waiting for lock
	rwMutexTrack trackedlock.RWMutexTrack // Track the lock to see how long its held
}

var localLockTrackPool = sync.Pool{
	New: func() interface{} {
		var track localLockTrack

		// every localLockTrack should have a waitReqQ
		track.waitReqQ = list.New()

		return &track
	},
}

type localLockRequest struct {
	requestedState lockState
	*sync.Cond
	wakeUp       bool
	LockCallerID CallerID
}

type lockState int

const (
	nilType lockState = iota
	shared
	exclusive
	stale
)

// NOTE: This is a test-only interface used for unit tests.
//
// This function assumes that globals.Lock() is held.
// TODO - can this be used in more cases without creating entry it if does not exist?
func getTrack(lockId string) (track *localLockTrack, ok bool) {
	track, ok = globals.localLockMap[lockId]
	if !ok {
		return track, ok
	}
	return track, ok
}

// NOTE: This is a test-only interface used for unit tests.
func waitCountWaiters(lockId string, count uint64) {
	for {
		globals.Lock()
		track, ok := getTrack(lockId)

		// If the tracking object has not been created yet, sleep and retry.
		if !ok {
			// Sleep 5 milliseconds and test again
			globals.Unlock()
			time.Sleep(5 * time.Millisecond)
			break
		}

		track.Mutex.Lock()

		globals.Unlock()

		waiters := track.waiters
		track.Mutex.Unlock()

		if waiters == count {
			return
		} else {
			// Sleep 5 milliseconds and test again
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// NOTE: This is a test-only interface used for unit tests.
func waitCountOwners(lockId string, count uint64) {
	for {
		globals.Lock()
		track, ok := getTrack(lockId)

		// If the tracking object has not been created yet, sleep and retry.
		if !ok {
			// Sleep 5 milliseconds and test again
			globals.Unlock()
			time.Sleep(5 * time.Millisecond)
			break
		}

		track.Mutex.Lock()

		globals.Unlock()

		owners := track.owners
		track.Mutex.Unlock()

		if owners == count {
			return
		} else {
			// Sleep 5 milliseconds and test again
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// This function assumes the mutex is held on the tracker structure
func (t *localLockTrack) removeFromListOfOwners(callerID CallerID) {

	// Find Position and delete entry (a map might be more efficient)
	for i, id := range t.listOfOwners {
		if id == callerID {
			lastIdx := len(t.listOfOwners) - 1
			t.listOfOwners[i] = t.listOfOwners[lastIdx]
			t.listOfOwners = t.listOfOwners[:lastIdx]
			return
		}
	}

	panic(fmt.Sprintf("Can't find CallerID: %v in list of lock owners!", callerID))
}

// This function assumes the mutex is held on the tracker structure
func callerInListOfOwners(listOfOwners []CallerID, callerID CallerID) (amOwner bool) {
	// Find Position
	for _, id := range listOfOwners {
		if id == callerID {
			return true
		}
	}

	return false
}

func isLockHeld(lockID string, callerID CallerID, lockHeldType LockHeldType) (held bool) {
	globals.Lock()
	// NOTE: Not doing a defer globals.Unlock() here since grabbing another lock below.

	track, ok := globals.localLockMap[lockID]
	if !ok {

		// Lock does not exist in map
		globals.Unlock()
		return false
	}

	track.Mutex.Lock()

	globals.Unlock()

	defer track.Mutex.Unlock()

	switch lockHeldType {
	case READLOCK:
		return (track.state == shared) && (callerInListOfOwners(track.listOfOwners, callerID))
	case WRITELOCK:
		return (track.state == exclusive) && (callerInListOfOwners(track.listOfOwners, callerID))
	case ANYLOCK:
		return ((track.state == exclusive) || (track.state == shared)) && (callerInListOfOwners(track.listOfOwners, callerID))
	}
	return false
}

func grantAndSignal(track *localLockTrack, localQRequest *localLockRequest) {
	track.state = localQRequest.requestedState
	track.listOfOwners = append(track.listOfOwners, localQRequest.LockCallerID)
	track.owners++

	if track.state == exclusive {
		if track.exclOwner != nil || track.owners != 1 {
			panic(fmt.Sprintf("granted exclusive lock when (exclOwner != nil || track.owners != 1)! "+
				"track lockId %v owners %d waiters %d lockState %v exclOwner %v listOfOwners %v",
				track.lockId, track.owners, track.waiters, track.state,
				*track.exclOwner, track.listOfOwners))
		}
		track.exclOwner = localQRequest.LockCallerID
	}

	localQRequest.wakeUp = true
	localQRequest.Cond.Broadcast()
}

// Process the waitReqQ and see if any locks can be granted.
//
// This function assumes that the tracking mutex is held.
func processLocalQ(track *localLockTrack) {

	// If nothing on queue then return
	if track.waitReqQ.Len() == 0 {
		return
	}

	// If the lock is already held exclusively then nothing to do.
	if track.state == exclusive {
		return
	}

	// At this point, the lock is either stale or shared
	//
	// Loop through Q and see if a request can be granted.  If it can then pop it off the Q.
	for track.waitReqQ.Len() > 0 {
		elem := track.waitReqQ.Remove(track.waitReqQ.Front())
		var localQRequest *localLockRequest
		var ok bool
		if localQRequest, ok = elem.(*localLockRequest); !ok {
			panic("Remove of elem failed!!!")
		}

		// If the lock is already free and then want it exclusive
		if (localQRequest.requestedState == exclusive) && (track.state == stale) {
			grantAndSignal(track, localQRequest)
			return
		}

		// If want exclusive and not free, we can't grant so push on front and break from loop.
		if localQRequest.requestedState == exclusive {
			track.waitReqQ.PushFront(localQRequest)
			return
		}

		// At this point we know the Q entry is shared.  Grant it now.
		grantAndSignal(track, localQRequest)
	}
}

func (l *RWLockStruct) commonLock(requestedState lockState, try bool) (err error) {

	globals.Lock()
	track, ok := globals.localLockMap[l.LockID]
	if !ok {
		// TODO - handle blocking waiting for lock from DLM

		// Lock does not exist in map, get one
		track = localLockTrackPool.Get().(*localLockTrack)
		if track.waitReqQ.Len() != 0 {
			panic(fmt.Sprintf("localLockTrack object %p from pool does not have empty waitReqQ",
				track))
		}
		if len(track.listOfOwners) != 0 {
			panic(fmt.Sprintf("localLockTrack object %p  from pool does not have empty ListOfOwners",
				track))
		}
		track.lockId = l.LockID
		track.state = stale

		globals.localLockMap[l.LockID] = track

	}

	track.Mutex.Lock()
	defer track.Mutex.Unlock()

	globals.Unlock()

	// If we are doing a TryWriteLock or TryReadLock, see if we could
	// grab the lock before putting on queue.
	if try {
		if (requestedState == exclusive) && (track.state != stale) {
			err = errors.New("Lock is busy - try again!")
			return blunder.AddError(err, blunder.TryAgainError)
		} else {
			if track.state == exclusive {
				err = errors.New("Lock is busy - try again!")
				return blunder.AddError(err, blunder.TryAgainError)
			}
		}
	}
	localRequest := localLockRequest{requestedState: requestedState, LockCallerID: l.LockCallerID, wakeUp: false}
	localRequest.Cond = sync.NewCond(&track.Mutex)
	track.waitReqQ.PushBack(&localRequest)

	track.waiters++

	// See if any locks can be granted
	processLocalQ(track)

	// wakeUp will already be true if processLocalQ() signaled this thread to wakeup.
	for localRequest.wakeUp == false {
		localRequest.Cond.Wait()
	}

	// sanity check request and lock state
	if localRequest.wakeUp != true {
		panic(fmt.Sprintf("commonLock(): thread awoke without being signalled; localRequest %v "+
			"track lockId %v owners %d waiters %d lockState %v exclOwner %v listOfOwners %v",
			localRequest, track.lockId, track.owners, track.waiters, track.state,
			*track.exclOwner, track.listOfOwners))
	}
	if track.state == stale || track.owners == 0 || (track.owners > 1 && track.state != shared) {
		panic(fmt.Sprintf("commonLock(): lock is in undefined state: localRequest %v "+
			"track lockId %v owners %d waiters %d lockState %v exclOwner %v listOfOwners %v",
			localRequest, track.lockId, track.owners, track.waiters, track.state,
			*track.exclOwner, track.listOfOwners))
	}

	// let trackedlock package track how long we hold the lock
	if track.state == exclusive {
		track.rwMutexTrack.LockTrack(track)
	} else {
		track.rwMutexTrack.RLockTrack(track)
	}

	// At this point, we got the lock either by the call to processLocalQ() above
	// or as a result of processLocalQ() being called from the unlock() path.

	// We decrement waiters here instead of in processLocalQ() so that other threads do not
	// assume there are no waiters between the time the Cond is signaled and we wakeup this thread.
	track.waiters--

	return nil
}

// unlock() releases the lock and signals any waiters that the lock is free.
func (l *RWLockStruct) unlock() (err error) {

	// TODO - assert not stale and if shared that count != 0
	globals.Lock()
	track, ok := globals.localLockMap[l.LockID]
	if !ok {
		panic(fmt.Sprintf("Trying to Unlock() inode: %v and lock not found in localLockMap()!", l.LockID))
	}

	track.Mutex.Lock()

	// Remove lock from localLockMap if no other thread using.
	//
	// We have track structure for lock.  While holding mutex on localLockMap, remove
	// lock from map if we are the last holder of the lock.
	// TODO - does this handle revoke case and any others?
	var deleted = false
	if (track.owners == 1) && (track.waiters == 0) {
		deleted = true
		delete(globals.localLockMap, l.LockID)
	}

	globals.Unlock()

	// TODO - handle release of lock back to DLM and delete from localLockMap
	// Set stale and signal any waiters
	track.owners--
	track.removeFromListOfOwners(l.LockCallerID)
	if track.state == exclusive {
		if track.owners != 0 || track.exclOwner == nil {
			panic(fmt.Sprintf("releasing exclusive lock when (exclOwner == nil || track.owners != 0)! "+
				"track lockId %v owners %d waiters %d lockState %v exclOwner %v listOfOwners %v",
				track.lockId, track.owners, track.waiters, track.state,
				*track.exclOwner, track.listOfOwners))
		}
		track.exclOwner = nil
	}

	if track.owners == 0 {
		track.state = stale
	} else {
		if track.owners < 0 {
			panic("track.owners < 0!!!")
		}
	}
	// record the release of the lock
	track.rwMutexTrack.DLMUnlockTrack(track)

	// See if any locks can be granted
	processLocalQ(track)

	track.Mutex.Unlock()

	// can't return the
	if deleted {
		if track.waitReqQ.Len() != 0 || track.waiters != 0 || track.state != stale {
			panic(fmt.Sprintf(
				"localLockTrack object %p retrieved from pool does not have an empty waitReqQ",
				track.waitReqQ))
		}
		localLockTrackPool.Put(track)
	}

	// TODO what error is possible?
	return nil
}
