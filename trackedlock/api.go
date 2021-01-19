// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package trackedlock

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/swiftstack/ProxyFS/logger"
)

/*

 * The trackedlock packages provides an implementation of sync.Mutex and
 * sync.RWMutex interfaces that provides additional functionality in the form of
 * lock hold tracking.  In addition, the trackedlock.RWMutexTrack routines can
 * be used to track other RWMutex-like synchronization primitives, like
 * dlm.RWLockStruct and chunked put connections.
 *
 * Specifically, if lock tracking is enabled, the trackedlock packages checks
 * the lock hold time.  When a lock unlocked, if it was held longer than
 * "LockHoldTimeLimit" then a warning is logged along with the stack trace of
 * the Lock() and Unlock() of the lock.  In addition, a daemon, the trackedlock
 * watcher, periodically checks to see if any lock has been locked too long.
 * When a lock is held too long, the daemon logs the goroutine ID and the stack
 * trace of the goroutine that acquired the lock.
 *
 * Subsequent versions of this package may include lock heirarchy checking.
 *
 * The config variable "TrackedLock.LockHoldTimeLimit" is the hold time that
 * triggers warning messages being logged.  If it is 0 then locks are not
 * tracked and the overhead of this package is minimal.
 *
 * The config variable "TrackedLock.LockCheckPeriod" is how often the daemon
 * checks tracked locks.  If it is 0 then no daemon is created and lock hold
 * time is checked only when the lock is unlocked (assuming it is unlocked).
 *
 * trackedlock locks can be locked before this package is initialized, but they
 * will not be tracked until the first time they are locked after
 * initializaiton.
 *
 * The API consists of the config based trackedlock.Up() / Down() /
 * PauseAndContract() / ExpandAndResume() (which are not defined here) and then
 * the Mutex, RWMutex, and RWMutexTrack interfaces.
 */

// The Mutex type that we export, which wraps sync.Mutex to add tracking of lock
// hold time and the stack trace of the locker.
//
type Mutex struct {
	wrappedMutex sync.Mutex // the actual Mutex
	tracker      MutexTrack // tracking information for the Mutex
}

// The RWMutex type that we export, which wraps sync.RWMutex to add tracking of
// lock hold time and the stack trace of the locker.
//
type RWMutex struct {
	wrappedRWMutex sync.RWMutex // actual Mutex
	rwTracker      RWMutexTrack // track holds in shared (reader) mode
}

//
// Tracked Mutex API
//
func (m *Mutex) Lock() {
	m.wrappedMutex.Lock()

	m.tracker.lockTrack(m, nil)
}

func (m *Mutex) Unlock() {
	m.tracker.unlockTrack(m)

	m.wrappedMutex.Unlock()
}

//
// Tracked RWMutex API
//
func (m *RWMutex) Lock() {
	m.wrappedRWMutex.Lock()

	m.rwTracker.lockTrack(m)
}

func (m *RWMutex) Unlock() {
	m.rwTracker.unlockTrack(m)

	m.wrappedRWMutex.Unlock()
}

func (m *RWMutex) RLock() {
	m.wrappedRWMutex.RLock()

	m.rwTracker.rLockTrack(m)
}

func (m *RWMutex) RUnlock() {
	m.rwTracker.rUnlockTrack(m)

	m.wrappedRWMutex.RUnlock()
}

//
// Direct access to trackedlock API for DLM locks
//
func (rwmt *RWMutexTrack) LockTrack(lck interface{}) {
	rwmt.lockTrack(lck)
}

func (rwmt *RWMutexTrack) UnlockTrack(lck interface{}) {
	rwmt.unlockTrack(lck)
}

func (rwmt *RWMutexTrack) RLockTrack(lck interface{}) {
	rwmt.rLockTrack(lck)
}

func (rwmt *RWMutexTrack) RUnlockTrack(lck interface{}) {
	rwmt.rUnlockTrack(lck)
}

func (rwmt *RWMutexTrack) DLMUnlockTrack(lck interface{}) {
	// This uses m.tracker.lockCnt without holding the mutex that protects it.
	// Because this goroutine holds the lock, it cannot change from -1 to 0
	// or >0 to 0 (unless there's a bug where another goroutine releases the
	// lock).  It can change from, say, 1 to 2 or 4 to 3, but that's benign
	// (let's hope the race detector doesn't complain).
	lockCnt := atomic.LoadInt32(&rwmt.tracker.lockCnt)
	switch {
	case lockCnt == -1:
		rwmt.unlockTrack(lck)
	case lockCnt > 0:
		rwmt.rUnlockTrack(lck)
	default:
		errstring := fmt.Errorf("tracker for RWMutexTrack has illegal lockCnt %d", lockCnt)
		logger.PanicfWithError(errstring, "%T lock at %p: %+v", lck, lck, lck)
	}
	return
}
