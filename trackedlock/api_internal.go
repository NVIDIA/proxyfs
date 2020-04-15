package trackedlock

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/utils"
)

// Tracked locks tracking structure and configurables.
//
type globalsStruct struct {
	mapMutex               sync.Mutex                    // protects mutexMap and rwMutexMap
	mutexMap               map[*MutexTrack]interface{}   // the Mutex like locks  being watched
	rwMutexMap             map[*RWMutexTrack]interface{} // the RWMutex like locks being watched
	lockCheckChan          <-chan time.Time              // wait here to check on locks
	stopChan               chan struct{}                 // time to shutdown and go home
	doneChan               chan struct{}                 // shutdown complete
	lockCheckTicker        *time.Ticker                  // ticker for lock check time
	dlmRWLockType          interface{}                   // set by a callback from the DLM code
	lockWatcherLocksLogged int                           // max overlimit locks logged by lockWatcher()
	lockHoldTimeLimit      int64                         // locks held longer then lockHoldTimeLimit
	// get logged; lockHoldTimeLimit is really a time.Duration but the locking and
	// unlocking routines need to fetch it atomically so it must be an int64
	lockCheckPeriod time.Duration // check locks once each period
}

var globals globalsStruct

// stackTraceSlice holds the stack trace of one thread.  stackTraceBuf is the
// storage required to hold one stack trace. We keep a pool of them around.
//
type stackTraceSlice []byte
type stackTraceBuf [4040]byte

// time.IsZero() will return true if applied to timeZero.  It indicates an
// uninitialized time stamp.
//
var timeZero time.Time

type stackTraceObj struct {
	stackTrace    stackTraceSlice // stack trace of curent or last locker
	stackTraceBuf stackTraceBuf   // storage for stack trace slice
}

var stackTraceObjPool = sync.Pool{
	New: func() interface{} {
		return &stackTraceObj{}
	},
}

// Track a Mutex or RWMutex held in exclusive mode (lockCnt is also used to track the
// number of shared lockers; updates must use atomic operations to load and store it
// because of a benign race with lockWatcher() that the go race detector flags).
//
type MutexTrack struct {
	isWatched  bool           // true if lock is on list of checked mutexes
	lockCnt    int32          // 0 if unlocked, -1 locked exclusive, > 0 locked shared
	lockTime   time.Time      // time last lock operation completed
	lockerGoId uint64         // goroutine ID of the last locker
	lockStack  *stackTraceObj // stack trace when object was last locked
}

// Track an RWMutex
//
type RWMutexTrack struct {
	tracker         MutexTrack                // tracking info when the lock is held exclusive
	sharedStateLock sync.Mutex                // lock the following fields (shared mode state) and lockCnt
	rLockTime       map[uint64]time.Time      // GoId -> lock acquired time
	rLockStack      map[uint64]*stackTraceObj // GoId -> locker stack trace
}

// Locking a Mutex or an RWMutex in exclusive (writer) mode.  If this an
// Mutex-like lock then rwmt is nil, otherwise it points to the RWMutexTrack.
//
// Note that holding an RWMutex in exclusive mode insures that no goroutine
// holds it in shared mode.
//
func (mt *MutexTrack) lockTrack(wrappedLock interface{}, rwmt *RWMutexTrack) {

	// if lock tracking is disabled, just mark the lock as locked and clear
	// the last locked time.  (Recording the current time using time.Now()
	// is too expensive, but we don't want this lock marked as held too long
	// if lock tracking is enabled while its locked.)
	if atomic.LoadInt64(&globals.lockHoldTimeLimit) == 0 {
		mt.lockTime = timeZero
		atomic.StoreInt32(&mt.lockCnt, -1)
		return
	}

	mt.lockStack = stackTraceObjPool.Get().(*stackTraceObj)
	mt.lockStack.stackTrace = mt.lockStack.stackTraceBuf[:]

	cnt := runtime.Stack(mt.lockStack.stackTrace, false)
	mt.lockStack.stackTrace = mt.lockStack.stackTrace[0:cnt]
	mt.lockerGoId = utils.StackTraceToGoId(mt.lockStack.stackTrace)
	mt.lockTime = time.Now()
	atomic.StoreInt32(&mt.lockCnt, -1)

	// add to the list of watched mutexes if anybody is watching -- its only
	// at this point that we need to know if this a Mutex or RWMutex, and it
	// only happens when not currently watched
	if !mt.isWatched && globals.lockCheckPeriod != 0 {
		globals.mapMutex.Lock()

		if rwmt != nil {
			globals.rwMutexMap[rwmt] = wrappedLock
		} else {
			globals.mutexMap[mt] = wrappedLock
		}
		globals.mapMutex.Unlock()
		mt.isWatched = true
	}

	return
}

// Unlocking a Mutex or unlocking an RWMutex held in exclusive (writer) mode
//
func (mt *MutexTrack) unlockTrack(wrappedLock interface{}) {

	// if we're checking the lock hold time and the locking time is recorded
	// then check it
	lockHoldTimeLimit := atomic.LoadInt64(&globals.lockHoldTimeLimit)
	if lockHoldTimeLimit != 0 && !mt.lockTime.IsZero() {
		now := time.Now()
		if now.Sub(mt.lockTime).Nanoseconds() >= lockHoldTimeLimit {

			var buf stackTraceBuf
			unlockStackTrace := buf[:]
			cnt := runtime.Stack(unlockStackTrace, false)
			unlockStr := string(unlockStackTrace[0:cnt])

			// mt.lockTime is recorded even if globals.lockHoldTimeLimit == 0,
			// so its possible mt.lockStack is not allocated
			lockStr := "goroutine 9999 [unknown]\nlocked before lock tracking enabled\n"
			if mt.lockStack != nil {
				lockStr = string(mt.lockStack.stackTrace)
			}
			logger.Warnf("Unlock(): %T at %p locked for %f sec;"+
				" stack at call to Lock():\n%s\nstack at Unlock():\n%s",
				wrappedLock, wrappedLock,
				float64(now.Sub(mt.lockTime))/float64(time.Second), lockStr, unlockStr)
		}
	}

	// release the lock
	atomic.StoreInt32(&mt.lockCnt, 0)
	if mt.lockStack != nil {
		stackTraceObjPool.Put(mt.lockStack)
		mt.lockStack = nil
	}
	return
}

// Tracking an RWMutex locked exclusive is just like a regular Mutex
//
func (rwmt *RWMutexTrack) lockTrack(wrappedLock interface{}) {
	rwmt.tracker.lockTrack(wrappedLock, rwmt)
}

func (rwmt *RWMutexTrack) unlockTrack(wrappedLock interface{}) {
	rwmt.tracker.unlockTrack(wrappedLock)
}

// Tracking an RWMutex locked shared is more work
//
func (rwmt *RWMutexTrack) rLockTrack(wrappedLock interface{}) {

	// if lock tracking is disabled, just mark the lock as locked and clear
	// the last locked time.  (Recording the current time using time.Now()
	// is too expensive, but we don't want this lock marked as held too long
	// if lock tracking is enabled while its locked.)
	if atomic.LoadInt64(&globals.lockHoldTimeLimit) == 0 {
		rwmt.sharedStateLock.Lock()
		lockCnt := rwmt.tracker.lockCnt + 1
		atomic.StoreInt32(&rwmt.tracker.lockCnt, lockCnt)
		rwmt.tracker.lockTime = timeZero
		rwmt.sharedStateLock.Unlock()

		return
	}

	// get the stack trace and goId before getting the shared state lock to
	// cut down on lock contention
	lockStack := stackTraceObjPool.Get().(*stackTraceObj)
	lockStack.stackTrace = lockStack.stackTraceBuf[:]

	cnt := runtime.Stack(lockStack.stackTrace, false)
	lockStack.stackTrace = lockStack.stackTrace[0:cnt]
	goId := utils.StackTraceToGoId(lockStack.stackTrace)

	// The lock is held as a reader (shared mode) so no goroutine can have
	// it locked exclusive.  Holding rwmt.sharedStateLock is sufficient to
	// insure that no other goroutine is changing rwmt.tracker.lockCnt.  The
	// atomic store is still required to make the race detector happy when
	// lockWatcher() loads it without getting a lock.
	rwmt.sharedStateLock.Lock()

	if rwmt.rLockStack == nil {
		rwmt.rLockStack = make(map[uint64]*stackTraceObj)
		rwmt.rLockTime = make(map[uint64]time.Time)
	}
	rwmt.tracker.lockTime = time.Now()
	rwmt.rLockTime[goId] = rwmt.tracker.lockTime
	rwmt.rLockStack[goId] = lockStack
	lockCnt := rwmt.tracker.lockCnt + 1
	atomic.StoreInt32(&rwmt.tracker.lockCnt, lockCnt)

	// add to the list of watched mutexes if anybody is watching
	if !rwmt.tracker.isWatched && globals.lockCheckPeriod != 0 {

		globals.mapMutex.Lock()
		globals.rwMutexMap[rwmt] = wrappedLock
		globals.mapMutex.Unlock()
		rwmt.tracker.isWatched = true
	}
	rwmt.sharedStateLock.Unlock()

	return
}

func (rwmt *RWMutexTrack) rUnlockTrack(wrappedLock interface{}) {

	lockHoldTimeLimit := atomic.LoadInt64(&globals.lockHoldTimeLimit)
	if lockHoldTimeLimit == 0 {
		rwmt.sharedStateLock.Lock()

		// since lock hold time is not being checked, discard any info
		// left over from an earlier time when it was
		if rwmt.rLockStack != nil {
			for goId, lockStack := range rwmt.rLockStack {
				stackTraceObjPool.Put(lockStack)
				delete(rwmt.rLockStack, goId)
				delete(rwmt.rLockTime, goId)
			}
			rwmt.rLockStack = nil
			rwmt.rLockTime = nil
		}

		lockCnt := rwmt.tracker.lockCnt - 1
		atomic.StoreInt32(&rwmt.tracker.lockCnt, lockCnt)

		rwmt.sharedStateLock.Unlock()
		return
	}

	// Lock tracking is enabled; track the unlock of this lock.
	//
	// If the goroutine unlocking a shared lock is not the same as the one
	// that locked it then we cannot match up RLock() and RUnlock()
	// operations (unless we add an API that allows lock "ownership" to be
	// passed between goroutines and the client of the API actually calls
	// it). This is complicated by the situation when lock tracking
	// (globals.lockHoldTimeLimit > 0) is enabled after some locks have
	// already been acquired in reader mode.
	//
	// To handle the second case this code does not kick up a fuss if it
	// can't find an RLock() operation with a goID corresponding to this
	// RUnlock(), assuming that the RLock() occurred when locks were not
	// being tracked. It does nothing (and does not remove an entry from
	// rwmt.rLockTime).  As a consequence, if shared locks are been passed
	// between go routines rwmt.rLockTime and rwmt.rLockStack can have a
	// bunch of stale entries which would trigger false instances of "lock
	// held too long" by the lock watcher.  To compensate for this, when the
	// last RLock() is released, we clear out extra stale entries.
	goId := utils.GetGoId()
	now := time.Now()
	rwmt.sharedStateLock.Lock()
	rLockTime, ok := rwmt.rLockTime[goId]
	if ok && now.Sub(rLockTime).Nanoseconds() >= lockHoldTimeLimit {

		var buf stackTraceBuf
		stackTrace := buf[:]
		cnt := runtime.Stack(stackTrace, false)
		stackTrace = stackTrace[0:cnt]

		logger.Warnf("RUnlock(): %T at %p locked for %f sec;"+
			" stack at call to RLock():\n%s\nstack at RUnlock():\n%s",
			wrappedLock, wrappedLock, float64(now.Sub(rLockTime))/float64(time.Second),
			rwmt.rLockStack[goId].stackTrace, string(stackTrace))
	}

	// atomic load not needed due to sharedStateLock
	if rwmt.tracker.lockCnt == 1 && len(rwmt.rLockTime) > 1 {
		for goId, stackTraceObj := range rwmt.rLockStack {
			stackTraceObjPool.Put(stackTraceObj)
			delete(rwmt.rLockStack, goId)
			delete(rwmt.rLockTime, goId)
		}
	} else if ok {
		stackTraceObjPool.Put(rwmt.rLockStack[goId])
		delete(rwmt.rLockStack, goId)
		delete(rwmt.rLockTime, goId)
	}

	lockCnt := rwmt.tracker.lockCnt - 1
	atomic.StoreInt32(&rwmt.tracker.lockCnt, lockCnt)
	rwmt.sharedStateLock.Unlock()

	return
}

// information about a lock that is held too long
type longLockHolder struct {
	lockPtr      interface{}   // pointer to the actual lock
	lockTime     time.Time     // time last lock operation completed
	lockerGoId   uint64        // goroutine ID of the last locker
	lockStackStr string        // stack trace when the object was locked
	lockOp       string        // lock operation name ("Lock()" or "RLock()")
	mutexTrack   *MutexTrack   // if lock is a Mutex, MutexTrack for lock
	rwMutexTrack *RWMutexTrack // if lock is an RWMutex, RWMutexTrack for lock
}

// Record a lock that has been held too long.
//
// longLockHolders is a slice containing longLockHolder information for upto
// globals.lockWatcherLocksLogged locks, sorted from longest to shortest held.  Add
// newHolder to the slice, potentially discarding the lock that has been held least
// long.
//
func recordLongLockHolder(longLockHolders []*longLockHolder, newHolder *longLockHolder) []*longLockHolder {

	// if there's room append the new entry, else overwrite the youngest lock holder
	if len(longLockHolders) < globals.lockWatcherLocksLogged {
		longLockHolders = append(longLockHolders, newHolder)
	} else {
		longLockHolders[len(longLockHolders)-1] = newHolder
	}

	// the new entry may not be the shortest lock holder; resort the list
	for i := len(longLockHolders) - 2; i >= 0; i -= 1 {

		if longLockHolders[i].lockTime.Before(longLockHolders[i+1].lockTime) {
			break
		}
		tmpHolder := longLockHolders[i]
		longLockHolders[i] = longLockHolders[i+1]
		longLockHolders[i+1] = tmpHolder
	}

	return longLockHolders
}

// Periodically check for locks that have been held too long.
//
func lockWatcher() {

	for shutdown := false; !shutdown; {
		select {
		case <-globals.stopChan:
			shutdown = true
			logger.Infof("trackedlock lock watcher shutting down")
			// fall through and perform one last check

		case <-globals.lockCheckChan:
			// fall through and perform checks
		}

		// Fetch globals.lockHoldTimeLimit so the race detector doesn't
		// complain about it ...
		lockHoldTimeLimit := atomic.LoadInt64(&globals.lockHoldTimeLimit)

		// LongLockHolders holds information about locks for locks that have
		// been held longer then globals.lockHoldTimeLimit, upto a maximum of
		// globals.lockWatcherLocksLogged locks.  They are sorted longest to
		// shortest.
		//
		// longestDuration is the minimum lock hold time needed to be added to the
		// slice, which increases once the slice is full.
		//
		// Note that this could be running after lock tracking or the lock watcher
		// has been disabled (globals.lockHoldTimeLimit and/or
		// globals.lockCheckPeriod could be 0).
		var (
			longLockHolders = make([]*longLockHolder, 0)
			longestDuration = time.Duration(lockHoldTimeLimit)
		)
		if lockHoldTimeLimit == 0 {
			// ignore locks held less than 10 years
			longestDuration, _ = time.ParseDuration("24h")
			longestDuration *= 365 * 10
		}

		// now does not change during a loop iteration
		now := time.Now()

		// See if any Mutex has been held longer then the limit and, if
		// so, find the one held the longest.
		//
		// Go guarantees that looking at mutex.tracker.lockTime is safe even if
		// we don't hold the mutex (looking at tracker.lockCnt is a benign race
		// but LoadInt32 is needed to make the go race detector happy).
		globals.mapMutex.Lock()
		for mt, lockPtr := range globals.mutexMap {

			// If the lock is not locked then skip it; if it has been idle
			// for the lockCheckPeriod then drop it from the locks being
			// watched.
			//
			// Note that this is the only goroutine that deletes locks from
			// globals.mutexMap so any mt pointer we save after this point
			// will continue to be valid until the next iteration.
			lockCnt := atomic.LoadInt32(&mt.lockCnt)
			if lockCnt == 0 {
				lastLocked := now.Sub(mt.lockTime)
				if lastLocked >= globals.lockCheckPeriod {
					mt.isWatched = false
					delete(globals.mutexMap, mt)
				}
				continue
			}

			lockedDuration := now.Sub(mt.lockTime)
			if lockedDuration > longestDuration {

				// We do not hold a lock that prevents mt.lockStack.stackTrace
				// from changing if the Mutex were to be unlocked right now,
				// although its unlikely since the mutex has been held so long,
				// so try to copy the stack robustly.  If it does change it
				// might be reused and now hold another stack and this stack
				// trace would be wrong.
				var (
					mtStackTraceObj *stackTraceObj = mt.lockStack
					mtStack         string
				)
				if mt.lockStack != nil {
					mtStack = string(mtStackTraceObj.stackTrace)
				}
				longHolder := &longLockHolder{
					lockPtr:      lockPtr,
					lockTime:     mt.lockTime,
					lockerGoId:   mt.lockerGoId,
					lockStackStr: mtStack,
					lockOp:       "Lock()",
					mutexTrack:   mt,
				}
				longLockHolders = recordLongLockHolder(longLockHolders, longHolder)

				// if we've hit the maximum number of locks then bump
				// longestDuration
				if len(longLockHolders) == globals.lockWatcherLocksLogged {
					longestDuration = lockedDuration
				}
			}
		}
		globals.mapMutex.Unlock()

		// give other routines a chance to get the global lock
		time.Sleep(10 * time.Millisecond)

		// see if any RWMutex has been held longer then the limit and, if
		// so, find the one held the longest.
		//
		// looking at the rwMutex.tracker.* fields is safe per the
		// argument for Mutex, but looking at rTracker maps requires we
		// get rtracker.sharedStateLock for each lock.
		globals.mapMutex.Lock()
		for rwmt, lockPtr := range globals.rwMutexMap {

			// If the lock is not locked then skip it.  If it has been
			// idle for the lockCheckPeriod (rwmt.tracker.lockTime is
			// updated when the lock is locked shared or exclusive) then
			// drop it from the locks being watched.
			lockCnt := atomic.LoadInt32(&rwmt.tracker.lockCnt)
			if lockCnt == 0 {
				lastLocked := now.Sub(rwmt.tracker.lockTime)
				if lastLocked >= globals.lockCheckPeriod {
					rwmt.tracker.isWatched = false
					delete(globals.rwMutexMap, rwmt)
				}
				continue
			}

			if lockCnt < 0 {
				lockedDuration := now.Sub(rwmt.tracker.lockTime)
				if lockedDuration > longestDuration {

					// We do not hold a lock that prevents
					// rwmt.lockStack.stackTrace from changing if the
					// RWMutex were to be unlocked right now, although its
					// unlikely since the rwmutex has been held so long, so
					// try to copy the stack robustly.  If it does change i
					// it might be reused and now hold another stack.
					// might be reused and now hold another stack and this
					// stack trace would be wrong.

					var (
						rwmtStackTraceObj *stackTraceObj = rwmt.tracker.lockStack
						rwmtStack         string
					)
					if rwmt.tracker.lockStack != nil {
						rwmtStack = string(rwmtStackTraceObj.stackTrace)
					}
					longHolder := &longLockHolder{
						lockPtr:      lockPtr,
						lockTime:     rwmt.tracker.lockTime,
						lockerGoId:   rwmt.tracker.lockerGoId,
						lockStackStr: rwmtStack,
						lockOp:       "Lock()",
						rwMutexTrack: rwmt,
					}
					longLockHolders = recordLongLockHolder(longLockHolders, longHolder)
					if len(longLockHolders) == globals.lockWatcherLocksLogged {
						longestDuration = lockedDuration
					}
				}

				continue
			}

			rwmt.sharedStateLock.Lock()

			for goId, lockTime := range rwmt.rLockTime {
				lockedDuration := now.Sub(lockTime)
				if lockedDuration > longestDuration {

					// we do hold a lock that protects rwmt.rLockStack
					longHolder := &longLockHolder{
						lockPtr:      lockPtr,
						lockTime:     lockTime,
						lockerGoId:   goId,
						lockStackStr: string(rwmt.rLockStack[goId].stackTrace),
						lockOp:       "RLock()",
						rwMutexTrack: rwmt,
					}
					longLockHolders = recordLongLockHolder(longLockHolders, longHolder)
					if len(longLockHolders) == globals.lockWatcherLocksLogged {
						longestDuration = lockedDuration
					}
				}
			}
			rwmt.sharedStateLock.Unlock()
		}
		globals.mapMutex.Unlock()

		if len(longLockHolders) == 0 {
			// nothing to see!  move along!
			continue
		}

		// log a Warning for each lock that has been held too long,
		// from longest to shortest
		for i := 0; i < len(longLockHolders); i += 1 {
			logger.Warnf("trackedlock watcher: %T at %p locked for %f sec rank %d;"+
				" stack at call to %s:\n%s\n",
				longLockHolders[i].lockPtr, longLockHolders[i].lockPtr,
				float64(now.Sub(longLockHolders[i].lockTime))/float64(time.Second), i,
				longLockHolders[i].lockOp, longLockHolders[i].lockStackStr)

		}
	}

	globals.doneChan <- struct{}{}
}
