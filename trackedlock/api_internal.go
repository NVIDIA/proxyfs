package trackedlock

import (
	"runtime"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/utils"
)

type globalsStruct struct {
	mapMutex          sync.Mutex                    // protects mutexMap and rwMutexMap
	mutexMap          map[*MutexTrack]interface{}   // the Mutex like locks  being watched
	rwMutexMap        map[*RWMutexTrack]interface{} // the RWMutex like locks being watched
	lockHoldTimeLimit time.Duration                 // locks held longer then this get logged
	lockCheckPeriod   time.Duration                 // check locks once each period
	lockCheckChan     <-chan time.Time              // wait here to check on locks
	stopChan          chan struct{}                 // time to shutdown and go home
	doneChan          chan struct{}                 // shutdown complete
	lockCheckTicker   *time.Ticker                  // ticker for lock check time
	dlmRWLockType     interface{}                   // set by a callback from the DLM code
}

var globals globalsStruct

// stackTraceSlice holds the stack trace of one thread.  stackTraceBuf is the
// storage required to hold one stack trace. We keep a pool of them around.
//
type stackTraceSlice []byte
type stackTraceBuf [4040]byte

type stackTraceObj struct {
	stackTrace    stackTraceSlice // stack trace of curent or last locker
	stackTraceBuf stackTraceBuf   // storage for stack trace slice
}

var stackTraceObjPool = sync.Pool{
	New: func() interface{} {
		return &stackTraceObj{}
	},
}

// Track a Mutex or RWMutex held in exclusive mode (lockCnt is also used to
// track the number of shared lockers)
//
type MutexTrack struct {
	isWatched  bool           // true if lock is on list of checked mutexes
	lockCnt    int            // 0 if unlocked, -1 locked exclusive, > 0 locked shared
	lockTime   time.Time      // time last lock operation completed
	lockerGoId uint64         // goroutine ID of the last locker
	lockStack  *stackTraceObj // stack trace of locker while locked
}

// Track an RWMutex
//
type RWMutexTrack struct {
	tracker         MutexTrack                // tracking info when the lock is held exclusive
	sharedStateLock sync.Mutex                // lock the following fields (shared mode state)
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

	// if lock tracking is disabled, just get the lock and record the
	// current time (time.Now() is kind've expensive, but may still be worth
	// getting)
	if globals.lockHoldTimeLimit == 0 {
		mt.lockTime = time.Now()
		mt.lockCnt = -1
		return
	}

	mt.lockStack = stackTraceObjPool.Get().(*stackTraceObj)
	mt.lockStack.stackTrace = mt.lockStack.stackTraceBuf[:]

	cnt := runtime.Stack(mt.lockStack.stackTrace, false)
	mt.lockStack.stackTrace = mt.lockStack.stackTrace[0:cnt]
	mt.lockerGoId = utils.StackTraceToGoId(mt.lockStack.stackTrace)
	mt.lockTime = time.Now()
	mt.lockCnt = -1

	// add to the list of watched mutexes if anybody is watching -- its only
	// at this point that we need to know if this a Mutex or RWMutex, and it
	// only happens once
	if !mt.isWatched && globals.lockCheckPeriod != 0 && globals.lockHoldTimeLimit != 0 {
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

	// if we're checking the lock hold time then check it
	if globals.lockHoldTimeLimit != 0 {
		now := time.Now()
		if now.Sub(mt.lockTime) >= globals.lockHoldTimeLimit {

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
			logger.Warnf("Unlock(): %T at %p locked for %d sec; stack at Lock() call:\n%s stack at Unlock():\n%s",
				wrappedLock, wrappedLock,
				now.Sub(mt.lockTime)/time.Second, lockStr, unlockStr)
		}
	}

	// release the lock
	mt.lockCnt = 0
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

	// if lock tracking is disabled, just get the lock and record the
	// current time (time.Now() is kind've expensive, still probably
	// worthwhile.
	//
	// Note that when lock tracking is disabled, mt.lockTime is being
	// updated when the locks is acquired shared and mt.lockCnt is not being
	// updated.
	if globals.lockHoldTimeLimit == 0 {
		rwmt.sharedStateLock.Lock()
		rwmt.tracker.lockCnt += 1
		rwmt.sharedStateLock.Unlock()

		rwmt.tracker.lockTime = time.Now()
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
	// insure that no other goroutine is changing rwmt.tracker.lockCnt.
	rwmt.sharedStateLock.Lock()

	if rwmt.rLockStack == nil {
		rwmt.rLockStack = make(map[uint64]*stackTraceObj)
		rwmt.rLockTime = make(map[uint64]time.Time)
	}
	rwmt.tracker.lockTime = time.Now()
	rwmt.rLockTime[goId] = rwmt.tracker.lockTime
	rwmt.rLockStack[goId] = lockStack
	rwmt.tracker.lockCnt += 1

	// add to the list of watched mutexes if anybody is watching
	if !rwmt.tracker.isWatched && globals.lockCheckPeriod != 0 && globals.lockHoldTimeLimit != 0 {

		globals.mapMutex.Lock()
		globals.rwMutexMap[rwmt] = wrappedLock
		globals.mapMutex.Unlock()
		rwmt.tracker.isWatched = true
	}
	rwmt.sharedStateLock.Unlock()

	return
}

func (rwmt *RWMutexTrack) rUnlockTrack(wrappedLock interface{}) {

	if globals.lockHoldTimeLimit == 0 {
		rwmt.sharedStateLock.Lock()

		// if lock hold time is not being checked, discard info left
		// over from an earlier time when it was
		if rwmt.rLockStack != nil {
			for goId, lockStack := range rwmt.rLockStack {
				stackTraceObjPool.Put(lockStack)
				delete(rwmt.rLockStack, goId)
				delete(rwmt.rLockTime, goId)
			}
			rwmt.rLockStack = nil
			rwmt.rLockTime = nil
		}
		rwmt.tracker.lockCnt -= 1

		// if lock hold time was being checked, discard that information
		if rwmt.rLockStack != nil {
			for goId, lockStack := range rwmt.rLockStack {
				stackTraceObjPool.Put(lockStack)
				delete(rwmt.rLockStack, goId)
				delete(rwmt.rLockTime, goId)
			}
			rwmt.rLockStack = nil
			rwmt.rLockTime = nil
		}
		rwmt.sharedStateLock.Unlock()
		return
	}

	// If the goroutine unlocking a shared lock is not the same as the one
	// that locked it then we cannot match up RLock() and RUnlock()
	// operations (unless we add an API that allows lock "ownership" to be
	// passed between goroutines and the client of the API actually calls
	// it). This is complicated by the situation when lock tracking
	// (globals.lockHoldTimeLimit > 0) is enabled after some locks have
	// already been acquired.
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
	//
	// This also leads to a performance optimization we fetch the goId from
	// the map instead of calling utils.GetGoID(), which gets a stack trace.
	//
	var goId uint64
	if rwmt.tracker.lockCnt == 1 && len(rwmt.rLockTime) == 1 {
		for goId, _ = range rwmt.rLockTime {
			break
		}
	} else {
		goId = utils.GetGoId()
	}

	now := time.Now()
	rwmt.sharedStateLock.Lock()
	rLockTime, ok := rwmt.rLockTime[goId]
	if ok && now.Sub(rLockTime) >= globals.lockHoldTimeLimit {

		var buf stackTraceBuf
		stackTrace := buf[:]
		cnt := runtime.Stack(stackTrace, false)
		stackTrace = stackTrace[0:cnt]

		logger.Warnf(
			"RUnlock(): %T at %p locked for %d sec; stack at RLock() call:\n%s stack at RUnlock():\n%s",
			wrappedLock, wrappedLock, now.Sub(rLockTime)/time.Second,
			rwmt.rLockStack[goId].stackTrace, string(stackTrace))
	}

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
	rwmt.tracker.lockCnt -= 1
	rwmt.sharedStateLock.Unlock()

	return
}

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

		var (
			longestMT       *MutexTrack
			longestRWMT     *RWMutexTrack
			longestDuration time.Duration
			longestGoId     uint64
		)
		now := time.Now()

		// see if any Mutex has been held longer then the limit and, if
		// so, find the one held the longest.
		//
		// looking at mutex.tracker.lockTime is safe even if we don't hold the
		// mutex (in practice, looking at mutex.tracker.lockCnt should be safe
		// as well, though go doesn't guarantee that).
		globals.mapMutex.Lock()
		for mt, _ := range globals.mutexMap {

			// If the lock is not locked then skip it If it has been idle
			// for the lockCheckPeriod then drop it from the locks being
			// watched.
			if mt.lockCnt == 0 {
				lastLocked := now.Sub(mt.lockTime)
				if lastLocked >= globals.lockCheckPeriod {
					mt.isWatched = false
					delete(globals.mutexMap, mt)
				}
				continue
			}
			lockedDuration := now.Sub(mt.lockTime)
			if lockedDuration >= globals.lockHoldTimeLimit && lockedDuration > longestDuration {
				longestMT = mt
				longestDuration = lockedDuration
			}
		}
		globals.mapMutex.Unlock()

		// give other routines a chance to get the lock
		time.Sleep(10 * time.Millisecond)

		// see if any RWMutex has been held longer then the limit and, if
		// so, find the one held the longest.
		//
		// looking at the rwMutex.tracker.* fields is safe per the
		// argument for Mutex, but looking at rTracker maps requires we
		// get rtracker.sharedStateLock for each lock.
		globals.mapMutex.Lock()
		for rwmt, _ := range globals.rwMutexMap {

			// If the lock is not locked then skip it.  If it has been
			// idle for the lockCheckPeriod (rwmt.tracker.lockTime is
			// updated when the lock is locked shared or exclusive) then
			// drop it from the locks being watched.
			if rwmt.tracker.lockCnt == 0 {
				lastLocked := now.Sub(rwmt.tracker.lockTime)
				if lastLocked >= globals.lockCheckPeriod {
					rwmt.tracker.isWatched = false
					delete(globals.rwMutexMap, rwmt)
				}
				continue
			}

			if rwmt.tracker.lockCnt < 0 {
				lockedDuration := now.Sub(rwmt.tracker.lockTime)
				if lockedDuration >= globals.lockHoldTimeLimit && lockedDuration > longestDuration {
					longestMT = nil
					longestRWMT = rwmt
					longestDuration = lockedDuration
				}
				continue
			}

			rwmt.sharedStateLock.Lock()

			for goId, lockTime := range rwmt.rLockTime {
				lockedDuration := now.Sub(lockTime)
				if lockedDuration >= globals.lockHoldTimeLimit && lockedDuration > longestDuration {
					longestMT = nil
					longestRWMT = rwmt
					longestDuration = lockedDuration
					longestGoId = goId
				}
			}
			rwmt.sharedStateLock.Unlock()
		}
		globals.mapMutex.Unlock()

		if longestMT == nil && longestRWMT == nil {
			continue
		}

		// figure out the call type, stack trace, etc.
		var (
			lockCall      string
			stackTraceStr string
			lockPtr       interface{}
		)

		// looking at tracker.lockStackTrace is not strictly
		// safe, so let's hope for the best
		//
		// this should really copy the values and then verify
		// the lock is still locked before printing
		switch {
		case longestMT != nil:
			lockCall = "Lock()"
			stackTraceStr = string(longestMT.lockStack.stackTrace)
			lockPtr = globals.mutexMap[longestMT]
		case longestGoId == 0:
			lockCall = "Lock()"
			stackTraceStr = string(longestRWMT.tracker.lockStack.stackTrace)
			lockPtr = globals.rwMutexMap[longestRWMT]
		default:
			lockCall = "RLock()"
			stackTraceStr = string(longestRWMT.rLockStack[longestGoId].stackTrace)
			lockPtr = globals.rwMutexMap[longestRWMT]
		}
		logger.Warnf("trackedlock watcher: %T at %p locked for %d sec; stack at %s call:\n%s",
			lockPtr, lockPtr, uint64(longestDuration/time.Second), lockCall, stackTraceStr)
	}

	globals.doneChan <- struct{}{}
}
