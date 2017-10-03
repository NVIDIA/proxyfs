package trackedlock

import (
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
)

func parseConfMap(confMap conf.ConfMap) (err error) {

	globals.lockHoldTimeLimit, err = confMap.FetchOptionValueDuration("TrackedLock", "LockHoldTimeLimit")
	if err != nil {
		logger.Warnf("config variable 'TrackedLock.LockHoldTimeLImit' defaulting to '0s': %v", err)
		globals.lockHoldTimeLimit = time.Duration(0 * time.Second)
	}

	// lockHoldTimeLimit must be >= 1 sec or 0
	if globals.lockHoldTimeLimit < time.Second && globals.lockHoldTimeLimit != 0 {
		logger.Warnf("config variable 'TrackedLock.LockHoldTimeLImit' value less then 1 sec; defaulting to '40s'")
		globals.lockHoldTimeLimit = time.Duration(40 * time.Second)
	}

	globals.lockCheckPeriod, err = confMap.FetchOptionValueDuration("TrackedLock", "LockCheckPeriod")
	if err != nil {
		logger.Warnf("config variable 'TrackedLock.LockCheckPeriod' defaulting to '0s': %v", err)
		globals.lockCheckPeriod = time.Duration(0 * time.Second)
	}

	// lockCheckPeriod must be >= 1 sec or 0
	if globals.lockCheckPeriod < time.Second && globals.lockCheckPeriod != 0 {
		logger.Warnf("config variable 'TrackedLock.LockCheckPeriod' value less then 1 sec; defaulting to '20s'")
		globals.lockCheckPeriod = time.Duration(20 * time.Second)
	}

	// log information upto 16 locks
	globals.lockWatcherLocksLogged = 16

	err = nil
	return
}

// Up() initializes the package.  It must be called and successfully return
// before locks will be tracked.  Locks can still be used before it is called
// but tracking will not start until the first Lock() call after the package is
// initialized.
//
func Up(confMap conf.ConfMap) (err error) {

	err = parseConfMap(confMap)
	if err != nil {
		// parseConfMap() has logged an error
		return
	}
	logger.Infof("trackedlock.Up(): LockHoldTimeLimit %d sec  LockCheckPeriod %d sec",
		globals.lockHoldTimeLimit/time.Second, globals.lockCheckPeriod/time.Second)

	globals.mutexMap = make(map[*MutexTrack]interface{}, 128)
	globals.rwMutexMap = make(map[*RWMutexTrack]interface{}, 128)
	globals.stopChan = make(chan struct{})
	globals.doneChan = make(chan struct{})

	// if the lock checker is disabled or there's no time limit then
	// there's no need to start the watcher
	if globals.lockCheckPeriod == 0 || globals.lockHoldTimeLimit == 0 {
		return
	}

	// watch the locks to see if they are held too long
	globals.lockCheckTicker = time.NewTicker(globals.lockCheckPeriod)
	globals.lockCheckChan = globals.lockCheckTicker.C
	go lockWatcher()

	return
}

func Down() (err error) {
	// shutdown lock tracker
	logger.Infof("tracklock.Down() called")
	if globals.lockCheckTicker != nil {
		globals.lockCheckTicker.Stop()
		globals.lockCheckTicker = nil
		globals.stopChan <- struct{}{}
		_ = <-globals.doneChan
	}

	// err is already nil
	return
}

// PauseAndContract does nothing (locks continue to be tracked)
func PauseAndContract(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

// ExpandAndResume applies any additions from the supplied confMap
func ExpandAndResume(confMap conf.ConfMap) (err error) {

	// read the new confmap; if the log period has changed or there was an
	// error shutdown the old logger prior to starting a new one
	oldCheckPeriod := globals.lockCheckPeriod
	oldTimeLimit := globals.lockHoldTimeLimit
	err = parseConfMap(confMap)
	if err != nil {
		logger.ErrorWithError(err, "cannot parse confMap")
		if oldCheckPeriod != 0 {
			globals.stopChan <- struct{}{}
			_ = <-globals.doneChan
		}
		return
	}

	// if no change required, just return
	if globals.lockCheckPeriod == oldCheckPeriod && globals.lockHoldTimeLimit == oldTimeLimit {
		return
	}

	logger.Infof("trackedlock lock hold time limit/lock check period changing from %d/%d sec to %d/%d sec",
		oldTimeLimit/time.Second, oldCheckPeriod/time.Second,
		globals.lockHoldTimeLimit/time.Second, globals.lockCheckPeriod/time.Second)

	// shutdown the old watcher (if any) and start a new one (if any)
	if oldCheckPeriod != 0 {
		globals.lockCheckTicker.Stop()
		globals.lockCheckTicker = nil
		globals.stopChan <- struct{}{}
		_ = <-globals.doneChan

		// if we're going to stop watching, clean out the map
		if globals.lockCheckPeriod == 0 {
			for key, _ := range globals.mutexMap {
				delete(globals.mutexMap, key)
			}
			for key, _ := range globals.rwMutexMap {
				delete(globals.rwMutexMap, key)
			}
		}
	}

	if globals.lockCheckPeriod == 0 || globals.lockHoldTimeLimit == 0 {
		return
	}

	globals.lockCheckTicker = time.NewTicker(globals.lockCheckPeriod)
	globals.lockCheckChan = globals.lockCheckTicker.C
	go lockWatcher()

	return
}
