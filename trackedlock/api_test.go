// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package trackedlock

/*
 * Test tracked locks.
 *
 * Most of this file is copied from statslogger/config_test.go because its a
 * good place to start.
 */

import (
	"fmt"
	"regexp"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
)

// Common configuration for all tests (unless overridden)
//
var (
	confStrings = []string{
		"TrackedLock.LockHoldTimeLimit=2s",
		"TrackedLock.LockCheckPeriod=1s",

		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",

		"Logging.LogFilePath=/dev/null",
		"Logging.LogToConsole=false",
	}

	// matches: "trackedlock watcher: *trackedlock.Mutex at 0xc420110000 locked for 2.003s sec rank 0; stack at call to Lock():\ngoroutine 19 [running]:..."
	// that is it only matches the entry for rank 0 (the longest held lock)
	//
	watcherRank0LogMatch = `^trackedlock watcher: (?P<type>[*a-zA-Z0-9_.]+) at (?P<ptr>0x[0-9a-f]+) locked for (?P<time>[0-9.]+) sec rank 0; stack at call to (?P<locker>[a-zA-Z0-9_()]+):\\n(?P<lockStack>.*)$`
	watcherRank1LogMatch = `^trackedlock watcher: (?P<type>[*a-zA-Z0-9_.]+) at (?P<ptr>0x[0-9a-f]+) locked for (?P<time>[0-9.]+) sec rank 1; stack at call to (?P<locker>[a-zA-Z0-9_()]+):\\n(?P<lockStack>.*)$`
	watcherRank2LogMatch = `^trackedlock watcher: (?P<type>[*a-zA-Z0-9_.]+) at (?P<ptr>0x[0-9a-f]+) locked for (?P<time>[0-9.]+) sec rank 2; stack at call to (?P<locker>[a-zA-Z0-9_()]+):\\n(?P<lockStack>.*)$`
	watcherRank3LogMatch = `^trackedlock watcher: (?P<type>[*a-zA-Z0-9_.]+) at (?P<ptr>0x[0-9a-f]+) locked for (?P<time>[0-9.]+) sec rank 3; stack at call to (?P<locker>[a-zA-Z0-9_()]+):\\n(?P<lockStack>.*)$`

	// matches: "Unlock(): *trackedlock.Mutex at 0xc420110000 locked for 3.003 sec; stack at call to Lock():\ngoroutine 19 [running]:\n...\nstack at Unlock():\ngoroutine 19 [running]:\n..."
	//
	unlockLogMatch = `^Unlock\(\): (?P<type>[*a-zA-Z0-9_.]+) at (?P<ptr>0x[0-9a-f]+) locked for (?P<time>[0-9.]+) sec; stack at call to (?P<locker>[a-zA-Z0-9_()]+):\\n(?P<lockStack>.*)\\nstack at Unlock\(\):\\n(?P<unlockStack>.*)$`

	// matches: "RUnlock(): *trackedlock.RWMutex at 0xc420100000 locked for 5.000001 sec; stack at call to RLock():\ngoroutine 5 [running]:\n...\nstack at RUnlock():\ngoroutine 5 [running]:\n..."
	//
	rUnlockLogMatch = `^RUnlock\(\): (?P<type>[*a-zA-Z0-9_.]+) at (?P<ptr>0x[0-9a-f]+) locked for (?P<time>[0-9.]+) sec; stack at call to (?P<locker>[a-zA-Z0-9_()]+):\\n(?P<lockStack>.*)\\nstack at RUnlock\(\):\\n(?P<unlockStack>.*)$`

	watcherRank0LogRE = regexp.MustCompile(watcherRank0LogMatch)
	watcherRank1LogRE = regexp.MustCompile(watcherRank1LogMatch)
	watcherRank2LogRE = regexp.MustCompile(watcherRank2LogMatch)
	watcherRank3LogRE = regexp.MustCompile(watcherRank3LogMatch)
	unlockLogRE       = regexp.MustCompile(unlockLogMatch)
	rUnlockLogRE      = regexp.MustCompile(rUnlockLogMatch)
)

// sleep for the requested number of seconds
//
func sleep(sec float64) {
	var usec int64 = int64(sec * 1000 * 1000)
	delay := time.Duration(usec) * time.Microsecond
	time.Sleep(delay)
}

// basic tests of the API
//
func TestAPI(t *testing.T) {
	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Startup packages involved
	err = logger.Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("logger.Up(confMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	err = globals.Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("Up() failed: %v", err)
		t.Fatalf(tErr)
	}

	// validate packge config variables
	if time.Duration(atomic.LoadInt64(&globals.lockHoldTimeLimit)) != 2*time.Second {
		t.Fatalf("after Up() globals.lockHoldTimeLimi != 2 sec")
	}

	// get a copy of what's written to the log
	var logcopy logger.LogTarget
	logcopy.Init(100)
	logger.AddLogTarget(logcopy)

	// basic tests
	testMutexes(t, logcopy, confMap)

	// Shutdown packages, starting with this one
	err = globals.Down(confMap)
	if nil != err {
		t.Fatalf("TestAPI(): Down() failed: %v", err)
	}

	// skip shutdown of logger (until last test case finishes)
}

// Test trackedlock for Mutex and RWMutex
//
func testMutexes(t *testing.T, logcopy logger.LogTarget, confMap conf.ConfMap) {
	var (
		testMutex1   Mutex
		testMutex2   Mutex
		testMutex3   Mutex
		testRWMutex1 RWMutex
		testRWMutex2 RWMutex
	)

	// verify we can lock and unlock some locks
	testMutex1.Lock()
	testMutex2.Lock()
	testRWMutex1.Lock()
	testRWMutex2.RLock()
	testRWMutex2.RUnlock()
	testRWMutex1.Unlock()
	testMutex2.Unlock()
	testMutex1.Unlock()

	// lock several locks for more than 3 sec; lock watcher should report
	// the one held the longest (the final sleep is 2.8 sec because the max
	// hold time is 2 sec and the watcher runs every 1 sec).  3.0 sec is
	// needed to insure that watcher has a chance to see the first lock but
	// add an extra 0.2 sec to it has run and its output is visible, i.e.
	// 0.1 + 0.1 + 0.1 + 0.1 + 2.8 = 3.2.
	testMutex1.Lock()
	sleep(0.1)
	testMutex2.Lock()
	sleep(0.1)
	testRWMutex1.RLock()
	sleep(0.1)
	testRWMutex2.Lock()
	sleep(0.1)
	testMutex3.Lock()
	sleep(2.8)

	/*
	 * Verify that lock watcher identifies the locks that have been held too long
	 * and sorts them correctly.
	 */

	// lockwatcher should have identified testMutex1 as the longest held mutex
	fields, _, err := logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank0LogRE, 10)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for lockWatcher for testMutex1: %s", err.Error())
	}
	ptr, _ := strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testMutex1)) {
		t.Errorf("testMutex: testMutex1 at %p is not the longest held Mutex", &testMutex1)
	}
	if fields["type"] != "*trackedlock.Mutex" {
		t.Errorf("testMutex: testMutex1 type reported as '%s' instead of '*trackedlock.Mutex'", fields["type"])
	}

	// lockwatcher should have identified testMutex2 as the second longest held mutex
	fields, _, err = logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank1LogRE, 10)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for lockWatcher for testMutex2: %s", err.Error())
	}
	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testMutex2)) {
		t.Errorf("testMutex: testMutex2 at %p is not the longest held Mutex", &testMutex2)
	}
	if fields["type"] != "*trackedlock.Mutex" {
		t.Errorf("testMutex: testMutex2 type reported as '%s' instead of '*trackedlock.Mutex'", fields["type"])
	}

	// lockwatcher should have identified testRWMutex1 as the third longest held mutex
	fields, _, err = logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank2LogRE, 10)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for lockWatcher for testRWMutex1: %s", err.Error())
	}
	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testRWMutex1)) {
		t.Errorf("testMutex: testRWMutex1 at %p is not the longest held Mutex", &testRWMutex1)
	}
	if fields["type"] != "*trackedlock.RWMutex" {
		t.Errorf("testMutex: testRWMutex1 type reported as '%s' instead of '*trackedlock.RWMutex'", fields["type"])
	}

	// lockwatcher should have identified testRWMutex2 as the third longest held mutex
	fields, _, err = logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank3LogRE, 10)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for lockWatcher for testRWMutex2: %s", err.Error())
	}
	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testRWMutex2)) {
		t.Errorf("testMutex: testRWMutex2 at %p is not the longest held Mutex", &testRWMutex2)
	}
	if fields["type"] != "*trackedlock.RWMutex" {
		t.Errorf("testMutex: testRWMutex2 type reported as '%s' instead of '*trackedlock.RWMutex'", fields["type"])
	}

	/*
	 * Verify that releasing locks changes lockWatcher() results
	 */

	// release testMutex1 and look for the unlock message
	testMutex1.Unlock()
	sleep(0.1)
	fields, _, err = logger.ParseLogForFunc(logcopy, "unlockTrack", unlockLogRE, 3)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for Mutex Unlock(): %s", err.Error())
	}

	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testMutex1)) {
		t.Errorf("testMutex: testMutex1 at %p was not the mutex unlocked", &testMutex1)
	}
	if fields["type"] != "*trackedlock.Mutex" {
		t.Errorf("testMutex: testMutex1 type reported as '%s' instead of '*trackedlock.Mutex'", fields["type"])
	}

	// after releasing testsMutex1 and waiting for the lockwatcher, it
	// should now identify testMutex2 as the longest held mutex
	sleep(1)
	fields, _, err = logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank0LogRE, 10)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for lockWatcher for testMutex2 at %p: %s",
			&testMutex2, err.Error())
	}

	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testMutex2)) {
		t.Errorf("testMutex: testMutex2 at %p is not the longest held Mutex", &testMutex2)
	}
	if fields["type"] != "*trackedlock.Mutex" {
		t.Errorf("testMutex: testMutex2 type reported as '%s' instead of '*trackedlock.Mutex'", fields["type"])
	}

	// release testMutex2 and look for testRWMutex1 as the longest held
	// RWMutex (in shared mode)
	testMutex2.Unlock()
	sleep(1.1)
	fields, _, err = logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank0LogRE, 3)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for lockWatcher for testRWMutex1: %s", err.Error())
	}

	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testRWMutex1)) {
		t.Errorf("testMutex: testRWMutex1 at %p is not the longest held Mutex", &testRWMutex1)
	}
	if fields["locker"] != "RLock()" {
		t.Errorf("testMutex: locker '%s' for testRWMutex1 is not 'Rlock()': %s",
			fields["locker"], fields["msg"])
	}
	if fields["type"] != "*trackedlock.RWMutex" {
		t.Errorf("testMutex: testRWMutex1 type reported as '%s' instead of '*trackedlock.RWMutex'",
			fields["type"])
	}

	// release testRWMutex1 and look for the log message about it
	testRWMutex1.RUnlock()
	sleep(0.1)
	fields, _, err = logger.ParseLogForFunc(logcopy, "rUnlockTrack", rUnlockLogRE, 3)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for testRWMutex1.RUnlock(): %s", err.Error())
	}
	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testRWMutex1)) {
		t.Errorf("testMutex: testRWMutex1 at %p was not the mutex unlocked", &testRWMutex1)
	}
	if fields["type"] != "*trackedlock.RWMutex" {
		t.Errorf("testMutex: testRWMutex1 type reported as '%s' instead of '*trackedlock.RWMutex'",
			fields["type"])
	}

	// look for testRWMutex2 as the longest held RWMutex
	sleep(1.0)
	fields, _, err = logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank0LogRE, 3)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for lockWatcher for testRWMutex2: %s", err.Error())
	}

	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testRWMutex2)) {
		t.Errorf("testMutex: testRWMutex2 at %p is not the longest held Mutex", &testRWMutex2)
	}
	if fields["locker"] != "Lock()" {
		t.Errorf("testMutex: locker '%s' for testRWMutex2 is not 'Rlock()': %s",
			fields["locker"], fields["msg"])
	}
	if fields["type"] != "*trackedlock.RWMutex" {
		t.Errorf("testMutex: testRWMutex2 type reported as '%s' instead of '*trackedlock.RWMutex'",
			fields["type"])
	}

	// release testRWMutex2 and look for the log message about it
	testRWMutex2.Unlock()
	sleep(0.1)
	fields, _, err = logger.ParseLogForFunc(logcopy, "unlockTrack", unlockLogRE, 3)
	if err != nil {
		t.Errorf("testMutex: could not find log entry for testRWMutex2.Unlock(): %s", err.Error())
	}

	ptr, _ = strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&testRWMutex2)) {
		t.Errorf("testMutex: testRWMutex2 at %p was not the mutex unlocked", &testRWMutex2)
	}
	if fields["type"] != "*trackedlock.RWMutex" {
		t.Errorf("testMutex: testRWMutex2 type reported as '%s' instead of '*trackedlock.RWMutex'",
			fields["type"])
	}

	// release the last mutex but don't bother checking the messages
	testMutex3.Unlock()
}

func TestStartupShutdown(t *testing.T) {
	// this succeeded the first time or we wouldn't be here, so don't check
	// for errors
	confMap, _ := conf.MakeConfMapFromStrings(confStrings)

	// startup of logger was already done

	// the locks we play with
	var (
		mutex1   Mutex
		rwMutex1 RWMutex
		rwMutex2 RWMutex
		err      error
	)

	// get and release some locks before lock tracking is Up()
	mutex1.Lock()
	rwMutex1.Lock()
	rwMutex2.RLock()

	mutex1.Unlock()
	rwMutex1.Unlock()
	rwMutex2.RUnlock()

	// second test -- get some locks before lock tracking is initialized and
	// then call Up(); test passes if nothing bad happens when they are
	// released after Up() returns
	mutex1.Lock()
	rwMutex1.Lock()
	rwMutex2.RLock()

	err = globals.Up(confMap)
	if err != nil {
		tErr := fmt.Sprintf("TestStartupShutdown(): Up() failed: %v", err)
		t.Fatalf(tErr)
	}

	// release the locks acquired before this package was "Up()"
	mutex1.Unlock()
	rwMutex1.Unlock()
	rwMutex2.RUnlock()

	// third test -- get some locks after lock tracking is initialized and
	// then call Down() for it; test passes if nothing bad happens when they
	// are released after Down() returns
	mutex1.Lock()
	rwMutex1.Lock()
	rwMutex2.RLock()

	// Shutdown this package
	err = globals.Down(confMap)
	if nil != err {
		t.Fatalf("Down() failed: %v", err)
	}

	// release the locks acquired before this package was "Up()"
	mutex1.Unlock()
	rwMutex1.Unlock()
	rwMutex2.RUnlock()

	// fourth test -- get and release locks even though lock tracking is down
	mutex1.Lock()
	rwMutex1.Lock()
	rwMutex2.RLock()

	mutex1.Unlock()
	rwMutex1.Unlock()
	rwMutex2.RUnlock()

	// skip shutdown of logger (until last test case)
}

// Change the lock tracking times to the duration values values specified.
//
// A duration of 0 sec for lockCheckPeriod disables the lock watcher and a
// duration of 0 sec for lockHoldTimeLimit disables both the lock watcher and
// lock tracking.
//
func updateTrackingState(t *testing.T, lockHoldTimeLimit string, lockCheckPeriod string) {

	// build the config to move to
	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("conf.MakeConfMapFromStrings failed: %v", err)
	}
	err = confMap.UpdateFromString("TrackedLock.LockCheckPeriod=" + lockCheckPeriod)
	if err != nil {
		t.Fatalf("UpdateFromString('TrackedLock.LockCheckPeriod=%s') failed: %v", lockCheckPeriod, err)
	}
	err = confMap.UpdateFromString("TrackedLock.LockHoldTimeLimit=" + lockHoldTimeLimit)
	if err != nil {
		t.Fatalf("UpdateFromString('TrackedLock.LockHoldTimeLimit=%s') failed: %v", lockHoldTimeLimit, err)
	}

	// update with the new values
	err = globals.updateStateFromConfMap(confMap)
	if nil != err {
		t.Fatalf("updateStateFromConfMap() for state 1 failed: %v", err)
	}

	// verify limits were set correctly
	limit, err := time.ParseDuration(lockHoldTimeLimit)
	if nil != err {
		t.Fatalf("time.ParseDuration(%s) failed: %v", lockHoldTimeLimit, err)
	}
	period, err := time.ParseDuration(lockCheckPeriod)
	if nil != err {
		t.Fatalf("time.ParseDuration(%s) failed: %v", lockCheckPeriod, err)
	}

	if time.Duration(atomic.LoadInt64(&globals.lockHoldTimeLimit)) != limit || time.Duration(atomic.LoadInt64(&globals.lockCheckPeriod)) != period {
		t.Fatalf("lockHoldTimeLimit=%d is not %s or lockCheckPeriod=%d is not %s",
			time.Duration(atomic.LoadInt64(&globals.lockHoldTimeLimit)), lockHoldTimeLimit,
			time.Duration(atomic.LoadInt64(&globals.lockCheckPeriod)), lockCheckPeriod)
	}
}

// Make sure we can reload trackedlock with new values.
//
// There are 3 possible states for lock tracking with 6 possible transitions
// between states, of which we're only going to test 5.  The states are:
//
//   T W  Description
//   0 0  Lock tracking and lock watcher disabled
//   1 0  Lock tracking enabled lock watcher disabled
//   1 1  Lock tracking enabled and lock watcher enabled
//
// In addition, locks can be acquired and released before the package is Up() or
// after it is Down(); TestStartupShutdown() tests those transitions
//
// This test will go through the following steps, acquiring locks in each of
// steps 0, 1, 2, and 3 and releasing them in states 1, 2, 3, 4.
//
// Step  T W  Description
//   0   0 0  Lock tracking and lock watcher disabled
//   1   1 0  Lock tracking enabled lock watcher disabled
//   2   1 1  Lock tracking enabled and lock watcher enabled
//   3   1 0  Lock tracking enabled lock watcher disabled
//   4   0 0  Lock tracking and lock watcher disabled
//   5   0 0  lock tracker shutdown
//
func TestReload(t *testing.T) {

	// bring the package up using the default config
	confMap, err := conf.MakeConfMapFromStrings(confStrings)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Startup packages involved
	err = logger.Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("logger.Up(confMap) failed: %v", err)
		t.Fatalf(tErr)
	}

	err = globals.Up(confMap)
	if nil != err {
		tErr := fmt.Sprintf("Up() failed: %v", err)
		t.Fatalf(tErr)
	}

	// get a copy of what's written to the log
	var logcopy logger.LogTarget
	logcopy.Init(256)
	logger.AddLogTarget(logcopy)

	// transition to step 0: lock tracking disabled; watching disabled
	updateTrackingState(t, "0s", "0s")

	// Acquire some locks in state 0 (the names all start with 0). Because
	// the code tracking of a RWMutex held in writer (exclusive) mode is the
	// same code used to track a Mutex, there is not need to test RWMutex
	// held in writer mode separately from the mutexes.
	var (
		mutex01, mutex02, mutex03, mutex04, mutex05           Mutex
		rwMutex01, rwMutex02, rwMutex03, rwMutex04, rwMutex05 RWMutex
	)
	mutex01.Lock()
	mutex02.Lock()
	mutex03.Lock()
	mutex04.Lock()
	mutex05.Lock()
	rwMutex01.RLock()
	rwMutex02.RLock()
	rwMutex03.RLock()
	rwMutex04.RLock()
	rwMutex05.RLock()

	// transistion to step 1: lock tracking enabled; watching disabled
	updateTrackingState(t, "1s", "0s")

	// release locks for step 1
	mutex01.Unlock()
	rwMutex01.RUnlock()

	// locks acquired in step 1
	var (
		mutex12, mutex13, mutex14, mutex15         Mutex
		rwMutex12, rwMutex13, rwMutex14, rwMutex15 RWMutex
	)
	mutex12.Lock()
	mutex13.Lock()
	mutex14.Lock()
	mutex15.Lock()
	rwMutex12.RLock()
	rwMutex13.RLock()
	rwMutex14.RLock()
	rwMutex15.RLock()

	// transistion to step 2: lock tracking enabled; watching enabled
	updateTrackingState(t, "1s", "2s")

	// release locks for step 2
	mutex02.Unlock()
	mutex12.Unlock()
	rwMutex02.RUnlock()
	rwMutex12.RUnlock()

	// locks acquired in step 2
	var (
		mutex23, mutex24, mutex25       Mutex
		rwMutex23, rwMutex24, rwMutex25 RWMutex
	)
	mutex23.Lock()
	mutex24.Lock()
	mutex25.Lock()
	rwMutex23.RLock()
	rwMutex24.RLock()
	rwMutex25.RLock()

	// change the lock watcher interval to 1 sec and verify locks aren't
	// forgotten (this is still step 2)
	updateTrackingState(t, "1s", "1s")

	// wait 2.1 sec so locks are held more than 1 sec and the lock watcher
	// has a chance to run and take notice
	sleep(2.1)

	// mutex23 was the first lock acquired after enabling the lock watcher,
	// so it should report it
	fields, _, err := logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank0LogRE, 10)
	if err != nil {
		t.Errorf("TestReload(): could not find log entry for lockWatcher: %s", err.Error())
	}

	ptr, _ := strconv.ParseUint(fields["ptr"], 0, 64)
	if uintptr(ptr) != uintptr(unsafe.Pointer(&mutex23)) {
		t.Errorf("TestReload(): mutex23 at %p is not the longest held Mutex", &mutex23)
	}
	if fields["type"] != "*trackedlock.Mutex" {
		t.Errorf("TestReload(): mutex23 type reported as '%s' instead of '*trackedlock.Mutex'", fields["type"])
	}

	// transistion to step 3: lock tracking enabled; watching disabled
	updateTrackingState(t, "1s", "0s")

	// release locks for step 3
	mutex03.Unlock()
	mutex13.Unlock()
	mutex23.Unlock()
	rwMutex03.RUnlock()
	rwMutex13.RUnlock()
	rwMutex23.RUnlock()

	// locks acquired in step 3
	var (
		mutex34, mutex35     Mutex
		rwMutex34, rwMutex35 RWMutex
	)
	mutex34.Lock()
	mutex35.Lock()
	rwMutex34.RLock()
	rwMutex35.RLock()

	// transistion to step 4: lock tracking disabled; watching disabled
	updateTrackingState(t, "0s", "0s")

	// locks acquired in step 4
	var (
		mutex45   Mutex
		rwMutex45 RWMutex
	)
	mutex45.Lock()
	rwMutex45.RLock()

	// release locks for step 4
	mutex04.Unlock()
	mutex14.Unlock()
	mutex24.Unlock()
	mutex34.Unlock()
	rwMutex04.RUnlock()
	rwMutex14.RUnlock()
	rwMutex24.RUnlock()
	rwMutex34.RUnlock()

	// transition to step 5 -- shutdown this package
	err = globals.Down(confMap)
	if nil != err {
		t.Fatalf("Down() failed: %v", err)
	}

	// release locks for step 5 (after tracking shutdown)
	mutex05.Unlock()
	mutex15.Unlock()
	mutex25.Unlock()
	mutex35.Unlock()
	mutex45.Unlock()
	rwMutex05.RUnlock()
	rwMutex15.RUnlock()
	rwMutex25.RUnlock()
	rwMutex35.RUnlock()
	rwMutex45.RUnlock()

}

// Make sure we can transition from one lock tracking state to another without
// panics or spurious log messages.
//
// There are 3 possible states of lock tracking, which are:
//
// Tracking
//  State   T W  Description
//    0     0 0  Lock tracking and lock watcher disabled
//    1     1 0  Lock tracking enabled; lock watcher disabled
//    2     1 1  Lock tracking enabled and lock watcher enabled
//
// A lock can be acquired in one tracking state and released in another, which
// might trigger misbehavior in the tracking code.  Further, while locks are
// tracked in states 1 and 2, they are watched only in state 2, and watching
// locks causes the allocation of additional data structures at lock time which
// persist across unlock (for the watcher), so a lock acquired in state 2,
// released in any state, then acquired again in state 0 (or 1) and then watched
// again as a result of transitioning to state 2 might trigger bogus log messages.
//
// Create a set of locks, each of which is acquired and released twice, and test
// all of the possibilites (there are 3^4 or 81 such combinations).  Check the
// log or bogus entries as well.
//
func TestStateTransitions(t *testing.T) {

	// get a copy of what's written to the log
	var logcopy logger.LogTarget
	logcopy.Init(256)
	logger.AddLogTarget(logcopy)

	// bring the package up using the default config
	confMap, _ := conf.MakeConfMapFromStrings(confStrings)
	_ = logger.Up(confMap)

	err := globals.Up(confMap)
	if err != nil {
		t.Fatalf("Up() failed: %v", err)
	}

	mutexes := [3][3][3][3]Mutex{}
	rwMutexes := [3][3][3][3]RWMutex{}

	// helper functcion to update the lock tracking state; note that state 2
	// enables the lock watcher and this is the last state set by the loops,
	// below, so the lock watcher is running during the pauses
	setTrackingState := func(newState int) {
		switch newState {

		case 0:
			// state 0: lock tracking disabled; watching disabled
			updateTrackingState(t, "0s", "0s")

		case 1:
			// state 1: lock tracking enabled; watching disabled
			updateTrackingState(t, "2s", "0s")

		case 2:
			// state 2: lock tracking enabled; watching enable
			updateTrackingState(t, "2s", "1s")
		}
	}

	// The first step is lock all of the mutexs; the first array index is
	// the tracking state the mutex is locked in.
	for state := 0; state < 3; state += 1 {

		setTrackingState(state)
		for j := 0; j < 3; j += 1 {
			for k := 0; k < 3; k += 1 {
				for l := 0; l < 3; l += 1 {
					mutexes[state][j][k][l].Lock()
					rwMutexes[state][j][k][l].RLock()
				}
			}
		}
	}

	// wait for the lock watcher to run
	sleep(1.2)

	// lockwatcher should not have identified any locks as held too long
	fields, _, err := logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank0LogRE, 256)
	if err == nil {
		t.Errorf("found a complaint by lockWatcher when none should exist: %v", fields)
	}

	// The second step is to unlock all of the mutexs; the second array
	// index is the tracking state the mutex is unlocked in.  Note that we
	// iterate over the second array index in the outermost loop.
	for state := 0; state < 3; state += 1 {

		setTrackingState(state)
		for i := 0; i < 3; i += 1 {
			for k := 0; k < 3; k += 1 {
				for l := 0; l < 3; l += 1 {
					mutexes[i][state][k][l].Unlock()
					rwMutexes[i][state][k][l].RUnlock()
				}
			}
		}
	}

	// unlock should not have flagged any locks as held too long
	// (first wait a little for log messages to get logged)
	sleep(0.1)
	fields, _, err = logger.ParseLogForFunc(logcopy, "unlockTrack", unlockLogRE, 256)
	if err == nil {
		t.Errorf("found a complaint by unlockTrack() when none should exist: %v", fields)
	}
	fields, _, err = logger.ParseLogForFunc(logcopy, "rUnlockTrack", rUnlockLogRE, 256)
	if err == nil {
		t.Errorf("found a complaint by rUnlockTrack() when none should exist: %v", fields)
	}

	// The third step is lock all of the mutexs a second time; the third
	// array index is the tracking state the mutex is locked in.
	for state := 0; state < 3; state += 1 {

		setTrackingState(state)
		for i := 0; i < 3; i += 1 {
			for j := 0; j < 3; j += 1 {
				for l := 0; l < 3; l += 1 {
					mutexes[i][j][state][l].Lock()
					rwMutexes[i][j][state][l].RLock()
				}
			}
		}
	}

	// wait for the lock watcher to run
	sleep(1.2)

	// lockwatcher should not have identified any locks as held too long
	fields, _, err = logger.ParseLogForFunc(logcopy, "lockWatcher", watcherRank0LogRE, 256)
	if err == nil {
		t.Errorf("found a complaint by lockWatcher when none should exist: %v", fields)
	}

	// The fourth step is to unlock all of the mutexs; the second array
	// index is the lock tracking state the mutex is unlocked in.
	for state := 0; state < 3; state += 1 {

		setTrackingState(state)
		for i := 0; i < 3; i += 1 {
			for j := 0; j < 3; j += 1 {
				for k := 0; k < 3; k += 1 {
					mutexes[i][j][k][state].Unlock()
					rwMutexes[i][j][k][state].RUnlock()
				}
			}
		}
	}

	// unlock should not have flagged any locks as held too long
	// (first wait a little for log messages to get logged)
	sleep(0.1)
	fields, _, err = logger.ParseLogForFunc(logcopy, "unlockTrack", unlockLogRE, 256)
	if err == nil {
		t.Errorf("found a complaint by unlockTrack() when none should exist: %v", fields)
	}
	fields, _, err = logger.ParseLogForFunc(logcopy, "rUnlockTrack", rUnlockLogRE, 256)
	if err == nil {
		t.Errorf("found a complaint by rUnlockTrack() when none should exist: %v", fields)
	}
}
