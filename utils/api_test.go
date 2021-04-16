// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"container/list"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	testTryLockMutex *TryLockMutex
)

func testTryLockMutexAsyncUnlock() {
	testTryLockMutex.Unlock()
}

func TestTryLockMutex(t *testing.T) {
	testTryLockMutex = NewTryLockMutex()
	testTryLockMutex.Lock()
	testTryLockMutex.Unlock()
	shouldHaveGottonIt := testTryLockMutex.TryLock(100 * time.Millisecond)
	if !shouldHaveGottonIt {
		t.Fatalf("1st TryLock() should have succeeded")
	}
	shouldNotHaveGottenIt := testTryLockMutex.TryLock(100 * time.Millisecond)
	if shouldNotHaveGottenIt {
		t.Fatalf("2nd TryLock() should have failed")
	}
	_ = time.AfterFunc(50*time.Millisecond, testTryLockMutexAsyncUnlock)
	shouldHaveGottonIt = testTryLockMutex.TryLock(100 * time.Millisecond)
	if !shouldHaveGottonIt {
		t.Fatalf("3rd TryLock() should have succeeded")
	}
	testTryLockMutex.Unlock()
}

func testMultiWaiterSyncDotWaitGroupWaiter(wgToWaitOn *sync.WaitGroup, currentPhase *uint64, phaseWhenDone *uint64, wgWhenDone *sync.WaitGroup) {
	wgToWaitOn.Wait()
	*phaseWhenDone = *currentPhase
	wgWhenDone.Done()
}

func TestMultiWaiterSyncDotWaitGroup(t *testing.T) {
	var (
		currentPhase  uint64
		mwwg          sync.WaitGroup // We will have two waiters on three signalers
		phaseWhenDone [2]uint64
		wgWhenDone    sync.WaitGroup
	)

	wgWhenDone.Add(2) // Used to await completion of each testMultiWaiterSyncDotWaitGroupWaiter() goroutine

	mwwg.Add(3) // We will have three entities signalling to this MultiWaiterWaitGroup

	currentPhase = 0
	go testMultiWaiterSyncDotWaitGroupWaiter(&mwwg, &currentPhase, &phaseWhenDone[0], &wgWhenDone)
	time.Sleep(100 * time.Millisecond)
	currentPhase = 1
	go testMultiWaiterSyncDotWaitGroupWaiter(&mwwg, &currentPhase, &phaseWhenDone[1], &wgWhenDone)
	time.Sleep(100 * time.Millisecond)
	currentPhase = 2
	mwwg.Done()
	time.Sleep(100 * time.Millisecond)
	currentPhase = 3
	mwwg.Done()
	time.Sleep(100 * time.Millisecond)
	currentPhase = 4
	mwwg.Done()                        // This is the point where the two testMultiWaiterSyncDotWaitGroupWaiter() goroutines should awake
	time.Sleep(100 * time.Millisecond) // ...and this gives them 100ms to wake up and record currentPhase before
	currentPhase = 5                   // ...we increment currentPhase beyond where it should have been recorded
	wgWhenDone.Wait()
	currentPhase = 6

	if (4 != phaseWhenDone[0]) || (4 != phaseWhenDone[1]) {
		// The two testMultiWaiterSyncDotWaitGroupWaiter() goroutines didn't properly capture the expected currentPhase
		t.Fatalf("(4 != phaseWhenDone[0]) || (4 != phaseWhenDone[1])")
	}
}

func testMultiWaiterWaitGroupWaiter(mwwgToWaitOn *MultiWaiterWaitGroup, currentPhase *uint64, phaseWhenDone *uint64, wgWhenDone *sync.WaitGroup) {
	mwwgToWaitOn.Wait()
	*phaseWhenDone = *currentPhase
	wgWhenDone.Done()
}

func TestMultiWaiterWaitGroup(t *testing.T) {
	var (
		currentPhase  uint64
		mwwg          *MultiWaiterWaitGroup // We will have two waiters on three signalers
		phaseWhenDone [2]uint64
		wgWhenDone    sync.WaitGroup
	)

	wgWhenDone.Add(2) // Used to await completion of each testMultiWaiterWaitGroupWaiter() goroutine

	mwwg = FetchMultiWaiterWaitGroup()
	mwwg.Add(3) // We will have three entities signalling to this MultiWaiterWaitGroup

	currentPhase = 0
	go testMultiWaiterWaitGroupWaiter(mwwg, &currentPhase, &phaseWhenDone[0], &wgWhenDone)
	time.Sleep(100 * time.Millisecond)
	currentPhase = 1
	go testMultiWaiterWaitGroupWaiter(mwwg, &currentPhase, &phaseWhenDone[1], &wgWhenDone)
	time.Sleep(100 * time.Millisecond)
	currentPhase = 2
	mwwg.Done()
	time.Sleep(100 * time.Millisecond)
	currentPhase = 3
	mwwg.Done()
	time.Sleep(100 * time.Millisecond)
	currentPhase = 4
	mwwg.Done()                        // This is the point where the two testMultiWaiterWaitGroupWaiter() goroutines should awake
	time.Sleep(100 * time.Millisecond) // ...and this gives them 100ms to wake up and record currentPhase before
	currentPhase = 5                   // ...we increment currentPhase beyond where it should have been recorded
	wgWhenDone.Wait()
	currentPhase = 6

	if (4 != phaseWhenDone[0]) || (4 != phaseWhenDone[1]) {
		// The two testMultiWaiterWaitGroupWaiter() goroutines didn't properly capture the expected currentPhase
		t.Fatalf("(4 != phaseWhenDone[0]) || (4 != phaseWhenDone[1])")
	}
}

func TestPathToAcctContObj(t *testing.T) {
	assert := assert.New(t)

	vPath := "/v1"
	acctString := "my_virtual_account"
	containerString := ""
	objectString := ""

	accountName, containerName, objectName, err := PathToAcctContObj(vPath + "/" + acctString)
	assert.Equal(err, nil)
	assert.Equal(accountName, acctString, "Wrong account name!")

	containerString = "my_virtual_container"
	accountName, containerName, objectName, err = PathToAcctContObj(vPath + "/" + acctString +
		"/" + containerString)
	assert.Equal(err, nil)
	assert.Equal(accountName, acctString, "Wrong account name!")
	assert.Equal(containerName, containerString, "Wrong container name!")

	objectString = "my_object_name"
	accountName, containerName, objectName, err = PathToAcctContObj(vPath + "/" + acctString +
		"/" + containerString + "/" + objectString)
	assert.Equal(err, nil)
	assert.Equal(accountName, acctString, "Wrong account name!")
	assert.Equal(containerName, containerString, "Wrong container name!")
	assert.Equal(objectName, objectString, "Wrong object name!")
}

func TestGetAFnName(t *testing.T) {
	assert := assert.New(t)

	fnWithPackage := GetAFnName(0)
	//fmt.Printf("Func plus package is %v\n", fnWithPackage)
	assert.Equal(fnWithPackage, "utils.TestGetAFnName")

	fn, pkg, gid := GetFuncPackage(0)
	if 0 == gid { // Dummuy reference to gid
	}
	//fmt.Printf("Func is %v, package is %v, gid is %v\n", fn, pkg, gid)
	assert.Equal(pkg, "utils")
	assert.Equal(fn, "TestGetAFnName")
}

func TestStopwatch(t *testing.T) {
	assert := assert.New(t)

	// Test normal operation

	//
	// Create stopwatch
	//
	sw1 := NewStopwatch()
	now := time.Now()

	// check stuff
	startTime1 := sw1.StartTime                                              // Save startTime for later checks
	assert.True(sw1.StartTime.Before(now), "time stopped!", startTime1, now) // Start time is in the past
	assert.True(sw1.StopTime.IsZero())                                       // Stop time isn't set yet
	assert.Equal(int64(sw1.ElapsedTime), int64(0))                           // Elapsed time isn't set yet
	assert.True(sw1.IsRunning)                                               // stopwatch is running

	// Delay then stop and check nonzero elapsed time
	sleepTime := 100 * time.Millisecond
	time.Sleep(sleepTime)

	// Stop it
	assert.True(sw1.IsRunning) // stopwatch is still running
	elapsed1 := sw1.Stop()
	now := time.Now()

	// check stuff
	assert.False(sw1.IsRunning)                                               // stopwatch is not running
	assert.False(sw1.StopTime.IsZero())                                       // Stop time is set
	assert.True(sw1.StopTime.Before(now), "time stopped!", sw1.StopTime, now) // Stop time is in the past
	assert.True(sw1.StartTime == startTime1)                                  // StartTime hasn't changed
	assert.True(elapsed1 >= sleepTime)                                        // elapsed time is reasonable

	//
	// Call Elapsed() when stopped
	//
	assert.True(sw1.Elapsed() == elapsed1) // elapsed time is the same as what was returned by Stop()

	// Check Elapsed* functions for correctness
	assert.True(sw1.ElapsedSec() == elapsed1.Nanoseconds()/int64(time.Second))
	assert.True(sw1.ElapsedMs() == elapsed1.Nanoseconds()/int64(time.Millisecond))
	assert.True(sw1.ElapsedUs() == elapsed1.Nanoseconds()/int64(time.Microsecond))
	assert.True(sw1.ElapsedNs() == elapsed1.Nanoseconds()/int64(time.Nanosecond))

	// XXX TODO: Check Elapsed*String functions for correctness

	//
	// Create another stopwatch
	//
	sw2 := NewStopwatch()
	now = time.Now()

	// check stuff
	startTime2 := sw2.StartTime                                              // Save startTime for later checks
	assert.True(sw2.StartTime.Before(now), "Time stopped!", startTime2, now) // Start time is in the past
	assert.True(sw2.StopTime.IsZero())                                       // Stop time isn't set yet
	assert.Equal(int64(sw2.ElapsedTime), int64(0))                           // Elapsed time isn't set yet
	assert.True(sw2.IsRunning)                                               // stopwatch is running

	// Delay then check nonzero elapsed time (but don't stop)
	sleepTime = 856 * time.Millisecond
	time.Sleep(sleepTime)

	//
	// Call Elapsed() when not stopped
	//
	assert.True(sw2.IsRunning) // stopwatch is still running
	elapsed2 := sw2.Elapsed()
	//fmt.Printf("While running: sw2 is %v\n", sw2)

	// check stuff
	assert.True(sw2.StopTime.IsZero())       // Stop time isn't set yet because we're not stopped
	assert.True(sw2.StartTime == startTime2) // StartTime hasn't changed
	assert.True(elapsed2 >= sleepTime)       // elapsed time is reasonable

	//
	// Stop and then call Elapsed() again
	//
	elapsed3 := sw2.Stop()
	now = time.Now()

	// check stuff
	assert.False(sw2.IsRunning)                                               // stopwatch is not running
	assert.False(sw2.StopTime.IsZero())                                       // Stop time is set
	assert.True(sw2.StopTime.Before(now), "time stopped?", sw2.StopTime, now) // Stop time is in the past
	assert.True(sw2.StartTime == startTime2)                                  // StartTime hasn't changed
	assert.True(elapsed3 > elapsed2)                                          // elapsed time is later than before
	assert.True(sw2.Elapsed() == elapsed3)

	//
	// restart a previously running stopwatch
	//
	sw2.Restart()
	now = time.Now()

	// check stuff
	startTime3 := sw2.StartTime                                                 // Save startTime for later checks
	assert.True(sw2.StartTime.Before(now), "time stopped?", sw2.StartTime, now) // Start time is in the past
	assert.True(sw2.StopTime.IsZero())                                          // Stop time isn't set yet
	assert.Equal(int64(sw2.ElapsedTime), int64(0))                              // Elapsed time isn't set yet
	assert.True(sw2.IsRunning)                                                  // stopwatch is running

	// Delay then stop and check nonzero elapsed time
	sleepTime = 912 * time.Microsecond
	time.Sleep(sleepTime)

	//
	// Stop it
	//
	assert.True(sw2.IsRunning) // stopwatch is still running
	elapsed4 := sw2.Stop()
	now = time.Now()

	// check stuff
	assert.False(sw2.IsRunning)                                               // stopwatch is not running
	assert.False(sw2.StopTime.IsZero())                                       // Stop time is set
	assert.True(sw2.StopTime.Before(now), "time stopped?", sw2.StopTime, now) // Stop time is in the past
	assert.True(sw2.StartTime == startTime3)                                  // StartTime hasn't changed
	assert.True(elapsed4 >= sleepTime)                                        // elapsed time is reasonable

	//
	// Call Elapsed() when stopped
	//
	assert.True(sw2.Elapsed() == elapsed4) // elapsed time is the same as what was returned by Stop()

	// Check Elapsed* functions for correctness
	assert.True(sw2.ElapsedSec() == elapsed4.Nanoseconds()/int64(time.Second))
	assert.True(sw2.ElapsedMs() == elapsed4.Nanoseconds()/int64(time.Millisecond))
	assert.True(sw2.ElapsedUs() == elapsed4.Nanoseconds()/int64(time.Microsecond))
	assert.True(sw2.ElapsedNs() == elapsed4.Nanoseconds()/int64(time.Nanosecond))

	//
	// restart a non-previously running stopwatch
	//
	sw3 := NewStopwatch()
	now = time.Now()

	// check stuff
	startTime4 := sw3.StartTime                                // Save startTime for later checks
	assert.True(sw3.StartTime.Before(now), sw3.StartTime, now) // Start time is in the past
	assert.True(sw3.StopTime.IsZero())                         // Stop time isn't set yet
	assert.Equal(int64(sw3.ElapsedTime), int64(0))             // Elapsed time isn't set yet
	assert.True(sw3.IsRunning)                                 // stopwatch is running

	// Attempt restart, shouldn't do anything since not stopped
	sw3.Restart()
	//fmt.Printf("After restart: sw3 is %v\n", sw3)

	// check stuff
	assert.True(sw3.StartTime == startTime4)       // StartTime hasn't changed
	assert.True(sw3.StopTime.IsZero())             // Stop time isn't set yet
	assert.Equal(int64(sw3.ElapsedTime), int64(0)) // Elapsed time isn't set yet
	assert.True(sw3.IsRunning)                     // stopwatch is running
}

func TestRandomizeList(t *testing.T) {
	var (
		l *list.List
		n int
	)

	for n = 0; n <= 20; n++ {
		l = testPopulateListOfInts(n)
		RandomizeList(l)
		// testDumpListOfInts(l)
	}
}

func testPopulateListOfInts(n int) (l *list.List) {
	var (
		lev int
	)

	l = list.New()

	for lev = 0; lev < n; lev++ {
		l.PushBack(lev)
	}

	return
}

func testDumpListOfInts(l *list.List) {
	var (
		le  *list.Element
		lev int
	)

	le = l.Front()

	for le != nil {
		lev = le.Value.(int)
		fmt.Printf(" %d", lev)
		le = le.Next()
	}

	fmt.Println()
}
