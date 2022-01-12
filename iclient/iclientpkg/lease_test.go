// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	testInodeLockInodeNumberA = uint64(1)
	testInodeLockInodeNumberB = uint64(2)
	testInodeLockInodeNumberC = uint64(3)

	testChildLockOperationDelay = time.Duration(100 * time.Millisecond)
)

type testChildLockStruct struct {
	sync.WaitGroup // Signaled to tell testChildLock goroutine to release their lock and exit
	exclusive      bool
	lockHeld       bool
	err            error
}

func testChildLockStart(exclusive bool) (testChildLock *testChildLockStruct) {
	testChildLock = &testChildLockStruct{
		exclusive: exclusive,
		lockHeld:  false,
		err:       nil,
	}

	testChildLock.Add(1)

	go testChildLock.gor()

	return
}

func (testChildLock *testChildLockStruct) gor() {
	var (
		inodeHeldLock    *inodeHeldLockStruct
		inodeLockRequest *inodeLockRequestStruct
		ok               bool
	)

	inodeLockRequest = newLockRequest()
	inodeLockRequest.inodeNumber = testInodeLockInodeNumberA
	inodeLockRequest.exclusive = testChildLock.exclusive
	inodeLockRequest.addThisLock()

	if len(inodeLockRequest.locksHeld) != 1 {
		testChildLock.err = fmt.Errorf("len(inodeLockRequest.locksHeld) (%v) should have been == 1", len(inodeLockRequest.locksHeld))
		runtime.Goexit()
	}
	inodeHeldLock, ok = inodeLockRequest.locksHeld[testInodeLockInodeNumberA]
	if !ok {
		testChildLock.err = fmt.Errorf("inodeLockRequest.locksHeld[testInodeLockInodeNumberA] returned !ok")
		runtime.Goexit()
	}
	if inodeHeldLock.inode.inodeNumber != testInodeLockInodeNumberA {
		testChildLock.err = fmt.Errorf("inodeHeldLock.inode.inodeNumber (%v) != testInodeLockInodeNumberA (%v)", inodeHeldLock.inode.inodeNumber, testInodeLockInodeNumberA)
		runtime.Goexit()
	}
	if inodeHeldLock.exclusive != testChildLock.exclusive {
		testChildLock.err = fmt.Errorf("inodeHeldLock.exclusive should have been %v", testChildLock.exclusive)
		runtime.Goexit()
	}

	testChildLock.lockHeld = true

	testChildLock.Wait()

	inodeLockRequest.unlockAll()
	if len(inodeLockRequest.locksHeld) != 0 {
		testChildLock.err = fmt.Errorf("len(inodeLockRequest.locksHeld) (%v) should have been == 0", len(inodeLockRequest.locksHeld))
		runtime.Goexit()
	}

	testChildLock.lockHeld = false
}

func (testChildLock *testChildLockStruct) delayAndCheckForError(t *testing.T) {
	time.Sleep(testChildLockOperationDelay)

	if testChildLock.err != nil {
		t.Fatal(testChildLock.err)
	}
}

func (testChildLock *testChildLockStruct) finish() {
	testChildLock.Done()
}

func TestLocks(t *testing.T) {
	var (
		childLock         [7]*testChildLockStruct
		err               error
		fissionErrChan    chan error
		inodeHeldLock     *inodeHeldLockStruct
		inodeLockRequestA *inodeLockRequestStruct
		inodeLockRequestB *inodeLockRequestStruct
		ok                bool
	)

	testSetup(t)

	fissionErrChan = make(chan error, 1)

	err = initializeGlobals(testGlobals.confMap, fissionErrChan)
	if nil != err {
		t.Fatalf("initializeGlobals(testGlobals.confMap, fissionErrChan) failed: %v", err)
	}

	err = startRPCHandler()
	if nil != err {
		t.Fatalf("startRPCHandler() failed: %v", err)
	}

	// Verify we can get a shared lock on currently unlocked testInodeLockInodeNumberA

	inodeLockRequestA = newLockRequest()
	inodeLockRequestA.inodeNumber = testInodeLockInodeNumberA
	inodeLockRequestA.exclusive = false
	inodeLockRequestA.addThisLock()
	if len(inodeLockRequestA.locksHeld) != 1 {
		t.Fatalf("len(inodeLockRequestA.locksHeld) (%v) should have been == 1", len(inodeLockRequestA.locksHeld))
	}
	inodeHeldLock, ok = inodeLockRequestA.locksHeld[testInodeLockInodeNumberA]
	if !ok {
		t.Fatalf("inodeLockRequestA.locksHeld[testInodeLockInodeNumberA] returned !ok")
	}
	if inodeHeldLock.inode.inodeNumber != testInodeLockInodeNumberA {
		t.Fatalf("inodeHeldLock.inode.inodeNumber (%v) != testInodeLockInodeNumberA (%v)", inodeHeldLock.inode.inodeNumber, testInodeLockInodeNumberA)
	}
	if inodeHeldLock.exclusive {
		t.Fatalf("inodeHeldLock.exclusive should have been false")
	}

	// Verify we can add in an exclusive lock on currently unlocked testInodeLockInodeNumberB

	inodeLockRequestA.inodeNumber = testInodeLockInodeNumberB
	inodeLockRequestA.exclusive = true
	inodeLockRequestA.addThisLock()
	if len(inodeLockRequestA.locksHeld) != 2 {
		t.Fatalf("len(inodeLockRequestA.locksHeld) (%v) should have been == 2", len(inodeLockRequestA.locksHeld))
	}
	inodeHeldLock, ok = inodeLockRequestA.locksHeld[testInodeLockInodeNumberA]
	if !ok {
		t.Fatalf("inodeLockRequestA.locksHeld[testInodeLockInodeNumberA] returned !ok")
	}
	if inodeHeldLock.inode.inodeNumber != testInodeLockInodeNumberA {
		t.Fatalf("inodeHeldLock.inode.inodeNumber (%v) != testInodeLockInodeNumberA (%v)", inodeHeldLock.inode.inodeNumber, testInodeLockInodeNumberA)
	}
	if inodeHeldLock.exclusive {
		t.Fatalf("inodeHeldLock.exclusive should have been false")
	}
	inodeHeldLock, ok = inodeLockRequestA.locksHeld[testInodeLockInodeNumberB]
	if !ok {
		t.Fatalf("inodeLockRequestA.locksHeld[testInodeLockInodeNumberB] returned !ok")
	}
	if inodeHeldLock.inode.inodeNumber != testInodeLockInodeNumberB {
		t.Fatalf("inodeHeldLock.inode.inodeNumber (%v) != testInodeLockInodeNumberB (%v)", inodeHeldLock.inode.inodeNumber, testInodeLockInodeNumberB)
	}
	if !inodeHeldLock.exclusive {
		t.Fatalf("inodeHeldLock.exclusive should have been true")
	}

	// Verify attempting a shared lock on a currently exclusively locked testInodeLockInodeNumberB while holding a shared lock on testInodeLockInodeNumberC fails releasing the shared lock on testInodeLockInodeNumberC

	inodeLockRequestB = newLockRequest()
	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberC
	inodeLockRequestB.exclusive = false
	inodeLockRequestB.addThisLock()
	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberB
	inodeLockRequestB.exclusive = false
	inodeLockRequestB.addThisLock()
	if len(inodeLockRequestB.locksHeld) != 0 {
		t.Fatalf("len(inodeLockRequestB.locksHeld) (%v) should have been == 0", len(inodeLockRequestB.locksHeld))
	}

	// Verify attempting an exclusive lock on a currently shared locked testInodeLockInodeNumberA while holding a shared lock on testInodeLockInodeNumberC fails releasing the shared lock on testInodeLockInodeNumberC

	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberC
	inodeLockRequestB.exclusive = false
	inodeLockRequestB.addThisLock()
	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberA
	inodeLockRequestB.exclusive = true
	inodeLockRequestB.addThisLock()
	if len(inodeLockRequestB.locksHeld) != 0 {
		t.Fatalf("len(inodeLockRequestB.locksHeld) (%v) should have been == 0", len(inodeLockRequestB.locksHeld))
	}

	// Verify attempting a shared lock on a currently exclusively locked testInodeLockInodeNumberB while holding an exclusive lock on testInodeLockInodeNumberC fails releasing the exclusive lock on testInodeLockInodeNumberC

	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberC
	inodeLockRequestB.exclusive = true
	inodeLockRequestB.addThisLock()
	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberB
	inodeLockRequestB.exclusive = false
	inodeLockRequestB.addThisLock()
	if len(inodeLockRequestB.locksHeld) != 0 {
		t.Fatalf("len(inodeLockRequestB.locksHeld) (%v) should have been == 0", len(inodeLockRequestB.locksHeld))
	}

	// Verify attempting an exclusive lock on a currently shared locked testInodeLockInodeNumberA while holding an exclusive lock on testInodeLockInodeNumberC fails releasing the exclusive lock on testInodeLockInodeNumberC

	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberC
	inodeLockRequestB.exclusive = true
	inodeLockRequestB.addThisLock()
	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberA
	inodeLockRequestB.exclusive = true
	inodeLockRequestB.addThisLock()
	if len(inodeLockRequestB.locksHeld) != 0 {
		t.Fatalf("len(inodeLockRequestB.locksHeld) (%v) should have been == 0", len(inodeLockRequestB.locksHeld))
	}

	// Verify we can release the shared lock on testInodeLockInodeNumberA and the exclusive lock on testInodeLockInodeNumberB

	inodeLockRequestA.unlockAll()
	if len(inodeLockRequestA.locksHeld) != 0 {
		t.Fatalf("len(inodeLockRequestA.locksHeld) (%v) should have been == 0", len(inodeLockRequestA.locksHeld))
	}

	// Perform a sequence of lock requests that all contend on testInodeLockInodeNumberA.
	//
	// The sequence will be:
	//
	//   0: Shared    - this should be granted since testInodeLockInodeNumberA was previously unlocked
	//   1: Shared    - this should block since testInodeLockInodeNumberA is currently share locked
	//                - this should unblock when both 0 is unlocked
	//   2: Exclusive - this should block since testInodeLockInodeNumberA is currently share locked
	//                - this should unblock when both 1 is unlocked
	//   3: Shared    - this should block since there is already a prior exclusive lock request
	//                - this should unblock when 2 is unlocked
	//   4: Shared    - this should block since there is already a prior exclusive lock request
	//                - this should unblock when 3 is unlocked
	//   5: Exclusive - this should block since testInodeLockInodeNumberA is currently share locked
	//                - this should unblock when 4 is unlocked
	//   6: Exclusive - this should block since testInodeLockInodeNumberA is currently share locked
	//                - this should unblock when 5 is unlocked

	childLock[0] = testChildLockStart(false)
	childLock[0].delayAndCheckForError(t)
	childLock[1] = testChildLockStart(false)
	childLock[1].delayAndCheckForError(t)
	childLock[2] = testChildLockStart(true)
	childLock[2].delayAndCheckForError(t)
	childLock[3] = testChildLockStart(false)
	childLock[3].delayAndCheckForError(t)
	childLock[4] = testChildLockStart(false)
	childLock[4].delayAndCheckForError(t)
	childLock[5] = testChildLockStart(true)
	childLock[5].delayAndCheckForError(t)
	childLock[6] = testChildLockStart(true)
	childLock[0].delayAndCheckForError(t)

	if !childLock[0].lockHeld ||
		childLock[1].lockHeld ||
		childLock[2].lockHeld ||
		childLock[3].lockHeld ||
		childLock[4].lockHeld ||
		childLock[5].lockHeld ||
		childLock[6].lockHeld {
		t.Fatalf("Initial sequence should have had testInodeLockInodeNumberA only held by 0")
	}

	childLock[0].finish()
	childLock[0].delayAndCheckForError(t)

	if childLock[0].lockHeld ||
		!childLock[1].lockHeld ||
		childLock[2].lockHeld ||
		childLock[3].lockHeld ||
		childLock[4].lockHeld ||
		childLock[5].lockHeld ||
		childLock[6].lockHeld {
		t.Fatalf("After 0 releases their lock, the sequence should have had testInodeLockInodeNumberA only held by 1")
	}

	childLock[1].finish()
	childLock[1].delayAndCheckForError(t)

	if childLock[0].lockHeld ||
		childLock[1].lockHeld ||
		!childLock[2].lockHeld ||
		childLock[3].lockHeld ||
		childLock[4].lockHeld ||
		childLock[5].lockHeld ||
		childLock[6].lockHeld {
		t.Fatalf("After 1 releases their lock, the sequence should have had testInodeLockInodeNumberA only held by 2")
	}

	childLock[2].finish()
	childLock[2].delayAndCheckForError(t)

	if childLock[0].lockHeld ||
		childLock[1].lockHeld ||
		childLock[2].lockHeld ||
		!childLock[3].lockHeld ||
		childLock[4].lockHeld ||
		childLock[5].lockHeld ||
		childLock[6].lockHeld {
		t.Fatalf("After 2 releases their lock, the sequence should have had testInodeLockInodeNumberA only held by 3")
	}

	childLock[3].finish()
	childLock[3].delayAndCheckForError(t)

	if childLock[0].lockHeld ||
		childLock[1].lockHeld ||
		childLock[2].lockHeld ||
		childLock[3].lockHeld ||
		!childLock[4].lockHeld ||
		childLock[5].lockHeld ||
		childLock[6].lockHeld {
		t.Fatalf("After 3 releases their lock, the sequence should have had testInodeLockInodeNumberA only held by 4")
	}

	childLock[4].finish()
	childLock[4].delayAndCheckForError(t)

	if childLock[0].lockHeld ||
		childLock[1].lockHeld ||
		childLock[2].lockHeld ||
		childLock[3].lockHeld ||
		childLock[4].lockHeld ||
		!childLock[5].lockHeld ||
		childLock[6].lockHeld {
		t.Fatalf("After 4 releases their lock, the sequence should have had testInodeLockInodeNumberA only held by 5")
	}

	childLock[5].finish()
	childLock[5].delayAndCheckForError(t)

	if childLock[0].lockHeld ||
		childLock[1].lockHeld ||
		childLock[2].lockHeld ||
		childLock[3].lockHeld ||
		childLock[4].lockHeld ||
		childLock[5].lockHeld ||
		!childLock[6].lockHeld {
		t.Fatalf("After 5 releases their lock, the sequence should have had testInodeLockInodeNumberA only held by 6")
	}

	childLock[6].finish()
	childLock[6].delayAndCheckForError(t)

	if childLock[0].lockHeld ||
		childLock[1].lockHeld ||
		childLock[2].lockHeld ||
		childLock[3].lockHeld ||
		childLock[4].lockHeld ||
		childLock[5].lockHeld ||
		childLock[6].lockHeld {
		t.Fatalf("After 6 releases their lock, the sequence should have had testInodeLockInodeNumberA held by none")
	}

	err = stopRPCHandler()
	if nil != err {
		t.Fatalf("stopRPCHandler() failed: %v", err)
	}

	err = uninitializeGlobals()
	if nil != err {
		t.Fatalf("uninitializeGlobals() failed: %v", err)
	}

	testTeardown(t)
}
