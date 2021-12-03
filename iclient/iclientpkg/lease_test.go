// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"testing"
)

const (
	testInodeLockInodeNumberA = uint64(1)
	testInodeLockInodeNumberB = uint64(2)
)

func TestLocks(t *testing.T) {
	var (
		err               error
		fissionErrChan    chan error
		inodeLockRequestA *inodeLockRequestStruct
		inodeLockRequestB *inodeLockRequestStruct
		// inodeLockRequestC *inodeLockRequestStruct
		inodeHeldLock *inodeHeldLockStruct
		ok            bool
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

	t.Logf("TODO... actually test locks in func TestLocks() below")

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

	inodeLockRequestB = newLockRequest()
	inodeLockRequestB.inodeNumber = testInodeLockInodeNumberA
	inodeLockRequestB.exclusive = false
	inodeLockRequestB.addThisLock()
	if len(inodeLockRequestB.locksHeld) != 1 {
		t.Fatalf("len(inodeLockRequestB.locksHeld) (%v) should have been == 1", len(inodeLockRequestB.locksHeld))
	}
	inodeHeldLock, ok = inodeLockRequestB.locksHeld[testInodeLockInodeNumberA]
	if !ok {
		t.Fatalf("inodeLockRequestB.locksHeld[testInodeLockInodeNumberA] returned !ok")
	}
	if inodeHeldLock.inode.inodeNumber != testInodeLockInodeNumberA {
		t.Fatalf("inodeHeldLock.inode.inodeNumber (%v) != testInodeLockInodeNumberA (%v)", inodeHeldLock.inode.inodeNumber, testInodeLockInodeNumberA)
	}
	if inodeHeldLock.exclusive {
		t.Fatalf("inodeHeldLock.exclusive should have been false")
	}

	// inodeLockRequestC = newLockRequest()
	// inodeLockRequestC.inodeNumber = testInodeLockInodeNumberB
	// inodeLockRequestC.exclusive = false
	// inodeLockRequestC.addThisLock()
	// if len(inodeLockRequestC.locksHeld) != 0 {
	// 	t.Fatalf("len(inodeLockRequestB.locksHeld) (%v) should have been == 0", len(inodeLockRequestB.locksHeld))
	// }

	// inodeLockRequestC.exclusive = true
	// inodeLockRequestC.addThisLock()
	// if len(inodeLockRequestC.locksHeld) != 0 {
	// 	t.Fatalf("len(inodeLockRequestB.locksHeld) (%v) should have been == 0", len(inodeLockRequestB.locksHeld))
	// }

	inodeLockRequestA.unlockAll()
	if len(inodeLockRequestA.locksHeld) != 0 {
		t.Fatalf("len(inodeLockRequestA.locksHeld) (%v) should have been == 0", len(inodeLockRequestA.locksHeld))
	}

	inodeLockRequestB.unlockAll()
	if len(inodeLockRequestB.locksHeld) != 0 {
		t.Fatalf("len(inodeLockRequestB.locksHeld) (%v) should have been == 0", len(inodeLockRequestB.locksHeld))
	}

	t.Logf("TODO... actually test locks in func TestLocks() above")

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
