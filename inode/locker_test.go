// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package inode

import (
	"testing"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/dlm"
)

func TestLockerStuff(t *testing.T) {
	var (
		inodeNumber InodeNumber
	)

	testSetup(t, false)

	inodeNumber = InodeNumber(2)

	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") failed: %v", err)
	}

	volume := testVolumeHandle.(*volumeStruct)

	// Test: get a lock for vol A inode 2
	srcDirLock, err := volume.InitInodeLock(inodeNumber, nil)
	if err != nil {
		t.Fatalf("Failed to initInodeLock: %v", err)
	}
	srcCallerID := srcDirLock.GetCallerID()

	// Test: get a lock for vol A inode 3
	inodeNumber++
	dstDirLock, err := volume.InitInodeLock(inodeNumber, nil)
	if err != nil {
		t.Fatalf("Failed to initInodeLock: %v", err)
	}
	dstCallerID := dstDirLock.GetCallerID()

	// Check that the caller IDs are different
	if *dstCallerID == *srcCallerID {
		t.Fatalf("Caller IDs should be different!")
	}

	// Now try to lock something
	srcDirLock.ReadLock()
	srcDirLock.Unlock()

	// Simulate multi-lock move sequence
	var srcDirInodeNumber InodeNumber = 9001
	var dstDirInodeNumber InodeNumber = 9002
	callerID := dlm.GenerateCallerID()
	srcDirLock, err = volume.InitInodeLock(srcDirInodeNumber, callerID)
	if err != nil {
		return
	}
	srcCallerID = srcDirLock.GetCallerID()

	dstDirLock, err = volume.InitInodeLock(dstDirInodeNumber, callerID)
	if err != nil {
		return
	}
	dstCallerID = dstDirLock.GetCallerID()

	// Check that the caller IDs are the same
	if *dstCallerID != *srcCallerID {
		t.Fatalf("Caller IDs should be the same!")
	}

retryLock:
	err = srcDirLock.WriteLock()
	if err != nil {
		return
	}

	err = dstDirLock.TryWriteLock()
	if blunder.Is(err, blunder.TryAgainError) {
		srcDirLock.Unlock()
		goto retryLock
	}

	// Here is where we would do our move

	dstDirLock.Unlock()
	srcDirLock.Unlock()

	testTeardown(t)
}
