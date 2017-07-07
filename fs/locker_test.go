package fs

import (
	"testing"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/inode"
)

func TestLockerStuff(t *testing.T) {

	var inodeNumber inode.InodeNumber = inode.InodeNumber(2)

	// Test: get a lock for vol A inode 2
	srcDirLock, err := mS.initInodeLock(inodeNumber, nil)
	if err != nil {
		t.Fatalf("Failed to initInodeLock: %v", err)
	}
	srcCallerID := srcDirLock.GetCallerID()

	// Test: get a lock for vol A inode 3
	inodeNumber++
	dstDirLock, err := mS.initInodeLock(inodeNumber, nil)
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
	var srcDirInodeNumber inode.InodeNumber = 9001
	var dstDirInodeNumber inode.InodeNumber = 9002
	callerID := dlm.GenerateCallerID()
	srcDirLock, err = mS.initInodeLock(srcDirInodeNumber, callerID)
	if err != nil {
		return
	}
	srcCallerID = srcDirLock.GetCallerID()

	dstDirLock, err = mS.initInodeLock(dstDirInodeNumber, callerID)
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
}
