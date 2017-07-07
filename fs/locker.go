package fs

// Lock-related wrappers for fs package
//
// These APIs wrap calls to package DLM, which is a generic locking package.
//
// These wrappers enforce a naming convention for lock IDs.
//

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/inode"
)

func (mount *mountStruct) makeLockID(inodeNumber inode.InodeNumber) (lockID string, err error) {
	// Get volume ID from mountID
	//volID := 102
	//myLockID := fmt.Sprintf("vol.%d:ino.%d", volID, inodeNumber)

	// XXX TODO: just using volume name for now
	myLockID := fmt.Sprintf("vol.%s:ino.%d", mount.volumeName, inodeNumber)

	return myLockID, nil
}

// getInodeLock creates an inode lock. If callerID is non-nil, it is used.
// Otherwise a new callerID is allocated.
func (mount *mountStruct) initInodeLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (lock *dlm.RWLockStruct, err error) {

	lockID, err := mount.makeLockID(inodeNumber)
	if err != nil {
		return nil, err
	}

	if callerID == nil {
		callerID = dlm.GenerateCallerID()
	}

	return &dlm.RWLockStruct{LockID: lockID,
		Notify:       nil,
		LockCallerID: callerID,
	}, nil
}

// Convenience functions to create and acquire an inode lock
func (mount *mountStruct) getReadLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := mount.initInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	err = lock.ReadLock()
	return lock, err
}

func (mount *mountStruct) getWriteLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := mount.initInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	err = lock.WriteLock()
	return lock, err
}

// These functions ensure that a lock of the right type is held by the given callerID. If the lock is not held, they
// acquire it. If the lock is held, they return nil (so you don't unlock twice; even if that's not crashworthy, you'd
// still release a lock that other code thinks it still holds).
func (mount *mountStruct) ensureReadLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := mount.initInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	if lock.IsReadHeld() {
		return nil, nil
	}

	err = lock.ReadLock()
	return lock, err
}

func (mount *mountStruct) ensureWriteLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := mount.initInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	if lock.IsWriteHeld() {
		return nil, nil
	}

	err = lock.WriteLock()
	return lock, err
}
