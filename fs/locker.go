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

func (vS *volumeStruct) makeLockID(inodeNumber inode.InodeNumber) (lockID string, err error) {
	myLockID := fmt.Sprintf("vol.%s:ino.%d", vS.volumeName, inodeNumber)

	return myLockID, nil
}

// getInodeLock creates an inode lock. If callerID is non-nil, it is used.
// Otherwise a new callerID is allocated.
func (vS *volumeStruct) initInodeLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (lock *dlm.RWLockStruct, err error) {
	lockID, err := vS.makeLockID(inodeNumber)
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
func (vS *volumeStruct) getReadLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.initInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	err = lock.ReadLock()
	if err != nil {
		return nil, err
	}
	return lock, nil
}

func (vS *volumeStruct) getWriteLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.initInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	err = lock.WriteLock()
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// These functions ensure that a lock of the right type is held by the given callerID. If the lock is not held, they
// acquire it. If the lock is held, they return nil (so you don't unlock twice; even if that's not crashworthy, you'd
// still release a lock that other code thinks it still holds).
func (vS *volumeStruct) ensureReadLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.initInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	if lock.IsReadHeld() {
		return nil, nil
	}

	err = lock.ReadLock()
	if err != nil {
		return nil, err
	}
	return lock, nil
}

func (vS *volumeStruct) ensureWriteLock(inodeNumber inode.InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.initInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	if lock.IsWriteHeld() {
		return nil, nil
	}

	err = lock.WriteLock()
	if err != nil {
		return nil, err
	}
	return lock, nil
}
