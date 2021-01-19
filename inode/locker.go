// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package inode

// Lock-related wrappers for inodes
//
// These APIs wrap calls to package DLM, which is a generic locking package.
//
// These wrappers enforce a naming convention for lock IDs.
//

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/dlm"
)

// MakeLockID creates the ID of an inode used in volume
func (vS *volumeStruct) MakeLockID(inodeNumber InodeNumber) (lockID string, err error) {
	myLockID := fmt.Sprintf("vol.%s:ino.%d", vS.volumeName, inodeNumber)

	return myLockID, nil
}

// InitInodeLock creates an inode lock. If callerID is non-nil, it is used.
// Otherwise a new callerID is allocated.
func (vS *volumeStruct) InitInodeLock(inodeNumber InodeNumber, callerID dlm.CallerID) (lock *dlm.RWLockStruct, err error) {
	lockID, err := vS.MakeLockID(inodeNumber)
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

// GetReadLock is a convenience function to create and acquire an inode lock
func (vS *volumeStruct) GetReadLock(inodeNumber InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.InitInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	err = lock.ReadLock()
	return lock, err
}

// GetWriteLock is a convenience function to create and acquire an inode lock
func (vS *volumeStruct) GetWriteLock(inodeNumber InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.InitInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	err = lock.WriteLock()
	return lock, err
}

// AttemptReadLock is a convenience function to create and try to acquire an inode lock
func (vS *volumeStruct) AttemptReadLock(inodeNumber InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.InitInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	err = lock.TryReadLock()
	return lock, err
}

// AttemptWriteLock is a convenience function to create and try to acquire an inode lock
func (vS *volumeStruct) AttemptWriteLock(inodeNumber InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.InitInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	err = lock.TryWriteLock()
	return lock, err
}

// EnsureReadLock ensures that a lock of the right type is held by the given callerID. If the lock is not held, it
// acquires it. If the lock is held, it returns nil (so you don't unlock twice; even if that's not crashworthy, you'd
// still release a lock that other code thinks it still holds).
func (vS *volumeStruct) EnsureReadLock(inodeNumber InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.InitInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	if lock.IsReadHeld() {
		return nil, nil
	}

	err = lock.ReadLock()
	return lock, err
}

// EnsureWriteLock ensures that a lock of the right type is held by the given callerID. If the lock is not held, it
// acquires it. If the lock is held, it returns nil (so you don't unlock twice; even if that's not crashworthy, you'd
// still release a lock that other code thinks it still holds).
func (vS *volumeStruct) EnsureWriteLock(inodeNumber InodeNumber, callerID dlm.CallerID) (*dlm.RWLockStruct, error) {
	lock, err := vS.InitInodeLock(inodeNumber, callerID)
	if err != nil {
		return nil, err
	}

	if lock.IsWriteHeld() {
		return nil, nil
	}

	err = lock.WriteLock()
	return lock, err
}
