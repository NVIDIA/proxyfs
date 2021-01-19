// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Distributed Lock Manager (DLM) provides locking between threads on the same
// system and between threads on different systems.
//
// Example use of the lock:
/*
type myStruct struct {
	Acct     string
	Volume   string
	InodeNum uint64
	myRwLock *RWLockStruct
	}

func my_function() {
	var myval myStruct
	myval.Acct = "Acct1"
	myval.Volume = "Vol1"
	myval.InodeNum = 11
	myLockId := fmt.Sprintf("%s:%s:%v\n", myval.Acct, myval.Volume, myval.InodeNum)

	myCookie := GenerateCallerID()
	myval.myRwLock = &RWLockStruct{LockID: myLockId, Notify: nil, LockCallerID: myCookie}
	myval.myRwLock.ReadLock()
	myval.myRwLock.Unlock()
	myval.myRwLock.WriteLock()
	myval.myRwLock.Unlock()
	err := myval.myRwLock.TryReadLock()
	errno := blunder.Errno(err)

	switch errno {
	case 0:
		log.Printf("Got TryReadLock")
		myval.myRwLock.Unlock()
	case EAGAIN: // give up other locks.
	default: // something wrong..
	}

	err = myval.myRwLock.TryWriteLock()
	errno := blunder.Errno(err)

	switch errno {
	case 0:
		log.Printf("Got TryWriteLock")
		myval.myRwLock.Unlock()
	case EAGAIN: // give up other locks.
	default: // something wrong..
	}
	}
*/
package dlm

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/trackedlock"
)

type NotifyReason uint32
type CallerID *string

const (
	ReasonWriteRequest NotifyReason = iota + 1 // Another thread in the cluster wants "Write" access
	ReasonReadRequest                          // Another thread wants "Read" access
)

type LockHeldType uint32

const (
	ANYLOCK LockHeldType = iota + 1
	READLOCK
	WRITELOCK
)

type Notify interface {
	NotifyNodeChange(reason NotifyReason) // DLM will call the last node which owns the lock before handing over
	// the lock to another node. Useful for leases and data caching.
}

type RWLockStruct struct {
	LockID       string
	Notify       Notify
	LockCallerID CallerID
}

// Lock for generating unique caller IDs
// For now, this is just an in-memory thing.
var callerIDLock trackedlock.Mutex
var nextCallerID uint64 = 1000

// GenerateCallerID() returns a cluster wide unique number useful in deadlock detection.
func GenerateCallerID() (callerID CallerID) {

	// TODO - we need to use a nonce value instead of this when we have clustering
	callerIDLock.Lock()

	callerIDStr := fmt.Sprintf("%d", nextCallerID)
	callerID = CallerID(&callerIDStr)
	nextCallerID++

	callerIDLock.Unlock()

	return callerID
}

// IsLockHeld() returns
func IsLockHeld(lockID string, callerID CallerID, lockHeldType LockHeldType) (held bool) {
	held = isLockHeld(lockID, callerID, lockHeldType)
	return held
}

// GetLockID() returns the lock ID from the lock struct
func (l *RWLockStruct) GetLockID() string {
	return l.LockID
}

// CallerID returns the caller ID from the lock struct
func (l *RWLockStruct) GetCallerID() CallerID {
	return l.LockCallerID
}

// Returns whether the lock is held for reading
func (l *RWLockStruct) IsReadHeld() bool {
	held := isLockHeld(l.LockID, l.LockCallerID, READLOCK)
	return held
}

// Returns whether the lock is held for writing
func (l *RWLockStruct) IsWriteHeld() bool {
	held := isLockHeld(l.LockID, l.LockCallerID, WRITELOCK)
	return held
}

// WriteLock() blocks until the lock for the inode can be held exclusively.
func (l *RWLockStruct) WriteLock() (err error) {
	// TODO - what errors are possible here?
	err = l.commonLock(exclusive, false)
	return err
}

// ReadLock() blocks until the lock for the inode can be held shared.
func (l *RWLockStruct) ReadLock() (err error) {
	// TODO - what errors are possible here?
	err = l.commonLock(shared, false)
	return err
}

// TryWriteLock() attempts to grab the lock if is is free.  Otherwise, it returns EAGAIN.
func (l *RWLockStruct) TryWriteLock() (err error) {
	err = l.commonLock(exclusive, true)
	return err
}

// TryReadLock() attempts to grab the lock if is is free or shared.  Otherwise, it returns EAGAIN.
func (l *RWLockStruct) TryReadLock() (err error) {
	err = l.commonLock(shared, true)
	return err
}

// Unlock() releases the lock and signals any waiters that the lock is free.
func (l *RWLockStruct) Unlock() (err error) {
	// TODO what error is possible?
	err = l.unlock()
	return err
}
