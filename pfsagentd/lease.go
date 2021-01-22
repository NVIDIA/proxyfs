// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"container/list"
	"encoding/json"
	"time"

	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
)

func (dummy *globalsStruct) Interrupt(rpcInterruptBuf []byte) {
	var (
		err          error
		fileInode    *fileInodeStruct
		ok           bool
		rpcInterrupt *jrpcfs.RPCInterrupt
	)

	rpcInterrupt = &jrpcfs.RPCInterrupt{}

	err = json.Unmarshal(rpcInterruptBuf, rpcInterrupt)
	if nil != err {
		logFatalf("(*globalsStruct).Interrupt() call to json.Unmarshal() failed: %v", err)
	}

	switch rpcInterrupt.RPCInterruptType {
	case jrpcfs.RPCInterruptTypeUnmount:
		logFatalf("UNSUPPORTED: (*globalsStruct).Interrupt() received jrpcfs.RPCInterruptTypeUnmount")
	case jrpcfs.RPCInterruptTypeDemote:
	case jrpcfs.RPCInterruptTypeRelease:
	default:
		logFatalf("(*globalsStruct).Interrupt() received unknown rpcInterrupt.RPCInterruptType: %v", rpcInterrupt.RPCInterruptType)
	}

	globals.Lock()

	fileInode, ok = globals.fileInodeMap[inode.InodeNumber(rpcInterrupt.InodeNumber)]
	if !ok {
		globals.Unlock()
		return
	}

	switch fileInode.leaseState {
	case fileInodeLeaseStateNone:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
	case fileInodeLeaseStateSharedRequested:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		fileInode.pendingLeaseInterrupt = &rpcInterrupt.RPCInterruptType
	case fileInodeLeaseStateSharedGranted:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		if nil == fileInode.lockWaiters {
			// Shared Lease... with nobody currently referencing it
			fileInode.lockWaiters = list.New()
			if jrpcfs.RPCInterruptTypeDemote == rpcInterrupt.RPCInterruptType {
				// We can safely ignore it
			} else { // jrpcfs.RPCInterruptTypeRelease == rpcInterrupt.RPCInterruptType
				fileInode.leaseState = fileInodeLeaseStateSharedReleasing
				_ = globals.sharedLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
				fileInode.leaseListElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)
				globals.Unlock()
				// TODO - invalidate cached state (cachedStat and extentMap)
				// TODO - inform ProxyFS that we'd like to Release our Shared Lease
				// TODO - finally service any waiters that have arrived (or delete lockWaiters)
			}
			return
		}
		fileInode.pendingLeaseInterrupt = &rpcInterrupt.RPCInterruptType
	case fileInodeLeaseStateSharedPromoting:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		fileInode.pendingLeaseInterrupt = &rpcInterrupt.RPCInterruptType
	case fileInodeLeaseStateSharedReleasing:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		fileInode.pendingLeaseInterrupt = &rpcInterrupt.RPCInterruptType
	case fileInodeLeaseStateExclusiveRequested:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		fileInode.pendingLeaseInterrupt = &rpcInterrupt.RPCInterruptType
	case fileInodeLeaseStateExclusiveGranted:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		if nil == fileInode.lockWaiters {
			// Exclusive Lease... with nobody currently referencing it
			if jrpcfs.RPCInterruptTypeDemote == rpcInterrupt.RPCInterruptType {
				fileInode.leaseState = fileInodeLeaseStateExclusiveDemoting
				_ = globals.exclusiveLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
				fileInode.leaseListElement = globals.sharedLeaseFileInodeCacheLRU.PushBack(fileInode)
				globals.Unlock()
				// TODO - flush dirty state (chunkedPutList)
				// TODO - inform ProxyFS that we'd like to Demote our Exclusive Lease
				// TODO - finally service any waiters that have arrived (or delete lockWaiters)
			} else { // jrpcfs.RPCInterruptTypeRelease == rpcInterrupt.RPCInterruptType
				fileInode.leaseState = fileInodeLeaseStateExclusiveReleasing
				_ = globals.exclusiveLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
				fileInode.leaseListElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)
				globals.Unlock()
				// TODO - flush dirty state (chunkedPutList)
				// TODO - invalidate cached state (cachedStat and extentMap)
				// TODO - inform ProxyFS that we'd like to Release our Exclusive Lease
				// TODO - finally service any waiters that have arrived (or delete lockWaiters)
			}
			return
		}
		fileInode.pendingLeaseInterrupt = &rpcInterrupt.RPCInterruptType
	case fileInodeLeaseStateExclusiveDemoting:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		fileInode.pendingLeaseInterrupt = &rpcInterrupt.RPCInterruptType
	case fileInodeLeaseStateExclusiveReleasing:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		fileInode.pendingLeaseInterrupt = &rpcInterrupt.RPCInterruptType
	default:
		logFatalf("(*globalsStruct).Interrupt() found unknown fileInode.leaseState: %v", fileInode.leaseState)
	}

	// If fileInode.pendingLeaseInterrupt was set, it'll be serviced in (*fileInodeStruct).unlock()

	globals.Unlock()
}

func lockInodeWithSharedLease(inodeNumber inode.InodeNumber) (fileInode *fileInodeStruct) {
	var (
		err                   error
		leaseReply            *jrpcfs.LeaseReply
		leaseRequest          *jrpcfs.LeaseRequest
		leaseRequestEndTime   time.Time
		leaseRequestStartTime time.Time
		ok                    bool
		waitChan              chan struct{}
	)

	globals.Lock()

	fileInode, ok = globals.fileInodeMap[inodeNumber]
	if !ok {
		fileInode = &fileInodeStruct{
			InodeNumber:               inodeNumber,
			cachedStat:                nil,
			lockWaiters:               nil,
			leaseState:                fileInodeLeaseStateNone,
			pendingLeaseInterrupt:     nil,
			leaseListElement:          nil,
			extentMap:                 nil,
			chunkedPutList:            list.New(),
			flushInProgress:           false,
			chunkedPutFlushWaiterList: list.New(),
			dirtyListElement:          nil,
		}

		fileInode.leaseListElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)
		globals.fileInodeMap[inodeNumber] = fileInode
	}

	switch fileInode.leaseState {
	case fileInodeLeaseStateNone:
		if nil == fileInode.lockWaiters {
			// No Lease currently held... so ask for a Shared Lease

			fileInode.lockWaiters = list.New()
			fileInode.leaseState = fileInodeLeaseStateSharedRequested
			_ = globals.unleasedFileInodeCacheLRU.Remove(fileInode.leaseListElement)
			fileInode.leaseListElement = globals.sharedLeaseFileInodeCacheLRU.PushBack(fileInode)

			globals.Unlock()

			leaseRequest = &jrpcfs.LeaseRequest{
				InodeHandle: jrpcfs.InodeHandle{
					MountID:     globals.mountID,
					InodeNumber: int64(inodeNumber),
				},
				LeaseRequestType: jrpcfs.LeaseRequestTypeShared,
			}
			leaseReply = &jrpcfs.LeaseReply{}

			leaseRequestStartTime = time.Now()
			err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
			if nil != err {
				logFatalf("lockInodeWithSharedLease() unable to obtain Shared Lease [case 1] - retryRPCClient.Send() failed: %v", err)
			}
			leaseRequestEndTime = time.Now()
			globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

			// Record leaseReply.LeaseReplyType

			globals.Lock()

			switch leaseReply.LeaseReplyType {
			case jrpcfs.LeaseReplyTypeShared:
				fileInode.leaseState = fileInodeLeaseStateSharedGranted
				globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
			case jrpcfs.LeaseReplyTypeExclusive:
				fileInode.leaseState = fileInodeLeaseStateExclusiveGranted
				_ = globals.sharedLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
				fileInode.leaseListElement = globals.exclusiveLeaseFileInodeCacheLRU.PushBack(fileInode)
			default:
				logFatalf("lockInodeWithSharedLease() unable to obtain Shared Lease [case 2] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
			}

			// Now that we have either a Shared or Exclusive Lease, we are also the first in line to use it - so just return

			globals.Unlock()

			return
		}

		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateSharedRequested:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateSharedGranted:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		if nil == fileInode.lockWaiters {
			// We already have a Shared Lease and we are also the first in line to use it

			fileInode.lockWaiters = list.New()

			globals.Unlock()

			return
		}

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateSharedPromoting:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateSharedReleasing:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateExclusiveRequested:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateExclusiveGranted:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		if nil == fileInode.lockWaiters {
			// We already have an Exclusive Lease and we are also the first in line to use it

			fileInode.lockWaiters = list.New()

			globals.Unlock()

			return
		}

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateExclusiveDemoting:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateExclusiveReleasing:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	default:
		logFatalf("lockInodeWithSharedLease() found unknown fileInode.leaseState [case 1]: %v", fileInode.leaseState)
	}

	// Time to block

	waitChan = make(chan struct{})
	_ = fileInode.lockWaiters.PushBack(waitChan)
	globals.Unlock()
	_ = <-waitChan

	// Time to check fileInode.leaseState again [(*fileInodeStruct).unlock() may have changed fileInode.leaseState)

	globals.Lock()

	switch fileInode.leaseState {
	case fileInodeLeaseStateNone:
		// No Lease currently held... so ask for a Shared Lease

		fileInode.leaseState = fileInodeLeaseStateSharedRequested
		_ = globals.unleasedFileInodeCacheLRU.Remove(fileInode.leaseListElement)
		fileInode.leaseListElement = globals.sharedLeaseFileInodeCacheLRU.PushBack(fileInode)

		globals.Unlock()

		leaseRequest = &jrpcfs.LeaseRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(inodeNumber),
			},
			LeaseRequestType: jrpcfs.LeaseRequestTypeShared,
		}
		leaseReply = &jrpcfs.LeaseReply{}

		leaseRequestStartTime = time.Now()
		err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
		if nil != err {
			logFatalf("lockInodeWithSharedLease() unable to obtain Shared Lease [case 3] - retryRPCClient.Send() failed: %v", err)
		}
		leaseRequestEndTime = time.Now()
		globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

		// Record leaseReply.LeaseReplyType

		globals.Lock()

		switch leaseReply.LeaseReplyType {
		case jrpcfs.LeaseReplyTypeShared:
			fileInode.leaseState = fileInodeLeaseStateSharedGranted
			globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		case jrpcfs.LeaseReplyTypeExclusive:
			fileInode.leaseState = fileInodeLeaseStateExclusiveGranted
			_ = globals.sharedLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
			fileInode.leaseListElement = globals.exclusiveLeaseFileInodeCacheLRU.PushBack(fileInode)
		default:
			logFatalf("lockInodeWithSharedLease() unable to obtain Shared Lease [case 4] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
		}

		// Now that we have a Shared or Exclusive Lease, we are also the first in line to use it - so we can grant the Lock
	case fileInodeLeaseStateSharedRequested:
		logFatalf("lockInodeWithSharedLease() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedRequested")
	case fileInodeLeaseStateSharedGranted:
		// Now that we have a Shared Lease, we are also the first in line to use it - so we can grant the Lock
	case fileInodeLeaseStateSharedPromoting:
		logFatalf("lockInodeWithSharedLease() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedPromoting")
	case fileInodeLeaseStateSharedReleasing:
		logFatalf("lockInodeWithSharedLease() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedReleasing")
	case fileInodeLeaseStateExclusiveRequested:
		logFatalf("lockInodeWithSharedLease() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveRequested")
	case fileInodeLeaseStateExclusiveGranted:
		// Now that we have an Exclusive Lease, we are also the first in line to use it - so we can grant the Lock
	case fileInodeLeaseStateExclusiveDemoting:
		logFatalf("lockInodeWithSharedLease() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveDemoting")
	case fileInodeLeaseStateExclusiveReleasing:
		logFatalf("lockInodeWithSharedLease() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveReleasing")
	default:
		logFatalf("lockInodeWithSharedLease() found unknown fileInode.leaseState [case 2]: %v", fileInode.leaseState)
	}

	globals.Unlock()

	return
}

// lockInodeIfExclusiveLeaseGranted is similar to lockInodeWithExclusiveLease() except that
// if an ExclusiveLease is not Granted, it will return nil. Note that callers may also be
// assured any inode state is not cached and dirty in the non-Granted cases. As such, if
// an ExclusiveLease is either being Demoted or Released, lockInodeIfExclusiveLeaseGranted()
// will block until the the Demote or Release has been completed.
//
func lockInodeIfExclusiveLeaseGranted(inodeNumber inode.InodeNumber) (fileInode *fileInodeStruct) {
	var (
		keepLock bool
		ok       bool
		waitChan chan struct{}
	)

	globals.Lock()

	fileInode, ok = globals.fileInodeMap[inodeNumber]
	if !ok {
		globals.Unlock()
		fileInode = nil
		return
	}

	switch fileInode.leaseState {
	case fileInodeLeaseStateNone:
		globals.Unlock()
		fileInode = nil
		return
	case fileInodeLeaseStateSharedRequested:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		globals.Unlock()
		fileInode = nil
		return
	case fileInodeLeaseStateSharedGranted:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		globals.Unlock()
		fileInode = nil
		return
	case fileInodeLeaseStateSharedPromoting:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		globals.Unlock()
		fileInode = nil
		return
	case fileInodeLeaseStateSharedReleasing:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		globals.Unlock()
		fileInode = nil
		return
	case fileInodeLeaseStateExclusiveRequested:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		globals.Unlock()
		fileInode = nil
		return
	case fileInodeLeaseStateExclusiveGranted:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		keepLock = true
	case fileInodeLeaseStateExclusiveDemoting:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		keepLock = false
	case fileInodeLeaseStateExclusiveReleasing:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)
		keepLock = false
	default:
		logFatalf("lockInodeIfExclusiveLeaseGranted() found unknown fileInode.leaseState [case 1]: %v", fileInode.leaseState)
	}

	if nil == fileInode.lockWaiters {
		fileInode.lockWaiters = list.New()
	} else {
		waitChan = make(chan struct{})
		_ = fileInode.lockWaiters.PushBack(waitChan)
		globals.Unlock()
		_ = <-waitChan
		globals.Lock()
	}

	if keepLock {
		// Time to check fileInode.leaseState again [(*fileInodeStruct).unlock() may have changed fileInode.leaseState)

		switch fileInode.leaseState {
		case fileInodeLeaseStateNone:
			globals.Unlock()
			fileInode.unlock(false)
			fileInode = nil
		case fileInodeLeaseStateSharedRequested:
			logFatalf("lockInodeIfExclusiveLeaseGranted() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedRequested")
		case fileInodeLeaseStateSharedGranted:
			globals.Unlock()
			fileInode.unlock(false)
			fileInode = nil
		case fileInodeLeaseStateSharedPromoting:
			logFatalf("lockInodeIfExclusiveLeaseGranted() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedPromoting")
		case fileInodeLeaseStateSharedReleasing:
			logFatalf("lockInodeIfExclusiveLeaseGranted() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedReleasing")
		case fileInodeLeaseStateExclusiveRequested:
			logFatalf("lockInodeIfExclusiveLeaseGranted() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveRequested")
		case fileInodeLeaseStateExclusiveGranted:
			// Now that we have an Exclusive Lease, we are also the first in line to use it - so we can grant the Lock
			globals.Unlock()
		case fileInodeLeaseStateExclusiveDemoting:
			logFatalf("lockInodeIfExclusiveLeaseGranted() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveDemoting")
		case fileInodeLeaseStateExclusiveReleasing:
			logFatalf("lockInodeIfExclusiveLeaseGranted() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveReleasing")
		default:
			logFatalf("lockInodeIfExclusiveLeaseGranted() found unknown fileInode.leaseState [case 2]: %v", fileInode.leaseState)
		}
	} else {
		globals.Unlock()
		fileInode.unlock(false)
		fileInode = nil
	}

	return
}

func lockInodeWithExclusiveLease(inodeNumber inode.InodeNumber) (fileInode *fileInodeStruct) {
	var (
		err                   error
		leaseReply            *jrpcfs.LeaseReply
		leaseRequest          *jrpcfs.LeaseRequest
		leaseRequestEndTime   time.Time
		leaseRequestStartTime time.Time
		ok                    bool
		waitChan              chan struct{}
	)

	globals.Lock()

	fileInode, ok = globals.fileInodeMap[inodeNumber]
	if !ok {
		fileInode = &fileInodeStruct{
			InodeNumber:               inodeNumber,
			cachedStat:                nil,
			lockWaiters:               nil,
			leaseState:                fileInodeLeaseStateNone,
			pendingLeaseInterrupt:     nil,
			leaseListElement:          nil,
			extentMap:                 nil,
			chunkedPutList:            list.New(),
			flushInProgress:           false,
			chunkedPutFlushWaiterList: list.New(),
			dirtyListElement:          nil,
		}

		fileInode.leaseListElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)
		globals.fileInodeMap[inodeNumber] = fileInode
	}

	switch fileInode.leaseState {
	case fileInodeLeaseStateNone:
		if nil == fileInode.lockWaiters {
			// No Lease currently held... so ask for an Exclusive Lease

			fileInode.lockWaiters = list.New()
			fileInode.leaseState = fileInodeLeaseStateExclusiveRequested
			_ = globals.unleasedFileInodeCacheLRU.Remove(fileInode.leaseListElement)
			fileInode.leaseListElement = globals.exclusiveLeaseFileInodeCacheLRU.PushBack(fileInode)

			globals.Unlock()

			leaseRequest = &jrpcfs.LeaseRequest{
				InodeHandle: jrpcfs.InodeHandle{
					MountID:     globals.mountID,
					InodeNumber: int64(inodeNumber),
				},
				LeaseRequestType: jrpcfs.LeaseRequestTypeExclusive,
			}
			leaseReply = &jrpcfs.LeaseReply{}

			leaseRequestStartTime = time.Now()
			err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
			if nil != err {
				logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 1] - retryRPCClient.Send() failed: %v", err)
			}
			leaseRequestEndTime = time.Now()
			globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

			// Record leaseReply.LeaseReplyType

			globals.Lock()

			switch leaseReply.LeaseReplyType {
			case jrpcfs.LeaseReplyTypeExclusive:
				fileInode.leaseState = fileInodeLeaseStateExclusiveGranted
			default:
				logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 2] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
			}

			// Now that we have an Exclusive Lease, we are also the first in line to use it - so just return

			globals.Unlock()

			return
		}

		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateSharedRequested:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateSharedGranted:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		if nil == fileInode.lockWaiters {
			// We already have a Shared Lease and we are also the first in line to use it
			// TODO: We need to promote it first... which could fail !!!

			fileInode.lockWaiters = list.New()

			globals.Unlock()

			return
		}

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateSharedPromoting:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateSharedReleasing:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateExclusiveRequested:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateExclusiveGranted:
		globals.exclusiveLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		if nil == fileInode.lockWaiters {
			// We already have an Exclusive Lease and we are also the first in line to use it

			fileInode.lockWaiters = list.New()

			globals.Unlock()

			return
		}

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateExclusiveDemoting:
		globals.sharedLeaseFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	case fileInodeLeaseStateExclusiveReleasing:
		globals.unleasedFileInodeCacheLRU.MoveToBack(fileInode.leaseListElement)

		// Somebody else is ahead of us... so fall through

	default:
		logFatalf("lockInodeWithExclusiveLease() found unknown fileInode.leaseState [case 1]: %v", fileInode.leaseState)
	}

	// Time to block

	waitChan = make(chan struct{})
	_ = fileInode.lockWaiters.PushBack(waitChan)
	globals.Unlock()
	_ = <-waitChan

	// Time to check fileInode.leaseState again [(*fileInodeStruct).unlock() may have changed fileInode.leaseState)

	globals.Lock()

	switch fileInode.leaseState {
	case fileInodeLeaseStateNone:
		// No Lease currently held... so ask for an Exclusive Lease

		fileInode.leaseState = fileInodeLeaseStateExclusiveRequested
		_ = globals.unleasedFileInodeCacheLRU.Remove(fileInode.leaseListElement)
		fileInode.leaseListElement = globals.exclusiveLeaseFileInodeCacheLRU.PushBack(fileInode)

		globals.Unlock()

		leaseRequest = &jrpcfs.LeaseRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(inodeNumber),
			},
			LeaseRequestType: jrpcfs.LeaseRequestTypeExclusive,
		}
		leaseReply = &jrpcfs.LeaseReply{}

		leaseRequestStartTime = time.Now()
		err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
		if nil != err {
			logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 3] - retryRPCClient.Send() failed: %v", err)
		}
		leaseRequestEndTime = time.Now()
		globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

		// Record leaseReply.LeaseReplyType

		globals.Lock()

		switch leaseReply.LeaseReplyType {
		case jrpcfs.LeaseReplyTypeExclusive:
			fileInode.leaseState = fileInodeLeaseStateExclusiveGranted
		default:
			logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 4] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
		}

		// Now that we have an Exclusive Lease, we are also the first in line to use it - so we can grant the Lock
	case fileInodeLeaseStateSharedRequested:
		logFatalf("lockInodeWithExclusiveLease() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedRequested")
	case fileInodeLeaseStateSharedGranted:
		// Now that we have a Shared Lease, we are also the first in line to use it - so we need to Promote the Lease

		fileInode.leaseState = fileInodeLeaseStateSharedPromoting
		_ = globals.sharedLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
		fileInode.leaseListElement = globals.exclusiveLeaseFileInodeCacheLRU.PushBack(fileInode)

		globals.Unlock()

		leaseRequest = &jrpcfs.LeaseRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(inodeNumber),
			},
			LeaseRequestType: jrpcfs.LeaseRequestTypePromote,
		}
		leaseReply = &jrpcfs.LeaseReply{}

		leaseRequestStartTime = time.Now()
		err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
		if nil != err {
			logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 5] - retryRPCClient.Send() failed: %v", err)
		}
		leaseRequestEndTime = time.Now()
		globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

		// Record leaseReply.LeaseReplyType

		globals.Lock()

		switch leaseReply.LeaseReplyType {
		case jrpcfs.LeaseReplyTypeExclusive:
			fileInode.leaseState = fileInodeLeaseStateExclusiveGranted

			// Now that we have an Exclusive Lease, we are also the first in line to use it - so we can grant the Lock
		case jrpcfs.LeaseReplyTypeDenied:
			// We need to Release first... and may have been interrupted to do so already

			// TODO - invalidate cached state (cachedStat and extentMap)

			fileInode.pendingLeaseInterrupt = nil

			fileInode.leaseState = fileInodeLeaseStateSharedReleasing
			_ = globals.exclusiveLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
			fileInode.leaseListElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)

			globals.Unlock()

			leaseRequest = &jrpcfs.LeaseRequest{
				InodeHandle: jrpcfs.InodeHandle{
					MountID:     globals.mountID,
					InodeNumber: int64(inodeNumber),
				},
				LeaseRequestType: jrpcfs.LeaseRequestTypeRelease,
			}
			leaseReply = &jrpcfs.LeaseReply{}

			leaseRequestStartTime = time.Now()
			err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
			if nil != err {
				logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 6] - retryRPCClient.Send() failed: %v", err)
			}
			leaseRequestEndTime = time.Now()
			globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

			globals.Lock()

			switch leaseReply.LeaseReplyType {
			case jrpcfs.LeaseReplyTypeReleased:
				// Now we can obtain Exclusive Lease

				fileInode.leaseState = fileInodeLeaseStateExclusiveRequested
				_ = globals.unleasedFileInodeCacheLRU.Remove(fileInode.leaseListElement)
				fileInode.leaseListElement = globals.exclusiveLeaseFileInodeCacheLRU.PushBack(fileInode)

				globals.Unlock()

				leaseRequest = &jrpcfs.LeaseRequest{
					InodeHandle: jrpcfs.InodeHandle{
						MountID:     globals.mountID,
						InodeNumber: int64(inodeNumber),
					},
					LeaseRequestType: jrpcfs.LeaseRequestTypeRelease,
				}
				leaseReply = &jrpcfs.LeaseReply{}

				leaseRequestStartTime = time.Now()
				err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
				if nil != err {
					logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 7] - retryRPCClient.Send() failed: %v", err)
				}
				leaseRequestEndTime = time.Now()
				globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

				globals.Lock()

				switch leaseReply.LeaseReplyType {
				case jrpcfs.LeaseReplyTypeExclusive:
					fileInode.leaseState = fileInodeLeaseStateExclusiveGranted
				default:
					logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 8] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
				}

				// Now that we have an Exclusive Lease, we are also the first in line to use it - so we can grant the Lock
			default:
				logFatalf("lockInodeWithExclusiveLease() unable to release Shared Lease - LeaseReplyType == %v", leaseReply.LeaseReplyType)
			}
		default:
			logFatalf("lockInodeWithExclusiveLease() unable to obtain Exclusive Lease [case 8] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
		}
	case fileInodeLeaseStateSharedPromoting:
		logFatalf("lockInodeWithExclusiveLease() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedPromoting")
	case fileInodeLeaseStateSharedReleasing:
		logFatalf("lockInodeWithExclusiveLease() found unexpected fileInode.leaseState: fileInodeLeaseStateSharedReleasing")
	case fileInodeLeaseStateExclusiveRequested:
		logFatalf("lockInodeWithExclusiveLease() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveRequested")
	case fileInodeLeaseStateExclusiveGranted:
		// Now that we have an Exclusive Lease, we are also the first in line to use it - so we can grant the Lock
	case fileInodeLeaseStateExclusiveDemoting:
		logFatalf("lockInodeWithExclusiveLease() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveDemoting")
	case fileInodeLeaseStateExclusiveReleasing:
		logFatalf("lockInodeWithExclusiveLease() found unexpected fileInode.leaseState: fileInodeLeaseStateExclusiveReleasing")
	default:
		logFatalf("lockInodeWithExclusiveLease() found unknown fileInode.leaseState [case 2]: %v", fileInode.leaseState)
	}

	globals.Unlock()

	return
}

func (fileInode *fileInodeStruct) unlock(forceRelease bool) {
	var (
		err                           error
		forcedRPCInterruptTypeRelease jrpcfs.RPCInterruptType
		leaseReply                    *jrpcfs.LeaseReply
		leaseRequest                  *jrpcfs.LeaseRequest
		leaseRequestEndTime           time.Time
		leaseRequestStartTime         time.Time
		lockWaitersListElement        *list.Element
		waitChan                      chan struct{}
	)

	globals.Lock()

	if forceRelease {
		// The unlock() call was likely made for a non-existing Inode
		// in which case we might as well clear out both the current
		// Lease as well as the fileInodeStruct (if not being referenced)

		forcedRPCInterruptTypeRelease = jrpcfs.RPCInterruptTypeRelease
		fileInode.pendingLeaseInterrupt = &forcedRPCInterruptTypeRelease
	}

	for nil != fileInode.pendingLeaseInterrupt {
		switch *fileInode.pendingLeaseInterrupt {
		case jrpcfs.RPCInterruptTypeUnmount:
			logFatalf("UNSUPPORTED: (*fileInodeStruct).unlock() received jrpcfs.RPCInterruptTypeUnmount")
		case jrpcfs.RPCInterruptTypeDemote:
			fileInode.pendingLeaseInterrupt = nil

			if fileInodeLeaseStateExclusiveGranted == fileInode.leaseState {
				fileInode.leaseState = fileInodeLeaseStateExclusiveDemoting
				_ = globals.exclusiveLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
				fileInode.leaseListElement = globals.sharedLeaseFileInodeCacheLRU.PushBack(fileInode)

				globals.Unlock()

				// TODO - flush dirty state (chunkedPutList)

				leaseRequest = &jrpcfs.LeaseRequest{
					InodeHandle: jrpcfs.InodeHandle{
						MountID:     globals.mountID,
						InodeNumber: int64(fileInode.InodeNumber),
					},
					LeaseRequestType: jrpcfs.LeaseRequestTypeDemote,
				}
				leaseReply = &jrpcfs.LeaseReply{}

				leaseRequestStartTime = time.Now()
				err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
				if nil != err {
					logFatalf("(*fileInodeStruct).unlock() unable to Demote Exclusive Lease [case 1] - retryRPCClient.Send() failed: %v", err)
				}
				leaseRequestEndTime = time.Now()
				globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

				// Record leaseReply.LeaseReplyType

				globals.Lock()

				switch leaseReply.LeaseReplyType {
				case jrpcfs.LeaseReplyTypeDemoted:
					fileInode.leaseState = fileInodeLeaseStateSharedGranted
				default:
					logFatalf("(*fileInodeStruct).unlock() unable to Demote Exclusive Lease [case 2] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
				}
			}
		case jrpcfs.RPCInterruptTypeRelease:
			fileInode.pendingLeaseInterrupt = nil

			if fileInodeLeaseStateSharedGranted == fileInode.leaseState {
				fileInode.leaseState = fileInodeLeaseStateSharedReleasing
				_ = globals.sharedLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
				fileInode.leaseListElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)

				globals.Unlock()

				// TODO - flush dirty state (chunkedPutList)

				leaseRequest = &jrpcfs.LeaseRequest{
					InodeHandle: jrpcfs.InodeHandle{
						MountID:     globals.mountID,
						InodeNumber: int64(fileInode.InodeNumber),
					},
					LeaseRequestType: jrpcfs.LeaseRequestTypeRelease,
				}
				leaseReply = &jrpcfs.LeaseReply{}

				leaseRequestStartTime = time.Now()
				err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
				if nil != err {
					logFatalf("(*fileInodeStruct).unlock() unable to Demote Exclusive Lease [case 3] - retryRPCClient.Send() failed: %v", err)
				}
				leaseRequestEndTime = time.Now()
				globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

				// Record leaseReply.LeaseReplyType

				globals.Lock()

				switch leaseReply.LeaseReplyType {
				case jrpcfs.LeaseReplyTypeReleased:
					fileInode.leaseState = fileInodeLeaseStateNone
				default:
					logFatalf("(*fileInodeStruct).unlock() unable to Demote Exclusive Lease [case 4] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
				}
			} else if fileInodeLeaseStateExclusiveGranted == fileInode.leaseState {
				fileInode.leaseState = fileInodeLeaseStateExclusiveReleasing
				_ = globals.exclusiveLeaseFileInodeCacheLRU.Remove(fileInode.leaseListElement)
				fileInode.leaseListElement = globals.unleasedFileInodeCacheLRU.PushBack(fileInode)

				globals.Unlock()

				// TODO - invalidate cached state (cachedStat and extentMap)
				// TODO - flush dirty state (chunkedPutList)

				leaseRequest = &jrpcfs.LeaseRequest{
					InodeHandle: jrpcfs.InodeHandle{
						MountID:     globals.mountID,
						InodeNumber: int64(fileInode.InodeNumber),
					},
					LeaseRequestType: jrpcfs.LeaseRequestTypeRelease,
				}
				leaseReply = &jrpcfs.LeaseReply{}

				leaseRequestStartTime = time.Now()
				err = globals.retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
				if nil != err {
					logFatalf("(*fileInodeStruct).unlock() unable to Demote Exclusive Lease [case 5] - retryRPCClient.Send() failed: %v", err)
				}
				leaseRequestEndTime = time.Now()
				globals.stats.LeaseRequests_Shared_Usec.Add(uint64(leaseRequestEndTime.Sub(leaseRequestStartTime) / time.Microsecond))

				// Record leaseReply.LeaseReplyType

				globals.Lock()

				switch leaseReply.LeaseReplyType {
				case jrpcfs.LeaseReplyTypeReleased:
					fileInode.leaseState = fileInodeLeaseStateNone
				default:
					logFatalf("(*fileInodeStruct).unlock() unable to Demote Exclusive Lease [case 6] - LeaseReplyType == %v", leaseReply.LeaseReplyType)
				}
			}
		default:
			logFatalf("(*fileInodeStruct).unlock() received unknown rpcInterrupt.RPCInterruptType: %v", *fileInode.pendingLeaseInterrupt)
		}

		// We need to loop here in case another Interrupt arrived while
		// doing any of the above Demotions/Releases while globals unlocked
	}

	if 0 == fileInode.lockWaiters.Len() {
		// Nobody was waiting for the fileInode lock

		if fileInodeLeaseStateNone == fileInode.leaseState {
			// And there is no held Lease... so just destroy it

			delete(globals.fileInodeMap, fileInode.InodeNumber)
			_ = globals.unleasedFileInodeCacheLRU.Remove(fileInode.leaseListElement)
		} else {
			// We should keep holding the Lease we have... but indicate the lock is available

			fileInode.lockWaiters = nil
		}

		globals.Unlock()
		return
	}

	// At least one caller to lockInodeWith{Shared|Exclusive}Lease() is waiting... so wake them up before exiting

	lockWaitersListElement = fileInode.lockWaiters.Front()
	fileInode.lockWaiters.Remove(lockWaitersListElement)

	globals.Unlock()

	waitChan = lockWaitersListElement.Value.(chan struct{})

	waitChan <- struct{}{}
}
