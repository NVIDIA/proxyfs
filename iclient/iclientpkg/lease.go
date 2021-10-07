// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"container/list"

	"github.com/NVIDIA/proxyfs/imgr/imgrpkg"
)

func startLeaseHandler() (err error) {
	return nil // TODO
}

func stopLeaseHandler() (err error) {
	return nil // TODO
}

// newLockRequest is called to create and initialize an inodeLockRequestStruct.
//
func newLockRequest() (inodeLockRequest *inodeLockRequestStruct) {
	inodeLockRequest = &inodeLockRequestStruct{
		inodeNumber: 0,
		exclusive:   false,
		listElement: nil,
		locksHeld:   make(map[uint64]*inodeHeldLockStruct),
	}

	return
}

// addThisLock is called for an existing inodeLockRequestStruct with the inodeNumber and
// exclusive fields set to specify the inode to be locked and whether or not the lock
// should be exlusive.
//
// Upon successful granting of the requested lock, the locksHeld map will contain a link
// to the inodeHeldLockStruct tracking the granted lock. If unsuccessful, the locksHeld map
// will be empty.
//
// To collect a set of locks, the caller may repeatably call addThisLock filling in a fresh
// inodeNumber and exclusive field tuple for each inode lock needed. If any lock attempt fails,
// the locksHeld map will be empty. Note that this indicates not only the failure to obtain the
// requested lock but also the implicit releasing of any locks previously in the locksHeld map
// at the time of the call.
//
func (inodeLockRequest *inodeLockRequestStruct) addThisLock() {
	var (
		err           error
		inodeHeldLock *inodeHeldLockStruct
		inodeLease    *inodeLeaseStruct
		leaseRequest  *imgrpkg.LeaseRequestStruct
		leaseResponse *imgrpkg.LeaseResponseStruct
		ok            bool
	)

	globals.Lock()

	if inodeLockRequest.inodeNumber == 0 {
		logFatalf("(*inodeLockRequestStruct)addThisLock() called with .inodeNumber == 0")
	}
	_, ok = inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber]
	if ok {
		logFatalf("*inodeLockRequestStruct)addThisLock() called with .inodeNumber already present in .locksHeld map")
	}

	inodeLease, ok = globals.inodeLeaseTable[inodeLockRequest.inodeNumber]
	if ok {
		switch inodeLease.leaseState {
		case inodeLeaseStateNone:
		case inodeLeaseStateSharedRequested:
			globals.sharedLeaseLRU.MoveToBack(inodeLease.listElement)
		case inodeLeaseStateSharedGranted:
			globals.sharedLeaseLRU.MoveToBack(inodeLease.listElement)
		case inodeLeaseStateSharedPromoting:
			globals.exclusiveLeaseLRU.MoveToBack(inodeLease.listElement)
		case inodeLeaseStateSharedReleasing:
		case inodeLeaseStateSharedExpired:
		case inodeLeaseStateExclusiveRequested:
			globals.exclusiveLeaseLRU.MoveToBack(inodeLease.listElement)
		case inodeLeaseStateExclusiveGranted:
			globals.exclusiveLeaseLRU.MoveToBack(inodeLease.listElement)
		case inodeLeaseStateExclusiveDemoting:
			globals.sharedLeaseLRU.MoveToBack(inodeLease.listElement)
		case inodeLeaseStateExclusiveReleasing:
		case inodeLeaseStateExclusiveExpired:
		default:
			logFatalf("switch inodeLease.leaseState unexpected: %v", inodeLease.leaseState)
		}
	} else {
		inodeLease = &inodeLeaseStruct{
			inodeNumber:                              inodeLockRequest.inodeNumber,
			leaseState:                               inodeLeaseStateNone,
			listElement:                              nil,
			heldList:                                 list.New(),
			requestList:                              list.New(),
			inodeHeadV1:                              nil,
			payload:                                  nil,
			layoutMap:                                nil,
			superBlockInodeObjectCountAdjustment:     0,
			superBlockInodeObjectSizeAdjustment:      0,
			superBlockInodeBytesReferencedAdjustment: 0,
			dereferencedObjectNumberArray:            make([]uint64, 0),
			putObjectNumber:                          0,
			putObjectBuffer:                          nil,
		}

		globals.inodeLeaseTable[inodeLockRequest.inodeNumber] = inodeLease
	}

	if inodeLease.requestList.Len() != 0 {
		// At lease one other inodeLockRequestStruct is blocked, so this one must block

		inodeLockRequest.Add(1)

		inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)

		globals.Unlock()

		inodeLockRequest.Wait()

		return
	}

	if inodeLease.heldList.Len() != 0 {
		if inodeLockRequest.exclusive {
			// Lock is held, so this exclusive inodeLockRequestStruct must block

			inodeLockRequest.Add(1)

			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)

			globals.Unlock()

			inodeLockRequest.Wait()

			return
		}

		inodeHeldLock, ok = inodeLease.heldList.Front().Value.(*inodeHeldLockStruct)
		if !ok {
			logFatalf("inodeLease.heldList.Front().Value.(*inodeHeldLockStruct) returned !ok")
		}

		if inodeHeldLock.exclusive {
			// Lock is held exclusively, so this inodeLockRequestStruct must block

			inodeLockRequest.Add(1)

			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)

			globals.Unlock()

			inodeLockRequest.Wait()

			return
		}
	}

	if inodeLockRequest.exclusive {
		switch inodeLease.leaseState {
		case inodeLeaseStateNone:
			inodeLockRequest.listElement = inodeLease.requestList.PushFront(inodeLockRequest)
			inodeLease.leaseState = inodeLeaseStateExclusiveRequested
			inodeLease.listElement = globals.exclusiveLeaseLRU.PushBack(inodeLease)

			globals.Unlock()

			leaseRequest = &imgrpkg.LeaseRequestStruct{
				MountID:          globals.mountID,
				InodeNumber:      inodeLockRequest.inodeNumber,
				LeaseRequestType: imgrpkg.LeaseRequestTypeExclusive,
			}
			leaseResponse = &imgrpkg.LeaseResponseStruct{}

			err = rpcLease(leaseRequest, leaseResponse)
			if nil != err {
				logFatal(err)
			}

			if leaseResponse.LeaseResponseType != imgrpkg.LeaseResponseTypeExclusive {
				logFatalf("TODO: for now, we don't handle a Lease Request actually failing")
			}

			globals.Lock()

			inodeLease.leaseState = inodeLeaseStateExclusiveGranted

			if inodeLease.requestList.Front() != inodeLockRequest.listElement {
				logFatalf("inodeLease.requestList.Front() != inodeLockRequest.listElement")
			}

			_ = inodeLease.requestList.Remove(inodeLockRequest.listElement)
			inodeLockRequest.listElement = nil

			inodeHeldLock = &inodeHeldLockStruct{
				inodeLease:       inodeLease,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}

			inodeHeldLock.listElement = inodeLease.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			globals.Unlock()
		case inodeLeaseStateSharedRequested:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedGranted:
			inodeLockRequest.listElement = inodeLease.requestList.PushFront(inodeLockRequest)
			inodeLease.leaseState = inodeLeaseStateSharedPromoting
			_ = globals.sharedLeaseLRU.Remove(inodeLease.listElement)
			inodeLease.listElement = globals.exclusiveLeaseLRU.PushBack(inodeLease)

			globals.Unlock()

			leaseRequest = &imgrpkg.LeaseRequestStruct{
				MountID:          globals.mountID,
				InodeNumber:      inodeLockRequest.inodeNumber,
				LeaseRequestType: imgrpkg.LeaseRequestTypePromote,
			}
			leaseResponse = &imgrpkg.LeaseResponseStruct{}

			err = rpcLease(leaseRequest, leaseResponse)
			if nil != err {
				logFatal(err)
			}

			if leaseResponse.LeaseResponseType != imgrpkg.LeaseResponseTypePromoted {
				logFatalf("TODO: for now, we don't handle a Lease Request actually failing")
			}

			globals.Lock()

			inodeLease.leaseState = inodeLeaseStateExclusiveGranted

			if inodeLease.requestList.Front() != inodeLockRequest.listElement {
				logFatalf("inodeLease.requestList.Front() != inodeLockRequest.listElement")
			}

			_ = inodeLease.requestList.Remove(inodeLockRequest.listElement)
			inodeLockRequest.listElement = nil

			inodeHeldLock = &inodeHeldLockStruct{
				inodeLease:       inodeLease,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}

			inodeHeldLock.listElement = inodeLease.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			globals.Unlock()
		case inodeLeaseStateSharedPromoting:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedReleasing:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedExpired:
			// Our mount point has expired... so let owner of inodeLockRequestStruct clean-up
			inodeLockRequest.unlockAllWhileLocked()
			globals.Unlock()
		case inodeLeaseStateExclusiveRequested:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveGranted:
			// We can immediately grant the exclusive inodeLockRequestStruct
			inodeHeldLock = &inodeHeldLockStruct{
				inodeLease:       inodeLease,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}
			inodeHeldLock.listElement = inodeLease.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock
			globals.Unlock()
		case inodeLeaseStateExclusiveDemoting:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveReleasing:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveExpired:
			// Our mount point has expired... so let owner of inodeLockRequestStruct clean-up
			inodeLockRequest.unlockAllWhileLocked()
			globals.Unlock()
		default:
			logFatalf("switch inodeLease.leaseState unexpected: %v", inodeLease.leaseState)
		}
	} else {
		switch inodeLease.leaseState {
		case inodeLeaseStateNone:
			inodeLockRequest.listElement = inodeLease.requestList.PushFront(inodeLockRequest)
			inodeLease.leaseState = inodeLeaseStateSharedRequested
			inodeLease.listElement = globals.sharedLeaseLRU.PushBack(inodeLease)

			globals.Unlock()

			leaseRequest = &imgrpkg.LeaseRequestStruct{
				MountID:          globals.mountID,
				InodeNumber:      inodeLockRequest.inodeNumber,
				LeaseRequestType: imgrpkg.LeaseRequestTypeShared,
			}
			leaseResponse = &imgrpkg.LeaseResponseStruct{}

			err = rpcLease(leaseRequest, leaseResponse)
			if nil != err {
				logFatal(err)
			}

			if leaseResponse.LeaseResponseType != imgrpkg.LeaseResponseTypeShared {
				logFatalf("TODO: for now, we don't handle a Lease Request actually failing")
			}

			globals.Lock()

			inodeLease.leaseState = inodeLeaseStateSharedGranted

			if inodeLease.requestList.Front() != inodeLockRequest.listElement {
				logFatalf("inodeLease.requestList.Front() != inodeLockRequest.listElement")
			}

			_ = inodeLease.requestList.Remove(inodeLockRequest.listElement)
			inodeLockRequest.listElement = nil

			inodeHeldLock = &inodeHeldLockStruct{
				inodeLease:       inodeLease,
				inodeLockRequest: inodeLockRequest,
				exclusive:        false,
			}

			inodeHeldLock.listElement = inodeLease.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			if inodeLease.requestList.Front() != nil {
				logFatalf("TODO: for now, we don't handle multiple shared lock requests queued up")
			}

			globals.Unlock()
		case inodeLeaseStateSharedRequested:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedGranted:
			// We can immediately grant the shared inodeLockRequestStruct
			inodeHeldLock = &inodeHeldLockStruct{
				inodeLease:       inodeLease,
				inodeLockRequest: inodeLockRequest,
				exclusive:        false,
			}
			inodeHeldLock.listElement = inodeLease.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock
			globals.Unlock()
		case inodeLeaseStateSharedPromoting:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedReleasing:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedExpired:
			// Our mount point has expired... so let owner of inodeLockRequestStruct clean-up
			inodeLockRequest.unlockAllWhileLocked()
			globals.Unlock()
		case inodeLeaseStateExclusiveRequested:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveGranted:
			// We can immediately grant the shared inodeLockRequestStruct
			inodeHeldLock = &inodeHeldLockStruct{
				inodeLease:       inodeLease,
				inodeLockRequest: inodeLockRequest,
				exclusive:        false,
			}
			inodeHeldLock.listElement = inodeLease.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock
			globals.Unlock()
		case inodeLeaseStateExclusiveDemoting:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveReleasing:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inodeLease.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveExpired:
			// Our mount point has expired... so let owner of inodeLockRequestStruct clean-up
			inodeLockRequest.unlockAllWhileLocked()
			globals.Unlock()
		default:
			logFatalf("switch inodeLease.leaseState unexpected: %v", inodeLease.leaseState)
		}
	}
}

// unlockAll is called to explicitly release all locks listed in the locksHeld map.
//
func (inodeLockRequest *inodeLockRequestStruct) unlockAll() {
	globals.Lock()
	inodeLockRequest.unlockAllWhileLocked()
	globals.Unlock()
}

// unlockAllWhileLocked is what unlockAll calls after obtaining globals.Lock().
//
func (inodeLockRequest *inodeLockRequestStruct) unlockAllWhileLocked() {
	var (
		inodeHeldLock *inodeHeldLockStruct
	)

	for _, inodeHeldLock = range inodeLockRequest.locksHeld {
		_ = inodeHeldLock.inodeLease.heldList.Remove(inodeHeldLock.listElement)
		if inodeHeldLock.inodeLease.requestList.Len() != 0 {
			logFatalf("TODO: for now, we don't handle blocked inodeLockRequestStruct's")
		}
	}

	inodeLockRequest.locksHeld = make(map[uint64]*inodeHeldLockStruct)
}

func lookupInodeLease(inodeNumber uint64) (inodeLease *inodeLeaseStruct) {
	globals.Lock()
	inodeLease = lookupInodeLeaseWhileLocked(inodeNumber)
	globals.Unlock()
	return
}

func lookupInodeLeaseWhileLocked(inodeNumber uint64) (inodeLease *inodeLeaseStruct) {
	var (
		ok bool
	)

	inodeLease, ok = globals.inodeLeaseTable[inodeNumber]
	if !ok {
		inodeLease = nil
	}

	return
}
