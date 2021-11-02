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
		inode         *inodeStruct
		inodeHeldLock *inodeHeldLockStruct
		leaseRequest  *imgrpkg.LeaseRequestStruct
		leaseResponse *imgrpkg.LeaseResponseStruct
		ok            bool
	)

Retry:

	globals.Lock()

	if inodeLockRequest.inodeNumber == 0 {
		logFatalf("(*inodeLockRequestStruct)addThisLock() called with .inodeNumber == 0")
	}
	_, ok = inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber]
	if ok {
		logFatalf("*inodeLockRequestStruct)addThisLock() called with .inodeNumber already present in .locksHeld map")
	}

	inode, ok = globals.inodeTable[inodeLockRequest.inodeNumber]
	if ok {
		if inode.markedForDelete {
			globals.Unlock()
			inode.Wait()
			goto Retry
		}

		switch inode.leaseState {
		case inodeLeaseStateNone:
		case inodeLeaseStateSharedRequested:
			globals.sharedLeaseLRU.MoveToBack(inode.listElement)
		case inodeLeaseStateSharedGranted:
			globals.sharedLeaseLRU.MoveToBack(inode.listElement)
		case inodeLeaseStateSharedPromoting:
			globals.exclusiveLeaseLRU.MoveToBack(inode.listElement)
		case inodeLeaseStateSharedReleasing:
		case inodeLeaseStateSharedExpired:
		case inodeLeaseStateExclusiveRequested:
			globals.exclusiveLeaseLRU.MoveToBack(inode.listElement)
		case inodeLeaseStateExclusiveGranted:
			globals.exclusiveLeaseLRU.MoveToBack(inode.listElement)
		case inodeLeaseStateExclusiveDemoting:
			globals.sharedLeaseLRU.MoveToBack(inode.listElement)
		case inodeLeaseStateExclusiveReleasing:
		case inodeLeaseStateExclusiveExpired:
		default:
			logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
		}
	} else {
		inode = &inodeStruct{
			inodeNumber:                              inodeLockRequest.inodeNumber,
			dirty:                                    false,
			markedForDelete:                          false,
			leaseState:                               inodeLeaseStateNone,
			listElement:                              nil,
			heldList:                                 list.New(),
			requestList:                              list.New(),
			inodeHeadV1:                              nil,
			linkSet:                                  nil,
			streamMap:                                nil,
			layoutMap:                                nil,
			payload:                                  nil,
			superBlockInodeObjectCountAdjustment:     0,
			superBlockInodeObjectSizeAdjustment:      0,
			superBlockInodeBytesReferencedAdjustment: 0,
			dereferencedObjectNumberArray:            make([]uint64, 0),
			putObjectNumber:                          0,
			putObjectBuffer:                          nil,
		}

		globals.inodeTable[inodeLockRequest.inodeNumber] = inode
	}

	if inode.requestList.Len() != 0 {
		// At lease one other inodeLockRequestStruct is blocked, so this one must block

		inodeLockRequest.Add(1)

		inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)

		globals.Unlock()

		inodeLockRequest.Wait()

		return
	}

	if inode.heldList.Len() != 0 {
		if inodeLockRequest.exclusive {
			// Lock is held, so this exclusive inodeLockRequestStruct must block

			inodeLockRequest.Add(1)

			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)

			globals.Unlock()

			inodeLockRequest.Wait()

			return
		}

		inodeHeldLock, ok = inode.heldList.Front().Value.(*inodeHeldLockStruct)
		if !ok {
			logFatalf("inode.heldList.Front().Value.(*inodeHeldLockStruct) returned !ok")
		}

		if inodeHeldLock.exclusive {
			// Lock is held exclusively, so this inodeLockRequestStruct must block

			inodeLockRequest.Add(1)

			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)

			globals.Unlock()

			inodeLockRequest.Wait()

			return
		}
	}

	if inodeLockRequest.exclusive {
		switch inode.leaseState {
		case inodeLeaseStateNone:
			inodeLockRequest.listElement = inode.requestList.PushFront(inodeLockRequest)
			inode.leaseState = inodeLeaseStateExclusiveRequested
			inode.listElement = globals.exclusiveLeaseLRU.PushBack(inode)

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

			inode.leaseState = inodeLeaseStateExclusiveGranted

			if inode.requestList.Front() != inodeLockRequest.listElement {
				logFatalf("inode.requestList.Front() != inodeLockRequest.listElement")
			}

			_ = inode.requestList.Remove(inodeLockRequest.listElement)
			inodeLockRequest.listElement = nil

			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}

			inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			globals.Unlock()
		case inodeLeaseStateSharedRequested:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedGranted:
			inodeLockRequest.listElement = inode.requestList.PushFront(inodeLockRequest)
			inode.leaseState = inodeLeaseStateSharedPromoting
			_ = globals.sharedLeaseLRU.Remove(inode.listElement)
			inode.listElement = globals.exclusiveLeaseLRU.PushBack(inode)

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

			inode.leaseState = inodeLeaseStateExclusiveGranted

			if inode.requestList.Front() != inodeLockRequest.listElement {
				logFatalf("inode.requestList.Front() != inodeLockRequest.listElement")
			}

			_ = inode.requestList.Remove(inodeLockRequest.listElement)
			inodeLockRequest.listElement = nil

			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}

			inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			globals.Unlock()
		case inodeLeaseStateSharedPromoting:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedReleasing:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedExpired:
			// Our mount point has expired... so let owner of inodeLockRequestStruct clean-up
			globals.Unlock()
			inodeLockRequest.unlockAll()
		case inodeLeaseStateExclusiveRequested:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveGranted:
			// We can immediately grant the exclusive inodeLockRequestStruct
			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}
			inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock
			globals.Unlock()
		case inodeLeaseStateExclusiveDemoting:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveReleasing:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveExpired:
			// Our mount point has expired... so let owner of inodeLockRequestStruct clean-up
			globals.Unlock()
			inodeLockRequest.unlockAll()
		default:
			logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
		}
	} else {
		switch inode.leaseState {
		case inodeLeaseStateNone:
			inodeLockRequest.listElement = inode.requestList.PushFront(inodeLockRequest)
			inode.leaseState = inodeLeaseStateSharedRequested
			inode.listElement = globals.sharedLeaseLRU.PushBack(inode)

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

			inode.leaseState = inodeLeaseStateSharedGranted

			if inode.requestList.Front() != inodeLockRequest.listElement {
				logFatalf("inode.requestList.Front() != inodeLockRequest.listElement")
			}

			_ = inode.requestList.Remove(inodeLockRequest.listElement)
			inodeLockRequest.listElement = nil

			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        false,
			}

			inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			if inode.requestList.Front() != nil {
				logFatalf("TODO: for now, we don't handle multiple shared lock requests queued up")
			}

			globals.Unlock()
		case inodeLeaseStateSharedRequested:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedGranted:
			// We can immediately grant the shared inodeLockRequestStruct
			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        false,
			}
			inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock
			globals.Unlock()
		case inodeLeaseStateSharedPromoting:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedReleasing:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateSharedExpired:
			// Our mount point has expired... so let owner of inodeLockRequestStruct clean-up
			globals.Unlock()
			inodeLockRequest.unlockAll()
		case inodeLeaseStateExclusiveRequested:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveGranted:
			// We can immediately grant the shared inodeLockRequestStruct
			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        false,
			}
			inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock
			globals.Unlock()
		case inodeLeaseStateExclusiveDemoting:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveReleasing:
			// Let whatever entity receives imgrpkg.LeaseResponseType* complete this inodeLockRequestStruct
			inodeLockRequest.Add(1)
			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)
			globals.Unlock()
			inodeLockRequest.Wait()
		case inodeLeaseStateExclusiveExpired:
			// Our mount point has expired... so let owner of inodeLockRequestStruct clean-up
			globals.Unlock()
			inodeLockRequest.unlockAll()
		default:
			logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
		}
	}
}

// unlockAll is called to explicitly release all locks listed in the locksHeld map.
//
func (inodeLockRequest *inodeLockRequestStruct) unlockAll() {
	var (
		err           error
		inode         *inodeStruct
		inodeHeldLock *inodeHeldLockStruct
		leaseRequest  *imgrpkg.LeaseRequestStruct
		leaseResponse *imgrpkg.LeaseResponseStruct
		releaseList   []*inodeStruct = make([]*inodeStruct, 0)
	)

	globals.Lock()

	for _, inodeHeldLock = range inodeLockRequest.locksHeld {
		_ = inodeHeldLock.inode.heldList.Remove(inodeHeldLock.listElement)
		if inodeHeldLock.inode.requestList.Len() == 0 {
			if inodeHeldLock.inode.heldList.Len() == 0 {
				if inodeHeldLock.inode.markedForDelete {
					releaseList = append(releaseList, inodeHeldLock.inode)
					delete(globals.inodeTable, inodeHeldLock.inode.inodeNumber)
				}
			}
		} else {
			logFatalf("TODO: for now, we don't handle blocked inodeLockRequestStruct's")
		}
	}

	globals.Unlock()

	for _, inode = range releaseList {
		switch inode.leaseState {
		case inodeLeaseStateSharedGranted:
			inode.leaseState = inodeLeaseStateSharedReleasing
		case inodeLeaseStateExclusiveGranted:
			inode.leaseState = inodeLeaseStateExclusiveReleasing
		default:
			logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
		}

		leaseRequest = &imgrpkg.LeaseRequestStruct{
			MountID:          globals.mountID,
			InodeNumber:      inodeLockRequest.inodeNumber,
			LeaseRequestType: imgrpkg.LeaseRequestTypeRelease,
		}
		leaseResponse = &imgrpkg.LeaseResponseStruct{}

		err = rpcLease(leaseRequest, leaseResponse)
		if nil != err {
			logFatal(err)
		}

		if leaseResponse.LeaseResponseType != imgrpkg.LeaseResponseTypeReleased {
			logFatalf("received unexpected leaseResponse.LeaseResponseType: %v", leaseResponse.LeaseResponseType)
		}

		inode.Done()
	}

	inodeLockRequest.locksHeld = make(map[uint64]*inodeHeldLockStruct)
}
