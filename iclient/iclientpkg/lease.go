// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"container/list"
	"math/rand"
	"time"

	"github.com/NVIDIA/proxyfs/imgr/imgrpkg"
)

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

// markForDelete is called to schedule an inode to be deleted from globals.inodeTable
// upon last dereference. Note that an exclusive lock must be held for the specified
// inodeNumber and that globals.Lock() must not be held.
//
func (inodeLockRequest *inodeLockRequestStruct) markForDelete(inodeNumber uint64) {
	var (
		inodeHeldLock *inodeHeldLockStruct
		ok            bool
	)

	globals.Lock()

	inodeHeldLock, ok = inodeLockRequest.locksHeld[inodeNumber]
	if !ok {
		logFatalf("inodeLockRequest.locksHeld[inodeNumber] returned !ok")
	}

	if !inodeHeldLock.exclusive {
		logFatalf("inodeHeldLock.exclusive was false")
	}

	inodeHeldLock.inode.markedForDelete = true

	globals.Unlock()
}

// performInodeLockRetryDelay simply delays the current goroutine for InodeLockRetryDelay
// interval +/- InodeLockRetryDelayVariance (interpreted as a percentage).
//
// It is expected that a caller to addThisLock(), when noticing locksHeld map is empty,
// will call performInodeLockRetryDelay() before re-attempting a lock sequence.
//
func performInodeLockRetryDelay() {
	var (
		delay    time.Duration
		variance int64
	)

	variance = rand.Int63n(int64(globals.config.InodeLockRetryDelay) & int64(globals.config.InodeLockRetryDelayVariance) / int64(100))

	if (variance % 2) == 0 {
		delay = time.Duration(int64(globals.config.InodeLockRetryDelay) + variance)
	} else {
		delay = time.Duration(int64(globals.config.InodeLockRetryDelay) - variance)
	}

	time.Sleep(delay)
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
// It is expected that the caller, when noticing locksHeld map is empty, will call
// performInodeLockRetryDelay() before re-attempting the lock sequence.
//
func (inodeLockRequest *inodeLockRequestStruct) addThisLock() {
	var (
		blockedInodeLockRequest *inodeLockRequestStruct
		err                     error
		inode                   *inodeStruct
		inodeHeldLock           *inodeHeldLockStruct
		leaseRequest            *imgrpkg.LeaseRequestStruct
		leaseResponse           *imgrpkg.LeaseResponseStruct
		ok                      bool
	)
	logTracef("UNDO: entered addThisLock() with .inodeNumber: %v .exclusive: %v", inodeLockRequest.inodeNumber, inodeLockRequest.exclusive)

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
			fileFlusher:                              nil,
		}

		globals.inodeTable[inodeLockRequest.inodeNumber] = inode
	}

	if inode.requestList.Len() != 0 {
		logTracef("UNDO: ok... addThisLock() found that we cannot immediately grant the lock as others are blocked...")
		// At lease one other inodeLockRequestStruct is blocked, so this one must either block or, to avoid deadlock, release other held locks and retry

		if len(inodeLockRequest.locksHeld) != 0 {
			// We must avoid deadlock by releasing other held locks and return
			logTracef("UNDO: ok... addThisLock() needs to avoid deadlock, so we must release our other locks and trigger a retry")

			globals.Unlock()

			inodeLockRequest.unlockAll()

			return
		}

		logTracef("UNDO: ok... addThisLock() will simply block this request")
		inodeLockRequest.Add(1)

		inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)

		globals.Unlock()

		inodeLockRequest.Wait()

		return
	}

	if inode.heldList.Len() != 0 {
		if inodeLockRequest.exclusive {
			// Lock is held, so this exclusive inodeLockRequestStruct must block
			logTracef("UNDO: ok... addThisLock() needs to block on .inodeNunber: %v .exclusive: TRUEv while inode.heldList.Len() == %v", inodeLockRequest.inodeNumber, inode.heldList.Len())

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
			logInfof("UNDO: @ CheckRequestList: loop in addThisLock()")

		CheckRequestList:

			if inode.requestList.Front() != nil {
				blockedInodeLockRequest = inode.requestList.Front().Value.(*inodeLockRequestStruct)

				if !blockedInodeLockRequest.exclusive {
					// We can also unblock blockedInodeLockRequest

					_ = inode.requestList.Remove(blockedInodeLockRequest.listElement)
					blockedInodeLockRequest.listElement = nil

					inodeHeldLock = &inodeHeldLockStruct{
						inode:            inode,
						inodeLockRequest: blockedInodeLockRequest,
						exclusive:        false,
					}

					inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
					blockedInodeLockRequest.locksHeld[blockedInodeLockRequest.inodeNumber] = inodeHeldLock

					blockedInodeLockRequest.Done()

					// Now go back and check for more

					goto CheckRequestList
				}
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
		blockedInodeLockRequest *inodeLockRequestStruct
		err                     error
		inode                   *inodeStruct
		inodeHeldLock           *inodeHeldLockStruct
		leaseRequest            *imgrpkg.LeaseRequestStruct
		leaseResponse           *imgrpkg.LeaseResponseStruct
		releaseList             []*inodeStruct = make([]*inodeStruct, 0)
	)

	globals.Lock()

	for _, inodeHeldLock = range inodeLockRequest.locksHeld {
		inode = inodeHeldLock.inode
		logTracef("UNDO: entered unlockAll()... found an inodeHeldLock with .inode..inodeNumber: %v .exclusive: %v", inodeHeldLock.inode.inodeNumber, inodeHeldLock.exclusive)

		_ = inode.heldList.Remove(inodeHeldLock.listElement)

		if inode.requestList.Len() == 0 {
			if inode.heldList.Len() == 0 {
				if inode.markedForDelete {
					releaseList = append(releaseList, inode)
					delete(globals.inodeTable, inode.inodeNumber)
				}
			}
		} else {
			if inodeHeldLock.exclusive {
				logInfof("UNDO: @ We can unblock at least one blockedInodeLockRequest")
				// We can unblock at least one blockedInodeLockRequest

				blockedInodeLockRequest = inode.requestList.Front().Value.(*inodeLockRequestStruct)

				_ = inode.requestList.Remove(blockedInodeLockRequest.listElement)
				blockedInodeLockRequest.listElement = nil

				inodeHeldLock = &inodeHeldLockStruct{
					inode:            inode,
					inodeLockRequest: blockedInodeLockRequest,
					exclusive:        blockedInodeLockRequest.exclusive,
				}

				inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
				blockedInodeLockRequest.locksHeld[blockedInodeLockRequest.inodeNumber] = inodeHeldLock

				blockedInodeLockRequest.Done()

				if !blockedInodeLockRequest.exclusive {
					// We can also unblock following blockedInodeLockRequest's that are !.exclusive

				CheckRequestList:

					if inode.requestList.Front() != nil {
						blockedInodeLockRequest = inode.requestList.Front().Value.(*inodeLockRequestStruct)

						if !blockedInodeLockRequest.exclusive {
							// We can also unblock next blockedInodeLockRequest

							_ = inode.requestList.Remove(blockedInodeLockRequest.listElement)
							blockedInodeLockRequest.listElement = nil

							inodeHeldLock = &inodeHeldLockStruct{
								inode:            inode,
								inodeLockRequest: blockedInodeLockRequest,
								exclusive:        false,
							}

							inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
							blockedInodeLockRequest.locksHeld[blockedInodeLockRequest.inodeNumber] = inodeHeldLock

							blockedInodeLockRequest.Done()

							// Now go back and check for more

							goto CheckRequestList
						}
					}
				}
			}
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
	}

	inodeLockRequest.locksHeld = make(map[uint64]*inodeHeldLockStruct)
}
