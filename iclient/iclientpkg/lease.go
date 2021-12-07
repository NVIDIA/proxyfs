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

	variance = rand.Int63n(int64(globals.config.InodeLockRetryDelay) * int64(globals.config.InodeLockRetryDelayVariance) / int64(100))

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

	globals.Lock()

	if inodeLockRequest.inodeNumber == 0 {
		logFatalf("(*inodeLockRequestStruct)addThisLock() called with .inodeNumber == 0")
	}
	_, ok = inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber]
	if ok {
		logFatalf("*inodeLockRequestStruct)addThisLock() called with .inodeNumber already present in .locksHeld map")
	}

	// First get inode in the proper state required for this inodeLockRequest

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
			openCount:                                0,
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

	if inodeLockRequest.exclusive {
		if inode.leaseState == inodeLeaseStateExclusiveGranted {
			// Attempt to grant exclusive inodeLockRequest

			if inode.heldList.Len() == 0 {
				// We can now grant exclusive inodeLockRequest [Case 1]

				inodeHeldLock = &inodeHeldLockStruct{
					inode:            inode,
					inodeLockRequest: inodeLockRequest,
					exclusive:        true,
				}

				inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
				inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

				globals.Unlock()

				return
			}

			// Block exlusive inodeLockRequest unless .locksHeld is non-empty

			if len(inodeLockRequest.locksHeld) == 0 {
				// We must block exclusive inodeLockRequest [Case 1]

				inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)

				inodeLockRequest.Add(1)

				globals.Unlock()

				inodeLockRequest.Wait()

				return
			} else {
				// We must avoid deadlock for this exclusive inodeLockRequest with non-empty .locksHeld [Case 1]

				globals.Unlock()

				inodeLockRequest.unlockAll()

				return
			}
		}
	} else { // !inodeLockRequest.exclusive
		if (inode.leaseState == inodeLeaseStateSharedGranted) || (inode.leaseState == inodeLeaseStateExclusiveGranted) {
			// Attempt to grant shared inodeLockRequest

			if inode.heldList.Len() == 0 {
				// We can now grant shared inodeLockRequest [Case 1]

				inodeHeldLock = &inodeHeldLockStruct{
					inode:            inode,
					inodeLockRequest: inodeLockRequest,
					exclusive:        false,
				}

				inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
				inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

				globals.Unlock()

				return
			}

			if inode.requestList.Len() == 0 {
				inodeHeldLock = inode.heldList.Front().Value.(*inodeHeldLockStruct)

				if !inodeHeldLock.exclusive {
					// We can now grant shared inodeLockRequest [Case 2]

					inodeHeldLock = &inodeHeldLockStruct{
						inode:            inode,
						inodeLockRequest: inodeLockRequest,
						exclusive:        false,
					}

					inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
					inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

					globals.Unlock()

					return
				}
			}

			// Block shared inodeLockRequest unless .locksHeld is non-empty

			if len(inodeLockRequest.locksHeld) == 0 {
				// We must block shared inodeLockRequest

				inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)

				inodeLockRequest.Add(1)

				globals.Unlock()

				inodeLockRequest.Wait()

				return
			} else {
				// We must avoid deadlock for this shared inodeLockRequest with non-empty .locksHeld

				globals.Unlock()

				inodeLockRequest.unlockAll()

				return
			}
		}
	}

	// If we reach here, inode was not in the proper state required for this inodeLockRequest

	if (inode.leaseState != inodeLeaseStateNone) && (inode.leaseState != inodeLeaseStateSharedGranted) {
		if len(inodeLockRequest.locksHeld) == 0 {
			// We must block inodeLockRequest
			//
			// Ultimately, .leaseState will transition triggering the servicing of .requestList

			inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)

			inodeLockRequest.Add(1)

			globals.Unlock()

			inodeLockRequest.Wait()

			return
		} else {
			// We must avoid deadlock for this inodeLockRequest with non-empty .locksHeld

			globals.Unlock()

			inodeLockRequest.unlockAll()

			return
		}
	}

	// If we reach here, we are in one of three states:
	//
	//   inodeLockRequest.exclusive == true  && inode.leaseState == inodeLeaseStateNone
	//   inodeLockRequest.exclusive == true  && inode.leaseState == inodeLeaseStateSharedGranted
	//   inodeLockRequest.exclusive == false && inode.leaseState == inodeLeaseStateNone

	if inodeLockRequest.exclusive {
		if inode.leaseState == inodeLeaseStateNone {
			// We need to issue a imgrpkg.LeaseRequestTypeExclusive

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

			// We can now grant exclusive inodeLockRequest [Case 2]

			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}

			inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock
		} else { // inode.leaseState == inodeLeaseStateSharedGranted
			if inode.heldList.Len() != 0 {
				if len(inodeLockRequest.locksHeld) == 0 {
					// We must block exclusive inodeLockRequest [Case 2]

					inodeLockRequest.listElement = inode.requestList.PushBack(inodeLockRequest)

					inodeLockRequest.Add(1)

					globals.Unlock()

					inodeLockRequest.Wait()

					return
				} else {
					// We must avoid deadlock for this exclusive inodeLockRequest with non-empty .locksHeld [Case 2]

					globals.Unlock()

					inodeLockRequest.unlockAll()

					return
				}
			}

			// We need to issue a imgrpkg.LeaseRequestTypePromote

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

			// We can now grant exclusive inodeLockRequest [Case 3]

			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}

			inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock
		}
	} else { // !inodeLockRequest.exclusive && inode.leaseState == inodeLeaseStateNone
		// We need to issue a imgrpkg.LeaseRequestTypeShared

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

		// We can now grant shared inodeLockRequest [Case 3]

		inodeHeldLock = &inodeHeldLockStruct{
			inode:            inode,
			inodeLockRequest: inodeLockRequest,
			exclusive:        false,
		}

		inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
		inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

		// Check to see if there are any grantable blocked shared inodeLockRequest's

	CheckRequestList:

		if inode.requestList.Front() != nil {
			blockedInodeLockRequest = inode.requestList.Front().Value.(*inodeLockRequestStruct)

			if !blockedInodeLockRequest.exclusive {
				// We can now also grant blockedInodeLockRequest

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

	globals.Unlock()
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

		_ = inode.heldList.Remove(inodeHeldLock.listElement)

		if inode.heldList.Len() == 0 {
			// This was the last lock holder

			if inode.requestList.Len() == 0 {
				// No pending lock requests exist... should we delete inode?

				if inode.markedForDelete {
					releaseList = append(releaseList, inode)
					delete(globals.inodeTable, inode.inodeNumber)
				}
			} else {
				// We can unblock one or more pending lock requests

				blockedInodeLockRequest = inode.requestList.Front().Value.(*inodeLockRequestStruct)

				if blockedInodeLockRequest.exclusive {
					switch inode.leaseState {
					case inodeLeaseStateSharedGranted:
						inode.leaseState = inodeLeaseStateSharedPromoting

						globals.Unlock()

						leaseRequest = &imgrpkg.LeaseRequestStruct{
							MountID:          globals.mountID,
							InodeNumber:      blockedInodeLockRequest.inodeNumber,
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

						_ = inode.requestList.Remove(blockedInodeLockRequest.listElement)
						blockedInodeLockRequest.listElement = nil

						inodeHeldLock = &inodeHeldLockStruct{
							inode:            inode,
							inodeLockRequest: blockedInodeLockRequest,
							exclusive:        true,
						}

						inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
						blockedInodeLockRequest.locksHeld[blockedInodeLockRequest.inodeNumber] = inodeHeldLock

						blockedInodeLockRequest.Done()
					case inodeLeaseStateSharedReleasing:
						logFatalf("TODO: for now, we don't handle a Shared Lease releasing")
					case inodeLeaseStateSharedExpired:
						logFatalf("TODO: for now, we don't handle a Shared Lease expiring")
					case inodeLeaseStateExclusiveGranted:
						_ = inode.requestList.Remove(blockedInodeLockRequest.listElement)
						blockedInodeLockRequest.listElement = nil

						inodeHeldLock = &inodeHeldLockStruct{
							inode:            inode,
							inodeLockRequest: blockedInodeLockRequest,
							exclusive:        true,
						}

						inodeHeldLock.listElement = inode.heldList.PushBack(inodeHeldLock)
						blockedInodeLockRequest.locksHeld[blockedInodeLockRequest.inodeNumber] = inodeHeldLock

						blockedInodeLockRequest.Done()
					case inodeLeaseStateExclusiveDemoting:
						logFatalf("TODO: for now, we don't handle an Exclusive Lease demoting")
					case inodeLeaseStateExclusiveReleasing:
						logFatalf("TODO: for now, we don't handle an Exclusive Lease releasing")
					case inodeLeaseStateExclusiveExpired:
						logFatalf("TODO: for now, we don't handle an Exclusive Lease expiring")
					default:
						logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
					}
				} else { // !blockedInodeLockRequest.exclusive
					switch inode.leaseState {
					case inodeLeaseStateSharedGranted:

					GrantBlockedInodeLockRequestWhileHoldingSharedLease:

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

						if inode.requestList.Len() == 0 {
							blockedInodeLockRequest = inode.requestList.Front().Value.(*inodeLockRequestStruct)

							if !blockedInodeLockRequest.exclusive {
								goto GrantBlockedInodeLockRequestWhileHoldingSharedLease
							}
						}
					case inodeLeaseStateSharedReleasing:
						logFatalf("TODO: for now, we don't handle a Shared Lease releasing")
					case inodeLeaseStateSharedExpired:
						logFatalf("TODO: for now, we don't handle a Shared Lease expiring")
					case inodeLeaseStateExclusiveGranted:

					GrantBlockedInodeLockRequestWhileHoldingExclusiveLease:

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

						if inode.requestList.Len() != 0 {
							blockedInodeLockRequest = inode.requestList.Front().Value.(*inodeLockRequestStruct)

							if !blockedInodeLockRequest.exclusive {
								goto GrantBlockedInodeLockRequestWhileHoldingExclusiveLease
							}
						}
					case inodeLeaseStateExclusiveDemoting:
						logFatalf("TODO: for now, we don't handle an Exclusive Lease demoting")
					case inodeLeaseStateExclusiveReleasing:
						logFatalf("TODO: for now, we don't handle an Exclusive Lease releasing")
					case inodeLeaseStateExclusiveExpired:
						logFatalf("TODO: for now, we don't handle an Exclusive Lease expiring")
					default:
						logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
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
