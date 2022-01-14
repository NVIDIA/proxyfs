// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"container/list"
	"math/rand"
	"sync"
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
// At any time an inode's lockHolder is nil and lockRequestList is empty (outside of a globals.Lock()
// globals.Unlock() sequence), no inodeLockRequestStruct is acting on the inode in any way (i.e. not
// holding a lock, attempting to acquire a lock, or releasing a lock). Hence, the only valid values
// for inode.leaseState are:
//
//   inodeLeaseStateNone               - no lock requests may be granted
//   inodeLeaseStateSharedGranted      - shared lock requests may be granted
//   inodeLeaseStateExclusiveGranted   - either shared or exclusive lock requests may be granted
//
// During lock ownership transitions, other values should be expected:
//
//   inodeLeaseStateSharedRequested    - we are trying to grant a shared lock request
//   inodeLeaseStateSharedPromoting    - we are trying to grant an exclusive lock request
//   inodeLeaseStateExclusiveRequested - we are trying to grant an exclusive lock request
//
// The other inode.leaseState values occur during Unmount or some Lease Demote/Expired/Release
// handling:
//
//   inodeLeaseStateSharedReleasing    - we are responding to an Unmount or Lease Release RPCInterrupt
//   inodeLeaseStateSharedExpired      - upon learning our Shared Lease has expired
//   inodeLeaseStateExclusiveDemoting  - we are responding to a Lease Demote RPCInterrupt
//   inodeLeaseStateExclusiveReleasing - we are responding to an Unmount or Lease Release RPCInterrupt
//   inodeLeaseStateExclusiveExpired   - upon learning our Exclusive Lease has expired
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

	globals.Lock()

	// Sanity check call

	if inodeLockRequest.inodeNumber == 0 {
		logFatalf("(*inodeLockRequestStruct)addThisLock() called with .inodeNumber == 0")
	}
	_, ok = inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber]
	if ok {
		logFatalf("*inodeLockRequestStruct)addThisLock() called with .inodeNumber already present in .locksHeld map")
	}

	// Ensure there is an inodeStruct for this possibly pre-creation inodeNumber

	inode, ok = globals.inodeTable[inodeLockRequest.inodeNumber]
	if !ok {
		inode = &inodeStruct{
			inodeNumber:                              inodeLockRequest.inodeNumber,
			dirty:                                    false,
			openCount:                                0,
			markedForDelete:                          false,
			leaseState:                               inodeLeaseStateNone,
			listElement:                              nil,
			lockHolder:                               nil,
			lockRequestList:                          list.New(),
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

	// Check if we will use another caller/context to finish our work

	if (inode.lockHolder != nil) || (inode.lockRequestList.Len() > 0) {
		// Another caller/context is managing the lock... but could this cause a potential deadlock?

		if len(inodeLockRequest.locksHeld) == 0 {
			// No deadlock concern... so just let the other caller/context complete this inodeLockRequest

			inodeLockRequest.listElement = inode.lockRequestList.PushBack(inodeLockRequest)

			inodeLockRequest.Add(1)

			globals.Unlock()

			inodeLockRequest.Wait()
		} else { // len(inodeLockRequest.locksHeld) != 0
			// Potential deadlock must be averted...

			globals.Unlock()

			inodeLockRequest.unlockAll()
		}

		return
	}

	// At this point, we will be the next inodeLockRequest to (hopefully) be granted
	// But first, we need to ensure the inodeLeaseState is sufficient

	if inodeLockRequest.exclusive {
		switch inode.leaseState {
		case inodeLeaseStateNone:
			// Before granting this exclusive inodeLockRequest, we must transition to inodeLeaseStateExclusiveGranted

			inode.leaseState = inodeLeaseStateExclusiveRequested

			inode.listElement = globals.exclusiveLeaseLRU.PushBack(inode)

			leaseRequest = &imgrpkg.LeaseRequestStruct{
				MountID:          globals.mountID,
				InodeNumber:      inodeLockRequest.inodeNumber,
				LeaseRequestType: imgrpkg.LeaseRequestTypeExclusive,
			}
			leaseResponse = &imgrpkg.LeaseResponseStruct{}

			// Put this inodeLockRequest at front of inode.lockRequestList to indicate we
			// are responsible for the inode (causing other inodeLockRequests to block)
			// while we leave the globals.Lock()'d state during the Lease Request

			inodeLockRequest.listElement = inode.lockRequestList.PushFront(inodeLockRequest)

			globals.Unlock()

			err = rpcLease(leaseRequest, leaseResponse)
			if nil != err {
				logFatal(err)
			}

			globals.Lock()

			switch leaseResponse.LeaseResponseType {
			case imgrpkg.LeaseResponseTypeDenied:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDenied")
			case imgrpkg.LeaseResponseTypeShared:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeShared")
			case imgrpkg.LeaseResponseTypePromoted:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypePromoted")
			case imgrpkg.LeaseResponseTypeExclusive:
				inode.leaseState = inodeLeaseStateExclusiveGranted

				_ = inode.lockRequestList.Remove(inodeLockRequest.listElement)

				// We can now grant this exclusive inodeLockRequest

				inodeHeldLock = &inodeHeldLockStruct{
					inode:            inode,
					inodeLockRequest: inodeLockRequest,
					exclusive:        true,
				}

				inode.lockHolder = inodeHeldLock
				inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

				globals.Unlock()

				return
			case imgrpkg.LeaseResponseTypeDemoted:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDemoted")
			case imgrpkg.LeaseResponseTypeReleased:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeReleased")
			default:
				logFatalf("switch leaseResponse.LeaseResponseType unexpected: %v", leaseResponse.LeaseResponseType)
			}
		case inodeLeaseStateSharedRequested:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedRequested")
		case inodeLeaseStateSharedGranted:
			// Before granting this exclusive inodeLockRequest, we must transition to inodeLeaseStateExclusiveGranted

			inode.leaseState = inodeLeaseStateSharedPromoting

			_ = globals.sharedLeaseLRU.Remove(inode.listElement)
			inode.listElement = globals.exclusiveLeaseLRU.PushBack(inode)

			leaseRequest = &imgrpkg.LeaseRequestStruct{
				MountID:          globals.mountID,
				InodeNumber:      inodeLockRequest.inodeNumber,
				LeaseRequestType: imgrpkg.LeaseRequestTypePromote,
			}
			leaseResponse = &imgrpkg.LeaseResponseStruct{}

			// Put this inodeLockRequest at front of inode.lockRequestList to indicate we
			// are responsible for the inode (causing other inodeLockRequests to block)
			// while we leave the globals.Lock()'d state during the Lease Request

			inodeLockRequest.listElement = inode.lockRequestList.PushFront(inodeLockRequest)

			globals.Unlock()

			err = rpcLease(leaseRequest, leaseResponse)
			if nil != err {
				logFatal(err)
			}

			globals.Lock()

			switch leaseResponse.LeaseResponseType {
			case imgrpkg.LeaseResponseTypeDenied:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDenied")
			case imgrpkg.LeaseResponseTypeShared:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeShared")
			case imgrpkg.LeaseResponseTypePromoted:
				inode.leaseState = inodeLeaseStateExclusiveGranted

				_ = inode.lockRequestList.Remove(inodeLockRequest.listElement)

				// We can now grant this exclusive inodeLockRequest

				inodeHeldLock = &inodeHeldLockStruct{
					inode:            inode,
					inodeLockRequest: inodeLockRequest,
					exclusive:        true,
				}

				inode.lockHolder = inodeHeldLock
				inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

				globals.Unlock()

				return
			case imgrpkg.LeaseResponseTypeExclusive:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeExclusive")
			case imgrpkg.LeaseResponseTypeDemoted:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDemoted")
			case imgrpkg.LeaseResponseTypeReleased:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeReleased")
			default:
				logFatalf("switch leaseResponse.LeaseResponseType unexpected: %v", leaseResponse.LeaseResponseType)
			}
		case inodeLeaseStateSharedPromoting:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedPromoting")
		case inodeLeaseStateSharedReleasing:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedReleasing")
		case inodeLeaseStateSharedExpired:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedExpired")
		case inodeLeaseStateExclusiveRequested:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveRequested")
		case inodeLeaseStateExclusiveGranted:
			// We can immediately grant this exclusive inodeLockRequest

			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        true,
			}

			inode.lockHolder = inodeHeldLock
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			globals.Unlock()

			return
		case inodeLeaseStateExclusiveDemoting:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveDemoting")
		case inodeLeaseStateExclusiveReleasing:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveReleasing")
		case inodeLeaseStateExclusiveExpired:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveExpired")
		default:
			logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
		}
	} else { // !inodeLockRequest.exclusive
		switch inode.leaseState {
		case inodeLeaseStateNone:
			// Before granting this shared inodeLockRequest, we must transition to inodeLeaseStateExclusiveGranted

			inode.leaseState = inodeLeaseStateSharedRequested

			inode.listElement = globals.sharedLeaseLRU.PushBack(inode)

			leaseRequest = &imgrpkg.LeaseRequestStruct{
				MountID:          globals.mountID,
				InodeNumber:      inodeLockRequest.inodeNumber,
				LeaseRequestType: imgrpkg.LeaseRequestTypeShared,
			}
			leaseResponse = &imgrpkg.LeaseResponseStruct{}

			// Put this inodeLockRequest at front of inode.lockRequestList to indicate we
			// are responsible for the inode (causing other inodeLockRequests to block)
			// while we leave the globals.Lock()'d state during the Lease Request

			inodeLockRequest.listElement = inode.lockRequestList.PushFront(inodeLockRequest)

			globals.Unlock()

			err = rpcLease(leaseRequest, leaseResponse)
			if nil != err {
				logFatal(err)
			}

			globals.Lock()

			switch leaseResponse.LeaseResponseType {
			case imgrpkg.LeaseResponseTypeDenied:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDenied")
			case imgrpkg.LeaseResponseTypeShared:
				inode.leaseState = inodeLeaseStateSharedGranted

				_ = inode.lockRequestList.Remove(inodeLockRequest.listElement)

				// We can now grant this shared inodeLockRequest

				inodeHeldLock = &inodeHeldLockStruct{
					inode:            inode,
					inodeLockRequest: inodeLockRequest,
					exclusive:        false,
				}

				inode.lockHolder = inodeHeldLock
				inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

				globals.Unlock()

				return
			case imgrpkg.LeaseResponseTypePromoted:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypePromoted")
			case imgrpkg.LeaseResponseTypeExclusive:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeExclusive")
			case imgrpkg.LeaseResponseTypeDemoted:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDemoted")
			case imgrpkg.LeaseResponseTypeReleased:
				logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeReleased")
			default:
				logFatalf("switch leaseResponse.LeaseResponseType unexpected: %v", leaseResponse.LeaseResponseType)
			}
		case inodeLeaseStateSharedRequested:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedRequested")
		case inodeLeaseStateSharedGranted:
			// We can immediately grant this shared inodeLockRequest

			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        false,
			}

			inode.lockHolder = inodeHeldLock
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			globals.Unlock()

			return
		case inodeLeaseStateSharedPromoting:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedPromoting")
		case inodeLeaseStateSharedReleasing:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedReleasing")
		case inodeLeaseStateSharedExpired:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedExpired")
		case inodeLeaseStateExclusiveRequested:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveRequested")
		case inodeLeaseStateExclusiveGranted:
			// We can immediately grant this shared inodeLockRequest

			inodeHeldLock = &inodeHeldLockStruct{
				inode:            inode,
				inodeLockRequest: inodeLockRequest,
				exclusive:        false,
			}

			inode.lockHolder = inodeHeldLock
			inodeLockRequest.locksHeld[inodeLockRequest.inodeNumber] = inodeHeldLock

			globals.Unlock()

			return
		case inodeLeaseStateExclusiveDemoting:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveDemoting")
		case inodeLeaseStateExclusiveReleasing:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveReleasing")
		case inodeLeaseStateExclusiveExpired:
			logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveExpired")
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

		inode.lockHolder = nil

		if inode.lockRequestList.Len() == 0 {
			// No pending lock requests exist... should we delete inode?

			if inode.markedForDelete {
				releaseList = append(releaseList, inode)
				delete(globals.inodeTable, inode.inodeNumber)
			}
		} else {
			// We can unblock one lock request

			blockedInodeLockRequest = inode.lockRequestList.Front().Value.(*inodeLockRequestStruct)

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

					_ = inode.lockRequestList.Remove(blockedInodeLockRequest.listElement)
					blockedInodeLockRequest.listElement = nil

					inodeHeldLock = &inodeHeldLockStruct{
						inode:            inode,
						inodeLockRequest: blockedInodeLockRequest,
						exclusive:        true,
					}

					inode.lockHolder = inodeHeldLock
					blockedInodeLockRequest.locksHeld[blockedInodeLockRequest.inodeNumber] = inodeHeldLock

					blockedInodeLockRequest.Done()
				case inodeLeaseStateSharedReleasing:
					logFatalf("TODO: for now, we don't handle a Shared Lease releasing")
				case inodeLeaseStateSharedExpired:
					logFatalf("TODO: for now, we don't handle a Shared Lease expiring")
				case inodeLeaseStateExclusiveGranted:
					_ = inode.lockRequestList.Remove(blockedInodeLockRequest.listElement)
					blockedInodeLockRequest.listElement = nil

					inodeHeldLock = &inodeHeldLockStruct{
						inode:            inode,
						inodeLockRequest: blockedInodeLockRequest,
						exclusive:        true,
					}

					inode.lockHolder = inodeHeldLock
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
					_ = inode.lockRequestList.Remove(blockedInodeLockRequest.listElement)
					blockedInodeLockRequest.listElement = nil

					inodeHeldLock = &inodeHeldLockStruct{
						inode:            inode,
						inodeLockRequest: blockedInodeLockRequest,
						exclusive:        false,
					}

					inode.lockHolder = inodeHeldLock
					blockedInodeLockRequest.locksHeld[blockedInodeLockRequest.inodeNumber] = inodeHeldLock

					blockedInodeLockRequest.Done()
				case inodeLeaseStateSharedReleasing:
					logFatalf("TODO: for now, we don't handle a Shared Lease releasing")
				case inodeLeaseStateSharedExpired:
					logFatalf("TODO: for now, we don't handle a Shared Lease expiring")
				case inodeLeaseStateExclusiveGranted:
					_ = inode.lockRequestList.Remove(blockedInodeLockRequest.listElement)
					blockedInodeLockRequest.listElement = nil

					inodeHeldLock = &inodeHeldLockStruct{
						inode:            inode,
						inodeLockRequest: blockedInodeLockRequest,
						exclusive:        false,
					}

					inode.lockHolder = inodeHeldLock
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

// demoteInodeLease will use a shared inodeLockRequest to ensure local exclusive access to
// the inode indicated by the specified inodeNumber and, if an Exclusive Lease is currently
// held, demote it. It is expected that this is called in a goroutine and, thus, signals
// completion via the specified wg (if non-nil).
//
func demoteInodeLease(inodeNumber uint64, wg *sync.WaitGroup) {
	var (
		blockedInodeLockRequest            *inodeLockRequestStruct
		blockedInodeLockRequestList        *list.List = list.New()
		blockedInodeLockRequestListElement *list.Element
		err                                error
		inode                              *inodeStruct
		inodeHeldLock                      *inodeHeldLockStruct
		leaseRequest                       *imgrpkg.LeaseRequestStruct
		leaseResponse                      *imgrpkg.LeaseResponseStruct
		ok                                 bool
		ourInodeLockRequest                *inodeLockRequestStruct
	)

Retry:
	ourInodeLockRequest = newLockRequest()
	ourInodeLockRequest.inodeNumber = inodeNumber
	ourInodeLockRequest.exclusive = false
	ourInodeLockRequest.addThisLock()
	if len(ourInodeLockRequest.locksHeld) == 0 {
		performInodeLockRetryDelay()
		goto Retry
	}

	globals.Lock()

	inode, ok = globals.inodeTable[inodeNumber]
	if !ok {
		logFatalf("globals.inodeTable[inodeNumber] returned !ok")
	}

	if inode.dirty {
		// In this case, we know that we must currently hold an Exclusive Lease,
		// so upgrade ourInodeLockRequest to indicate a Shared Lock

		inodeHeldLock, ok = ourInodeLockRequest.locksHeld[inodeNumber]
		if !ok {
			logFatalf("ourInodeLockRequest.locksHeld[inodeNumber] returned !ok")
		}

		inodeHeldLock.exclusive = true

		// Next, we must perform a "flush" on the inode while not in globals.Lock() section

		globals.Unlock()
		flushInodesInSlice([]*inodeStruct{inode})
		globals.Lock()
	}

	switch inode.leaseState {
	case inodeLeaseStateNone:
		// Nothing to be done
	case inodeLeaseStateSharedRequested:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedRequested")
	case inodeLeaseStateSharedGranted:
		// Nothing to be done
	case inodeLeaseStateSharedPromoting:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedPromoting")
	case inodeLeaseStateSharedReleasing:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedReleasing")
	case inodeLeaseStateSharedExpired:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedExpired")
	case inodeLeaseStateExclusiveRequested:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveRequested")
	case inodeLeaseStateExclusiveGranted:
		// Perform the requested Lease Demotion

		inode.leaseState = inodeLeaseStateExclusiveDemoting

		_ = globals.exclusiveLeaseLRU.Remove(inode.listElement)
		inode.listElement = globals.sharedLeaseLRU.PushBack(inode)

		leaseRequest = &imgrpkg.LeaseRequestStruct{
			MountID:          globals.mountID,
			InodeNumber:      inodeNumber,
			LeaseRequestType: imgrpkg.LeaseRequestTypeDemote,
		}
		leaseResponse = &imgrpkg.LeaseResponseStruct{}

		globals.Unlock()

		err = rpcLease(leaseRequest, leaseResponse)
		if nil != err {
			logFatal(err)
		}

		globals.Lock()

		switch leaseResponse.LeaseResponseType {
		case imgrpkg.LeaseResponseTypeDenied:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDenied")
		case imgrpkg.LeaseResponseTypeShared:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeShared")
		case imgrpkg.LeaseResponseTypePromoted:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypePromoted")
		case imgrpkg.LeaseResponseTypeExclusive:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeExclusive")
		case imgrpkg.LeaseResponseTypeDemoted:
			inode.leaseState = inodeLeaseStateSharedGranted

			// Trigger any blocked inodeLockRequest's to retry
			//
			// We must postpone the blockedInodeLockRequest.unlockAll() and blockedInodeLockRequest.Done()
			// calls required until after we leave the globals.Lock()'d state

			for inode.lockRequestList.Front() != nil {
				blockedInodeLockRequestListElement = inode.lockRequestList.Front()
				blockedInodeLockRequest, ok = blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct)
				if !ok {
					logFatalf("blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct) returned !ok")
				}
				_ = inode.lockRequestList.Remove(blockedInodeLockRequestListElement)
				_ = blockedInodeLockRequestList.PushBack(blockedInodeLockRequest)
			}
		case imgrpkg.LeaseResponseTypeReleased:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeReleased")
		default:
			logFatalf("switch leaseResponse.LeaseResponseType unexpected: %v", leaseResponse.LeaseResponseType)
		}
	case inodeLeaseStateExclusiveDemoting:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveDemoting")
	case inodeLeaseStateExclusiveReleasing:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveReleasing")
	case inodeLeaseStateExclusiveExpired:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveExpired")
	default:
		logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
	}

	// Safely release ourInodeLockRequest before we issue the globals.Unlock() with the knowledge
	// that there are no blockedInodeLockRequest's on the inode.lockRequestList

	inode.lockHolder = nil

	globals.Unlock()

	// We can now issue those postponed blockedInodeLockRequest.unlockAll() and
	// blockedInodeLockRequest.Done() calls

	for blockedInodeLockRequestList.Front() != nil {
		blockedInodeLockRequestListElement = blockedInodeLockRequestList.Front()
		blockedInodeLockRequest, ok = blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct)
		if !ok {
			logFatalf("blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct) returned !ok")
		}
		_ = blockedInodeLockRequestList.Remove(blockedInodeLockRequestListElement)

		if len(blockedInodeLockRequest.locksHeld) != 0 {
			blockedInodeLockRequest.unlockAll()
		}

		blockedInodeLockRequest.Done()
	}

	// Optionally indicate we are done

	if wg != nil {
		wg.Done()
	}
}

// releaseInodeLease will use a shared inodeLockRequest to ensure local exclusive access to
// the inode indicated by the specified inodeNumber and, if a Lease is currently held,
// release it. It is expected that this is called in a goroutine and, thus, signals
// completion via the specified wg (if non-nil).
//
func releaseInodeLease(inodeNumber uint64, wg *sync.WaitGroup) {
	var (
		blockedInodeLockRequest            *inodeLockRequestStruct
		blockedInodeLockRequestList        *list.List = list.New()
		blockedInodeLockRequestListElement *list.Element
		err                                error
		inode                              *inodeStruct
		inodeHeldLock                      *inodeHeldLockStruct
		leaseRequest                       *imgrpkg.LeaseRequestStruct
		leaseResponse                      *imgrpkg.LeaseResponseStruct
		ok                                 bool
		ourInodeLockRequest                *inodeLockRequestStruct
	)

Retry:
	ourInodeLockRequest = newLockRequest()
	ourInodeLockRequest.inodeNumber = inodeNumber
	ourInodeLockRequest.exclusive = false
	ourInodeLockRequest.addThisLock()
	if len(ourInodeLockRequest.locksHeld) == 0 {
		performInodeLockRetryDelay()
		goto Retry
	}

	globals.Lock()

	inode, ok = globals.inodeTable[inodeNumber]
	if !ok {
		logFatalf("globals.inodeTable[inodeNumber] returned !ok")
	}

	if inode.dirty {
		// In this case, we know that we must currently hold an Exclusive Lease,
		// so upgrade ourInodeLockRequest to indicate a Shared Lock

		inodeHeldLock, ok = ourInodeLockRequest.locksHeld[inodeNumber]
		if !ok {
			logFatalf("ourInodeLockRequest.locksHeld[inodeNumber] returned !ok")
		}

		inodeHeldLock.exclusive = true

		// Next, we must perform a "flush" on the inode while not in globals.Lock() section

		globals.Unlock()
		flushInodesInSlice([]*inodeStruct{inode})
		globals.Lock()
	}

	if inode.inodeHeadV1 != nil {
		// In this case, we know that we must currently hold a Lease of some kind

		if inode.payload != nil {
			// As we are to end up with inode.leaseState == inodeLeaseStateNone,
			// we should purge the caching of inode.payload

			inode.payload = nil
		}

		// As we are to end up with inode.leaseState == inodeLeaseStateNone,
		// we should purge the caching of inode.inodeHeadV1

		inode.inodeHeadV1 = nil
	}

	switch inode.leaseState {
	case inodeLeaseStateNone:
		// Nothing to be done
	case inodeLeaseStateSharedRequested:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedRequested")
	case inodeLeaseStateSharedGranted:
		// Perform the requested Lease Release

		inode.leaseState = inodeLeaseStateSharedReleasing

		_ = globals.sharedLeaseLRU.Remove(inode.listElement)
		inode.listElement = nil

		leaseRequest = &imgrpkg.LeaseRequestStruct{
			MountID:          globals.mountID,
			InodeNumber:      inodeNumber,
			LeaseRequestType: imgrpkg.LeaseRequestTypeRelease,
		}
		leaseResponse = &imgrpkg.LeaseResponseStruct{}

		globals.Unlock()

		err = rpcLease(leaseRequest, leaseResponse)
		if nil != err {
			logFatal(err)
		}

		globals.Lock()

		switch leaseResponse.LeaseResponseType {
		case imgrpkg.LeaseResponseTypeDenied:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDenied")
		case imgrpkg.LeaseResponseTypeShared:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeShared")
		case imgrpkg.LeaseResponseTypePromoted:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypePromoted")
		case imgrpkg.LeaseResponseTypeExclusive:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeExclusive")
		case imgrpkg.LeaseResponseTypeDemoted:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDemoted")
		case imgrpkg.LeaseResponseTypeReleased:
			inode.leaseState = inodeLeaseStateNone

			// Trigger any blocked inodeLockRequest's to retry
			//
			// We must postpone the blockedInodeLockRequest.unlockAll() and blockedInodeLockRequest.Done()
			// calls required until after we leave the globals.Lock()'d state

			for inode.lockRequestList.Front() != nil {
				blockedInodeLockRequestListElement = inode.lockRequestList.Front()
				blockedInodeLockRequest, ok = blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct)
				if !ok {
					logFatalf("blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct) returned !ok")
				}
				_ = inode.lockRequestList.Remove(blockedInodeLockRequestListElement)
				_ = blockedInodeLockRequestList.PushBack(blockedInodeLockRequest)
			}

			// As we've emptied inode.lockRequestList, it's appropriate for us to
			// also remove the inodeStruct from globals.inodeTable

			delete(globals.inodeTable, inodeNumber)
		default:
			logFatalf("switch leaseResponse.LeaseResponseType unexpected: %v", leaseResponse.LeaseResponseType)
		}
	case inodeLeaseStateSharedPromoting:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedPromoting")
	case inodeLeaseStateSharedReleasing:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedReleasing")
	case inodeLeaseStateSharedExpired:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateSharedExpired")
	case inodeLeaseStateExclusiveRequested:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveRequested")
	case inodeLeaseStateExclusiveGranted:
		// Perform the requested Lease Release

		inode.leaseState = inodeLeaseStateExclusiveReleasing

		_ = globals.exclusiveLeaseLRU.Remove(inode.listElement)
		inode.listElement = nil

		leaseRequest = &imgrpkg.LeaseRequestStruct{
			MountID:          globals.mountID,
			InodeNumber:      inodeNumber,
			LeaseRequestType: imgrpkg.LeaseRequestTypeRelease,
		}
		leaseResponse = &imgrpkg.LeaseResponseStruct{}

		globals.Unlock()

		err = rpcLease(leaseRequest, leaseResponse)
		if nil != err {
			logFatal(err)
		}

		globals.Lock()

		switch leaseResponse.LeaseResponseType {
		case imgrpkg.LeaseResponseTypeDenied:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDenied")
		case imgrpkg.LeaseResponseTypeShared:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeShared")
		case imgrpkg.LeaseResponseTypePromoted:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypePromoted")
		case imgrpkg.LeaseResponseTypeExclusive:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeExclusive")
		case imgrpkg.LeaseResponseTypeDemoted:
			logFatalf("TODO: for now, we don't handle leaseResponse.LeaseResponseType: imgrpkg.LeaseResponseTypeDemoted")
		case imgrpkg.LeaseResponseTypeReleased:
			inode.leaseState = inodeLeaseStateNone

			// Trigger any blocked inodeLockRequest's to retry
			//
			// We must postpone the blockedInodeLockRequest.unlockAll() and blockedInodeLockRequest.Done()
			// calls required until after we leave the globals.Lock()'d state

			for inode.lockRequestList.Front() != nil {
				blockedInodeLockRequestListElement = inode.lockRequestList.Front()
				blockedInodeLockRequest, ok = blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct)
				if !ok {
					logFatalf("blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct) returned !ok")
				}
				_ = inode.lockRequestList.Remove(blockedInodeLockRequestListElement)
				_ = blockedInodeLockRequestList.PushBack(blockedInodeLockRequest)
			}

			// As we've emptied inode.lockRequestList, it's appropriate for us to
			// also remove the inodeStruct from globals.inodeTable

			delete(globals.inodeTable, inodeNumber)
		default:
			logFatalf("switch leaseResponse.LeaseResponseType unexpected: %v", leaseResponse.LeaseResponseType)
		}
	case inodeLeaseStateExclusiveDemoting:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveDemoting")
	case inodeLeaseStateExclusiveReleasing:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveReleasing")
	case inodeLeaseStateExclusiveExpired:
		logFatalf("switch inode.leaseState unexpected: inodeLeaseStateExclusiveExpired")
	default:
		logFatalf("switch inode.leaseState unexpected: %v", inode.leaseState)
	}

	// Safely release ourInodeLockRequest before we issue the globals.Unlock() with the knowledge
	// that there are no blockedInodeLockRequest's on the inode.lockRequestList

	inode.lockHolder = nil

	globals.Unlock()

	// We can now issue those postponed blockedInodeLockRequest.unlockAll() and
	// blockedInodeLockRequest.Done() calls

	for blockedInodeLockRequestList.Front() != nil {
		blockedInodeLockRequestListElement = blockedInodeLockRequestList.Front()
		blockedInodeLockRequest, ok = blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct)
		if !ok {
			logFatalf("blockedInodeLockRequestListElement.Value.(*inodeLockRequestStruct) returned !ok")
		}
		_ = blockedInodeLockRequestList.Remove(blockedInodeLockRequestListElement)

		if len(blockedInodeLockRequest.locksHeld) != 0 {
			blockedInodeLockRequest.unlockAll()
		}

		blockedInodeLockRequest.Done()
	}

	// Optionally indicate we are done

	if wg != nil {
		wg.Done()
	}
}

// demoteAllExclusiveLeases will schedule all inodeStructs in the globals.exclusiveLeaseLRU
// to demote their Exclusive Lease.
//
func demoteAllExclusiveLeases() {
	var (
		inode          *inodeStruct
		lruListElement *list.Element
		ok             bool
		wg             sync.WaitGroup
	)

	globals.Lock()

	lruListElement = globals.exclusiveLeaseLRU.Front()

	for lruListElement != nil {
		inode, ok = lruListElement.Value.(*inodeStruct)
		if !ok {
			logFatalf("lruListElement.Value.(*inodeStruct) returned !ok")
		}

		wg.Add(1)

		go demoteInodeLease(inode.inodeNumber, &wg)

		lruListElement = lruListElement.Next()
	}

	globals.Unlock()

	wg.Wait()
}

// releaseAllLeases will schedule all inodeStructs in either the globals.sharedLeaseLRU
// or globals.exclusiveLeaseLRU to release their Lease.
//
func releaseAllLeases() {
	var (
		inode          *inodeStruct
		lruListElement *list.Element
		ok             bool
		wg             sync.WaitGroup
	)

	globals.Lock()

	lruListElement = globals.sharedLeaseLRU.Front()

	for lruListElement != nil {
		inode, ok = lruListElement.Value.(*inodeStruct)
		if !ok {
			logFatalf("lruListElement.Value.(*inodeStruct) returned !ok")
		}

		wg.Add(1)

		go releaseInodeLease(inode.inodeNumber, &wg)

		lruListElement = lruListElement.Next()
	}

	lruListElement = globals.exclusiveLeaseLRU.Front()

	for lruListElement != nil {
		inode, ok = lruListElement.Value.(*inodeStruct)
		if !ok {
			logFatalf("lruListElement.Value.(*inodeStruct) returned !ok")
		}

		wg.Add(1)

		go demoteInodeLease(inode.inodeNumber, &wg)

		lruListElement = lruListElement.Next()
	}

	globals.Unlock()

	wg.Wait()
}
