// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

func startLeaseHandler() (err error) {
	return nil // TODO
}

func stopLeaseHandler() (err error) {
	return nil // TODO
}

func newLockRequest() (inodeLockRequest *inodeLockRequestStruct) {
	inodeLockRequest = &inodeLockRequestStruct{
		inodeNumber: 0,
		exclusive:   false,
		listElement: nil,
		locksHeld:   make(map[uint64]*inodeHeldLockStruct),
	}

	return
}

func (inodeLockRequest *inodeLockRequestStruct) addThisLock() {
	// TODO
}

func (inodeLockRequest *inodeLockRequestStruct) unlockAll() {
	// TODO
}

// func inodeLeaseRequestWhileLocked(inodeNumber uint64, inodeLeaseRequestTag inodeLeaseRequestTagType) {
// 	var (
// 		inodeLease        *inodeLeaseStruct
// 		inodeLeaseRequest *inodeLeaseRequestStruct
// 		ok                bool
// 	)

// 	inodeLease, ok = globals.inodeLeaseTable[inodeNumber]

// 	if !ok {
// 		inodeLease = &inodeLeaseStruct{
// 			state:       inodeLeaseStateNone,
// 			requestList: list.New(),
// 		}

// 		globals.inodeLeaseWG.Add(1)
// 		inodeLease.Add(1)
// 		go inodeLease.goroutine()
// 	}

// 	inodeLeaseRequest = &inodeLeaseRequestStruct{
// 		tag: inodeLeaseRequestTag,
// 	}

// 	inodeLeaseRequest.Add(1)

// 	inodeLeaseRequest.listElement = inodeLease.requestList.PushBack(inodeLeaseRequest)

// 	if inodeLease.requestList.Len() == 1 {
// 		inodeLease.Done()
// 	}

// 	globals.Unlock()

// 	inodeLeaseRequest.Wait()

// 	globals.Lock()
// }

// func (inodeLease *inodeLeaseStruct) goroutine() {
// 	var (
// 		inodeLeaseRequest            *inodeLeaseRequestStruct
// 		inodeLeaseRequestListElement *list.Element
// 		ok                           bool
// 	)

// 	defer globals.inodeLeaseWG.Done()

// 	for {
// 		inodeLease.Wait()

// 		globals.Lock()

// 		inodeLeaseRequestListElement = inodeLease.requestList.Front()
// 		if nil == inodeLeaseRequestListElement {
// 			logFatalf("(*inodeLeaseStruct).goroutine() awakened with empty inodeLease.requestList")
// 		}

// 		inodeLease.requestList.Remove(inodeLeaseRequestListElement)

// 		if inodeLease.requestList.Len() == 0 {
// 			inodeLease.Add(1) // Make sure we stay awake
// 		}

// 		globals.Unlock()

// 		inodeLeaseRequest, ok = inodeLeaseRequestListElement.Value.(*inodeLeaseRequestStruct)
// 		if !ok {
// 			logFatalf("inodeLeaseRequestListElement.Value.(*inodeLeaseRequestStruct) returned !ok")
// 		}

// 		// TODO: For now, just grant the requested operation

// 		switch inodeLease.state {
// 		case inodeLeaseStateNone:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateNone...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateSharedRequested:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateSharedRequested...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateSharedGranted:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateSharedGranted...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateSharedPromoting:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateSharedPromoting...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateSharedReleasing:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateSharedReleasing...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateSharedExpired:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateSharedExpired...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateExclusiveRequested:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateExclusiveRequested...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateExclusiveGranted:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateExclusiveGranted...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateExclusiveDemoting:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateExclusiveDemoting...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateExclusiveReleasing:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateExclusiveReleasing...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		case inodeLeaseStateExclusiveExpired:
// 			switch inodeLeaseRequest.tag {
// 			case inodeLeaseRequestShared:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestPromote:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestExclusive:
// 				inodeLease.state = inodeLeaseStateExclusiveGranted
// 			case inodeLeaseRequestDemote:
// 				inodeLease.state = inodeLeaseStateSharedGranted
// 			case inodeLeaseRequestRelease:
// 				inodeLease.state = inodeLeaseStateNone
// 			default:
// 				logFatalf("inodeLease.state == inodeLeaseStateExclusiveExpired...switch inodeLeaseRequest.tag unexpected: %v", inodeLeaseRequest.tag)
// 			}
// 		default:
// 			logFatalf("switch inodeLease.state unexpected: %v", inodeLease.state)
// 		}

// 		inodeLeaseRequest.Done()
// 	}
// }
