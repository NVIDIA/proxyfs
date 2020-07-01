package jrpcfs

import (
	"container/list"
	"fmt"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/inode"
)

// RpcLease is called to either request a Shared|Exclusive Lease or to
// Promote|Demote|Release a granted Shared|Exclusive|either Lease.
//
func (s *Server) RpcLease(in *LeaseRequest, reply *LeaseReply) (err error) {
	var (
		inodeLease            *inodeLeaseStruct
		inodeNumber           inode.InodeNumber
		leaseRequestOperation *leaseRequestOperationStruct
		mount                 *mountStruct
		ok                    bool
		volume                *volumeStruct
	)

	enterGate()
	defer leaveGate()

	switch in.LeaseRequestType {
	case LeaseRequestTypeShared:
	case LeaseRequestTypePromote:
	case LeaseRequestTypeExclusive:
	case LeaseRequestTypeDemote:
	case LeaseRequestTypeRelease:
	default:
		reply = &LeaseReply{
			LeaseReplyType: LeaseReplyTypeDenied,
		}
		err = fmt.Errorf("LeaseRequestType %v not supported", in.LeaseRequestType)
		err = blunder.AddError(err, blunder.BadLeaseRequest)
		return
	}

	inodeNumber = inode.InodeNumber(in.InodeHandle.InodeNumber)

	globals.volumesLock.Lock()

	mount, ok = globals.mountMapByMountIDAsString[in.MountID]
	if !ok {
		reply = &LeaseReply{
			LeaseReplyType: LeaseReplyTypeDenied,
		}
		err = fmt.Errorf("MountID %s not found in jrpcfs globals.mountMapByMountIDAsString", in.MountID)
		err = blunder.AddError(err, blunder.BadMountIDError)
		return
	}

	globals.volumesLock.Lock()

	mount, ok = globals.mountMapByMountIDAsString[in.MountID]
	if !ok {
		globals.volumesLock.Unlock()
		reply = &LeaseReply{
			LeaseReplyType: LeaseReplyTypeDenied,
		}
		err = fmt.Errorf("MountID %s not found in jrpcfs globals.mountMapByMountIDAsString", in.MountID)
		err = blunder.AddError(err, blunder.BadMountIDError)
		return
	}

	volume = mount.volume

	if (in.LeaseRequestType == LeaseRequestTypeShared) || (in.LeaseRequestType == LeaseRequestTypeExclusive) {
		inodeLease, ok = volume.inodeLeaseMap[inodeNumber]
		if !ok {
			inodeLease = &inodeLeaseStruct{
				volume:               volume,
				inodeNumber:          inodeNumber,
				leaseState:           inodeLeaseStateNone,
				requestChan:          make(chan *leaseRequestOperationStruct),
				sharedHoldersList:    list.New(),
				promotingHolder:      nil,
				exclusiveHolder:      nil,
				releasingHoldersList: list.New(),
				requestedList:        list.New(),
			}

			volume.inodeLeaseMap[inodeNumber] = inodeLease

			volume.leaseHandlerWG.Add(1)
			go inodeLease.handler()
		}
	} else { // in.LeaseRequestType is one of LeaseRequestType{Promote|Demote|Release}
		inodeLease, ok = volume.inodeLeaseMap[inodeNumber]
		if !ok {
			globals.volumesLock.Unlock()
			reply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			err = fmt.Errorf("LeaseRequestType %v not allowed for non-existent Lease", in.LeaseRequestType)
			err = blunder.AddError(err, blunder.BadLeaseRequest)
			return
		}
	}

	// Send Lease Request Operation to *inodeLeaseStruct.handler()
	//
	// Note that we still hold the volumesLock, so inodeLease can't disappear out from under us

	leaseRequestOperation = &leaseRequestOperationStruct{
		mount:            mount,
		inodeLease:       inodeLease,
		LeaseRequestType: in.LeaseRequestType,
		replyChan:        make(chan *LeaseReply),
	}

	inodeLease.requestChan <- leaseRequestOperation

	globals.volumesLock.Unlock()

	reply = <-leaseRequestOperation.replyChan

	if reply.LeaseReplyType == LeaseReplyTypeDenied {
		err = fmt.Errorf("LeaseRequestType %v was denied", in.LeaseRequestType)
		err = blunder.AddError(err, blunder.BadLeaseRequest)
	} else {
		err = nil
	}

	return
}

func (inodeLease *inodeLeaseStruct) handler() {
	// TODO
}

/*
func (s *Server) RpcLeaseOLD(in *LeaseRequest, reply *LeaseReply) (err error) {
	var (
		inodeLease   *inodeLeaseStruct
		leaseRequest *leaseRequestStruct
		mount        *mountStruct
		ok           bool
		volume       *volumeStruct
	)

	enterGate()

	volume = mount.volume

	switch in.LeaseRequestType {

	case LeaseRequestTypeShared, LeaseRequestTypeExclusive:

		if LeaseHandleEmpty != in.LeaseHandle {
			globals.volumesLock.Unlock()
			leaveGate()
			reply = &LeaseReply{
				LeaseHandle:    in.LeaseHandle,
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			err = fmt.Errorf("For LeaseRequestType %v, LeaseHandle (%v) must be %v", in.LeaseRequestType, in.LeaseHandle, LeaseHandleEmpty)
			err = blunder.AddError(err, blunder.BadLeaseRequest)
			return
		}

		volume.lastLeaseHandle++

		inodeLease, ok = volume.inodeLeaseMap[inode.InodeNumber(in.InodeNumber)]
		if !ok {
			inodeLease = &inodeLeaseStruct{
				inodeNumber:       inode.InodeNumber(in.InodeNumber),
				volume:            volume,
				leaseState:        inodeLeaseStateNone,
				lastGrantTime:     time.Time{},
				exclusiveHolder:   nil,
				promotingHolder:   nil,
				sharedHoldersList: list.New(),
				waitersList:       list.New(),
			}

			volume.inodeLeaseMap[inodeLease.inodeNumber] = inodeLease
		}

		leaseRequest = &leaseRequestStruct{
			leaseHandle:   volume.lastLeaseHandle,
			inodeLease:    inodeLease,
			mount:         mount,
			requestState:  leaseRequestStateNone,
			listElement:   nil,
			operationCV:   sync.NewCond(&globals.volumesLock),
			operationList: list.New(),
			unblockCV:     sync.NewCond(&globals.volumesLock),
			replyChan:     make(chan *LeaseReply, 1),
		}

		mount.leaseRequestMap[leaseRequest.leaseHandle] = leaseRequest

		volume.leaseHandlerWG.Add(1)
		go leaseRequest.handler()

	case LeaseRequestTypePromote, LeaseRequestTypeDemote, LeaseRequestTypeRelease:

		if LeaseHandleEmpty == in.LeaseHandle {
			globals.volumesLock.Unlock()
			leaveGate()
			reply = &LeaseReply{
				LeaseHandle:    in.LeaseHandle,
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			err = fmt.Errorf("For LeaseRequestType %v, LeaseHandle must not be %v", in.LeaseRequestType, LeaseHandleEmpty)
			err = blunder.AddError(err, blunder.BadLeaseRequest)
			return
		}

		leaseRequest, ok = mount.leaseRequestMap[in.LeaseHandle]
		if !ok {
			globals.volumesLock.Unlock()
			leaveGate()
			reply = &LeaseReply{
				LeaseHandle:    in.LeaseHandle,
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			err = fmt.Errorf("For LeaseRequestType %v, unknown LeaseHandle (%v)", in.LeaseRequestType, in.LeaseHandle)
			err = blunder.AddError(err, blunder.BadLeaseRequest)
			return
		}
		if leaseRequest.mount != mount {
			globals.volumesLock.Unlock()
			leaveGate()
			reply = &LeaseReply{
				LeaseHandle:    in.LeaseHandle,
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			err = fmt.Errorf("For LeaseRequestType %v, invalid LeaseHandle (%v)", in.LeaseRequestType, in.LeaseHandle)
			err = blunder.AddError(err, blunder.BadLeaseRequest)
			return
		}

	default:
		globals.volumesLock.Unlock()
		leaveGate()
		reply = &LeaseReply{
			LeaseHandle:    in.LeaseHandle,
			LeaseReplyType: LeaseReplyTypeDenied,
		}
		err = fmt.Errorf("LeaseRequestType %v not supported", in.LeaseRequestType)
		err = blunder.AddError(err, blunder.BadLeaseRequest)
		return
	}

	_ = leaseRequest.operationList.PushBack(in)

	leaseRequest.operationCV.Signal()

	globals.volumesLock.Unlock()
	leaveGate()

	reply = <-leaseRequest.replyChan

	if LeaseReplyTypeDenied == reply.LeaseReplyType {
		err = fmt.Errorf("LeaseRequestType %v was denied", in.LeaseRequestType)
		err = blunder.AddError(err, blunder.BadLeaseRequest)
	} else {
		err = nil
	}

	return
}

func (leaseRequest *leaseRequestStruct) handler() {
	var (
		ok                   bool
		operationListElement *list.Element
		operationReply       *LeaseReply
		operationReplyType   LeaseReplyType
		operationRequest     *LeaseRequest
	)

	// Fall into LeaseRequestServer state machine

	for {
		globals.volumesLock.Lock()

		leaseRequest.operationCV.Wait()

		operationListElement = leaseRequest.operationList.Front()
		if nil == operationListElement {
			globals.volumesLock.Unlock()
			continue
		}

		leaseRequest.operationList.Remove(operationListElement)
		operationRequest = operationListElement.Value.(*LeaseRequest)

		// See if LeaseManagement has determined we are to expire the currently held Lease

		if nil == operationRequest {
			// Drain any outstanding operationRequests

			for 0 != leaseRequest.operationList.Len() {
				operationListElement = leaseRequest.operationList.Front()
				leaseRequest.operationList.Remove(operationListElement)
				operationRequest = operationListElement.Value.(*LeaseRequest)

				if nil != operationRequest {
					operationReply = &LeaseReply{
						LeaseHandle:    leaseRequest.leaseHandle,
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequest.replyChan <- operationReply
				}
			}

			// If necessary, remove this leaseRequest from its inodeLease holdersList or waitersList

			switch leaseRequest.requestState {
			case leaseRequestStateNone:
				// Not on either list
			case leaseRequestStateSharedGranted, leaseRequestStateSharedPromoting, leaseRequestStateSharedReleasing, leaseRequestStateExclusiveGranted, leaseRequestStateExclusiveDemoting, leaseRequestStateExclusiveReleasing:
				leaseRequest.inodeLease.holdersList.Remove(leaseRequest.listElement)
			case leaseRequestStateSharedRequested, leaseRequestStateExclusiveRequested:
				leaseRequest.inodeLease.waitersList.Remove(leaseRequest.listElement)
			}

			// Remove this listRequest from mount

			delete(leaseRequest.mount.leaseRequestMap, leaseRequest.leaseHandle)

			// Cleanly exit

			globals.volumesLock.Unlock()
			leaseRequest.inodeLease.volume.leaseHandlerWG.Done()
			runtime.Goexit()
		}

		switch leaseRequest.requestState {
		case leaseRequestStateNone:
			if (LeaseRequestTypeShared != operationRequest.LeaseRequestType) && (LeaseRequestTypeExclusive != operationRequest.LeaseRequestType) {
				operationReplyType = LeaseReplyTypeDenied
			} else {
				switch leaseRequest.inodeLease.leaseState {
				case inodeLeaseStateNone:
					leaseRequest.requestState = leaseRequestStateExclusiveGranted
					leaseRequest.listElement = leaseRequest.inodeLease.sharedHoldersList.PushBack(leaseRequest)
					leaseRequest.inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
					leaseRequest.inodeLease.lastGrantTime = time.Now()
					operationReplyType = LeaseReplyTypeExclusive
				case inodeLeaseStateSharedGrantedRecently:
					if LeaseRequestTypeShared == operationRequest.LeaseRequestType {
						leaseRequest.requestState = leaseRequestStateSharedGranted
						leaseRequest.listElement = leaseRequest.inodeLease.sharedHoldersList.PushBack(leaseRequest)
						leaseRequest.inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
						leaseRequest.inodeLease.lastGrantTime = time.Now()
						operationReplyType = LeaseReplyTypeShared
					} else { // LeaseRequestTypeExclusive == operationRequest.LeaseRequestType
						// TODO
					}
				case inodeLeaseStateSharedGrantedLongAgo:
					if LeaseRequestTypeShared == operationRequest.LeaseRequestType {
						leaseRequest.requestState = leaseRequestStateSharedGranted
						leaseRequest.listElement = leaseRequest.inodeLease.sharedHoldersList.PushBack(leaseRequest)
						leaseRequest.inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
						leaseRequest.inodeLease.lastGrantTime = time.Now()
						operationReplyType = LeaseReplyTypeShared
					} else { // LeaseRequestTypeExclusive == operationRequest.LeaseRequestType
						// TODO - block
					}
				case inodeLeaseStateSharedReleasing:
					if LeaseRequestTypeShared == operationRequest.LeaseRequestType {
						leaseRequest.requestState = leaseRequestStateSharedRequested
					} else { // LeaseRequestTypeExclusive == operationRequest.LeaseRequestType
						leaseRequest.requestState = leaseRequestStateExclusiveRequested
					}
					leaseRequest.listElement = leaseRequest.inodeLease.waitersList.PushBack(leaseRequest)
					// TODO - block
				case inodeLeaseStateSharedExpired:
					if LeaseRequestTypeShared == operationRequest.LeaseRequestType {
						leaseRequest.requestState = leaseRequestStateSharedRequested
					} else { // LeaseRequestTypeExclusive == operationRequest.LeaseRequestType
						leaseRequest.requestState = leaseRequestStateExclusiveRequested
					}
					leaseRequest.listElement = leaseRequest.inodeLease.waitersList.PushBack(leaseRequest)
					// TODO - block
				case inodeLeaseStateExclusiveGrantedRecently:
					if LeaseRequestTypeShared == operationRequest.LeaseRequestType {
						leaseRequest.requestState = leaseRequestStateSharedRequested
					} else { // LeaseRequestTypeExclusive == operationRequest.LeaseRequestType
						leaseRequest.requestState = leaseRequestStateExclusiveRequested
					}
					leaseRequest.listElement = leaseRequest.inodeLease.waitersList.PushBack(leaseRequest)
					// TODO - possibly delay until inodeLeaseStateExclusiveGrantedLongAgo
					// TODO - tell current exlusive holder to either demote or release
					// TODO - block
				case inodeLeaseStateExclusiveGrantedLongAgo:
					if LeaseRequestTypeShared == operationRequest.LeaseRequestType {
						leaseRequest.requestState = leaseRequestStateSharedRequested
					} else { // LeaseRequestTypeExclusive == operationRequest.LeaseRequestType
						leaseRequest.requestState = leaseRequestStateExclusiveRequested
					}
					leaseRequest.listElement = leaseRequest.inodeLease.waitersList.PushBack(leaseRequest)
					// TODO - tell current exlusive holder to either demote or release
					// TODO - block
				case inodeLeaseStateExclusiveReleasing:
					if LeaseRequestTypeShared == operationRequest.LeaseRequestType {
						leaseRequest.requestState = leaseRequestStateSharedRequested
					} else { // LeaseRequestTypeExclusive == operationRequest.LeaseRequestType
						leaseRequest.requestState = leaseRequestStateExclusiveRequested
					}
					leaseRequest.listElement = leaseRequest.inodeLease.waitersList.PushBack(leaseRequest)
					// TODO - tell current exlusive holder to either demote or release
					// TODO - block
				case inodeLeaseStateExclusiveDemoting:
					// TODO - ???
					leaseRequest.requestState = leaseRequestStateExclusiveRequested
				case inodeLeaseStateExclusiveExpired:
					if LeaseRequestTypeShared == operationRequest.LeaseRequestType {
						leaseRequest.requestState = leaseRequestStateSharedRequested
					} else { // LeaseRequestTypeExclusive == operationRequest.LeaseRequestType
						leaseRequest.requestState = leaseRequestStateExclusiveRequested
					}
					leaseRequest.listElement = leaseRequest.inodeLease.waitersList.PushBack(leaseRequest)
					// TODO - tell current exlusive holder to either demote or release
					// TODO - block
				}
			}
		case leaseRequestStateSharedRequested:
			operationReplyType = LeaseReplyTypeDenied
		case leaseRequestStateSharedGranted:
			if LeaseRequestTypeRelease != operationRequest.LeaseRequestType {
				operationReplyType = LeaseReplyTypeDenied
			} else {
				switch leaseRequest.inodeLease.leaseState {
				case inodeLeaseStateNone:
					// TODO
				case inodeLeaseStateSharedGrantedRecently:
					// TODO
				case inodeLeaseStateSharedGrantedLongAgo:
					// TODO
				case inodeLeaseStateSharedReleasing:
					// TODO
				case inodeLeaseStateSharedExpired:
					// TODO
				case inodeLeaseStateExclusiveGrantedRecently:
					// TODO
				case inodeLeaseStateExclusiveGrantedLongAgo:
					// TODO
				case inodeLeaseStateExclusiveReleasing:
					// TODO
				case inodeLeaseStateExclusiveDemoting:
					// TODO
				case inodeLeaseStateExclusiveExpired:
					// TODO
				}
			}
		case leaseRequestStateSharedPromoting:
			operationReplyType = LeaseReplyTypeDenied
		case leaseRequestStateSharedReleasing:
			if LeaseRequestTypeRelease != operationRequest.LeaseRequestType {
				operationReplyType = LeaseReplyTypeDenied
			} else {
				switch leaseRequest.inodeLease.leaseState {
				case inodeLeaseStateNone:
					// TODO
				case inodeLeaseStateSharedGrantedRecently:
					// TODO
				case inodeLeaseStateSharedGrantedLongAgo:
					// TODO
				case inodeLeaseStateSharedReleasing:
					// TODO
				case inodeLeaseStateSharedExpired:
					// TODO
				case inodeLeaseStateExclusiveGrantedRecently:
					// TODO
				case inodeLeaseStateExclusiveGrantedLongAgo:
					// TODO
				case inodeLeaseStateExclusiveReleasing:
					// TODO
				case inodeLeaseStateExclusiveDemoting:
					// TODO
				case inodeLeaseStateExclusiveExpired:
					// TODO
				}
			}
		case leaseRequestStateExclusiveRequested:
			operationReplyType = LeaseReplyTypeDenied
		case leaseRequestStateExclusiveGranted:
			if (LeaseRequestTypeDemote != operationRequest.LeaseRequestType) && (LeaseRequestTypeRelease != operationRequest.LeaseRequestType) {
				operationReplyType = LeaseReplyTypeDenied
			} else {
				switch leaseRequest.inodeLease.leaseState {
				case inodeLeaseStateNone:
					// TODO
				case inodeLeaseStateSharedGrantedRecently:
					// TODO
				case inodeLeaseStateSharedGrantedLongAgo:
					// TODO
				case inodeLeaseStateSharedReleasing:
					// TODO
				case inodeLeaseStateSharedExpired:
					// TODO
				case inodeLeaseStateExclusiveGrantedRecently:
					// TODO
				case inodeLeaseStateExclusiveGrantedLongAgo:
					// TODO
				case inodeLeaseStateExclusiveReleasing:
					// TODO
				case inodeLeaseStateExclusiveDemoting:
					// TODO
				case inodeLeaseStateExclusiveExpired:
					// TODO
				}
			}
		case leaseRequestStateExclusiveDemoting:
			if (LeaseRequestTypeDemote != operationRequest.LeaseRequestType) && (LeaseRequestTypeRelease != operationRequest.LeaseRequestType) {
				operationReplyType = LeaseReplyTypeDenied
			} else {
				switch leaseRequest.inodeLease.leaseState {
				case inodeLeaseStateNone:
					// TODO
				case inodeLeaseStateSharedGrantedRecently:
					// TODO
				case inodeLeaseStateSharedGrantedLongAgo:
					// TODO
				case inodeLeaseStateSharedReleasing:
					// TODO
				case inodeLeaseStateSharedExpired:
					// TODO
				case inodeLeaseStateExclusiveGrantedRecently:
					// TODO
				case inodeLeaseStateExclusiveGrantedLongAgo:
					// TODO
				case inodeLeaseStateExclusiveReleasing:
					// TODO
				case inodeLeaseStateExclusiveDemoting:
					// TODO
				case inodeLeaseStateExclusiveExpired:
					// TODO
				}
			}
		case leaseRequestStateExclusiveReleasing:
			if LeaseRequestTypeRelease != operationRequest.LeaseRequestType {
				operationReplyType = LeaseReplyTypeDenied
			} else {
				switch leaseRequest.inodeLease.leaseState {
				case inodeLeaseStateNone:
					// TODO
				case inodeLeaseStateSharedGrantedRecently:
					// TODO
				case inodeLeaseStateSharedGrantedLongAgo:
					// TODO
				case inodeLeaseStateSharedReleasing:
					// TODO
				case inodeLeaseStateSharedExpired:
					// TODO
				case inodeLeaseStateExclusiveGrantedRecently:
					// TODO
				case inodeLeaseStateExclusiveGrantedLongAgo:
					// TODO
				case inodeLeaseStateExclusiveReleasing:
					// TODO
				case inodeLeaseStateExclusiveDemoting:
					// TODO
				case inodeLeaseStateExclusiveExpired:
					// TODO
				}
			}
		}

		operationReply = &LeaseReply{
			LeaseHandle:    leaseRequest.leaseHandle,
			LeaseReplyType: operationReplyType,
		}

		// Now address the race with RpcLease() that may have queued subsequent operations on this leaseRequest

		enterGate()
		globals.volumesLock.Lock()

		leaseRequest.replyChan <- operationReply

		// If leaseRequest.requestState is now leaseRequestStateNone, we can destroy this leaseRequest

		if leaseRequestStateNone == leaseRequest.requestState {
			// We can destroy this leaseRequest... but first, subsequent operations should be denied

			for {

			}
		}

		// If destroying this leaseRequest and inodeLease.leaseState is now inodeLeaseStateNone. we can destroy this inodeLease as well

		// In either case, if we destroyed this leaseRequest, we can exit this goroutine
	}
}
*/
