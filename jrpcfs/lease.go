// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

import (
	"container/list"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/blunder"
	"github.com/NVIDIA/proxyfs/evtlog"
	"github.com/NVIDIA/proxyfs/inode"
	"github.com/NVIDIA/proxyfs/logger"
	"github.com/NVIDIA/sortedmap"
)

// RpcLease is called to either request a Shared|Exclusive Lease or to
// Promote|Demote|Release a granted Shared|Exclusive|either Lease.
//
func (s *Server) RpcLease(in *LeaseRequest, reply *LeaseReply) (err error) {
	var (
		additionalEvictionsInitiated         uint64
		additionalEvictionsNeeded            uint64
		delayedUnmount                       *mountStruct
		delayedUnmountLeaseReleaseFinishedWG sync.WaitGroup
		delayedUnmountLeaseReleaseStartWG    sync.WaitGroup
		delayedUnmountListElement            *list.Element
		inodeLease                           *inodeLeaseStruct
		inodeLeaseFromLRU                    *inodeLeaseStruct
		inodeLeaseLRUElement                 *list.Element
		inodeNumber                          inode.InodeNumber
		leaseRequestOperation                *leaseRequestOperationStruct
		mount                                *mountStruct
		ok                                   bool
		replyToReturn                        *LeaseReply
		volume                               *volumeStruct
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
		reply.LeaseReplyType = LeaseReplyTypeDenied
		err = fmt.Errorf("LeaseRequestType %v not supported", in.LeaseRequestType)
		err = blunder.AddError(err, blunder.BadLeaseRequest)
		return
	}

	inodeNumber = inode.InodeNumber(in.InodeHandle.InodeNumber)

	globals.volumesLock.Lock()

	mount, ok = globals.mountMapByMountIDAsString[in.MountID]
	if !ok {
		globals.volumesLock.Unlock()
		reply.LeaseReplyType = LeaseReplyTypeDenied
		err = fmt.Errorf("MountID %s not found in jrpcfs globals.mountMapByMountIDAsString", in.MountID)
		err = blunder.AddError(err, blunder.BadMountIDError)
		return
	}

	volume = mount.volume

	evtlog.Record(evtlog.FormatLeaseRequest, volume.volumeName, string(in.InodeHandle.MountID), uint64(inode.InodeNumber(in.InodeHandle.InodeNumber)), uint32(in.LeaseRequestType))
	defer func() {
		evtlog.Record(evtlog.FormatLeaseReply, volume.volumeName, string(in.InodeHandle.MountID), uint64(inode.InodeNumber(in.InodeHandle.InodeNumber)), uint32(in.LeaseRequestType), uint32(reply.LeaseReplyType))
	}()

	if (in.LeaseRequestType == LeaseRequestTypeShared) || (in.LeaseRequestType == LeaseRequestTypeExclusive) {
		if !volume.acceptingMountsAndLeaseRequests || !mount.acceptingLeaseRequests {
			globals.volumesLock.Unlock()
			reply.LeaseReplyType = LeaseReplyTypeDenied
			err = fmt.Errorf("LeaseRequestType %v not allowed while dismounting Volume", in.LeaseRequestType)
			err = blunder.AddError(err, blunder.BadLeaseRequest)
			return
		}
		inodeLease, ok = volume.inodeLeaseMap[inodeNumber]
		if !ok {
			inodeLease = &inodeLeaseStruct{
				volume:               volume,
				InodeNumber:          inodeNumber,
				leaseState:           inodeLeaseStateNone,
				beingEvicted:         false,
				requestChan:          make(chan *leaseRequestOperationStruct),
				stopChan:             make(chan struct{}),
				sharedHoldersList:    list.New(),
				promotingHolder:      nil,
				exclusiveHolder:      nil,
				releasingHoldersList: list.New(),
				requestedList:        list.New(),
				lastGrantTime:        time.Time{},
				lastInterruptTime:    time.Time{},
				interruptsSent:       0,
				longAgoTimer:         &time.Timer{},
				interruptTimer:       &time.Timer{},
			}

			volume.inodeLeaseMap[inodeNumber] = inodeLease
			inodeLease.lruElement = volume.inodeLeaseLRU.PushBack(inodeLease)

			volume.leaseHandlerWG.Add(1)
			go inodeLease.handler()

			if uint64(volume.inodeLeaseLRU.Len()) >= volume.activeLeaseEvictHighLimit {
				additionalEvictionsNeeded = (volume.activeLeaseEvictHighLimit - volume.activeLeaseEvictLowLimit) - volume.ongoingLeaseEvictions
				additionalEvictionsInitiated = 0
				inodeLeaseLRUElement = volume.inodeLeaseLRU.Front()
				for (nil != inodeLeaseLRUElement) && (additionalEvictionsInitiated < additionalEvictionsNeeded) {
					inodeLeaseFromLRU = inodeLeaseLRUElement.Value.(*inodeLeaseStruct)
					if !inodeLeaseFromLRU.beingEvicted {
						inodeLeaseFromLRU.beingEvicted = true
						close(inodeLeaseFromLRU.stopChan)
						additionalEvictionsInitiated++
					}
					inodeLeaseLRUElement = inodeLeaseLRUElement.Next()
				}
				volume.ongoingLeaseEvictions += additionalEvictionsInitiated
			}
		}
	} else { // in.LeaseRequestType is one of LeaseRequestType{Promote|Demote|Release}
		inodeLease, ok = volume.inodeLeaseMap[inodeNumber]
		if !ok {
			globals.volumesLock.Unlock()
			reply.LeaseReplyType = LeaseReplyTypeDenied
			err = fmt.Errorf("LeaseRequestType %v not allowed for non-existent Lease [case 1]", in.LeaseRequestType)
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

	replyToReturn = <-leaseRequestOperation.replyChan
	reply.LeaseReplyType = replyToReturn.LeaseReplyType

	globals.volumesLock.Lock()

	delayedUnmountLeaseReleaseStartWG.Add(1)

	delayedUnmountListElement = volume.delayedUnmountList.Front()

	for nil != delayedUnmountListElement {
		delayedUnmount = delayedUnmountListElement.Value.(*mountStruct)
		_ = volume.delayedUnmountList.Remove(delayedUnmountListElement)

		delayedUnmount.armReleaseOfAllLeasesWhileLocked(&delayedUnmountLeaseReleaseStartWG, &delayedUnmountLeaseReleaseFinishedWG)

		delete(volume.mountMapByMountIDAsByteArray, delayedUnmount.mountIDAsByteArray)
		delete(volume.mountMapByMountIDAsString, delayedUnmount.mountIDAsString)

		delete(globals.mountMapByMountIDAsByteArray, delayedUnmount.mountIDAsByteArray)
		delete(globals.mountMapByMountIDAsString, delayedUnmount.mountIDAsString)

		delayedUnmountListElement = volume.delayedUnmountList.Front()
	}

	globals.volumesLock.Unlock()

	delayedUnmountLeaseReleaseStartWG.Done()
	delayedUnmountLeaseReleaseFinishedWG.Wait()

	err = nil
	return
}

func (inodeLease *inodeLeaseStruct) handler() {
	var (
		leaseRequestOperation *leaseRequestOperationStruct
	)

	for {
		select {
		case leaseRequestOperation = <-inodeLease.requestChan:
			inodeLease.handleOperation(leaseRequestOperation)
		case _ = <-inodeLease.longAgoTimer.C:
			inodeLease.handleLongAgoTimerPop()
		case _ = <-inodeLease.interruptTimer.C:
			inodeLease.handleInterruptTimerPop()
		case _, _ = <-inodeLease.stopChan:
			inodeLease.handleStopChanClose() // will not return
		}
	}
}

func (inodeLease *inodeLeaseStruct) handleOperation(leaseRequestOperation *leaseRequestOperationStruct) {
	var (
		err                      error
		leaseReply               *LeaseReply
		leaseRequest             *leaseRequestStruct
		leaseRequestElement      *list.Element
		ok                       bool
		rpcInterrupt             *RPCInterrupt
		rpcInterruptBuf          []byte
		sharedHolderLeaseRequest *leaseRequestStruct
		sharedHolderListElement  *list.Element
	)

	globals.volumesLock.Lock()
	defer globals.volumesLock.Unlock()

	inodeLease.volume.inodeLeaseLRU.MoveToBack(inodeLease.lruElement)

	switch leaseRequestOperation.LeaseRequestType {
	case LeaseRequestTypeShared:
		_, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber]
		if ok {
			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			leaseRequestOperation.replyChan <- leaseReply
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber] returned !ok
			leaseRequest = &leaseRequestStruct{
				mount:        leaseRequestOperation.mount,
				inodeLease:   inodeLease,
				requestState: leaseRequestStateSharedRequested,
				replyChan:    leaseRequestOperation.replyChan,
			}
			leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber] = leaseRequest
			switch inodeLease.leaseState {
			case inodeLeaseStateNone:
				leaseRequest.requestState = leaseRequestStateSharedGranted
				inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
				leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
				inodeLease.lastGrantTime = time.Now()
				inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeShared,
				}
				leaseRequest.replyChan <- leaseReply
			case inodeLeaseStateSharedGrantedRecently:
				if !inodeLease.longAgoTimer.Stop() {
					<-inodeLease.longAgoTimer.C
				}
				inodeLease.lastGrantTime = time.Time{}
				inodeLease.longAgoTimer = &time.Timer{}
				leaseRequest.requestState = leaseRequestStateSharedGranted
				inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
				leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
				inodeLease.lastGrantTime = time.Now()
				inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeShared,
				}
				leaseRequest.replyChan <- leaseReply
			case inodeLeaseStateSharedGrantedLongAgo:
				leaseRequest.requestState = leaseRequestStateSharedGranted
				inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
				leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
				inodeLease.lastGrantTime = time.Now()
				inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeShared,
				}
				leaseRequest.replyChan <- leaseReply
			case inodeLeaseStateSharedPromoting:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedReleasing:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedExpired:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeShared, found unexpected inodeLease.leaseState inodeLeaseStateSharedExpired")
			case inodeLeaseStateExclusiveGrantedRecently:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveGrantedLongAgo:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
				inodeLease.leaseState = inodeLeaseStateExclusiveDemoting
				inodeLease.demotingHolder = inodeLease.exclusiveHolder
				inodeLease.demotingHolder.requestState = leaseRequestStateExclusiveDemoting
				inodeLease.exclusiveHolder = nil
				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeDemote,
					InodeNumber:      int64(inodeLease.InodeNumber),
				}
				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logger.Fatalf("(*inodeLeaseStruct).handleOperation() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
				}
				evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(inodeLease.demotingHolder.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))
				globals.retryrpcSvr.SendCallback(string(inodeLease.demotingHolder.mount.mountIDAsString), rpcInterruptBuf)
				inodeLease.lastInterruptTime = time.Now()
				inodeLease.interruptsSent = 1
				inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
			case inodeLeaseStateExclusiveDemoting:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveReleasing:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveExpired:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeShared, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveExpired")
			default:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeShared, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		}
	case LeaseRequestTypePromote:
		leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber]
		if ok {
			if leaseRequestStateSharedGranted == leaseRequest.requestState {
				switch inodeLease.leaseState {
				case inodeLeaseStateNone:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateNone")
				case inodeLeaseStateSharedGrantedRecently:
					if nil == inodeLease.promotingHolder {
						_ = inodeLease.sharedHoldersList.Remove(leaseRequest.listElement)
						leaseRequest.listElement = nil
						if 0 == inodeLease.sharedHoldersList.Len() {
							leaseRequest.requestState = leaseRequestStateExclusiveGranted
							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypePromoted,
							}
							leaseRequestOperation.replyChan <- leaseReply
							inodeLease.exclusiveHolder = leaseRequest
							inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
							inodeLease.lastGrantTime = time.Now()
							inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
						} else {
							inodeLease.promotingHolder = leaseRequest
							leaseRequest.replyChan = leaseRequestOperation.replyChan
						}
					} else { // nil != inodeLease.promotingHolder
						leaseReply = &LeaseReply{
							LeaseReplyType: LeaseReplyTypeDenied,
						}
						leaseRequestOperation.replyChan <- leaseReply
					}
				case inodeLeaseStateSharedGrantedLongAgo:
					_ = inodeLease.sharedHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					if 0 == inodeLease.sharedHoldersList.Len() {
						inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
						inodeLease.exclusiveHolder = leaseRequest
						leaseRequest.requestState = leaseRequestStateExclusiveGranted
						leaseReply = &LeaseReply{
							LeaseReplyType: LeaseReplyTypePromoted,
						}
						leaseRequestOperation.replyChan <- leaseReply
						inodeLease.lastGrantTime = time.Now()
						inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
					} else {
						inodeLease.leaseState = inodeLeaseStateSharedPromoting
						inodeLease.promotingHolder = leaseRequest
						leaseRequest.requestState = leaseRequestStateSharedPromoting
						rpcInterrupt = &RPCInterrupt{
							RPCInterruptType: RPCInterruptTypeRelease,
							InodeNumber:      int64(inodeLease.InodeNumber),
						}
						rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
						if nil != err {
							logger.Fatalf("(*inodeLeaseStruct).handleOperation() unable to json.Marshal(rpcInterrupt: %#v): %v [case 2]", rpcInterrupt, err)
						}
						for nil != inodeLease.sharedHoldersList.Front() {
							leaseRequestElement = inodeLease.sharedHoldersList.Front()
							leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
							_ = inodeLease.sharedHoldersList.Remove(leaseRequestElement)
							leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)
							leaseRequest.requestState = leaseRequestStateSharedReleasing
							evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))
							globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)
						}
						inodeLease.lastInterruptTime = time.Now()
						inodeLease.interruptsSent = 1
						inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
					}
				case inodeLeaseStateSharedPromoting:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateSharedPromoting")
				case inodeLeaseStateSharedReleasing:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateSharedReleasing")
				case inodeLeaseStateSharedExpired:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateSharedExpired")
				case inodeLeaseStateExclusiveGrantedRecently:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveGrantedRecently")
				case inodeLeaseStateExclusiveGrantedLongAgo:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveGrantedLongAgo")
				case inodeLeaseStateExclusiveDemoting:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveDemoting")
				case inodeLeaseStateExclusiveReleasing:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveReleasing")
				case inodeLeaseStateExclusiveExpired:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveExpired")
				default:
					logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
				}
			} else { // leaseRequestStateSharedGranted != leaseRequest.requestState
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply
			}
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber] returned !ok
			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			leaseRequestOperation.replyChan <- leaseReply
		}
	case LeaseRequestTypeExclusive:
		_, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber]
		if ok {
			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			leaseRequestOperation.replyChan <- leaseReply
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber] returned !ok
			leaseRequest = &leaseRequestStruct{
				mount:        leaseRequestOperation.mount,
				inodeLease:   inodeLease,
				requestState: leaseRequestStateExclusiveRequested,
				replyChan:    leaseRequestOperation.replyChan,
			}
			leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber] = leaseRequest
			switch inodeLease.leaseState {
			case inodeLeaseStateNone:
				leaseRequest.requestState = leaseRequestStateExclusiveGranted
				inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
				inodeLease.exclusiveHolder = leaseRequest
				inodeLease.lastGrantTime = time.Now()
				inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeExclusive,
				}
				leaseRequest.replyChan <- leaseReply
			case inodeLeaseStateSharedGrantedRecently:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedGrantedLongAgo:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
				inodeLease.leaseState = inodeLeaseStateSharedReleasing
				for nil != inodeLease.sharedHoldersList.Front() {
					sharedHolderListElement = inodeLease.sharedHoldersList.Front()
					sharedHolderLeaseRequest = sharedHolderListElement.Value.(*leaseRequestStruct)
					_ = inodeLease.sharedHoldersList.Remove(sharedHolderListElement)
					sharedHolderLeaseRequest.requestState = leaseRequestStateSharedReleasing
					sharedHolderLeaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(sharedHolderLeaseRequest)
					rpcInterrupt = &RPCInterrupt{
						RPCInterruptType: RPCInterruptTypeRelease,
						InodeNumber:      int64(inodeLease.InodeNumber),
					}
					rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
					if nil != err {
						logger.Fatalf("(*inodeLeaseStruct).handleOperation() unable to json.Marshal(rpcInterrupt: %#v): %v [case 3]", rpcInterrupt, err)
					}
					evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(sharedHolderLeaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))
					globals.retryrpcSvr.SendCallback(string(sharedHolderLeaseRequest.mount.mountIDAsString), rpcInterruptBuf)
				}
				inodeLease.lastInterruptTime = time.Now()
				inodeLease.interruptsSent = 1
				inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
			case inodeLeaseStateSharedPromoting:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedReleasing:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedExpired:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeExclusive, found unexpected inodeLease.leaseState inodeLeaseStateSharedExpired")
			case inodeLeaseStateExclusiveGrantedRecently:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveGrantedLongAgo:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
				inodeLease.leaseState = inodeLeaseStateExclusiveReleasing
				inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveReleasing
				inodeLease.exclusiveHolder.listElement = inodeLease.releasingHoldersList.PushBack(inodeLease.exclusiveHolder)
				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      int64(inodeLease.InodeNumber),
				}
				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logger.Fatalf("(*inodeLeaseStruct).handleOperation() unable to json.Marshal(rpcInterrupt: %#v): %v [case 4]", rpcInterrupt, err)
				}
				evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(inodeLease.exclusiveHolder.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))
				globals.retryrpcSvr.SendCallback(string(inodeLease.exclusiveHolder.mount.mountIDAsString), rpcInterruptBuf)
				inodeLease.exclusiveHolder = nil
				inodeLease.lastInterruptTime = time.Now()
				inodeLease.interruptsSent = 1
				inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
			case inodeLeaseStateExclusiveDemoting:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveReleasing:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveExpired:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeExclusive, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveExpired")
			default:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeExclusive, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		}
	case LeaseRequestTypeDemote:
		leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber]
		if ok {
			switch inodeLease.leaseState {
			case inodeLeaseStateNone:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply
			case inodeLeaseStateSharedGrantedRecently:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply
			case inodeLeaseStateSharedGrantedLongAgo:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply
			case inodeLeaseStateSharedPromoting:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply
			case inodeLeaseStateSharedReleasing:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply
			case inodeLeaseStateSharedExpired:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeDemote, found unexpected inodeLease.leaseState inodeLeaseStateSharedExpired")
			case inodeLeaseStateExclusiveGrantedRecently:
				if leaseRequestStateExclusiveGranted == leaseRequest.requestState {
					if !inodeLease.longAgoTimer.Stop() {
						<-inodeLease.longAgoTimer.C
					}
					inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
					leaseRequest.requestState = leaseRequestStateSharedGranted
					inodeLease.exclusiveHolder = nil
					leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDemoted,
					}
					leaseRequestOperation.replyChan <- leaseReply
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeShared,
					}
					leaseRequestElement = inodeLease.requestedList.Front()
					for nil != leaseRequestElement {
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
						if leaseRequestStateSharedRequested == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateSharedGranted
							_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
							leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
							leaseRequest.replyChan <- leaseReply
							leaseRequestElement = inodeLease.requestedList.Front()
						} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
							leaseRequestElement = nil
						}
					}
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				} else { // leaseRequestStateExclusiveGranted == leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateExclusiveGrantedLongAgo:
				if leaseRequestStateExclusiveGranted == leaseRequest.requestState {
					inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
					leaseRequest.requestState = leaseRequestStateSharedGranted
					inodeLease.exclusiveHolder = nil
					leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDemoted,
					}
					leaseRequestOperation.replyChan <- leaseReply
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				} else { // leaseRequestStateExclusiveGranted != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateExclusiveDemoting:
				if leaseRequestStateExclusiveDemoting == leaseRequest.requestState {
					if !inodeLease.interruptTimer.Stop() {
						<-inodeLease.interruptTimer.C
					}
					inodeLease.lastInterruptTime = time.Time{}
					inodeLease.interruptsSent = 0
					inodeLease.interruptTimer = &time.Timer{}
					inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
					inodeLease.demotingHolder = nil
					leaseRequest.requestState = leaseRequestStateSharedGranted
					leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDemoted,
					}
					leaseRequestOperation.replyChan <- leaseReply
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeShared,
					}
					leaseRequestElement = inodeLease.requestedList.Front()
					for nil != leaseRequestElement {
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
						if leaseRequestStateSharedRequested == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateSharedGranted
							_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
							leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
							leaseRequest.replyChan <- leaseReply
							leaseRequestElement = inodeLease.requestedList.Front()
						} else { // leaseRequestStateSharedRequested != leaseRequest.requestState
							leaseRequestElement = nil
						}
					}
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				} else { // leaseRequestStateExclusiveDemoting == leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateExclusiveReleasing:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply
			case inodeLeaseStateExclusiveExpired:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeDemote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveExpired")
			default:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeDemote, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber] returned !ok
			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			leaseRequestOperation.replyChan <- leaseReply
		}
	case LeaseRequestTypeRelease:
		leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber]
		if ok {
			switch inodeLease.leaseState {
			case inodeLeaseStateNone:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply
			case inodeLeaseStateSharedGrantedRecently:
				if leaseRequestStateSharedGranted == leaseRequest.requestState {
					_ = inodeLease.sharedHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeReleased,
					}
					leaseRequestOperation.replyChan <- leaseReply
					if 0 == inodeLease.sharedHoldersList.Len() {
						if !inodeLease.longAgoTimer.Stop() {
							<-inodeLease.longAgoTimer.C
						}
						if nil == inodeLease.promotingHolder {
							leaseRequestElement = inodeLease.requestedList.Front()
							if nil == leaseRequestElement {
								inodeLease.leaseState = inodeLeaseStateNone
							} else { // nil != leaseRequestElement
								leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
								_ = inodeLease.requestedList.Remove(leaseRequestElement)
								if leaseRequestStateSharedRequested == leaseRequest.requestState {
									if 0 == inodeLease.requestedList.Len() {
										inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
										inodeLease.exclusiveHolder = leaseRequest
										leaseRequest.listElement = nil
										leaseRequest.requestState = leaseRequestStateExclusiveGranted
										leaseReply = &LeaseReply{
											LeaseReplyType: LeaseReplyTypeExclusive,
										}
										leaseRequest.replyChan <- leaseReply
									} else { // 0 < inodeLease.requestedList.Len()
										inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
										leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
										leaseRequest.requestState = leaseRequestStateSharedGranted
										leaseReply = &LeaseReply{
											LeaseReplyType: LeaseReplyTypeShared,
										}
										leaseRequest.replyChan <- leaseReply
										leaseRequestElement = inodeLease.requestedList.Front()
										for nil != leaseRequestElement {
											leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
											_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
											if leaseRequestStateSharedRequested == leaseRequest.requestState {
												leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
												leaseRequest.requestState = leaseRequestStateSharedGranted
												leaseRequest.replyChan <- leaseReply
												leaseRequestElement = inodeLease.requestedList.Front()
											} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState {
												leaseRequestElement = nil
											}
										}
									}
								} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
									inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
									inodeLease.exclusiveHolder = leaseRequest
									leaseRequest.listElement = nil
									leaseRequest.requestState = leaseRequestStateExclusiveGranted
									leaseReply = &LeaseReply{
										LeaseReplyType: LeaseReplyTypeExclusive,
									}
									leaseRequest.replyChan <- leaseReply
								}
								inodeLease.lastGrantTime = time.Now()
								inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
							}
						} else { // nil != inodeLease.promotingHolder
							inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
							inodeLease.exclusiveHolder = inodeLease.promotingHolder
							inodeLease.promotingHolder = nil
							inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted
							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypePromoted,
							}
							inodeLease.exclusiveHolder.replyChan <- leaseReply
							inodeLease.lastGrantTime = time.Now()
							inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
						}
					}
				} else { // leaseRequestStateSharedGranted != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateSharedGrantedLongAgo:
				if leaseRequestStateSharedGranted == leaseRequest.requestState {
					_ = inodeLease.sharedHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeReleased,
					}
					leaseRequestOperation.replyChan <- leaseReply
					if 0 == inodeLease.sharedHoldersList.Len() {
						inodeLease.leaseState = inodeLeaseStateNone
					}
				} else { // leaseRequestStateSharedGranted != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateSharedPromoting:
				if leaseRequestStateSharedReleasing == leaseRequest.requestState {
					_ = inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeReleased,
					}
					leaseRequestOperation.replyChan <- leaseReply
					if 0 == inodeLease.releasingHoldersList.Len() {
						if !inodeLease.interruptTimer.Stop() {
							<-inodeLease.interruptTimer.C
						}
						inodeLease.lastInterruptTime = time.Time{}
						inodeLease.interruptsSent = 0
						inodeLease.interruptTimer = &time.Timer{}
						inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
						inodeLease.exclusiveHolder = inodeLease.promotingHolder
						inodeLease.promotingHolder = nil
						inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted
						leaseReply = &LeaseReply{
							LeaseReplyType: LeaseReplyTypePromoted,
						}
						inodeLease.exclusiveHolder.replyChan <- leaseReply
						inodeLease.lastGrantTime = time.Now()
						inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
					}
				} else { // leaseRequestStateSharedReleasing != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateSharedReleasing:
				if leaseRequestStateSharedReleasing == leaseRequest.requestState {
					_ = inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeReleased,
					}
					leaseRequestOperation.replyChan <- leaseReply
					if 0 == inodeLease.releasingHoldersList.Len() {
						if !inodeLease.interruptTimer.Stop() {
							<-inodeLease.interruptTimer.C
						}
						inodeLease.lastInterruptTime = time.Time{}
						inodeLease.interruptsSent = 0
						inodeLease.interruptTimer = &time.Timer{}
						inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
						leaseRequestElement = inodeLease.requestedList.Front()
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
						_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
						leaseRequest.listElement = nil
						inodeLease.exclusiveHolder = leaseRequest
						leaseRequest.requestState = leaseRequestStateExclusiveGranted
						leaseReply = &LeaseReply{
							LeaseReplyType: LeaseReplyTypeExclusive,
						}
						leaseRequest.replyChan <- leaseReply
						inodeLease.lastGrantTime = time.Now()
						inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
					}
				} else { // leaseRequestStateSharedReleasing != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateSharedExpired:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeRelease, found unknown inodeLease.leaseState inodeLeaseStateSharedExpired")
			case inodeLeaseStateExclusiveGrantedRecently:
				if leaseRequestStateExclusiveGranted == leaseRequest.requestState {
					if !inodeLease.longAgoTimer.Stop() {
						<-inodeLease.longAgoTimer.C
					}
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeReleased,
					}
					leaseRequestOperation.replyChan <- leaseReply
					leaseRequestElement = inodeLease.requestedList.Front()
					if nil == leaseRequestElement {
						inodeLease.leaseState = inodeLeaseStateNone
						inodeLease.exclusiveHolder = nil
						inodeLease.lastGrantTime = time.Time{}
						inodeLease.longAgoTimer = &time.Timer{}
					} else { // nil != leaseRequestElement
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
						_ = inodeLease.requestedList.Remove(leaseRequestElement)
						if leaseRequestStateSharedRequested == leaseRequest.requestState {
							leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
							inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
							leaseRequest.requestState = leaseRequestStateSharedGranted
							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeShared,
							}
							leaseRequest.replyChan <- leaseReply
							leaseRequestElement = inodeLease.requestedList.Front()
							for nil != leaseRequestElement {
								leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
								if leaseRequestStateSharedRequested == leaseRequest.requestState {
									_ = inodeLease.requestedList.Remove(leaseRequestElement)
									leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
									leaseRequest.requestState = leaseRequestStateSharedGranted
									leaseRequest.replyChan <- leaseReply
									leaseRequestElement = inodeLease.requestedList.Front()
								} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
									leaseRequestElement = nil
								}
							}
						} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
							inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
							leaseRequest.requestState = leaseRequestStateExclusiveGranted
							inodeLease.exclusiveHolder = leaseRequest
							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeExclusive,
							}
							leaseRequest.replyChan <- leaseReply
						}
						inodeLease.lastGrantTime = time.Now()
						inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
					}
				} else { // leaseRequestStateExclusiveGranted != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateExclusiveGrantedLongAgo:
				if leaseRequestStateExclusiveGranted == leaseRequest.requestState {
					inodeLease.leaseState = inodeLeaseStateNone
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
					inodeLease.exclusiveHolder = nil
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeReleased,
					}
					leaseRequestOperation.replyChan <- leaseReply
				} else { // leaseRequestStateExclusiveGranted != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateExclusiveDemoting:
				if leaseRequestStateExclusiveDemoting == leaseRequest.requestState {
					if !inodeLease.interruptTimer.Stop() {
						<-inodeLease.interruptTimer.C
					}
					inodeLease.lastInterruptTime = time.Time{}
					inodeLease.interruptsSent = 0
					inodeLease.interruptTimer = &time.Timer{}
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
					inodeLease.demotingHolder = nil
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeReleased,
					}
					leaseRequestOperation.replyChan <- leaseReply
					leaseRequestElement = inodeLease.requestedList.Front()
					leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
					_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
					if (nil == inodeLease.requestedList.Front()) || (leaseRequestStateExclusiveRequested == inodeLease.requestedList.Front().Value.(*leaseRequestStruct).requestState) {
						inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
						leaseRequest.requestState = leaseRequestStateExclusiveGranted
						leaseRequest.listElement = nil
						inodeLease.exclusiveHolder = leaseRequest
						leaseReply = &LeaseReply{
							LeaseReplyType: LeaseReplyTypeExclusive,
						}
						leaseRequest.replyChan <- leaseReply
					} else {
						inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
						leaseRequest.requestState = leaseRequestStateSharedGranted
						leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
						leaseReply = &LeaseReply{
							LeaseReplyType: LeaseReplyTypeShared,
						}
						leaseRequest.replyChan <- leaseReply
						leaseRequestElement = inodeLease.requestedList.Front()
						for nil != leaseRequestElement {
							leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
							if leaseRequestStateSharedRequested == leaseRequest.requestState {
								_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
								leaseRequest.requestState = leaseRequestStateSharedGranted
								leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
								leaseRequest.replyChan <- leaseReply
								leaseRequestElement = inodeLease.requestedList.Front()
							} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
								leaseRequestElement = nil
							}
						}
					}
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				} else { // leaseRequestStateExclusiveDemoting != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateExclusiveReleasing:
				if leaseRequestStateExclusiveReleasing == leaseRequest.requestState {
					if !inodeLease.interruptTimer.Stop() {
						<-inodeLease.interruptTimer.C
					}
					inodeLease.lastInterruptTime = time.Time{}
					inodeLease.interruptsSent = 0
					inodeLease.interruptTimer = &time.Timer{}
					inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
					_ = inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeReleased,
					}
					leaseRequestOperation.replyChan <- leaseReply
					leaseRequestElement = inodeLease.requestedList.Front()
					leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
					leaseRequest.requestState = leaseRequestStateExclusiveGranted
					_ = inodeLease.requestedList.Remove(leaseRequestElement)
					leaseRequest.listElement = nil
					inodeLease.exclusiveHolder = leaseRequest
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeExclusive,
					}
					leaseRequest.replyChan <- leaseReply
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)
				} else { // leaseRequestStateExclusiveReleasing != leaseRequest.requestState
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}
			case inodeLeaseStateExclusiveExpired:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeRelease, found unknown inodeLease.leaseState inodeLeaseStateExclusiveExpired")
			default:
				logger.Fatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeRelease, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.InodeNumber] returned !ok
			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			leaseRequestOperation.replyChan <- leaseReply
		}
	default:
		logger.Fatalf("(*inodeLeaseStruct).handleOperation() found unexpected leaseRequestOperation.LeaseRequestType: %v", leaseRequestOperation.LeaseRequestType)
	}
}

func (inodeLease *inodeLeaseStruct) handleLongAgoTimerPop() {
	var (
		err                 error
		leaseRequest        *leaseRequestStruct
		leaseRequestElement *list.Element
		rpcInterrupt        *RPCInterrupt
		rpcInterruptBuf     []byte
	)

	globals.volumesLock.Lock()

	inodeLease.lastGrantTime = time.Time{}
	inodeLease.longAgoTimer = &time.Timer{}

	switch inodeLease.leaseState {
	case inodeLeaseStateSharedGrantedRecently:
		inodeLease.leaseState = inodeLeaseStateSharedGrantedLongAgo

		if (nil != inodeLease.promotingHolder) || (0 != inodeLease.requestedList.Len()) {
			if nil != inodeLease.promotingHolder {
				inodeLease.leaseState = inodeLeaseStateSharedPromoting
				inodeLease.promotingHolder.requestState = leaseRequestStateSharedPromoting
			} else {
				inodeLease.leaseState = inodeLeaseStateSharedReleasing
			}

			leaseRequestElement = inodeLease.sharedHoldersList.Front()
			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
				leaseRequest.requestState = leaseRequestStateSharedReleasing
				_ = inodeLease.sharedHoldersList.Remove(leaseRequestElement)
				leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)

				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      int64(inodeLease.InodeNumber),
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logger.Fatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
				}

				evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

				globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

				leaseRequestElement = inodeLease.sharedHoldersList.Front()
			}

			inodeLease.lastInterruptTime = time.Now()
			inodeLease.interruptsSent = 1

			inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
		}
	case inodeLeaseStateExclusiveGrantedRecently:
		inodeLease.leaseState = inodeLeaseStateExclusiveGrantedLongAgo

		leaseRequestElement = inodeLease.requestedList.Front()
		if nil != leaseRequestElement {
			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
			switch leaseRequest.requestState {
			case leaseRequestStateSharedRequested:
				inodeLease.leaseState = inodeLeaseStateExclusiveDemoting

				inodeLease.demotingHolder = inodeLease.exclusiveHolder
				inodeLease.demotingHolder.requestState = leaseRequestStateExclusiveDemoting
				inodeLease.exclusiveHolder = nil

				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeDemote,
					InodeNumber:      int64(inodeLease.InodeNumber),
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logger.Fatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 2]", rpcInterrupt, err)
				}

				evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(inodeLease.demotingHolder.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

				globals.retryrpcSvr.SendCallback(string(inodeLease.demotingHolder.mount.mountIDAsString), rpcInterruptBuf)
			case leaseRequestStateExclusiveRequested:
				inodeLease.leaseState = inodeLeaseStateExclusiveReleasing

				inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveReleasing
				inodeLease.exclusiveHolder.listElement = inodeLease.releasingHoldersList.PushBack(inodeLease.exclusiveHolder)

				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      int64(inodeLease.InodeNumber),
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logger.Fatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 3]", rpcInterrupt, err)
				}

				evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(inodeLease.exclusiveHolder.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

				globals.retryrpcSvr.SendCallback(string(inodeLease.exclusiveHolder.mount.mountIDAsString), rpcInterruptBuf)

				inodeLease.exclusiveHolder = nil
			default:
				logger.Fatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() found requestedList with unexpected leaseRequest.requestState: %v", leaseRequest.requestState)
			}

			inodeLease.lastInterruptTime = time.Now()
			inodeLease.interruptsSent = 1

			inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
		}
	default:
		logger.Fatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() called while in wrong state (%v)", inodeLease.leaseState)
	}

	globals.volumesLock.Unlock()
}

func (inodeLease *inodeLeaseStruct) handleInterruptTimerPop() {
	var (
		err                 error
		leaseReply          *LeaseReply
		leaseRequest        *leaseRequestStruct
		leaseRequestElement *list.Element
		rpcInterrupt        *RPCInterrupt
		rpcInterruptBuf     []byte
	)

	globals.volumesLock.Lock()

	if globals.leaseInterruptLimit <= inodeLease.interruptsSent {
		switch inodeLease.leaseState {
		case inodeLeaseStateSharedPromoting:
			inodeLease.leaseState = inodeLeaseStateSharedExpired

			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 1]")
			}

			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

				delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)

				if leaseRequest.mount.acceptingLeaseRequests {
					leaseRequest.mount.acceptingLeaseRequests = false
					_ = leaseRequest.mount.volume.delayedUnmountList.PushBack(leaseRequest.mount)
				}

				inodeLease.releasingHoldersList.Remove(leaseRequestElement)
				leaseRequest.listElement = nil

				leaseRequestElement = inodeLease.releasingHoldersList.Front()
			}

			inodeLease.exclusiveHolder = inodeLease.promotingHolder
			inodeLease.promotingHolder = nil

			inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted

			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeExclusive,
			}

			inodeLease.exclusiveHolder.replyChan <- leaseReply

			inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
		case inodeLeaseStateSharedReleasing:
			inodeLease.leaseState = inodeLeaseStateSharedExpired

			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 2]")
			}

			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

				delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)

				if leaseRequest.mount.acceptingLeaseRequests {
					leaseRequest.mount.acceptingLeaseRequests = false
					_ = leaseRequest.mount.volume.delayedUnmountList.PushBack(leaseRequest.mount)
				}

				inodeLease.releasingHoldersList.Remove(leaseRequestElement)
				leaseRequest.listElement = nil

				leaseRequestElement = inodeLease.releasingHoldersList.Front()
			}

			leaseRequestElement = inodeLease.requestedList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty requestedList [case 1]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
			if leaseRequestStateExclusiveRequested != leaseRequest.requestState {
				logger.Fatalf("(inodeLeaseStruct).handleInterruptTimerPop() found unexpected requestedList.Front().requestState: %v [case 1]", leaseRequest.requestState)
			}

			inodeLease.requestedList.Remove(leaseRequest.listElement)
			leaseRequest.listElement = nil
			inodeLease.exclusiveHolder = leaseRequest

			inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted

			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeExclusive,
			}

			inodeLease.exclusiveHolder.replyChan <- leaseReply

			inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
		case inodeLeaseStateExclusiveReleasing:
			inodeLease.leaseState = inodeLeaseStateExclusiveExpired

			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 3]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

			delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)

			if leaseRequest.mount.acceptingLeaseRequests {
				leaseRequest.mount.acceptingLeaseRequests = false
				_ = leaseRequest.mount.volume.delayedUnmountList.PushBack(leaseRequest.mount)
			}

			inodeLease.releasingHoldersList.Remove(leaseRequestElement)
			leaseRequest.listElement = nil

			if nil != inodeLease.releasingHoldersList.Front() {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found releasingHoldersList unexpectedly with >1 leaseRequestElements")
			}

			leaseRequestElement = inodeLease.requestedList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty requestedList [case 2]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
			if leaseRequestStateExclusiveRequested != leaseRequest.requestState {
				logger.Fatalf("(inodeLeaseStruct).handleInterruptTimerPop() found unexpected requestedList.Front().requestState: %v [case 2]", leaseRequest.requestState)
			}

			inodeLease.requestedList.Remove(leaseRequest.listElement)
			leaseRequest.listElement = nil
			inodeLease.exclusiveHolder = leaseRequest

			inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted

			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeExclusive,
			}

			inodeLease.exclusiveHolder.replyChan <- leaseReply

			inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
		case inodeLeaseStateExclusiveDemoting:
			inodeLease.leaseState = inodeLeaseStateExclusiveExpired

			if nil == inodeLease.demotingHolder {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty demotingHolder [case 1]")
			}

			delete(inodeLease.demotingHolder.mount.leaseRequestMap, inodeLease.InodeNumber)

			if inodeLease.demotingHolder.mount.acceptingLeaseRequests {
				inodeLease.demotingHolder.mount.acceptingLeaseRequests = false
				_ = inodeLease.demotingHolder.mount.volume.delayedUnmountList.PushBack(inodeLease.demotingHolder.mount)
			}

			inodeLease.demotingHolder = nil

			leaseRequestElement = inodeLease.requestedList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty requestedList [case 3]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
			if leaseRequestStateSharedRequested != leaseRequest.requestState {
				logger.Fatalf("(inodeLeaseStruct).handleInterruptTimerPop() found unexpected requestedList.Front().requestState: %v [case 3]", leaseRequest.requestState)
			}

			for {
				inodeLease.requestedList.Remove(leaseRequestElement)
				leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)

				leaseRequest.requestState = leaseRequestStateSharedGranted

				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeShared,
				}

				leaseRequest.replyChan <- leaseReply

				leaseRequestElement = inodeLease.requestedList.Front()
				if nil == leaseRequestElement {
					break
				}

				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
				if leaseRequestStateExclusiveRequested == leaseRequest.requestState {
					break
				}
			}

			inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
		default:
			logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found unexpected leaseState: %v [case 1]", inodeLease.leaseState)
		}

		inodeLease.lastGrantTime = time.Now()
		inodeLease.longAgoTimer = time.NewTimer(globals.minLeaseDuration)

		inodeLease.lastInterruptTime = time.Time{}
		inodeLease.interruptsSent = 0

		inodeLease.interruptTimer = &time.Timer{}
	} else { // globals.leaseInterruptLimit > inodeLease.interruptsSent
		switch inodeLease.leaseState {
		case inodeLeaseStateSharedPromoting:
			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 4]")
			}
			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      int64(inodeLease.InodeNumber),
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
				}

				evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

				globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

				leaseRequestElement = leaseRequestElement.Next()
			}
		case inodeLeaseStateSharedReleasing:
			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 5]")
			}
			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      int64(inodeLease.InodeNumber),
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
				}

				evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

				globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

				leaseRequestElement = leaseRequestElement.Next()
			}
		case inodeLeaseStateExclusiveReleasing:
			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 6]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

			rpcInterrupt = &RPCInterrupt{
				RPCInterruptType: RPCInterruptTypeRelease,
				InodeNumber:      int64(inodeLease.InodeNumber),
			}

			rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
			if nil != err {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 2]", rpcInterrupt, err)
			}

			evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

			globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)
		case inodeLeaseStateExclusiveDemoting:
			if nil == inodeLease.demotingHolder {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty demotingHolder [case 2]")
			}

			rpcInterrupt = &RPCInterrupt{
				RPCInterruptType: RPCInterruptTypeDemote,
				InodeNumber:      int64(inodeLease.InodeNumber),
			}

			rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
			if nil != err {
				logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 3]", rpcInterrupt, err)
			}

			evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(inodeLease.demotingHolder.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

			globals.retryrpcSvr.SendCallback(string(inodeLease.demotingHolder.mount.mountIDAsString), rpcInterruptBuf)
		default:
			logger.Fatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found unexpected leaseState: %v [case 2]", inodeLease.leaseState)
		}

		inodeLease.lastInterruptTime = time.Now()
		inodeLease.interruptsSent++

		inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
	}

	globals.volumesLock.Unlock()
}

func (inodeLease *inodeLeaseStruct) handleStopChanClose() {
	var (
		err                   error
		leaseReply            *LeaseReply
		leaseRequest          *leaseRequestStruct
		leaseRequestElement   *list.Element
		leaseRequestOperation *leaseRequestOperationStruct
		ok                    bool
		rpcInterrupt          *RPCInterrupt
		rpcInterruptBuf       []byte
	)

	// Deny all pending requests:

	globals.volumesLock.Lock()

	for nil != inodeLease.requestedList.Front() {
		leaseRequestElement = inodeLease.requestedList.Front()
		leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
		inodeLease.requestedList.Remove(leaseRequest.listElement)
		leaseRequest.listElement = nil
		leaseRequest.requestState = leaseRequestStateNone
		leaseReply = &LeaseReply{
			LeaseReplyType: LeaseReplyTypeDenied,
		}
		leaseRequest.replyChan <- leaseReply
	}

	// If inodeLease.leaseState is inodeLeaseStateSharedPromoting:
	//   Reject inodeLease.promotingHolder's LeaseRequestTypePromote
	//   Ensure formerly inodeLease.promotingHolder is also now releasing

	if inodeLeaseStateSharedPromoting == inodeLease.leaseState {
		leaseRequest = inodeLease.promotingHolder
		inodeLease.promotingHolder = nil

		leaseReply = &LeaseReply{
			LeaseReplyType: LeaseReplyTypeDenied,
		}

		leaseRequest.replyChan <- leaseReply

		leaseRequest.requestState = leaseRequestStateSharedReleasing

		leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)

		rpcInterrupt = &RPCInterrupt{
			RPCInterruptType: RPCInterruptTypeRelease,
			InodeNumber:      int64(inodeLease.InodeNumber),
		}

		rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
		if nil != err {
			logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
		}

		evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

		globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

		inodeLease.leaseState = inodeLeaseStateSharedReleasing
	}

	// Ensure that inodeLease.leaseState is not inodeLeaseState{Shared|Exclusive}GrantedRecently

	switch inodeLease.leaseState {
	case inodeLeaseStateSharedGrantedRecently:
		if !inodeLease.longAgoTimer.Stop() {
			<-inodeLease.longAgoTimer.C
		}
		inodeLease.lastGrantTime = time.Time{}
		inodeLease.longAgoTimer = &time.Timer{}

		inodeLease.leaseState = inodeLeaseStateSharedGrantedLongAgo
	case inodeLeaseStateExclusiveGrantedRecently:
		if !inodeLease.longAgoTimer.Stop() {
			<-inodeLease.longAgoTimer.C
		}
		inodeLease.lastGrantTime = time.Time{}
		inodeLease.longAgoTimer = &time.Timer{}

		inodeLease.leaseState = inodeLeaseStateExclusiveGrantedLongAgo
	default:
		// Nothing to do here
	}

	// If necessary, transition inodeLease.leaseState from inodeLeaseState{Shared|Exclusive}GrantedLongAgo
	//                                                to   inodeLeaseState{Shared|Exclusive}Releasing

	switch inodeLease.leaseState {
	case inodeLeaseStateSharedGrantedLongAgo:
		for nil != inodeLease.sharedHoldersList.Front() {
			leaseRequestElement = inodeLease.sharedHoldersList.Front()
			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

			inodeLease.sharedHoldersList.Remove(leaseRequestElement)

			leaseRequest.requestState = leaseRequestStateSharedReleasing

			leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)

			rpcInterrupt = &RPCInterrupt{
				RPCInterruptType: RPCInterruptTypeRelease,
				InodeNumber:      int64(inodeLease.InodeNumber),
			}

			rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
			if nil != err {
				logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 2]", rpcInterrupt, err)
			}

			evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

			globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)
		}

		inodeLease.leaseState = inodeLeaseStateSharedReleasing

		inodeLease.lastInterruptTime = time.Now()
		inodeLease.interruptsSent = 1

		inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
	case inodeLeaseStateExclusiveGrantedLongAgo:
		leaseRequest = inodeLease.exclusiveHolder
		inodeLease.exclusiveHolder = nil

		leaseRequest.requestState = leaseRequestStateExclusiveReleasing

		leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)

		rpcInterrupt = &RPCInterrupt{
			RPCInterruptType: RPCInterruptTypeRelease,
			InodeNumber:      int64(inodeLease.InodeNumber),
		}

		rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
		if nil != err {
			logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 3]", rpcInterrupt, err)
		}

		evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

		globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

		inodeLease.leaseState = inodeLeaseStateExclusiveReleasing

		inodeLease.lastInterruptTime = time.Now()
		inodeLease.interruptsSent = 1

		inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
	default:
		// Nothing to do here
	}

	// Loop until inodeLease.leaseState is inodeLeaseStateNone

	for inodeLeaseStateNone != inodeLease.leaseState {
		globals.volumesLock.Unlock()

		select {
		case leaseRequestOperation = <-inodeLease.requestChan:
			globals.volumesLock.Lock()

			switch leaseRequestOperation.LeaseRequestType {
			case LeaseRequestTypeShared:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply

			case LeaseRequestTypePromote:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply

			case LeaseRequestTypeExclusive:
				leaseReply = &LeaseReply{
					LeaseReplyType: LeaseReplyTypeDenied,
				}
				leaseRequestOperation.replyChan <- leaseReply

			case LeaseRequestTypeDemote:
				if inodeLeaseStateExclusiveDemoting == inodeLease.leaseState {
					leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[leaseRequestOperation.inodeLease.InodeNumber]
					if ok {
						if leaseRequestStateExclusiveDemoting == leaseRequest.requestState {
							if leaseRequest == inodeLease.demotingHolder {
								leaseReply = &LeaseReply{
									LeaseReplyType: LeaseReplyTypeDemoted,
								}
								leaseRequestOperation.replyChan <- leaseReply

								inodeLease.demotingHolder = nil

								leaseRequest.requestState = leaseRequestStateExclusiveReleasing

								leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)

								rpcInterrupt = &RPCInterrupt{
									RPCInterruptType: RPCInterruptTypeRelease,
									InodeNumber:      int64(inodeLease.InodeNumber),
								}

								rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
								if nil != err {
									logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 4]", rpcInterrupt, err)
								}

								evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

								globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

								inodeLease.leaseState = inodeLeaseStateExclusiveReleasing

								if !inodeLease.interruptTimer.Stop() {
									<-inodeLease.interruptTimer.C
								}

								inodeLease.lastInterruptTime = time.Now()
								inodeLease.interruptsSent = 1

								inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
							} else {
								leaseReply = &LeaseReply{
									LeaseReplyType: LeaseReplyTypeDenied,
								}
								leaseRequestOperation.replyChan <- leaseReply
							}
						} else {
							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeDenied,
							}
							leaseRequestOperation.replyChan <- leaseReply
						}
					} else {
						leaseReply = &LeaseReply{
							LeaseReplyType: LeaseReplyTypeDenied,
						}
						leaseRequestOperation.replyChan <- leaseReply
					}
				} else {
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}

			case LeaseRequestTypeRelease:
				leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[leaseRequestOperation.inodeLease.InodeNumber]
				if ok {
					switch inodeLease.leaseState {
					case inodeLeaseStateSharedReleasing:
						if leaseRequestStateSharedReleasing == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateNone
							delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
							inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
							leaseRequest.listElement = nil

							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeReleased,
							}
							leaseRequestOperation.replyChan <- leaseReply

							if 0 == inodeLease.releasingHoldersList.Len() {
								inodeLease.leaseState = inodeLeaseStateNone

								if !inodeLease.interruptTimer.Stop() {
									<-inodeLease.interruptTimer.C
								}

								inodeLease.lastInterruptTime = time.Time{}
								inodeLease.interruptsSent = 0

								inodeLease.interruptTimer = &time.Timer{}
							}
						} else {
							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeDenied,
							}
							leaseRequestOperation.replyChan <- leaseReply
						}
					case inodeLeaseStateExclusiveDemoting:
						if leaseRequestStateExclusiveDemoting == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateNone
							delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
							inodeLease.demotingHolder = nil

							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeReleased,
							}
							leaseRequestOperation.replyChan <- leaseReply

							inodeLease.leaseState = inodeLeaseStateNone

							if !inodeLease.interruptTimer.Stop() {
								<-inodeLease.interruptTimer.C
							}

							inodeLease.lastInterruptTime = time.Time{}
							inodeLease.interruptsSent = 0

							inodeLease.interruptTimer = &time.Timer{}
						} else {
							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeDenied,
							}
							leaseRequestOperation.replyChan <- leaseReply
						}
					case inodeLeaseStateExclusiveReleasing:
						if leaseRequestStateExclusiveReleasing == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateNone
							delete(leaseRequest.mount.leaseRequestMap, inodeLease.InodeNumber)
							inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
							leaseRequest.listElement = nil

							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeReleased,
							}
							leaseRequestOperation.replyChan <- leaseReply

							inodeLease.leaseState = inodeLeaseStateNone

							if !inodeLease.interruptTimer.Stop() {
								<-inodeLease.interruptTimer.C
							}

							inodeLease.lastInterruptTime = time.Time{}
							inodeLease.interruptsSent = 0

							inodeLease.interruptTimer = &time.Timer{}
						} else {
							leaseReply = &LeaseReply{
								LeaseReplyType: LeaseReplyTypeDenied,
							}
							leaseRequestOperation.replyChan <- leaseReply
						}
					default:
						leaseReply = &LeaseReply{
							LeaseReplyType: LeaseReplyTypeDenied,
						}
						leaseRequestOperation.replyChan <- leaseReply
					}
				} else {
					leaseReply = &LeaseReply{
						LeaseReplyType: LeaseReplyTypeDenied,
					}
					leaseRequestOperation.replyChan <- leaseReply
				}

			default:
				logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() read unexected leaseRequestOperationLeaseRequestType: %v", leaseRequestOperation.LeaseRequestType)
			}

		case _ = <-inodeLease.interruptTimer.C:
			globals.volumesLock.Lock()

			switch inodeLease.leaseState {
			case inodeLeaseStateSharedGrantedLongAgo:
				logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() hit an interruptTimer pop while unexpectedly in inodeLeaseStateSharedGrantedLongAgo")
			case inodeLeaseStateSharedReleasing:
				if globals.leaseInterruptLimit <= inodeLease.interruptsSent {
					inodeLease.leaseState = inodeLeaseStateSharedExpired

					inodeLease.lastInterruptTime = time.Time{}
					inodeLease.interruptsSent = 0

					inodeLease.interruptTimer = &time.Timer{}

					for nil != inodeLease.releasingHoldersList.Front() {
						leaseRequestElement = inodeLease.releasingHoldersList.Front()
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
						inodeLease.releasingHoldersList.Remove(leaseRequestElement)
						leaseRequest.listElement = nil
						leaseRequest.requestState = leaseRequestStateNone
					}

					inodeLease.leaseState = inodeLeaseStateNone
				} else { // globals.leaseInterruptLimit > inodeLease.interruptsSent {
					leaseRequestElement = inodeLease.releasingHoldersList.Front()

					for nil != leaseRequestElement {
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

						rpcInterrupt = &RPCInterrupt{
							RPCInterruptType: RPCInterruptTypeRelease,
							InodeNumber:      int64(inodeLease.InodeNumber),
						}

						rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
						if nil != err {
							logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 5]", rpcInterrupt, err)
						}

						evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

						globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

						leaseRequestElement = leaseRequestElement.Next()
					}

					inodeLease.lastInterruptTime = time.Now()
					inodeLease.interruptsSent++

					inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
				}
			case inodeLeaseStateExclusiveGrantedLongAgo:
				logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() hit an interruptTimer pop while unexpectedly in inodeLeaseStateExclusiveGrantedLongAgo")
			case inodeLeaseStateExclusiveDemoting:
				if globals.leaseInterruptLimit <= inodeLease.interruptsSent {
					inodeLease.leaseState = inodeLeaseStateExclusiveExpired

					inodeLease.lastInterruptTime = time.Time{}
					inodeLease.interruptsSent = 0

					inodeLease.interruptTimer = &time.Timer{}

					leaseRequest = inodeLease.demotingHolder
					inodeLease.demotingHolder = nil
					leaseRequest.requestState = leaseRequestStateNone

					inodeLease.leaseState = inodeLeaseStateNone
				} else { // globals.leaseInterruptLimit > inodeLease.interruptsSent {
					leaseRequest = inodeLease.demotingHolder

					rpcInterrupt = &RPCInterrupt{
						RPCInterruptType: RPCInterruptTypeRelease,
						InodeNumber:      int64(inodeLease.InodeNumber),
					}

					rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
					if nil != err {
						logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 6]", rpcInterrupt, err)
					}

					evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

					globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

					inodeLease.lastInterruptTime = time.Now()
					inodeLease.interruptsSent++

					inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
				}
			case inodeLeaseStateExclusiveReleasing:
				if globals.leaseInterruptLimit <= inodeLease.interruptsSent {
					inodeLease.leaseState = inodeLeaseStateExclusiveExpired

					inodeLease.lastInterruptTime = time.Time{}
					inodeLease.interruptsSent = 0

					inodeLease.interruptTimer = &time.Timer{}

					leaseRequestElement = inodeLease.releasingHoldersList.Front()
					leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
					inodeLease.releasingHoldersList.Remove(leaseRequestElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone

					inodeLease.leaseState = inodeLeaseStateNone
				} else { // globals.leaseInterruptLimit > inodeLease.interruptsSent {
					leaseRequestElement = inodeLease.releasingHoldersList.Front()
					leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

					rpcInterrupt = &RPCInterrupt{
						RPCInterruptType: RPCInterruptTypeRelease,
						InodeNumber:      int64(inodeLease.InodeNumber),
					}

					rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
					if nil != err {
						logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 7]", rpcInterrupt, err)
					}

					evtlog.Record(evtlog.FormatLeaseInterrupt, inodeLease.volume.volumeName, string(leaseRequest.mount.mountIDAsString), uint64(rpcInterrupt.InodeNumber), uint32(rpcInterrupt.RPCInterruptType))

					globals.retryrpcSvr.SendCallback(string(leaseRequest.mount.mountIDAsString), rpcInterruptBuf)

					inodeLease.lastInterruptTime = time.Now()
					inodeLease.interruptsSent++

					inodeLease.interruptTimer = time.NewTimer(globals.leaseInterruptInterval)
				}
			default:
				logger.Fatalf("(*inodeLeaseStruct).handleStopChanClose() hit an interruptTimer pop while unexpectedly in unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		}
	}

	// Drain requestChan before exiting

	for {
		select {
		case leaseRequestOperation = <-inodeLease.requestChan:
			leaseReply = &LeaseReply{
				LeaseReplyType: LeaseReplyTypeDenied,
			}
			leaseRequestOperation.replyChan <- leaseReply
		default:
			goto RequestChanDrained
		}
	}

RequestChanDrained:

	delete(inodeLease.volume.inodeLeaseMap, inodeLease.InodeNumber)
	_ = inodeLease.volume.inodeLeaseLRU.Remove(inodeLease.lruElement)

	if inodeLease.beingEvicted {
		inodeLease.volume.ongoingLeaseEvictions--
	}

	inodeLease.volume.leaseHandlerWG.Done()

	globals.volumesLock.Unlock()

	runtime.Goexit()
}

// armReleaseOfAllLeasesWhileLocked is called to schedule releasing of all held leases
// for a specific mountStruct. It is called while globals.volumesLock is locked. The
// leaseReleaseStartWG is assumed to be a sync.WaitGroup with a count of 1 such that
// the actual releasing of each lease will occur once leaseReleaseStartWG is signaled
// by calling Done() once. The caller would presumably do this after having released
// globals.volumesLock and then await completion by calling leaseReleaseFinishedWG.Wait().
//
func (mount *mountStruct) armReleaseOfAllLeasesWhileLocked(leaseReleaseStartWG *sync.WaitGroup, leaseReleaseFinishedWG *sync.WaitGroup) {
	var (
		leaseRequest          *leaseRequestStruct
		leaseRequestOperation *leaseRequestOperationStruct
	)

	for _, leaseRequest = range mount.leaseRequestMap {
		if nil != leaseRequest.inodeLease {
			leaseRequestOperation = &leaseRequestOperationStruct{
				mount:            mount,
				inodeLease:       leaseRequest.inodeLease,
				LeaseRequestType: LeaseRequestTypeRelease,
				replyChan:        make(chan *LeaseReply),
			}

			leaseReleaseFinishedWG.Add(1)

			go func(leaseRequestOperation *leaseRequestOperationStruct) {
				leaseReleaseStartWG.Wait()
				leaseRequestOperation.inodeLease.requestChan <- leaseRequestOperation
				_ = <-leaseRequestOperation.replyChan
				leaseReleaseFinishedWG.Done()
			}(leaseRequestOperation)
		}
	}
}

func fetchLeaseReport(volumeName string) (leaseReport []*LeaseReportElementStruct, err error) {
	var (
		annotatedMountID           string
		inodeLease                 *inodeLeaseStruct
		leaseReportElement         *LeaseReportElementStruct
		leaseReportLLRBTree        sortedmap.LLRBTree
		leaseReportLLRBTreeElement sortedmap.Value
		leaseReportLLRBTreeIndex   int
		leaseReportLLRBTreeLen     int
		leaseRequest               *leaseRequestStruct
		leaseRequestListElement    *list.Element
		ok                         bool
		volume                     *volumeStruct
	)

	enterGate()
	defer leaveGate()

	globals.volumesLock.Lock()
	defer globals.volumesLock.Unlock()

	volume, ok = globals.volumeMap[volumeName]
	if !ok {
		err = fmt.Errorf("fetchLeaseReport(volumeName: \"%s\") could not find Volume", volumeName)
		return
	}

	// leaseReport = make([]*LeaseReportElementStruct, 0)

	leaseReportLLRBTree = sortedmap.NewLLRBTree(sortedmap.CompareUint64, &globals)

	for _, inodeLease = range volume.inodeLeaseMap {
		leaseReportElement = &LeaseReportElementStruct{
			InodeNumber:          fmt.Sprintf("%016X", inodeLease.InodeNumber),
			SharedHoldersList:    make([]MountIDAsString, 0),
			PromotingHolder:      MountIDAsString(""),
			ExclusiveHolder:      MountIDAsString(""),
			DemotingHolder:       MountIDAsString(""),
			ReleasingHoldersList: make([]MountIDAsString, 0),
			RequestedList:        make([]string, 0),
		}

		for leaseRequestListElement = inodeLease.sharedHoldersList.Front(); leaseRequestListElement != nil; leaseRequestListElement = leaseRequestListElement.Next() {
			leaseRequest = leaseRequestListElement.Value.(*leaseRequestStruct)
			leaseReportElement.SharedHoldersList = append(leaseReportElement.SharedHoldersList, leaseRequest.mount.mountIDAsString)
		}

		if nil != inodeLease.promotingHolder {
			leaseReportElement.PromotingHolder = inodeLease.promotingHolder.mount.mountIDAsString
		}

		if nil != inodeLease.exclusiveHolder {
			leaseReportElement.ExclusiveHolder = inodeLease.exclusiveHolder.mount.mountIDAsString
		}

		if nil != inodeLease.demotingHolder {
			leaseReportElement.DemotingHolder = inodeLease.demotingHolder.mount.mountIDAsString
		}

		for leaseRequestListElement = inodeLease.releasingHoldersList.Front(); leaseRequestListElement != nil; leaseRequestListElement = leaseRequestListElement.Next() {
			leaseRequest = leaseRequestListElement.Value.(*leaseRequestStruct)
			leaseReportElement.ReleasingHoldersList = append(leaseReportElement.ReleasingHoldersList, leaseRequest.mount.mountIDAsString)
		}

		for leaseRequestListElement = inodeLease.requestedList.Front(); leaseRequestListElement != nil; leaseRequestListElement = leaseRequestListElement.Next() {
			leaseRequest = leaseRequestListElement.Value.(*leaseRequestStruct)
			switch leaseRequest.requestState {
			case leaseRequestStateSharedRequested:
				annotatedMountID = fmt.Sprintf("S(%s)", leaseRequest.mount.mountIDAsString)
			case leaseRequestStateExclusiveRequested:
				annotatedMountID = fmt.Sprintf("E(%s)", leaseRequest.mount.mountIDAsString)
			default:
				logger.Fatalf("leaseRequest.requestState (%v) unexpected", leaseRequest.requestState)
			}
			leaseReportElement.RequestedList = append(leaseReportElement.RequestedList, annotatedMountID)
		}

		ok, err = leaseReportLLRBTree.Put(uint64(inodeLease.InodeNumber), leaseReportElement)
		if nil != err {
			logger.Fatalf("leaseReportLLRBTree.Put(0x%016X,) failed: %v", uint64(inodeLease.InodeNumber), err)
		}
		if !ok {
			logger.Fatalf("leaseReportLLRBTree.Put(0x%016X,) returned !ok", uint64(inodeLease.InodeNumber))
		}
	}

	leaseReportLLRBTreeLen, err = leaseReportLLRBTree.Len()
	if nil != err {
		logger.Fatalf("leaseReportLLRBTree.Len() failed: %v", err)
	}

	leaseReport = make([]*LeaseReportElementStruct, 0, leaseReportLLRBTreeLen)

	for leaseReportLLRBTreeIndex = 0; leaseReportLLRBTreeIndex < leaseReportLLRBTreeLen; leaseReportLLRBTreeIndex++ {
		_, leaseReportLLRBTreeElement, ok, err = leaseReportLLRBTree.GetByIndex(leaseReportLLRBTreeIndex)
		if nil != err {
			logger.Fatalf("leaseReportLLRBTree.GetByIndex() failed: %v", err)
		}
		if !ok {
			logger.Fatalf("leaseReportLLRBTree.GetByIndex() returned !ok")
		}

		leaseReportElement = leaseReportLLRBTreeElement.(*LeaseReportElementStruct)

		leaseReport = append(leaseReport, leaseReportElement)
	}

	err = nil
	return
}

func (dummy *globalsStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("Not implemented")
	return
}

func (dummy *globalsStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("Not implemented")
	return
}
