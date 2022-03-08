// Copyright (c) 2015-2022, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"container/list"
	"encoding/json"
	"runtime"
	"sync"
	"time"
)

func (inodeLease *inodeLeaseStruct) handler() {
	var (
		leaseRequestOperation *leaseRequestOperationStruct
	)

	for {
		select {
		case leaseRequestOperation = <-inodeLease.requestChan:
			inodeLease.handleOperation(leaseRequestOperation)
		case <-inodeLease.longAgoTimer.C:
			inodeLease.handleLongAgoTimerPop()
		case <-inodeLease.interruptTimer.C:
			inodeLease.handleInterruptTimerPop()
		case <-inodeLease.stopChan:
			inodeLease.handleStopChanClose() // will not return
		}
	}
}

func (inodeLease *inodeLeaseStruct) handleOperation(leaseRequestOperation *leaseRequestOperationStruct) {
	var (
		err                      error
		leaseRequest             *leaseRequestStruct
		leaseRequestElement      *list.Element
		ok                       bool
		rpcInterrupt             *RPCInterrupt
		rpcInterruptBuf          []byte
		sharedHolderLeaseRequest *leaseRequestStruct
		sharedHolderListElement  *list.Element
	)

	globals.Lock()
	defer globals.Unlock()

	globals.inodeLeaseLRU.MoveToBack(inodeLease.lruElement)

	switch leaseRequestOperation.LeaseRequestType {
	case LeaseRequestTypeShared:
		_, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber]
		if ok {
			leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber] returned !ok
			leaseRequest = &leaseRequestStruct{
				mount:        leaseRequestOperation.mount,
				inodeLease:   inodeLease,
				requestState: leaseRequestStateSharedRequested,
				replyChan:    leaseRequestOperation.replyChan,
			}
			leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber] = leaseRequest
			switch inodeLease.leaseState {
			case inodeLeaseStateNone:
				leaseRequest.requestState = leaseRequestStateSharedGranted
				inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
				leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
				inodeLease.lastGrantTime = time.Now()
				inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				leaseRequest.replyChan <- LeaseResponseTypeShared
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
				inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				leaseRequest.replyChan <- LeaseResponseTypeShared
			case inodeLeaseStateSharedGrantedLongAgo:
				leaseRequest.requestState = leaseRequestStateSharedGranted
				inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
				leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
				inodeLease.lastGrantTime = time.Now()
				inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				leaseRequest.replyChan <- LeaseResponseTypeShared
			case inodeLeaseStateSharedPromoting:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedReleasing:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedExpired:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeShared, found unexpected inodeLease.leaseState inodeLeaseStateSharedExpired")
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
					InodeNumber:      inodeLease.inodeNumber,
				}
				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logFatalf("(*inodeLeaseStruct).handleOperation() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
				}
				globals.retryrpcServer.SendCallback(inodeLease.demotingHolder.mount.retryRPCClientID, rpcInterruptBuf)
				logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", inodeLease.demotingHolder.mount.retryRPCClientID, rpcInterrupt)
				inodeLease.lastInterruptTime = time.Now()
				inodeLease.interruptsSent = 1
				inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
			case inodeLeaseStateExclusiveDemoting:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveReleasing:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveExpired:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeShared, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveExpired")
			default:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeShared, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		}
	case LeaseRequestTypePromote:
		leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber]
		if ok {
			if leaseRequestStateSharedGranted == leaseRequest.requestState {
				switch inodeLease.leaseState {
				case inodeLeaseStateNone:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateNone")
				case inodeLeaseStateSharedGrantedRecently:
					if nil == inodeLease.promotingHolder {
						_ = inodeLease.sharedHoldersList.Remove(leaseRequest.listElement)
						leaseRequest.listElement = nil
						if inodeLease.sharedHoldersList.Len() == 0 {
							leaseRequest.requestState = leaseRequestStateExclusiveGranted
							leaseRequestOperation.replyChan <- LeaseResponseTypePromoted
							inodeLease.exclusiveHolder = leaseRequest
							inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
							inodeLease.lastGrantTime = time.Now()
							inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
						} else {
							inodeLease.promotingHolder = leaseRequest
							leaseRequest.replyChan = leaseRequestOperation.replyChan
						}
					} else { // nil != inodeLease.promotingHolder
						leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
					}
				case inodeLeaseStateSharedGrantedLongAgo:
					_ = inodeLease.sharedHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					if inodeLease.sharedHoldersList.Len() == 0 {
						inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
						inodeLease.exclusiveHolder = leaseRequest
						leaseRequest.requestState = leaseRequestStateExclusiveGranted
						leaseRequestOperation.replyChan <- LeaseResponseTypePromoted
						inodeLease.lastGrantTime = time.Now()
						inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
					} else {
						leaseRequest.replyChan = leaseRequestOperation.replyChan
						inodeLease.leaseState = inodeLeaseStateSharedPromoting
						inodeLease.promotingHolder = leaseRequest
						leaseRequest.requestState = leaseRequestStateSharedPromoting
						rpcInterrupt = &RPCInterrupt{
							RPCInterruptType: RPCInterruptTypeRelease,
							InodeNumber:      inodeLease.inodeNumber,
						}
						rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
						if nil != err {
							logFatalf("(*inodeLeaseStruct).handleOperation() unable to json.Marshal(rpcInterrupt: %#v): %v [case 2]", rpcInterrupt, err)
						}
						for nil != inodeLease.sharedHoldersList.Front() {
							leaseRequestElement = inodeLease.sharedHoldersList.Front()
							leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
							_ = inodeLease.sharedHoldersList.Remove(leaseRequestElement)
							leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)
							leaseRequest.requestState = leaseRequestStateSharedReleasing
							globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)
							logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)
						}
						inodeLease.lastInterruptTime = time.Now()
						inodeLease.interruptsSent = 1
						inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
					}
				case inodeLeaseStateSharedPromoting:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateSharedPromoting")
				case inodeLeaseStateSharedReleasing:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateSharedReleasing")
				case inodeLeaseStateSharedExpired:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateSharedExpired")
				case inodeLeaseStateExclusiveGrantedRecently:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveGrantedRecently")
				case inodeLeaseStateExclusiveGrantedLongAgo:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveGrantedLongAgo")
				case inodeLeaseStateExclusiveDemoting:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveDemoting")
				case inodeLeaseStateExclusiveReleasing:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveReleasing")
				case inodeLeaseStateExclusiveExpired:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveExpired")
				default:
					logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypePromote, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
				}
			} else { // leaseRequestStateSharedGranted != leaseRequest.requestState
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
			}
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber] returned !ok
			leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
		}
	case LeaseRequestTypeExclusive:
		_, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber]
		if ok {
			leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber] returned !ok
			leaseRequest = &leaseRequestStruct{
				mount:        leaseRequestOperation.mount,
				inodeLease:   inodeLease,
				requestState: leaseRequestStateExclusiveRequested,
				replyChan:    leaseRequestOperation.replyChan,
			}
			leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber] = leaseRequest
			switch inodeLease.leaseState {
			case inodeLeaseStateNone:
				leaseRequest.requestState = leaseRequestStateExclusiveGranted
				inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
				inodeLease.exclusiveHolder = leaseRequest
				inodeLease.lastGrantTime = time.Now()
				inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				leaseRequest.replyChan <- LeaseResponseTypeExclusive
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
						InodeNumber:      inodeLease.inodeNumber,
					}
					rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
					if nil != err {
						logFatalf("(*inodeLeaseStruct).handleOperation() unable to json.Marshal(rpcInterrupt: %#v): %v [case 3]", rpcInterrupt, err)
					}
					globals.retryrpcServer.SendCallback(sharedHolderLeaseRequest.mount.retryRPCClientID, rpcInterruptBuf)
					logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", sharedHolderLeaseRequest.mount.retryRPCClientID, rpcInterrupt)
				}
				inodeLease.lastInterruptTime = time.Now()
				inodeLease.interruptsSent = 1
				inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
			case inodeLeaseStateSharedPromoting:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedReleasing:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateSharedExpired:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeExclusive, found unexpected inodeLease.leaseState inodeLeaseStateSharedExpired")
			case inodeLeaseStateExclusiveGrantedRecently:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveGrantedLongAgo:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
				inodeLease.leaseState = inodeLeaseStateExclusiveReleasing
				inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveReleasing
				inodeLease.exclusiveHolder.listElement = inodeLease.releasingHoldersList.PushBack(inodeLease.exclusiveHolder)
				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      inodeLease.inodeNumber,
				}
				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logFatalf("(*inodeLeaseStruct).handleOperation() unable to json.Marshal(rpcInterrupt: %#v): %v [case 4]", rpcInterrupt, err)
				}
				globals.retryrpcServer.SendCallback(inodeLease.exclusiveHolder.mount.retryRPCClientID, rpcInterruptBuf)
				logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", inodeLease.exclusiveHolder.mount.retryRPCClientID, rpcInterrupt)
				inodeLease.exclusiveHolder = nil
				inodeLease.lastInterruptTime = time.Now()
				inodeLease.interruptsSent = 1
				inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
			case inodeLeaseStateExclusiveDemoting:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveReleasing:
				leaseRequest.listElement = inodeLease.requestedList.PushBack(leaseRequest)
			case inodeLeaseStateExclusiveExpired:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeExclusive, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveExpired")
			default:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeExclusive, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		}
	case LeaseRequestTypeDemote:
		leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber]
		if ok {
			switch inodeLease.leaseState {
			case inodeLeaseStateNone:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
			case inodeLeaseStateSharedGrantedRecently:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
			case inodeLeaseStateSharedGrantedLongAgo:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
			case inodeLeaseStateSharedPromoting:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
			case inodeLeaseStateSharedReleasing:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
			case inodeLeaseStateSharedExpired:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeDemote, found unexpected inodeLease.leaseState inodeLeaseStateSharedExpired")
			case inodeLeaseStateExclusiveGrantedRecently:
				if leaseRequestStateExclusiveGranted == leaseRequest.requestState {
					if !inodeLease.longAgoTimer.Stop() {
						<-inodeLease.longAgoTimer.C
					}
					inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
					leaseRequest.requestState = leaseRequestStateSharedGranted
					inodeLease.exclusiveHolder = nil
					leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
					leaseRequestOperation.replyChan <- LeaseResponseTypeDemoted
					leaseRequestElement = inodeLease.requestedList.Front()
					for nil != leaseRequestElement {
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
						if leaseRequestStateSharedRequested == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateSharedGranted
							_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
							leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
							leaseRequest.replyChan <- LeaseResponseTypeShared
							leaseRequestElement = inodeLease.requestedList.Front()
						} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
							leaseRequestElement = nil
						}
					}
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				} else { // leaseRequestStateExclusiveGranted == leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}
			case inodeLeaseStateExclusiveGrantedLongAgo:
				if leaseRequestStateExclusiveGranted == leaseRequest.requestState {
					inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
					leaseRequest.requestState = leaseRequestStateSharedGranted
					inodeLease.exclusiveHolder = nil
					leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
					leaseRequestOperation.replyChan <- LeaseResponseTypeDemoted
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				} else { // leaseRequestStateExclusiveGranted != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
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
					leaseRequestOperation.replyChan <- LeaseResponseTypeDemoted
					leaseRequestElement = inodeLease.requestedList.Front()
					for nil != leaseRequestElement {
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
						if leaseRequestStateSharedRequested == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateSharedGranted
							_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
							leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
							leaseRequest.replyChan <- LeaseResponseTypeShared
							leaseRequestElement = inodeLease.requestedList.Front()
						} else { // leaseRequestStateSharedRequested != leaseRequest.requestState
							leaseRequestElement = nil
						}
					}
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				} else { // leaseRequestStateExclusiveDemoting == leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}
			case inodeLeaseStateExclusiveReleasing:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
			case inodeLeaseStateExclusiveExpired:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeDemote, found unexpected inodeLease.leaseState inodeLeaseStateExclusiveExpired")
			default:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeDemote, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber] returned !ok
			leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
		}
	case LeaseRequestTypeRelease:
		leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber]
		if ok {
			switch inodeLease.leaseState {
			case inodeLeaseStateNone:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
			case inodeLeaseStateSharedGrantedRecently:
				if leaseRequestStateSharedGranted == leaseRequest.requestState {
					_ = inodeLease.sharedHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
					leaseRequestOperation.replyChan <- LeaseResponseTypeReleased
					if inodeLease.sharedHoldersList.Len() == 0 {
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
										leaseRequest.replyChan <- LeaseResponseTypeExclusive
									} else { // 0 < inodeLease.requestedList.Len()
										inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
										leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
										leaseRequest.requestState = leaseRequestStateSharedGranted
										leaseRequest.replyChan <- LeaseResponseTypeShared
										leaseRequestElement = inodeLease.requestedList.Front()
										for nil != leaseRequestElement {
											leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
											_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
											if leaseRequestStateSharedRequested == leaseRequest.requestState {
												leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
												leaseRequest.requestState = leaseRequestStateSharedGranted
												leaseRequest.replyChan <- LeaseResponseTypeShared
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
									leaseRequest.replyChan <- LeaseResponseTypeExclusive
								}
								inodeLease.lastGrantTime = time.Now()
								inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
							}
						} else { // nil != inodeLease.promotingHolder
							inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
							inodeLease.exclusiveHolder = inodeLease.promotingHolder
							inodeLease.promotingHolder = nil
							inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted
							inodeLease.exclusiveHolder.replyChan <- LeaseResponseTypePromoted
							inodeLease.lastGrantTime = time.Now()
							inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
						}
					}
				} else { // leaseRequestStateSharedGranted != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}
			case inodeLeaseStateSharedGrantedLongAgo:
				if leaseRequestStateSharedGranted == leaseRequest.requestState {
					_ = inodeLease.sharedHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
					leaseRequestOperation.replyChan <- LeaseResponseTypeReleased
					if inodeLease.sharedHoldersList.Len() == 0 {
						inodeLease.leaseState = inodeLeaseStateNone
					}
				} else { // leaseRequestStateSharedGranted != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}
			case inodeLeaseStateSharedPromoting:
				if leaseRequestStateSharedReleasing == leaseRequest.requestState {
					_ = inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
					leaseRequestOperation.replyChan <- LeaseResponseTypeReleased
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
						inodeLease.exclusiveHolder.replyChan <- LeaseResponseTypePromoted
						inodeLease.lastGrantTime = time.Now()
						inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
					}
				} else { // leaseRequestStateSharedReleasing != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}
			case inodeLeaseStateSharedReleasing:
				if leaseRequestStateSharedReleasing == leaseRequest.requestState {
					_ = inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
					leaseRequestOperation.replyChan <- LeaseResponseTypeReleased
					if inodeLease.releasingHoldersList.Len() == 0 {
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
						leaseRequest.replyChan <- LeaseResponseTypeExclusive
						inodeLease.lastGrantTime = time.Now()
						inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
					}
				} else { // leaseRequestStateSharedReleasing != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}
			case inodeLeaseStateSharedExpired:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeRelease, found unknown inodeLease.leaseState inodeLeaseStateSharedExpired")
			case inodeLeaseStateExclusiveGrantedRecently:
				if leaseRequestStateExclusiveGranted == leaseRequest.requestState {
					if !inodeLease.longAgoTimer.Stop() {
						<-inodeLease.longAgoTimer.C
					}
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
					leaseRequestOperation.replyChan <- LeaseResponseTypeReleased
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
							leaseRequest.replyChan <- LeaseResponseTypeShared
							leaseRequestElement = inodeLease.requestedList.Front()
							for nil != leaseRequestElement {
								leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
								if leaseRequestStateSharedRequested == leaseRequest.requestState {
									_ = inodeLease.requestedList.Remove(leaseRequestElement)
									leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
									leaseRequest.requestState = leaseRequestStateSharedGranted
									leaseRequest.replyChan <- LeaseResponseTypeShared
									leaseRequestElement = inodeLease.requestedList.Front()
								} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
									leaseRequestElement = nil
								}
							}
						} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
							inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
							leaseRequest.requestState = leaseRequestStateExclusiveGranted
							inodeLease.exclusiveHolder = leaseRequest
							leaseRequest.replyChan <- LeaseResponseTypeExclusive
						}
						inodeLease.lastGrantTime = time.Now()
						inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
					}
				} else { // leaseRequestStateExclusiveGranted != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}
			case inodeLeaseStateExclusiveGrantedLongAgo:
				if leaseRequestStateExclusiveGranted == leaseRequest.requestState {
					inodeLease.leaseState = inodeLeaseStateNone
					leaseRequest.requestState = leaseRequestStateNone
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
					inodeLease.exclusiveHolder = nil
					leaseRequestOperation.replyChan <- LeaseResponseTypeReleased
				} else { // leaseRequestStateExclusiveGranted != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
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
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
					inodeLease.demotingHolder = nil
					leaseRequestOperation.replyChan <- LeaseResponseTypeReleased
					leaseRequestElement = inodeLease.requestedList.Front()
					leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
					_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
					if (nil == inodeLease.requestedList.Front()) || (leaseRequestStateExclusiveRequested == inodeLease.requestedList.Front().Value.(*leaseRequestStruct).requestState) {
						inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
						leaseRequest.requestState = leaseRequestStateExclusiveGranted
						leaseRequest.listElement = nil
						inodeLease.exclusiveHolder = leaseRequest
						leaseRequest.replyChan <- LeaseResponseTypeExclusive
					} else {
						inodeLease.leaseState = inodeLeaseStateSharedGrantedRecently
						leaseRequest.requestState = leaseRequestStateSharedGranted
						leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
						leaseRequest.replyChan <- LeaseResponseTypeShared
						leaseRequestElement = inodeLease.requestedList.Front()
						for nil != leaseRequestElement {
							leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
							if leaseRequestStateSharedRequested == leaseRequest.requestState {
								_ = inodeLease.requestedList.Remove(leaseRequest.listElement)
								leaseRequest.requestState = leaseRequestStateSharedGranted
								leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)
								leaseRequest.replyChan <- LeaseResponseTypeShared
								leaseRequestElement = inodeLease.requestedList.Front()
							} else { // leaseRequestStateExclusiveRequested == leaseRequest.requestState
								leaseRequestElement = nil
							}
						}
					}
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				} else { // leaseRequestStateExclusiveDemoting != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
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
					delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
					_ = inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
					leaseRequest.listElement = nil
					leaseRequestOperation.replyChan <- LeaseResponseTypeReleased
					leaseRequestElement = inodeLease.requestedList.Front()
					leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
					leaseRequest.requestState = leaseRequestStateExclusiveGranted
					_ = inodeLease.requestedList.Remove(leaseRequestElement)
					leaseRequest.listElement = nil
					inodeLease.exclusiveHolder = leaseRequest
					leaseRequest.replyChan <- LeaseResponseTypeExclusive
					inodeLease.lastGrantTime = time.Now()
					inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)
				} else { // leaseRequestStateExclusiveReleasing != leaseRequest.requestState
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}
			case inodeLeaseStateExclusiveExpired:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeRelease, found unknown inodeLease.leaseState inodeLeaseStateExclusiveExpired")
			default:
				logFatalf("(*inodeLeaseStruct).handleOperation(), while in leaseRequestOperation.LeaseRequestType LeaseRequestTypeRelease, found unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		} else { // leaseRequestOperation.mount.leaseRequestMap[inodeLease.inodeNumber] returned !ok
			leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
		}
	default:
		logFatalf("(*inodeLeaseStruct).handleOperation() found unexpected leaseRequestOperation.LeaseRequestType: %v", leaseRequestOperation.LeaseRequestType)
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

	globals.Lock()
	defer globals.Unlock()

	inodeLease.lastGrantTime = time.Time{}
	inodeLease.longAgoTimer = &time.Timer{}

	switch inodeLease.leaseState {
	case inodeLeaseStateSharedGrantedRecently:
		inodeLease.leaseState = inodeLeaseStateSharedGrantedLongAgo

		if (nil != inodeLease.promotingHolder) || (inodeLease.requestedList.Len() != 0) {
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
					InodeNumber:      inodeLease.inodeNumber,
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logFatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
				}

				globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

				logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

				leaseRequestElement = inodeLease.sharedHoldersList.Front()
			}

			inodeLease.lastInterruptTime = time.Now()
			inodeLease.interruptsSent = 1

			inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
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
					InodeNumber:      inodeLease.inodeNumber,
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logFatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 2]", rpcInterrupt, err)
				}

				globals.retryrpcServer.SendCallback(inodeLease.demotingHolder.mount.retryRPCClientID, rpcInterruptBuf)

				logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", inodeLease.demotingHolder.mount.retryRPCClientID, rpcInterrupt)
			case leaseRequestStateExclusiveRequested:
				inodeLease.leaseState = inodeLeaseStateExclusiveReleasing

				inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveReleasing
				inodeLease.exclusiveHolder.listElement = inodeLease.releasingHoldersList.PushBack(inodeLease.exclusiveHolder)

				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      inodeLease.inodeNumber,
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logFatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 3]", rpcInterrupt, err)
				}

				globals.retryrpcServer.SendCallback(inodeLease.exclusiveHolder.mount.retryRPCClientID, rpcInterruptBuf)

				logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", inodeLease.exclusiveHolder.mount.retryRPCClientID, rpcInterrupt)

				inodeLease.exclusiveHolder = nil
			default:
				logFatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() found requestedList with unexpected leaseRequest.requestState: %v", leaseRequest.requestState)
			}

			inodeLease.lastInterruptTime = time.Now()
			inodeLease.interruptsSent = 1

			inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
		}
	default:
		logFatalf("(*inodeLeaseStruct).handleLongAgoTimerPop() called while in wrong state (%v)", inodeLease.leaseState)
	}
}

func (inodeLease *inodeLeaseStruct) handleInterruptTimerPop() {
	var (
		err                 error
		leaseRequest        *leaseRequestStruct
		leaseRequestElement *list.Element
		rpcInterrupt        *RPCInterrupt
		rpcInterruptBuf     []byte
	)

	globals.Lock()
	defer globals.Unlock()

	if globals.config.LeaseInterruptLimit <= inodeLease.interruptsSent {
		switch inodeLease.leaseState {
		case inodeLeaseStateSharedPromoting:
			inodeLease.leaseState = inodeLeaseStateSharedExpired

			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 1]")
			}

			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

				delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)

				leaseRequest.mount.acceptingLeaseRequests = false

				inodeLease.releasingHoldersList.Remove(leaseRequestElement)
				leaseRequest.listElement = nil

				leaseRequestElement = inodeLease.releasingHoldersList.Front()
			}

			inodeLease.exclusiveHolder = inodeLease.promotingHolder
			inodeLease.promotingHolder = nil

			inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted

			inodeLease.exclusiveHolder.replyChan <- LeaseResponseTypeExclusive

			inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
		case inodeLeaseStateSharedReleasing:
			inodeLease.leaseState = inodeLeaseStateSharedExpired

			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 2]")
			}

			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

				delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)

				leaseRequest.mount.acceptingLeaseRequests = false

				inodeLease.releasingHoldersList.Remove(leaseRequestElement)
				leaseRequest.listElement = nil

				leaseRequestElement = inodeLease.releasingHoldersList.Front()
			}

			leaseRequestElement = inodeLease.requestedList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty requestedList [case 1]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
			if leaseRequestStateExclusiveRequested != leaseRequest.requestState {
				logFatalf("(inodeLeaseStruct).handleInterruptTimerPop() found unexpected requestedList.Front().requestState: %v [case 1]", leaseRequest.requestState)
			}

			inodeLease.requestedList.Remove(leaseRequest.listElement)
			leaseRequest.listElement = nil
			inodeLease.exclusiveHolder = leaseRequest

			inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted

			inodeLease.exclusiveHolder.replyChan <- LeaseResponseTypeExclusive

			inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
		case inodeLeaseStateExclusiveDemoting:
			inodeLease.leaseState = inodeLeaseStateExclusiveExpired

			if nil == inodeLease.demotingHolder {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty demotingHolder [case 1]")
			}

			delete(inodeLease.demotingHolder.mount.leaseRequestMap, inodeLease.inodeNumber)

			inodeLease.demotingHolder.mount.acceptingLeaseRequests = false

			inodeLease.demotingHolder = nil

			leaseRequestElement = inodeLease.requestedList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty requestedList [case 3]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
			if leaseRequestStateSharedRequested != leaseRequest.requestState {
				logFatalf("(inodeLeaseStruct).handleInterruptTimerPop() found unexpected requestedList.Front().requestState: %v [case 3]", leaseRequest.requestState)
			}

			for {
				inodeLease.requestedList.Remove(leaseRequestElement)
				leaseRequest.listElement = inodeLease.sharedHoldersList.PushBack(leaseRequest)

				leaseRequest.requestState = leaseRequestStateSharedGranted

				leaseRequest.replyChan <- LeaseResponseTypeShared

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
		case inodeLeaseStateExclusiveReleasing:
			inodeLease.leaseState = inodeLeaseStateExclusiveExpired

			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 3]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

			delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)

			leaseRequest.mount.acceptingLeaseRequests = false

			inodeLease.releasingHoldersList.Remove(leaseRequestElement)
			leaseRequest.listElement = nil

			if nil != inodeLease.releasingHoldersList.Front() {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found releasingHoldersList unexpectedly with >1 leaseRequestElements")
			}

			leaseRequestElement = inodeLease.requestedList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty requestedList [case 2]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
			if leaseRequestStateExclusiveRequested != leaseRequest.requestState {
				logFatalf("(inodeLeaseStruct).handleInterruptTimerPop() found unexpected requestedList.Front().requestState: %v [case 2]", leaseRequest.requestState)
			}

			inodeLease.requestedList.Remove(leaseRequest.listElement)
			leaseRequest.listElement = nil
			inodeLease.exclusiveHolder = leaseRequest

			inodeLease.exclusiveHolder.requestState = leaseRequestStateExclusiveGranted

			inodeLease.exclusiveHolder.replyChan <- LeaseResponseTypeExclusive

			inodeLease.leaseState = inodeLeaseStateExclusiveGrantedRecently
		default:
			logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found unexpected leaseState: %v [case 1]", inodeLease.leaseState)
		}

		inodeLease.lastGrantTime = time.Now()
		inodeLease.longAgoTimer = time.NewTimer(globals.config.MinLeaseDuration)

		inodeLease.lastInterruptTime = time.Time{}
		inodeLease.interruptsSent = 0

		inodeLease.interruptTimer = &time.Timer{}
	} else { // globals.config.LeaseInterruptLimit > inodeLease.interruptsSent
		switch inodeLease.leaseState {
		case inodeLeaseStateSharedPromoting:
			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 4]")
			}
			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      inodeLease.inodeNumber,
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
				}

				globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

				logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

				leaseRequestElement = leaseRequestElement.Next()
			}
		case inodeLeaseStateSharedReleasing:
			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 5]")
			}
			for nil != leaseRequestElement {
				leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

				rpcInterrupt = &RPCInterrupt{
					RPCInterruptType: RPCInterruptTypeRelease,
					InodeNumber:      inodeLease.inodeNumber,
				}

				rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
				if nil != err {
					logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
				}

				globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

				logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

				leaseRequestElement = leaseRequestElement.Next()
			}
		case inodeLeaseStateExclusiveReleasing:
			leaseRequestElement = inodeLease.releasingHoldersList.Front()
			if nil == leaseRequestElement {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty releasingHoldersList [case 6]")
			}

			leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

			rpcInterrupt = &RPCInterrupt{
				RPCInterruptType: RPCInterruptTypeRelease,
				InodeNumber:      inodeLease.inodeNumber,
			}

			rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
			if nil != err {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 2]", rpcInterrupt, err)
			}

			globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

			logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)
		case inodeLeaseStateExclusiveDemoting:
			if nil == inodeLease.demotingHolder {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found empty demotingHolder [case 2]")
			}

			rpcInterrupt = &RPCInterrupt{
				RPCInterruptType: RPCInterruptTypeDemote,
				InodeNumber:      inodeLease.inodeNumber,
			}

			rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
			if nil != err {
				logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() unable to json.Marshal(rpcInterrupt: %#v): %v [case 3]", rpcInterrupt, err)
			}

			globals.retryrpcServer.SendCallback(inodeLease.demotingHolder.mount.retryRPCClientID, rpcInterruptBuf)

			logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", inodeLease.demotingHolder.mount.retryRPCClientID, rpcInterrupt)
		default:
			logFatalf("(*inodeLeaseStruct).handleInterruptTimerPop() found unexpected leaseState: %v [case 2]", inodeLease.leaseState)
		}

		inodeLease.lastInterruptTime = time.Now()
		inodeLease.interruptsSent++

		inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
	}
}

func (inodeLease *inodeLeaseStruct) handleStopChanClose() {
	var (
		err                   error
		leaseRequest          *leaseRequestStruct
		leaseRequestElement   *list.Element
		leaseRequestOperation *leaseRequestOperationStruct
		ok                    bool
		rpcInterrupt          *RPCInterrupt
		rpcInterruptBuf       []byte
	)

	// Deny all pending requests:

	globals.Lock()

	for nil != inodeLease.requestedList.Front() {
		leaseRequestElement = inodeLease.requestedList.Front()
		leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)
		inodeLease.requestedList.Remove(leaseRequest.listElement)
		leaseRequest.listElement = nil
		leaseRequest.requestState = leaseRequestStateNone
		leaseRequest.replyChan <- LeaseResponseTypeDenied
	}

	// If inodeLease.leaseState is inodeLeaseStateSharedPromoting:
	//   Reject inodeLease.promotingHolder's LeaseRequestTypePromote
	//   Ensure formerly inodeLease.promotingHolder is also now releasing

	if inodeLeaseStateSharedPromoting == inodeLease.leaseState {
		leaseRequest = inodeLease.promotingHolder
		inodeLease.promotingHolder = nil

		leaseRequest.replyChan <- LeaseResponseTypeDenied

		leaseRequest.requestState = leaseRequestStateSharedReleasing

		leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)

		rpcInterrupt = &RPCInterrupt{
			RPCInterruptType: RPCInterruptTypeRelease,
			InodeNumber:      inodeLease.inodeNumber,
		}

		rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
		if nil != err {
			logFatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 1]", rpcInterrupt, err)
		}

		globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

		logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

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
				InodeNumber:      inodeLease.inodeNumber,
			}

			rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
			if nil != err {
				logFatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 2]", rpcInterrupt, err)
			}

			globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

			logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)
		}

		inodeLease.leaseState = inodeLeaseStateSharedReleasing

		inodeLease.lastInterruptTime = time.Now()
		inodeLease.interruptsSent = 1

		inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
	case inodeLeaseStateExclusiveGrantedLongAgo:
		leaseRequest = inodeLease.exclusiveHolder
		inodeLease.exclusiveHolder = nil

		leaseRequest.requestState = leaseRequestStateExclusiveReleasing

		leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)

		rpcInterrupt = &RPCInterrupt{
			RPCInterruptType: RPCInterruptTypeRelease,
			InodeNumber:      inodeLease.inodeNumber,
		}

		rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
		if nil != err {
			logFatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 3]", rpcInterrupt, err)
		}

		globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

		logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

		inodeLease.leaseState = inodeLeaseStateExclusiveReleasing

		inodeLease.lastInterruptTime = time.Now()
		inodeLease.interruptsSent = 1

		inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
	default:
		// Nothing to do here
	}

	// Loop until inodeLease.leaseState is inodeLeaseStateNone

	for inodeLeaseStateNone != inodeLease.leaseState {
		globals.Unlock()

		select {
		case leaseRequestOperation = <-inodeLease.requestChan:
			globals.Lock()

			switch leaseRequestOperation.LeaseRequestType {
			case LeaseRequestTypeShared:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied

			case LeaseRequestTypePromote:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied

			case LeaseRequestTypeExclusive:
				leaseRequestOperation.replyChan <- LeaseResponseTypeDenied

			case LeaseRequestTypeDemote:
				if inodeLeaseStateExclusiveDemoting == inodeLease.leaseState {
					leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[leaseRequestOperation.inodeLease.inodeNumber]
					if ok {
						if leaseRequestStateExclusiveDemoting == leaseRequest.requestState {
							if leaseRequest == inodeLease.demotingHolder {
								leaseRequestOperation.replyChan <- LeaseResponseTypeDemoted

								inodeLease.demotingHolder = nil

								leaseRequest.requestState = leaseRequestStateExclusiveReleasing

								leaseRequest.listElement = inodeLease.releasingHoldersList.PushBack(leaseRequest)

								rpcInterrupt = &RPCInterrupt{
									RPCInterruptType: RPCInterruptTypeRelease,
									InodeNumber:      inodeLease.inodeNumber,
								}

								rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
								if nil != err {
									logFatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 4]", rpcInterrupt, err)
								}

								globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

								logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

								inodeLease.leaseState = inodeLeaseStateExclusiveReleasing

								if !inodeLease.interruptTimer.Stop() {
									<-inodeLease.interruptTimer.C
								}

								inodeLease.lastInterruptTime = time.Now()
								inodeLease.interruptsSent = 1

								inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
							} else {
								leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
							}
						} else {
							leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
						}
					} else {
						leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
					}
				} else {
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}

			case LeaseRequestTypeRelease:
				leaseRequest, ok = leaseRequestOperation.mount.leaseRequestMap[leaseRequestOperation.inodeLease.inodeNumber]
				if ok {
					switch inodeLease.leaseState {
					case inodeLeaseStateSharedReleasing:
						if leaseRequestStateSharedReleasing == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateNone
							delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
							inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
							leaseRequest.listElement = nil

							leaseRequestOperation.replyChan <- LeaseResponseTypeReleased

							if inodeLease.releasingHoldersList.Len() == 0 {
								inodeLease.leaseState = inodeLeaseStateNone

								if !inodeLease.interruptTimer.Stop() {
									<-inodeLease.interruptTimer.C
								}

								inodeLease.lastInterruptTime = time.Time{}
								inodeLease.interruptsSent = 0

								inodeLease.interruptTimer = &time.Timer{}
							}
						} else {
							leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
						}
					case inodeLeaseStateExclusiveDemoting:
						if leaseRequestStateExclusiveDemoting == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateNone
							delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
							inodeLease.demotingHolder = nil

							leaseRequestOperation.replyChan <- LeaseResponseTypeReleased

							inodeLease.leaseState = inodeLeaseStateNone

							if !inodeLease.interruptTimer.Stop() {
								<-inodeLease.interruptTimer.C
							}

							inodeLease.lastInterruptTime = time.Time{}
							inodeLease.interruptsSent = 0

							inodeLease.interruptTimer = &time.Timer{}
						} else {
							leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
						}
					case inodeLeaseStateExclusiveReleasing:
						if leaseRequestStateExclusiveReleasing == leaseRequest.requestState {
							leaseRequest.requestState = leaseRequestStateNone
							delete(leaseRequest.mount.leaseRequestMap, inodeLease.inodeNumber)
							inodeLease.releasingHoldersList.Remove(leaseRequest.listElement)
							leaseRequest.listElement = nil

							leaseRequestOperation.replyChan <- LeaseResponseTypeReleased

							inodeLease.leaseState = inodeLeaseStateNone

							if !inodeLease.interruptTimer.Stop() {
								<-inodeLease.interruptTimer.C
							}

							inodeLease.lastInterruptTime = time.Time{}
							inodeLease.interruptsSent = 0

							inodeLease.interruptTimer = &time.Timer{}
						} else {
							leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
						}
					default:
						leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
					}
				} else {
					leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
				}

			default:
				logFatalf("(*inodeLeaseStruct).handleStopChanClose() read unexected leaseRequestOperationLeaseRequestType: %v", leaseRequestOperation.LeaseRequestType)
			}

		case <-inodeLease.interruptTimer.C:
			globals.Lock()

			switch inodeLease.leaseState {
			case inodeLeaseStateSharedGrantedLongAgo:
				logFatalf("(*inodeLeaseStruct).handleStopChanClose() hit an interruptTimer pop while unexpectedly in inodeLeaseStateSharedGrantedLongAgo")
			case inodeLeaseStateSharedReleasing:
				if globals.config.LeaseInterruptLimit <= inodeLease.interruptsSent {
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
				} else { // globals.config.LeaseInterruptLimit > inodeLease.interruptsSent {
					leaseRequestElement = inodeLease.releasingHoldersList.Front()

					for nil != leaseRequestElement {
						leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

						rpcInterrupt = &RPCInterrupt{
							RPCInterruptType: RPCInterruptTypeRelease,
							InodeNumber:      inodeLease.inodeNumber,
						}

						rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
						if nil != err {
							logFatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 5]", rpcInterrupt, err)
						}

						globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

						logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

						leaseRequestElement = leaseRequestElement.Next()
					}

					inodeLease.lastInterruptTime = time.Now()
					inodeLease.interruptsSent++

					inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
				}
			case inodeLeaseStateExclusiveGrantedLongAgo:
				logFatalf("(*inodeLeaseStruct).handleStopChanClose() hit an interruptTimer pop while unexpectedly in inodeLeaseStateExclusiveGrantedLongAgo")
			case inodeLeaseStateExclusiveDemoting:
				if globals.config.LeaseInterruptLimit <= inodeLease.interruptsSent {
					inodeLease.leaseState = inodeLeaseStateExclusiveExpired

					inodeLease.lastInterruptTime = time.Time{}
					inodeLease.interruptsSent = 0

					inodeLease.interruptTimer = &time.Timer{}

					leaseRequest = inodeLease.demotingHolder
					inodeLease.demotingHolder = nil
					leaseRequest.requestState = leaseRequestStateNone

					inodeLease.leaseState = inodeLeaseStateNone
				} else { // globals.config.LeaseInterruptLimit > inodeLease.interruptsSent {
					leaseRequest = inodeLease.demotingHolder

					rpcInterrupt = &RPCInterrupt{
						RPCInterruptType: RPCInterruptTypeRelease,
						InodeNumber:      inodeLease.inodeNumber,
					}

					rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
					if nil != err {
						logFatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 6]", rpcInterrupt, err)
					}

					globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

					logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

					inodeLease.lastInterruptTime = time.Now()
					inodeLease.interruptsSent++

					inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
				}
			case inodeLeaseStateExclusiveReleasing:
				if globals.config.LeaseInterruptLimit <= inodeLease.interruptsSent {
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
				} else { // globals.config.LeaseInterruptLimit > inodeLease.interruptsSent {
					leaseRequestElement = inodeLease.releasingHoldersList.Front()
					leaseRequest = leaseRequestElement.Value.(*leaseRequestStruct)

					rpcInterrupt = &RPCInterrupt{
						RPCInterruptType: RPCInterruptTypeRelease,
						InodeNumber:      inodeLease.inodeNumber,
					}

					rpcInterruptBuf, err = json.Marshal(rpcInterrupt)
					if nil != err {
						logFatalf("(*inodeLeaseStruct).handleStopChanClose() unable to json.Marshal(rpcInterrupt: %#v): %v [case 7]", rpcInterrupt, err)
					}

					globals.retryrpcServer.SendCallback(leaseRequest.mount.retryRPCClientID, rpcInterruptBuf)

					logTracef("<== [RPC] SendCallback(clientID: 0x%016X, rpcInterrupt: %+v)", leaseRequest.mount.retryRPCClientID, rpcInterrupt)

					inodeLease.lastInterruptTime = time.Now()
					inodeLease.interruptsSent++

					inodeLease.interruptTimer = time.NewTimer(globals.config.LeaseInterruptInterval)
				}
			default:
				logFatalf("(*inodeLeaseStruct).handleStopChanClose() hit an interruptTimer pop while unexpectedly in unknown inodeLease.leaseState: %v", inodeLease.leaseState)
			}
		}
	}

	// Drain requestChan before exiting

	for {
		select {
		case leaseRequestOperation = <-inodeLease.requestChan:
			leaseRequestOperation.replyChan <- LeaseResponseTypeDenied
		default:
			goto RequestChanDrained
		}
	}

RequestChanDrained:

	delete(inodeLease.volume.inodeLeaseMap, inodeLease.inodeNumber)
	_ = globals.inodeLeaseLRU.Remove(inodeLease.lruElement)

	inodeLease.volume.leaseHandlerWG.Done()

	globals.Unlock()

	runtime.Goexit()
}

func (leaseRequest *leaseRequestStruct) okToRead() (ok bool) {
	switch leaseRequest.requestState {
	case leaseRequestStateNone:
		ok = false
	case leaseRequestStateSharedRequested:
		ok = false
	case leaseRequestStateSharedGranted:
		ok = true
	case leaseRequestStateSharedPromoting:
		ok = true
	case leaseRequestStateSharedReleasing:
		ok = true
	case leaseRequestStateExclusiveRequested:
		ok = false
	case leaseRequestStateExclusiveGranted:
		ok = true
	case leaseRequestStateExclusiveDemoting:
		ok = true
	case leaseRequestStateExclusiveReleasing:
		ok = true
	default:
		logFatalf("(*leaseRequestStruct).okToRead() called while for unknown leaseRequest.requestState: %v", leaseRequest.requestState)
	}

	return
}

func (leaseRequest *leaseRequestStruct) okToWrite() (ok bool) {
	switch leaseRequest.requestState {
	case leaseRequestStateNone:
		ok = false
	case leaseRequestStateSharedRequested:
		ok = false
	case leaseRequestStateSharedGranted:
		ok = false
	case leaseRequestStateSharedPromoting:
		ok = false
	case leaseRequestStateSharedReleasing:
		ok = false
	case leaseRequestStateExclusiveRequested:
		ok = false
	case leaseRequestStateExclusiveGranted:
		ok = true
	case leaseRequestStateExclusiveDemoting:
		ok = true
	case leaseRequestStateExclusiveReleasing:
		ok = true
	default:
		logFatalf("(*leaseRequestStruct).okToWrite() called while for unknown leaseRequest.requestState: %v", leaseRequest.requestState)
	}

	return
}

// armReleaseOfAllLeasesWhileLocked is called to schedule releasing of all held leases
// for a specific mountStruct. It is called while inodeLease.volume is locked. The
// leaseReleaseStartWG is assumed to be a sync.WaitGroup with a count of 1 such that
// the actual releasing of each lease will occur once leaseReleaseStartWG is signaled
// by calling Done() once. The caller would presumably do this after having released
// inodeLease.volume and then await completion by calling leaseReleaseFinishedWG.Wait().
//
func (mount *mountStruct) armReleaseOfAllLeasesWhileLocked(leaseReleaseStartWG *sync.WaitGroup, leaseReleaseFinishedWG *sync.WaitGroup) {
	var (
		leaseRequest          *leaseRequestStruct
		leaseRequestOperation *leaseRequestOperationStruct
	)

	for _, leaseRequest = range mount.leaseRequestMap {
		leaseRequestOperation = &leaseRequestOperationStruct{
			mount:            mount,
			inodeLease:       leaseRequest.inodeLease,
			LeaseRequestType: LeaseRequestTypeRelease,
			replyChan:        make(chan LeaseResponseType),
		}

		leaseReleaseFinishedWG.Add(1)

		go func(leaseRequestOperation *leaseRequestOperationStruct) {
			leaseReleaseStartWG.Wait()
			leaseRequestOperation.inodeLease.requestChan <- leaseRequestOperation
			<-leaseRequestOperation.replyChan
			leaseReleaseFinishedWG.Done()
		}(leaseRequestOperation)
	}
}
