// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"crypto/rand"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/NVIDIA/proxyfs/inode"
	"github.com/NVIDIA/proxyfs/logger"
)

func stateMachine() {
	for {
		globals.nextState()
	}
}

func doCandidate() {
	var (
		awaitingResponses                               map[*peerStruct]struct{}
		durationDelta                                   time.Duration
		err                                             error
		livenessReportWhileCandidate                    *internalLivenessReportStruct
		msgAsFetchLivenessReportRequest                 *FetchLivenessReportRequestStruct
		msgAsFetchLivenessReportResponse                *FetchLivenessReportResponseStruct
		msgAsHeartBeatRequest                           *HeartBeatRequestStruct
		msgAsHeartBeatResponse                          *HeartBeatResponseStruct
		msgAsRequestVoteRequest                         *RequestVoteRequestStruct
		msgAsRequestVoteResponse                        *RequestVoteResponseStruct
		ok                                              bool
		recvMsgQueueElement                             *recvMsgQueueElementStruct
		peer                                            *peerStruct
		peers                                           []*peerStruct
		randByteBuf                                     []byte
		requestVoteSuccessfulResponses                  uint64
		requestVoteSuccessfulResponsesRequiredForQuorum uint64
		requestVoteExpirationTime                       time.Time
		requestVoteExpirationDurationRemaining          time.Duration
		requestVoteMsgTag                               uint64
		timeNow                                         time.Time
	)

	if LogLevelStateChanges <= globals.logLevel {
		logger.Infof("%s entered Candidate state", globals.myUDPAddr)
	}

	// Point all LivenessCheckAssignments at globals.whoAmI

	globals.Lock()
	livenessReportWhileCandidate = computeLivenessCheckAssignments([]string{globals.whoAmI})
	updateMyObservingPeerReportWhileLocked(livenessReportWhileCandidate.observingPeer[globals.whoAmI])
	globals.Unlock()
	globals.livenessCheckerControlChan <- true

	// Attempt to start a new term

	globals.currentTerm++

	if 0 == len(globals.peersByTuple) {
		// Special one peer cluster case... there will be no RequestForVote Responses, so just convert to Leader
		globals.nextState = doLeader
		return
	}

	// Issue RequestVoteRequest to all other Peers

	requestVoteMsgTag = fetchNonce()

	msgAsRequestVoteRequest = &RequestVoteRequestStruct{
		MsgType:       MsgTypeRequestVoteRequest,
		MsgTag:        requestVoteMsgTag,
		CandidateTerm: globals.currentTerm,
	}

	peers, err = sendMsg(nil, msgAsRequestVoteRequest)
	if nil != err {
		panic(err)
	}

	awaitingResponses = make(map[*peerStruct]struct{})

	for _, peer = range peers {
		awaitingResponses[peer] = struct{}{}
	}

	// Minimize split votes by picking a requestVoteExpirationTime at some random
	// point between globals.heartbeatDuration and globals.heartbeatMissDuration

	randByteBuf = make([]byte, 1)
	_, err = rand.Read(randByteBuf)
	if nil != err {
		err = fmt.Errorf("rand.Read(randByteBuf) failed: %v", err)
		panic(err)
	}

	durationDelta = globals.heartbeatMissDuration - globals.heartbeatDuration
	durationDelta *= time.Duration(randByteBuf[0])
	durationDelta /= time.Duration(0x100)
	durationDelta += globals.heartbeatDuration

	requestVoteExpirationTime = time.Now().Add(durationDelta)

	requestVoteSuccessfulResponsesRequiredForQuorum = (uint64(len(awaitingResponses)) + 1) / 2
	requestVoteSuccessfulResponses = 0

	for {
		timeNow = time.Now()

		if timeNow.After(requestVoteExpirationTime) || timeNow.Equal(requestVoteExpirationTime) {
			// Simply return to try again
			return
		}

		requestVoteExpirationDurationRemaining = requestVoteExpirationTime.Sub(timeNow)

		select {
		case <-globals.stateMachineStopChan:
			globals.stateMachineDone.Done()
			runtime.Goexit()
		case <-globals.recvMsgChan:
			recvMsgQueueElement = popGlobalMsg()
			if nil != recvMsgQueueElement {
				peer = recvMsgQueueElement.peer
				switch recvMsgQueueElement.msgType {
				case MsgTypeHeartBeatRequest:
					msgAsHeartBeatRequest = recvMsgQueueElement.msg.(*HeartBeatRequestStruct)
					if msgAsHeartBeatRequest.LeaderTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsHeartBeatRequest.LeaderTerm == globals.currentTerm {
						// Somebody else must have won the election... so convert to Follower
						globals.currentLeader = peer
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{
							MsgType:     MsgTypeHeartBeatResponse,
							MsgTag:      msgAsHeartBeatRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							Success:     true,
						}
						_, err = sendMsg(peer, msgAsHeartBeatResponse)
						if nil != err {
							panic(err)
						}
						globals.nextState = doFollower
						return
					} else { // msgAsHeartBeatRequest.LeaderTerm > globals.currentTerm
						globals.currentTerm = msgAsHeartBeatRequest.LeaderTerm
						// We missed a subsequent election, so convert to Follower state
						globals.currentLeader = peer
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{
							MsgType:     MsgTypeHeartBeatResponse,
							MsgTag:      msgAsHeartBeatRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							Success:     true,
						}
						_, err = sendMsg(peer, msgAsHeartBeatResponse)
						if nil != err {
							panic(err)
						}
						globals.nextState = doFollower
						return
					}
				case MsgTypeHeartBeatResponse:
					msgAsHeartBeatResponse = recvMsgQueueElement.msg.(*HeartBeatResponseStruct)
					if msgAsHeartBeatResponse.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsHeartBeatResponse.CurrentTerm == globals.currentTerm {
						// Unexpected... so convert to Follower state
						globals.nextState = doFollower
						return
					} else { // msgAsHeartBeatResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsHeartBeatResponse.CurrentTerm
						// Convert to Follower state
						globals.nextState = doFollower
						return
					}
				case MsgTypeRequestVoteRequest:
					msgAsRequestVoteRequest = recvMsgQueueElement.msg.(*RequestVoteRequestStruct)
					if msgAsRequestVoteRequest.CandidateTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsRequestVoteRequest.CandidateTerm == globals.currentTerm {
						// We voted for ourself, so vote no
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{
							MsgType:     MsgTypeRequestVoteResponse,
							MsgTag:      msgAsRequestVoteRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							VoteGranted: false,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
					} else { // msgAsRequestVoteRequest.CandidateTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteRequest.CandidateTerm
						// Abandon our election, vote yes, and convert to Follower
						globals.currentLeader = nil
						globals.currentVote = peer
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{
							MsgType:     MsgTypeRequestVoteResponse,
							MsgTag:      msgAsRequestVoteRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							VoteGranted: true,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
						globals.nextState = doFollower
						return
					}
				case MsgTypeRequestVoteResponse:
					msgAsRequestVoteResponse = recvMsgQueueElement.msg.(*RequestVoteResponseStruct)
					if msgAsRequestVoteResponse.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsRequestVoteResponse.CurrentTerm == globals.currentTerm {
						if requestVoteMsgTag == msgAsRequestVoteResponse.MsgTag {
							// If this is an unduplicated VoteGranted==true response, check if we are now Leader
							_, ok = awaitingResponses[peer]
							if ok {
								delete(awaitingResponses, peer)
								if msgAsRequestVoteResponse.VoteGranted {
									requestVoteSuccessfulResponses++
									if requestVoteSuccessfulResponses >= requestVoteSuccessfulResponsesRequiredForQuorum {
										// Convert to Leader
										globals.nextState = doLeader
										return
									}
								}
							}
						} else {
							// Unexpected... but ignore it
						}
					} else { // msgAsRequestVoteResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteResponse.CurrentTerm
						// Unexpected... so convert to Follower state
						globals.nextState = doFollower
						return
					}
				case MsgTypeFetchLivenessReportRequest:
					msgAsFetchLivenessReportRequest = recvMsgQueueElement.msg.(*FetchLivenessReportRequestStruct)
					if msgAsFetchLivenessReportRequest.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsFetchLivenessReportRequest.CurrentTerm == globals.currentTerm {
						// Unexpected... reject it
						msgAsFetchLivenessReportResponse = &FetchLivenessReportResponseStruct{
							MsgType:        MsgTypeFetchLivenessReportResponse,
							MsgTag:         msgAsFetchLivenessReportRequest.MsgTag,
							CurrentTerm:    globals.currentTerm,
							CurrentLeader:  "",
							Success:        false,
							LivenessReport: nil,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
					} else { // msgAsFetchLivenessReportRequest.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteResponse.CurrentTerm
						// Unexpected... reject it and convert to Follower state
						msgAsFetchLivenessReportResponse = &FetchLivenessReportResponseStruct{
							MsgType:        MsgTypeFetchLivenessReportResponse,
							MsgTag:         msgAsFetchLivenessReportRequest.MsgTag,
							CurrentTerm:    globals.currentTerm,
							CurrentLeader:  "",
							Success:        false,
							LivenessReport: nil,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
						globals.nextState = doFollower
						return
					}
				case MsgTypeFetchLivenessReportResponse:
					msgAsFetchLivenessReportResponse = recvMsgQueueElement.msg.(*FetchLivenessReportResponseStruct)
					if msgAsFetchLivenessReportResponse.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsFetchLivenessReportResponse.CurrentTerm == globals.currentTerm {
						// Unexpected... so convert to Follower state
						globals.nextState = doFollower
						return
					} else { // msgAsFetchLivenessReportResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsHeartBeatResponse.CurrentTerm
						// Convert to Follower state
						globals.nextState = doFollower
						return
					}
				default:
					err = fmt.Errorf("Unexpected recvMsgQueueElement.msg: %v", reflect.TypeOf(recvMsgQueueElement.msg))
					panic(err)
				}
			}
		case <-time.After(requestVoteExpirationDurationRemaining):
			// We didn't win... but nobody else claims to have either.. so simply return to try again
			return
		}
	}
}

func doFollower() {
	var (
		err                              error
		heartbeatMissTime                time.Time
		heartbeatMissDurationRemaining   time.Duration
		msgAsFetchLivenessReportRequest  *FetchLivenessReportRequestStruct
		msgAsFetchLivenessReportResponse *FetchLivenessReportResponseStruct
		msgAsHeartBeatRequest            *HeartBeatRequestStruct
		msgAsHeartBeatResponse           *HeartBeatResponseStruct
		msgAsRequestVoteRequest          *RequestVoteRequestStruct
		msgAsRequestVoteResponse         *RequestVoteResponseStruct
		observedPeerReport               *ObservingPeerStruct
		peer                             *peerStruct
		recvMsgQueueElement              *recvMsgQueueElementStruct
		timeNow                          time.Time
	)

	if LogLevelStateChanges <= globals.logLevel {
		logger.Infof("%s entered Follower state", globals.myUDPAddr)
	}

	heartbeatMissTime = time.Now().Add(globals.heartbeatMissDuration)

	for {
		timeNow = time.Now()

		if timeNow.After(heartbeatMissTime) || timeNow.Equal(heartbeatMissTime) {
			globals.nextState = doCandidate
			return
		}

		heartbeatMissDurationRemaining = heartbeatMissTime.Sub(timeNow)

		select {
		case <-globals.stateMachineStopChan:
			globals.stateMachineDone.Done()
			runtime.Goexit()
		case <-globals.recvMsgChan:
			recvMsgQueueElement = popGlobalMsg()
			if nil != recvMsgQueueElement {
				peer = recvMsgQueueElement.peer
				switch recvMsgQueueElement.msgType {
				case MsgTypeHeartBeatRequest:
					msgAsHeartBeatRequest = recvMsgQueueElement.msg.(*HeartBeatRequestStruct)
					if msgAsHeartBeatRequest.LeaderTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsHeartBeatRequest.LeaderTerm == globals.currentTerm {
						// In case this is the first, record .currentLeader
						globals.currentLeader = peer
						globals.currentVote = nil
						// Update RWMode
						globals.curRWMode = msgAsHeartBeatRequest.NewRWMode
						err = inode.SetRWMode(globals.curRWMode)
						if nil != err {
							logger.FatalfWithError(err, "inode.SetRWMode(%d) failed", globals.curRWMode)
						}
						// Compute msgAsHeartBeatResponse.Observed & reset globals.myObservingPeerReport
						globals.Lock()
						observedPeerReport = convertInternalToExternalObservingPeerReport(globals.myObservingPeerReport)
						updateMyObservingPeerReportWhileLocked(convertExternalToInternalObservingPeerReport(msgAsHeartBeatRequest.ToObserve))
						globals.Unlock()
						globals.livenessCheckerControlChan <- true
						// Send HeartBeat response
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{
							MsgType:     MsgTypeHeartBeatResponse,
							MsgTag:      msgAsHeartBeatRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							Success:     true,
							Observed:    observedPeerReport,
						}
						_, err = sendMsg(peer, msgAsHeartBeatResponse)
						if nil != err {
							panic(err)
						}
						// Reset heartBeatMissTime
						heartbeatMissTime = time.Now().Add(globals.heartbeatMissDuration)
					} else { // msgAsHeartBeatRequest.LeaderTerm > globals.currentTerm
						globals.currentTerm = msgAsHeartBeatRequest.LeaderTerm
						// We missed out on Leader election, so record .currentLeader
						globals.currentLeader = peer
						globals.currentVote = nil
						// Update RWMode
						globals.curRWMode = msgAsHeartBeatRequest.NewRWMode
						err = inode.SetRWMode(globals.curRWMode)
						if nil != err {
							logger.FatalfWithError(err, "inode.SetRWMode(%d) failed", globals.curRWMode)
						}
						// Compute msgAsHeartBeatResponse.Observed & reset globals.myObservingPeerReport
						globals.Lock()
						observedPeerReport = convertInternalToExternalObservingPeerReport(globals.myObservingPeerReport)
						updateMyObservingPeerReportWhileLocked(convertExternalToInternalObservingPeerReport(msgAsHeartBeatRequest.ToObserve))
						globals.Unlock()
						globals.livenessCheckerControlChan <- true
						// Send HeartBeat response
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{
							MsgType:     MsgTypeHeartBeatResponse,
							MsgTag:      msgAsHeartBeatRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							Success:     true,
							Observed:    observedPeerReport,
						}
						_, err = sendMsg(peer, msgAsHeartBeatResponse)
						if nil != err {
							panic(err)
						}
						// Reset heartBeatMissTime
						heartbeatMissTime = time.Now().Add(globals.heartbeatMissDuration)
					}
				case MsgTypeHeartBeatResponse:
					msgAsHeartBeatResponse = recvMsgQueueElement.msg.(*HeartBeatResponseStruct)
					if msgAsHeartBeatResponse.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsHeartBeatResponse.CurrentTerm == globals.currentTerm {
						// Unexpected... but ignore it
					} else { // msgAsHeartBeatResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsHeartBeatResponse.CurrentTerm
						// Unexpected... but ignore it
					}
				case MsgTypeRequestVoteRequest:
					msgAsRequestVoteRequest = recvMsgQueueElement.msg.(*RequestVoteRequestStruct)
					if msgAsRequestVoteRequest.CandidateTerm < globals.currentTerm {
						// Reject it
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{
							MsgType:     MsgTypeRequestVoteResponse,
							MsgTag:      msgAsRequestVoteRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							VoteGranted: false,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
					} else if msgAsRequestVoteRequest.CandidateTerm == globals.currentTerm {
						if nil != globals.currentLeader {
							// Candidate missed Leader election, so vote no
							msgAsRequestVoteResponse = &RequestVoteResponseStruct{
								MsgType:     MsgTypeRequestVoteResponse,
								MsgTag:      msgAsRequestVoteRequest.MsgTag,
								CurrentTerm: globals.currentTerm,
								VoteGranted: false,
							}
							_, err = sendMsg(peer, msgAsRequestVoteResponse)
							if nil != err {
								panic(err)
							}
						} else {
							if peer == globals.currentVote {
								// Candidate we voted for missed our yes vote and we received msg twice, so vote yes again
								msgAsRequestVoteResponse = &RequestVoteResponseStruct{
									MsgType:     MsgTypeRequestVoteResponse,
									MsgTag:      msgAsRequestVoteRequest.MsgTag,
									CurrentTerm: globals.currentTerm,
									VoteGranted: true,
								}
								_, err = sendMsg(peer, msgAsRequestVoteResponse)
								if nil != err {
									panic(err)
								}
								// Reset heartBeatMissTime
								heartbeatMissTime = time.Now().Add(globals.heartbeatMissDuration)
							} else { // peer != globals.currentVote
								// We voted for someone else or didn't vote, so vote no
								msgAsRequestVoteResponse = &RequestVoteResponseStruct{
									MsgType:     MsgTypeRequestVoteResponse,
									MsgTag:      msgAsRequestVoteRequest.MsgTag,
									CurrentTerm: globals.currentTerm,
									VoteGranted: false,
								}
								_, err = sendMsg(peer, msgAsRequestVoteResponse)
								if nil != err {
									panic(err)
								}
							}
						}
					} else { // msgAsRequestVoteRequest.CandidateTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteRequest.CandidateTerm
						// Vote yes
						globals.currentLeader = nil
						globals.currentVote = peer
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{
							MsgType:     MsgTypeRequestVoteResponse,
							MsgTag:      msgAsRequestVoteRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							VoteGranted: true,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
						// Reset heartBeatMissTime
						heartbeatMissTime = time.Now().Add(globals.heartbeatMissDuration)
					}
				case MsgTypeRequestVoteResponse:
					msgAsRequestVoteResponse = recvMsgQueueElement.msg.(*RequestVoteResponseStruct)
					if msgAsRequestVoteResponse.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsRequestVoteResponse.CurrentTerm == globals.currentTerm {
						// Ignore it
					} else { // msgAsRequestVoteResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteResponse.CurrentTerm
					}
				case MsgTypeFetchLivenessReportRequest:
					msgAsFetchLivenessReportRequest = recvMsgQueueElement.msg.(*FetchLivenessReportRequestStruct)
					if msgAsFetchLivenessReportRequest.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsFetchLivenessReportRequest.CurrentTerm == globals.currentTerm {
						// Unexpected... inform requestor of the actual Leader
						msgAsFetchLivenessReportResponse = &FetchLivenessReportResponseStruct{
							MsgType:        MsgTypeFetchLivenessReportResponse,
							MsgTag:         msgAsFetchLivenessReportRequest.MsgTag,
							CurrentTerm:    globals.currentTerm,
							CurrentLeader:  globals.currentLeader.udpAddr.String(),
							Success:        false,
							LivenessReport: nil,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
					} else { // msgAsFetchLivenessReportRequest.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsFetchLivenessReportRequest.CurrentTerm
						msgAsFetchLivenessReportResponse = &FetchLivenessReportResponseStruct{
							MsgType:        MsgTypeFetchLivenessReportResponse,
							MsgTag:         msgAsFetchLivenessReportRequest.MsgTag,
							CurrentTerm:    globals.currentTerm,
							CurrentLeader:  globals.currentLeader.udpAddr.String(),
							Success:        false,
							LivenessReport: nil,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
					}
				case MsgTypeFetchLivenessReportResponse:
					msgAsFetchLivenessReportResponse = recvMsgQueueElement.msg.(*FetchLivenessReportResponseStruct)
					deliverResponse(recvMsgQueueElement.msgTag, msgAsFetchLivenessReportResponse)
				default:
					err = fmt.Errorf("Unexpected recvMsgQueueElement.msg: %v", reflect.TypeOf(recvMsgQueueElement.msg))
					panic(err)
				}
			}
		case <-time.After(heartbeatMissDurationRemaining):
			globals.nextState = doCandidate
			return
		}
	}
}

func doLeader() {
	var (
		awaitingResponses                             map[*peerStruct]struct{}
		err                                           error
		heartbeatDurationRemaining                    time.Duration
		heartbeatMsgTag                               uint64
		heartbeatSendTime                             time.Time
		heartbeatSuccessfulResponses                  uint64
		heartbeatSuccessfulResponsesRequiredForQuorum uint64
		livenessReportThisHeartBeat                   *internalLivenessReportStruct
		maxDiskUsagePercentage                        uint8
		msgAsFetchLivenessReportRequest               *FetchLivenessReportRequestStruct
		msgAsFetchLivenessReportResponse              *FetchLivenessReportResponseStruct
		msgAsHeartBeatRequest                         *HeartBeatRequestStruct
		msgAsHeartBeatResponse                        *HeartBeatResponseStruct
		msgAsRequestVoteRequest                       *RequestVoteRequestStruct
		msgAsRequestVoteResponse                      *RequestVoteResponseStruct
		ok                                            bool
		peer                                          *peerStruct
		observingPeerReport                           *internalObservingPeerReportStruct
		quorumMembersLastHeartBeat                    []string
		quorumMembersThisHeartBeat                    []string
		reconEndpointReport                           *internalReconEndpointReportStruct
		recvMsgQueueElement                           *recvMsgQueueElementStruct
		timeNow                                       time.Time
	)

	if LogLevelStateChanges <= globals.logLevel {
		logger.Infof("%s entered Leader state", globals.myUDPAddr)
	}

	heartbeatSendTime = time.Now() // Force first time through for{} loop to send a heartbeat

	quorumMembersThisHeartBeat = []string{globals.whoAmI}

	globals.Lock()
	globals.myObservingPeerReport = &internalObservingPeerReportStruct{
		name:          globals.whoAmI,
		servingPeer:   make(map[string]*internalServingPeerReportStruct),
		reconEndpoint: make(map[string]*internalReconEndpointReportStruct),
	}
	globals.Unlock()
	globals.livenessCheckerControlChan <- true

	livenessReportThisHeartBeat = &internalLivenessReportStruct{
		observingPeer: make(map[string]*internalObservingPeerReportStruct),
	}

	for {
		timeNow = time.Now()

		if timeNow.Before(heartbeatSendTime) {
			heartbeatDurationRemaining = heartbeatSendTime.Sub(timeNow)
		} else {
			globals.Lock()

			mergeObservingPeerReportIntoLivenessReport(globals.myObservingPeerReport, livenessReportThisHeartBeat)

			globals.livenessReport = livenessReportThisHeartBeat

			quorumMembersLastHeartBeat = make([]string, len(quorumMembersThisHeartBeat))
			_ = copy(quorumMembersLastHeartBeat, quorumMembersThisHeartBeat)

			livenessReportThisHeartBeat = computeLivenessCheckAssignments(quorumMembersLastHeartBeat)

			updateMyObservingPeerReportWhileLocked(livenessReportThisHeartBeat.observingPeer[globals.whoAmI])

			globals.Unlock()

			quorumMembersThisHeartBeat = make([]string, 1, 1+len(globals.peersByName))
			quorumMembersThisHeartBeat[0] = globals.whoAmI

			heartbeatMsgTag = fetchNonce()

			awaitingResponses = make(map[*peerStruct]struct{})

			for _, peer = range globals.peersByName {
				msgAsHeartBeatRequest = &HeartBeatRequestStruct{
					MsgType:    MsgTypeHeartBeatRequest,
					MsgTag:     heartbeatMsgTag,
					LeaderTerm: globals.currentTerm,
					NewRWMode:  globals.curRWMode,
					ToObserve:  convertInternalToExternalObservingPeerReport(livenessReportThisHeartBeat.observingPeer[peer.name]),
				}

				_, err = sendMsg(peer, msgAsHeartBeatRequest)
				if nil != err {
					panic(err)
				}

				awaitingResponses[peer] = struct{}{}
			}

			heartbeatSendTime = timeNow.Add(globals.heartbeatDuration)
			heartbeatDurationRemaining = globals.heartbeatDuration

			heartbeatSuccessfulResponsesRequiredForQuorum = (uint64(len(awaitingResponses)) + 1) / 2
			heartbeatSuccessfulResponses = 0
		}

		select {
		case <-globals.stateMachineStopChan:
			globals.stateMachineDone.Done()
			runtime.Goexit()
		case <-globals.recvMsgChan:
			recvMsgQueueElement = popGlobalMsg()
			if nil != recvMsgQueueElement {
				peer = recvMsgQueueElement.peer
				switch recvMsgQueueElement.msgType {
				case MsgTypeHeartBeatRequest:
					msgAsHeartBeatRequest = recvMsgQueueElement.msg.(*HeartBeatRequestStruct)
					if msgAsHeartBeatRequest.LeaderTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsHeartBeatRequest.LeaderTerm == globals.currentTerm {
						// Unexpected... so convert to Candidate state
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{
							MsgType:     MsgTypeHeartBeatResponse,
							MsgTag:      msgAsHeartBeatRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							Success:     false,
						}
						_, err = sendMsg(peer, msgAsHeartBeatResponse)
						if nil != err {
							panic(err)
						}
						globals.nextState = doCandidate
						return
					} else { // msgAsHeartBeatRequest.LeaderTerm > globals.currentTerm
						globals.currentTerm = msgAsHeartBeatRequest.LeaderTerm
						// We missed a subsequent election, so convert to Follower state
						globals.currentLeader = peer
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{
							MsgType:     MsgTypeHeartBeatResponse,
							MsgTag:      msgAsHeartBeatRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							Success:     true,
						}
						_, err = sendMsg(peer, msgAsHeartBeatResponse)
						if nil != err {
							panic(err)
						}
						globals.nextState = doFollower
						return
					}
				case MsgTypeHeartBeatResponse:
					msgAsHeartBeatResponse = recvMsgQueueElement.msg.(*HeartBeatResponseStruct)
					if msgAsHeartBeatResponse.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsHeartBeatResponse.CurrentTerm == globals.currentTerm {
						if heartbeatMsgTag == msgAsHeartBeatResponse.MsgTag {
							_, ok = awaitingResponses[peer]
							if ok {
								delete(awaitingResponses, peer)
								if msgAsHeartBeatResponse.Success {
									heartbeatSuccessfulResponses++
									quorumMembersThisHeartBeat = append(quorumMembersThisHeartBeat, peer.name)
									if nil != msgAsHeartBeatResponse.Observed {
										observingPeerReport = convertExternalToInternalObservingPeerReport(msgAsHeartBeatResponse.Observed)
										if nil != observingPeerReport {
											mergeObservingPeerReportIntoLivenessReport(observingPeerReport, livenessReportThisHeartBeat)
										}
									}
								} else {
									// Unexpected... so convert to Follower state
									globals.nextState = doFollower
									return
								}
							}
						} else {
							// Ignore it
						}
					} else { // msgAsHeartBeatResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsHeartBeatResponse.CurrentTerm
						// Convert to Follower state
						globals.nextState = doFollower
						return
					}
				case MsgTypeRequestVoteRequest:
					msgAsRequestVoteRequest = recvMsgQueueElement.msg.(*RequestVoteRequestStruct)
					if msgAsRequestVoteRequest.CandidateTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsRequestVoteRequest.CandidateTerm == globals.currentTerm {
						// Ignore it
					} else { // msgAsRequestVoteRequest.CandidateTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteRequest.CandidateTerm
						// Abandon our Leadership, vote yes, and convert to Follower
						globals.currentLeader = nil
						globals.currentVote = peer
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{
							MsgType:     MsgTypeRequestVoteResponse,
							MsgTag:      msgAsRequestVoteRequest.MsgTag,
							CurrentTerm: globals.currentTerm,
							VoteGranted: true,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
						globals.nextState = doFollower
						return
					}
				case MsgTypeRequestVoteResponse:
					msgAsRequestVoteResponse = recvMsgQueueElement.msg.(*RequestVoteResponseStruct)
					if msgAsRequestVoteResponse.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsRequestVoteResponse.CurrentTerm == globals.currentTerm {
						// Ignore it
					} else { // msgAsRequestVoteResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteResponse.CurrentTerm
						// Unexpected... so convert to Follower state
						globals.nextState = doFollower
						return
					}
				case MsgTypeFetchLivenessReportRequest:
					msgAsFetchLivenessReportRequest = recvMsgQueueElement.msg.(*FetchLivenessReportRequestStruct)
					if msgAsFetchLivenessReportRequest.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsFetchLivenessReportRequest.CurrentTerm == globals.currentTerm {
						msgAsFetchLivenessReportResponse = &FetchLivenessReportResponseStruct{
							MsgType:       MsgTypeFetchLivenessReportResponse,
							MsgTag:        msgAsFetchLivenessReportRequest.MsgTag,
							CurrentTerm:   globals.currentTerm,
							CurrentLeader: globals.myUDPAddr.String(),
						}
						msgAsFetchLivenessReportResponse.LivenessReport = convertInternalToExternalLivenessReport(globals.livenessReport)
						msgAsFetchLivenessReportResponse.Success = (nil != msgAsFetchLivenessReportResponse.LivenessReport)
						_, err = sendMsg(peer, msgAsFetchLivenessReportResponse)
						if nil != err {
							panic(err)
						}
					} else { // msgAsFetchLivenessReportRequest.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsFetchLivenessReportRequest.CurrentTerm
						// Unexpected... reject it and convert to Follower state
						msgAsFetchLivenessReportResponse = &FetchLivenessReportResponseStruct{
							MsgType:        MsgTypeFetchLivenessReportResponse,
							MsgTag:         msgAsFetchLivenessReportRequest.MsgTag,
							CurrentTerm:    globals.currentTerm,
							CurrentLeader:  "",
							Success:        false,
							LivenessReport: nil,
						}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
						globals.nextState = doFollower
						return
					}
				case MsgTypeFetchLivenessReportResponse:
					msgAsFetchLivenessReportResponse = recvMsgQueueElement.msg.(*FetchLivenessReportResponseStruct)
					if msgAsFetchLivenessReportResponse.CurrentTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsFetchLivenessReportResponse.CurrentTerm == globals.currentTerm {
						// Unexpected... so convert to Follower state
						globals.nextState = doFollower
						return
					} else { // msgAsFetchLivenessReportResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsHeartBeatResponse.CurrentTerm
						// Convert to Follower state
						globals.nextState = doFollower
						return
					}
				default:
					err = fmt.Errorf("Unexpected recvMsgQueueElement.msg: %v", reflect.TypeOf(recvMsgQueueElement.msg))
					panic(err)
				}
			}
		case <-time.After(heartbeatDurationRemaining):
			if heartbeatSuccessfulResponses >= heartbeatSuccessfulResponsesRequiredForQuorum {
				// Compute new RWMode

				maxDiskUsagePercentage = 0

				for _, observingPeerReport = range globals.livenessReport.observingPeer {
					for _, reconEndpointReport = range observingPeerReport.reconEndpoint {
						if reconEndpointReport.maxDiskUsagePercentage > maxDiskUsagePercentage {
							maxDiskUsagePercentage = reconEndpointReport.maxDiskUsagePercentage
						}
					}
				}

				if maxDiskUsagePercentage >= globals.swiftReconReadOnlyThreshold {
					globals.curRWMode = inode.RWModeReadOnly
				} else if maxDiskUsagePercentage >= globals.swiftReconNoWriteThreshold {
					globals.curRWMode = inode.RWModeNoWrite
				} else {
					globals.curRWMode = inode.RWModeNormal
				}

				err = inode.SetRWMode(globals.curRWMode)
				if nil != err {
					logger.FatalfWithError(err, "inode.SetRWMode(%d) failed", globals.curRWMode)
				}

				// Now just loop back and issue a fresh HeartBeat
			} else {
				// Quorum lost... convert to Candidate state
				globals.nextState = doCandidate
				return
			}
		}
	}
}
