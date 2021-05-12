// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package liveness

import (
	"container/list"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"net"
	"reflect"
	"time"

	"github.com/NVIDIA/proxyfs/inode"
	"github.com/NVIDIA/proxyfs/logger"
)

// Each UDP Packet Header is made up of (also in LittleEndian byte order):
//   CRC64       uint64
//   MsgNonce    uint64
//   PacketIndex uint8
//   PacketCount uint8

const udpPacketHeaderSize uint64 = 8 + 8 + 1 + 1 // sizeof(CRC64) + sizeof(MsgNonce) + sizeof(PacketIndex) + sizeof(PacketCount)

// Every msg is JSON-encoded

type MsgType uint8

const (
	MsgTypeHeartBeatRequest MsgType = iota + 1 // Skip zero to avoid msg's missing the MsgType field
	MsgTypeHeartBeatResponse
	MsgTypeRequestVoteRequest
	MsgTypeRequestVoteResponse
	MsgTypeFetchLivenessReportRequest
	MsgTypeFetchLivenessReportResponse
)

type MsgTypeStruct struct {
	MsgType MsgType
	MsgTag  uint64 // Used for matching a *RequestStruct to a subsequent *ResponseStruct
}

type HeartBeatRequestStruct struct {
	MsgType    MsgType //              == MsgTypeHeartBeatRequest
	MsgTag     uint64  //              Used for matching this HeartBeatRequestStruct to a subsequent HeartBeatResponseStruct
	LeaderTerm uint64
	NewRWMode  inode.RWModeType     // One of inode.RWModeNormal, inode.RWModeNoWrite, or inode.RWModeReadOnly
	ToObserve  *ObservingPeerStruct // VolumeStruct.State & VolumeStruct.LastCheckTime are ignored
}

type HeartBeatResponseStruct struct {
	MsgType     MsgType // == MsgTypeHeartBeatResponse
	MsgTag      uint64  // Used for matching this HeartBeatResponseStruct to a previous HeartBeatRequestStruct
	CurrentTerm uint64
	Success     bool
	Observed    *ObservingPeerStruct
}

type RequestVoteRequestStruct struct {
	MsgType       MsgType // == MsgTypeRequestVoteRequest
	MsgTag        uint64  // Used for matching this RequestVoteRequestStruct to a subsequent RequestVoteResponseStruct
	CandidateTerm uint64
}

type RequestVoteResponseStruct struct {
	MsgType     MsgType // == MsgTypeRequestVoteResponse
	MsgTag      uint64  // Used for matching this RequestVoteResponseStruct to a previous RequestVoteRequestStruct
	CurrentTerm uint64
	VoteGranted bool
}

type FetchLivenessReportRequestStruct struct {
	MsgType MsgType //    == MsgTypeFetchLivenessReportRequest
	//                    Used to request Liveness Report from who we think is the Leader
	MsgTag      uint64 // Used for matching this FetchLivenessReportRequestStruct to a subsequent FetchLivenessReportResponseStruct
	CurrentTerm uint64
}

type FetchLivenessReportResponseStruct struct {
	MsgType        MsgType               // == MsgTypeFetchLivenessReportResponse
	MsgTag         uint64                // Used for matching this FetchLivenessReportResponseStruct to a previous FetchLivenessReportRequestStruct
	CurrentTerm    uint64                // == LeaderTerm if Success === true (by definition)
	CurrentLeader  string                // If Success == false, this is who should actually be contacted for this (if known)
	Success        bool                  // == true if Leader is responding; == false if we are not the Leader
	LivenessReport *LivenessReportStruct // Liveness Report as collected by Leader
}

type recvMsgQueueElementStruct struct {
	peer                      *peerStruct
	msgNonce                  uint64
	packetCount               uint8
	packetSumSize             uint64
	packetMap                 map[uint8][]byte // Key == PacketIndex
	peerRecvMsgQueueElement   *list.Element
	globalRecvMsgQueueElement *list.Element
	msgType                   MsgType     // Even though it's inside msg, make it easier to decode
	msgTag                    uint64      // Even though it's inside msg, make it easier to decode
	msg                       interface{} // Must be a pointer to one of the above Msg structs (other than CommonMsgHeaderStruct)
}

type requestStruct struct {
	element        *list.Element
	msgTag         uint64
	expirationTime time.Time
	expired        bool
	requestContext interface{}
	requestMsg     interface{}
	responseMsg    interface{}
	callback       func(request *requestStruct) // If expired == true, responseMsg will be nil
}

func fetchNonceWhileLocked() (nonce uint64) {
	nonce = globals.nextNonce
	globals.nextNonce++
	if 0 == globals.nextNonce {
		globals.nextNonce = 1
	}
	return
}

func fetchNonce() (nonce uint64) {
	globals.Lock()
	nonce = fetchNonceWhileLocked()
	globals.Unlock()
	return
}

func serializeU64LittleEndian(u64 uint64) (u64Buf []byte) {
	u64Buf = make([]byte, 8)
	u64Buf[0] = byte(u64 & 0xFF)
	u64Buf[1] = byte((u64 >> 8) & 0xFF)
	u64Buf[2] = byte((u64 >> 16) & 0xFF)
	u64Buf[3] = byte((u64 >> 24) & 0xFF)
	u64Buf[4] = byte((u64 >> 32) & 0xFF)
	u64Buf[5] = byte((u64 >> 40) & 0xFF)
	u64Buf[6] = byte((u64 >> 48) & 0xFF)
	u64Buf[7] = byte((u64 >> 56) & 0xFF)
	return
}

func deserializeU64LittleEndian(u64Buf []byte) (u64 uint64) {
	u64 = uint64(u64Buf[7])
	u64 = (u64 << 8) | uint64(u64Buf[6])
	u64 = (u64 << 8) | uint64(u64Buf[5])
	u64 = (u64 << 8) | uint64(u64Buf[4])
	u64 = (u64 << 8) | uint64(u64Buf[3])
	u64 = (u64 << 8) | uint64(u64Buf[2])
	u64 = (u64 << 8) | uint64(u64Buf[1])
	u64 = (u64 << 8) | uint64(u64Buf[0])
	return
}

func appendGlobalRecvMsgQueueElementWhileLocked(recvMsgQueueElement *recvMsgQueueElementStruct) {
	recvMsgQueueElement.globalRecvMsgQueueElement = globals.recvMsgQueue.PushBack(recvMsgQueueElement)
}
func appendGlobalRecvMsgQueueElement(recvMsgQueueElement *recvMsgQueueElementStruct) {
	globals.Lock()
	appendGlobalRecvMsgQueueElementWhileLocked(recvMsgQueueElement)
	globals.Unlock()
}

func removeGlobalRecvMsgQueueElementWhileLocked(recvMsgQueueElement *recvMsgQueueElementStruct) {
	_ = globals.recvMsgQueue.Remove(recvMsgQueueElement.globalRecvMsgQueueElement)
}
func removeGlobalRecvMsgQueueElement(recvMsgQueueElement *recvMsgQueueElementStruct) {
	globals.Lock()
	removeGlobalRecvMsgQueueElementWhileLocked(recvMsgQueueElement)
	globals.Unlock()
}

func popGlobalMsgWhileLocked() (recvMsgQueueElement *recvMsgQueueElementStruct) {
	if 0 == globals.recvMsgQueue.Len() {
		recvMsgQueueElement = nil
	} else {
		recvMsgQueueElement = globals.recvMsgQueue.Front().Value.(*recvMsgQueueElementStruct)
		recvMsgQueueElement.peer.completeRecvMsgQueue.Remove(recvMsgQueueElement.peerRecvMsgQueueElement)
		removeGlobalRecvMsgQueueElementWhileLocked(recvMsgQueueElement)
	}
	return
}
func popGlobalMsg() (recvMsgQueueElement *recvMsgQueueElementStruct) {
	globals.Lock()
	recvMsgQueueElement = popGlobalMsgWhileLocked()
	globals.Unlock()
	return
}

func recvMsgs() {
	var (
		computedCRC64       uint64
		err                 error
		msgBuf              []byte
		msgNonce            uint64
		msgTypeStruct       MsgTypeStruct
		ok                  bool
		packetBuf           []byte
		packetCount         uint8
		packetIndex         uint8
		packetSize          int
		peer                *peerStruct
		receivedCRC64       uint64
		recvMsgQueueElement *recvMsgQueueElementStruct
		udpAddr             *net.UDPAddr
	)

	for {
		// Read next packet

		packetBuf = make([]byte, globals.udpPacketRecvSize)

		packetSize, udpAddr, err = globals.myUDPConn.ReadFromUDP(packetBuf)

		if nil != err {
			globals.recvMsgsDoneChan <- struct{}{}
			return
		}

		// Decode packet header

		if uint64(packetSize) < udpPacketHeaderSize {
			continue // Ignore it
		}

		packetBuf = packetBuf[:packetSize]

		receivedCRC64 = deserializeU64LittleEndian(packetBuf[:8])
		msgNonce = deserializeU64LittleEndian(packetBuf[8:16])
		packetIndex = packetBuf[16]
		packetCount = packetBuf[17]

		// Validate packet

		computedCRC64 = crc64.Checksum(packetBuf[8:], globals.crc64ECMATable)

		if receivedCRC64 != computedCRC64 {
			continue // Ignore it
		}

		if 0 == msgNonce {
			continue // Ignore it
		}

		if packetIndex >= packetCount {
			continue // Ignore it
		}

		if packetCount > globals.udpPacketCapPerMessage {
			continue // Ignore it
		}

		// Locate peer

		globals.Lock()
		peer, ok = globals.peersByTuple[udpAddr.String()]
		globals.Unlock()

		if !ok {
			continue // Ignore it
		}

		// Check if packet is part of a new msg

		recvMsgQueueElement, ok = peer.incompleteRecvMsgMap[msgNonce]
		if ok {
			// Packet is part of an existing incomplete msg

			peer.incompleteRecvMsgQueue.MoveToBack(recvMsgQueueElement.peerRecvMsgQueueElement)

			if packetCount != recvMsgQueueElement.packetCount {
				// Forget prior msg packets and start receiving a new msg with this packet

				recvMsgQueueElement.packetCount = packetCount
				recvMsgQueueElement.packetSumSize = uint64(len(packetBuf[18:]))
				recvMsgQueueElement.packetMap = make(map[uint8][]byte)

				recvMsgQueueElement.packetMap[packetIndex] = packetBuf[18:]
			} else {
				// Update existing incomplete msg with this packet

				_, ok = recvMsgQueueElement.packetMap[packetIndex]
				if ok {
					continue // Ignore it
				}

				recvMsgQueueElement.packetSumSize += uint64(len(packetBuf[18:]))
				recvMsgQueueElement.packetMap[packetIndex] = packetBuf[18:]
			}
		} else {
			// Packet is part of a new msg

			if uint64(peer.incompleteRecvMsgQueue.Len()) >= globals.messageQueueDepthPerPeer {
				// Make room for this new msg in .incompleteRecvMsgQueue

				recvMsgQueueElement = peer.incompleteRecvMsgQueue.Front().Value.(*recvMsgQueueElementStruct)
				delete(peer.incompleteRecvMsgMap, recvMsgQueueElement.msgNonce)
				_ = peer.incompleteRecvMsgQueue.Remove(recvMsgQueueElement.peerRecvMsgQueueElement)
			}

			// Construct a new recvMsgQueueElement

			recvMsgQueueElement = &recvMsgQueueElementStruct{
				peer:          peer,
				msgNonce:      msgNonce,
				packetCount:   packetCount,
				packetSumSize: uint64(len(packetBuf[18:])),
				packetMap:     make(map[uint8][]byte),
			}

			recvMsgQueueElement.packetMap[packetIndex] = packetBuf[18:]

			peer.incompleteRecvMsgMap[recvMsgQueueElement.msgNonce] = recvMsgQueueElement
			recvMsgQueueElement.peerRecvMsgQueueElement = peer.incompleteRecvMsgQueue.PushBack(recvMsgQueueElement)
		}

		// Have all packets of msg been received?

		if len(recvMsgQueueElement.packetMap) == int(recvMsgQueueElement.packetCount) {
			// All packets received... assemble completed msg

			delete(peer.incompleteRecvMsgMap, recvMsgQueueElement.msgNonce)
			_ = peer.incompleteRecvMsgQueue.Remove(recvMsgQueueElement.peerRecvMsgQueueElement)

			msgBuf = make([]byte, 0, recvMsgQueueElement.packetSumSize)

			for packetIndex = 0; packetIndex < recvMsgQueueElement.packetCount; packetIndex++ {
				msgBuf = append(msgBuf, recvMsgQueueElement.packetMap[packetIndex]...)
			}

			// Decode the msg

			msgTypeStruct = MsgTypeStruct{}

			err = json.Unmarshal(msgBuf, &msgTypeStruct)
			if nil != err {
				continue // Ignore it
			}

			recvMsgQueueElement.msgType = msgTypeStruct.MsgType
			recvMsgQueueElement.msgTag = msgTypeStruct.MsgTag

			switch recvMsgQueueElement.msgType {
			case MsgTypeHeartBeatRequest:
				recvMsgQueueElement.msg = &HeartBeatRequestStruct{}
			case MsgTypeHeartBeatResponse:
				recvMsgQueueElement.msg = &HeartBeatResponseStruct{}
			case MsgTypeRequestVoteRequest:
				recvMsgQueueElement.msg = &RequestVoteRequestStruct{}
			case MsgTypeRequestVoteResponse:
				recvMsgQueueElement.msg = &RequestVoteResponseStruct{}
			case MsgTypeFetchLivenessReportRequest:
				recvMsgQueueElement.msg = &FetchLivenessReportRequestStruct{}
			case MsgTypeFetchLivenessReportResponse:
				recvMsgQueueElement.msg = &FetchLivenessReportResponseStruct{}
			default:
				continue // Ignore it
			}

			err = json.Unmarshal(msgBuf, recvMsgQueueElement.msg)
			if nil != err {
				continue // Ignore it
			}

			// Deliver msg

			globals.Lock()

			recvMsgQueueElement.peerRecvMsgQueueElement = peer.completeRecvMsgQueue.PushBack(recvMsgQueueElement)

			appendGlobalRecvMsgQueueElementWhileLocked(recvMsgQueueElement)

			globals.Unlock()

			globals.recvMsgChan <- struct{}{}

			// Log delivery if requested

			if LogLevelMessages <= globals.logLevel {
				if LogLevelMessageDetails > globals.logLevel {
					logger.Infof("%s rec'd %s from %s", globals.myUDPAddr, reflect.TypeOf(recvMsgQueueElement.msg), udpAddr)
				} else {
					logger.Infof("%s rec'd %s from %s [%#v]", globals.myUDPAddr, reflect.TypeOf(recvMsgQueueElement.msg), udpAddr, recvMsgQueueElement.msg)
				}
			}
		}
	}
}

// sendMsg JSON-encodes msg and sends it to peer (all peers if nil == peer)
func sendMsg(peer *peerStruct, msg interface{}) (peers []*peerStruct, err error) {
	var (
		computedCRC64    uint64
		computedCRC64Buf []byte
		loggerInfofBuf   string
		msgBuf           []byte
		msgBufOffset     uint64
		msgBufSize       uint64
		msgNonce         uint64
		msgNonceBuf      []byte
		packetBuf        []byte
		packetBufIndex   uint64
		packetCount      uint8
		packetIndex      uint8
	)

	if nil == peer {
		globals.Lock()
		peers = make([]*peerStruct, 0, len(globals.peersByTuple))
		for _, peer = range globals.peersByTuple {
			peers = append(peers, peer)
		}
		globals.Unlock()
	} else {
		peers = make([]*peerStruct, 1)
		peers[0] = peer
	}

	msgNonce = fetchNonce()

	msgBuf, err = json.Marshal(msg)
	if nil != err {
		return
	}

	msgBufSize = uint64(len(msgBuf))

	if msgBufSize > globals.sendMsgMessageSizeMax {
		err = fmt.Errorf("sendMsg() called for excessive len(msgBuf) == %v (must be <= %v)", msgBufSize, globals.sendMsgMessageSizeMax)
		return
	}

	packetCount = uint8((msgBufSize + globals.udpPacketSendPayloadSize - 1) / globals.udpPacketSendPayloadSize)

	msgNonceBuf = serializeU64LittleEndian(msgNonce)

	msgBufOffset = 0

	for packetIndex = 0; packetIndex < packetCount; packetIndex++ {
		if packetIndex < (packetCount - 1) {
			packetBuf = make([]byte, udpPacketHeaderSize, globals.udpPacketSendSize)
			packetBuf = append(packetBuf, msgBuf[msgBufOffset:msgBufOffset+globals.udpPacketSendPayloadSize]...)
			msgBufOffset += globals.udpPacketSendPayloadSize
		} else { // packetIndex == (packetCount - 1)
			packetBuf = make([]byte, udpPacketHeaderSize, udpPacketHeaderSize+msgBufSize-msgBufOffset)
			packetBuf = append(packetBuf, msgBuf[msgBufOffset:]...)
		}

		for packetBufIndex = uint64(8); packetBufIndex < uint64(16); packetBufIndex++ {
			packetBuf[packetBufIndex] = msgNonceBuf[packetBufIndex-8]
		}

		packetBuf[16] = packetIndex
		packetBuf[17] = packetCount

		computedCRC64 = crc64.Checksum(packetBuf[8:], globals.crc64ECMATable)
		computedCRC64Buf = serializeU64LittleEndian(computedCRC64)

		for packetBufIndex = uint64(0); packetBufIndex < uint64(8); packetBufIndex++ {
			packetBuf[packetBufIndex] = computedCRC64Buf[packetBufIndex]
		}

		for _, peer = range peers {
			_, err = globals.myUDPConn.WriteToUDP(packetBuf, peer.udpAddr)
			if nil != err {
				err = fmt.Errorf("sendMsg() failed writing to %v: %v", peer.udpAddr, err)
				return
			}
		}
	}

	if LogLevelMessages <= globals.logLevel {
		loggerInfofBuf = fmt.Sprintf("%s sent %s to", globals.myUDPAddr, reflect.TypeOf(msg))
		for _, peer = range peers {
			loggerInfofBuf = loggerInfofBuf + fmt.Sprintf(" %s", peer.udpAddr)
		}
		if LogLevelMessageDetails <= globals.logLevel {
			loggerInfofBuf = loggerInfofBuf + fmt.Sprintf(" [%#v]", msg)
		}
		logger.Info(loggerInfofBuf)
	}

	err = nil
	return
}

func requestExpirer() {
	var (
		expirationDuration  time.Duration
		frontRequest        *requestStruct
		frontRequestElement *list.Element
		ok                  bool
	)

	for {
		globals.Lock()
		frontRequestElement = globals.requestsByExpirationTime.Front()
		globals.Unlock()

		if nil == frontRequestElement {
			select {
			case <-globals.requestExpirerStartChan:
				// Go look again... there is likely something in globals.requestsByExpirationTime now
			case <-globals.requestExpirerStopChan:
				globals.requestExpirerDone.Done()
				return
			}
		} else {
			frontRequest = frontRequestElement.Value.(*requestStruct)
			expirationDuration = frontRequest.expirationTime.Sub(time.Now())

			select {
			case <-time.After(expirationDuration):
				// Expire this request if it hasn't already been responded to
				globals.Lock()
				_, ok = globals.requestsByMsgTag[frontRequest.msgTag]
				if ok {
					_ = globals.requestsByExpirationTime.Remove(frontRequest.element)
					delete(globals.requestsByMsgTag, frontRequest.msgTag)
				}
				globals.Unlock()
				if ok {
					frontRequest.expired = true
					frontRequest.responseMsg = nil
					frontRequest.callback(frontRequest)
				}
			case <-globals.requestExpirerStopChan:
				globals.requestExpirerDone.Done()
				return
			}
		}
	}
}

func sendRequest(peer *peerStruct, msgTag uint64, requestContext interface{}, requestMsg interface{}, callback func(request *requestStruct)) (err error) {
	var (
		request *requestStruct
	)

	if nil == peer {
		err = fmt.Errorf("sendRequest() requires non-nil peer")
		return
	}

	request = &requestStruct{
		msgTag:         msgTag,
		expirationTime: time.Now().Add(globals.maxRequestDuration),
		expired:        false,
		requestContext: requestContext,
		requestMsg:     requestMsg,
		responseMsg:    nil,
		callback:       callback,
	}

	globals.Lock()
	if nil == globals.requestsByExpirationTime.Front() {
		globals.requestExpirerStartChan <- struct{}{}
	}
	request.element = globals.requestsByExpirationTime.PushBack(request)
	globals.requestsByMsgTag[request.msgTag] = request
	globals.Unlock()

	_, err = sendMsg(peer, request.requestMsg)

	if nil != err {
		globals.Lock()
		if !request.expired {
			_ = globals.requestsByExpirationTime.Remove(request.element)
			delete(globals.requestsByMsgTag, request.msgTag)
		}
		globals.Unlock()
	}

	return // err return from sendMsg() is sufficient status
}

func deliverResponse(msgTag uint64, responseMsg interface{}) {
	var (
		ok      bool
		request *requestStruct
	)

	globals.Lock()
	request, ok = globals.requestsByMsgTag[msgTag]
	if ok {
		_ = globals.requestsByExpirationTime.Remove(request.element)
		delete(globals.requestsByMsgTag, request.msgTag)
	}
	globals.Unlock()

	if ok {
		request.responseMsg = responseMsg
		request.callback(request)
	} else {
		// Already marked expired... so ignore it
	}
}
