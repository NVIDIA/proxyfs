package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"hash/crc64"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/cstruct"
)

const (
	udpPacketSizeMin = uint64(1000) // Presumably >> udpPacketHeaderSize
	udpPacketSizeMax = uint64(8000) // Presumably >> udpPacketHeaderSize

	udpPacketSendSizeDefault = uint64(1400)
	udpPacketRecvSizeDefault = uint64(1500)

	udpPacketCapPerMessageMin = uint8(1)
	udpPacketCapPerMessageMax = uint8(10)

	udpPacketCapPerMessageDefault = uint64(5)

	udpPacketHeaderSize uint64 = 8 + 8 + 1 + 1 // sizeof(CRC64) + sizeof(MsgNonce) + sizeof(PacketIndex) + sizeof(PacketCount)

	heartbeatDurationDefault = "1s"

	heartbeatMissLimitMin     = uint64(2)
	heartbeatMissLimitDefault = uint64(3)

	verbosityNone           = uint64(0) // also the default
	verbosityStateChanges   = uint64(1)
	verbosityMessages       = uint64(2)
	verbosityMessageDetails = uint64(3)
	verbosityMax            = uint64(4)
)

// Every msg is cstruct-encoded in cstruct.LittleEndian byte order

type MsgType uint8

const (
	MsgTypeHeartBeatRequest MsgType = iota
	MsgTypeHeartBeatResponse
	MsgTypeRequestVoteRequest
	MsgTypeRequestVoteResponse
)

type MsgTypeStruct struct {
	MsgType MsgType
}

type HeartBeatRequestStruct struct {
	MsgType    MsgType // == MsgTypeHeartBeatRequest
	LeaderTerm uint64
	Nonce      uint64
}

type HeartBeatResponseStruct struct {
	MsgType     MsgType // == MsgTypeHeartBeatResponse
	CurrentTerm uint64
	Nonce       uint64
	Success     bool
}

type RequestVoteRequestStruct struct {
	MsgType       MsgType // == MsgTypeRequestVoteRequest
	CandidateTerm uint64
}

type RequestVoteResponseStruct struct {
	MsgType     MsgType // == MsgTypeRequestVoteResponse
	CurrentTerm uint64
	VoteGranted bool
}

type recvMsgQueueElementStruct struct {
	next    *recvMsgQueueElementStruct
	prev    *recvMsgQueueElementStruct
	peer    *peerStruct
	msgType MsgType     // Even though it's inside msg, make it easier to decode
	msg     interface{} // Must be a pointer to one of the above Msg structs (other than CommonMsgHeaderStruct)
}

type peerStruct struct {
	udpAddr                 *net.UDPAddr
	curRecvMsgNonce         uint64
	curRecvPacketCount      uint8
	curRecvPacketSumSize    uint64
	curRecvPacketMap        map[uint8][]byte           // Key is PacketIndex
	prevRecvMsgQueueElement *recvMsgQueueElementStruct // Protected by globalsStruct.sync.Mutex
	//                                                    Note: Since there is only a single pointer here,
	//                                                          the total number of buffered received msgs
	//                                                          is capped by the number of listed peers
}

type globalsStruct struct {
	sync.Mutex                 // Protects all of globalsStruct as well as peerStruct.prevRecvMsgQueueElement
	myUDPAddr                  *net.UDPAddr
	myUDPConn                  *net.UDPConn
	peers                      map[string]*peerStruct // Key == peerStruct.udpAddr.String() (~= peerStruct.tuple)
	udpPacketSendSize          uint64
	udpPacketSendPayloadSize   uint64
	udpPacketRecvSize          uint64
	udpPacketRecvPayloadSize   uint64
	udpPacketCapPerMessage     uint8
	sendMsgMessageSizeMax      uint64
	heartbeatDuration          time.Duration
	heartbeatMissLimit         uint64
	heartbeatMissDuration      time.Duration
	verbosity                  uint64
	msgTypeBufSize             uint64
	heartBeatRequestBufSize    uint64
	heartBeatResponseBufSize   uint64
	requestVoteRequestBufSize  uint64
	requestVoteResponseBufSize uint64
	crc64ECMATable             *crc64.Table
	nextNonce                  uint64 // Randomly initialized... skips 0
	recvMsgsDoneChan           chan struct{}
	recvMsgQueueHead           *recvMsgQueueElementStruct
	recvMsgQueueTail           *recvMsgQueueElementStruct
	recvMsgChan                chan struct{}
	currentLeader              *peerStruct
	currentVote                *peerStruct
	currentTerm                uint64
	nextState                  func()
}

var globals globalsStruct

func main() {
	var (
		err                          error
		heartbeatDurationStringPtr   *string
		heartbeatMissLimitU64Ptr     *uint64
		myTupleStringPtr             *string
		peerTuplesStringPtr          *string
		signalChan                   chan os.Signal
		udpPacketCapPerMessageU64Ptr *uint64
		udpPacketRecvSizeU64Ptr      *uint64
		udpPacketSendSizeU64Ptr      *uint64
		verbosityU64Ptr              *uint64
	)

	// Parse arguments

	myTupleStringPtr = flag.String("me", "not-supplied", "the Address:UDPPort of this peer")
	peerTuplesStringPtr = flag.String("peers", "", "comma-separated list of other peers in Address:UDPPort form")
	udpPacketSendSizeU64Ptr = flag.Uint64("send", udpPacketSendSizeDefault, "max size of a sent UDP packet")
	udpPacketRecvSizeU64Ptr = flag.Uint64("receive", udpPacketRecvSizeDefault, "max size of a received UDP packet")
	udpPacketCapPerMessageU64Ptr = flag.Uint64("packets", udpPacketCapPerMessageDefault, "max number of UDP packets per message")
	heartbeatDurationStringPtr = flag.String("hbrate", heartbeatDurationDefault, "time.Duration for heartbeat rate")
	heartbeatMissLimitU64Ptr = flag.Uint64("hbmiss", heartbeatMissLimitDefault, "number of heartbeat intervals missed before reelection")
	verbosityU64Ptr = flag.Uint64("verbosity", verbosityNone, "if non-zero, enables logging to os.Stdout")

	flag.Parse()

	// Initialize globals

	err = initializeGlobals(
		*myTupleStringPtr,
		*peerTuplesStringPtr,
		*udpPacketSendSizeU64Ptr,
		*udpPacketRecvSizeU64Ptr,
		*udpPacketCapPerMessageU64Ptr,
		*heartbeatDurationStringPtr,
		*heartbeatMissLimitU64Ptr,
		*verbosityU64Ptr)

	if nil != err {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		flag.PrintDefaults()
		os.Exit(int(syscall.EPERM))
	}

	if verbosityMax <= globals.verbosity {
		dumpGlobals("  ")
	}

	// Set up cleanup signal handler

	signalChan = make(chan os.Signal)
	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM)
	go shutdownSignalHandler(signalChan)

	// Enter processing loop

	for {
		globals.nextState()
	}
}

func initializeGlobals(myTuple string, peerTuples string, udpPacketSendSize uint64, udpPacketRecvSize uint64, udpPacketCapPerMessage uint64, heartbeatDuration string, heartbeatMissLimit uint64, verbosity uint64) (err error) {
	var (
		dummyMsgTypeStruct             MsgTypeStruct
		dummyHeartBeatRequestStruct    HeartBeatRequestStruct
		dummyHeartBeatResponseStruct   HeartBeatResponseStruct
		dummyRequestVoteRequestStruct  RequestVoteRequestStruct
		dummyRequestVoteResponseStruct RequestVoteResponseStruct
		ok                             bool
		peer                           *peerStruct
		peerTuple                      string
		peerTupleSlice                 []string
		u64RandBuf                     []byte
	)

	globals.myUDPAddr, err = net.ResolveUDPAddr("udp", myTuple)
	if nil != err {
		err = fmt.Errorf("Cannot parse meTuple (%s): %v", myTuple, err)
		return
	}

	globals.myUDPConn, err = net.ListenUDP("udp", globals.myUDPAddr)
	if nil != err {
		err = fmt.Errorf("Cannot bind to meTuple (%v): %v", globals.myUDPAddr, err)
		return
	}

	globals.peers = make(map[string]*peerStruct)

	if "" != peerTuples {
		peerTupleSlice = strings.Split(peerTuples, ",")

		for _, peerTuple = range peerTupleSlice {
			if "" == peerTuple {
				err = fmt.Errorf("No peerTuple can be the empty string")
				return
			}
			peer = &peerStruct{
				curRecvMsgNonce:         0,
				curRecvPacketCount:      0,
				curRecvPacketSumSize:    0,
				curRecvPacketMap:        nil,
				prevRecvMsgQueueElement: nil,
			}
			peer.udpAddr, err = net.ResolveUDPAddr("udp", peerTuple)
			if nil != err {
				err = fmt.Errorf("Cannot parse peerTuple (%s): %v", peerTuple, err)
				return
			}
			if globals.myUDPAddr.String() == peer.udpAddr.String() {
				err = fmt.Errorf("peerTuples must not contain meTuple (%v)", globals.myUDPAddr)
				return
			}
			_, ok = globals.peers[peer.udpAddr.String()]
			if ok {
				err = fmt.Errorf("peerTuples must not contain duplicate peers (%v)", peer.udpAddr)
				return
			}
			globals.peers[peer.udpAddr.String()] = peer
		}
	}

	if (udpPacketSendSize < udpPacketSizeMin) || (udpPacketSendSize > udpPacketSizeMax) {
		err = fmt.Errorf("udpPacketSendSize (%v) must be between %v and %v (inclusive)", udpPacketSendSize, udpPacketSizeMin, udpPacketSizeMax)
		return
	}

	globals.udpPacketSendSize = udpPacketSendSize
	globals.udpPacketSendPayloadSize = udpPacketSendSize - udpPacketHeaderSize

	if (udpPacketRecvSize < udpPacketSizeMin) || (udpPacketRecvSize > udpPacketSizeMax) {
		err = fmt.Errorf("udpPacketRecvSize (%v) must be between %v and %v (inclusive)", udpPacketRecvSize, udpPacketSizeMin, udpPacketSizeMax)
		return
	}

	globals.udpPacketRecvSize = udpPacketRecvSize
	globals.udpPacketRecvPayloadSize = udpPacketRecvSize - udpPacketHeaderSize

	if (udpPacketCapPerMessage < uint64(udpPacketCapPerMessageMin)) || (udpPacketCapPerMessage > uint64(udpPacketCapPerMessageMax)) {
		err = fmt.Errorf("udpPacketCapPerMessage (%v) must be between %v and %v (inclusive)", udpPacketCapPerMessage, udpPacketCapPerMessageMin, udpPacketCapPerMessageMax)
		return
	}

	globals.udpPacketCapPerMessage = uint8(udpPacketCapPerMessage)

	globals.sendMsgMessageSizeMax = udpPacketCapPerMessage * globals.udpPacketSendPayloadSize

	globals.heartbeatDuration, err = time.ParseDuration(heartbeatDuration)
	if nil != err {
		err = fmt.Errorf("heartbeatDuration (%s) parsing error: %v", heartbeatDuration, err)
		return
	}
	if time.Duration(0) == globals.heartbeatDuration {
		err = fmt.Errorf("heartbeatDuration must be non-zero")
		return
	}

	if heartbeatMissLimit < heartbeatMissLimitMin {
		err = fmt.Errorf("heartbeatMissLimit (%v) must be at least %v", heartbeatMissLimit, heartbeatMissLimitMin)
		return
	}

	globals.heartbeatMissLimit = heartbeatMissLimit
	globals.heartbeatMissDuration = time.Duration(heartbeatMissLimit) * globals.heartbeatDuration

	if verbosity > verbosityMax {
		err = fmt.Errorf("verbosity (%v) must be between 0 and %v (inclusive)", verbosity, verbosityMax)
		return
	}

	globals.verbosity = verbosity

	globals.msgTypeBufSize, _, err = cstruct.Examine(dummyMsgTypeStruct)
	if nil != err {
		err = fmt.Errorf("cstruct.Examine(dummyMsgTypeStruct) failed: %v", err)
		return
	}
	globals.heartBeatRequestBufSize, _, err = cstruct.Examine(dummyHeartBeatRequestStruct)
	if nil != err {
		err = fmt.Errorf("cstruct.Examine(dummyHeartBeatRequestStruct) failed: %v", err)
		return
	}
	globals.heartBeatResponseBufSize, _, err = cstruct.Examine(dummyHeartBeatResponseStruct)
	if nil != err {
		err = fmt.Errorf("cstruct.Examine(dummyHeartBeatResponseStruct) failed: %v", err)
		return
	}
	globals.requestVoteRequestBufSize, _, err = cstruct.Examine(dummyRequestVoteRequestStruct)
	if nil != err {
		err = fmt.Errorf("cstruct.Examine(dummyRequestVoteRequestStruct) failed: %v", err)
		return
	}
	globals.requestVoteResponseBufSize, _, err = cstruct.Examine(dummyRequestVoteResponseStruct)
	if nil != err {
		err = fmt.Errorf("cstruct.Examine(dummyRequestVoteResponseStruct) failed: %v", err)
		return
	}

	globals.crc64ECMATable = crc64.MakeTable(crc64.ECMA)

	u64RandBuf = make([]byte, 8)
	_, err = rand.Read(u64RandBuf)
	if nil != err {
		err = fmt.Errorf("read.Rand() failed: %v", err)
		return
	}
	globals.nextNonce = deserializeU64LittleEndian(u64RandBuf)
	if 0 == globals.nextNonce {
		globals.nextNonce = 1
	}

	globals.recvMsgQueueHead = nil
	globals.recvMsgQueueTail = nil

	globals.recvMsgChan = make(chan struct{})

	globals.recvMsgsDoneChan = make(chan struct{})
	go recvMsgs()

	globals.currentLeader = nil
	globals.currentVote = nil
	globals.currentTerm = 0

	globals.nextState = doFollower

	err = nil
	return
}

func dumpGlobals(indent string) {
	var (
		peersListNeedsAlignment bool
		peerUDPAddrString       string
	)

	fmt.Printf("%smyUDPAddr:                %22v\n", indent, globals.myUDPAddr)
	fmt.Printf("%sPeers:                   ", indent)
	peersListNeedsAlignment = true
	for peerUDPAddrString = range globals.peers {
		if peersListNeedsAlignment {
			fmt.Printf(" %22v", peerUDPAddrString)
			peersListNeedsAlignment = false
		} else {
			fmt.Printf(" %v", peerUDPAddrString)
		}
	}
	fmt.Printf("\n")
	fmt.Printf("%sudpPacketSendSize:        %22v\n", indent, globals.udpPacketSendSize)
	fmt.Printf("%sudpPacketSendPayloadSize: %22v\n", indent, globals.udpPacketSendPayloadSize)
	fmt.Printf("%sudpPacketRecvSize:        %22v\n", indent, globals.udpPacketRecvSize)
	fmt.Printf("%sudpPacketRecvPayloadSize: %22v\n", indent, globals.udpPacketRecvPayloadSize)
	fmt.Printf("%sudpPacketCapPerMessage:   %22v\n", indent, globals.udpPacketCapPerMessage)
	fmt.Printf("%ssendMsgMessageSizeMax:    %22v\n", indent, globals.sendMsgMessageSizeMax)
	fmt.Printf("%sheartbeatDuration:        %22v\n", indent, globals.heartbeatDuration)
	fmt.Printf("%sheartbeatMissLimit:       %22v\n", indent, globals.heartbeatMissLimit)
	fmt.Printf("%sheartbeatMissDuration:    %22v\n", indent, globals.heartbeatMissDuration)
	fmt.Printf("%snextNonce:                    0x%016X\n", indent, globals.nextNonce)
	fmt.Printf("%scurrentTerm:              %22v\n", indent, globals.currentTerm)
}

func shutdownSignalHandler(signalChan chan os.Signal) {
	_ = <-signalChan
	_ = globals.myUDPConn.Close()
	for {
		select {
		case <-globals.recvMsgChan:
			// Just discard it
		case <-globals.recvMsgsDoneChan:
			os.Exit(0)
		}
	}
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

func appendRecvMsgQueueElementWhileLocked(recvMsgQueueElement *recvMsgQueueElementStruct) {
	if nil == globals.recvMsgQueueHead { // && nil == globals.recvMsgQueueTail
		globals.recvMsgQueueHead = recvMsgQueueElement
		globals.recvMsgQueueTail = recvMsgQueueElement
	} else {
		globals.recvMsgQueueTail.next = recvMsgQueueElement
		recvMsgQueueElement.prev = globals.recvMsgQueueTail
		globals.recvMsgQueueTail = recvMsgQueueElement
	}
}

func removeRecvMsgQueueElementWhileLocked(recvMsgQueueElement *recvMsgQueueElementStruct) {
	if recvMsgQueueElement == globals.recvMsgQueueHead {
		if recvMsgQueueElement == globals.recvMsgQueueTail {
			globals.recvMsgQueueHead = nil
			globals.recvMsgQueueTail = nil
		} else {
			globals.recvMsgQueueHead = globals.recvMsgQueueHead.next
			globals.recvMsgQueueHead.prev = nil
		}
	} else {
		if recvMsgQueueElement == globals.recvMsgQueueTail {
			globals.recvMsgQueueTail = globals.recvMsgQueueTail.prev
			globals.recvMsgQueueTail.next = nil
		} else {
			recvMsgQueueElement.prev.next = recvMsgQueueElement.next
			recvMsgQueueElement.next.prev = recvMsgQueueElement.prev
		}
	}
}

func popMsgWhileLocked() (recvMsgQueueElement *recvMsgQueueElementStruct) {
	if nil == globals.recvMsgQueueHead {
		recvMsgQueueElement = nil
		return
	}

	if globals.recvMsgQueueHead == globals.recvMsgQueueTail {
		recvMsgQueueElement = globals.recvMsgQueueHead
		globals.recvMsgQueueHead = nil
		globals.recvMsgQueueTail = nil
	} else {
		recvMsgQueueElement = globals.recvMsgQueueHead
		globals.recvMsgQueueHead = recvMsgQueueElement.next
		recvMsgQueueElement.next = nil
	}

	recvMsgQueueElement.peer.prevRecvMsgQueueElement = nil

	return
}

func popMsg() (recvMsgQueueElement *recvMsgQueueElementStruct) {
	globals.Lock()
	recvMsgQueueElement = popMsgWhileLocked()
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
		peer, ok = globals.peers[udpAddr.String()]
		globals.Unlock()

		if !ok {
			continue // Ignore it
		}

		// Check if packet is part of a new msg

		if msgNonce != peer.curRecvMsgNonce {
			// Forget prior msg packets and start receiving a new msg with this packet

			peer.curRecvMsgNonce = msgNonce
			peer.curRecvPacketCount = packetCount
			peer.curRecvPacketMap = make(map[uint8][]byte)

			peer.curRecvPacketSumSize = uint64(len(packetBuf[18:]))
			peer.curRecvPacketMap[packetIndex] = packetBuf[18:]
		} else if packetCount != peer.curRecvPacketCount {
			// Must be a re-used msgNonce... forget prior msg packets and start receiving a new msg with this packet

			peer.curRecvPacketCount = packetCount
			peer.curRecvPacketMap = make(map[uint8][]byte)

			peer.curRecvPacketSumSize = uint64(len(packetBuf[18:]))
			peer.curRecvPacketMap[packetIndex] = packetBuf[18:]
		} else {
			// Fill-in (if not already received) this packet into current msg

			_, ok = peer.curRecvPacketMap[packetIndex]

			if ok {
				continue // Ignore it
			}

			peer.curRecvPacketSumSize += uint64(len(packetBuf[18:]))
			peer.curRecvPacketMap[packetIndex] = packetBuf[18:]
		}

		// Have all packets of msg been received?

		if len(peer.curRecvPacketMap) == int(packetCount) {
			// All packets received... assemble completed msg

			msgBuf = make([]byte, 0, peer.curRecvPacketSumSize)

			for packetIndex = 0; packetIndex < peer.curRecvPacketCount; packetIndex++ {
				msgBuf = append(msgBuf, peer.curRecvPacketMap[packetIndex]...)
			}

			// Now discard the packets

			peer.curRecvMsgNonce = 0
			peer.curRecvPacketCount = 0
			peer.curRecvPacketSumSize = 0
			peer.curRecvPacketMap = nil

			// Decode the msg

			msgTypeStruct = MsgTypeStruct{}

			_, err = cstruct.Unpack(msgBuf, &msgTypeStruct, cstruct.LittleEndian)
			if nil != err {
				continue // Ignore it
			}

			recvMsgQueueElement = &recvMsgQueueElementStruct{
				next:    nil,
				prev:    nil,
				peer:    peer,
				msgType: msgTypeStruct.MsgType,
			}

			switch msgTypeStruct.MsgType {
			case MsgTypeHeartBeatRequest:
				if uint64(len(msgBuf)) != globals.heartBeatRequestBufSize {
					continue // Ignore it
				}
				recvMsgQueueElement.msg = &HeartBeatRequestStruct{}
			case MsgTypeHeartBeatResponse:
				if uint64(len(msgBuf)) != globals.heartBeatResponseBufSize {
					continue // Ignore it
				}
				recvMsgQueueElement.msg = &HeartBeatResponseStruct{}
			case MsgTypeRequestVoteRequest:
				if uint64(len(msgBuf)) != globals.requestVoteRequestBufSize {
					continue // Ignore it
				}
				recvMsgQueueElement.msg = &RequestVoteRequestStruct{}
			case MsgTypeRequestVoteResponse:
				if uint64(len(msgBuf)) != globals.requestVoteResponseBufSize {
					continue // Ignore it
				}
				recvMsgQueueElement.msg = &RequestVoteResponseStruct{}
			default:
				continue // Ignore it
			}

			_, err = cstruct.Unpack(msgBuf, recvMsgQueueElement.msg, cstruct.LittleEndian)
			if nil != err {
				continue // Ignore it
			}

			// Deliver msg

			globals.Lock()

			if nil != peer.prevRecvMsgQueueElement {
				// Erase prior msg from globals.recvMsgQueue

				removeRecvMsgQueueElementWhileLocked(peer.prevRecvMsgQueueElement)
			}

			appendRecvMsgQueueElementWhileLocked(recvMsgQueueElement)

			peer.prevRecvMsgQueueElement = recvMsgQueueElement

			globals.Unlock()

			globals.recvMsgChan <- struct{}{}

			if verbosityMessages <= globals.verbosity {
				if verbosityMessageDetails > globals.verbosity {
					fmt.Printf("[%s] %s rec'd %s from %s\n", time.Now().Format(time.RFC3339), globals.myUDPAddr, reflect.TypeOf(recvMsgQueueElement.msg), udpAddr)
				} else {
					fmt.Printf("[%s] %s rec'd %s from %s [%#v]\n", time.Now().Format(time.RFC3339), globals.myUDPAddr, reflect.TypeOf(recvMsgQueueElement.msg), udpAddr, recvMsgQueueElement.msg)
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
		peers = make([]*peerStruct, 0, len(globals.peers))
		for _, peer = range globals.peers {
			peers = append(peers, peer)
		}
		globals.Unlock()
	} else {
		peers = make([]*peerStruct, 1)
		peers[0] = peer
	}

	msgNonce = fetchNonce()

	msgBuf, err = cstruct.Pack(msg, cstruct.LittleEndian)
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

	if verbosityMessages <= globals.verbosity {
		fmt.Printf("[%s] %s sent %s to", time.Now().Format(time.RFC3339), globals.myUDPAddr, reflect.TypeOf(msg))
		for _, peer = range peers {
			fmt.Printf(" %s", peer.udpAddr)
		}
		if verbosityMessageDetails <= globals.verbosity {
			fmt.Printf(" [%#v]", msg)
		}
		fmt.Println()
	}

	err = nil
	return
}

func doCandidate() {
	var (
		awaitingResponses                               map[*peerStruct]struct{}
		durationDelta                                   time.Duration
		err                                             error
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
		timeNow                                         time.Time
	)

	if verbosityStateChanges <= globals.verbosity {
		fmt.Printf("[%s] %s entered Candidate state\n", time.Now().Format(time.RFC3339), globals.myUDPAddr)
	}

	globals.currentTerm++

	msgAsRequestVoteRequest = &RequestVoteRequestStruct{MsgType: MsgTypeRequestVoteRequest, CandidateTerm: globals.currentTerm}

	peers, err = sendMsg(nil, msgAsRequestVoteRequest)
	if nil != err {
		panic(err)
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

	awaitingResponses = make(map[*peerStruct]struct{})
	for _, peer = range peers {
		awaitingResponses[peer] = struct{}{}
	}

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
		case <-globals.recvMsgChan:
			recvMsgQueueElement = popMsg()
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
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{MsgType: MsgTypeHeartBeatResponse, CurrentTerm: globals.currentTerm, Nonce: msgAsHeartBeatRequest.Nonce, Success: true}
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
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{MsgType: MsgTypeHeartBeatResponse, CurrentTerm: globals.currentTerm, Nonce: msgAsHeartBeatRequest.Nonce, Success: true}
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
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{MsgType: MsgTypeRequestVoteResponse, CurrentTerm: globals.currentTerm, VoteGranted: false}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
					} else { // msgAsRequestVoteRequest.CandidateTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteRequest.CandidateTerm
						// Abandon our election, vote yes, and convert to Follower
						globals.currentLeader = nil
						globals.currentVote = peer
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{MsgType: MsgTypeRequestVoteResponse, CurrentTerm: globals.currentTerm, VoteGranted: true}
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
					} else { // msgAsRequestVoteResponse.CurrentTerm > globals.currentTerm
						globals.currentTerm = msgAsRequestVoteResponse.CurrentTerm
						// Unexpected... so convert to Follower state
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
		err                            error
		heartbeatMissTime              time.Time
		heartbeatMissDurationRemaining time.Duration
		msgAsHeartBeatRequest          *HeartBeatRequestStruct
		msgAsHeartBeatResponse         *HeartBeatResponseStruct
		msgAsRequestVoteRequest        *RequestVoteRequestStruct
		msgAsRequestVoteResponse       *RequestVoteResponseStruct
		peer                           *peerStruct
		recvMsgQueueElement            *recvMsgQueueElementStruct
		timeNow                        time.Time
	)

	if verbosityStateChanges <= globals.verbosity {
		fmt.Printf("[%s] %s entered Follower state\n", time.Now().Format(time.RFC3339), globals.myUDPAddr)
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
		case <-globals.recvMsgChan:
			recvMsgQueueElement = popMsg()
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
						// Send HeartBeat response
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{MsgType: MsgTypeHeartBeatResponse, CurrentTerm: globals.currentTerm, Nonce: msgAsHeartBeatRequest.Nonce, Success: true}
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
						// Send HeartBeat response
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{MsgType: MsgTypeHeartBeatResponse, CurrentTerm: globals.currentTerm, Nonce: msgAsHeartBeatRequest.Nonce, Success: true}
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
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{MsgType: MsgTypeRequestVoteResponse, CurrentTerm: globals.currentTerm, VoteGranted: false}
						_, err = sendMsg(peer, msgAsRequestVoteResponse)
						if nil != err {
							panic(err)
						}
					} else if msgAsRequestVoteRequest.CandidateTerm == globals.currentTerm {
						if nil != globals.currentLeader {
							// Candidate missed Leader election, so vote no
							msgAsRequestVoteResponse = &RequestVoteResponseStruct{MsgType: MsgTypeRequestVoteResponse, CurrentTerm: globals.currentTerm, VoteGranted: false}
							_, err = sendMsg(peer, msgAsRequestVoteResponse)
							if nil != err {
								panic(err)
							}
						} else {
							if peer == globals.currentVote {
								// Candidate we voted for missed our yes vote and we received msg twice, so vote yes again
								msgAsRequestVoteResponse = &RequestVoteResponseStruct{MsgType: MsgTypeRequestVoteResponse, CurrentTerm: globals.currentTerm, VoteGranted: true}
								_, err = sendMsg(peer, msgAsRequestVoteResponse)
								if nil != err {
									panic(err)
								}
								// Reset heartBeatMissTime
								heartbeatMissTime = time.Now().Add(globals.heartbeatMissDuration)
							} else { // peer != globals.currentVote
								// We voted for someone else or didn't vote, so vote no
								msgAsRequestVoteResponse = &RequestVoteResponseStruct{MsgType: MsgTypeRequestVoteResponse, CurrentTerm: globals.currentTerm, VoteGranted: false}
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
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{MsgType: MsgTypeRequestVoteResponse, CurrentTerm: globals.currentTerm, VoteGranted: true}
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
		heartbeatNonce                                uint64
		heartbeatSendTime                             time.Time
		heartbeatSuccessfulResponses                  uint64
		heartbeatSuccessfulResponsesRequiredForQuorum uint64
		msgAsHeartBeatRequest                         *HeartBeatRequestStruct
		msgAsHeartBeatResponse                        *HeartBeatResponseStruct
		msgAsRequestVoteRequest                       *RequestVoteRequestStruct
		msgAsRequestVoteResponse                      *RequestVoteResponseStruct
		ok                                            bool
		peer                                          *peerStruct
		peers                                         []*peerStruct
		recvMsgQueueElement                           *recvMsgQueueElementStruct
		timeNow                                       time.Time
	)

	if verbosityStateChanges <= globals.verbosity {
		fmt.Printf("[%s] %s entered Leader state\n", time.Now().Format(time.RFC3339), globals.myUDPAddr)
	}

	heartbeatSendTime = time.Now() // Force first time through for{} loop to send a heartbeat

	for {
		timeNow = time.Now()

		if timeNow.Before(heartbeatSendTime) {
			heartbeatDurationRemaining = heartbeatSendTime.Sub(timeNow)
		} else {
			heartbeatNonce = fetchNonce()

			msgAsHeartBeatRequest = &HeartBeatRequestStruct{MsgType: MsgTypeHeartBeatRequest, LeaderTerm: globals.currentTerm, Nonce: heartbeatNonce}

			peers, err = sendMsg(nil, msgAsHeartBeatRequest)
			if nil != err {
				panic(err)
			}

			heartbeatSendTime = timeNow.Add(globals.heartbeatDuration)
			heartbeatDurationRemaining = globals.heartbeatDuration

			awaitingResponses = make(map[*peerStruct]struct{})
			for _, peer = range peers {
				awaitingResponses[peer] = struct{}{}
			}

			heartbeatSuccessfulResponsesRequiredForQuorum = (uint64(len(awaitingResponses)) + 1) / 2
			heartbeatSuccessfulResponses = 0
		}

		select {
		case <-globals.recvMsgChan:
			recvMsgQueueElement = popMsg()
			if nil != recvMsgQueueElement {
				peer = recvMsgQueueElement.peer
				switch recvMsgQueueElement.msgType {
				case MsgTypeHeartBeatRequest:
					msgAsHeartBeatRequest = recvMsgQueueElement.msg.(*HeartBeatRequestStruct)
					if msgAsHeartBeatRequest.LeaderTerm < globals.currentTerm {
						// Ignore it
					} else if msgAsHeartBeatRequest.LeaderTerm == globals.currentTerm {
						// Unexpected... so convert to Candidate state
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{MsgType: MsgTypeHeartBeatResponse, CurrentTerm: globals.currentTerm, Nonce: msgAsHeartBeatRequest.Nonce, Success: false}
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
						msgAsHeartBeatResponse = &HeartBeatResponseStruct{MsgType: MsgTypeHeartBeatResponse, CurrentTerm: globals.currentTerm, Nonce: msgAsHeartBeatRequest.Nonce, Success: true}
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
						if heartbeatNonce == msgAsHeartBeatResponse.Nonce {
							_, ok = awaitingResponses[peer]
							if ok {
								delete(awaitingResponses, peer)
								if msgAsHeartBeatResponse.Success {
									heartbeatSuccessfulResponses++
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
						msgAsRequestVoteResponse = &RequestVoteResponseStruct{MsgType: MsgTypeRequestVoteResponse, CurrentTerm: globals.currentTerm, VoteGranted: true}
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
				default:
					err = fmt.Errorf("Unexpected recvMsgQueueElement.msg: %v", reflect.TypeOf(recvMsgQueueElement.msg))
					panic(err)
				}
			}
		case <-time.After(heartbeatDurationRemaining):
			if heartbeatSuccessfulResponses >= heartbeatSuccessfulResponsesRequiredForQuorum {
				// Just loop back and issue a fresh HeartBeat
			} else {
				// Quorum lost... convert to Candidate state
				globals.nextState = doCandidate
				return
			}
		}
	}
}
