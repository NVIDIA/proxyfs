// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
)

// PayloadProtocols defines the supported protocols for the payload
type PayloadProtocols int

// Support payload protocols
const (
	JSON PayloadProtocols = 1
)

const (
	currentRetryVersion = 1
	bucketStatsPkgName  = "proxyfs.retryrpc"
)

type requestID uint64

// methodStats tracks per method stats
type methodStats struct {
	Method        string                      // Name of method
	Count         bucketstats.Total           // Number of times this method called
	TimeOfRPCCall bucketstats.BucketLog2Round // Length of time of this method of RPC
}

// Useful stats for the clientInfo instance
type statsInfo struct {
	TrimAddCompleted       bucketstats.Total           // Number added to completed list
	TrimRmCompleted        bucketstats.Total           // Number removed from completed list
	CallWrapRPCUsec        bucketstats.BucketLog2Round // Tracks time to unmarshal request before actual RPC
	ReplySize              bucketstats.BucketLog2Round // Tracks completed RPC reply size
	longestRPC             time.Duration               // Time of longest RPC
	longestRPCMethod       string                      // Method of longest RPC
	largestReplySize       uint64                      // Tracks largest RPC reply size
	largestReplySizeMethod string                      // Method of largest RPC reply size completed
	RPCattempted           bucketstats.Total           // Number of RPCs attempted - may be completed or in process
	RPCcompleted           bucketstats.Total           // Number of RPCs which completed - incremented after call returns
	RPCretried             bucketstats.Total           // Number of RPCs which were just pulled from completed list
	PerMethodStats         map[string]*methodStats     // Per method bucketstats
}

// Server side data structure storing per client information
// such as completed requests, etc
type clientInfo struct {
	sync.Mutex
	cCtx                     *connCtx                      // Current connCtx for client
	myUniqueID               uint64                        // Unique ID of this client
	completedRequest         map[requestID]*completedEntry // Key: "RequestID"
	completedRequestLRU      *list.List                    // LRU used to remove completed request in ticker
	highestReplySeen         requestID                     // Highest consectutive requestID client has seen
	previousHighestReplySeen requestID                     // Previous highest consectutive requestID client has seen
	stats                    statsInfo
}

type completedEntry struct {
	reply   *ioReply
	lruElem *list.Element
}

// connCtx tracks a conn which has been accepted.
//
// It also contains the lock used for serialization when
// reading or writing on the socket.
type connCtx struct {
	sync.Mutex
	conn                net.Conn
	activeRPCsWG        sync.WaitGroup // WaitGroup tracking active RPCs from this client on this connection
	cond                *sync.Cond     // Signal waiting goroutines that serviceClient() has exited
	serviceClientExited bool
	ci                  *clientInfo // Back pointer to the CI
}

// methodArgs defines the method provided by the RPC server
// as well as the request type and reply type arguments
type methodArgs struct {
	methodPtr    *reflect.Method
	passClientID bool
	request      reflect.Type
	reply        reflect.Type
}

// completedLRUEntry tracks time entry was completed for
// expiration from cache
type completedLRUEntry struct {
	requestID     requestID
	timeCompleted time.Time
}

// Magic number written at the end of the ioHeader.   Used
// to detect if the complete header has been read.
const headerMagic uint32 = 0xCAFEFEED

// MsgType is the type of message being sent
type MsgType uint16

const (
	// RPC represents an RPC from client to server
	RPC MsgType = iota + 1
	// Upcall represents an upcall from server to client
	Upcall
	// AskMyUniqueID is the message sent by the client during it's initial connection
	// to get it's unique client ID.
	AskMyUniqueID
	// ReturnUniqueID is the message sent by the server to client after initial connection.
	// The server is returning the unique ID created for this client.
	ReturnUniqueID
	// PassID is the message sent by the client to identify itself to server.
	// This is used when we are retransmitting.
	PassID
)

// ioHeader is the header sent on the socket
type ioHeader struct {
	Len      uint32 // Number of bytes following header
	Protocol uint16
	Version  uint16
	Type     MsgType
	Magic    uint32 // Magic number - if invalid means have not read complete header
}

// ioRequest tracks fields written on wire
type ioRequest struct {
	Hdr  ioHeader
	JReq []byte // JSON containing request
}

// ioReply is the structure returned over the wire
type ioReply struct {
	Hdr     ioHeader
	JResult []byte // JSON containing response
}

// internalSetIDRequest is the structure sent over the wire
// when the connection existed, was broken and is being recreated
// by the client as a result of reDial().   This is how the server
// learns existing client ID.
type internalSetIDRequest struct {
	Hdr        ioHeader
	MyUniqueID []byte // Client unique ID as byte
}

// internalINeedIDRequest is the structure sent over the wire
// when the connection is first made.   This is how the client
// learns its client ID
type internalINeedIDRequest struct {
	Hdr      ioHeader
	UniqueID []byte // Client unique ID as byte
}

type replyCtx struct {
	err error
}

// reqCtx exists on the client and tracks a request passed to Send()
type reqCtx struct {
	ioreq     *ioRequest // Wrapped request passed to Send()
	rpcReply  interface{}
	answer    chan replyCtx
	genNum    uint64    // Generation number of socket when request sent
	startTime time.Time // Time Send() called sendToServer()
}

// jsonRequest is used to marshal an RPC request in/out of JSON
type jsonRequest struct {
	MyUniqueID       uint64         `json:"myuniqueid"`       // ID of client
	RequestID        requestID      `json:"requestid"`        // ID of this request
	HighestReplySeen requestID      `json:"highestReplySeen"` // Used to trim completedRequests on server
	Method           string         `json:"method"`
	Params           [1]interface{} `json:"params"`
}

// jsonReply is used to marshal an RPC response in/out of JSON
type jsonReply struct {
	MyUniqueID uint64      `json:"myuniqueid"` // ID of client
	RequestID  requestID   `json:"requestid"`  // ID of this request
	ErrStr     string      `json:"errstr"`
	Result     interface{} `json:"result"`
}

// svrRequest is used with jsonRequest when we unmarshal the
// parameters passed in an RPC.  This is how we get the rpcReply
// structure specific to the RPC
type svrRequest struct {
	Params [1]interface{} `json:"params"`
}

// svrReply is used with jsonReply when we marshal the reply
type svrResponse struct {
	Result interface{} `json:"result"`
}

func buildIoRequest(jReq *jsonRequest) (ioreq *ioRequest, err error) {
	ioreq = &ioRequest{}
	ioreq.JReq, err = json.Marshal(*jReq)
	if err != nil {
		return nil, err
	}
	ioreq.Hdr.Len = uint32(len(ioreq.JReq))
	ioreq.Hdr.Protocol = uint16(JSON)
	ioreq.Hdr.Version = currentRetryVersion
	ioreq.Hdr.Type = RPC
	ioreq.Hdr.Magic = headerMagic
	return
}

func setupHdrReply(ioreply *ioReply, t MsgType) {
	ioreply.Hdr.Len = uint32(len(ioreply.JResult))
	ioreply.Hdr.Protocol = uint16(JSON)
	ioreply.Hdr.Version = currentRetryVersion
	ioreply.Hdr.Type = t
	ioreply.Hdr.Magic = headerMagic
}

func buildSetIDRequest(myUniqueID uint64) (isreq *internalSetIDRequest, err error) {
	isreq = &internalSetIDRequest{}
	isreq.MyUniqueID, err = json.Marshal(myUniqueID)
	if err != nil {
		return nil, err
	}
	isreq.Hdr.Len = uint32(len(isreq.MyUniqueID))
	isreq.Hdr.Protocol = uint16(JSON)
	isreq.Hdr.Version = currentRetryVersion
	isreq.Hdr.Type = PassID
	isreq.Hdr.Magic = headerMagic
	return
}

func buildINeedIDRequest() (iinreq *internalINeedIDRequest, err error) {
	iinreq = &internalINeedIDRequest{}
	if err != nil {
		return nil, err
	}
	iinreq.Hdr.Len = uint32(0)
	iinreq.Hdr.Protocol = uint16(JSON)
	iinreq.Hdr.Version = currentRetryVersion
	iinreq.Hdr.Type = AskMyUniqueID
	iinreq.Hdr.Magic = headerMagic
	return
}

func getIO(genNum uint64, deadlineIO time.Duration, conn net.Conn) (buf []byte, msgType MsgType, err error) {
	// Read in the header of the request first
	var hdr ioHeader

	err = conn.SetDeadline(time.Now().Add(deadlineIO))
	if err != nil {
		return
	}

	err = binary.Read(conn, binary.BigEndian, &hdr)
	if err != nil {
		return
	}

	if hdr.Magic != headerMagic {
		err = fmt.Errorf("Incomplete read of header")
		return
	}

	msgType = hdr.Type

	// Now read the rest of the structure off the wire.
	var numBytes int
	buf = make([]byte, hdr.Len)
	conn.SetDeadline(time.Now().Add(deadlineIO))
	numBytes, err = io.ReadFull(conn, buf)
	if err != nil {
		err = fmt.Errorf("Incomplete read of body")
		return
	}

	if hdr.Len != uint32(numBytes) {
		err = fmt.Errorf("Incomplete read of body")
		return
	}

	return
}
