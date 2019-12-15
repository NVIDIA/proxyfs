// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

import (
	"encoding/binary"
	"io"
	"net"
	"reflect"

	"github.com/swiftstack/ProxyFS/logger"
)

type methodArgs struct {
	methodPtr *reflect.Method
	request   reflect.Type
	reply     reflect.Type
}

// Request is the structure sent over the wire
type ioRequest struct {
	Len    int64  // Length of JReq
	Method string // Needed by "read" goroutine to create Reply{}
	JReq   []byte // JSON containing request
}

// Reply is the structure returned over the wire
type ioReply struct {
	Len     int64  // Length of JResult
	JResult []byte // JSON containing response
}

// reqCtx tracks a request passed to Send() on the client until Send() returns
type reqCtx struct {
	ioreq    ioRequest // Wrapped request passed to Send()
	rpcReply interface{}
	answer   chan interface{}
}

// jsonRequest is used to marshal an RPC request in/out of JSON
type jsonRequest struct {
	MyUniqueID string         `json:"myuniqueid"` // ID of client
	RequestID  uint64         `json:"requestid"`  // ID of this request
	Method     string         `json:"method"`
	Params     [1]interface{} `json:"params"`
}

// jsonReply is used to marshal an RPC response in/out of JSON
type jsonReply struct {
	MyUniqueID string `json:"myuniqueid"` // ID of client
	RequestID  uint64 `json:"requestid"`  // ID of this request
	Err        error  `json:"err"`
	// TODO - include errno too?
	Result interface{} `json:"result"`
}

func getIO(conn net.Conn, who string) (buf []byte, err error) {
	if printDebugLogs {
		logger.Infof("conn: %v", conn)
	}

	// Read in the length of the request first
	var reqLen int64
	err = binary.Read(conn, binary.BigEndian, &reqLen)

	// Now read the rest of the structure off the wire.
	buf = make([]byte, reqLen)
	_, writeErr := io.ReadFull(conn, buf)

	if writeErr != nil {
		err = writeErr
		return
	}

	return
}
