// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/swiftstack/ProxyFS/logger"
)

// Request is the structure sent
type ioRequest struct {
	Len    int64  // Length of JReq
	Method string // Needed by "read" goroutine to create Reply{}
	JReq   []byte // JSON containing request
}

// Reply is the structure returned
type ioReply struct {
	Len     int64  // Length of JResult
	JResult []byte // JSON containing response
}

// reqCtx tracks a request passed to Send() until Send() returns
type reqCtx struct {
	ioreq  ioRequest        // Wrapped request passed to Send()
	answer chan interface{} // Channel with RPC reply
}

// NOTE: jsonRequest and jsonReply get "redefined" by the client's
// version of these data structures.   This is because the
// client code has to unmarshal the Params and Result fields
// into data structure's specific to the RPC.

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
	Result [1]interface{} `json:"result"`
}

// TODO clean up the names of these data structures ... jReq is already
// unmarshaled so should not be named jReq...
func getIO(conn net.Conn, who string) (buf []byte, err error) {
	if printDebugLogs {
		logger.Infof("conn: %v", conn)
	}

	// Read in the length of the request first
	var reqLen int64
	err = binary.Read(conn, binary.BigEndian, &reqLen)
	fmt.Printf("%v: Read header length: %v err: %v\n", who, reqLen, err)

	// Now read the rest of the structure off the wire.
	fmt.Printf("%v: Try to read request of length: %v\n", who, reqLen)
	buf = make([]byte, reqLen)
	bytesRead, writeErr := io.ReadFull(conn, buf)
	fmt.Printf("%v: Read cnt bytes: %v err: %v\n", who, bytesRead, writeErr)

	if writeErr != nil {
		err = writeErr
		return
	}

	return
}
