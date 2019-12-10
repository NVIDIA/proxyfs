// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/logger"
)

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

// These data structures in effect "redefine" jsonRequest and jsonReply.
//
// This is the only way we can unmarshal Params and Result into the
// appropriate concrete type
type pingJSONReq struct {
	Params [1]jrpcfs.PingReq `json:"params"`
}
type pingJSONReply struct {
	Result [1]jrpcfs.PingReply `json:"result"`
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
