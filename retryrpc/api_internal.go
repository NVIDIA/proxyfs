// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"reflect"

	"github.com/swiftstack/ProxyFS/logger"
)

// PayloadProtocols defines the supported protocols for the payload
type PayloadProtocols int

// Support payload protocols
const (
	JSON PayloadProtocols = 1
)

const (
	currentRetryVersion = 1
)

type methodArgs struct {
	methodPtr *reflect.Method
	request   reflect.Type
	reply     reflect.Type
}

type ioHeader struct {
	Len      uint32
	Protocol uint16
	Version  uint16
}

// Request is the structure sent over the wire
type ioRequest struct {
	Hdr    ioHeader
	Method string // Needed by "read" goroutine to create Reply{}
	JReq   []byte // JSON containing request
}

// Reply is the structure returned over the wire
type ioReply struct {
	Hdr     ioHeader
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

func buildIoRequest(method string, jReq jsonRequest) (ioreq *ioRequest, err error) {
	ioreq = &ioRequest{Method: method} // Will be needed by Read goroutine
	ioreq.JReq, err = json.Marshal(jReq)
	if err != nil {
		return nil, err
	}
	ioreq.Hdr.Len = uint32(len(ioreq.JReq))
	ioreq.Hdr.Protocol = uint16(JSON)
	ioreq.Hdr.Version = currentRetryVersion
	return
}

func setupHdrReply(ioreply *ioReply) {
	ioreply.Hdr.Len = uint32(len(ioreply.JResult))
	ioreply.Hdr.Protocol = uint16(JSON)
	ioreply.Hdr.Version = currentRetryVersion
	return
}

func getIO(conn net.Conn, who string) (buf []byte, err error) {
	if printDebugLogs {
		logger.Infof("conn: %v", conn)
	}

	// Read in the header of the request first
	var hdr ioHeader
	err = binary.Read(conn, binary.BigEndian, &hdr)

	// Now read the rest of the structure off the wire.
	buf = make([]byte, hdr.Len)
	_, writeErr := io.ReadFull(conn, buf)

	if writeErr != nil {
		err = writeErr
		return
	}

	return
}
