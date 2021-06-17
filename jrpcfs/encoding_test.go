//Allocate response/ Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The encoding tests want to experiment with various ways to encode the same
// RPC requests sent over the wire.  The retryrpc code wraps an RPC request with
// jsonRequest and then prepends ioHeader before sending it over the wire.
// Replies get the same treatment.
//
// Copy jsonRequest, ioRequest, jsonReply, ioReply, and some routines that fill
// them in from the retryrpc code so we can experiment with different ways of
// encoding & decoding them.
type requestID uint64
type MsgType uint16

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

// ioRequest tracks fields written on wire
type ioRequestRetryRpc struct {
	Hdr  ioHeader
	JReq []byte // JSON containing request
}

// ioReply is the structure returned over the wire
type ioReplyRetryRpc struct {
	Hdr     ioHeader
	JResult []byte // JSON containing response
}

type ioHeader struct {
	Len      uint32 // Number of bytes following header
	Protocol uint16
	Version  uint16
	Type     MsgType
	Magic    uint32 // Magic number - if invalid means have not read complete header
}

// Encode a LeaseRequest to binary, json, or some other form.
type encodeLeaseRequestFunc func(leaseReq *LeaseRequest, jreq *jsonRequest) (hdrBytes []byte, jsonBytes []byte)

// Decode a LeaseRequest from binary, json, or some other form.
type decodeLeaseRequestFunc func(hdrBytes []byte, jsonBytes []byte) (leaseReq *LeaseRequest, jreq *jsonRequest)

// Encode a LeaseRequest to binary, json, or some other form.
type encodeLeaseReplyFunc func(leaseReply *LeaseReply, jreply *jsonReply) (hdrBytes []byte, jsonBytes []byte)

// Decode a LeaseRequest from binary, json, or some other form.
type decodeLeaseReplyFunc func(hdrBytes []byte, jsonBytes []byte) (leaseReply *LeaseReply, jreply *jsonReply)

// Test that the request and reply decode functions match the encode functions
func TestEncodeDecodeBinary(t *testing.T) {
	testEncodeDecodeFunctions(t,
		encodeLeaseRequestBinary, encodeLeaseReplyBinary,
		decodeLeaseRequestBinary, decodeLeaseReplyBinary)
}

// Test that the request and reply decode functions match the encode functions
func TestEncodeDecodeJSON(t *testing.T) {
	testEncodeDecodeFunctions(t,
		encodeLeaseRequestJson, encodeLeaseReplyJson,
		decodeLeaseRequestJson, decodeLeaseReplyJson)
}

// Test that the request and reply decode functions match the encode functions
func TestEncodeDecodeGob(t *testing.T) {
	testEncodeDecodeFunctions(t,
		encodeLeaseRequestGob, encodeLeaseReplyGob,
		decodeLeaseRequestGob, decodeLeaseReplyGob)
}

// BenchmarkRpcLeaseEncodeBinary emulates the Lease request/reply RPC encoding
// by building the structures and simply calling the routines that do a binary
// encoding.
func BenchmarkRpcLeaseEncodeBinary(b *testing.B) {
	benchmarkRpcLeaseEncode(b,
		encodeLeaseRequestBinary, encodeLeaseReplyBinary)
}

// BenchmarkRpcLeaseEncodeJSON emulates the Lease request/reply RPC encoding
// by building the structures and simply calling the routines that do a json
// encoding.
func BenchmarkRpcLeaseEncodeJSON(b *testing.B) {
	benchmarkRpcLeaseEncode(b,
		encodeLeaseRequestJson, encodeLeaseReplyJson)
}

// BenchmarkRpcLeaseEncodeGob emulates the Lease request/reply RPC encoding
// by building the structures and simply calling the routines that do a gob
// encoding.
func BenchmarkRpcLeaseEncodeGob(b *testing.B) {
	benchmarkRpcLeaseEncode(b,
		encodeLeaseRequestGob, encodeLeaseReplyGob)
}

// BenchmarkRpcLeaseDecodeBinary emulates the Lease request/reply RPC encoding
// by building the structures and simply calling the routines that do a binary
// encoding.
func BenchmarkRpcLeaseDecodeBinary(b *testing.B) {
	benchmarkRpcLeaseDecode(b,
		encodeLeaseRequestJson, encodeLeaseReplyJson,
		decodeLeaseRequestJson, decodeLeaseReplyJson)
}

// BenchmarkRpcLeaseDecodeJSON emulates the Lease request/reply RPC encoding
// by building the structures and simply calling the routines that do a binary
// encoding.
func BenchmarkRpcLeaseDecodeJSON(b *testing.B) {
	benchmarkRpcLeaseDecode(b,
		encodeLeaseRequestJson, encodeLeaseReplyJson,
		decodeLeaseRequestJson, decodeLeaseReplyJson)
}

// BenchmarkRpcLeaseDecodeGob emulates the Lease request/reply RPC encoding
// by building the structures and simply calling the routines that do a binary
// encoding.
func BenchmarkRpcLeaseDecodeGob(b *testing.B) {
	benchmarkRpcLeaseDecode(b,
		encodeLeaseRequestGob, encodeLeaseReplyGob,
		decodeLeaseRequestGob, decodeLeaseReplyGob)
}

// testEncodeDecodeFunctions tests "generic" encoding and decoding functions for
// LeaseRequests to see if the decoded requests match what was encoded.
func testEncodeDecodeFunctions(t *testing.T,
	encodeLeaseRequest encodeLeaseRequestFunc,
	encodeLeaseReply encodeLeaseReplyFunc,
	decodeLeaseRequest decodeLeaseRequestFunc,
	decodeLeaseReply decodeLeaseReplyFunc) {

	assert := assert.New(t)

	// encode a lease request and then decode it
	mountByAccountNameReply := &MountByAccountNameReply{
		MountID: "66",
	}
	leaseRequest := &LeaseRequest{
		InodeHandle: InodeHandle{
			MountID:     mountByAccountNameReply.MountID,
			InodeNumber: testRpcLeaseSingleInodeNumber,
		},
		LeaseRequestType: LeaseRequestTypeExclusive,
	}
	jsonRequest := newJsonRequest("RpcLease")

	// this marshals both the RPCrequest and the header for the request
	leaseRequestHdrBuf, leaseRequestPayloadBuf :=
		encodeLeaseRequest(leaseRequest, jsonRequest)

	leaseRequest2, jsonRequest2 :=
		decodeLeaseRequest(leaseRequestHdrBuf, leaseRequestPayloadBuf)

	assert.Equal(leaseRequest, leaseRequest2, "Decoded struct should match the original")
	assert.Equal(jsonRequest, jsonRequest2, "Decoded struct should match the original")

	// try again with release lease request
	leaseRequest.LeaseRequestType = LeaseRequestTypeRelease

	leaseRequestHdrBuf, leaseRequestPayloadBuf =
		encodeLeaseRequest(leaseRequest, jsonRequest)

	leaseRequest2, jsonRequest2 =
		decodeLeaseRequest(leaseRequestHdrBuf, leaseRequestPayloadBuf)

	assert.Equal(leaseRequest, leaseRequest2, "Decoded struct should match the original")
	assert.Equal(jsonRequest, jsonRequest2, "Decoded struct should match the original")

	// encode reply and decode it
	leaseReply := &LeaseReply{
		LeaseReplyType: LeaseReplyTypeExclusive,
	}
	jsonReply := newJsonReply()
	leaseReplyHdrBuf, leaseReplyPayloadBuf :=
		encodeLeaseReply(leaseReply, jsonReply)

	leaseReply2, jsonReply2 :=
		decodeLeaseReply(leaseReplyHdrBuf, leaseReplyPayloadBuf)

	assert.Equal(leaseReply, leaseReply2, "Decoded struct should match the original")
	assert.Equal(jsonReply, jsonReply2, "Decoded struct should match the original")

	// try again with release lease reply
	leaseReply.LeaseReplyType = LeaseReplyTypeReleased

	leaseReplyHdrBuf, leaseReplyPayloadBuf =
		encodeLeaseReply(leaseReply, jsonReply)

	leaseReply2, jsonReply2 =
		decodeLeaseReply(leaseReplyHdrBuf, leaseReplyPayloadBuf)

	assert.Equal(leaseReply, leaseReply2, "Decoded struct should match the original")
	assert.Equal(jsonReply, jsonReply2, "Decoded struct should match the original")
}

// benchmarkRpcLeaseEncode is the guts to benchmark the cost of encoding the LeaseRequest
// and LeaseReply parts of a LeaseRequest RPC.
func benchmarkRpcLeaseEncode(b *testing.B,
	encodeLeaseRequest encodeLeaseRequestFunc,
	encodeLeaseReply encodeLeaseReplyFunc) {

	var (
		benchmarkIteration      int
		leaseReply              *LeaseReply
		mountByAccountNameReply *MountByAccountNameReply
	)

	mountByAccountNameReply = &MountByAccountNameReply{
		MountID: "66",
	}

	b.ResetTimer()

	for benchmarkIteration = 0; benchmarkIteration < b.N; benchmarkIteration++ {
		leaseRequest := &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeExclusive,
		}

		// this marshals both the RPCrequest and the header for the request
		hdrBuf, requestBuf := encodeLeaseRequest(leaseRequest, newJsonRequest("RpcLease"))
		_ = hdrBuf
		_ = requestBuf

		leaseReply = &LeaseReply{
			LeaseReplyType: LeaseReplyTypeExclusive,
		}

		// marshal the RPC reply
		hdrBuf, replyBuf := encodeLeaseReply(leaseReply, newJsonReply())
		_ = hdrBuf
		_ = replyBuf

		// "validate" the returned value
		if LeaseReplyTypeExclusive != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseRequestTypeExclusive", leaseReply.LeaseReplyType)
		}

		// release the lock
		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeRelease,
		}

		hdrBuf, requestBuf = encodeLeaseRequest(leaseRequest, newJsonRequest("RpcLease"))
		_ = hdrBuf
		_ = requestBuf

		leaseReply = &LeaseReply{
			LeaseReplyType: LeaseReplyTypeReleased,
		}

		hdrBuf, replyBuf = encodeLeaseReply(leaseReply, newJsonReply())
		_ = hdrBuf
		_ = replyBuf

		// "validate" the return value
		if LeaseReplyTypeReleased != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseReplyTypeReleased", leaseReply.LeaseReplyType)
		}
	}

	b.StopTimer()
}

// benchmarkRpcLeaseEncode is the guts to benchmark the cost of decoding the
// LeaseRequest and LeaseReply parts of a LeaseRequest RPC.
func benchmarkRpcLeaseDecode(b *testing.B,
	encodeLeaseRequest encodeLeaseRequestFunc,
	encodeLeaseReply encodeLeaseReplyFunc,
	decodeLeaseRequest decodeLeaseRequestFunc,
	decodeLeaseReply decodeLeaseReplyFunc) {

	var (
		benchmarkIteration      int
		leaseReply              *LeaseReply
		mountByAccountNameReply *MountByAccountNameReply
	)

	mountByAccountNameReply = &MountByAccountNameReply{
		MountID: "6",
	}

	// build the over-the-wire buffers that will be decoded later
	//
	// get lease request first
	leaseRequest := &LeaseRequest{
		InodeHandle: InodeHandle{
			MountID:     mountByAccountNameReply.MountID,
			InodeNumber: testRpcLeaseSingleInodeNumber,
		},
		LeaseRequestType: LeaseRequestTypeExclusive,
	}

	// this marshals both the RPCrequest and the header for the request
	getLeaseRequestHdrBuf, getLeaseRequestPayloadBuf :=
		encodeLeaseRequest(leaseRequest, newJsonRequest("RpcLease"))

	// release lease request
	leaseRequest.LeaseRequestType = LeaseRequestTypeRelease
	releaseLeaseRequestHdrBuf, releaseLeaseRequestPayloadBuf :=
		encodeLeaseRequest(leaseRequest, newJsonRequest("RpcLease"))

	// now build the replies
	// get lease reply
	leaseReply = &LeaseReply{
		LeaseReplyType: LeaseReplyTypeExclusive,
	}
	getLeaseReplyHdrBuf, getLeaseReplyPayloadBuf :=
		encodeLeaseReply(leaseReply, newJsonReply())

	// release lease reply
	leaseReply.LeaseReplyType = LeaseReplyTypeReleased
	releaseLeaseReplyHdrBuf, releaseLeaseReplyPayloadBuf :=
		encodeLeaseReply(leaseReply, newJsonReply())

	b.ResetTimer()

	for benchmarkIteration = 0; benchmarkIteration < b.N; benchmarkIteration++ {

		leaseRequest, jRequest :=
			decodeLeaseRequest(getLeaseRequestHdrBuf, getLeaseRequestPayloadBuf)
		_ = leaseRequest
		_ = jRequest

		leaseReply, jReply :=
			decodeLeaseReply(getLeaseReplyHdrBuf, getLeaseReplyPayloadBuf)
		_ = jReply

		// "validate" the returned value
		if LeaseReplyTypeExclusive != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseRequestTypeExclusive",
				leaseReply.LeaseReplyType)
		}

		// release the lock
		leaseRequest, jRequest =
			decodeLeaseRequest(releaseLeaseRequestHdrBuf, releaseLeaseRequestPayloadBuf)

		leaseReply, jReply =
			decodeLeaseReply(releaseLeaseReplyHdrBuf, releaseLeaseReplyPayloadBuf)

		// "validate" the return value
		if LeaseReplyTypeReleased != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseReplyTypeReleased",
				leaseReply.LeaseReplyType)
		}
	}

	b.StopTimer()
}

func newJsonRequest(method string) (jreq *jsonRequest) {
	jreq = &jsonRequest{
		MyUniqueID:       77,
		RequestID:        88,
		HighestReplySeen: 33,
		Method:           method,
	}

	return
}

func newJsonReply() (jreply *jsonReply) {
	jreply = &jsonReply{
		MyUniqueID: 77,
		RequestID:  88,
		ErrStr:     "",
	}

	return
}

// Perform all of the encoding required by the client to send a LeaseRequest.
// This includes encoding the LeaseRequest, the jsonRequest "wrapper", and the
// ioRequest header (which is always a fixed binary encoding).
//
// The encoding of the jsonRequest and LeaseRequest also use a hand-written
// binary encoding.
func encodeLeaseRequestBinary(leaseReq *LeaseRequest, jreq *jsonRequest) (hdrBytes []byte, jsonBytes []byte) {
	var err error

	hdrBuf := &bytes.Buffer{}
	jsonBuf := &bytes.Buffer{}

	// marshal jsonRequest fields
	err = binary.Write(jsonBuf, binary.LittleEndian, jreq.MyUniqueID)
	if err != nil {
		panic("jreq.MyUniqueID")
	}
	err = binary.Write(jsonBuf, binary.LittleEndian, jreq.RequestID)
	if err != nil {
		panic("jreq.RequestID")
	}
	err = binary.Write(jsonBuf, binary.LittleEndian, jreq.HighestReplySeen)
	if err != nil {
		panic("jreq.HighestReplySeen")
	}
	// use a Nul terminated string
	methodWithNul := jreq.Method + string([]byte{0})
	_, err = jsonBuf.WriteString(methodWithNul)
	if err != nil {
		panic("jreq.Method")
	}

	// marshal LeaseRequest fields
	mountIDWithNul := leaseReq.MountID + MountIDAsString([]byte{0})
	_, err = jsonBuf.WriteString(string(mountIDWithNul))
	if err != nil {
		panic("leaseReq.MountID")
	}
	err = binary.Write(jsonBuf, binary.LittleEndian, leaseReq.InodeNumber)
	if err != nil {
		panic("leaseReq.LeaseInodeNumber")
	}
	err = binary.Write(jsonBuf, binary.LittleEndian, leaseReq.LeaseRequestType)
	if err != nil {
		panic("leaseReq.LeaseRequestType")
	}

	// now create the IoRequest header and Marshal it
	ioReq := ioRequestRetryRpc{
		Hdr: ioHeader{
			Len:      uint32(jsonBuf.Len()),
			Protocol: uint16(1),
			Version:  1,
			Type:     1,
			Magic:    0xCAFEFEED,
		},
	}

	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Len)
	if err != nil {
		panic("ioReq.Hdr.Len")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Protocol)
	if err != nil {
		panic("ioReq.Hdr.Protocol")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Version)
	if err != nil {
		panic("ioReq.Hdr.Version")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Type)
	if err != nil {
		panic("ioReq.Hdr.Type")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Magic)
	if err != nil {
		panic("ioReq.Hdr.Magic")
	}

	hdrBytes = hdrBuf.Bytes()
	jsonBytes = jsonBuf.Bytes()
	return
}

// Perform all of the encoding required on the server to send a LeaseReply.
// This includes encoding the LeaseReply, the jsonReply "wrapper", and the
// ioReply header (which is always a fixed binary encoding).
//
// The encoding of the jsonReply and LeaseReply also use a hand-written binary
// encoding.
func encodeLeaseReplyBinary(leaseReply *LeaseReply, jreply *jsonReply) (hdrBytes []byte, jsonBytes []byte) {
	var err error

	hdrBuf := &bytes.Buffer{}
	jsonBuf := &bytes.Buffer{}

	// marshal jsonReply fields
	err = binary.Write(jsonBuf, binary.LittleEndian, jreply.MyUniqueID)
	if err != nil {
		panic("jreply.MyUniqueID")
	}
	err = binary.Write(jsonBuf, binary.LittleEndian, jreply.RequestID)
	if err != nil {
		panic("jreply.RequestID")
	}
	// use a Nul termianted string
	errStrWithNul := jreply.ErrStr + string([]byte{0})
	_, err = jsonBuf.WriteString(errStrWithNul)
	if err != nil {
		panic("jreply.ErrStr")
	}

	// marshal LeaseReply fields
	err = binary.Write(jsonBuf, binary.LittleEndian, leaseReply.LeaseReplyType)
	if err != nil {
		panic("leaseReply.LeaseReplyType")
	}

	// now create the IoReply header and Marshal it
	ioReply := ioReplyRetryRpc{
		Hdr: ioHeader{
			Len:      uint32(jsonBuf.Len()),
			Protocol: uint16(1),
			Version:  1,
			Type:     1,
			Magic:    0xCAFEFEED,
		},
	}

	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Len)
	if err != nil {
		panic("ioReply.Hdr.Len")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Protocol)
	if err != nil {
		panic("ioReply.Hdr.Protocol")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Version)
	if err != nil {
		panic("ioReply.Hdr.Version")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Type)
	if err != nil {
		panic("ioReply.Hdr.Type")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Magic)
	if err != nil {
		panic("ioReply.Hdr.Magic")
	}

	hdrBytes = hdrBuf.Bytes()
	jsonBytes = jsonBuf.Bytes()
	return
}

// Perform all of the decoding required on the server to recieve a LeaseRequest.
// This includes decoding the LeaseRequest, the jsonRequest "wrapper", and the
// ioRequest header (which is always a fixed binary encoding).
//
// The decoding of the jsonRequest and LeaseRequest use a hand-written binary
// decoder.
func decodeLeaseRequestBinary(hdrBytes []byte, jsonBytes []byte) (leaseReq *LeaseRequest, jreq *jsonRequest) {
	var err error

	leaseReq = &LeaseRequest{}
	jreq = &jsonRequest{}
	ioReq := &ioRequestRetryRpc{}

	hdrBuf := bytes.NewBuffer(hdrBytes)
	jsonBuf := bytes.NewBuffer(jsonBytes)

	// Unmarshal the IoRequest header
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Len)
	if err != nil {
		panic("ioReq.Hdr.Len")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Protocol)
	if err != nil {
		panic("ioReq.Hdr.Protocol")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Version)
	if err != nil {
		panic("ioReq.Hdr.Version")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Type)
	if err != nil {
		panic("ioReq.Hdr.Type")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Magic)
	if err != nil {
		panic("ioReq.Hdr.Magic")
	}

	// now unmarshal the jsonRequest fields
	err = binary.Read(jsonBuf, binary.LittleEndian, &jreq.MyUniqueID)
	if err != nil {
		panic("jreq.MyUniqueID")
	}
	err = binary.Read(jsonBuf, binary.LittleEndian, &jreq.RequestID)
	if err != nil {
		panic("jreq.RequestID")
	}
	err = binary.Read(jsonBuf, binary.LittleEndian, &jreq.HighestReplySeen)
	if err != nil {
		panic("jreq.HighestReplySeen")
	}
	// method is Nul terminated
	method, err := jsonBuf.ReadString(0)
	if err != nil {
		panic(fmt.Sprintf("jreq.Method '%s' buf '%+v'  err: %v", jreq.Method, jsonBuf, err))
	}
	jreq.Method = method[0 : len(method)-1]

	// unmarshal LeaseRequest fields (MountID is Nul terminated)
	mountID, err := jsonBuf.ReadString(0)
	if err != nil {
		panic("leaseReq.MountID")
	}
	leaseReq.MountID = MountIDAsString(mountID[0 : len(mountID)-1])
	err = binary.Read(jsonBuf, binary.LittleEndian, &leaseReq.InodeNumber)
	if err != nil {
		panic("leaseReq.LeaseInodeNumber")
	}
	err = binary.Read(jsonBuf, binary.LittleEndian, &leaseReq.LeaseRequestType)
	if err != nil {
		panic("leaseReq.LeaseRequestType")
	}

	hdrBytes = hdrBuf.Bytes()
	jsonBytes = jsonBuf.Bytes()
	return
}

// Perform all of the decoding required on the server to recieve a LeaseReply.
// This includes decoding the LeaseReply, the jsonReply "wrapper", and the
// ioReply header (which is always a fixed binary encoding).
//
// The decoding of the jsonReply and LeaseRequest are also a hand-written binary
// decoder.
func decodeLeaseReplyBinary(hdrBytes []byte, jsonBytes []byte) (leaseReply *LeaseReply, jreply *jsonReply) {
	var err error

	leaseReply = &LeaseReply{}
	jreply = &jsonReply{}
	ioReply := &ioReplyRetryRpc{}

	hdrBuf := bytes.NewBuffer(hdrBytes)
	jsonBuf := bytes.NewBuffer(jsonBytes)

	// first unmarshal the IoReply
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Len)
	if err != nil {
		panic("ioReply.Hdr.Len")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Protocol)
	if err != nil {
		panic("ioReply.Hdr.Protocol")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Version)
	if err != nil {
		panic("ioReply.Hdr.Version")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Type)
	if err != nil {
		panic("ioReply.Hdr.Type")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Magic)
	if err != nil {
		panic("ioReply.Hdr.Magic")
	}

	// now unmarshal jsonReply fields
	err = binary.Read(jsonBuf, binary.LittleEndian, &jreply.MyUniqueID)
	if err != nil {
		panic("jreply.MyUniqueID")
	}
	err = binary.Read(jsonBuf, binary.LittleEndian, &jreply.RequestID)
	if err != nil {
		panic("jreply.RequestID")
	}
	// ErrStr is Nul terminated
	errStr, err := jsonBuf.ReadString(0)
	if err != nil {
		panic("jreply.ErrStr")
	}
	jreply.ErrStr = errStr[0 : len(errStr)-1]

	// unmarshal LeaseReply fields
	err = binary.Read(jsonBuf, binary.LittleEndian, &leaseReply.LeaseReplyType)
	if err != nil {
		panic("leaseReply.LeaseReplyType")
	}

	hdrBytes = hdrBuf.Bytes()
	jsonBytes = jsonBuf.Bytes()
	return
}

// Perform all of the encoding required by the client to send a LeaseRequest.
// This includes encoding the LeaseRequest, the jsonRequest "wrapper", and the
// ioRequest header (which is always a fixed binary encoding).
//
// The encoding of the jsonRequest and LeaseRequest uses json.Marshal().
func encodeLeaseRequestJson(leaseReq *LeaseRequest, jreq *jsonRequest) (hdrBytes []byte, jsonBytes []byte) {
	var err error

	hdrBuf := &bytes.Buffer{}

	// the Lease Request is part of the JSON request
	jreq.Params[0] = leaseReq

	// marshal the json request (and lease request)
	jsonBytes, err = json.Marshal(jreq)
	if err != nil {
		panic("json.Marshal")
	}

	// now create the IoRequest header and Marshal it
	// (this is always binary)
	ioReq := ioRequestRetryRpc{
		Hdr: ioHeader{
			Len:      uint32(len(jsonBytes)),
			Protocol: uint16(1),
			Version:  1,
			Type:     1,
			Magic:    0xCAFEFEED,
		},
	}

	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Len)
	if err != nil {
		panic("ioReq.Hdr.Len")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Protocol)
	if err != nil {
		panic("ioReq.Hdr.Protocol")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Version)
	if err != nil {
		panic("ioReq.Hdr.Version")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Type)
	if err != nil {
		panic("ioReq.Hdr.Type")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Magic)
	if err != nil {
		panic("ioReq.Hdr.Magic")
	}

	hdrBytes = hdrBuf.Bytes()
	return
}

// Perform all of the decoding required on the server to recieve a LeaseRequest.
// This includes decoding the LeaseRequest, the jsonRequest "wrapper", and the
// ioRequest header (which is always a fixed binary encoding).
//
// The decoding of the jsonRequest and LeaseRequest use json.Unmarshal().
func decodeLeaseRequestJson(hdrBytes []byte, jsonBytes []byte) (leaseReq *LeaseRequest, jreq *jsonRequest) {
	var err error

	leaseReq = &LeaseRequest{}
	jreq = &jsonRequest{}
	jreq.Params[0] = leaseReq
	ioReq := &ioRequestRetryRpc{}

	hdrBuf := bytes.NewBuffer(hdrBytes)

	// Unmarshal the IoRequest header (always binary)
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Len)
	if err != nil {
		panic("ioReq.Hdr.Len")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Protocol)
	if err != nil {
		panic("ioReq.Hdr.Protocol")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Version)
	if err != nil {
		panic("ioReq.Hdr.Version")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Type)
	if err != nil {
		panic("ioReq.Hdr.Type")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Magic)
	if err != nil {
		panic("ioReq.Hdr.Magic")
	}

	// now unmarshal the jsonRequest fields
	err = json.Unmarshal(jsonBytes, jreq)
	if err != nil {
		panic("json.Unmarshal of json")
	}

	return
}

// Perform all of the encoding required on the server to send a LeaseReply.
// This includes encoding the LeaseReply, the jsonReply "wrapper", and the
// ioReply header (which is always a fixed binary encoding).
//
// The encoding of the jsonReply and LeaseReply use json.Marshal().
func encodeLeaseReplyJson(leaseReply *LeaseReply, jreply *jsonReply) (hdrBytes []byte, jsonBytes []byte) {
	var err error

	hdrBuf := &bytes.Buffer{}

	// the Lease Reply is part of the JSON reply
	jreply.Result = leaseReply

	// marshal the json reply (and lease reply)
	jsonBytes, err = json.Marshal(jreply)
	if err != nil {
		panic("json.Marshal")
	}

	// now create the IoReply header and Marshal it
	// (this is always binary)
	ioReply := ioReplyRetryRpc{
		Hdr: ioHeader{
			Len:      uint32(len(jsonBytes)),
			Protocol: uint16(1),
			Version:  1,
			Type:     1,
			Magic:    0xCAFEFEED,
		},
	}

	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Len)
	if err != nil {
		panic("ioReply.Hdr.Len")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Protocol)
	if err != nil {
		panic("ioReply.Hdr.Protocol")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Version)
	if err != nil {
		panic("ioReply.Hdr.Version")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Type)
	if err != nil {
		panic("ioReply.Hdr.Type")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Magic)
	if err != nil {
		panic("ioReply.Hdr.Magic")
	}

	hdrBytes = hdrBuf.Bytes()
	return
}

// Perform all of the decoding required on the server to recieve a LeaseReply.
// This includes decoding the LeaseReply, the jsonReply "wrapper", and the
// ioReply header (which is always a fixed binary encoding).
//
// The decoding of the jsonReply and LeaseReply use a json.Unmarshal().
func decodeLeaseReplyJson(hdrBytes []byte, jsonBytes []byte) (leaseReply *LeaseReply, jreply *jsonReply) {
	var err error

	leaseReply = &LeaseReply{}
	jreply = &jsonReply{}
	jreply.Result = leaseReply
	ioReply := &ioReplyRetryRpc{}

	hdrBuf := bytes.NewBuffer(hdrBytes)

	// Unmarshal the IoReply header (always binary)
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Len)
	if err != nil {
		panic("ioReply.Hdr.Len")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Protocol)
	if err != nil {
		panic("ioReply.Hdr.Protocol")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Version)
	if err != nil {
		panic("ioReply.Hdr.Version")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Type)
	if err != nil {
		panic("ioReply.Hdr.Type")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Magic)
	if err != nil {
		panic("ioReply.Hdr.Magic")
	}

	// now unmarshal the jsonReply fields
	err = json.Unmarshal(jsonBytes, jreply)
	if err != nil {
		panic("json.Unmarshal of json")
	}

	leaseReply = jreply.Result.(*LeaseReply)
	return
}

// Create gob LeaseRequest and LeaseReply encoders and decoders that are
// globally available.
var (
	encodeLeaseRequestGobBuffer  *bytes.Buffer = &bytes.Buffer{}
	decodeLeaseRequestGobBuffer  *bytes.Buffer = &bytes.Buffer{}
	encodeLeaseRequestGobEncoder *gob.Encoder  = gob.NewEncoder(encodeLeaseRequestGobBuffer)
	decodeLeaseRequestGobDecoder *gob.Decoder  = gob.NewDecoder(decodeLeaseRequestGobBuffer)

	encodeLeaseReplyGobBuffer  *bytes.Buffer = &bytes.Buffer{}
	decodeLeaseReplyGobBuffer  *bytes.Buffer = &bytes.Buffer{}
	encodeLeaseReplyGobEncoder *gob.Encoder  = gob.NewEncoder(encodeLeaseReplyGobBuffer)
	decodeLeaseReplyGobDecoder *gob.Decoder  = gob.NewDecoder(decodeLeaseReplyGobBuffer)
)

// Perform all of the encoding required by the client to send a LeaseRequest.
// This includes encoding the LeaseRequest, the jsonRequest "wrapper", and the
// ioRequest header (which is always a fixed binary encoding).
//
// The encoding of the jsonRequest and LeaseRequest use a pre-defined gob
// encoding.
func encodeLeaseRequestGob(leaseReq *LeaseRequest, jreq *jsonRequest) (hdrBytes []byte, gobBytes []byte) {
	var err error

	hdrBuf := &bytes.Buffer{}

	// the Lease Request is part of the gob request
	jreq.Params[0] = leaseReq

	// marshal jreq (and lease request)
	err = encodeLeaseRequestGobEncoder.Encode(jreq)
	if err != nil {
		panic("encodeLeaseRequestGobEncoder")
	}

	// consume the results encoded in the (global) buffer
	gobBytes = make([]byte, encodeLeaseRequestGobBuffer.Len())
	n, err := encodeLeaseRequestGobBuffer.Read(gobBytes)
	if n != cap(gobBytes) {
		panic("didn't read enough bytes")
	}

	// now create the IoRequest header and Marshal it
	// (this is always binary)
	ioReq := ioRequestRetryRpc{
		Hdr: ioHeader{
			Len:      uint32(len(gobBytes)),
			Protocol: uint16(1),
			Version:  1,
			Type:     1,
			Magic:    0xCAFEFEED,
		},
	}

	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Len)
	if err != nil {
		panic("ioReq.Hdr.Len")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Protocol)
	if err != nil {
		panic("ioReq.Hdr.Protocol")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Version)
	if err != nil {
		panic("ioReq.Hdr.Version")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Type)
	if err != nil {
		panic("ioReq.Hdr.Type")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReq.Hdr.Magic)
	if err != nil {
		panic("ioReq.Hdr.Magic")
	}

	hdrBytes = hdrBuf.Bytes()
	return
}

// Perform all of the decoding required on the server to receive a LeaseRequest.
// This includes decoding the LeaseRequest, the jsonRequest "wrapper", and the
// ioRequest header (which is always a fixed binary encoding).
//
// The decoding of the jsonRequest and LeaseRequest use a pre-defined gob decoder.
func decodeLeaseRequestGob(hdrBytes []byte, gobBytes []byte) (leaseReq *LeaseRequest, jreq *jsonRequest) {
	var err error

	jreq = &jsonRequest{}
	ioReq := &ioRequestRetryRpc{}

	hdrBuf := bytes.NewBuffer(hdrBytes)

	// Unmarshal the IoRequest header (always binary)
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Len)
	if err != nil {
		panic("ioReq.Hdr.Len")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Protocol)
	if err != nil {
		panic("ioReq.Hdr.Protocol")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Version)
	if err != nil {
		panic("ioReq.Hdr.Version")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Type)
	if err != nil {
		panic("ioReq.Hdr.Type")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReq.Hdr.Magic)
	if err != nil {
		panic("ioReq.Hdr.Magic")
	}

	// now unmarshal the jsonRequest fields using gob (can't fail)
	_, _ = decodeLeaseRequestGobBuffer.Write(gobBytes)
	err = decodeLeaseRequestGobDecoder.Decode(jreq)
	if err != nil {
		panic("decodeLeaseRequestGobDecoder.Decode")
	}
	leaseReq = jreq.Params[0].(*LeaseRequest)

	return
}

// Perform all of the encoding required on the server to send a LeaseReply.
// This includes encoding the LeaseReply, the jsonReply "wrapper", and the
// ioReply header (which is always a fixed binary encoding).
//
// The encoding of the jsonReply and LeaseReply use a pre-defined gob encoder.
func encodeLeaseReplyGob(leaseReply *LeaseReply, jreply *jsonReply) (hdrBytes []byte, gobBytes []byte) {
	var err error

	hdrBuf := &bytes.Buffer{}

	// the Lease Reply is part of the JSON reply
	jreply.Result = leaseReply

	// marshal jreq (and lease request)
	err = encodeLeaseReplyGobEncoder.Encode(jreply)
	if err != nil {
		panic("encodeLeaseReplyGobEncoder")
	}

	// consume the results encoded in the (global) buffer
	gobBytes = make([]byte, encodeLeaseReplyGobBuffer.Len())
	n, err := encodeLeaseReplyGobBuffer.Read(gobBytes)
	if n != cap(gobBytes) {
		panic("didn't read enough bytes")
	}

	// now create the IoReply header and Marshal it
	// (this is always binary)
	ioReply := ioReplyRetryRpc{
		Hdr: ioHeader{
			Len:      uint32(len(gobBytes)),
			Protocol: uint16(1),
			Version:  1,
			Type:     1,
			Magic:    0xCAFEFEED,
		},
	}

	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Len)
	if err != nil {
		panic("ioReply.Hdr.Len")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Protocol)
	if err != nil {
		panic("ioReply.Hdr.Protocol")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Version)
	if err != nil {
		panic("ioReply.Hdr.Version")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Type)
	if err != nil {
		panic("ioReply.Hdr.Type")
	}
	err = binary.Write(hdrBuf, binary.LittleEndian, ioReply.Hdr.Magic)
	if err != nil {
		panic("ioReply.Hdr.Magic")
	}

	hdrBytes = hdrBuf.Bytes()
	return
}

// Perform all of the decoding required on the server to recieve a LeaseReply.
// This includes decoding the LeaseReply, the jsonReply "wrapper", and the
// ioReply header (which is always a fixed binary encoding).
//
// The decoding of the jsonReply and LeaseReply use a pre-defined gob decoder.
func decodeLeaseReplyGob(hdrBytes []byte, gobBytes []byte) (leaseReply *LeaseReply, jreply *jsonReply) {
	var err error

	leaseReply = &LeaseReply{}
	jreply = &jsonReply{}
	jreply.Result = leaseReply
	ioReply := &ioReplyRetryRpc{}

	hdrBuf := bytes.NewBuffer(hdrBytes)

	// Unmarshal the IoReply header (always binary)
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Len)
	if err != nil {
		panic("ioReply.Hdr.Len")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Protocol)
	if err != nil {
		panic("ioReply.Hdr.Protocol")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Version)
	if err != nil {
		panic("ioReply.Hdr.Version")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Type)
	if err != nil {
		panic("ioReply.Hdr.Type")
	}
	err = binary.Read(hdrBuf, binary.LittleEndian, &ioReply.Hdr.Magic)
	if err != nil {
		panic("ioReply.Hdr.Magic")
	}

	// now unmarshal the jsonReply fields using gob (can't fail)
	_, _ = decodeLeaseReplyGobBuffer.Write(gobBytes)
	err = decodeLeaseReplyGobDecoder.Decode(jreply)
	if err != nil {
		panic("decodeLeaseReplyGobDecoder.Decode")
	}
	leaseReply = jreply.Result.(*LeaseReply)

	return
}
