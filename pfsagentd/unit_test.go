package main

import (
	"fmt"
	"testing"

	"github.com/swiftstack/ProxyFS/jrpcfs"
)

func TestJrpcMarshaling(t *testing.T) {
	var (
		marshalErr               error
		pingReplyBuf             []byte
		pingReplyInput           *jrpcfs.PingReply
		pingReplyInputRequestID  uint64
		pingReplyOutput          *jrpcfs.PingReply
		pingReplyOutputRequestID uint64
		pingReqBuf               []byte
		pingReqInput             *jrpcfs.PingReq
		pingReqInputRequestID    uint64
		pingReqOutput            *jrpcfs.PingReq
		pingReqOutputRequestID   uint64
		requestMethod            string
		responseErr              error
		unmarshalErr             error
	)

	pingReqInput = &jrpcfs.PingReq{
		Message: "TestMessage",
	}

	pingReqInputRequestID, pingReqBuf, marshalErr = jrpcMarshalRequest("Server.RpcPing", pingReqInput)
	if nil != marshalErr {
		t.Fatalf("jrpcMarshalRequest() failed: %v", marshalErr)
	}

	requestMethod, pingReqOutputRequestID, unmarshalErr = jrpcUnmarshalRequestForMethodAndID(pingReqBuf)
	if nil != unmarshalErr {
		t.Fatalf("jrpcUnmarshalRequestForMethod() failed: %v", unmarshalErr)
	}
	if "Server.RpcPing" != requestMethod {
		t.Fatalf("jrpcUnmarshalRequestForMethod() returned unexpected requestMethod")
	}
	if pingReqInputRequestID != pingReqOutputRequestID {
		t.Fatalf("jrpcUnmarshalRequest() returned unexpected requestID")
	}

	pingReqOutput = &jrpcfs.PingReq{}

	unmarshalErr = jrpcUnmarshalRequest(pingReqOutputRequestID, pingReqBuf, pingReqOutput)
	if nil != unmarshalErr {
		t.Fatalf("jrpcUnmarshalRequest() failed: %v", unmarshalErr)
	}
	if "TestMessage" != pingReqOutput.Message {
		t.Fatalf("jrpcUnmarshalRequest() returned unexpected jrpcfs.PingReq.Message")
	}

	pingReplyInputRequestID = pingReqOutputRequestID

	pingReplyInput = &jrpcfs.PingReply{
		Message: "TestMessage",
	}

	pingReplyBuf, marshalErr = jrpcMarshalResponse(pingReplyInputRequestID, nil, pingReplyInput)
	if nil != marshalErr {
		t.Fatalf("jrpcMarshalResponse(,nil-responseErr,non-nil-response) failed: %v", marshalErr)
	}

	pingReplyOutputRequestID, responseErr, unmarshalErr = jrpcUnmarshalResponseForIDAndError(pingReplyBuf)
	if nil != unmarshalErr {
		t.Fatalf("jrpcUnmarshalResponseForIDAndError() failed: %v", unmarshalErr)
	}
	if nil != responseErr {
		t.Fatalf("jrpcUnmarshalResponseForIDAndError() returned unexpected responseErr")
	}
	if pingReplyInputRequestID != pingReplyOutputRequestID {
		t.Fatalf("jrpcUnmarshalResponseForIDAndError() returned unexpected requestID")
	}

	pingReplyOutput = &jrpcfs.PingReply{}

	unmarshalErr = jrpcUnmarshalResponse(pingReplyOutputRequestID, pingReplyBuf, pingReplyOutput)
	if nil != unmarshalErr {
		t.Fatalf("jrpcUnmarshalResponse() failed: %v", unmarshalErr)
	}
	if "TestMessage" != pingReplyOutput.Message {
		t.Fatalf("jrpcUnmarshalRequest() returned unexpected jrpcfs.PingReply.Message")
	}

	pingReplyBuf, marshalErr = jrpcMarshalResponse(pingReplyInputRequestID, nil, nil)
	if nil != marshalErr {
		t.Fatalf("jrpcMarshalResponse(,nil-responseErr,nil-response) failed: %v", marshalErr)
	}

	pingReplyBuf, marshalErr = jrpcMarshalResponse(pingReplyInputRequestID, fmt.Errorf("TestError"), nil)
	if nil != marshalErr {
		t.Fatalf("jrpcMarshalResponse(,non-nil-responseErr,nil-response) failed: %v", marshalErr)
	}

	pingReplyOutputRequestID, responseErr, unmarshalErr = jrpcUnmarshalResponseForIDAndError(pingReplyBuf)
	if nil != unmarshalErr {
		t.Fatalf("jrpcUnmarshalResponseForIDAndError() failed: %v", unmarshalErr)
	}
	if (nil == responseErr) || ("TestError" != responseErr.Error()) {
		t.Fatalf("jrpcUnmarshalResponseForIDAndError() returned unexpected responseErr")
	}
	if pingReplyInputRequestID != pingReplyOutputRequestID {
		t.Fatalf("jrpcUnmarshalResponseForIDAndError() returned unexpected requestID")
	}

	pingReplyBuf, marshalErr = jrpcMarshalResponse(pingReplyInputRequestID, fmt.Errorf("TestError"), pingReplyOutput)
	if nil != marshalErr {
		t.Fatalf("jrpcMarshalResponse(,non-nil-responseErr,non-nil-response) failed: %v", marshalErr)
	}
}
