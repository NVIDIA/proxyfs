package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/swiftstack/ProxyFS/jrpcfs"
)

func TestSwiftProxyEmulation(t *testing.T) {
	var (
		authURL      string
		err          error
		infoBuf      []byte
		infoResponse *http.Response
		infoURL      string
	)

	testSetup(t)

	infoURL = strings.Join(append(strings.Split(globals.swiftAccountURL, "/")[:3], "info"), "/")

	infoResponse, err = http.Get(infoURL)
	if nil != err {
		t.Fatalf("GET /info failed: %v", err)
	}
	if http.StatusOK != infoResponse.StatusCode {
		t.Fatalf("GET /info returned bad status: %v (%v)", infoResponse.Status, infoResponse.StatusCode)
	}

	infoBuf, err = ioutil.ReadAll(infoResponse.Body)
	if nil != err {
		t.Fatalf("GET /info returned unreadable Body: %v", err)
	}

	err = infoResponse.Body.Close()
	if nil != err {
		t.Fatalf("GET /info returned uncloseable Body: %v", err)
	}

	t.Logf("GET /info returned: %v", string(infoBuf[:]))

	authURL = strings.Join(append(strings.Split(globals.swiftAccountURL, "/")[:3], "auth", "v1.0"), "/")

	fmt.Println("authURL ==", authURL) // TODO: Exercise AUTH

	// TODO: Exercise PUT of a Container

	// TODO: Exercise PUT of an Object

	// TODO: Exercise Ranged GET of an Object

	// TODO: Exercise JSON RPC Ping

	testTeardown(t)
}

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
