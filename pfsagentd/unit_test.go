package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/swiftstack/ProxyFS/jrpcfs"
)

const (
	testMaxAccountNameLength   = 256
	testMaxContainerNameLength = 512
	testMaxObjectNameLength    = 1024
	testAccountListingLimit    = 10000
	testContainerListingLimit  = 20000
)

type testSwiftInfoInnerStruct struct {
	MaxAccountNameLength   uint64 `json:"max_account_name_length"`
	MaxContainerNameLength uint64 `json:"max_container_name_length"`
	MaxObjectNameLength    uint64 `json:"max_object_name_length"`
	AccountListingLimit    uint64 `json:"account_listing_limit"`
	ContainerListingLimit  uint64 `json:"container_listing_limit"`
}

type testSwiftInfoOuterStruct struct {
	Swift testSwiftInfoInnerStruct `json:"swift"`
}

func TestSwiftProxyEmulation(t *testing.T) {
	var (
		authRequest  *http.Request
		authResponse *http.Response
		authURL      string
		err          error
		infoBuf      []byte
		infoResponse *http.Response
		infoURL      string
		swiftInfo    *testSwiftInfoOuterStruct
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

	swiftInfo = &testSwiftInfoOuterStruct{}

	err = json.Unmarshal(infoBuf, swiftInfo)
	if nil != err {
		t.Fatalf("GET /info returned unparseable Body: %v", err)
	}

	if (testMaxAccountNameLength != swiftInfo.Swift.MaxAccountNameLength) ||
		(testMaxContainerNameLength != swiftInfo.Swift.MaxContainerNameLength) ||
		(testMaxObjectNameLength != swiftInfo.Swift.MaxObjectNameLength) ||
		(testAccountListingLimit != swiftInfo.Swift.AccountListingLimit) ||
		(testContainerListingLimit != swiftInfo.Swift.ContainerListingLimit) {
		t.Fatalf("GET /info returned unexpected swiftInfo")
	}

	authURL = strings.Join(append(strings.Split(globals.swiftAccountURL, "/")[:3], "auth", "v1.0"), "/")

	authRequest, err = http.NewRequest("GET", authURL, nil)
	if nil != err {
		t.Fatalf("Creating GET /auth/v1.0 http.Request failed: %v", err)
	}

	authRequest.Header.Add("X-Auth-User", globals.config.SwiftAuthUser)
	authRequest.Header.Add("X-Auth-Key", globals.config.SwiftAuthKey)

	authResponse, err = testSwiftProxyEmulatorGlobals.httpClient.Do(authRequest)
	if nil != err {
		t.Fatalf("GET /auth/v1.0 failed: %v", err)
	}
	if http.StatusOK != authResponse.StatusCode {
		t.Fatalf("GET /auth/v1.0 returned bad status: %v (%v)", authResponse.Status, authResponse.StatusCode)
	}

	err = authResponse.Body.Close()
	if nil != err {
		t.Fatalf("GET /auth/v1.0 returned uncloseable Body: %v", err)
	}

	if testAuthToken != authResponse.Header.Get("X-Auth-Token") {
		t.Fatalf("GET /auth/v1.0 returned incorrect X-Auth-Token")
	}

	if "http://"+testSwiftProxyAddr+"/v1/"+globals.config.SwiftAccountName != authResponse.Header.Get("X-Storage-Url") {
		t.Fatalf("GET /auth/v1.0 returned incorrect X-Storage-Url")
	}

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
