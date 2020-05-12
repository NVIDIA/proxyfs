package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

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

func TestJRPCRequest(t *testing.T) {
	var (
		err       error
		pingReply *jrpcfs.PingReply
		pingReq   *jrpcfs.PingReq
	)

	testSetup(t)

	pingReq = &jrpcfs.PingReq{
		Message: "TestMessage",
	}

	pingReply = &jrpcfs.PingReply{}

	err = doJRPCRequest("Server.RpcPing", pingReq, pingReply)

	if nil != err {
		t.Fatalf("doJRPCRequest(\"Server.RpcPing\",,) failed: %v", err)
	}
	if fmt.Sprintf("pong %d bytes", len("TestMessage")) != pingReply.Message {
		t.Fatalf("doJRPCRequest(\"Server.RpcPing\",,) returned unexpected response: %s", pingReply.Message)
	}

	testTeardown(t)
}

func TestSwiftProxyEmulation(t *testing.T) {
	var (
		authRequest          *http.Request
		authResponse         *http.Response
		authURL              string
		err                  error
		getBuf               []byte
		getObjectRequest     *http.Request
		getObjectResponse    *http.Response
		infoBuf              []byte
		infoResponse         *http.Response
		infoURL              string
		pingReq              *jrpcfs.PingReq
		pingReqBuf           []byte
		pingReqMessageID     uint64
		pingRequest          *http.Request
		pingReply            *jrpcfs.PingReply
		pingReplyBuf         []byte
		pingReplyMessageID   uint64
		pingResponse         *http.Response
		putContainerRequest  *http.Request
		putContainerResponse *http.Response
		putObjectRequest     *http.Request
		putObjectResponse    *http.Response
		responseErr          error
		swiftAccountURLSplit []string
		swiftInfo            *testSwiftInfoOuterStruct
		unmarshalErr         error
	)

	testSetup(t)

	for {
		swiftAccountURLSplit = strings.Split(globals.swiftStorageURL, "/")
		if len(swiftAccountURLSplit) >= 3 {
			infoURL = strings.Join(append(strings.Split(globals.swiftStorageURL, "/")[:3], "info"), "/")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	infoResponse, err = http.Get(infoURL)
	if nil != err {
		t.Fatalf("GET /info failed: %v", err)
	}
	if http.StatusOK != infoResponse.StatusCode {
		t.Fatalf("GET /info returned bad status: %v", infoResponse.Status)
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

	authURL = strings.Join(append(strings.Split(globals.swiftStorageURL, "/")[:3], "auth", "v1.0"), "/")

	authRequest, err = http.NewRequest("GET", authURL, nil)
	if nil != err {
		t.Fatalf("Creating GET /auth/v1.0 http.Request failed: %v", err)
	}

	authRequest.Header.Add("X-Auth-User", testAuthUser)
	authRequest.Header.Add("X-Auth-Key", testAuthKey)

	authResponse, err = testSwiftProxyEmulatorGlobals.httpClient.Do(authRequest)
	if nil != err {
		t.Fatalf("GET /auth/v1.0 failed: %v", err)
	}
	if http.StatusOK != authResponse.StatusCode {
		t.Fatalf("GET /auth/v1.0 returned bad status: %v", authResponse.Status)
	}

	err = authResponse.Body.Close()
	if nil != err {
		t.Fatalf("GET /auth/v1.0 returned uncloseable Body: %v", err)
	}

	if testAuthToken != authResponse.Header.Get("X-Auth-Token") {
		t.Fatalf("GET /auth/v1.0 returned incorrect X-Auth-Token")
	}

	if "http://"+testSwiftProxyAddr+"/v1/"+testAccountName != authResponse.Header.Get("X-Storage-Url") {
		t.Fatalf("GET /auth/v1.0 returned incorrect X-Storage-Url")
	}

	putContainerRequest, err = http.NewRequest("PUT", globals.swiftStorageURL+"/TestContainer", nil)
	if nil != err {
		t.Fatalf("Creating PUT .../TestContainer failed: %v", err)
	}

	putContainerRequest.Header.Add("X-Auth-Token", testAuthToken)

	putContainerResponse, err = testSwiftProxyEmulatorGlobals.httpClient.Do(putContainerRequest)
	if nil != err {
		t.Fatalf("PUT .../TestContainer failed: %v", err)
	}
	if http.StatusCreated != putContainerResponse.StatusCode {
		t.Fatalf("PUT .../TestContainer returned bad status: %v", putContainerResponse.Status)
	}

	putObjectRequest, err = http.NewRequest("PUT", globals.swiftStorageURL+"/TestContainer/TestObject", bytes.NewReader([]byte{0x00, 0x01, 0x02, 0x03, 0x04}))
	if nil != err {
		t.Fatalf("Creating PUT .../TestContainer/TestObject failed: %v", err)
	}

	putObjectRequest.Header.Add("X-Auth-Token", testAuthToken)

	putObjectResponse, err = testSwiftProxyEmulatorGlobals.httpClient.Do(putObjectRequest)
	if nil != err {
		t.Fatalf("PUT .../TestContainer/TestObject failed: %v", err)
	}
	if http.StatusCreated != putObjectResponse.StatusCode {
		t.Fatalf("PUT .../TestContainer/TestObject returned bad status: %v", putObjectResponse.Status)
	}

	getObjectRequest, err = http.NewRequest("GET", globals.swiftStorageURL+"/TestContainer/TestObject", nil)
	if nil != err {
		t.Fatalf("Creating GET .../TestContainer/TestObject failed: %v", err)
	}

	getObjectRequest.Header.Add("X-Auth-Token", testAuthToken)

	getObjectRequest.Header.Add("Range", "bytes=1-3")

	getObjectResponse, err = testSwiftProxyEmulatorGlobals.httpClient.Do(getObjectRequest)
	if nil != err {
		t.Fatalf("GET .../TestContainer/TestObject failed: %v", err)
	}
	if http.StatusPartialContent != getObjectResponse.StatusCode {
		t.Fatalf("GET .../TestContainer/TestObject returned bad status: %v", getObjectResponse.Status)
	}

	getBuf, err = ioutil.ReadAll(getObjectResponse.Body)
	if nil != err {
		t.Fatalf("GET .../TestContainer/TestObject returned unreadable Body: %v", err)
	}

	err = getObjectResponse.Body.Close()
	if nil != err {
		t.Fatalf("GET .../TestContainer/TestObject returned uncloseable Body: %v", err)
	}

	if (3 != len(getBuf)) ||
		(0x01 != getBuf[0]) ||
		(0x02 != getBuf[1]) ||
		(0x03 != getBuf[2]) {
		t.Fatalf("GET .../TestContainer/TestObject returned unexpected contents")
	}

	pingReq = &jrpcfs.PingReq{
		Message: "TestMessage",
	}

	pingReqMessageID, pingReqBuf, err = jrpcMarshalRequest("Server.RpcPing", pingReq)
	if nil != err {
		t.Fatalf("Marshaling pingReq failed: %v", err)
	}

	pingRequest, err = http.NewRequest("PROXYFS", globals.swiftStorageURL, bytes.NewReader(pingReqBuf))
	if nil != err {
		t.Fatalf("Creating PROXYFS pingReq failed: %v", err)
	}

	pingRequest.Header.Add("X-Auth-Token", testAuthToken)
	pingRequest.Header.Add("Content-Type", "application/json")

	pingResponse, err = testSwiftProxyEmulatorGlobals.httpClient.Do(pingRequest)
	if nil != err {
		t.Fatalf("PROXYFS pingReq failed: %v", err)
	}
	if http.StatusOK != pingResponse.StatusCode {
		t.Fatalf("PROXYFS pingReq returned bad status: %v", pingResponse.Status)
	}

	pingReplyBuf, err = ioutil.ReadAll(pingResponse.Body)
	if nil != err {
		t.Fatalf("PROXYFS pingReq returned unreadable Body: %v", err)
	}

	err = pingResponse.Body.Close()
	if nil != err {
		t.Fatalf("PROXYFS pingReq returned uncloseable Body: %v", err)
	}

	pingReplyMessageID, responseErr, unmarshalErr = jrpcUnmarshalResponseForIDAndError(pingReplyBuf)
	if nil != unmarshalErr {
		t.Fatalf("Unmarshaling ID & Error failed: %v", unmarshalErr)
	}
	if nil != responseErr {
		t.Fatalf("Unmarshaling ID & Error returned unexpected Error")
	}
	if pingReqMessageID != pingReplyMessageID {
		t.Fatal("Unmarshaling ID & Error returned unexpected ID")
	}

	pingReply = &jrpcfs.PingReply{}

	err = jrpcUnmarshalResponse(pingReqMessageID, pingReplyBuf, pingReply)
	if nil != err {
		t.Fatalf("Unmarshaling pingReply failed: %v", err)
	}
	if fmt.Sprintf("pong %d bytes", len("TestMessage")) != pingReply.Message {
		t.Fatalf("PROXYFS pingReply returned unexpected Message")
	}

	testTeardown(t)
}

func TestJRPCMarshaling(t *testing.T) {
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
