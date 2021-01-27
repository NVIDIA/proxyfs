// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package emswiftpkg

import (
	"bytes"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/utils"
)

func TestAuthEmulation(t *testing.T) {
	var (
		confMap     conf.ConfMap
		confStrings = []string{
			"EMSWIFT.AuthIPAddr=127.0.0.1",
			"EMSWIFT.AuthTCPPort=9997",
			"EMSWIFT.JRPCIPAddr=127.0.0.1",
			"EMSWIFT.JRPCTCPPort=9998",
			"EMSWIFT.NoAuthIPAddr=127.0.0.1",
			"EMSWIFT.NoAuthTCPPort=9999",
			"EMSWIFT.MaxAccountNameLength=256",
			"EMSWIFT.MaxContainerNameLength=256",
			"EMSWIFT.MaxObjectNameLength=1024",
			"EMSWIFT.AccountListingLimit=10000",
			"EMSWIFT.ContainerListingLimit=10000",
		}
		err                              error
		expectedInfo                     string
		expectedStorageURL               string
		httpClient                       *http.Client
		httpRequest                      *http.Request
		httpResponse                     *http.Response
		readBuf                          []byte
		testJRPCServerHandlerTCPListener *net.TCPListener
		testJRPCServerHandlerWG          *sync.WaitGroup
		urlForAuth                       string
		urlForInfo                       string
		urlPrefix                        string
	)

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned unexpected error: %v", err)
	}

	err = Start(confMap)
	if nil != err {
		t.Fatalf("Start(confMap) returned unexpected error: %v", err)
	}

	testJRPCServerHandlerTCPListener, err = net.ListenTCP("tcp", globals.authEmulator.resolvedJRPCTCPAddr)
	if nil != err {
		t.Fatalf("net.ListenTCP() returned unexpected error: %v", err)
	}

	testJRPCServerHandlerWG = new(sync.WaitGroup)
	testJRPCServerHandlerWG.Add(1)
	go testJRPCServerHandler(testJRPCServerHandlerTCPListener, testJRPCServerHandlerWG)

	// Format URLs

	urlForInfo = "http://" + globals.authEmulator.httpServer.Addr + "/info"
	urlForAuth = "http://" + globals.authEmulator.httpServer.Addr + "/auth/v1.0"
	urlPrefix = "http://" + globals.authEmulator.httpServer.Addr + "/proxyfs/"

	expectedStorageURL = "http://" + globals.authEmulator.httpServer.Addr + "/v1/" + "AUTH_test"

	// Setup http.Client that we will use for all HTTP requests

	httpClient = &http.Client{}

	// Send a GET for "/info" expecting [EMSWIFT] data in compact JSON form

	httpRequest, err = http.NewRequest("GET", urlForInfo, nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	expectedInfo = "{\"swift\": {\"max_account_name_length\": 256,\"max_container_name_length\": 256,\"max_object_name_length\": 1024,\"account_listing_limit\": 10000,\"container_listing_limit\": 10000}}"
	if int64(len(expectedInfo)) != httpResponse.ContentLength {
		t.Fatalf("GET of /info httpResponse.ContentLength unexpected")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if expectedInfo != utils.ByteSliceToString(readBuf) {
		t.Fatalf("GET of /info httpResponse.Body contents unexpected")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for "/auth/v1.0" expecting X-Auth-Token & X-Storage-Url

	httpRequest, err = http.NewRequest("GET", urlForAuth, nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("X-Auth-User", "test:tester")
	httpRequest.Header.Add("X-Auth-Key", "testing")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}
	if httpResponse.Header.Get("X-Auth-Token") != fixedAuthToken {
		t.Fatalf("Auth response should have header X-Auth-Token: %s", fixedAuthToken)
	}
	if httpResponse.Header.Get("X-Storage-Url") != expectedStorageURL {
		t.Fatalf("Auth response should have header X-Storage-Url: %s", expectedStorageURL)
	}

	// Send a PUT for account "TestAccount"

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("X-Auth-Token", fixedAuthToken)
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" expecting Content-Length: 0

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("X-Auth-Token", fixedAuthToken)
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	t.Logf("TODO: Implement remaining TestAuthEmulation() PROXYFS RPC Method")

	err = testJRPCServerHandlerTCPListener.Close()
	if nil != err {
		t.Fatalf("testJRPCServerHandlerTCPListener.Close() returned unexpected error: %v", err)
	}

	testJRPCServerHandlerWG.Wait()

	err = Stop()
	if nil != err {
		t.Fatalf("Stop() returned unexpected error: %v", err)
	}
}

func testJRPCServerHandler(testJRPCServerHandlerTCPListener *net.TCPListener, testJRPCServerHandlerWG *sync.WaitGroup) {
	var (
		bytesWritten                int
		bytesWrittenInTotal         int
		err                         error
		jrpcRequest                 []byte
		jrpcRequestNestedLeftBraces uint32
		jrpcRequestSingleByte       []byte
		jrpcResponse                []byte
		jrpcTCPConn                 *net.TCPConn
	)

DoAcceptTCP:
	jrpcTCPConn, err = testJRPCServerHandlerTCPListener.AcceptTCP()
	if nil != err {
		testJRPCServerHandlerWG.Done()
		return
	}

	jrpcRequest = make([]byte, 0)
	jrpcRequestSingleByte = make([]byte, 1)

	_, err = jrpcTCPConn.Read(jrpcRequestSingleByte)
	if nil != err {
		_ = jrpcTCPConn.Close()
		goto DoAcceptTCP
	}
	jrpcRequest = append(jrpcRequest, jrpcRequestSingleByte[0])

	if '{' != jrpcRequestSingleByte[0] {
		_ = jrpcTCPConn.Close()
		goto DoAcceptTCP
	}

	jrpcRequestNestedLeftBraces = 1

	for 0 < jrpcRequestNestedLeftBraces {
		_, err = jrpcTCPConn.Read(jrpcRequestSingleByte)
		if nil != err {
			_ = jrpcTCPConn.Close()
			goto DoAcceptTCP
		}
		jrpcRequest = append(jrpcRequest, jrpcRequestSingleByte[0])

		switch jrpcRequestSingleByte[0] {
		case '{':
			jrpcRequestNestedLeftBraces++
		case '}':
			jrpcRequestNestedLeftBraces--
		default:
			// Nothing special to do here
		}
	}

	jrpcResponse = jrpcRequest

	bytesWrittenInTotal = 0

	for bytesWrittenInTotal < len(jrpcResponse) {
		bytesWritten, err = jrpcTCPConn.Write(jrpcResponse[len(jrpcResponse)-bytesWrittenInTotal:])
		if nil != err {
			_ = jrpcTCPConn.Close()
			goto DoAcceptTCP
		}
		bytesWrittenInTotal += bytesWritten
	}

	_ = jrpcTCPConn.Close()
	goto DoAcceptTCP
}

func TestNoAuthEmulation(t *testing.T) {
	var (
		confMap     conf.ConfMap
		confStrings = []string{
			"EMSWIFT.NoAuthIPAddr=127.0.0.1",
			"EMSWIFT.NoAuthTCPPort=9999",
			"EMSWIFT.MaxAccountNameLength=256",
			"EMSWIFT.MaxContainerNameLength=256",
			"EMSWIFT.MaxObjectNameLength=1024",
			"EMSWIFT.AccountListingLimit=10000",
			"EMSWIFT.ContainerListingLimit=10000",
		}
		contentType                  string
		contentTypeMultiPartBoundary string
		err                          error
		errChan                      chan error
		expectedBuf                  []byte
		expectedInfo                 string
		httpClient                   *http.Client
		httpRequest                  *http.Request
		httpResponse                 *http.Response
		mouseHeaderPresent           bool
		pipeReader                   *io.PipeReader
		pipeWriter                   *io.PipeWriter
		readBuf                      []byte
		urlForInfo                   string
		urlPrefix                    string
	)

	confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) returned unexpected error: %v", err)
	}

	err = Start(confMap)
	if nil != err {
		t.Fatalf("Start(confMap) returned unexpected error: %v", err)
	}

	// Format URLs

	urlForInfo = "http://" + globals.noAuthEmulator.httpServer.Addr + "/info"
	urlPrefix = "http://" + globals.noAuthEmulator.httpServer.Addr + "/v1/"

	// Setup http.Client that we will use for all HTTP requests

	httpClient = &http.Client{}

	// Send a GET for "/info" expecting [EMSWIFT] data in compact JSON form

	httpRequest, err = http.NewRequest("GET", urlForInfo, nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	expectedInfo = "{\"swift\": {\"max_account_name_length\": 256,\"max_container_name_length\": 256,\"max_object_name_length\": 1024,\"account_listing_limit\": 10000,\"container_listing_limit\": 10000}}"
	if int64(len(expectedInfo)) != httpResponse.ContentLength {
		t.Fatalf("GET of /info httpResponse.ContentLength unexpected")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if expectedInfo != utils.ByteSliceToString(readBuf) {
		t.Fatalf("GET of /info httpResponse.Body contents unexpected")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for account "TestAccount" and header Cat: Dog

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Cat", "Dog")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for account "TestAccount" and header Mouse: Bird

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestAccount should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for account "TestAccount" deleting header Mouse

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestAccount should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for account "TestAccount" and header Mouse: Bird

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusAccepted != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestAccount should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for account "TestAccount" deleting header Mouse

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusAccepted != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for account "TestAccount" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestAccount should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for container "TestContainer" and header Cat: Dog

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Cat", "Dog")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" expecting "TestContainer\n" and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if int64(len("TestContainer\n")) != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain only \"TestContainer\\n\" at this point")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if "TestContainer\n" != utils.ByteSliceToString(readBuf) {
		t.Fatalf("TestAccount should contain only \"TestContainer\\n\" at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" with marker "AAA" expecting "TestContainer\n" and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount?marker=AAA", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if int64(len("TestContainer\n")) != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain only \"TestContainer\\n\" at this point")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if "TestContainer\n" != utils.ByteSliceToString(readBuf) {
		t.Fatalf("TestAccount should contain only \"TestContainer\\n\" at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" with marker "ZZZ" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount?marker=ZZZ", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for container "TestContainer" and header Mouse: Bird

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestContainer should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for container "TestContainer" deleting header Mouse

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestContainer should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for container "TestContainer" and header Mouse: Bird

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusAccepted != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestContainer should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for container "TestContainer" deleting header Mouse

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusAccepted != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for container "TestContainer" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestContainer should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a non-chunked PUT for object "Foo" to contain []byte{0x00, 0x01, 0x02}

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer/Foo", bytes.NewReader([]byte{0x00, 0x01, 0x02}))
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a chunked PUT for object "Bar"" with 1st chunk being []byte{0xAA, 0xBB} & 2nd chunk being []byte{0xCC, 0xDD, 0xEE}

	pipeReader, pipeWriter = io.Pipe()
	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer/Bar", pipeReader)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.ContentLength = -1
	httpRequest.Header.Del("Content-Length")
	errChan = make(chan error, 1)
	go func() {
		var (
			nonShadowingErr          error
			nonShadowingHTTPResponse *http.Response
		)
		nonShadowingHTTPResponse, nonShadowingErr = httpClient.Do(httpRequest)
		if nil == nonShadowingErr {
			httpResponse = nonShadowingHTTPResponse
		}
		errChan <- nonShadowingErr
	}()
	_, err = pipeWriter.Write([]byte{0xAA, 0xBB})
	if nil != err {
		t.Fatalf("pipeWriter.Write() returned unexpected error: %v", err)
	}
	_, err = pipeWriter.Write([]byte{0xCC, 0xDD, 0xEE})
	if nil != err {
		t.Fatalf("pipeWriter.Write() returned unexpected error: %v", err)
	}
	err = pipeWriter.Close()
	if nil != err {
		t.Fatalf("pipeWriter.Close() returned unexpected error: %v", err)
	}
	err = <-errChan
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" expecting "Bar\nFoo\n" and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if int64(len("Bar\nFoo\n")) != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain only \"Bar\\nFoo\\n\" at this point")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if "Bar\nFoo\n" != utils.ByteSliceToString(readBuf) {
		t.Fatalf("TestContainer should contain only \"Bar\\nFoo\\n\" at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" with marker "AAA" expecting "Bar\nFoo\n" and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer?marker=AAA", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if int64(len("Bar\nFoo\n")) != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain only \"Bar\\nFoo\\n\" at this point")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if "Bar\nFoo\n" != utils.ByteSliceToString(readBuf) {
		t.Fatalf("TestContainer should contain only \"Bar\\nFoo\\n\" at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" with marker "ZZZ" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer?marker=ZZZ", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for object "Foo" expecting Content-Length: 3

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer/Foo", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if 3 != httpResponse.ContentLength {
		t.Fatalf("httpResponse.ContentLength contained unexpected value: %v", httpResponse.ContentLength)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a full object GET for object "Foo" expecting []byte{0x00, 0x01, 0x02}

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer/Foo", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if int64(len([]byte{0x00, 0x01, 0x02})) != httpResponse.ContentLength {
		t.Fatalf("Foo should contain precisely []byte{0x00, 0x01, 0x02}")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare([]byte{0x00, 0x01, 0x02}, readBuf) {
		t.Fatalf("Foo should contain precisely []byte{0x00, 0x01, 0x02}")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a range GET of bytes at offset 1 for length 3 for object "Bar" expecting []byte{0xBB, 0xCC, 0xDD}

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer/Bar", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Range", "bytes=1-3")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusPartialContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if int64(len([]byte{0xBB, 0xCC, 0xDD})) != httpResponse.ContentLength {
		t.Fatalf("Bar's bytes 1-3 should contain precisely []byte{0xBB, 0xCC, 0xDD}")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare([]byte{0xBB, 0xCC, 0xDD}, readBuf) {
		t.Fatalf("Bar's bytes 1-3 should contain precisely []byte{0xBB, 0xCC, 0xDD}")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a range GET of bytes at offset 0 for length 2
	//                          and offset 3 for length of 1 for object "Bar"
	// expecting two MIME parts: []byte{0xAA, 0xBB} and  []byte{0xDD}

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer/Bar", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Range", "bytes=0-1,3-3")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusPartialContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	contentType = httpResponse.Header.Get("Content-Type")
	contentTypeMultiPartBoundary = strings.TrimPrefix(contentType, "multipart/byteranges; boundary=")
	if (len(contentType) == len(contentTypeMultiPartBoundary)) || (0 == len(contentTypeMultiPartBoundary)) {
		t.Fatalf("httpReponse.Header[\"Content-Type\"] contained unexpected value: \"%v\"", contentType)
	}
	expectedBuf = make([]byte, 0, httpResponse.ContentLength)
	expectedBuf = append(expectedBuf, []byte("--"+contentTypeMultiPartBoundary+"\r\n")...)
	expectedBuf = append(expectedBuf, []byte("Content-Type: application/octet-stream\r\n")...)
	expectedBuf = append(expectedBuf, []byte("Content-Range: bytes 0-1/5\r\n")...)
	expectedBuf = append(expectedBuf, []byte("\r\n")...)
	expectedBuf = append(expectedBuf, []byte{0xAA, 0xBB}...)
	expectedBuf = append(expectedBuf, []byte("\r\n")...)
	expectedBuf = append(expectedBuf, []byte("--"+contentTypeMultiPartBoundary+"\r\n")...)
	expectedBuf = append(expectedBuf, []byte("Content-Type: application/octet-stream\r\n")...)
	expectedBuf = append(expectedBuf, []byte("Content-Range: bytes 3-3/5\r\n")...)
	expectedBuf = append(expectedBuf, []byte("\r\n")...)
	expectedBuf = append(expectedBuf, []byte{0xDD}...)
	expectedBuf = append(expectedBuf, []byte("\r\n")...)
	expectedBuf = append(expectedBuf, []byte("--"+contentTypeMultiPartBoundary+"--")...)
	if int64(len(expectedBuf)) != httpResponse.ContentLength {
		t.Fatalf("Unexpected multi-part GET response Content-Length")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(expectedBuf, readBuf) {
		t.Fatalf("Unexpected payload of multi-part GET response")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a tail GET of the last 2 bytes for object "Bar" expecting []byte{0xDD, 0xEE}

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer/Bar", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Range", "bytes=-2")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusPartialContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if int64(len([]byte{0xDD, 0xEE})) != httpResponse.ContentLength {
		t.Fatalf("Bar's last 2 bytes should contain precisely []byte{0xDD, 0xEE}")
	}
	readBuf, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare([]byte{0xDD, 0xEE}, readBuf) {
		t.Fatalf("Bar's last 2 bytes should contain precisely []byte{0xDD, 0xEE}")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a PUT for object "ZigZag" and header Cat: Dog

	httpRequest, err = http.NewRequest("PUT", urlPrefix+"TestAccount/TestContainer/ZigZag", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Cat", "Dog")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusCreated != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for object "ZigZag" expecting header Cat: Dog

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer/ZigZag", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for object "ZigZag" expecting header Cat: Dog

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer/ZigZag", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for object "ZigZag" and header Mouse: Bird

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount/TestContainer/ZigZag", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "Bird")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for object "ZigZag" expecting header Cat: Dog & Mouse: Bird

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer/ZigZag", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if httpResponse.Header.Get("Mouse") != "Bird" {
		t.Fatalf("TestAccount should have header Mouse: Bird")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a POST for object "ZigZag" deleting header Mouse

	httpRequest, err = http.NewRequest("POST", urlPrefix+"TestAccount/TestContainer/ZigZag", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpRequest.Header.Add("Mouse", "")
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a HEAD for object "ZigZag" expecting header Cat: Dog & no Mouse header

	httpRequest, err = http.NewRequest("HEAD", urlPrefix+"TestAccount/TestContainer/ZigZag", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusOK != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	_, mouseHeaderPresent = httpResponse.Header["Mouse"]
	if mouseHeaderPresent {
		t.Fatalf("TestAccount should not have header Mouse")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for object "Foo"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount/TestContainer/Foo", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for object "Bar"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount/TestContainer/Bar", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for object "ZigZag"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount/TestContainer/ZigZag", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for container "TestContainer" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestContainer should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestContainer should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for container "TestContainer"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount/TestContainer", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a GET for account "TestAccount" expecting Content-Length: 0 and header Cat: Dog

	httpRequest, err = http.NewRequest("GET", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	if httpResponse.Header.Get("Cat") != "Dog" {
		t.Fatalf("TestAccount should have header Cat: Dog")
	}
	if 0 != httpResponse.ContentLength {
		t.Fatalf("TestAccount should contain no elements at this point")
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	// Send a DELETE for account "TestAccount"

	httpRequest, err = http.NewRequest("DELETE", urlPrefix+"TestAccount", nil)
	if nil != err {
		t.Fatalf("http.NewRequest() returned unexpected error: %v", err)
	}
	httpResponse, err = httpClient.Do(httpRequest)
	if nil != err {
		t.Fatalf("httpClient.Do() returned unexpected error: %v", err)
	}
	if http.StatusNoContent != httpResponse.StatusCode {
		t.Fatalf("httpResponse.StatusCode contained unexpected value: %v", httpResponse.StatusCode)
	}
	err = httpResponse.Body.Close()
	if nil != err {
		t.Fatalf("http.Response.Body.Close() returned unexpected error: %v", err)
	}

	err = Stop()
	if nil != err {
		t.Fatalf("Stop() returned unexpected error: %v", err)
	}
}
