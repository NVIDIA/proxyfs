// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/NVIDIA/proxyfs/ilayout"
)

func TestHTTPServer(t *testing.T) {
	var (
		err                  error
		getRequestHeaders    http.Header
		postRequestBody      string
		putRequestBody       string
		responseBody         []byte
		responseBodyExpected string
	)

	testSetup(t, nil)

	_, _, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/config", nil, nil)
	if nil != err {
		t.Fatalf("GET /config failed: %v", err)
	}

	_, _, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/stats", nil, nil)
	if nil != err {
		t.Fatalf("GET /stats failed: %v", err)
	}

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume [case 1] failed: %v", err)
	}
	if "[]" != string(responseBody[:]) {
		t.Fatalf("GET /volume [case 1] should have returned \"[]\" - it returned \"%s\"", string(responseBody[:]))
	}

	postRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\",\"AuthToken\":\"%s\"}", testGlobals.containerURL, testGlobals.authToken)

	_, _, err = testDoHTTPRequest("POST", testGlobals.httpServerURL+"/volume", nil, strings.NewReader(postRequestBody))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"POST\", testGlobals.httpServerURL+\"/volume\", nil, strings.NewReader(postRequestBody)) failed: %v", err)
	}

	getRequestHeaders = make(http.Header)

	getRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.containerURL, getRequestHeaders, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"GET\", testGlobals.containerURL, getRequestHeaders, nil) failed: %v", err)
	}
	if "0000000000000000\n0000000000000002\n0000000000000003\n" != string(responseBody[:]) {
		t.Fatalf("testDoHTTPRequest(\"GET\", testGlobals.containerURL, getRequestHeaders, nil) returned unexpected Object List: \"%s\"", string(responseBody[:]))
	}

	_, responseBody, err = testDoHTTPRequest("GET", fmt.Sprintf("%s/%016X", testGlobals.containerURL, ilayout.CheckPointObjectNumber), getRequestHeaders, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"GET\", testGlobals.containerURL/ilayout.CheckPointObjectNumber, getRequestHeaders, nil) failed: %v", err)
	}
	if "0000000000000001 0000000000000003 0000000000000060 0000000000000003" != string(responseBody[:]) {
		t.Fatalf("testDoHTTPRequest(\"GET\", testGlobals.containerURL/ilayout.CheckPointObjectNumber, getRequestHeaders, nil) returned unexpected Object List: \"%s\"", string(responseBody[:]))
	}

	putRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\"}", testGlobals.containerURL)

	_, _, err = testDoHTTPRequest("PUT", testGlobals.httpServerURL+"/volume/"+testVolume, nil, strings.NewReader(putRequestBody))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.httpServerURL+\"/volume\"+testVolume, nil, strings.NewReader(putRequestBody)) failed: %v", err)
	}

	responseBodyExpected = fmt.Sprintf("{\"Name\":\"%s\",\"StorageURL\":\"%s\",\"HealthyMounts\":0,\"LeasesExpiredMounts\":0,\"AuthTokenExpiredMounts\":0}", testVolume, testGlobals.containerURL)

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume/"+testVolume, nil, nil)
	if nil != err {
		t.Fatalf("GET /volume/%s failed: %v", testVolume, err)
	}
	if string(responseBody[:]) != responseBodyExpected {
		t.Fatalf("GET /volume/%s returned unexpected responseBody: \"%s\"", testVolume, responseBody)
	}

	responseBodyExpected = "[" + responseBodyExpected + "]"

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume [case 2] failed: %v", err)
	}
	if string(responseBody[:]) != responseBodyExpected {
		t.Fatalf("GET /volume [case 2] returned unexpected responseBody: \"%s\"", responseBody)
	}

	_, _, err = testDoHTTPRequest("DELETE", testGlobals.httpServerURL+"/volume/"+testVolume, nil, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"DELETE\", testGlobals.httpServerURL+\"/volume/\"+testVolume, nil, nil) failed: %v", err)
	}

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume [case 3] failed: %v", err)
	}
	if "[]" != string(responseBody[:]) {
		t.Fatalf("GET /volume [case 3] should have returned \"[]\" - it returned \"%s\"", string(responseBody[:]))
	}

	testTeardown(t)
}
