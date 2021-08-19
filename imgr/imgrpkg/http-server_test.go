// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/version"
)

func TestHTTPServer(t *testing.T) {
	var (
		err                              error
		getRequestHeaders                http.Header
		postRequestBody                  string
		putRequestBody                   string
		responseBody                     []byte
		responseBodyAsString             string
		responseBodyAsStringSplit        []string
		responseBodyExpected             string
		responseBodyExpectedStaticPrefix string
		responseBodyExpectedStaticMiddle string
		responseBodyExpectedStaticSuffix string
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

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/version", nil, nil)
	if nil != err {
		t.Fatalf("GET /version failed: %v", err)
	}
	if string(responseBody[:]) != version.ProxyFSVersion {
		t.Fatalf("GET /version should have returned \"%s\" - it returned \"%s\"", version.ProxyFSVersion, string(responseBody[:]))
	}

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume [case 1] failed: %v", err)
	}
	if string(responseBody[:]) != "[]" {
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
	if string(responseBody[:]) != ilayout.GetObjectNameAsString(ilayout.CheckPointObjectNumber)+"\n"+ilayout.GetObjectNameAsString(ilayout.CheckPointObjectNumber+2)+"\n"+ilayout.GetObjectNameAsString(ilayout.CheckPointObjectNumber+3)+"\n" {
		t.Fatalf("testDoHTTPRequest(\"GET\", testGlobals.containerURL, getRequestHeaders, nil) returned unexpected Object List: \"%s\"", string(responseBody[:]))
	}

	_, responseBody, err = testDoHTTPRequest("GET", fmt.Sprintf("%s/%016X", testGlobals.containerURL, ilayout.CheckPointObjectNumber), getRequestHeaders, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"GET\", testGlobals.containerURL/ilayout.CheckPointObjectNumber, getRequestHeaders, nil) failed: %v", err)
	}
	if string(responseBody[:]) != fmt.Sprintf("%016X %016X %016X %016X", ilayout.CheckPointVersionV1, ilayout.CheckPointObjectNumber+3, 96, ilayout.CheckPointObjectNumber+3) {
		t.Fatalf("testDoHTTPRequest(\"GET\", testGlobals.containerURL/ilayout.CheckPointObjectNumber, getRequestHeaders, nil) returned unexpected Object List: \"%s\"", string(responseBody[:]))
	}

	putRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\"}", testGlobals.containerURL)

	_, _, err = testDoHTTPRequest("PUT", testGlobals.httpServerURL+"/volume/"+testVolume, nil, strings.NewReader(putRequestBody))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.httpServerURL+\"/volume\"+testVolume, nil, strings.NewReader(putRequestBody)) [case 1] failed: %v", err)
	}

	_, _, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume/"+testVolume, nil, nil)
	if nil == err {
		t.Fatalf("GET /volume/%s [case 1] should have failed", testVolume)
	}

	responseBodyExpected = fmt.Sprintf("[{\"Name\":\"%s\",\"StorageURL\":\"%s\",\"AuthToken\":\"\",\"HealthyMounts\":0,\"LeasesExpiredMounts\":0,\"AuthTokenExpiredMounts\":0}]", testVolume, testGlobals.containerURL)

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume [case 2] failed: %v", err)
	}
	if string(responseBody[:]) != responseBodyExpected {
		t.Fatalf("GET /volume [case 2] returned unexpected responseBody: \"%s\"", responseBody)
	}

	_, _, err = testDoHTTPRequest("DELETE", testGlobals.httpServerURL+"/volume/"+testVolume, nil, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"DELETE\", testGlobals.httpServerURL+\"/volume/\"+testVolume, nil, nil) [case 1] failed: %v", err)
	}

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume [case 3] failed: %v", err)
	}
	if string(responseBody[:]) != "[]" {
		t.Fatalf("GET /volume [case 3] should have returned \"[]\" - it returned \"%s\"", string(responseBody[:]))
	}

	putRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\",\"AuthToken\":\"%s\"}", testGlobals.containerURL, testGlobals.authToken)

	_, _, err = testDoHTTPRequest("PUT", testGlobals.httpServerURL+"/volume/"+testVolume, nil, strings.NewReader(putRequestBody))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.httpServerURL+\"/volume\"+testVolume, nil, strings.NewReader(putRequestBody)) [case 2] failed: %v", err)
	}

	responseBodyExpected = fmt.Sprintf("{\"Name\":\"%s\",\"StorageURL\":\"%s\",\"AuthToken\":\"%s\",\"HealthyMounts\":0,\"LeasesExpiredMounts\":0,\"AuthTokenExpiredMounts\":0,\"SuperBlockObjectName\":\"3000000000000000\",\"SuperBlockLength\":96,\"ReservedToNonce\":3,\"InodeTableMinInodesPerNode\":1024,\"InodeTableMaxInodesPerNode\":2048,\"InodeTableInodeCount\":1,\"InodeTableHeight\":1,\"InodeTableLayout\":[{\"ObjectName\":\"3000000000000000\",\"ObjectSize\":58,\"BytesReferenced\":58}],\"InodeObjectCount\":1,\"InodeObjectSize\":237,\"InodeBytesReferenced\":237,\"PendingDeleteObjectNameArray\":[],\"InodeTable\":[{\"InodeNumber\":1,\"InodeHeadObjectName\":\"2000000000000000\",\"InodeHeadLength\":174}]}", testVolume, testGlobals.containerURL, testGlobals.authToken)

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume/"+testVolume, nil, nil)
	if nil != err {
		t.Fatalf("GET /volume/%s [case 2] failed: %v", testVolume, err)
	}
	if string(responseBody[:]) != responseBodyExpected {
		t.Fatalf("GET /volume/%s [case 2] returned unexpected responseBody: \"%s\"", testVolume, responseBody)
	}

	responseBodyExpectedStaticPrefix = "{\"InodeNumber\":1,\"InodeType\":\"Dir\",\"LinkTable\":[{\"ParentDirInodeNumber\":1,\"ParentDirEntryName\":\".\"},{\"ParentDirInodeNumber\":1,\"ParentDirEntryName\":\"..\"}],\"ModificationTime\":\""
	// Next segment to be a quoted ModificationTime.Format(time.RFC3339)
	responseBodyExpectedStaticMiddle = "\",\"StatusChangeTime\":\""
	// Next segment to be a quoted StatusChangeTime.Format(time.RFC3339)
	responseBodyExpectedStaticSuffix = "\",\"Mode\":511,\"UserID\":0,\"GroupID\":0,\"StreamTable\":[],\"MinDirEntriesPerNode\":512,\"MaxDirEntriesPerNode\":1024,\"DirEntryCount\":2,\"DirectoryHeight\":1,\"Payload\":[{\"BaseName\":\".\",\"InodeNumber\":1,\"InodeType\":\"Dir\"},{\"BaseName\":\"..\",\"InodeNumber\":1,\"InodeType\":\"Dir\"}],\"Layout\":[{\"ObjectName\":\"2000000000000000\",\"ObjectSize\":63,\"BytesReferenced\":63}]}"

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume/"+testVolume+"/inode/1", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume/%s/inode/1 failed: %v", testVolume, err)
	}
	responseBodyAsString = string(responseBody[:])
	if !strings.HasPrefix(responseBodyAsString, responseBodyExpectedStaticPrefix) {
		t.Fatalf("GET /volume/%s/inode/1 returned unexpected responseBody [case 1]: \"%s\"", testVolume, responseBody)
	}
	if !strings.HasSuffix(responseBodyAsString, responseBodyExpectedStaticSuffix) {
		t.Fatalf("GET /volume/%s/inode/1 returned unexpected responseBody [case 2]: \"%s\"", testVolume, responseBody)
	}
	responseBodyAsString = strings.TrimPrefix(responseBodyAsString, responseBodyExpectedStaticPrefix)
	responseBodyAsString = strings.TrimSuffix(responseBodyAsString, responseBodyExpectedStaticSuffix)
	responseBodyAsStringSplit = strings.Split(responseBodyAsString, responseBodyExpectedStaticMiddle)
	if len(responseBodyAsStringSplit) != 2 {
		t.Fatalf("GET /volume/%s/inode/1 returned unexpected responseBody [case 3]: \"%s\"", testVolume, responseBody)
	}
	_, err = time.Parse(time.RFC3339, responseBodyAsStringSplit[0])
	if nil != err {
		t.Fatalf("GET /volume/%s/inode/1 returned unexpected responseBody [case 4]: \"%s\"", testVolume, responseBody)
	}
	_, err = time.Parse(time.RFC3339, responseBodyAsStringSplit[1])
	if nil != err {
		t.Fatalf("GET /volume/%s/inode/1 returned unexpected responseBody [case 5]: \"%s\"", testVolume, responseBody)
	}

	responseBodyExpected = fmt.Sprintf("[{\"Name\":\"%s\",\"StorageURL\":\"%s\",\"AuthToken\":\"%s\",\"HealthyMounts\":0,\"LeasesExpiredMounts\":0,\"AuthTokenExpiredMounts\":0}]", testVolume, testGlobals.containerURL, testGlobals.authToken)

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume [case 4] failed: %v", err)
	}
	if string(responseBody[:]) != responseBodyExpected {
		t.Fatalf("GET /volume [case 4] returned unexpected responseBody: \"%s\"", responseBody)
	}

	_, _, err = testDoHTTPRequest("DELETE", testGlobals.httpServerURL+"/volume/"+testVolume, nil, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"DELETE\", testGlobals.httpServerURL+\"/volume/\"+testVolume, nil, nil) [case 2] failed: %v", err)
	}

	_, responseBody, err = testDoHTTPRequest("GET", testGlobals.httpServerURL+"/volume", nil, nil)
	if nil != err {
		t.Fatalf("GET /volume [case 5] failed: %v", err)
	}
	if string(responseBody[:]) != "[]" {
		t.Fatalf("GET /volume [case 5] should have returned \"[]\" - it returned \"%s\"", string(responseBody[:]))
	}

	testTeardown(t)
}
