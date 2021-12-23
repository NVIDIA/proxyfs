// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"crypto/x509/pkix"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/icert/icertpkg"
	"github.com/NVIDIA/proxyfs/imgr/imgrpkg"
	"github.com/NVIDIA/proxyfs/iswift/iswiftpkg"
)

const (
	testIPAddr               = "127.0.0.1" // Don't use IPv6... the code doesn't properly "join" this with :port #s
	testRetryRPCPort         = 32356
	testMgrHTTPServerPort    = 15346
	testClientHTTPServerPort = 15347
	testSwiftProxyTCPPort    = 8080
	testSwiftAuthUser        = "test"
	testSwiftAuthKey         = "test"
	testAccount              = "AUTH_test"
	testContainer            = "con"
	testVolume               = "testvol"
	testRPCDeadlineIO        = "60s"
	testRPCKeepAlivePeriod   = "60s"
)

type testGlobalsStruct struct {
	tempDir              string
	mountPointDir        string
	caCertFile           string
	caKeyFile            string
	caCertPEMBlock       []byte
	caKeyPEMBlock        []byte
	endpointCertFile     string
	endpointKeyFile      string
	endpointCertPEMBlock []byte
	endpointKeyPEMBlock  []byte
	confMap              conf.ConfMap
	imgrHTTPServerURL    string
	authURL              string
	authToken            string
	accountURL           string
	containerURL         string
}

var testGlobals *testGlobalsStruct

func testSetup(t *testing.T) {
	var (
		authRequestHeaders         http.Header
		authResponseHeaders        http.Header
		confStrings                []string
		err                        error
		expectedAccountURL         string
		postAndPutVolumePayload    string
		putAccountRequestHeaders   http.Header
		putContainerRequestHeaders http.Header
		tempDir                    string
	)

	tempDir, err = ioutil.TempDir("", "iclientpkg_test")
	if nil != err {
		t.Fatalf("ioutil.TempDir(\"\", \"iclientpkg_test\") failed: %v", err)
	}

	testGlobals = &testGlobalsStruct{
		tempDir:           tempDir,
		mountPointDir:     tempDir + "/mnt",
		caCertFile:        tempDir + "/caCertFile",
		caKeyFile:         tempDir + "/caKeyFile",
		endpointCertFile:  tempDir + "/endpoingCertFile",
		endpointKeyFile:   tempDir + "/endpointKeyFile",
		imgrHTTPServerURL: fmt.Sprintf("http://%s:%d", testIPAddr, testMgrHTTPServerPort),
		authURL:           fmt.Sprintf("http://%s:%d/auth/v1.0", testIPAddr, testSwiftProxyTCPPort),
	}

	testGlobals.caCertPEMBlock, testGlobals.caKeyPEMBlock, err = icertpkg.GenCACert(
		icertpkg.GenerateKeyAlgorithmEd25519,
		pkix.Name{
			Organization:  []string{"Test Organization CA"},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		time.Hour,
		testGlobals.caCertFile,
		testGlobals.caKeyFile)
	if nil != err {
		t.Fatalf("icertpkg.GenCACert() failed: %v", err)
	}

	testGlobals.endpointCertPEMBlock, testGlobals.endpointKeyPEMBlock, err = icertpkg.GenEndpointCert(
		icertpkg.GenerateKeyAlgorithmEd25519,
		pkix.Name{
			Organization:  []string{"Test Organization Endpoint"},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		[]string{},
		[]net.IP{net.ParseIP(testIPAddr)},
		time.Hour,
		testGlobals.caCertPEMBlock,
		testGlobals.caKeyPEMBlock,
		testGlobals.endpointCertFile,
		testGlobals.endpointKeyFile)
	if nil != err {
		t.Fatalf("icertpkg.GenEndpointCert() failed: %v", err)
	}

	err = os.MkdirAll(testGlobals.mountPointDir, os.ModePerm)
	if nil != err {
		t.Fatalf("os.MkdirAll() failed: %v", err)
	}

	confStrings = []string{
		"ICLIENT.VolumeName=" + testVolume,
		"ICLIENT.MountPointDirPath=" + testGlobals.mountPointDir,
		"ICLIENT.FUSEBlockSize=512",
		"ICLIENT.FUSEAllowOther=true",
		"ICLIENT.FUSEMaxBackground=1000",
		"ICLIENT.FUSECongestionThreshhold=0",
		"ICLIENT.FUSEMaxRead=131076",
		"ICLIENT.FUSEMaxWrite=131076",
		"ICLIENT.FUSEEntryValidDuration=250ms",
		"ICLIENT.FUSEAttrValidDuration=250ms",
		"ICLIENT.AuthPlugInPath=../../iauth/iauth-swift/iauth-swift.so",
		"ICLIENT.AuthPlugInEnvName=",
		"ICLIENT.AuthPlugInEnvValue=" + fmt.Sprintf("{\"AuthURL\":\"http://%s:%d/auth/v1.0\"\\u002C\"AuthUser\":\"%s\"\\u002C\"AuthKey\":\"%s\"\\u002C\"Account\":\"%s\"\\u002C\"Container\":\"%s\"}", testIPAddr, testSwiftProxyTCPPort, testSwiftAuthUser, testSwiftAuthKey, testAccount, testContainer),
		"ICLIENT.SwiftTimeout=10m",
		"ICLIENT.SwiftRetryLimit=4",
		"ICLIENT.SwiftRetryDelay=100ms",
		"ICLIENT.SwiftRetryDelayVariance=25",
		"ICLIENT.SwiftRetryExponentialBackoff=1.4",
		"ICLIENT.SwiftConnectionPoolSize=128",
		"ICLIENT.RetryRPCPublicIPAddr=" + testIPAddr,
		"ICLIENT.RetryRPCPort=" + fmt.Sprintf("%d", testRetryRPCPort),
		"ICLIENT.RetryRPCDeadlineIO=60s",
		"ICLIENT.RetryRPCKeepAlivePeriod=60s",
		"ICLIENT.RetryRPCCACertFilePath=" + testGlobals.caCertFile,
		"ICLIENT.MaxSharedLeases=500",
		"ICLIENT.MaxExclusiveLeases=100",
		"ICLIENT.InodePayloadEvictLowLimit=100000",
		"ICLIENT.InodePayloadEvictHighLimit=100010",
		"ICLIENT.DirInodeMaxKeysPerBPlusTreePage=1024",
		"ICLIENT.FileInodeMaxKeysPerBPlusTreePage=2048",
		"ICLIENT.ReadCacheLineSize=1048576",
		"ICLIENT.ReadCacheLineCountMax=1024",
		"ICLIENT.FileFlushTriggerSize=10485760",
		"ICLIENT.FileFlushTriggerDuration=10s",
		"ICLIENT.InodeLockRetryDelay=10ms",
		"ICLIENT.InodeLockRetryDelayVariance=50",
		"ICLIENT.LogFilePath:",
		"ICLIENT.LogToConsole=true",
		"ICLIENT.TraceEnabled=false",
		"ICLIENT.FUSELogEnabled=false",
		"ICLIENT.HTTPServerIPAddr=" + testIPAddr,
		"ICLIENT.HTTPServerPort=" + fmt.Sprintf("%d", testClientHTTPServerPort),

		"IMGR.PublicIPAddr=" + testIPAddr,
		"IMGR.PrivateIPAddr=" + testIPAddr,
		"IMGR.RetryRPCPort=" + fmt.Sprintf("%d", testRetryRPCPort),
		"IMGR.HTTPServerPort=" + fmt.Sprintf("%d", testMgrHTTPServerPort),

		"IMGR.RetryRPCTTLCompleted=10m",
		"IMGR.RetryRPCAckTrim=100ms",
		"IMGR.RetryRPCDeadlineIO=60s",
		"IMGR.RetryRPCKeepAlivePeriod=60s",

		"IMGR.RetryRPCCertFilePath=" + testGlobals.endpointCertFile,
		"IMGR.RetryRPCKeyFilePath=" + testGlobals.endpointKeyFile,

		"IMGR.CheckPointInterval=10s",

		"IMGR.AuthTokenCheckInterval=1m",

		"IMGR.FetchNonceRangeToReturn=100",

		"IMGR.MinLeaseDuration=250ms",
		"IMGR.LeaseInterruptInterval=250ms",
		"IMGR.LeaseInterruptLimit=5",
		"IMGR.LeaseEvictLowLimit=100000",
		"IMGR.LeaseEvictHighLimit=100010",

		"IMGR.SwiftRetryDelay=100ms",
		"IMGR.SwiftRetryExpBackoff=2",
		"IMGR.SwiftRetryLimit=4",

		"IMGR.SwiftTimeout=10m",
		"IMGR.SwiftConnectionPoolSize=128",

		"IMGR.ParallelObjectDeletePerVolumeLimit=100",

		"IMGR.InodeTableCacheEvictLowLimit=10000",
		"IMGR.InodeTableCacheEvictHighLimit=10010",

		"IMGR.InodeTableMaxInodesPerBPlusTreePage=2048",
		"IMGR.RootDirMaxDirEntriesPerBPlusTreePage=1024",

		"IMGR.LogFilePath=",
		"IMGR.LogToConsole=true",
		"IMGR.TraceEnabled=false",

		"ISWIFT.SwiftProxyIPAddr=" + testIPAddr,
		"ISWIFT.SwiftProxyTCPPort=" + fmt.Sprintf("%d", testSwiftProxyTCPPort),

		"ISWIFT.MaxAccountNameLength=256",
		"ISWIFT.MaxContainerNameLength=256",
		"ISWIFT.MaxObjectNameLength=1024",
		"ISWIFT.AccountListingLimit=10000",
		"ISWIFT.ContainerListingLimit=10000",
	}

	testGlobals.confMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings(confStrings) failed: %v", err)
	}

	err = iswiftpkg.Start(testGlobals.confMap)
	if nil != err {
		t.Fatalf("iswiftpkg.Start(testGlobals.confMap) failed: %v", err)
	}

	authRequestHeaders = make(http.Header)

	authRequestHeaders["X-Auth-User"] = []string{testSwiftAuthUser}
	authRequestHeaders["X-Auth-Key"] = []string{testSwiftAuthKey}

	authResponseHeaders, _, err = testDoHTTPRequest("GET", testGlobals.authURL, authRequestHeaders, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"GET\", testGlobals.authURL, authRequestHeaders, nil) failed: %v", err)
	}

	testGlobals.authToken = authResponseHeaders.Get("X-Auth-Token")
	testGlobals.accountURL = authResponseHeaders.Get("X-Storage-Url")

	expectedAccountURL = fmt.Sprintf("http://%s:%d/v1/%s", testIPAddr, testSwiftProxyTCPPort, testAccount)

	if expectedAccountURL != testGlobals.accountURL {
		t.Fatalf("expectedAccountURL: %s but X-Storage-Url: %s", expectedAccountURL, testGlobals.accountURL)
	}

	testGlobals.containerURL = testGlobals.accountURL + "/" + testContainer

	putAccountRequestHeaders = make(http.Header)

	putAccountRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}

	_, _, err = testDoHTTPRequest("PUT", testGlobals.accountURL, putAccountRequestHeaders, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.accountURL, putAccountRequestHeaders, nil) failed: %v", err)
	}

	putContainerRequestHeaders = make(http.Header)

	putContainerRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}

	_, _, err = testDoHTTPRequest("PUT", testGlobals.containerURL, putContainerRequestHeaders, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.storageURL, putContainerRequestHeaders, nil) failed: %v", err)
	}

	err = imgrpkg.Start(testGlobals.confMap)
	if nil != err {
		t.Fatalf("imgrpkg.Start(testGlobals.confMap) failed: %v", err)
	}

	postAndPutVolumePayload = fmt.Sprintf("{\"StorageURL\":\"%s\",\"AuthToken\":\"%s\"}", testGlobals.containerURL, testGlobals.authToken)

	_, _, err = testDoHTTPRequest("POST", testGlobals.imgrHTTPServerURL+"/volume", nil, strings.NewReader(postAndPutVolumePayload))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"POST\", testGlobals.imgrHTTPServerURL+\"/volume\", nil, strings.NewReader(postAndPutVolumePayload)) failed: %v", err)
	}

	_, _, err = testDoHTTPRequest("PUT", testGlobals.imgrHTTPServerURL+"/volume/"+testVolume, nil, strings.NewReader(postAndPutVolumePayload))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.imgrHTTPServerURL+\"/volume/\"+testVolume, nil, strings.NewReader(postAndPutVolumePayload)) failed: %v", err)
	}
}

func testTeardown(t *testing.T) {
	var (
		err error
	)

	err = imgrpkg.Stop()
	if nil != err {
		t.Fatalf("imgrpkg.Stop() failed: %v", err)
	}

	err = iswiftpkg.Stop()
	if nil != err {
		t.Fatalf("iswiftpkg.Stop() failed: %v", err)
	}

	err = os.RemoveAll(testGlobals.tempDir)
	if nil != err {
		t.Fatalf("os.RemoveAll(testGlobals.tempDir) failed: %v", err)
	}

	testGlobals = nil
}

func testDoHTTPRequest(method string, url string, requestHeaders http.Header, requestBody io.Reader) (responseHeaders http.Header, responseBody []byte, err error) {
	var (
		headerKey    string
		headerValues []string
		httpRequest  *http.Request
		httpResponse *http.Response
	)

	httpRequest, err = http.NewRequest(method, url, requestBody)
	if nil != err {
		err = fmt.Errorf("http.NewRequest(\"%s\", \"%s\", nil) failed: %v", method, url, err)
		return
	}

	if nil != requestHeaders {
		for headerKey, headerValues = range requestHeaders {
			httpRequest.Header[headerKey] = headerValues
		}
	}

	httpResponse, err = http.DefaultClient.Do(httpRequest)
	if nil != err {
		err = fmt.Errorf("http.Do(httpRequest) failed: %v", err)
		return
	}

	responseBody, err = ioutil.ReadAll(httpResponse.Body)
	if nil != err {
		err = fmt.Errorf("ioutil.ReadAll(httpResponse.Body) failed: %v", err)
		return
	}
	err = httpResponse.Body.Close()
	if nil != err {
		err = fmt.Errorf("httpResponse.Body.Close() failed: %v", err)
		return
	}

	if (200 > httpResponse.StatusCode) || (299 < httpResponse.StatusCode) {
		err = fmt.Errorf("httpResponse.StatusCode unexpected: %s", httpResponse.Status)
		return
	}

	responseHeaders = httpResponse.Header

	err = nil
	return
}
