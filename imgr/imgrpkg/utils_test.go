// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"crypto/x509/pkix"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/icert/icertpkg"
	"github.com/NVIDIA/proxyfs/iswift/iswiftpkg"
	"github.com/NVIDIA/proxyfs/retryrpc"
)

const (
	testIPAddr             = "127.0.0.1" // Don't use IPv6... the code doesn't properly "join" this with :port #s
	testRetryRPCPort       = 32356
	testHTTPServerPort     = 15346
	testSwiftProxyTCPPort  = 8080
	testSwiftAuthUser      = "test"
	testSwiftAuthKey       = "test"
	testContainer          = "testContainer"
	testVolume             = "testVolume"
	testRPCDeadlineIO      = "60s"
	testRPCKeepAlivePeriod = "60s"
)

type testGlobalsStruct struct {
	tempDir              string
	caCertFile           string
	caKeyFile            string
	caCertPEMBlock       []byte
	caKeyPEMBlock        []byte
	endpointCertFile     string
	endpointKeyFile      string
	endpointCertPEMBlock []byte
	endpointKeyPEMBlock  []byte
	confMap              conf.ConfMap
	httpServerURL        string
	authURL              string
	authToken            string
	accountURL           string
	containerURL         string
	retryrpcClientConfig *retryrpc.ClientConfig
}

var testGlobals *testGlobalsStruct

func testSetup(t *testing.T, retryrpcCallbacks interface{}) {
	var (
		confStrings                []string
		err                        error
		putAccountRequestHeaders   http.Header
		putContainerRequestHeaders http.Header
		retryrpcDeadlineIO         time.Duration
		retryrpcKeepAlivePeriod    time.Duration
		tempDir                    string
	)

	tempDir, err = ioutil.TempDir("", "imgrpkg_test")
	if nil != err {
		t.Fatalf("ioutil.TempDir(\"\", \"imgrpkg_test\") failed: %v", err)
	}

	testGlobals = &testGlobalsStruct{
		tempDir:          tempDir,
		caCertFile:       tempDir + "/caCertFile",
		caKeyFile:        tempDir + "/caKeyFile",
		endpointCertFile: tempDir + "/endpoingCertFile",
		endpointKeyFile:  tempDir + "/endpointKeyFile",
		httpServerURL:    fmt.Sprintf("http://%s:%d", testIPAddr, testHTTPServerPort),
		authURL:          fmt.Sprintf("http://%s:%d/auth/v1.0", testIPAddr, testSwiftProxyTCPPort),
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

	confStrings = []string{
		"IMGR.PublicIPAddr=" + testIPAddr,
		"IMGR.PrivateIPAddr=" + testIPAddr,
		"IMGR.RetryRPCPort=" + fmt.Sprintf("%d", testRetryRPCPort),
		"IMGR.HTTPServerPort=" + fmt.Sprintf("%d", testHTTPServerPort),

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

		"IMGR.ParallelObjectDeletePerVolumeLimit=10",

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

	err = testDoAuth()
	if nil != err {
		t.Fatalf("testDoAuth() failed: %v", err)
	}

	testGlobals.containerURL = testGlobals.accountURL + "/" + testContainer

	putAccountRequestHeaders = make(http.Header)

	putAccountRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}

	_, _, err = testDoHTTPRequest("PUT", testGlobals.accountURL, putAccountRequestHeaders, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.accountURL, putAccountRequestHeaders) failed: %v", err)
	}

	putContainerRequestHeaders = make(http.Header)

	putContainerRequestHeaders["X-Auth-Token"] = []string{testGlobals.authToken}

	_, _, err = testDoHTTPRequest("PUT", testGlobals.containerURL, putContainerRequestHeaders, nil)
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.storageURL, putContainerRequestHeaders) failed: %v", err)
	}

	err = Start(testGlobals.confMap)
	if nil != err {
		t.Fatalf("Start(testGlobals.confMap) failed: %v", err)
	}

	retryrpcDeadlineIO, err = time.ParseDuration(testRPCDeadlineIO)
	if nil != err {
		t.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRPCDeadlineIO, err)
	}
	retryrpcKeepAlivePeriod, err = time.ParseDuration(testRPCKeepAlivePeriod)
	if nil != err {
		t.Fatalf("time.ParseDuration(\"%s\") failed: %v", retryrpcKeepAlivePeriod, err)
	}

	testGlobals.retryrpcClientConfig = &retryrpc.ClientConfig{
		DNSOrIPAddr:              testIPAddr,
		Port:                     testRetryRPCPort,
		RootCAx509CertificatePEM: testGlobals.caCertPEMBlock,
		Callbacks:                retryrpcCallbacks,
		DeadlineIO:               retryrpcDeadlineIO,
		KeepAlivePeriod:          retryrpcKeepAlivePeriod,
	}
}

func testTeardown(t *testing.T) {
	var (
		err error
	)

	err = Stop()
	if nil != err {
		t.Fatalf("Stop() failed: %v", err)
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

func testDoAuth() (err error) {
	var (
		authRequestHeaders  http.Header
		authResponseHeaders http.Header
	)

	authRequestHeaders = make(http.Header)

	authRequestHeaders["X-Auth-User"] = []string{testSwiftAuthUser}
	authRequestHeaders["X-Auth-Key"] = []string{testSwiftAuthKey}

	authResponseHeaders, _, err = testDoHTTPRequest("GET", testGlobals.authURL, authRequestHeaders, nil)
	if nil == err {
		testGlobals.authToken = authResponseHeaders.Get("X-Auth-Token")
		testGlobals.accountURL = authResponseHeaders.Get("X-Storage-Url")
	}

	return
}
