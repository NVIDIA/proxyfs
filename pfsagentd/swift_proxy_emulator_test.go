package main

import (
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
)

const (
	testAuthToken           = "AUTH_tkTestToken"
	testSwiftProxyAddr      = "localhost:38080"
	testJrpcResponseBufSize = 1024 * 1024
)

type testObjectStruct struct {
	sync.Mutex
	name     string
	contents []byte
}

type testContainerStruct struct {
	sync.Mutex
	name   string
	object map[string]*testObjectStruct // key == testObjectStruct.name
}

type testSwiftProxyEmulatorGlobalsStruct struct {
	t                   *testing.T
	ramswiftNoAuthURL   string
	proxyfsdJrpcTCPAddr *net.TCPAddr
	jrpcResponsePool    *sync.Pool
	httpClient          *http.Client
	httpServer          *http.Server

	// UNDO: These should go away when I'm no longer emulating ramswift & proxyfsd here
	sync.Mutex
	sync.WaitGroup
	container map[string]*testContainerStruct // key == testContainerStruct.name
}

var testSwiftProxyEmulatorGlobals testSwiftProxyEmulatorGlobalsStruct

func startSwiftProxyEmulator(t *testing.T, confMap conf.ConfMap) {
	var (
		err                      error
		jrpcServerIPAddr         string
		jrpcServerTCPPort        uint16
		swiftClientNoAuthIPAddr  string
		swiftClientNoAuthTCPPort uint16
		whoAmI                   string
	)

	testSwiftProxyEmulatorGlobals.t = t

	swiftClientNoAuthIPAddr, err = confMap.FetchOptionValueString("SwiftClient", "NoAuthIPAddr")
	if nil != err {
		t.Fatal(err)
	}

	swiftClientNoAuthTCPPort, err = confMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		t.Fatal(err)
	}

	testSwiftProxyEmulatorGlobals.ramswiftNoAuthURL = "http://" + net.JoinHostPort(swiftClientNoAuthIPAddr, strconv.FormatUint(uint64(swiftClientNoAuthTCPPort), 10)) + "/"

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		t.Fatal(err)
	}

	jrpcServerIPAddr, err = confMap.FetchOptionValueString("Peer:"+whoAmI, "PrivateIPAddr")
	if nil != err {
		t.Fatal(err)
	}

	jrpcServerTCPPort, err = confMap.FetchOptionValueUint16("JSONRPCServer", "TCPPort")
	if nil != err {
		t.Fatal(err)
	}

	testSwiftProxyEmulatorGlobals.proxyfsdJrpcTCPAddr, err = net.ResolveTCPAddr("tcp", net.JoinHostPort(jrpcServerIPAddr, strconv.FormatUint(uint64(jrpcServerTCPPort), 10)))
	if nil != err {
		t.Fatal(err)
	}

	testSwiftProxyEmulatorGlobals.httpClient = &http.Client{}

	testSwiftProxyEmulatorGlobals.httpServer = &http.Server{
		Addr:    testSwiftProxyAddr,
		Handler: &testSwiftProxyEmulatorGlobals,
	}

	testSwiftProxyEmulatorGlobals.jrpcResponsePool = &sync.Pool{
		New: func() (bufAsInterface interface{}) {
			var (
				bufAsByteSlice []byte
			)

			bufAsByteSlice = make([]byte, testJrpcResponseBufSize)

			bufAsInterface = bufAsByteSlice

			return
		},
	}

	testSwiftProxyEmulatorGlobals.Add(1)

	go func() {
		_ = testSwiftProxyEmulatorGlobals.httpServer.ListenAndServe()

		testSwiftProxyEmulatorGlobals.Done()
	}()
}

func stopSwiftProxyEmulator() {
	var (
		err error
	)

	err = testSwiftProxyEmulatorGlobals.httpServer.Shutdown(context.Background())
	if nil != err {
		testSwiftProxyEmulatorGlobals.t.Fatalf("testSwiftProxyEmulatorGlobals.httpServer.Shutdown() failed: %v", err)
	}

	testSwiftProxyEmulatorGlobals.Wait()

	testSwiftProxyEmulatorGlobals.jrpcResponsePool = nil
}

func (dummy *testSwiftProxyEmulatorGlobalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	// Handle the AuthURL case

	if (http.MethodGet == request.Method) && ("/auth/v1.0" == request.URL.Path) {
		if request.Header.Get("X-Auth-User") != globals.config.SwiftAuthUser {
			responseWriter.WriteHeader(http.StatusUnauthorized)
			return
		}
		if request.Header.Get("X-Auth-Key") != globals.config.SwiftAuthKey {
			responseWriter.WriteHeader(http.StatusUnauthorized)
			return
		}
		responseWriter.Header().Add("X-Auth-Token", testAuthToken)
		responseWriter.Header().Add("X-Storage-Url", "http://"+testSwiftProxyAddr+"/v1/"+globals.config.SwiftAccountName)
		responseWriter.WriteHeader(http.StatusOK)
		return
	}

	// Exclude non-emulated paths

	if !strings.HasPrefix(request.URL.Path, "/v1/"+globals.config.SwiftAccountName) {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	// Reject unauthorized requests

	if request.Header.Get("X-Auth-Token") != testAuthToken {
		responseWriter.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Branch off to individual request method handlers

	switch request.Method {
	case http.MethodGet:
		doGET(responseWriter, request)
	case http.MethodPut:
		doPUT(responseWriter, request)
	case "PROXYFS":
		doRPC(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// doGET proxies the GET over to ramswift
//
func doGET(authResponseWriter http.ResponseWriter, authRequest *http.Request) {
	var (
		contentRangeHeader string
		contentTypeHeader  string
		err                error
		getBuf             []byte
		hostHeader         string
		noAuthRequest      *http.Request
		noAuthResponse     *http.Response
		noAuthStatusCode   int
		rangeHeader        string
	)

	if authRequest.Header.Get("X-Bypass-Proxyfs") != "true" {
		authResponseWriter.WriteHeader(http.StatusForbidden)
		return
	}

	noAuthRequest, err = http.NewRequest("GET", authRequest.URL.Path, nil)
	if nil != err {
		authResponseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	hostHeader = authRequest.Header.Get("Host")
	if "" != hostHeader {
		noAuthRequest.Header.Add("Host", hostHeader)
	}

	rangeHeader = authRequest.Header.Get("Range")
	if "" != rangeHeader {
		noAuthRequest.Header.Add("Range", rangeHeader)
	}

	noAuthResponse, err = testSwiftProxyEmulatorGlobals.httpClient.Do(noAuthRequest)
	if nil != err {
		authResponseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	noAuthStatusCode = noAuthResponse.StatusCode

	if (http.StatusOK != noAuthStatusCode) && (http.StatusPartialContent != noAuthStatusCode) {
		_ = noAuthResponse.Body.Close()
		authResponseWriter.WriteHeader(noAuthStatusCode)
		return
	}

	getBuf, err = ioutil.ReadAll(noAuthResponse.Body)
	if nil != err {
		_ = noAuthResponse.Body.Close()
		authResponseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	err = noAuthResponse.Body.Close()
	if nil != err {
		authResponseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	contentTypeHeader = noAuthResponse.Header.Get("Content-Type")
	if "" != contentTypeHeader {
		authResponseWriter.Header().Add("Content-Type", contentTypeHeader)
	}

	contentRangeHeader = noAuthResponse.Header.Get("Content-Range")
	if "" != contentRangeHeader {
		authResponseWriter.Header().Add("Content-Range", contentRangeHeader)
	}

	authResponseWriter.WriteHeader(noAuthStatusCode)

	_, _ = authResponseWriter.Write(getBuf)
}

// doPUT proxies the GET over to ramswift
//
func doPUT(authResponseWriter http.ResponseWriter, authRequest *http.Request) {
	var (
		err            error
		hostHeader     string
		noAuthRequest  *http.Request
		noAuthResponse *http.Response
	)

	if authRequest.Header.Get("X-Bypass-Proxyfs") != "true" {
		_ = authRequest.Body.Close()
		authResponseWriter.WriteHeader(http.StatusForbidden)
		return
	}

	noAuthRequest, err = http.NewRequest("PUT", authRequest.URL.Path, authRequest.Body)
	if nil != err {
		_ = authRequest.Body.Close()
		authResponseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	hostHeader = authRequest.Header.Get("Host")
	if "" != hostHeader {
		noAuthRequest.Header.Add("Host", hostHeader)
	}

	noAuthResponse, err = testSwiftProxyEmulatorGlobals.httpClient.Do(noAuthRequest)
	if nil != err {
		_ = authRequest.Body.Close()
		authResponseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	err = authRequest.Body.Close()
	if nil != err {
		authResponseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	authResponseWriter.WriteHeader(noAuthResponse.StatusCode)
}

// doRPC proxies the payload as a JSON RPC request over to proxyfsd
//
func doRPC(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err             error
		jrpcResponseBuf []byte
		jrpcResponseLen int
		jrpcRequestBuf  []byte
		tcpConn         *net.TCPConn
	)

	jrpcRequestBuf, err = ioutil.ReadAll(request.Body)
	_ = request.Body.Close()
	if nil != err {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	if request.Header.Get("Content-Type") != "application/json" {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	tcpConn, err = net.DialTCP("tcp", nil, testSwiftProxyEmulatorGlobals.proxyfsdJrpcTCPAddr)
	if nil != err {
		responseWriter.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	_, err = tcpConn.Write(jrpcRequestBuf)
	if nil != err {
		_ = tcpConn.Close()
		responseWriter.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	jrpcResponseBuf = testSwiftProxyEmulatorGlobals.jrpcResponsePool.Get().([]byte)

	jrpcResponseLen, err = tcpConn.Read(jrpcResponseBuf)
	if nil != err {
		_ = tcpConn.Close()
		responseWriter.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	err = tcpConn.Close()
	if nil != err {
		responseWriter.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	jrpcResponseBuf = jrpcResponseBuf[:jrpcResponseLen]

	responseWriter.Header().Add("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write(jrpcResponseBuf)

	testSwiftProxyEmulatorGlobals.jrpcResponsePool.Put(jrpcResponseBuf)
}
