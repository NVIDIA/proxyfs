package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/swiftstack/ProxyFS/jrpcfs"
)

const (
	testAuthToken      = "AUTH_tkTestToken"
	testSwiftProxyAddr = "localhost:38080"
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

type testGlobalsStruct struct {
	sync.Mutex
	sync.WaitGroup
	t         *testing.T
	server    *http.Server
	container map[string]*testContainerStruct // key == testContainerStruct.name
}

var testGlobals testGlobalsStruct

func startSwiftProxyEmulator(t *testing.T) {
	testGlobals.t = t

	testGlobals.server = &http.Server{
		Addr:    testSwiftProxyAddr,
		Handler: &testGlobals,
	}

	testGlobals.Add(1)

	go func() {
		_ = testGlobals.server.ListenAndServe()

		testGlobals.Done()
	}()
}

func stopSwiftProxyEmulator() {
	var (
		err error
	)

	err = testGlobals.server.Shutdown(context.Background())
	if nil != err {
		testGlobals.t.Fatalf("testGlobals.server.Shutdown() failed: %v", err)
	}

	testGlobals.Wait()
}

func (dummy *testGlobalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
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
		doGet(responseWriter, request)
	case http.MethodPut:
		doPut(responseWriter, request)
	case "PROXYFS":
		doRpc(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func parsePath(path string) (containerName string, objectName string, ok bool) {
	var (
		pathSplit []string
	)

	pathSplit = strings.Split(path, "/")

	if 5 == len(pathSplit) {
		containerName = pathSplit[3]
		objectName = pathSplit[4]
		ok = true
	} else {
		ok = false
	}

	return
}

func doGet(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		container             *testContainerStruct
		containerName         string
		err                   error
		object                *testObjectStruct
		objectName            string
		ok                    bool
		rangeHeader           string
		rangeHeaderBytesSplit []string
		startOffset           uint64
		startOffsetSupplied   bool
		stopOffset            uint64
		stopOffsetSupplied    bool
	)

	if request.Header.Get("X-Bypass-Proxyfs") != "true" {
		responseWriter.WriteHeader(http.StatusForbidden)
		return
	}

	containerName, objectName, ok = parsePath(request.URL.Path)
	if !ok {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	testGlobals.Lock()

	container, ok = testGlobals.container[containerName]
	if !ok {
		testGlobals.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	container.Lock()
	testGlobals.Unlock()

	object, ok = container.object[objectName]
	if !ok {
		container.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	object.Lock()
	container.Unlock()

	rangeHeader = request.Header.Get("Range")

	if "" == rangeHeader {
		responseWriter.Header().Add("Content-Type", "application/octet-stream")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(object.contents)
		object.Unlock()
		return
	}

	if !strings.HasPrefix(rangeHeader, "bytes=") {
		object.Unlock()
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	rangeHeaderBytesSplit = strings.Split(rangeHeader[len("bytes="):], "-")

	if 2 != len(rangeHeaderBytesSplit) {
		object.Unlock()
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	if "" == rangeHeaderBytesSplit[0] {
		startOffsetSupplied = false
	} else {
		startOffsetSupplied = true
		startOffset, err = strconv.ParseUint(rangeHeaderBytesSplit[0], 10, 64)
		if nil != err {
			object.Unlock()
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	if "" == rangeHeaderBytesSplit[1] {
		stopOffsetSupplied = false
	} else {
		stopOffsetSupplied = true
		stopOffset, err = strconv.ParseUint(rangeHeaderBytesSplit[1], 10, 64)
		if nil != err {
			object.Unlock()
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	if startOffsetSupplied {
		if stopOffsetSupplied {
			if (stopOffset + 1) > uint64(len(object.contents)) {
				stopOffset = uint64(len(object.contents)) - 1
			}
		} else {
			stopOffset = uint64(len(object.contents)) - 1
		}
	} else {
		if stopOffsetSupplied {
			startOffset = uint64(len(object.contents)) - stopOffset
			stopOffset = uint64(len(object.contents)) - 1
		} else {
			object.Unlock()
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	responseWriter.Header().Add("Content-Type", "application/octet-stream")
	responseWriter.Header().Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startOffset, stopOffset, len(object.contents)))
	responseWriter.WriteHeader(http.StatusPartialContent)
	_, _ = responseWriter.Write(object.contents)

	object.Unlock()

	return
}

func doPut(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		container     *testContainerStruct
		containerName string
		contents      []byte
		err           error
		object        *testObjectStruct
		objectName    string
		ok            bool
	)

	contents, err = ioutil.ReadAll(request.Body)
	_ = request.Body.Close()
	if nil != err {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	if request.Header.Get("X-Bypass-Proxyfs") != "true" {
		responseWriter.WriteHeader(http.StatusForbidden)
		return
	}

	containerName, objectName, ok = parsePath(request.URL.Path)
	if !ok {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	testGlobals.Lock()

	container, ok = testGlobals.container[containerName]
	if !ok {
		testGlobals.Unlock()
		responseWriter.WriteHeader(http.StatusForbidden)
		return
	}

	container.Lock()
	testGlobals.Unlock()

	object, ok = container.object[objectName]
	if ok {
		container.Unlock()
		responseWriter.WriteHeader(http.StatusForbidden)
		return
	}

	object = &testObjectStruct{
		name:     objectName,
		contents: contents,
	}

	container.object[objectName] = object

	container.Unlock()

	responseWriter.WriteHeader(http.StatusCreated)
}

func doRpc(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err            error
		jrpcReplyBuf   []byte
		jrpcRequestBuf []byte
		requestID      uint64
		requestMethod  string
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

	requestMethod, requestID, err = jrpcUnmarshalRequestForMethodAndID(jrpcRequestBuf)
	if nil != err {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	switch requestMethod {
	case "Server.RpcPing":
		pingReq := &jrpcfs.PingReq{}
		err = jrpcUnmarshalRequest(requestID, jrpcRequestBuf, pingReq)
		if nil != err {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		pingReply := &jrpcfs.PingReply{
			Message: pingReq.Message,
		}
		jrpcReplyBuf, err = jrpcMarshalResponse(requestID, nil, pingReply)
		if nil != err {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
	default:
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	responseWriter.Header().Add("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write(jrpcReplyBuf)
}
