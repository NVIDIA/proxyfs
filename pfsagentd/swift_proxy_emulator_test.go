package main

import (
	"context"
	"encoding/json"
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
	var (
		authKeyHeader   []string
		authTokenHeader []string
		authUserHeader  []string
		ok              bool
	)

	// Handle the AuthURL case

	if (http.MethodGet == request.Method) && ("/auth/v1.0" == request.URL.Path) {
		authUserHeader, ok = request.Header["X-Auth-User"]
		if !ok || (1 != len(authUserHeader)) || (globals.config.SwiftAuthUser != authUserHeader[0]) {
			responseWriter.WriteHeader(http.StatusUnauthorized)
			return
		}
		authKeyHeader, ok = request.Header["X-Auth-Key"]
		if !ok || (1 != len(authKeyHeader)) || (globals.config.SwiftAuthKey != authKeyHeader[0]) {
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

	authTokenHeader, ok = request.Header["X-Auth-Token"]
	if !ok || (1 != len(authTokenHeader)) || (testAuthToken != authTokenHeader[0]) {
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
		rangeHeader           []string
		rangeHeaderBytesSplit []string
		startOffset           uint64
		startOffsetSupplied   bool
		stopOffset            uint64
		stopOffsetSupplied    bool
	)

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

	rangeHeader, ok = request.Header["Range"]

	if !ok {
		responseWriter.Header().Add("Content-Type", "application/octet-stream")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(object.contents)
		object.Unlock()
		return
	}

	if (1 != len(rangeHeader)) || !strings.HasPrefix(rangeHeader[0], "bytes=") {
		object.Unlock()
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	rangeHeaderBytesSplit = strings.Split(rangeHeader[0][len("bytes="):], "-")

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
		contentTypeHeader  []string
		err                error
		jrpcGenericRequest JrpcGenericRequestStruct
		jrpcPingReply      *JrpcPingReplyStruct
		jrpcPingReq        *JrpcPingReqStruct
		jrpcReplyBuf       []byte
		jrpcRequestBuf     []byte
		ok                 bool
	)

	jrpcRequestBuf, err = ioutil.ReadAll(request.Body)
	_ = request.Body.Close()
	if nil != err {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	contentTypeHeader, ok = request.Header["Content-Type"]
	if !ok || (1 != len(contentTypeHeader)) || ("application/json" != contentTypeHeader[0]) {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	err = json.Unmarshal(jrpcRequestBuf, &jrpcGenericRequest)
	if (nil != err) || ("2.0" != jrpcGenericRequest.JSONrpc) {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	switch jrpcGenericRequest.Method {
	case "Server.RpcPing":
		jrpcPingReq = &JrpcPingReqStruct{}
		err = json.Unmarshal(jrpcRequestBuf, jrpcPingReq)
		if (nil != err) || (1 != len(jrpcPingReq.Params)) {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		jrpcPingReply = &JrpcPingReplyStruct{
			ID:     jrpcPingReq.ID,
			Error:  "",
			Result: jrpcfs.PingReply{Message: jrpcPingReq.Params[0].Message},
		}

		jrpcReplyBuf, err = json.Marshal(jrpcPingReply)
		if nil != err {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		responseWriter.WriteHeader(http.StatusNotImplemented)
	default:
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	responseWriter.Header().Add("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusBadRequest)
	_, _ = responseWriter.Write(jrpcReplyBuf)
}
