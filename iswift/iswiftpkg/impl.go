// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iswiftpkg

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/utils"
)

type swiftAccountStruct struct {
	name               string
	headers            http.Header
	swiftContainerTree sortedmap.LLRBTree // key is swiftContainerStruct.name; value is *swiftContainerStruct
}

type swiftContainerStruct struct {
	name            string
	swiftAccount    *swiftAccountStruct // back-reference to swiftAccountStruct
	headers         http.Header
	swiftObjectTree sortedmap.LLRBTree //  key is swiftObjectStruct.name; value is *swiftObjectStruct
}

type swiftObjectStruct struct {
	name           string
	swiftContainer *swiftContainerStruct // back-reference to swiftContainerStruct
	headers        http.Header
	contents       []byte
}

const (
	startGETInfoMaxRetries = 10
	startGETInfoRetryDelay = 100 * time.Millisecond
)

const (
	fixedAuthTokenPrefix     = "AUTH_tk0123456789abcde012345678" // To be appended with %08x formatted uint32 nonce
	fixedUserToAccountPrefix = "AUTH_"                           // Prefixed to User truncated before colon (":") if necessary
)

type rangeStruct struct {
	startOffset uint64
	stopOffset  uint64
}

type stringSet map[string]bool

var headerNameIgnoreSet = stringSet{"Accept": true, "Accept-Encoding": true, "User-Agent": true, "Content-Length": true}

type configStruct struct {
	SwiftProxyIPAddr  string
	SwiftProxyTCPPort uint16

	MaxAccountNameLength   uint64
	MaxContainerNameLength uint64
	MaxObjectNameLength    uint64
	AccountListingLimit    uint64
	ContainerListingLimit  uint64
}

type globalsStruct struct {
	sync.Mutex
	config             configStruct
	emulatorHTTPServer *http.Server
	emulatorWG         sync.WaitGroup
	currentAuthNonce   uint32                         // incremented each call to forceReAuth()
	currentAuthToken   string                         // fmt.Sprintf("%s%08x", fixedAuthTokenPrefix, currentAuthNonce)
	swiftAccountMap    map[string]*swiftAccountStruct // key is swiftAccountStruct.name; value is *swiftAccountStruct
}

var globals globalsStruct

func start(confMap conf.ConfMap) (err error) {
	err = initializeGlobals(confMap)
	if nil != err {
		return
	}

	err = startEmulator()
	if nil != err {
		return
	}

	return
}

func stop() (err error) {
	err = stopEmulator()
	if nil != err {
		return
	}

	uninitializeGlobals()

	err = nil
	return
}

func forceReAuth() {
	globals.Lock()
	globals.currentAuthNonce++ // ignore overflow/wrap
	globals.currentAuthToken = fmt.Sprintf("%s%08x", fixedAuthTokenPrefix, globals.currentAuthNonce)
	globals.Unlock()
}

func getCurrentAuthToken() (currentAuthToken string) {
	globals.Lock()
	currentAuthToken = globals.currentAuthToken
	globals.Unlock()

	return
}

func initializeGlobals(confMap conf.ConfMap) (err error) {
	globals.config.SwiftProxyIPAddr, err = confMap.FetchOptionValueString("ISWIFT", "SwiftProxyIPAddr")
	if nil != err {
		return
	}
	globals.config.SwiftProxyTCPPort, err = confMap.FetchOptionValueUint16("ISWIFT", "SwiftProxyTCPPort")
	if nil != err {
		return
	}

	globals.config.MaxAccountNameLength, err = confMap.FetchOptionValueUint64("ISWIFT", "MaxAccountNameLength")
	if nil != err {
		return
	}
	globals.config.MaxContainerNameLength, err = confMap.FetchOptionValueUint64("ISWIFT", "MaxContainerNameLength")
	if nil != err {
		return
	}
	globals.config.MaxObjectNameLength, err = confMap.FetchOptionValueUint64("ISWIFT", "MaxObjectNameLength")
	if nil != err {
		return
	}
	globals.config.AccountListingLimit, err = confMap.FetchOptionValueUint64("ISWIFT", "AccountListingLimit")
	if nil != err {
		return
	}
	globals.config.ContainerListingLimit, err = confMap.FetchOptionValueUint64("ISWIFT", "ContainerListingLimit")
	if nil != err {
		return
	}

	globals.currentAuthNonce = 0
	globals.currentAuthToken = fmt.Sprintf("%s%08x", fixedAuthTokenPrefix, globals.currentAuthNonce)

	globals.swiftAccountMap = make(map[string]*swiftAccountStruct)

	return
}

func uninitializeGlobals() {
	globals.config.SwiftProxyIPAddr = ""
	globals.config.SwiftProxyTCPPort = 0

	globals.config.MaxAccountNameLength = 0
	globals.config.MaxContainerNameLength = 0
	globals.config.MaxObjectNameLength = 0
	globals.config.AccountListingLimit = 0
	globals.config.ContainerListingLimit = 0

	globals.swiftAccountMap = make(map[string]*swiftAccountStruct)
}

func startEmulator() (err error) {
	var (
		startGETInfoNumRetries int
	)

	globals.emulatorHTTPServer = &http.Server{
		Addr:    net.JoinHostPort(globals.config.SwiftProxyIPAddr, fmt.Sprintf("%d", globals.config.SwiftProxyTCPPort)),
		Handler: &globals,
	}

	globals.emulatorWG.Add(1)

	go func() {
		_ = globals.emulatorHTTPServer.ListenAndServe()
		globals.emulatorWG.Done()
	}()

	startGETInfoNumRetries = 0

	for {
		_, err = http.Get("http://" + globals.emulatorHTTPServer.Addr + "/info")
		if nil == err {
			break
		}
		startGETInfoNumRetries++
		if startGETInfoNumRetries > startGETInfoMaxRetries {
			_ = stopEmulator()
			err = fmt.Errorf("startEmulator() failed to establish that emulator is up")
			return
		}
		time.Sleep(startGETInfoRetryDelay)
	}

	err = nil
	return
}

func stopEmulator() (err error) {
	err = globals.emulatorHTTPServer.Close()
	if nil != err {
		return
	}

	globals.emulatorWG.Wait()

	err = nil
	return
}

func (dummy *globalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		xAuthKey               string
		xAuthUser              string
		xAuthUserSplit2OnColon []string
		xStorageURL            string
	)

	// Handle the GET of/on info & AuthURL cases

	if http.MethodGet == request.Method {
		switch request.URL.Path {
		case "/info":
			_ = request.Body.Close()
			doGET(responseWriter, request)
			return
		case "/auth/v1.0":
			_ = request.Body.Close()
			xAuthUser = request.Header.Get("X-Auth-User")
			xAuthKey = request.Header.Get("X-Auth-Key")
			if (xAuthUser == "") || (xAuthKey == "") {
				responseWriter.WriteHeader(http.StatusUnauthorized)
				return
			}
			xAuthUserSplit2OnColon = strings.SplitN(xAuthUser, ":", 2)
			xStorageURL = "http://" + globals.emulatorHTTPServer.Addr + "/v1/" + fixedUserToAccountPrefix + xAuthUserSplit2OnColon[0]
			responseWriter.Header().Add("X-Auth-Token", getCurrentAuthToken())
			responseWriter.Header().Add("X-Storage-Url", xStorageURL)
			return
		default:
			// Fall through to normal processing
		}
	}

	// Require X-Auth-Token to match fixedAuthToken

	if getCurrentAuthToken() != request.Header.Get("X-Auth-Token") {
		_ = request.Body.Close()
		responseWriter.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Require "version" portion of request.URL.Path to be "v1"

	if !strings.HasPrefix(request.URL.Path, "/v1/") {
		_ = request.Body.Close()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	// Branch off to individual request method handlers

	switch request.Method {
	case http.MethodDelete:
		doDELETE(responseWriter, request)
	case http.MethodGet:
		doGET(responseWriter, request)
	case http.MethodHead:
		doHEAD(responseWriter, request)
	case http.MethodPost:
		doPOST(responseWriter, request)
	case http.MethodPut:
		doPUT(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (dummy *globalsStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString = fmt.Sprintf("%v", key)
	err = nil
	return
}

func (dummy *globalsStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsString = fmt.Sprintf("%v", value)
	err = nil
	return
}

func parsePath(request *http.Request) (infoOnly bool, swiftAccountName string, swiftContainerName string, swiftObjectName string) {
	var (
		pathSplit []string
	)

	infoOnly = false
	swiftAccountName = ""
	swiftContainerName = ""
	swiftObjectName = ""

	if "/info" == request.URL.Path {
		infoOnly = true
		return
	}

	if strings.HasPrefix(request.URL.Path, "/v1/") {
		pathSplit = strings.SplitN(request.URL.Path[4:], "/", 3)
		swiftAccountName = pathSplit[0]
		if 1 == len(pathSplit) {
			swiftContainerName = ""
			swiftObjectName = ""
		} else {
			swiftContainerName = pathSplit[1]
			if 2 == len(pathSplit) {
				swiftObjectName = ""
			} else {
				swiftObjectName = pathSplit[2]
			}
		}
	}

	return
}

func parseRangeHeader(request *http.Request, objectLen int) (ranges []rangeStruct, err error) {
	var (
		off                    int
		rangeHeaderValue       string
		rangeHeaderValueSuffix string
		rangeString            string
		rangeStringSlice       []string
		rangesStrings          []string
		rangesStringsIndex     int
		startOffset            int64
		stopOffset             int64
	)

	rangeHeaderValue = request.Header.Get("Range")
	if "" == rangeHeaderValue {
		ranges = make([]rangeStruct, 0)
		err = nil
		return
	}

	if !strings.HasPrefix(rangeHeaderValue, "bytes=") {
		err = fmt.Errorf("rangeHeaderValue (%v) does not start with expected \"bytes=\"", rangeHeaderValue)
		return
	}

	rangeHeaderValueSuffix = rangeHeaderValue[len("bytes="):]

	rangesStrings = strings.SplitN(rangeHeaderValueSuffix, ",", 2)

	ranges = make([]rangeStruct, len(rangesStrings))

	for rangesStringsIndex, rangeString = range rangesStrings {
		rangeStringSlice = strings.SplitN(rangeString, "-", 2)
		if 2 != len(rangeStringSlice) {
			err = fmt.Errorf("rangeHeaderValue (%v) malformed", rangeHeaderValue)
			return
		}
		if "" == rangeStringSlice[0] {
			startOffset = int64(-1)
		} else {
			off, err = strconv.Atoi(rangeStringSlice[0])
			if nil != err {
				err = fmt.Errorf("rangeHeaderValue (%v) malformed (strconv.Atoi() failure: %v)", rangeHeaderValue, err)
				return
			}
			startOffset = int64(off)
		}

		if "" == rangeStringSlice[1] {
			stopOffset = int64(-1)
		} else {
			off, err = strconv.Atoi(rangeStringSlice[1])
			if nil != err {
				err = fmt.Errorf("rangeHeaderValue (%v) malformed (strconv.Atoi() failure: %v)", rangeHeaderValue, err)
				return
			}
			stopOffset = int64(off)
		}

		if ((0 > startOffset) && (0 > stopOffset)) || (startOffset > stopOffset) {
			err = fmt.Errorf("rangeHeaderValue (%v) malformed", rangeHeaderValue)
			return
		}

		if startOffset < 0 {
			startOffset = int64(objectLen) - stopOffset
			if startOffset < 0 {
				err = fmt.Errorf("rangeHeaderValue (%v) malformed...computed startOffset negative", rangeHeaderValue)
				return
			}
			stopOffset = int64(objectLen - 1)
		} else if stopOffset < 0 {
			stopOffset = int64(objectLen - 1)
		} else {
			if stopOffset > int64(objectLen-1) {
				stopOffset = int64(objectLen - 1)
			}
		}

		ranges[rangesStringsIndex].startOffset = uint64(startOffset)
		ranges[rangesStringsIndex].stopOffset = uint64(stopOffset)
	}

	err = nil
	return
}

func locateSwiftAccount(swiftAccountName string) (swiftAccount *swiftAccountStruct, errno syscall.Errno) {
	var (
		ok bool
	)

	swiftAccount, ok = globals.swiftAccountMap[swiftAccountName]
	if !ok {
		errno = unix.ENOENT
		return
	}
	errno = 0
	return
}

func createSwiftAccount(swiftAccountName string) (swiftAccount *swiftAccountStruct, errno syscall.Errno) {
	var (
		ok bool
	)

	_, ok = globals.swiftAccountMap[swiftAccountName]
	if ok {
		errno = unix.EEXIST
		return
	}
	swiftAccount = &swiftAccountStruct{
		name:               swiftAccountName,
		headers:            make(http.Header),
		swiftContainerTree: sortedmap.NewLLRBTree(sortedmap.CompareString, &globals),
	}
	globals.swiftAccountMap[swiftAccountName] = swiftAccount
	errno = 0
	return
}

func createOrLocateSwiftAccount(swiftAccountName string) (swiftAccount *swiftAccountStruct, wasCreated bool) {
	var (
		ok bool
	)

	swiftAccount, ok = globals.swiftAccountMap[swiftAccountName]
	if ok {
		wasCreated = false
	} else {
		swiftAccount = &swiftAccountStruct{
			name:               swiftAccountName,
			headers:            make(http.Header),
			swiftContainerTree: sortedmap.NewLLRBTree(sortedmap.CompareString, &globals),
		}
		globals.swiftAccountMap[swiftAccountName] = swiftAccount
		wasCreated = true
	}
	return
}

func deleteSwiftAccount(swiftAccountName string, force bool) (errno syscall.Errno) {
	var (
		err                             error
		ok                              bool
		swiftAccount                    *swiftAccountStruct
		swiftswiftAccountContainerCount int
	)

	swiftAccount, ok = globals.swiftAccountMap[swiftAccountName]
	if ok {
		if force {
			// ok if account contains data... we'll forget it
		} else {
			swiftswiftAccountContainerCount, err = swiftAccount.swiftContainerTree.Len()
			if nil != err {
				panic(err)
			}
			if 0 != swiftswiftAccountContainerCount {
				errno = unix.ENOTEMPTY
				return
			}
		}
		delete(globals.swiftAccountMap, swiftAccountName)
	} else {
		errno = unix.ENOENT
		return
	}
	errno = 0
	return
}

func locateSwiftContainer(swiftAccount *swiftAccountStruct, swiftContainerName string) (swiftContainer *swiftContainerStruct, errno syscall.Errno) {
	var (
		err                   error
		ok                    bool
		swiftContainerAsValue sortedmap.Value
	)

	swiftContainerAsValue, ok, err = swiftAccount.swiftContainerTree.GetByKey(swiftContainerName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftContainer = swiftContainerAsValue.(*swiftContainerStruct)
	} else {
		errno = unix.ENOENT
		return
	}
	errno = 0
	return
}

func createSwiftContainer(swiftAccount *swiftAccountStruct, swiftContainerName string) (swiftContainer *swiftContainerStruct, errno syscall.Errno) {
	var (
		err error
		ok  bool
	)

	_, ok, err = swiftAccount.swiftContainerTree.GetByKey(swiftContainerName)
	if nil != err {
		panic(err)
	}
	if ok {
		errno = unix.EEXIST
		return
	} else {
		swiftContainer = &swiftContainerStruct{
			name:            swiftContainerName,
			swiftAccount:    swiftAccount,
			headers:         make(http.Header),
			swiftObjectTree: sortedmap.NewLLRBTree(sortedmap.CompareString, &globals),
		}
		_, err = swiftAccount.swiftContainerTree.Put(swiftContainerName, swiftContainer)
		if nil != err {
			panic(err)
		}
	}
	errno = 0
	return
}

func createOrLocateSwiftContainer(swiftAccount *swiftAccountStruct, swiftContainerName string) (swiftContainer *swiftContainerStruct, wasCreated bool) {
	var (
		err                   error
		ok                    bool
		swiftContainerAsValue sortedmap.Value
	)

	swiftContainerAsValue, ok, err = swiftAccount.swiftContainerTree.GetByKey(swiftContainerName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftContainer = swiftContainerAsValue.(*swiftContainerStruct)
		wasCreated = false
	} else {
		swiftContainer = &swiftContainerStruct{
			name:            swiftContainerName,
			swiftAccount:    swiftAccount,
			headers:         make(http.Header),
			swiftObjectTree: sortedmap.NewLLRBTree(sortedmap.CompareString, &globals),
		}
		_, err = swiftAccount.swiftContainerTree.Put(swiftContainerName, swiftContainer)
		if nil != err {
			panic(err)
		}
		wasCreated = true
	}
	return
}

func deleteSwiftContainer(swiftAccount *swiftAccountStruct, swiftContainerName string) (errno syscall.Errno) {
	var (
		err                       error
		ok                        bool
		swiftContainer            *swiftContainerStruct
		swiftContainerAsValue     sortedmap.Value
		swiftContainerObjectCount int
	)

	swiftContainerAsValue, ok, err = swiftAccount.swiftContainerTree.GetByKey(swiftContainerName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftContainer = swiftContainerAsValue.(*swiftContainerStruct)
		swiftContainerObjectCount, err = swiftContainer.swiftObjectTree.Len()
		if nil != err {
			panic(err)
		}
		if 0 != swiftContainerObjectCount {
			errno = unix.ENOTEMPTY
			return
		}
		_, err = swiftAccount.swiftContainerTree.DeleteByKey(swiftContainerName)
		if nil != err {
			panic(err)
		}
	} else {
		errno = unix.ENOENT
		return
	}
	errno = 0
	return
}

func locateSwiftObject(swiftContainer *swiftContainerStruct, swiftObjectName string) (swiftObject *swiftObjectStruct, errno syscall.Errno) {
	var (
		err                error
		ok                 bool
		swiftObjectAsValue sortedmap.Value
	)

	swiftObjectAsValue, ok, err = swiftContainer.swiftObjectTree.GetByKey(swiftObjectName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftObject = swiftObjectAsValue.(*swiftObjectStruct)
	} else {
		errno = unix.ENOENT
		return
	}
	errno = 0
	return
}

func createSwiftObject(swiftContainer *swiftContainerStruct, swiftObjectName string) (swiftObject *swiftObjectStruct, errno syscall.Errno) {
	var (
		err error
		ok  bool
	)

	_, ok, err = swiftContainer.swiftObjectTree.GetByKey(swiftObjectName)
	if nil != err {
		panic(err)
	}
	if ok {
		errno = unix.EEXIST
		return
	} else {
		swiftObject = &swiftObjectStruct{name: swiftObjectName, swiftContainer: swiftContainer, contents: []byte{}}
		_, err = swiftContainer.swiftObjectTree.Put(swiftObjectName, swiftObject)
		if nil != err {
			panic(err)
		}
	}
	errno = 0
	return
}

func createOrLocateSwiftObject(swiftContainer *swiftContainerStruct, swiftObjectName string) (swiftObject *swiftObjectStruct, wasCreated bool) {
	var (
		err                error
		ok                 bool
		swiftObjectAsValue sortedmap.Value
	)

	swiftObjectAsValue, ok, err = swiftContainer.swiftObjectTree.GetByKey(swiftObjectName)
	if nil != err {
		panic(err)
	}
	if ok {
		swiftObject = swiftObjectAsValue.(*swiftObjectStruct)
		wasCreated = false
	} else {
		swiftObject = &swiftObjectStruct{name: swiftObjectName, swiftContainer: swiftContainer, contents: []byte{}}
		_, err = swiftContainer.swiftObjectTree.Put(swiftObjectName, swiftObject)
		if nil != err {
			panic(err)
		}
		wasCreated = true
	}
	return
}

func deleteSwiftObject(swiftContainer *swiftContainerStruct, swiftObjectName string) (errno syscall.Errno) {
	var (
		err error
		ok  bool
	)

	_, ok, err = swiftContainer.swiftObjectTree.GetByKey(swiftObjectName)
	if nil != err {
		panic(err)
	}
	if ok {
		_, err = swiftContainer.swiftObjectTree.DeleteByKey(swiftObjectName)
		if nil != err {
			panic(err)
		}
	} else {
		errno = unix.ENOENT
		return
	}
	errno = 0
	return
}

func doDELETE(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err                error
		errno              syscall.Errno
		infoOnly           bool
		swiftAccount       *swiftAccountStruct
		swiftAccountName   string
		swiftContainer     *swiftContainerStruct
		swiftContainerName string
		swiftObjectName    string
	)

	globals.Lock()
	defer globals.Unlock()

	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName = parsePath(request)
	if infoOnly || ("" == swiftAccountName) {
		responseWriter.WriteHeader(http.StatusForbidden)
	} else {
		if "" == swiftContainerName {
			// DELETE SwiftAccount
			errno = deleteSwiftAccount(swiftAccountName, false)
			switch errno {
			case 0:
				responseWriter.WriteHeader(http.StatusNoContent)
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusNotFound)
			case unix.ENOTEMPTY:
				responseWriter.WriteHeader(http.StatusConflict)
			default:
				err = fmt.Errorf("deleteSwiftAccount(\"%v\", false) returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		} else {
			// DELETE SwiftContainer or SwiftObject
			swiftAccount, errno = locateSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				if "" == swiftObjectName {
					// DELETE SwiftContainer
					errno = deleteSwiftContainer(swiftAccount, swiftContainerName)
					switch errno {
					case 0:
						responseWriter.WriteHeader(http.StatusNoContent)
					case unix.ENOENT:
						responseWriter.WriteHeader(http.StatusNotFound)
					case unix.ENOTEMPTY:
						responseWriter.WriteHeader(http.StatusConflict)
					default:
						err = fmt.Errorf("deleteSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
						panic(err)
					}
				} else {
					// DELETE SwiftObject
					swiftContainer, errno = locateSwiftContainer(swiftAccount, swiftContainerName)
					switch errno {
					case 0:
						errno = deleteSwiftObject(swiftContainer, swiftObjectName)
						switch errno {
						case 0:
							responseWriter.WriteHeader(http.StatusNoContent)
						case unix.ENOENT:
							responseWriter.WriteHeader(http.StatusNotFound)
						default:
							err = fmt.Errorf("deleteSwiftObject(\"%v\") returned unexpected errno: %v", swiftObjectName, errno)
							panic(err)
						}
					case unix.ENOENT:
						responseWriter.WriteHeader(http.StatusNotFound)
					default:
						err = fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
						panic(err)
					}
				}
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusNotFound)
			default:
				err = fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		}
	}
}

func doGET(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		boundaryString          string
		containerIndex          int
		containerIndexLimit     int
		err                     error
		errno                   syscall.Errno
		found                   bool
		headerName              string
		headerValue             string
		headerValueSlice        []string
		infoOnly                bool
		marker                  string
		markerSlice             []string
		objectIndex             int
		objectIndexLimit        int
		ok                      bool
		numContainers           int
		numObjects              int
		ranges                  []rangeStruct
		rS                      rangeStruct
		swiftAccount            *swiftAccountStruct
		swiftAccountName        string
		swiftContainer          *swiftContainerStruct
		swiftContainerName      string
		swiftContainerNameAsKey sortedmap.Key
		swiftObject             *swiftObjectStruct
		swiftObjectName         string
		swiftObjectNameAsKey    sortedmap.Key
	)

	globals.Lock()
	defer globals.Unlock()

	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName = parsePath(request)
	if infoOnly {
		_, _ = responseWriter.Write(utils.StringToByteSlice("{"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"swift\": {"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"max_account_name_length\": " + strconv.Itoa(int(globals.config.MaxAccountNameLength)) + ","))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"max_container_name_length\": " + strconv.Itoa(int(globals.config.MaxContainerNameLength)) + ","))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"max_object_name_length\": " + strconv.Itoa(int(globals.config.MaxObjectNameLength)) + ","))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"account_listing_limit\": " + strconv.Itoa(int(globals.config.AccountListingLimit)) + ","))
		_, _ = responseWriter.Write(utils.StringToByteSlice("\"container_listing_limit\": " + strconv.Itoa(int(globals.config.ContainerListingLimit))))
		_, _ = responseWriter.Write(utils.StringToByteSlice("}"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("}"))
	} else {
		if "" == swiftAccountName {
			responseWriter.WriteHeader(http.StatusForbidden)
		} else {
			swiftAccount, errno = locateSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				if "" == swiftContainerName {
					// GET SwiftAccount
					for headerName, headerValueSlice = range swiftAccount.headers {
						for _, headerValue = range headerValueSlice {
							responseWriter.Header().Add(headerName, headerValue)
						}
					}
					numContainers, err = swiftAccount.swiftContainerTree.Len()
					if nil != err {
						panic(err)
					}
					if 0 == numContainers {
						responseWriter.WriteHeader(http.StatusNoContent)
					} else {
						marker = ""
						markerSlice, ok = request.URL.Query()["marker"]
						if ok && (0 < len(markerSlice)) {
							marker = markerSlice[0]
						}
						containerIndex, found, err = swiftAccount.swiftContainerTree.BisectRight(marker)
						if nil != err {
							panic(err)
						}
						if found {
							containerIndex++
						}
						if containerIndex < numContainers {
							containerIndexLimit = numContainers
							if (containerIndexLimit - containerIndex) > int(globals.config.AccountListingLimit) {
								containerIndexLimit = containerIndex + int(globals.config.AccountListingLimit)
							}
							for containerIndex < containerIndexLimit {
								swiftContainerNameAsKey, _, _, err = swiftAccount.swiftContainerTree.GetByIndex(containerIndex)
								if nil != err {
									panic(err)
								}
								swiftContainerName = swiftContainerNameAsKey.(string)
								_, _ = responseWriter.Write(utils.StringToByteSlice(swiftContainerName))
								_, _ = responseWriter.Write([]byte{'\n'})
								containerIndex++
							}
						} else {
							responseWriter.WriteHeader(http.StatusNoContent)
						}
					}
				} else {
					// GET SwiftContainer or SwiftObject
					swiftContainer, errno = locateSwiftContainer(swiftAccount, swiftContainerName)
					switch errno {
					case 0:
						if "" == swiftObjectName {
							// GET SwiftContainer
							for headerName, headerValueSlice = range swiftContainer.headers {
								for _, headerValue = range headerValueSlice {
									responseWriter.Header().Add(headerName, headerValue)
								}
							}
							numObjects, err = swiftContainer.swiftObjectTree.Len()
							if nil != err {
								panic(err)
							}
							if 0 == numObjects {
								responseWriter.WriteHeader(http.StatusNoContent)
							} else {
								marker = ""
								markerSlice, ok = request.URL.Query()["marker"]
								if ok && (0 < len(markerSlice)) {
									marker = markerSlice[0]
								}
								objectIndex, found, err = swiftContainer.swiftObjectTree.BisectRight(marker)
								if nil != err {
									panic(err)
								}
								if found {
									objectIndex++
								}
								if objectIndex < numObjects {
									objectIndexLimit = numObjects
									if (objectIndexLimit - objectIndex) > int(globals.config.ContainerListingLimit) {
										objectIndexLimit = objectIndex + int(globals.config.ContainerListingLimit)
									}
									for objectIndex < objectIndexLimit {
										swiftObjectNameAsKey, _, _, err = swiftContainer.swiftObjectTree.GetByIndex(objectIndex)
										if nil != err {
											panic(err)
										}
										swiftObjectName = swiftObjectNameAsKey.(string)
										_, _ = responseWriter.Write(utils.StringToByteSlice(swiftObjectName))
										_, _ = responseWriter.Write([]byte{'\n'})
										objectIndex++
									}
								} else {
									responseWriter.WriteHeader(http.StatusNoContent)
								}
							}
						} else {
							// GET SwiftObject
							swiftObject, errno = locateSwiftObject(swiftContainer, swiftObjectName)
							switch errno {
							case 0:
								for headerName, headerValueSlice = range swiftObject.headers {
									for _, headerValue = range headerValueSlice {
										responseWriter.Header().Add(headerName, headerValue)
									}
								}
								ranges, err = parseRangeHeader(request, len(swiftObject.contents))
								if nil == err {
									switch len(ranges) {
									case 0:
										responseWriter.Header().Add("Content-Type", "application/octet-stream")
										responseWriter.WriteHeader(http.StatusOK)
										_, _ = responseWriter.Write(swiftObject.contents)
									case 1:
										responseWriter.Header().Add("Content-Type", "application/octet-stream")
										responseWriter.Header().Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", ranges[0].startOffset, ranges[0].stopOffset, len(swiftObject.contents)))
										responseWriter.WriteHeader(http.StatusPartialContent)
										_, _ = responseWriter.Write(swiftObject.contents[ranges[0].startOffset:(ranges[0].stopOffset + 1)])
									default:
										boundaryString = fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64())
										responseWriter.Header().Add("Content-Type", fmt.Sprintf("multipart/byteranges; boundary=%v", boundaryString))
										responseWriter.WriteHeader(http.StatusPartialContent)
										for _, rS = range ranges {
											_, _ = responseWriter.Write([]byte("--" + boundaryString + "\r\n"))
											_, _ = responseWriter.Write([]byte("Content-Type: application/octet-stream\r\n"))
											_, _ = responseWriter.Write([]byte(fmt.Sprintf("Content-Range: bytes %d-%d/%d\r\n", rS.startOffset, rS.stopOffset, len(swiftObject.contents))))
											_, _ = responseWriter.Write([]byte("\r\n"))
											_, _ = responseWriter.Write(swiftObject.contents[rS.startOffset:(rS.stopOffset + 1)])
											_, _ = responseWriter.Write([]byte("\r\n"))
										}
										_, _ = responseWriter.Write([]byte("--" + boundaryString + "--"))
									}
								} else {
									responseWriter.WriteHeader(http.StatusBadRequest)
								}
							case unix.ENOENT:
								responseWriter.WriteHeader(http.StatusNotFound)
							default:
								err = fmt.Errorf("locateSwiftObject(\"%v\") returned unexpected errno: %v", swiftObjectName, errno)
								panic(err)
							}
						}
					case unix.ENOENT:
						responseWriter.WriteHeader(http.StatusNotFound)
					default:
						err = fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
						panic(err)
					}
				}
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusNotFound)
			default:
				err = fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		}
	}
}

func doHEAD(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err                error
		errno              syscall.Errno
		headerName         string
		headerValue        string
		headerValueSlice   []string
		infoOnly           bool
		swiftAccount       *swiftAccountStruct
		swiftAccountName   string
		swiftContainer     *swiftContainerStruct
		swiftContainerName string
		swiftObject        *swiftObjectStruct
		swiftObjectName    string
	)

	globals.Lock()
	defer globals.Unlock()

	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName = parsePath(request)
	if infoOnly || ("" == swiftAccountName) {
		responseWriter.WriteHeader(http.StatusForbidden)
	} else {
		swiftAccount, errno = locateSwiftAccount(swiftAccountName)
		switch errno {
		case 0:
			if "" == swiftContainerName {
				// HEAD SwiftAccount
				for headerName, headerValueSlice = range swiftAccount.headers {
					for _, headerValue = range headerValueSlice {
						responseWriter.Header().Add(headerName, headerValue)
					}
				}
				responseWriter.WriteHeader(http.StatusNoContent)
			} else {
				// HEAD SwiftContainer or SwiftObject
				swiftContainer, errno = locateSwiftContainer(swiftAccount, swiftContainerName)
				switch errno {
				case 0:
					if "" == swiftObjectName {
						// HEAD SwiftContainer
						for headerName, headerValueSlice = range swiftContainer.headers {
							for _, headerValue = range headerValueSlice {
								responseWriter.Header().Add(headerName, headerValue)
							}
						}
						responseWriter.WriteHeader(http.StatusNoContent)
					} else {
						// HEAD SwiftObject
						swiftObject, errno = locateSwiftObject(swiftContainer, swiftObjectName)
						switch errno {
						case 0:
							for headerName, headerValueSlice = range swiftObject.headers {
								for _, headerValue = range headerValueSlice {
									responseWriter.Header().Add(headerName, headerValue)
								}
							}
							responseWriter.Header().Set("Content-Length", strconv.Itoa(len(swiftObject.contents)))
							responseWriter.WriteHeader(http.StatusOK)
						case unix.ENOENT:
							responseWriter.WriteHeader(http.StatusNotFound)
						default:
							err = fmt.Errorf("locateSwiftObject(\"%v\") returned unexpected errno: %v", swiftObjectName, errno)
							panic(err)
						}
					}
				case unix.ENOENT:
					responseWriter.WriteHeader(http.StatusNotFound)
				default:
					err = fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
					panic(err)
				}
			}
		case unix.ENOENT:
			responseWriter.WriteHeader(http.StatusNotFound)
		default:
			err = fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
			panic(err)
		}
	}
}

func doPOST(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err                 error
		errno               syscall.Errno
		headerName          string
		headerValue         string
		headerValueSlice    []string
		headerValueSliceLen int
		ignoreHeader        bool
		infoOnly            bool
		swiftAccount        *swiftAccountStruct
		swiftAccountName    string
		swiftContainer      *swiftContainerStruct
		swiftContainerName  string
		swiftObject         *swiftObjectStruct
		swiftObjectName     string
	)

	globals.Lock()
	defer globals.Unlock()

	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName = parsePath(request)
	if infoOnly || ("" == swiftAccountName) {
		responseWriter.WriteHeader(http.StatusForbidden)
	} else {
		swiftAccount, errno = locateSwiftAccount(swiftAccountName)
		switch errno {
		case 0:
			if "" == swiftContainerName {
				// POST SwiftAccount
				for headerName, headerValueSlice = range request.Header {
					_, ignoreHeader = headerNameIgnoreSet[headerName]
					if !ignoreHeader {
						headerValueSliceLen = len(headerValueSlice)
						if 0 < headerValueSliceLen {
							swiftAccount.headers[headerName] = make([]string, 0, headerValueSliceLen)
							for _, headerValue = range headerValueSlice {
								if 0 < len(headerValue) {
									swiftAccount.headers[headerName] = append(swiftAccount.headers[headerName], headerValue)
								}
							}
							if 0 == len(swiftAccount.headers[headerName]) {
								delete(swiftAccount.headers, headerName)
							}
						}
					}
				}
				responseWriter.WriteHeader(http.StatusNoContent)
			} else {
				// POST SwiftContainer or SwiftObject
				swiftContainer, errno = locateSwiftContainer(swiftAccount, swiftContainerName)
				switch errno {
				case 0:
					if "" == swiftObjectName {
						// POST SwiftContainer
						for headerName, headerValueSlice = range request.Header {
							_, ignoreHeader = headerNameIgnoreSet[headerName]
							if !ignoreHeader {
								headerValueSliceLen = len(headerValueSlice)
								if 0 < headerValueSliceLen {
									swiftContainer.headers[headerName] = make([]string, 0, headerValueSliceLen)
									for _, headerValue = range headerValueSlice {
										if 0 < len(headerValue) {
											swiftContainer.headers[headerName] = append(swiftContainer.headers[headerName], headerValue)
										}
									}
									if 0 == len(swiftContainer.headers[headerName]) {
										delete(swiftContainer.headers, headerName)
									}
								}
							}
						}
						responseWriter.WriteHeader(http.StatusNoContent)
					} else {
						// POST SwiftObject
						swiftObject, errno = locateSwiftObject(swiftContainer, swiftObjectName)
						switch errno {
						case 0:
							for headerName, headerValueSlice = range request.Header {
								_, ignoreHeader = headerNameIgnoreSet[headerName]
								if !ignoreHeader {
									headerValueSliceLen = len(headerValueSlice)
									if 0 < headerValueSliceLen {
										swiftObject.headers[headerName] = make([]string, 0, headerValueSliceLen)
										for _, headerValue = range headerValueSlice {
											if 0 < len(headerValue) {
												swiftObject.headers[headerName] = append(swiftObject.headers[headerName], headerValue)
											}
										}
										if 0 == len(swiftObject.headers[headerName]) {
											delete(swiftObject.headers, headerName)
										}
									}
								}
							}
							responseWriter.WriteHeader(http.StatusNoContent)
						case unix.ENOENT:
							responseWriter.WriteHeader(http.StatusNotFound)
						default:
							err = fmt.Errorf("locateSwiftObject(\"%v\") returned unexpected errno: %v", swiftObjectName, errno)
							panic(err)
						}
					}
				case unix.ENOENT:
					responseWriter.WriteHeader(http.StatusNotFound)
				default:
					err = fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
					panic(err)
				}
			}
		case unix.ENOENT:
			responseWriter.WriteHeader(http.StatusNotFound)
		default:
			err = fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
			panic(err)
		}
	}
}

func doPUT(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err                 error
		errno               syscall.Errno
		headerName          string
		headerValue         string
		headerValueSlice    []string
		headerValueSliceLen int
		ignoreHeader        bool
		infoOnly            bool
		swiftAccount        *swiftAccountStruct
		swiftAccountName    string
		swiftContainer      *swiftContainerStruct
		swiftContainerName  string
		swiftObject         *swiftObjectStruct
		swiftObjectName     string
		wasCreated          bool
	)

	globals.Lock()
	defer globals.Unlock()

	infoOnly, swiftAccountName, swiftContainerName, swiftObjectName = parsePath(request)
	if infoOnly || ("" == swiftAccountName) {
		responseWriter.WriteHeader(http.StatusForbidden)
	} else {
		if "" == swiftContainerName {
			// PUT SwiftAccount
			swiftAccount, wasCreated = createOrLocateSwiftAccount(swiftAccountName)
			if wasCreated {
				swiftAccount.headers = make(http.Header)
			}
			for headerName, headerValueSlice = range request.Header {
				_, ignoreHeader = headerNameIgnoreSet[headerName]
				if !ignoreHeader {
					headerValueSliceLen = len(headerValueSlice)
					if 0 < headerValueSliceLen {
						swiftAccount.headers[headerName] = make([]string, 0, headerValueSliceLen)
						for _, headerValue = range headerValueSlice {
							if 0 < len(headerValue) {
								swiftAccount.headers[headerName] = append(swiftAccount.headers[headerName], headerValue)
							}
						}
						if 0 == len(swiftAccount.headers[headerName]) {
							delete(swiftAccount.headers, headerName)
						}
					}
				}
			}
			if wasCreated {
				responseWriter.WriteHeader(http.StatusCreated)
			} else {
				responseWriter.WriteHeader(http.StatusAccepted)
			}
		} else {
			// PUT SwiftContainer or SwiftObject
			swiftAccount, errno = locateSwiftAccount(swiftAccountName)
			switch errno {
			case 0:
				if "" == swiftObjectName {
					// PUT SwiftContainer
					swiftContainer, wasCreated = createOrLocateSwiftContainer(swiftAccount, swiftContainerName)
					if wasCreated {
						swiftContainer.headers = make(http.Header)
					}
					for headerName, headerValueSlice = range request.Header {
						_, ignoreHeader = headerNameIgnoreSet[headerName]
						if !ignoreHeader {
							headerValueSliceLen = len(headerValueSlice)
							if 0 < headerValueSliceLen {
								swiftContainer.headers[headerName] = make([]string, 0, headerValueSliceLen)
								for _, headerValue = range headerValueSlice {
									if 0 < len(headerValue) {
										swiftContainer.headers[headerName] = append(swiftContainer.headers[headerName], headerValue)
									}
								}
								if 0 == len(swiftContainer.headers[headerName]) {
									delete(swiftContainer.headers, headerName)
								}
							}
						}
					}
					if wasCreated {
						responseWriter.WriteHeader(http.StatusCreated)
					} else {
						responseWriter.WriteHeader(http.StatusAccepted)
					}
				} else {
					// PUT SwiftObject
					swiftContainer, errno = locateSwiftContainer(swiftAccount, swiftContainerName)
					switch errno {
					case 0:
						swiftObject, wasCreated = createOrLocateSwiftObject(swiftContainer, swiftObjectName)
						if wasCreated {
							swiftObject.headers = make(http.Header)
						}
						for headerName, headerValueSlice = range request.Header {
							_, ignoreHeader = headerNameIgnoreSet[headerName]
							if !ignoreHeader {
								headerValueSliceLen = len(headerValueSlice)
								if 0 < headerValueSliceLen {
									swiftObject.headers[headerName] = make([]string, 0, headerValueSliceLen)
									for _, headerValue = range headerValueSlice {
										if 0 < len(headerValue) {
											swiftObject.headers[headerName] = append(swiftObject.headers[headerName], headerValue)
										}
									}
									if 0 == len(swiftObject.headers[headerName]) {
										delete(swiftObject.headers, headerName)
									}
								}
							}
						}
						swiftObject.contents, _ = ioutil.ReadAll(request.Body)
						if wasCreated {
							responseWriter.WriteHeader(http.StatusCreated)
						} else {
							responseWriter.WriteHeader(http.StatusCreated)
						}
					case unix.ENOENT:
						responseWriter.WriteHeader(http.StatusForbidden)
					default:
						err = fmt.Errorf("locateSwiftContainer(\"%v\") returned unexpected errno: %v", swiftContainerName, errno)
						panic(err)
					}
				}
			case unix.ENOENT:
				responseWriter.WriteHeader(http.StatusForbidden)
			default:
				err = fmt.Errorf("locateSwiftAccount(\"%v\") returned unexpected errno: %v", swiftAccountName, errno)
				panic(err)
			}
		}
	}
}
