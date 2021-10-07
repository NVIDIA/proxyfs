// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/iauth"
	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/imgr/imgrpkg"
	"github.com/NVIDIA/proxyfs/retryrpc"
)

const (
	HTTPUserAgent = "iclient"
)

func startRPCHandler() (err error) {
	var (
		customTransport            *http.Transport
		defaultTransport           *http.Transport
		mountRequest               *imgrpkg.MountRequestStruct
		mountResponse              *imgrpkg.MountResponseStruct
		nextSwiftRetryDelayNominal time.Duration
		ok                         bool
		swiftRetryDelayIndex       uint64
	)

	defaultTransport, ok = http.DefaultTransport.(*http.Transport)
	if !ok {
		err = fmt.Errorf("http.DefaultTransport.(*http.Transport) returned !ok\n")
		return
	}

	customTransport = &http.Transport{ // Up-to-date as of Golang 1.11
		Proxy:                  defaultTransport.Proxy,
		DialContext:            defaultTransport.DialContext,
		Dial:                   defaultTransport.Dial,
		DialTLS:                defaultTransport.DialTLS,
		TLSClientConfig:        defaultTransport.TLSClientConfig,
		TLSHandshakeTimeout:    globals.config.SwiftTimeout,
		DisableKeepAlives:      false,
		DisableCompression:     defaultTransport.DisableCompression,
		MaxIdleConns:           int(globals.config.SwiftConnectionPoolSize),
		MaxIdleConnsPerHost:    int(globals.config.SwiftConnectionPoolSize),
		MaxConnsPerHost:        int(globals.config.SwiftConnectionPoolSize),
		IdleConnTimeout:        globals.config.SwiftTimeout,
		ResponseHeaderTimeout:  globals.config.SwiftTimeout,
		ExpectContinueTimeout:  globals.config.SwiftTimeout,
		TLSNextProto:           defaultTransport.TLSNextProto,
		ProxyConnectHeader:     defaultTransport.ProxyConnectHeader,
		MaxResponseHeaderBytes: defaultTransport.MaxResponseHeaderBytes,
	}

	globals.httpClient = &http.Client{
		Transport: customTransport,
		Timeout:   globals.config.SwiftTimeout,
	}

	globals.swiftRetryDelay = make([]swiftRetryDelayElementStruct, globals.config.SwiftRetryLimit)

	nextSwiftRetryDelayNominal = globals.config.SwiftRetryDelay

	for swiftRetryDelayIndex = 0; swiftRetryDelayIndex < globals.config.SwiftRetryLimit; swiftRetryDelayIndex++ {
		globals.swiftRetryDelay[swiftRetryDelayIndex].nominal = nextSwiftRetryDelayNominal
		globals.swiftRetryDelay[swiftRetryDelayIndex].variance = nextSwiftRetryDelayNominal * time.Duration(globals.config.SwiftRetryDelayVariance) / time.Duration(100)
	}

	if globals.config.AuthPlugInEnvName == "" {
		globals.swiftAuthInString = globals.config.AuthPlugInEnvValue
	} else {
		globals.swiftAuthInString = os.Getenv(globals.config.AuthPlugInEnvName)
	}

	updateSwithAuthTokenAndSwiftStorageURL()

	globals.retryRPCClientConfig = &retryrpc.ClientConfig{
		DNSOrIPAddr:              globals.config.RetryRPCPublicIPAddr,
		Port:                     int(globals.config.RetryRPCPort),
		RootCAx509CertificatePEM: globals.retryRPCCACertPEM,
		Callbacks:                &globals,
		DeadlineIO:               globals.config.RetryRPCDeadlineIO,
		KeepAlivePeriod:          globals.config.RetryRPCKeepAlivePeriod,
	}

	globals.retryRPCClient, err = retryrpc.NewClient(globals.retryRPCClientConfig)
	if nil != err {
		return
	}

	mountRequest = &imgrpkg.MountRequestStruct{
		VolumeName: globals.config.VolumeName,
		AuthToken:  fetchSwiftAuthToken(),
	}
	mountResponse = &imgrpkg.MountResponseStruct{}

	err = rpcMount(mountRequest, mountResponse)
	if nil != err {
		return
	}

	globals.mountID = mountResponse.MountID

	err = nil
	return
}

func renewRPCHandler() (err error) {
	var (
		renewMountRequest  *imgrpkg.RenewMountRequestStruct
		renewMountResponse *imgrpkg.RenewMountResponseStruct
	)

	updateSwithAuthTokenAndSwiftStorageURL()

	renewMountRequest = &imgrpkg.RenewMountRequestStruct{
		MountID:   globals.mountID,
		AuthToken: fetchSwiftAuthToken(),
	}
	renewMountResponse = &imgrpkg.RenewMountResponseStruct{}

	err = rpcRenewMount(renewMountRequest, renewMountResponse)

	return // err, as set by rpcRenewMount(renewMountRequest, renewMountResponse) is sufficient
}

func stopRPCHandler() (err error) {
	var (
		unmountRequest  *imgrpkg.UnmountRequestStruct
		unmountResponse *imgrpkg.UnmountResponseStruct
	)

	unmountRequest = &imgrpkg.UnmountRequestStruct{
		MountID: globals.mountID,
	}
	unmountResponse = &imgrpkg.UnmountResponseStruct{}

	err = rpcUnmount(unmountRequest, unmountResponse)
	if nil != err {
		logWarn(err)
	}

	globals.retryRPCClient.Close()

	globals.httpClient = nil

	globals.swiftRetryDelay = nil

	globals.retryRPCClientConfig = nil
	globals.retryRPCClient = nil

	err = nil
	return
}

func fetchSwiftAuthToken() (swiftAuthToken string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftAuthToken = globals.swiftAuthToken
			globals.Unlock()
			return
		}

		globals.Lock()

		swiftAuthWaitGroup.Wait()
	}
}

func fetchSwiftStorageURL() (swiftStorageURL string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftStorageURL = globals.swiftStorageURL
			globals.Unlock()
			return
		}

		globals.Lock()

		swiftAuthWaitGroup.Wait()
	}
}

func updateSwithAuthTokenAndSwiftStorageURL() {
	var (
		err                error
		swiftAuthWaitGroup *sync.WaitGroup
	)

	globals.Lock()

	swiftAuthWaitGroup = globals.swiftAuthWaitGroup

	if nil != swiftAuthWaitGroup {
		globals.Unlock()
		swiftAuthWaitGroup.Wait()
		return
	}

	globals.swiftAuthWaitGroup = &sync.WaitGroup{}
	globals.swiftAuthWaitGroup.Add(1)

	globals.Unlock()

	globals.swiftAuthToken, globals.swiftStorageURL, err = iauth.PerformAuth(globals.config.AuthPlugInPath, globals.swiftAuthInString)
	if nil != err {
		logFatalf("iauth.PerformAuth() failed: %v", err)
	}

	globals.Lock()
	globals.swiftAuthWaitGroup.Done()
	globals.swiftAuthWaitGroup = nil
	globals.Unlock()
}

func performRenewableRPC(method string, request interface{}, reply interface{}) (err error) {
Retry:

	err = globals.retryRPCClient.Send(method, request, reply)
	if nil != err {
		err = renewRPCHandler()
		if nil != err {
			logFatal(err)
		}
		goto Retry
	}

	return
}

func rpcAdjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *imgrpkg.AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountResponse *imgrpkg.AdjustInodeTableEntryOpenCountResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.AdjustInodeTableEntryOpenCountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = performRenewableRPC("AdjustInodeTableEntryOpenCount", adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)

	return
}

func rpcDeleteInodeTableEntry(deleteInodeTableEntryRequest *imgrpkg.DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryResponse *imgrpkg.DeleteInodeTableEntryResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DeleteInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = performRenewableRPC("DeleteInodeTableEntry", deleteInodeTableEntryRequest, deleteInodeTableEntryResponse)

	return
}

func rpcFetchNonceRange(fetchNonceRangeRequest *imgrpkg.FetchNonceRangeRequestStruct, fetchNonceRangeResponse *imgrpkg.FetchNonceRangeResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.FetchNonceRangeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = performRenewableRPC("FetchNonceRange", fetchNonceRangeRequest, fetchNonceRangeResponse)

	return
}

func rpcFlush(flushRequest *imgrpkg.FlushRequestStruct, flushResponse *imgrpkg.FlushResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.FlushUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = performRenewableRPC("Flush", flushRequest, flushResponse)

	return
}

func rpcGetInodeTableEntry(getInodeTableEntryRequest *imgrpkg.GetInodeTableEntryRequestStruct, getInodeTableEntryResponse *imgrpkg.GetInodeTableEntryResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.GetInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = performRenewableRPC("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)

	return
}

func rpcLease(leaseRequest *imgrpkg.LeaseRequestStruct, leaseResponse *imgrpkg.LeaseResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.LeaseUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = performRenewableRPC("Lease", leaseRequest, leaseResponse)

	return
}

func rpcMount(mountRequest *imgrpkg.MountRequestStruct, mountResponse *imgrpkg.MountResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.MountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = globals.retryRPCClient.Send("Mount", mountRequest, mountResponse)

	return
}

func rpcPutInodeTableEntries(putInodeTableEntriesRequest *imgrpkg.PutInodeTableEntriesRequestStruct, putInodeTableEntriesResponse *imgrpkg.PutInodeTableEntriesResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.PutInodeTableEntriesUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = performRenewableRPC("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)

	return
}

func rpcRenewMount(renewMountRequest *imgrpkg.RenewMountRequestStruct, renewMountResponse *imgrpkg.RenewMountResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoGetAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = globals.retryRPCClient.Send("RenewMount", renewMountRequest, renewMountResponse)

	return
}

func rpcUnmount(unmountRequest *imgrpkg.UnmountRequestStruct, unmountResponse *imgrpkg.UnmountResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.UnmountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = performRenewableRPC("Unmount", unmountRequest, unmountResponse)

	return
}

func objectGETRange(objectNumber uint64, offset uint64, length uint64) (buf []byte, err error) {
	var (
		rangeHeader string
	)

	rangeHeader = "bytes=" + strconv.FormatUint(offset, 10) + "-" + strconv.FormatUint((offset+length-1), 10)

	buf, err = objectGETWithRangeHeader(objectNumber, rangeHeader)

	return
}

func objectGETTail(objectNumber uint64, length uint64) (buf []byte, err error) {
	var (
		rangeHeader string
	)

	rangeHeader = "bytes=-" + strconv.FormatUint(length, 10)

	buf, err = objectGETWithRangeHeader(objectNumber, rangeHeader)

	return
}

func objectGETWithRangeHeader(objectNumber uint64, rangeHeader string) (buf []byte, err error) {
	var (
		httpRequest  *http.Request
		httpResponse *http.Response
		retryDelay   time.Duration
		retryIndex   uint64
	)

	retryIndex = 0

	for {
		httpRequest, err = http.NewRequest("GET", fetchSwiftStorageURL()+"/"+ilayout.GetObjectNameAsString(objectNumber), nil)
		if nil != err {
			return
		}

		httpRequest.Header["User-Agent"] = []string{HTTPUserAgent}
		httpRequest.Header["X-Auth-Token"] = []string{fetchSwiftAuthToken()}
		httpRequest.Header["Range"] = []string{rangeHeader}

		httpResponse, err = globals.httpClient.Do(httpRequest)
		if nil != err {
			logFatalf("globals.httpClient.Do(httpRequest) failed: %v", err)
		}

		buf, err = ioutil.ReadAll(httpResponse.Body)
		_ = httpResponse.Body.Close()
		if nil != err {
			logFatalf("ioutil.ReadAll(httpResponse.Body) failed: %v", err)
		}

		if (200 <= httpResponse.StatusCode) && (299 >= httpResponse.StatusCode) {
			err = nil
			return
		}

		if retryIndex >= globals.config.SwiftRetryLimit {
			err = fmt.Errorf("objectGETWithRangeHeader(objectNumber: %v, rangeHeader: \"%s\") reached SwiftRetryLimit", objectNumber, rangeHeader)
			logWarn(err)
			return
		}

		if http.StatusUnauthorized == httpResponse.StatusCode {
			updateSwithAuthTokenAndSwiftStorageURL()
		}

		retryDelay = globals.swiftRetryDelay[retryIndex].nominal - time.Duration(rand.Int63n(int64(globals.swiftRetryDelay[retryIndex].variance)))
		time.Sleep(retryDelay)
		retryIndex++
	}
}
