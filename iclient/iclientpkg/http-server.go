// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/ihtml"
	"github.com/NVIDIA/proxyfs/version"
)

const (
	startHTTPServerUpCheckDelay      = 100 * time.Millisecond
	startHTTPServerUpCheckMaxRetries = 10
)

func startHTTPServer() (err error) {
	var (
		ipAddrTCPPort                 string
		startHTTPServerUpCheckRetries uint32
	)

	if globals.config.HTTPServerPort == 0 {
		globals.httpServer = nil
		err = nil
		return
	}

	ipAddrTCPPort = net.JoinHostPort(globals.config.HTTPServerIPAddr, strconv.Itoa(int(globals.config.HTTPServerPort)))

	globals.httpServer = &http.Server{
		Addr:    ipAddrTCPPort,
		Handler: &globals,
	}

	globals.httpServerWG.Add(1)

	go func() {
		var (
			err error
		)

		err = globals.httpServer.ListenAndServe()
		if http.ErrServerClosed != err {
			log.Fatalf("httpServer.ListenAndServe() exited unexpectedly: %v", err)
		}

		globals.httpServerWG.Done()
	}()

	for startHTTPServerUpCheckRetries = 0; startHTTPServerUpCheckRetries < startHTTPServerUpCheckMaxRetries; startHTTPServerUpCheckRetries++ {
		_, err = http.Get("http://" + ipAddrTCPPort + "/version")
		if nil == err {
			return
		}

		time.Sleep(startHTTPServerUpCheckDelay)
	}

	err = fmt.Errorf("startHTTPServerUpCheckMaxRetries (%v) exceeded", startHTTPServerUpCheckMaxRetries)
	return
}

func stopHTTPServer() (err error) {
	if globals.config.HTTPServerPort == 0 {
		globals.httpServer = nil
		err = nil
		return
	}

	err = globals.httpServer.Shutdown(context.TODO())
	if nil == err {
		globals.httpServerWG.Wait()
	}

	return
}

func (dummy *globalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err         error
		requestPath string
	)

	requestPath = strings.TrimRight(request.URL.Path, "/")

	_, err = ioutil.ReadAll(request.Body)
	if nil == err {
		err = request.Body.Close()
		if nil != err {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
	} else {
		_ = request.Body.Close()
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	switch request.Method {
	case http.MethodGet:
		serveHTTPGet(responseWriter, request, requestPath)
	case http.MethodPost:
		serveHTTPPost(responseWriter, request, requestPath)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func serveHTTPGet(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	var (
		ok bool
	)

	switch {
	case "" == requestPath:
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(fmt.Sprintf(indexDotHTMLTemplate, version.ProxyFSVersion)))
	case "/config" == requestPath:
		serveHTTPGetOfConfig(responseWriter, request)
	case "/index.html" == requestPath:
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(fmt.Sprintf(indexDotHTMLTemplate, version.ProxyFSVersion)))
	case "/leases" == requestPath:
		serveHTTPGetOfLeases(responseWriter, request)
	case "/stats" == requestPath:
		serveHTTPGetOfStats(responseWriter, request)
	case "/version" == requestPath:
		serveHTTPGetOfVersion(responseWriter, request)
	default:
		ok = ihtml.ServeHTTPGet(responseWriter, requestPath)
		if !ok {
			responseWriter.WriteHeader(http.StatusNotFound)
		}
	}
}

func serveHTTPGetOfConfig(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		confMapJSON []byte
		err         error
		startTime   time.Time = time.Now()
	)

	defer func() {
		globals.stats.GetConfigUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	confMapJSON, err = json.Marshal(globals.config)
	if nil != err {
		logFatalf("json.Marshal(globals.config) failed: %v", err)
	}

	if strings.Contains(request.Header.Get("Accept"), "text/html") {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, err = responseWriter.Write([]byte(fmt.Sprintf(configTemplate, version.ProxyFSVersion, string(confMapJSON[:]))))
		if nil != err {
			logWarnf("responseWriter.Write([]byte(fmt.Sprintf(configTemplate, version.ProxyFSVersion, string(confMapJSON[:])))) failed: %v", err)
		}
	} else {
		responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(confMapJSON)))
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		_, err = responseWriter.Write(confMapJSON)
		if nil != err {
			logWarnf("responseWriter.Write(confMapJSON) failed: %v", err)
		}
	}
}

type inodeTableByInodeNumberElement struct {
	InodeNumber uint64
	State       string
}

type inodeTableByInodeNumberSlice []inodeTableByInodeNumberElement

func (s inodeTableByInodeNumberSlice) Len() int {
	return len(s)
}

func (s inodeTableByInodeNumberSlice) Swap(i, j int) {
	s[i].InodeNumber, s[j].InodeNumber = s[j].InodeNumber, s[i].InodeNumber
	s[i].State, s[j].State = s[j].State, s[i].State
}

func (s inodeTableByInodeNumberSlice) Less(i, j int) bool {
	return s[i].InodeNumber < s[j].InodeNumber
}

func serveHTTPGetOfLeases(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err             error
		inode           *inodeStruct
		inodeTable      inodeTableByInodeNumberSlice
		inodeTableJSON  []byte
		inodeTableIndex int
		inodeNumber     uint64
		startTime       time.Time = time.Now()
	)

	defer func() {
		globals.stats.GetLeasesUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	inodeTable = make(inodeTableByInodeNumberSlice, len(globals.inodeTable))
	inodeTableIndex = 0
	for inodeNumber, inode = range globals.inodeTable {
		inodeTable[inodeTableIndex].InodeNumber = inodeNumber
		switch inode.leaseState {
		case inodeLeaseStateNone:
			inodeTable[inodeTableIndex].State = "None"
		case inodeLeaseStateSharedRequested:
			inodeTable[inodeTableIndex].State = "SharedRequested"
		case inodeLeaseStateSharedGranted:
			inodeTable[inodeTableIndex].State = "SharedGranted"
		case inodeLeaseStateSharedPromoting:
			inodeTable[inodeTableIndex].State = "SharedPromoting"
		case inodeLeaseStateSharedReleasing:
			inodeTable[inodeTableIndex].State = "SharedReleasing"
		case inodeLeaseStateSharedExpired:
			inodeTable[inodeTableIndex].State = "SharedExpired"
		case inodeLeaseStateExclusiveRequested:
			inodeTable[inodeTableIndex].State = "ExclusiveRequested"
		case inodeLeaseStateExclusiveGranted:
			inodeTable[inodeTableIndex].State = "ExclusiveGranted"
		case inodeLeaseStateExclusiveDemoting:
			inodeTable[inodeTableIndex].State = "ExclusiveDemoting"
		case inodeLeaseStateExclusiveReleasing:
			inodeTable[inodeTableIndex].State = "ExclusiveReleasing"
		case inodeLeaseStateExclusiveExpired:
			inodeTable[inodeTableIndex].State = "ExclusiveExpired"
		default:
			logFatalf("globals.inodeTable[inudeNumber:0x%016X].leaseState (%v) unrecognized", inodeNumber, inode.leaseState)
		}
		inodeTableIndex++
	}

	globals.Unlock()

	sort.Sort(inodeTable)

	inodeTableJSON, err = json.Marshal(inodeTable)
	if nil != err {
		logFatalf("json.Marshal(inodeTable) failed: %v", err)
	}

	if strings.Contains(request.Header.Get("Accept"), "text/html") {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, err = responseWriter.Write([]byte(fmt.Sprintf(leasesTemplate, version.ProxyFSVersion, string(inodeTableJSON[:]))))
		if nil != err {
			logWarnf("responseWriter.Write([]byte(fmt.Sprintf(leasesTemplate, version.ProxyFSVersion, string(inodeTableJSON[:])))) failed: %v", err)
		}
	} else {
		responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(inodeTableJSON)))
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		_, err = responseWriter.Write(inodeTableJSON)
		if nil != err {
			logWarnf("responseWriter.Write(inodeTableJSON) failed: %v", err)
		}
	}
}

func serveHTTPGetOfStats(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err           error
		startTime     time.Time = time.Now()
		statsAsString string
	)

	defer func() {
		globals.stats.GetStatsUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	statsAsString = bucketstats.SprintStats(bucketstats.StatFormatParsable1, "*", "*")

	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(statsAsString)))
	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)

	_, err = responseWriter.Write([]byte(statsAsString))
	if nil != err {
		logWarnf("responseWriter.Write([]byte(statsAsString)) failed: %v", err)
	}
}

func serveHTTPGetOfVersion(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err       error
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.GetVersionUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(version.ProxyFSVersion)))
	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)

	_, err = responseWriter.Write([]byte(version.ProxyFSVersion))
	if nil != err {
		logWarnf("responseWriter.Write([]byte(statsAsString)) failed: %v", err)
	}
}

func serveHTTPPost(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	switch {
	case "/leases/demote" == requestPath:
		serveHTTPPostOfLeasesDemote(responseWriter, request)
	case "/leases/release" == requestPath:
		serveHTTPPostOfLeasesRelease(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func serveHTTPPostOfLeasesDemote(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.PostLeasesDemoteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	logWarnf("serveHTTPPostOfLeasesDemote() TODO")

	responseWriter.WriteHeader(http.StatusOK)
}

func serveHTTPPostOfLeasesRelease(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.PostLeasesReleaseUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	logWarnf("serveHTTPPostOfLeasesRelease() TODO")

	responseWriter.WriteHeader(http.StatusOK)
}
