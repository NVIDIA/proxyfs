// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
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

	ipAddrTCPPort = net.JoinHostPort(globals.config.PrivateIPAddr, strconv.Itoa(int(globals.config.HTTPServerPort)))

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
		_, err = http.Get("http://" + ipAddrTCPPort + "/config")
		if nil == err {
			return
		}

		time.Sleep(startHTTPServerUpCheckDelay)
	}

	err = fmt.Errorf("startHTTPServerUpCheckMaxRetries (%v) exceeded", startHTTPServerUpCheckMaxRetries)
	return
}

func stopHTTPServer() (err error) {
	err = globals.httpServer.Shutdown(context.TODO())
	if nil == err {
		globals.httpServerWG.Wait()
	}

	return
}

func (dummy *globalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err         error
		requestBody []byte
		requestPath string
	)

	requestPath = strings.TrimRight(request.URL.Path, "/")

	requestBody, err = ioutil.ReadAll(request.Body)
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
	case http.MethodDelete:
		serveHTTPDelete(responseWriter, request, requestPath)
	case http.MethodGet:
		serveHTTPGet(responseWriter, request, requestPath)
	case http.MethodPost:
		serveHTTPPost(responseWriter, request, requestPath, requestBody)
	case http.MethodPut:
		serveHTTPPut(responseWriter, request, requestPath, requestBody)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func serveHTTPDelete(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	switch {
	case strings.HasPrefix(requestPath, "/volume"):
		serveHTTPDeleteOfVolume(responseWriter, request, requestPath)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func serveHTTPDeleteOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	var (
		err       error
		pathSplit []string
		startTime time.Time
	)

	startTime = time.Now()

	pathSplit = strings.Split(requestPath, "/")

	switch len(pathSplit) {
	case 3:
		defer func() {
			globals.stats.DeleteVolumeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		err = deleteVolume(pathSplit[2])
		if nil == err {
			responseWriter.WriteHeader(http.StatusNoContent)
		} else {
			responseWriter.WriteHeader(http.StatusNotFound)
		}
	default:
		responseWriter.WriteHeader(http.StatusBadRequest)
	}
}

func serveHTTPGet(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	switch {
	case "/config" == requestPath:
		serveHTTPGetOfConfig(responseWriter, request)
	case "/stats" == requestPath:
		serveHTTPGetOfStats(responseWriter, request)
	case "/version" == requestPath:
		serveHTTPGetOfVersion(responseWriter, request)
	case strings.HasPrefix(requestPath, "/volume"):
		serveHTTPGetOfVolume(responseWriter, request, requestPath)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func serveHTTPGetOfConfig(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		confMapJSON []byte
		err         error
		startTime   time.Time
	)

	startTime = time.Now()
	defer func() {
		globals.stats.GetConfigUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	confMapJSON, err = json.Marshal(globals.config)
	if nil != err {
		logFatalf("json.Marshal(globals.config) failed: %v", err)
	}

	responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(confMapJSON)))
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)

	_, err = responseWriter.Write(confMapJSON)
	if nil != err {
		logWarnf("responseWriter.Write(confMapJSON) failed: %v", err)
	}
}

func serveHTTPGetOfStats(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err           error
		startTime     time.Time
		statsAsString string
	)

	startTime = time.Now()
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
		startTime time.Time
	)

	startTime = time.Now()
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

func serveHTTPGetOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestPath string) {
	var (
		err          error
		jsonToReturn []byte
		pathSplit    []string
		startTime    time.Time
	)

	startTime = time.Now()

	pathSplit = strings.Split(requestPath, "/")

	switch len(pathSplit) {
	case 2:
		startTime = time.Now()
		defer func() {
			globals.stats.GetVolumeListUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		jsonToReturn = getVolumeListAsJSON()

		responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(jsonToReturn)))
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		_, err = responseWriter.Write(jsonToReturn)
		if nil != err {
			logWarnf("responseWriter.Write(jsonToReturn) failed: %v", err)
		}
	case 3:
		defer func() {
			globals.stats.GetVolumeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		jsonToReturn, err = getVolumeAsJSON(pathSplit[2])
		if nil == err {
			responseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(jsonToReturn)))
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)

			_, err = responseWriter.Write(jsonToReturn)
			if nil != err {
				logWarnf("responseWriter.Write(jsonToReturn) failed: %v", err)
			}
		} else {
			responseWriter.WriteHeader(http.StatusNotFound)
		}
	default:
		responseWriter.WriteHeader(http.StatusBadRequest)
	}
}

func serveHTTPPost(responseWriter http.ResponseWriter, request *http.Request, requestPath string, requestBody []byte) {
	switch {
	case "/volume" == requestPath:
		serveHTTPPostOfVolume(responseWriter, request, requestBody)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

type serveHTTPPostOfVolumeRequestBodyAsJSONStruct struct {
	StorageURL string
	AuthToken  string
}

func serveHTTPPostOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestBody []byte) {
	var (
		err               error
		requestBodyAsJSON serveHTTPPostOfVolumeRequestBodyAsJSONStruct
		startTime         time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.PostVolumeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	err = json.Unmarshal(requestBody, &requestBodyAsJSON)
	if nil != err {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	err = postVolume(requestBodyAsJSON.StorageURL, requestBodyAsJSON.AuthToken)
	if nil == err {
		responseWriter.WriteHeader(http.StatusCreated)
	} else {
		responseWriter.WriteHeader(http.StatusConflict)
	}
}

func serveHTTPPut(responseWriter http.ResponseWriter, request *http.Request, requestPath string, requestBody []byte) {
	switch {
	case strings.HasPrefix(requestPath, "/volume"):
		serveHTTPPutOfVolume(responseWriter, request, requestPath, requestBody)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

type serveHTTPPutOfVolumeRequestBodyAsJSONStruct struct {
	StorageURL string
}

func serveHTTPPutOfVolume(responseWriter http.ResponseWriter, request *http.Request, requestPath string, requestBody []byte) {
	var (
		err               error
		pathSplit         []string
		requestBodyAsJSON serveHTTPPutOfVolumeRequestBodyAsJSONStruct
		startTime         time.Time
	)

	startTime = time.Now()

	pathSplit = strings.Split(requestPath, "/")

	switch len(pathSplit) {
	case 3:
		defer func() {
			globals.stats.PutVolumeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()

		err = json.Unmarshal(requestBody, &requestBodyAsJSON)
		if nil != err {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}

		err = putVolume(pathSplit[2], requestBodyAsJSON.StorageURL)
		if nil == err {
			responseWriter.WriteHeader(http.StatusCreated)
		} else {
			responseWriter.WriteHeader(http.StatusConflict)
		}
	default:
		responseWriter.WriteHeader(http.StatusBadRequest)
	}
}
