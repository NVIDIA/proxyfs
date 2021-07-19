// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/NVIDIA/proxyfs/ilayout"
)

func startSwiftClient() (err error) {
	var (
		customTransport  *http.Transport
		defaultTransport *http.Transport
		ok               bool
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

	err = nil
	return
}

func stopSwiftClient() (err error) {
	err = nil
	return
}

func swiftObjectDelete(storageURL string, authToken string, objectNumber uint64) (err error) {
	var (
		httpRequest         *http.Request
		httpResponse        *http.Response
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.SwiftObjectDeleteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		httpRequest, err = http.NewRequest("DELETE", objectURL, nil)
		if nil != err {
			return
		}

		if "" != authToken {
			httpRequest.Header["X-Auth-Token"] = []string{authToken}
		}

		httpResponse, err = globals.httpClient.Do(httpRequest)
		if nil != err {
			err = fmt.Errorf("globals.httpClient.Do(HEAD %s) failed: %v\n", storageURL, err)
			return
		}

		_, err = ioutil.ReadAll(httpResponse.Body)
		if nil != err {
			err = fmt.Errorf("ioutil.ReadAll(httpResponse.Body) failed: %v\n", err)
			return
		}
		err = httpResponse.Body.Close()
		if nil != err {
			err = fmt.Errorf("httpResponse.Body.Close() failed: %v\n", err)
			return
		}

		if (200 <= httpResponse.StatusCode) && (299 >= httpResponse.StatusCode) {
			err = nil
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	return
}

func swiftObjectGet(storageURL string, authToken string, objectNumber uint64) (buf []byte, err error) {
	var (
		httpRequest         *http.Request
		httpResponse        *http.Response
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.SwiftObjectGetUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		httpRequest, err = http.NewRequest("GET", objectURL, nil)
		if nil != err {
			return
		}

		if authToken != "" {
			httpRequest.Header["X-Auth-Token"] = []string{authToken}
		}

		httpResponse, err = globals.httpClient.Do(httpRequest)
		if nil != err {
			err = fmt.Errorf("globals.httpClient.Do(HEAD %s) failed: %v", storageURL, err)
			return
		}

		buf, err = ioutil.ReadAll(httpResponse.Body)
		if nil != err {
			err = fmt.Errorf("ioutil.ReadAll(httpResponse.Body) failed: %v", err)
			return
		}
		err = httpResponse.Body.Close()
		if nil != err {
			err = fmt.Errorf("httpResponse.Body.Close() failed: %v", err)
			return
		}

		if (200 <= httpResponse.StatusCode) && (299 >= httpResponse.StatusCode) {
			err = nil
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	return
}

func swiftObjectGetRange(storageURL string, authToken string, objectNumber uint64, objectOffset uint64, objectLength uint64) (buf []byte, err error) {
	var (
		httpRequest         *http.Request
		httpResponse        *http.Response
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		rangeHeaderValue    string
		startTime           time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.SwiftObjectGetRangeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	rangeHeaderValue = fmt.Sprintf("bytes=%d-%d", objectOffset, (objectOffset + objectLength - 1))

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		httpRequest, err = http.NewRequest("GET", objectURL, nil)
		if nil != err {
			return
		}

		httpRequest.Header["Range"] = []string{rangeHeaderValue}

		if authToken != "" {
			httpRequest.Header["X-Auth-Token"] = []string{authToken}
		}

		httpResponse, err = globals.httpClient.Do(httpRequest)
		if nil != err {
			err = fmt.Errorf("globals.httpClient.Do(HEAD %s) failed: %v", storageURL, err)
			return
		}

		buf, err = ioutil.ReadAll(httpResponse.Body)
		if nil != err {
			err = fmt.Errorf("ioutil.ReadAll(httpResponse.Body) failed: %v", err)
			return
		}
		err = httpResponse.Body.Close()
		if nil != err {
			err = fmt.Errorf("httpResponse.Body.Close() failed: %v", err)
			return
		}

		if (200 <= httpResponse.StatusCode) && (299 >= httpResponse.StatusCode) {
			err = nil
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	return
}

func swiftObjectGetTail(storageURL string, authToken string, objectNumber uint64, objectLength uint64) (buf []byte, err error) {
	var (
		httpRequest         *http.Request
		httpResponse        *http.Response
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		rangeHeaderValue    string
		startTime           time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.SwiftObjectGetTailUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	rangeHeaderValue = fmt.Sprintf("bytes=-%d", objectLength)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		httpRequest, err = http.NewRequest("GET", objectURL, nil)
		if nil != err {
			return
		}

		httpRequest.Header["Range"] = []string{rangeHeaderValue}

		if authToken != "" {
			httpRequest.Header["X-Auth-Token"] = []string{authToken}
		}

		httpResponse, err = globals.httpClient.Do(httpRequest)
		if nil != err {
			err = fmt.Errorf("globals.httpClient.Do(HEAD %s) failed: %v", storageURL, err)
			return
		}

		buf, err = ioutil.ReadAll(httpResponse.Body)
		if nil != err {
			err = fmt.Errorf("ioutil.ReadAll(httpResponse.Body) failed: %v", err)
			return
		}
		err = httpResponse.Body.Close()
		if nil != err {
			err = fmt.Errorf("httpResponse.Body.Close() failed: %v", err)
			return
		}

		if (200 <= httpResponse.StatusCode) && (299 >= httpResponse.StatusCode) {
			err = nil
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	return
}

func swiftObjectPut(storageURL string, authToken string, objectNumber uint64, body io.ReadSeeker) (err error) {
	var (
		httpRequest         *http.Request
		httpResponse        *http.Response
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.SwiftObjectPutUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		body.Seek(0, io.SeekStart)

		httpRequest, err = http.NewRequest("PUT", objectURL, body)
		if nil != err {
			return
		}

		if authToken != "" {
			httpRequest.Header["X-Auth-Token"] = []string{authToken}
		}

		httpResponse, err = globals.httpClient.Do(httpRequest)
		if nil != err {
			err = fmt.Errorf("globals.httpClient.Do(HEAD %s) failed: %v", storageURL, err)
			return
		}

		_, err = ioutil.ReadAll(httpResponse.Body)
		if nil != err {
			err = fmt.Errorf("ioutil.ReadAll(httpResponse.Body) failed: %v", err)
			return
		}
		err = httpResponse.Body.Close()
		if nil != err {
			err = fmt.Errorf("httpResponse.Body.Close() failed: %v", err)
			return
		}

		if (200 <= httpResponse.StatusCode) && (299 >= httpResponse.StatusCode) {
			err = nil
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	return
}
