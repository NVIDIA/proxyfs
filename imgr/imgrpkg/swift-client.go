// Copyright (c) 2015-2022, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"container/list"
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
		err = fmt.Errorf("http.DefaultTransport.(*http.Transport) returned !ok")
		return
	}

	customTransport = &http.Transport{ // Up-to-date as of Golang 1.17
		Proxy:                  defaultTransport.Proxy,
		DialContext:            defaultTransport.DialContext,
		DialTLSContext:         defaultTransport.DialTLSContext,
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
		WriteBufferSize:        0,
		ReadBufferSize:         0,
	}

	globals.swiftHTTPClient = &http.Client{
		Transport: customTransport,
		Timeout:   globals.config.SwiftTimeout,
	}

	err = nil
	return
}

func stopSwiftClient() (err error) {
	globals.swiftHTTPClient = nil

	err = nil
	return
}

func swiftObjectDeleteOnce(objectURL string, authToken string) (authOK bool, err error) {
	var (
		httpRequest  *http.Request
		httpResponse *http.Response
	)

	httpRequest, err = http.NewRequest("DELETE", objectURL, nil)
	if nil != err {
		return
	}

	if authToken != "" {
		httpRequest.Header["X-Auth-Token"] = []string{authToken}
	}

	httpResponse, err = globals.swiftHTTPClient.Do(httpRequest)
	if nil != err {
		err = fmt.Errorf("globals.swiftHTTPClient.Do(DELETE %s) failed: %v", objectURL, err)
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
		authOK = true
		err = nil
	} else if http.StatusUnauthorized == httpResponse.StatusCode {
		authOK = false // Auth failed,
		err = nil      //   but we will still indicate the func succeeded
	} else {
		err = fmt.Errorf("httpResponse.Status: %s", httpResponse.Status)
	}

	return
}

func (volume *volumeStruct) swiftObjectDeleteOnce(alreadyLocked bool, objectURL string) (authOK bool, err error) {
	var (
		authToken            string
		mount                *mountStruct
		mountListElement     *list.Element
		ok                   bool
		usingVolumeAuthToken bool
	)

	if !alreadyLocked {
		globals.Lock()
	}

	mountListElement = volume.healthyMountList.Front()

	if mountListElement == nil {
		if volume.authToken == "" {
			if !alreadyLocked {
				globals.Unlock()
			}
			authOK = false // Auth failed,
			err = nil      //   but we still indicate the func succeeded
			return
		}

		authToken = volume.authToken
		usingVolumeAuthToken = true
	} else {
		mount, ok = mountListElement.Value.(*mountStruct)
		if !ok {
			logFatalf("mountListElement.Value.(*mountStruct) returned !ok")
		}

		// We know that mount.mountListMembership == onHealthyMountList

		volume.healthyMountList.MoveToBack(mount.mountListElement)

		authToken = mount.authToken
		usingVolumeAuthToken = true
	}

	if !alreadyLocked {
		globals.Unlock()
	}

	authOK, err = swiftObjectDeleteOnce(objectURL, authToken)
	if err == nil {
		if !authOK {
			if !alreadyLocked {
				globals.Lock()
			}

			if usingVolumeAuthToken {
				logWarnf("swiftObjectDeleteOnce(,volume.authToken) returned !authOK for volume %s...clearing volume.authToken", volume.name)
				volume.authToken = ""
			} else {
				mount.authTokenExpired = true

				// It's possible that mount has "moved" from volume.healthyMountList

				switch mount.mountListMembership {
				case onHealthyMountList:
					_ = mount.volume.healthyMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onLeasesExpiredMountList:
					_ = mount.volume.leasesExpiredMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onAuthTokenExpiredMountList:
					volume.authTokenExpiredMountList.MoveToBack(mount.mountListElement)
				default:
					logFatalf("mount.mountListMembership (%v) not one of on{Healthy|LeasesExpired|AuthTokenExpired}MountList")
				}
			}

			if !alreadyLocked {
				globals.Unlock()
			}
		}
	}

	return
}

func swiftObjectDelete(storageURL string, authToken string, objectNumber uint64) (err error) {
	var (
		authOK              bool
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectDeleteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		authOK, err = swiftObjectDeleteOnce(objectURL, authToken)
		if nil == err {
			if !authOK {
				err = fmt.Errorf("httpResponse.Status: http.StatusUnauthorized")
			}
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")

	return
}

func (volume *volumeStruct) swiftObjectDelete(alreadyLocked bool, objectNumber uint64) (authOK bool, err error) {
	var (
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectDeleteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = volume.storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		authOK, err = volume.swiftObjectDeleteOnce(alreadyLocked, objectURL)
		if nil == err {
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	if (volume.healthyMountList.Len() == 0) && (volume.authToken == "") {
		authOK = false // Auth failed,
		err = nil      //   but we will still indicate the func succeeded
	} else {
		authOK = true
		err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	}

	return
}

func swiftObjectGetOnce(objectURL string, authToken string, rangeHeaderValue string) (buf []byte, authOK bool, err error) {
	var (
		httpRequest  *http.Request
		httpResponse *http.Response
	)

	httpRequest, err = http.NewRequest("GET", objectURL, nil)
	if nil != err {
		return
	}

	if authToken != "" {
		httpRequest.Header["X-Auth-Token"] = []string{authToken}
	}
	if rangeHeaderValue != "" {
		httpRequest.Header["Range"] = []string{rangeHeaderValue}
	}

	httpResponse, err = globals.swiftHTTPClient.Do(httpRequest)
	if nil != err {
		err = fmt.Errorf("globals.swiftHTTPClient.Do(GET %s) failed: %v", objectURL, err)
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
		authOK = true
		err = nil
	} else if http.StatusUnauthorized == httpResponse.StatusCode {
		authOK = false // Auth failed,
		err = nil      //   but we will still indicate the func succeeded
	} else {
		err = fmt.Errorf("httpResponse.Status: %s", httpResponse.Status)
	}

	return
}

func (volume *volumeStruct) swiftObjectGetOnce(alreadyLocked bool, objectURL string, rangeHeaderValue string) (buf []byte, authOK bool, err error) {
	var (
		authToken            string
		mount                *mountStruct
		mountListElement     *list.Element
		ok                   bool
		usingVolumeAuthToken bool
	)

	if !alreadyLocked {
		globals.Lock()
	}

	mountListElement = volume.healthyMountList.Front()

	if mountListElement == nil {
		if volume.authToken == "" {
			if !alreadyLocked {
				globals.Unlock()
			}
			authOK = false // Auth failed,
			err = nil      //   but we still indicate the func succeeded
			return
		}

		authToken = volume.authToken
		usingVolumeAuthToken = true
	} else {
		mount, ok = mountListElement.Value.(*mountStruct)
		if !ok {
			logFatalf("mountListElement.Value.(*mountStruct) returned !ok")
		}

		// We know that mount.mountListMembership == onHealthyMountList

		volume.healthyMountList.MoveToBack(mount.mountListElement)

		authToken = mount.authToken
		usingVolumeAuthToken = true
	}

	if !alreadyLocked {
		globals.Unlock()
	}

	buf, authOK, err = swiftObjectGetOnce(objectURL, authToken, rangeHeaderValue)
	if err == nil {
		if !authOK {
			if !alreadyLocked {
				globals.Lock()
			}

			if usingVolumeAuthToken {
				logWarnf("swiftObjectGetOnce(,volume.authToken,) returned !authOK for volume %s...clearing volume.authToken", volume.name)
				volume.authToken = ""
			} else {
				mount.authTokenExpired = true

				// It's possible that mount has "moved" from volume.healthyMountList

				switch mount.mountListMembership {
				case onHealthyMountList:
					_ = mount.volume.healthyMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onLeasesExpiredMountList:
					_ = mount.volume.leasesExpiredMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onAuthTokenExpiredMountList:
					volume.authTokenExpiredMountList.MoveToBack(mount.mountListElement)
				default:
					logFatalf("mount.mountListMembership (%v) not one of on{Healthy|LeasesExpired|AuthTokenExpired}MountList")
				}
			}

			if !alreadyLocked {
				globals.Unlock()
			}
		}
	}

	return
}

func swiftObjectGet(storageURL string, authToken string, objectNumber uint64) (buf []byte, err error) {
	var (
		authOK              bool
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectGetUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		buf, authOK, err = swiftObjectGetOnce(objectURL, authToken, "")
		if nil == err {
			if !authOK {
				err = fmt.Errorf("httpResponse.Status: http.StatusUnauthorized")
			}
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")

	return
}

func (volume *volumeStruct) swiftObjectGet(alreadyLocked bool, objectNumber uint64) (buf []byte, authOK bool, err error) {
	var (
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectGetUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = volume.storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		buf, authOK, err = volume.swiftObjectGetOnce(alreadyLocked, objectURL, "")
		if nil == err {
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	if (volume.healthyMountList.Len() == 0) && (volume.authToken == "") {
		authOK = false // Auth failed,
		err = nil      //   but we will still indicate the func succeeded
	} else {
		authOK = true
		err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	}

	return
}

func swiftObjectGetRange(storageURL string, authToken string, objectNumber uint64, objectOffset uint64, objectLength uint64) (buf []byte, err error) {
	var (
		authOK              bool
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		rangeHeaderValue    string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectGetRangeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	rangeHeaderValue = fmt.Sprintf("bytes=%d-%d", objectOffset, (objectOffset + objectLength - 1))

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		buf, authOK, err = swiftObjectGetOnce(objectURL, authToken, rangeHeaderValue)
		if nil == err {
			if !authOK {
				err = fmt.Errorf("httpResponse.Status: http.StatusUnauthorized")
			}
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")

	return
}

func (volume *volumeStruct) swiftObjectGetRange(alreadyLocked bool, objectNumber uint64, objectOffset uint64, objectLength uint64) (buf []byte, authOK bool, err error) {
	var (
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		rangeHeaderValue    string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectGetRangeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = volume.storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	rangeHeaderValue = fmt.Sprintf("bytes=%d-%d", objectOffset, (objectOffset + objectLength - 1))

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		buf, authOK, err = volume.swiftObjectGetOnce(alreadyLocked, objectURL, rangeHeaderValue)
		if nil == err {
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	if (volume.healthyMountList.Len() == 0) && (volume.authToken == "") {
		authOK = false // Auth failed,
		err = nil      //   but we will still indicate the func succeeded
	} else {
		authOK = true
		err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	}

	return
}

func swiftObjectGetTail(storageURL string, authToken string, objectNumber uint64, objectLength uint64) (buf []byte, err error) {
	var (
		authOK              bool
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		rangeHeaderValue    string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectGetTailUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	rangeHeaderValue = fmt.Sprintf("bytes=-%d", objectLength)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		buf, authOK, err = swiftObjectGetOnce(objectURL, authToken, rangeHeaderValue)
		if nil == err {
			if !authOK {
				err = fmt.Errorf("httpResponse.Status: http.StatusUnauthorized")
			}
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")

	return
}

func (volume *volumeStruct) swiftObjectGetTail(alreadyLocked bool, objectNumber uint64, objectLength uint64) (buf []byte, authOK bool, err error) {
	var (
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		rangeHeaderValue    string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectGetTailUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = volume.storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	rangeHeaderValue = fmt.Sprintf("bytes=-%d", objectLength)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		buf, authOK, err = volume.swiftObjectGetOnce(alreadyLocked, objectURL, rangeHeaderValue)
		if nil == err {
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	if (volume.healthyMountList.Len() == 0) && (volume.authToken == "") {
		authOK = false // Auth failed,
		err = nil      //   but we will still indicate the func succeeded
	} else {
		authOK = true
		err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	}

	return
}

func swiftObjectPutOnce(objectURL string, authToken string, body io.ReadSeeker) (authOK bool, err error) {
	var (
		httpRequest  *http.Request
		httpResponse *http.Response
	)

	body.Seek(0, io.SeekStart)

	httpRequest, err = http.NewRequest("PUT", objectURL, body)
	if nil != err {
		return
	}

	if authToken != "" {
		httpRequest.Header["X-Auth-Token"] = []string{authToken}
	}

	httpResponse, err = globals.swiftHTTPClient.Do(httpRequest)
	if nil != err {
		err = fmt.Errorf("globals.swiftHTTPClient.Do(PUT %s) failed: %v", objectURL, err)
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
		authOK = true
		err = nil
	} else if http.StatusUnauthorized == httpResponse.StatusCode {
		authOK = false // Auth failed,
		err = nil      //   but we will still indicate the func succeeded
	} else {
		err = fmt.Errorf("httpResponse.Status: %s", httpResponse.Status)
	}

	return
}

func (volume *volumeStruct) swiftObjectPutOnce(alreadyLocked bool, objectURL string, body io.ReadSeeker) (authOK bool, err error) {
	var (
		authToken            string
		mount                *mountStruct
		mountListElement     *list.Element
		ok                   bool
		usingVolumeAuthToken bool
	)

	if !alreadyLocked {
		globals.Lock()
	}

	mountListElement = volume.healthyMountList.Front()

	if mountListElement == nil {
		if volume.authToken == "" {
			if !alreadyLocked {
				globals.Unlock()
			}
			authOK = false // Auth failed,
			err = nil      //   but we still indicate the func succeeded
			return
		}

		authToken = volume.authToken
		usingVolumeAuthToken = true
	} else {
		mount, ok = mountListElement.Value.(*mountStruct)
		if !ok {
			logFatalf("mountListElement.Value.(*mountStruct) returned !ok")
		}

		// We know that mount.mountListMembership == onHealthyMountList

		volume.healthyMountList.MoveToBack(mount.mountListElement)

		authToken = mount.authToken
		usingVolumeAuthToken = true
	}

	if !alreadyLocked {
		globals.Unlock()
	}

	authOK, err = swiftObjectPutOnce(objectURL, authToken, body)
	if err == nil {
		if !authOK {
			if !alreadyLocked {
				globals.Lock()
			}

			if usingVolumeAuthToken {
				logWarnf("swiftObjectPutOnce(,volume.authToken,) returned !authOK for volume %s...clearing volume.authToken", volume.name)
				volume.authToken = ""
			} else {
				mount.authTokenExpired = true

				// It's possible that mount has "moved" from volume.healthyMountList

				switch mount.mountListMembership {
				case onHealthyMountList:
					_ = mount.volume.healthyMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onLeasesExpiredMountList:
					_ = mount.volume.leasesExpiredMountList.Remove(mount.mountListElement)
					mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
					mount.mountListMembership = onAuthTokenExpiredMountList
				case onAuthTokenExpiredMountList:
					volume.authTokenExpiredMountList.MoveToBack(mount.mountListElement)
				default:
					logFatalf("mount.mountListMembership (%v) not one of on{Healthy|LeasesExpired|AuthTokenExpired}MountList")
				}
			}

			if !alreadyLocked {
				globals.Unlock()
			}
		}
	}

	return
}

func swiftObjectPut(storageURL string, authToken string, objectNumber uint64, body io.ReadSeeker) (err error) {
	var (
		authOK              bool
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectPutUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		authOK, err = swiftObjectPutOnce(objectURL, authToken, body)
		if nil == err {
			if !authOK {
				err = fmt.Errorf("httpResponse.Status: http.StatusUnauthorized")
			}
			return
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")

	return
}

func (volume *volumeStruct) swiftObjectPut(alreadyLocked bool, objectNumber uint64, body io.ReadSeeker) (authOK bool, err error) {
	var (
		nextSwiftRetryDelay time.Duration
		numSwiftRetries     uint32
		objectURL           string
		startTime           time.Time = time.Now()
	)

	defer func() {
		globals.stats.SwiftObjectPutUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	objectURL = volume.storageURL + "/" + ilayout.GetObjectNameAsString(objectNumber)

	nextSwiftRetryDelay = globals.config.SwiftRetryDelay

	for numSwiftRetries = 0; numSwiftRetries <= globals.config.SwiftRetryLimit; numSwiftRetries++ {
		authOK, err = volume.swiftObjectPutOnce(alreadyLocked, objectURL, body)
		if nil == err {
			if authOK {
				return
			}
		}

		time.Sleep(nextSwiftRetryDelay)

		nextSwiftRetryDelay = time.Duration(float64(nextSwiftRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	if (volume.healthyMountList.Len() == 0) && (volume.authToken == "") {
		authOK = false // Auth failed,
		err = nil      //   but we will still indicate the func succeeded
	} else {
		authOK = true
		err = fmt.Errorf("globals.config.SwiftRetryLimit exceeded")
	}

	return
}
