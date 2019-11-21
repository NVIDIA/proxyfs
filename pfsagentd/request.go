package main

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swiftstack/ProxyFS/jrpcfs"
)

func doMountProxyFS() {
	var (
		err          error
		mountReply   *jrpcfs.MountByAccountNameReply
		mountRequest *jrpcfs.MountByAccountNameRequest
	)

	mountRequest = &jrpcfs.MountByAccountNameRequest{
		AccountName:  globals.config.SwiftAccountName,
		MountOptions: 0,
		AuthUserID:   0,
		AuthGroupID:  0,
	}

	mountReply = &jrpcfs.MountByAccountNameReply{}

	err = doJRPCRequest("Server.RpcMountByAccountName", mountRequest, mountReply)

	if nil != err {
		logFatalf("unable to mount PROXYFS SwiftAccount %v: %v", globals.config.SwiftAccountName, err)
	}

	globals.mountID = mountReply.MountID
	globals.rootDirInodeNumber = uint64(mountReply.RootDirInodeNumber)
}

func doJRPCRequest(jrpcMethod string, jrpcParam interface{}, jrpcResult interface{}) (err error) {
	var (
		httpErr         error
		httpRequest     *http.Request
		jrpcRequest     []byte
		jrpcRequestID   uint64
		jrpcResponse    []byte
		marshalErr      error
		ok              bool
		unmarshalErr    error
		swiftAccountURL string
	)

	jrpcRequestID, jrpcRequest, marshalErr = jrpcMarshalRequest(jrpcMethod, jrpcParam)
	if nil != marshalErr {
		logFatalf("unable to marshal request (jrpcMethod=%s jrpcParam=%v): %#v", jrpcMethod, jrpcParam, marshalErr)
	}

	_, swiftAccountURL = fetchAuthTokenAndAccountURL()

	httpRequest, httpErr = http.NewRequest("PROXYFS", swiftAccountURL, bytes.NewReader(jrpcRequest))
	if nil != httpErr {
		logFatalf("unable to create PROXYFS http.Request (jrpcMethod=%s jrpcParam=%#v): %v", jrpcMethod, jrpcParam, httpErr)
	}

	httpRequest.Header["Content-Type"] = []string{"application/json"}

	_, jrpcResponse, ok, _ = doHTTPRequest(httpRequest, http.StatusOK, http.StatusUnprocessableEntity)
	if !ok {
		logFatalf("unable to contact ProxyFS")
	}

	_, err, unmarshalErr = jrpcUnmarshalResponseForIDAndError(jrpcResponse)
	if nil != unmarshalErr {
		logFatalf("unable to unmarshal response [case 1] (jrpcMethod=%s jrpcParam=%#v): %v", jrpcMethod, jrpcParam, unmarshalErr)
	}

	if nil != err {
		return
	}

	unmarshalErr = jrpcUnmarshalResponse(jrpcRequestID, jrpcResponse, jrpcResult)
	if nil != unmarshalErr {
		logFatalf("unable to unmarshal response [case 2] (jrpcMethod=%s jrpcParam=%#v): %v", jrpcMethod, jrpcParam, unmarshalErr)
	}

	return
}

func doHTTPRequest(request *http.Request, okStatusCodes ...int) (response *http.Response, responseBody []byte, ok bool, statusCode int) {
	var (
		err              error
		okStatusCode     int
		okStatusCodesSet map[int]struct{}
		retryIndex       uint64
		swiftAuthToken   string
	)

	_ = atomic.AddUint64(&globals.metrics.HTTPRequests, 1)

	okStatusCodesSet = make(map[int]struct{})
	for _, okStatusCode = range okStatusCodes {
		okStatusCodesSet[okStatusCode] = struct{}{}
	}

	retryIndex = 0

	for {
		swiftAuthToken, _ = fetchAuthTokenAndAccountURL()

		request.Header["X-Auth-Token"] = []string{swiftAuthToken}

		response, err = globals.httpClient.Do(request)
		if nil != err {
			_ = atomic.AddUint64(&globals.metrics.HTTPRequestSubmissionFailures, 1)
			logErrorf("doHTTPRequest() failed to submit request: %v", err)
			ok = false
			return
		}

		responseBody, err = ioutil.ReadAll(response.Body)
		_ = response.Body.Close()
		if nil != err {
			_ = atomic.AddUint64(&globals.metrics.HTTPRequestResponseBodyCorruptions, 1)
			logErrorf("doHTTPRequest() failed to read responseBody: %v", err)
			ok = false
			return
		}

		_, ok = okStatusCodesSet[response.StatusCode]
		if ok {
			statusCode = response.StatusCode
			return
		}

		if retryIndex >= globals.config.SwiftRetryLimit {
			_ = atomic.AddUint64(&globals.metrics.HTTPRequestRetryLimitExceededCount, 1)
			logWarnf("doHTTPRequest() reached SwiftRetryLimit")
			ok = false
			return
		}

		if http.StatusUnauthorized == response.StatusCode {
			_ = atomic.AddUint64(&globals.metrics.HTTPRequestsRequiringReauthorization, 1)
			logInfof("doHTTPRequest() needs to call updateAuthTokenAndAccountURL()")
			updateAuthTokenAndAccountURL()
		} else {
			logWarnf("doHTTPRequest() needs to retry due to unexpected http.Status: %s", response.Status)
		}

		time.Sleep(globals.retryDelay[retryIndex])
		retryIndex++

		_ = atomic.AddUint64(&globals.metrics.HTTPRequestRetries, 1)
	}
}

func fetchAuthTokenAndAccountURL() (swiftAuthToken string, swiftAccountURL string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftAuthToken = globals.swiftAuthToken
			swiftAccountURL = globals.swiftAccountURL
			globals.Unlock()
			return
		}

		globals.Unlock()

		swiftAuthWaitGroup.Wait()
	}
}

func updateAuthTokenAndAccountURL() {
	var (
		err                         error
		getRequest                  *http.Request
		getResponse                 *http.Response
		swiftAuthToken              string
		swiftAccountURL             string
		swiftStorageAccountURLSplit []string
		swiftStorageURL             string
	)

	globals.Lock()

	if nil != globals.swiftAuthWaitGroup {
		globals.Unlock()

		_, _ = fetchAuthTokenAndAccountURL()

		return
	}

	globals.swiftAuthWaitGroup = &sync.WaitGroup{}
	globals.swiftAuthWaitGroup.Add(1)

	globals.Unlock()

	getRequest, err = http.NewRequest("GET", globals.config.SwiftAuthURL, nil)
	if nil != err {
		logFatal(err)
	}

	getRequest.Header.Add("X-Auth-User", globals.config.SwiftAuthUser)
	getRequest.Header.Add("X-Auth-Key", globals.config.SwiftAuthKey)

	getResponse, err = globals.httpClient.Do(getRequest)
	if nil != err {
		logErrorf("updateAuthTokenAndAccountURL() failed to submit request: %v", err)
		swiftAuthToken = ""
		swiftAccountURL = ""
	} else {
		_, err = ioutil.ReadAll(getResponse.Body)
		_ = getResponse.Body.Close()
		if nil != err {
			logErrorf("updateAuthTokenAndAccountURL() failed to read responseBody: %v", err)
			swiftAuthToken = ""
			swiftAccountURL = ""
		} else {
			if http.StatusOK != getResponse.StatusCode {
				logWarnf("updateAuthTokenAndAccountURL() got unexpected http.Status %s (%d)", getResponse.Status, getResponse.StatusCode)
				swiftAuthToken = ""
				swiftAccountURL = ""
			} else {
				swiftAuthToken = getResponse.Header.Get("X-Auth-Token")
				swiftStorageURL = getResponse.Header.Get("X-Storage-Url")

				swiftStorageAccountURLSplit = strings.Split(swiftStorageURL, "/")
				if 0 == len(swiftStorageAccountURLSplit) {
					swiftAccountURL = ""
				} else {
					swiftStorageAccountURLSplit[len(swiftStorageAccountURLSplit)-1] = globals.config.SwiftAccountName
					swiftAccountURL = strings.Join(swiftStorageAccountURLSplit, "/")

					if strings.HasPrefix(swiftAccountURL, "http:") && strings.HasPrefix(getRequest.URL.String(), "https:") {
						swiftAccountURL = strings.Replace(swiftAccountURL, "http:", "https:", 1)
					}
				}
			}
		}
	}

	globals.Lock()

	globals.swiftAuthToken = swiftAuthToken
	globals.swiftAccountURL = swiftAccountURL

	globals.swiftAuthWaitGroup.Done()
	globals.swiftAuthWaitGroup = nil

	globals.Unlock()
}
