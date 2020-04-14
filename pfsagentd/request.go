package main

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/retryrpc"
	"github.com/swiftstack/ProxyFS/version"
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
	globals.retryRPCPublicIPAddr = mountReply.RetryRPCPublicIPAddr
	globals.retryRPCPort = mountReply.RetryRPCPort
	globals.rootCAx509CertificatePEM = mountReply.RootCAx509CertificatePEM

	globals.retryRPCClient, err = retryrpc.NewClient(string(globals.mountID), globals.retryRPCPublicIPAddr, int(globals.retryRPCPort), globals.rootCAx509CertificatePEM, nil)
	if nil != err {
		logFatalf("unable to retryRPCClient.NewClient(%v,%v): SwiftAccountName: %v err: %v", globals.retryRPCPublicIPAddr, globals.retryRPCPort, globals.config.SwiftAccountName, err)
	}
}

func doUnmountProxyFS() {
	// TODO: Flush outstanding FileInode's
	// TODO: Tell ProxyFS we are releasing all leases
	// TODO: Tell ProxyFS we are unmounting

	globals.retryRPCClient.Close()
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
		swiftAccountURL string
		unmarshalErr    error
	)

	jrpcRequestID, jrpcRequest, marshalErr = jrpcMarshalRequest(jrpcMethod, jrpcParam)
	if nil != marshalErr {
		logFatalf("unable to marshal request (jrpcMethod=%s jrpcParam=%v): %#v", jrpcMethod, jrpcParam, marshalErr)
	}

	_, swiftAccountURL, _ = fetchAuthTokenAndURLs()

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
		retryDelay       time.Duration
		retryIndex       uint64
		swiftAuthToken   string
	)

	_ = atomic.AddUint64(&globals.metrics.HTTPRequests, 1)

	request.Header["User-Agent"] = []string{"PFSAgent " + version.ProxyFSVersion}

	okStatusCodesSet = make(map[int]struct{})
	for _, okStatusCode = range okStatusCodes {
		okStatusCodesSet[okStatusCode] = struct{}{}
	}

	retryIndex = 0

	for {
		swiftAuthToken, _, _ = fetchAuthTokenAndURLs()

		request.Header["X-Auth-Token"] = []string{swiftAuthToken}

		_ = atomic.AddUint64(&globals.metrics.HTTPRequestsInFlight, 1)
		response, err = globals.httpClient.Do(request)
		_ = atomic.AddUint64(&globals.metrics.HTTPRequestsInFlight, ^uint64(0))
		if nil != err {
			_ = atomic.AddUint64(&globals.metrics.HTTPRequestSubmissionFailures, 1)
			logErrorf("doHTTPRequest(%s %s) failed to submit request: %v", request.Method, request.URL.String(), err)
			ok = false
			return
		}

		responseBody, err = ioutil.ReadAll(response.Body)
		_ = response.Body.Close()
		if nil != err {
			_ = atomic.AddUint64(&globals.metrics.HTTPRequestResponseBodyCorruptions, 1)
			logErrorf("doHTTPRequest(%s %s) failed to read responseBody: %v", request.Method, request.URL.String(), err)
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
			logWarnf("doHTTPRequest(%s %s) reached SwiftRetryLimit", request.Method, request.URL.String())
			ok = false
			return
		}

		if http.StatusUnauthorized == response.StatusCode {
			_ = atomic.AddUint64(&globals.metrics.HTTPRequestsRequiringReauthorization, 1)
			logInfof("doHTTPRequest(%s %s) needs to call updateAuthTokenAndAccountURL()", request.Method, request.URL.String())
			updateAuthTokenAndAccountURL()
		} else {
			logWarnf("doHTTPRequest(%s %s) needs to retry due to unexpected http.Status: %s", request.Method, request.URL.String(), response.Status)

			// Close request.Body (if any) at this time just in case...
			//
			// It appears that net/http.Do() will actually return
			// even if it has an outstanding Read() call to
			// request.Body.Read() and calling request.Body.Close()
			// will give it a chance to force request.Body.Read()
			// to exit cleanly.

			if nil != request.Body {
				_ = request.Body.Close()
			}
		}

		retryDelay = globals.retryDelay[retryIndex].nominal - time.Duration(rand.Int63n(int64(globals.retryDelay[retryIndex].variance)))
		time.Sleep(retryDelay)
		retryIndex++

		_ = atomic.AddUint64(&globals.metrics.HTTPRequestRetries, 1)
	}
}

func fetchAuthTokenAndURLs() (swiftAuthToken string, swiftAccountURL string, swiftAccountBypassURL string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftAuthToken = globals.swiftAuthToken
			swiftAccountURL = globals.swiftAccountURL
			swiftAccountBypassURL = globals.swiftAccountBypassURL
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
		swiftAccountBypassURL       string
		swiftAccountBypassURLSplit  []string
		swiftAccountURL             string
		swiftAuthToken              string
		swiftStorageAccountURLSplit []string
		swiftStorageURL             string
	)

	globals.Lock()

	if nil != globals.swiftAuthWaitGroup {
		globals.Unlock()

		_, _, _ = fetchAuthTokenAndURLs()

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

	getRequest.Header.Add("User-Agent", "PFSAgent "+version.ProxyFSVersion)

	getResponse, err = globals.httpClient.Do(getRequest)
	if nil != err {
		logErrorf("updateAuthTokenAndAccountURL() failed to submit request: %v", err)
		swiftAuthToken = ""
		swiftAccountURL = ""
		swiftAccountBypassURL = ""
	} else {
		_, err = ioutil.ReadAll(getResponse.Body)
		_ = getResponse.Body.Close()
		if nil != err {
			logErrorf("updateAuthTokenAndAccountURL() failed to read responseBody: %v", err)
			swiftAuthToken = ""
			swiftAccountURL = ""
			swiftAccountBypassURL = ""
		} else {
			if http.StatusOK != getResponse.StatusCode {
				logWarnf("updateAuthTokenAndAccountURL() got unexpected http.Status %s (%d)", getResponse.Status, getResponse.StatusCode)
				swiftAuthToken = ""
				swiftAccountURL = ""
				swiftAccountBypassURL = ""
			} else {
				swiftAuthToken = getResponse.Header.Get("X-Auth-Token")
				swiftStorageURL = getResponse.Header.Get("X-Storage-Url")

				swiftStorageAccountURLSplit = strings.Split(swiftStorageURL, "/")
				if 0 == len(swiftStorageAccountURLSplit) {
					swiftAccountURL = ""
					swiftAccountBypassURL = ""
				} else {
					swiftStorageAccountURLSplit[len(swiftStorageAccountURLSplit)-1] = globals.config.SwiftAccountName
					swiftAccountURL = strings.Join(swiftStorageAccountURLSplit, "/")

					if strings.HasPrefix(swiftAccountURL, "http:") && strings.HasPrefix(getRequest.URL.String(), "https:") {
						swiftAccountURL = strings.Replace(swiftAccountURL, "http:", "https:", 1)
					}

					swiftAccountBypassURLSplit = strings.Split(swiftAccountURL, "/")
					swiftAccountBypassURLSplit[len(swiftStorageAccountURLSplit)-2] = "proxyfs"
					swiftAccountBypassURL = strings.Join(swiftAccountBypassURLSplit, "/")
				}
			}
		}
	}

	globals.Lock()

	globals.swiftAuthToken = swiftAuthToken
	globals.swiftAccountURL = swiftAccountURL
	globals.swiftAccountBypassURL = swiftAccountBypassURL

	globals.swiftAuthWaitGroup.Done()
	globals.swiftAuthWaitGroup = nil

	globals.Unlock()
}
