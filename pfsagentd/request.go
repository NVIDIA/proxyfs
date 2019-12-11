package main

import (
	"bytes"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swiftstack/ProxyFS/jrpcfs"
)

const jrpcResponseBufferIncrement = 1024 // Max # bytes to read from jrpcSwiftProxyBypassTCPConn

type jrpcSwiftProxyBypassRequestStruct struct {
	sync.WaitGroup
	jrpcMethod string
	jrpcParam  interface{}
	jrpcResult interface{}
	err        error
}

func startJRPCSwiftProxyBypass() {
	globals.jrpcSwiftProxyBypassRequestChan = make(chan *jrpcSwiftProxyBypassRequestStruct)
	globals.jrpcSwiftProxyBypassStopChan = make(chan struct{})
	globals.jrpcSwiftProxyBypassParentDoneWG.Add(1)
	go jrpcSwiftProxyBypassParent()
}

func stopJRPCSwiftProxyBypass() {
	globals.jrpcSwiftProxyBypassStopChan <- struct{}{}
	globals.jrpcSwiftProxyBypassParentDoneWG.Wait()
	globals.jrpcSwiftProxyBypassStopChan = nil
	globals.jrpcSwiftProxyBypassRequestChan = nil
}

func jrpcSwiftProxyBypassParent() {
	var (
		err                         error
		jrpcSwiftProxyBypassRequest *jrpcSwiftProxyBypassRequestStruct
		jrpcRequest                 []byte
		jrpcRequestID               uint64
		jrpcRequestBytePosition     int
		jrpcRequestBytesWritten     int
		jrpcResponse                []byte
		marshalErr                  error
		ok                          bool
		responseErr                 error
		unmarshalErr                error
	)

	globals.jrpcSwiftProxyBypassRequestMap = make(map[uint64]*jrpcSwiftProxyBypassRequestStruct)

	// Enter special select loop one time before doMountProxyFS() has fetched globals.jrpcSwiftProxyBypassTCPAddr

	select {
	case jrpcSwiftProxyBypassRequest = <-globals.jrpcSwiftProxyBypassRequestChan:
		// Now it's time to make the connection to ProxyFS's JSON RPC Server

		globals.jrpcSwiftProxyBypassTCPConn, err = net.DialTCP("tcp", nil, globals.jrpcSwiftProxyBypassTCPAddr)
		if nil != err {
			logFatalf("unable to net.DialTCP(\"tcp\", nil, \"%s\"): %v", globals.jrpcSwiftProxyBypassTCPAddr.String(), err)
		}

		// Fall-through to "real" select loop with jrpcSwiftProxyBypassRequest non-nil
	case _ = <-globals.jrpcSwiftProxyBypassStopChan:
		globals.jrpcSwiftProxyBypassParentDoneWG.Done()
		return
	}

	// Now that we have a jrpcSwiftProxyBypassTCPConn, launch response reader

	globals.jrpcSwiftProxyBypassResponseChan = make(chan []byte)

	globals.jrpcSwiftProxyBypassChildDoneWG.Add(1)
	go jrpcSwiftProxyBypassChild()

	// Now we can fall into the "real" select loop

	for {
		if nil != jrpcSwiftProxyBypassRequest {
			// Process jrpcSwiftProxyBypassRequest now (either from above or below)

			jrpcRequestID, jrpcRequest, marshalErr = jrpcMarshalRequest(jrpcSwiftProxyBypassRequest.jrpcMethod, jrpcSwiftProxyBypassRequest.jrpcParam)
			if nil != marshalErr {
				logFatalf("unable to marshal request (jrpcMethod=%s jrpcParam=%v): %#v", jrpcSwiftProxyBypassRequest.jrpcMethod, jrpcSwiftProxyBypassRequest.jrpcParam, marshalErr)
			}

			globals.jrpcSwiftProxyBypassRequestMap[jrpcRequestID] = jrpcSwiftProxyBypassRequest

			// Send the request

			jrpcRequestBytePosition = 0

			for jrpcRequestBytePosition < len(jrpcRequest) {
				jrpcRequestBytesWritten, err = globals.jrpcSwiftProxyBypassTCPConn.Write(jrpcRequest[jrpcRequestBytePosition:])
				if nil != err {
					logFatalf("unable to write jrpcRequest to jrpcSwiftProxyBypassTCPConn: %v", err)
				}
				jrpcRequestBytePosition += jrpcRequestBytesWritten
			}
		}

		select {
		case jrpcSwiftProxyBypassRequest = <-globals.jrpcSwiftProxyBypassRequestChan:
			// Fall-through to be executed in next for{} loop iteration
		case jrpcResponse = <-globals.jrpcSwiftProxyBypassResponseChan:
			// Process response

			jrpcRequestID, responseErr, unmarshalErr = jrpcUnmarshalResponseForIDAndError(jrpcResponse)
			if nil != unmarshalErr {
				logFatalf("unable to unmarshal response [case 1]: %v", unmarshalErr)
			}

			jrpcSwiftProxyBypassRequest, ok = globals.jrpcSwiftProxyBypassRequestMap[jrpcRequestID]
			if !ok {
				logFatalf("unable to find jrpcRequestID (0x%16X)", jrpcRequestID)
			}

			jrpcSwiftProxyBypassRequest.err = responseErr

			if nil == responseErr {
				unmarshalErr = jrpcUnmarshalResponse(jrpcRequestID, jrpcResponse, jrpcSwiftProxyBypassRequest.jrpcResult)
				if nil != unmarshalErr {
					logFatalf("unable to unmarshal response [case 2]: %v", unmarshalErr)
				}
			}

			jrpcSwiftProxyBypassRequest.Done()

			// Ensure we don't re-send it

			jrpcSwiftProxyBypassRequest = nil
		case _ = <-globals.jrpcSwiftProxyBypassStopChan:
			// Time to exit...

			globals.jrpcSwiftProxyBypassRequestMap = nil
			_ = globals.jrpcSwiftProxyBypassTCPConn.Close()
			globals.jrpcSwiftProxyBypassParentDoneWG.Done()

			return
		}
	}
}

func jrpcSwiftProxyBypassChild() {
	var (
		braceDepth      uint32
		err             error
		insideString    bool
		jrpcResponse    []byte
		jrpcReadBuf     []byte
		jrpcReadByte    byte
		jrpcReadBufPos  int
		nextByteEscaped bool
		numBytesRead    int
	)

	jrpcResponse = make([]byte, 0)

	braceDepth = 0
	insideString = false
	nextByteEscaped = false

	for {
		jrpcReadBuf = make([]byte, jrpcResponseBufferIncrement)
		numBytesRead, err = globals.jrpcSwiftProxyBypassTCPConn.Read(jrpcReadBuf)
		if nil != err {
			// Presumably this error is likely due to a call to Close()... so we should just exit
			globals.jrpcSwiftProxyBypassChildDoneWG.Done()
			return
		}
		jrpcReadBuf = jrpcReadBuf[:numBytesRead]
		for jrpcReadBufPos = 0; jrpcReadBufPos < numBytesRead; jrpcReadBufPos++ {
			jrpcReadByte = jrpcReadBuf[jrpcReadBufPos]
			jrpcResponse = append(jrpcResponse, jrpcReadByte)
			if insideString {
				if nextByteEscaped {
					nextByteEscaped = false
				} else if '\\' == jrpcReadByte {
					nextByteEscaped = true
				} else if '"' == jrpcReadByte {
					insideString = false
				} else {
					// No change to any state
				}
			} else {
				if '{' == jrpcReadByte {
					braceDepth++
				} else if '}' == jrpcReadByte {
					if 0 == braceDepth {
						logFatalf("malformed JSONRPC 2.0 received... closing brace with no opening brace")
					} else {
						braceDepth--
						if 0 == braceDepth {
							globals.jrpcSwiftProxyBypassResponseChan <- jrpcResponse
							jrpcResponse = make([]byte, 0)
						}
					}
				} else if '"' == jrpcReadByte {
					insideString = true
				} else {
					// No change to any state
				}
			}
		}
	}
}

func doMountProxyFS() {
	var (
		err                  error
		jsonRpcTCPAddrString string
		mountReply           *jrpcfs.MountByAccountNameReply
		mountRequest         *jrpcfs.MountByAccountNameRequest
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

	jsonRpcTCPAddrString = mountReply.JSONRpcTCPAddrString

	globals.jrpcSwiftProxyBypassTCPAddr, err = net.ResolveTCPAddr("tcp", jsonRpcTCPAddrString)
	if nil != err {
		logFatalf("unable to net.ResolveTCPAddr(\"tcp\", \"%s\"): %v", jsonRpcTCPAddrString, err)
	}
}

func doJRPCRequest(jrpcMethod string, jrpcParam interface{}, jrpcResult interface{}) (err error) {
	var (
		httpErr                     error
		httpRequest                 *http.Request
		jrpcRequest                 []byte
		jrpcRequestID               uint64
		jrpcResponse                []byte
		jrpcSwiftProxyBypassRequest *jrpcSwiftProxyBypassRequestStruct
		marshalErr                  error
		ok                          bool
		unmarshalErr                error
		swiftAccountURL             string
	)

	if "Server.RpcMountByAccountName" != jrpcMethod {
		jrpcSwiftProxyBypassRequest = &jrpcSwiftProxyBypassRequestStruct{
			jrpcMethod: jrpcMethod,
			jrpcParam:  jrpcParam,
			jrpcResult: jrpcResult,
		}

		jrpcSwiftProxyBypassRequest.Add(1)

		globals.jrpcSwiftProxyBypassRequestChan <- jrpcSwiftProxyBypassRequest

		jrpcSwiftProxyBypassRequest.Wait()

		err = jrpcSwiftProxyBypassRequest.err

		return
	}

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

		_ = atomic.AddUint64(&globals.metrics.HTTPRequestsInFlight, 1)
		response, err = globals.httpClient.Do(request)
		_ = atomic.AddUint64(&globals.metrics.HTTPRequestsInFlight, ^uint64(0))
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

			// Close request.Body at this time just in case...
			//
			// It appears that net/http.Do() will actually return
			// even if it has an outstanding Read() call to
			// request.Body.Read() and calling request.Body.Close()
			// will give it a chance to force request.Body.Read()
			// to exit cleanly.

			_ = request.Body.Close()
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
