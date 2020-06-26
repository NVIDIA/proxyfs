package main

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/retryrpc"
	"github.com/swiftstack/ProxyFS/version"
)

const (
	authPipeReadBufSize = 1024
)

type authOutStruct struct {
	AuthToken  string
	StorageURL string
}

func doMountProxyFS() {
	var (
		accountName          string
		err                  error
		mountReply           *jrpcfs.MountByAccountNameReply
		mountRequest         *jrpcfs.MountByAccountNameRequest
		swiftStorageURL      string
		swiftStorageURLSplit []string
	)

	swiftStorageURL = fetchStorageURL()

	swiftStorageURLSplit = strings.Split(swiftStorageURL, "/")

	accountName = swiftStorageURLSplit[4]

	mountRequest = &jrpcfs.MountByAccountNameRequest{
		AccountName:  accountName,
		MountOptions: 0,
		AuthUserID:   0,
		AuthGroupID:  0,
	}

	mountReply = &jrpcfs.MountByAccountNameReply{}

	err = doJRPCRequest("Server.RpcMountByAccountName", mountRequest, mountReply)
	if nil != err {
		logFatalf("unable to mount Volume %s (Account: %s): %v", globals.config.FUSEVolumeName, accountName, err)
	}

	globals.mountID = mountReply.MountID
	globals.rootDirInodeNumber = uint64(mountReply.RootDirInodeNumber)
	globals.retryRPCPublicIPAddr = mountReply.RetryRPCPublicIPAddr
	globals.retryRPCPort = mountReply.RetryRPCPort
	globals.rootCAx509CertificatePEM = mountReply.RootCAx509CertificatePEM

	retryrpcConfig := &retryrpc.ClientConfig{MyUniqueID: string(globals.mountID), IPAddr: globals.retryRPCPublicIPAddr, Port: int(globals.retryRPCPort),
		RootCAx509CertificatePEM: globals.rootCAx509CertificatePEM, DeadlineIO: globals.config.RetryRPCDeadlineIO,
		KeepAlivePeriod: globals.config.RetryRPCKeepAlivePeriod}
	globals.retryRPCClient, err = retryrpc.NewClient(retryrpcConfig)
	if nil != err {
		logFatalf("unable to retryRPCClient.NewClient(%v,%v): Volume: %s (Account: %s) err: %v", globals.retryRPCPublicIPAddr, globals.retryRPCPort, globals.config.FUSEVolumeName, accountName, err)
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
		swiftStorageURL string
		unmarshalErr    error
	)

	jrpcRequestID, jrpcRequest, marshalErr = jrpcMarshalRequest(jrpcMethod, jrpcParam)
	if nil != marshalErr {
		logFatalf("unable to marshal request (jrpcMethod=%s jrpcParam=%v): %#v", jrpcMethod, jrpcParam, marshalErr)
	}

	swiftStorageURL = fetchStorageURL()

	httpRequest, httpErr = http.NewRequest("PROXYFS", swiftStorageURL, bytes.NewReader(jrpcRequest))
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
		swiftAuthToken = fetchAuthToken()

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
			logInfof("doHTTPRequest(%s %s) needs to call updateAuthTokenAndStorageURL()", request.Method, request.URL.String())
			updateAuthTokenAndStorageURL()
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

func fetchAuthToken() (swiftAuthToken string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		// Make a copy of globals.swiftAuthWaitGroup (if any) thus
		// avoiding a race where, after the active instance of
		// updateAuthTokenAndStorageURL() signals completion, it
		// will erase it from globals (indicating no auth is in
		// progress anymore)

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftAuthToken = globals.swiftAuthToken
			globals.Unlock()
			return
		}

		globals.Unlock()

		swiftAuthWaitGroup.Wait()
	}
}

func fetchStorageURL() (swiftStorageURL string) {
	var (
		swiftAuthWaitGroup *sync.WaitGroup
	)

	for {
		globals.Lock()

		// Make a copy of globals.swiftAuthWaitGroup (if any) thus
		// avoiding a race where, after the active instance of
		// updateAuthTokenAndStorageURL() signals completion, it
		// will erase it from globals (indicating no auth is in
		// progress anymore)

		swiftAuthWaitGroup = globals.swiftAuthWaitGroup

		if nil == swiftAuthWaitGroup {
			swiftStorageURL = globals.swiftStorageURL
			globals.Unlock()
			return
		}

		globals.Unlock()

		swiftAuthWaitGroup.Wait()
	}
}

func updateAuthTokenAndStorageURL() {
	var (
		authOut            authOutStruct
		err                error
		stderrChanBuf      []byte
		stderrChanBufChunk []byte
		stdoutChanBuf      []byte
		stdoutChanBufChunk []byte
		swiftAuthWaitGroup *sync.WaitGroup
	)

	// First check and see if another instance is already in-flight

	globals.Lock()

	// Make a copy of globals.swiftAuthWaitGroup (if any) thus
	// avoiding a race where, after the active instance signals
	// completion, it will erase it from globals (indicating no
	// auth is in progress anymore)

	swiftAuthWaitGroup = globals.swiftAuthWaitGroup

	if nil != swiftAuthWaitGroup {
		// Another instance is already in flight... just await its completion

		globals.Unlock()

		swiftAuthWaitGroup.Wait()

		return
	}

	// We will be doing active instance performing the auth,
	// so create a sync.WaitGroup for other instances and fetches to await

	globals.swiftAuthWaitGroup = &sync.WaitGroup{}
	globals.swiftAuthWaitGroup.Add(1)

	globals.Unlock()

	if nil != globals.authPlugInControl {
		// There seems to be an active authPlugIn... drain any bytes sent to stdoutChan first

		for {
			select {
			case _ = <-globals.authPlugInControl.stdoutChan:
			default:
				goto EscapeStdoutChanDrain
			}
		}

	EscapeStdoutChanDrain:

		// See if there is anything in stderrChan

		stderrChanBuf = make([]byte, 0, authPipeReadBufSize)

		for {
			select {
			case stderrChanBufChunk = <-globals.authPlugInControl.stderrChan:
				stderrChanBuf = append(stderrChanBuf, stderrChanBufChunk...)
			default:
				goto EscapeStderrChanRead1
			}
		}

	EscapeStderrChanRead1:

		if 0 < len(stderrChanBuf) {
			logWarnf("got unexpected authPlugInStderr data: %s", string(stderrChanBuf[:]))

			stopAuthPlugIn()
		} else {
			// No errors... so lets try sending a byte to authPlugIn to request a fresh authorization

			_, err = globals.authPlugInControl.stdinPipe.Write([]byte{0})
			if nil != err {
				logWarnf("got unexpected error sending SIGHUP to authPlugIn: %v", err)

				stopAuthPlugIn()
			}
		}
	}

	if nil == globals.authPlugInControl {
		// Either authPlugIn wasn't (thought to be) running, or it failed above... so (re)start it

		startAuthPlugIn()
	}

	// Now read authPlugInStdout for a valid JSON-marshalled authOutStruct

	stdoutChanBuf = make([]byte, 0, authPipeReadBufSize)

	for {
		select {
		case stdoutChanBufChunk = <-globals.authPlugInControl.stdoutChan:
			stdoutChanBuf = append(stdoutChanBuf, stdoutChanBufChunk...)

			// Perhaps we've received the entire authOutStruct

			err = json.Unmarshal(stdoutChanBuf, &authOut)
			if nil == err {
				// Got a clean JSON-formatted authOutStruct

				goto EscapeFetchAuthOut
			}
		case stderrChanBuf = <-globals.authPlugInControl.stderrChan:
			// Uh oh... started receiving an error... drain it and "error out"

			for {
				select {
				case stderrChanBufChunk = <-globals.authPlugInControl.stderrChan:
					stderrChanBuf = append(stderrChanBuf, stderrChanBufChunk...)
				default:
					goto EscapeStderrChanRead2
				}
			}

		EscapeStderrChanRead2:

			logWarnf("got unexpected authPlugInStderr data: %s", string(stderrChanBuf[:]))

			authOut.AuthToken = ""
			authOut.StorageURL = ""

			goto EscapeFetchAuthOut
		}
	}

EscapeFetchAuthOut:

	globals.Lock()

	globals.swiftAuthToken = authOut.AuthToken
	globals.swiftStorageURL = authOut.StorageURL

	// Finally, indicate to waiters we are done and also enable
	// the next call to updateAuthTokenAndStorageURL() to perform
	// the auth again

	globals.swiftAuthWaitGroup.Done()
	globals.swiftAuthWaitGroup = nil

	globals.Unlock()
}

func authPlugInPipeReader(pipeToRead io.ReadCloser, chanToWrite chan []byte, wg *sync.WaitGroup) {
	var (
		buf []byte
		eof bool
		err error
		n   int
	)

	for {
		buf = make([]byte, authPipeReadBufSize)

		n, err = pipeToRead.Read(buf)
		switch err {
		case nil:
			eof = false
		case io.EOF:
			eof = true
		default:
			logFatalf("got unexpected error reading authPlugInPipe: %v", err)
		}

		if 0 < n {
			chanToWrite <- buf[:n]
		}

		if eof {
			wg.Done()
			return // Exits this goroutine
		}
	}
}

func startAuthPlugIn() {
	var (
		err error
	)

	globals.authPlugInControl = &authPlugInControlStruct{
		cmd:        exec.Command(globals.config.PlugInPath, globals.config.PlugInEnvName),
		stdoutChan: make(chan []byte),
		stderrChan: make(chan []byte),
	}

	globals.authPlugInControl.stdinPipe, err = globals.authPlugInControl.cmd.StdinPipe()
	if nil != err {
		logFatalf("got unexpected error creating authPlugIn stdinPipe: %v", err)
	}
	globals.authPlugInControl.stdoutPipe, err = globals.authPlugInControl.cmd.StdoutPipe()
	if nil != err {
		logFatalf("got unexpected error creating authPlugIn stdoutPipe: %v", err)
	}
	globals.authPlugInControl.stderrPipe, err = globals.authPlugInControl.cmd.StderrPipe()
	if nil != err {
		logFatalf("got unexpected error creating authPlugIn stderrPipe: %v", err)
	}

	globals.authPlugInControl.wg.Add(2)

	go authPlugInPipeReader(globals.authPlugInControl.stdoutPipe, globals.authPlugInControl.stdoutChan, &globals.authPlugInControl.wg)
	go authPlugInPipeReader(globals.authPlugInControl.stderrPipe, globals.authPlugInControl.stderrChan, &globals.authPlugInControl.wg)

	if "" != globals.config.PlugInEnvValue {
		globals.authPlugInControl.cmd.Env = append(os.Environ(), globals.config.PlugInEnvName+"="+globals.config.PlugInEnvValue)
	}

	err = globals.authPlugInControl.cmd.Start()
	if nil != err {
		logFatalf("got unexpected error starting authPlugIn: %v", err)
	}
}

func stopAuthPlugIn() {
	if nil == globals.authPlugInControl {
		// No authPlugIn running
		return
	}

	// Stop authPlugIn (ignore errors since they just indicate authPlugIn failed)

	_ = globals.authPlugInControl.stdinPipe.Close()

	_ = globals.authPlugInControl.cmd.Wait()

	// Drain stdoutChan & stderrChan

	for {
		select {
		case _ = <-globals.authPlugInControl.stdoutChan:
		default:
			goto EscapeStdoutChanDrain
		}
	}

EscapeStdoutChanDrain:

	for {
		select {
		case _ = <-globals.authPlugInControl.stderrChan:
		default:
			goto EscapeStderrChanDrain
		}
	}

EscapeStderrChanDrain:

	// Tell stdoutPipe and stderrPipe readers to go away (ignore errors)

	_ = globals.authPlugInControl.stdoutPipe.Close()
	_ = globals.authPlugInControl.stderrPipe.Close()

	// Wait for stdoutPipe and stderrPipe readers to exit

	globals.authPlugInControl.wg.Wait()

	// Finally, clean out authPlugInControl

	globals.authPlugInControl = nil
}
