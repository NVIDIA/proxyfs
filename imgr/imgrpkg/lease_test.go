// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/proxyfs/retryrpc"
)

const (
	testRpcLeaseDelayAfterSendingRequest         = 10 * time.Millisecond
	testRpcLeaseDelayBeforeSendingRequest        = 10 * time.Millisecond
	testRpcLeaseMultiFirstInodeNumber     uint64 = 1
	testRpcLeaseMultiNumInstances         uint64 = 5
	testRpcLeaseSingleInodeNumber         uint64 = 1
	testRpcLeaseSingleNumInstances        uint64 = 21 // 101 // Must be >= 4
	testRpcLeaseTimeFormat                       = "15:04:05.000"
)

var (
	testRpcLeaseRequestLetters   = [5]string{"S", "P", "E", "D", "R"}
	testRpcLeaseResponseLetters  = [6]string{"D", "S", "P", "E", "D", "R"}
	testRpcLeaseInterruptLetters = [3]string{"U", "D", "R"}
	testRpcLeaseLogVerbosely     bool
)

type testRpcLeaseClientStruct struct {
	instance         uint64
	inodeNumber      uint64
	chIn             chan LeaseRequestType // close it to terminate testRpcLeaseClient instance
	chOut            chan interface{}      // either a LeaseResponseType or an RPCInterruptType
	alreadyUnmounted bool                  // if true, no RpcUnmount will be issued
	wg               *sync.WaitGroup       // signaled when testRpcLeaseClient instance exits
	t                *testing.T
}

func TestRPCLease(t *testing.T) {
	var (
		err                error
		instance           uint64
		postRequestBody    string
		putRequestBody     string
		testRpcLeaseClient []*testRpcLeaseClientStruct
		wg                 sync.WaitGroup
	)

	// Setup test environment

	testSetup(t, nil)

	// Format testVolume

	postRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\",\"AuthToken\":\"%s\"}", testGlobals.containerURL, testGlobals.authToken)

	_, _, err = testDoHTTPRequest("POST", testGlobals.httpServerURL+"/volume", nil, strings.NewReader(postRequestBody))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"POST\", testGlobals.httpServerURL+\"/volume\", nil, strings.NewReader(postRequestBody)) failed: %v", err)
	}

	// Start serving testVolume

	putRequestBody = fmt.Sprintf("{\"StorageURL\":\"%s\"}", testGlobals.containerURL)

	_, _, err = testDoHTTPRequest("PUT", testGlobals.httpServerURL+"/volume/"+testVolume, nil, strings.NewReader(putRequestBody))
	if nil != err {
		t.Fatalf("testDoHTTPRequest(\"PUT\", testGlobals.httpServerURL+\"/volume\"+testVolume, nil, strings.NewReader(putRequestBody)) failed: %v", err)
	}

	// Setup Single Lease instances

	wg.Add(int(testRpcLeaseSingleNumInstances))

	testRpcLeaseClient = make([]*testRpcLeaseClientStruct, testRpcLeaseSingleNumInstances)

	for instance = 0; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance] = &testRpcLeaseClientStruct{
			instance:         instance,
			inodeNumber:      testRpcLeaseSingleInodeNumber,
			chIn:             make(chan LeaseRequestType),
			chOut:            make(chan interface{}),
			alreadyUnmounted: false,
			wg:               &wg,
			t:                t,
		}

		go testRpcLeaseClient[instance].instanceGoroutine()
	}

	// Perform Single Lease test cases

	testRpcLeaseLogTestCase("1 Shared", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("2 Shared", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("3 Shared", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then Demote", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeDemoted)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 1 Shared leading to Demotion", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeDemote)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeDemoted, RPCInterruptTypeDemote)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("1 Shared then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("2 Shared then Promotion leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypePromoted)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 2 Shared leading to Demotion", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeDemote)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeDemoted, RPCInterruptTypeDemote)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("2 Shared then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase("2 Exclusives leading to Release that Expires", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	// TODO: testRpcLeaseClient[0].alreadyUnmounted = true
	//
	//       The above expiration should ultimately trigger an implicit Unmount

	testRpcLeaseLogTestCase("2 Shared then 2 Promotions leading to Release", true)

	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[2].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeDenied, RPCInterruptTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypePromoted)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	testRpcLeaseLogTestCase(fmt.Sprintf("%v Shared", testRpcLeaseSingleNumInstances-1), false)

	for instance = 1; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeShared)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseResponseType(LeaseResponseTypeShared)
	}
	for instance = 1; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeRelease)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	}

	testRpcLeaseLogTestCase(fmt.Sprintf("%v Exclusives", testRpcLeaseSingleNumInstances-1), false)

	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	for instance = 2; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeExclusive)
		testRpcLeaseClient[(instance - 1)].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
		testRpcLeaseClient[(instance - 1)].sendLeaseRequest(LeaseRequestTypeRelease)
		testRpcLeaseClient[(instance-1)].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	}
	testRpcLeaseClient[(testRpcLeaseSingleNumInstances - 1)].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[(testRpcLeaseSingleNumInstances - 1)].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	// Shutdown Single Lease instances

	for instance = 0; instance < testRpcLeaseSingleNumInstances; instance++ {
		close(testRpcLeaseClient[instance].chIn)
	}

	wg.Wait()

	// Setup Multi Lease instances

	wg.Add(int(testRpcLeaseMultiNumInstances))

	testRpcLeaseClient = make([]*testRpcLeaseClientStruct, testRpcLeaseMultiNumInstances)

	for instance = 0; instance < testRpcLeaseMultiNumInstances; instance++ {
		testRpcLeaseClient[instance] = &testRpcLeaseClientStruct{
			instance:         instance,
			inodeNumber:      testRpcLeaseMultiFirstInodeNumber + instance,
			chIn:             make(chan LeaseRequestType),
			chOut:            make(chan interface{}),
			alreadyUnmounted: false,
			wg:               &wg,
			t:                t,
		}

		go testRpcLeaseClient[instance].instanceGoroutine()
	}

	// Perform Multi Lease test case

	testRpcLeaseLogTestCase(fmt.Sprintf("%v Unique InodeNumber Exclusives", testRpcLeaseMultiNumInstances), true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeExclusive)

	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)
	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)

	testRpcLeaseClient[3].sendLeaseRequest(LeaseRequestTypeExclusive)

	// TODO testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	// TODO testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	//
	//      Above must be re-enabled once Lease Limits are enforced

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)

	testRpcLeaseClient[0].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(LeaseResponseTypeReleased, RPCInterruptTypeRelease)

	testRpcLeaseClient[3].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)

	testRpcLeaseClient[4].sendLeaseRequest(LeaseRequestTypeExclusive)

	testRpcLeaseClient[4].validateChOutValueIsLeaseResponseType(LeaseResponseTypeExclusive)

	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[3].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[4].sendLeaseRequest(LeaseRequestTypeRelease)

	testRpcLeaseClient[2].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	testRpcLeaseClient[3].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)
	testRpcLeaseClient[4].validateChOutValueIsLeaseResponseType(LeaseResponseTypeReleased)

	// Shutdown Multi Lease instances

	for instance = 0; instance < testRpcLeaseMultiNumInstances; instance++ {
		close(testRpcLeaseClient[instance].chIn)
	}

	wg.Wait()

	// Teardown test environment

	testTeardown(t)
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) instanceGoroutine() {
	var (
		err                  error
		leaseRequest         *LeaseRequestStruct
		leaseRequestType     LeaseRequestType
		leaseResponse        *LeaseResponseStruct
		mountRequest         *MountRequestStruct
		mountResponse        *MountResponseStruct
		ok                   bool
		retryrpcClient       *retryrpc.Client
		retryrpcClientConfig *retryrpc.ClientConfig
		unmountRequest       *UnmountRequestStruct
		unmountResponse      *UnmountResponseStruct
	)

	retryrpcClientConfig = &retryrpc.ClientConfig{}
	*retryrpcClientConfig = *testGlobals.retryrpcClientConfig
	retryrpcClientConfig.Callbacks = testRpcLeaseClient

	retryrpcClient, err = retryrpc.NewClient(retryrpcClientConfig)
	if nil != err {
		testRpcLeaseClient.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

	mountRequest = &MountRequestStruct{
		VolumeName: testVolume,
		AuthToken:  testGlobals.authToken,
	}
	mountResponse = &MountResponseStruct{}

	err = retryrpcClient.Send("Mount", mountRequest, mountResponse)
	if nil != err {
		testRpcLeaseClient.Fatalf("retryrpcClient.Send(\"Mount\",,) failed: %v", err)
	}

	for {
		leaseRequestType, ok = <-testRpcLeaseClient.chIn

		if ok {
			leaseRequest = &LeaseRequestStruct{
				MountID:          mountResponse.MountID,
				InodeNumber:      testRpcLeaseClient.inodeNumber,
				LeaseRequestType: leaseRequestType,
			}
			leaseResponse = &LeaseResponseStruct{}

			testRpcLeaseClient.logEvent(leaseRequest.LeaseRequestType)

			err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
			if nil != err {
				testRpcLeaseClient.Fatalf("retryrpcClient.Send(\"Lease\",LeaseRequestType=%d) failed: %v", leaseRequestType, err)
			}

			testRpcLeaseClient.logEvent(leaseResponse.LeaseResponseType)

			testRpcLeaseClient.chOut <- leaseResponse.LeaseResponseType
		} else {
			unmountRequest = &UnmountRequestStruct{
				MountID: mountResponse.MountID,
			}
			unmountResponse = &UnmountResponseStruct{}

			err = retryrpcClient.Send("Unmount", unmountRequest, unmountResponse)
			if testRpcLeaseClient.alreadyUnmounted {
				if nil == err {
					testRpcLeaseClient.Fatalf("retryrpcClient.Send(\"Unmount\",,) should have failed")
				}
			} else {
				if nil != err {
					if !strings.HasPrefix(err.Error(), ETODO) {
						testRpcLeaseClient.Fatalf("retryrpcClient.Send(\"Unmount\",,) failed: %v", err)
					}
				}
			}

			retryrpcClient.Close()

			testRpcLeaseClient.wg.Done()

			runtime.Goexit()
		}
	}
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) Interrupt(rpcInterruptBuf []byte) {
	var (
		err          error
		rpcInterrupt *RPCInterrupt
	)

	rpcInterrupt = &RPCInterrupt{}

	err = json.Unmarshal(rpcInterruptBuf, rpcInterrupt)
	if nil != err {
		testRpcLeaseClient.Fatalf("json.Unmarshal() failed: %v", err)
	}
	if rpcInterrupt.InodeNumber != testRpcLeaseClient.inodeNumber {
		testRpcLeaseClient.Fatalf("Interrupt() called for InodeNumber %v... expected to be for %v", rpcInterrupt.InodeNumber, testRpcLeaseClient.inodeNumber)
	}

	testRpcLeaseClient.logEvent(rpcInterrupt.RPCInterruptType)

	testRpcLeaseClient.chOut <- rpcInterrupt.RPCInterruptType
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) Fatalf(format string, args ...interface{}) {
	var (
		argsForPrintf   []interface{}
		argsIndex       int
		argsValue       interface{}
		formatForPrintf string
	)

	formatForPrintf = "Failing testRpcLeaseClient %v: " + format + "\n"

	argsForPrintf = make([]interface{}, len(args)+1)
	argsForPrintf[0] = testRpcLeaseClient.instance
	for argsIndex, argsValue = range args {
		argsForPrintf[argsIndex+1] = argsValue
	}

	fmt.Printf(formatForPrintf, argsForPrintf...)

	os.Exit(-1)
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) sendLeaseRequest(leaseRequestType LeaseRequestType) {
	time.Sleep(testRpcLeaseDelayBeforeSendingRequest)
	testRpcLeaseClient.chIn <- leaseRequestType
	time.Sleep(testRpcLeaseDelayAfterSendingRequest)
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) sendLeaseRequestPromptly(leaseRequestType LeaseRequestType) {
	testRpcLeaseClient.chIn <- leaseRequestType
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) validateChOutValueIsLeaseResponseType(expectedLeaseResponseType LeaseResponseType) {
	var (
		chOutValueAsInterface         interface{}
		chOutValueAsLeaseResponseType LeaseResponseType
		ok                            bool
	)

	chOutValueAsInterface = <-testRpcLeaseClient.chOut

	chOutValueAsLeaseResponseType, ok = chOutValueAsInterface.(LeaseResponseType)
	if !ok {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return a LeaseResponseType")
	}
	if chOutValueAsLeaseResponseType != expectedLeaseResponseType {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut returned LeaseResponseType %v... expected %v", chOutValueAsLeaseResponseType, expectedLeaseResponseType)
	}
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) validateChOutValueIsLeaseResponseTypeIgnoringRPCInterruptType(expectedLeaseResponseType LeaseResponseType, ignoredRPCInterruptType RPCInterruptType) {
	var (
		chOutValueAsInterface         interface{}
		chOutValueAsRPCInterruptType  RPCInterruptType
		chOutValueAsLeaseResponseType LeaseResponseType
		ok                            bool
	)

	for {
		chOutValueAsInterface = <-testRpcLeaseClient.chOut

		chOutValueAsRPCInterruptType, ok = chOutValueAsInterface.(RPCInterruptType)
		if ok {
			if chOutValueAsRPCInterruptType != ignoredRPCInterruptType {
				testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return an ignored RPCInterruptType")
			}
		} else {
			break
		}
	}

	chOutValueAsLeaseResponseType, ok = chOutValueAsInterface.(LeaseResponseType)
	if !ok {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return a LeaseResponseType or ignored RPCInterruptType")
	}
	if chOutValueAsLeaseResponseType != expectedLeaseResponseType {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut returned LeaseResponseType %v... expected %v", chOutValueAsLeaseResponseType, expectedLeaseResponseType)
	}
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) validateChOutValueIsRPCInterruptType(expectedRPCInterruptType RPCInterruptType) {
	var (
		chOutValueAsInterface        interface{}
		chOutValueAsRPCInterruptType RPCInterruptType
		ok                           bool
	)

	chOutValueAsInterface = <-testRpcLeaseClient.chOut

	chOutValueAsRPCInterruptType, ok = chOutValueAsInterface.(RPCInterruptType)
	if !ok {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return a RPCInterruptType")
	}
	if chOutValueAsRPCInterruptType != expectedRPCInterruptType {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut returned RPCInterruptType %v... expected %v", chOutValueAsRPCInterruptType, expectedRPCInterruptType)
	}
}

func testRpcLeaseLogTestCase(testCase string, verbose bool) {
	fmt.Printf("%v %s\n", time.Now().Format(testRpcLeaseTimeFormat), testCase)
	testRpcLeaseLogVerbosely = verbose
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) logEvent(ev interface{}) {
	if testRpcLeaseLogVerbosely {
		switch ev.(type) {
		case LeaseRequestType:
			fmt.Printf("%v      %s%s-> \n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", int(testRpcLeaseClient.instance)), testRpcLeaseRequestLetters[ev.(LeaseRequestType)])
		case LeaseResponseType:
			fmt.Printf("%v      %s <-%s\n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", int(testRpcLeaseClient.instance)), testRpcLeaseResponseLetters[ev.(LeaseResponseType)])
		case RPCInterruptType:
			fmt.Printf("%v      %s ^^%s\n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", int(testRpcLeaseClient.instance)), testRpcLeaseInterruptLetters[ev.(RPCInterruptType)])
		}
	}
}
