// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

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
	testRpcLeaseDelayAfterSendingRequest        = 10 * time.Millisecond
	testRpcLeaseDelayBeforeSendingRequest       = 10 * time.Millisecond
	testRpcLeaseRetryRPCDeadlineIO              = "60s"
	testRpcLeaseRetryRPCKeepAlivePeriod         = "60s"
	testRpcLeaseMultiFirstInodeNumber     int64 = 1
	testRpcLeaseMultiNumInstances         int   = 5
	testRpcLeaseSingleInodeNumber         int64 = 1
	testRpcLeaseSingleNumInstances        int   = 101 // Must be >= 4
	testRpcLeaseTimeFormat                      = "15:04:05.000"
)

var (
	testRpcLeaseRequestLetters   = [5]string{"S", "P", "E", "D", "R"}
	testRpcLeaseReplyLetters     = [6]string{"D", "S", "P", "E", "D", "R"}
	testRpcLeaseInterruptLetters = [3]string{"U", "D", "R"}
	testRpcLeaseLogVerbosely     bool
)

type testRpcLeaseClientStruct struct {
	instance         int
	inodeNumber      int64
	chIn             chan LeaseRequestType // close it to terminate testRpcLeaseClient instance
	chOut            chan interface{}      // either a LeaseReplyType or an RPCInterruptType
	alreadyUnmounted bool                  // if true, no RpcUnmount will be issued
	wg               *sync.WaitGroup       // signaled when testRpcLeaseClient instance exits
	t                *testing.T
}

func BenchmarkRpcLease(b *testing.B) {
	var (
		benchmarkIteration        int
		deadlineIO                time.Duration
		err                       error
		keepAlivePeriod           time.Duration
		leaseReply                *LeaseReply
		leaseRequest              *LeaseRequest
		mountByAccountNameRequest *MountByAccountNameRequest
		mountByAccountNameReply   *MountByAccountNameReply
		retryRPCClient            *retryrpc.Client
		retryrpcClientConfig      *retryrpc.ClientConfig
		testRpcLeaseClient        *testRpcLeaseClientStruct
		unmountReply              *Reply
		unmountRequest            *UnmountRequest
	)

	deadlineIO, err = time.ParseDuration(testRpcLeaseRetryRPCDeadlineIO)
	if nil != err {
		b.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRpcLeaseRetryRPCDeadlineIO, err)
	}
	keepAlivePeriod, err = time.ParseDuration(testRpcLeaseRetryRPCKeepAlivePeriod)
	if nil != err {
		b.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRpcLeaseRetryRPCKeepAlivePeriod, err)
	}

	retryrpcClientConfig = &retryrpc.ClientConfig{
		IPAddr:                   globals.publicIPAddr,
		Port:                     int(globals.retryRPCPort),
		RootCAx509CertificatePEM: testTLSCerts.caCertPEMBlock,
		Callbacks:                testRpcLeaseClient,
		DeadlineIO:               deadlineIO,
		KeepAlivePeriod:          keepAlivePeriod,
	}

	retryRPCClient, err = retryrpc.NewClient(retryrpcClientConfig)
	if nil != err {
		b.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

	mountByAccountNameRequest = &MountByAccountNameRequest{
		AccountName: testAccountName,
		AuthToken:   "",
	}
	mountByAccountNameReply = &MountByAccountNameReply{}

	err = retryRPCClient.Send("RpcMountByAccountName", mountByAccountNameRequest, mountByAccountNameReply)
	if nil != err {
		b.Fatalf("retryRPCClient.Send(\"RpcMountByAccountName\",,) failed: %v", err)
	}

	b.ResetTimer()

	for benchmarkIteration = 0; benchmarkIteration < b.N; benchmarkIteration++ {
		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeExclusive,
		}
		leaseReply = &LeaseReply{}

		err = retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
		if nil != err {
			b.Fatalf("retryRPCClient.Send(\"RpcLease\",LeaseRequestTypeExclusive) failed: %v", err)
		}

		if LeaseReplyTypeExclusive != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseRequestTypeExclusive", leaseReply.LeaseReplyType)
		}

		leaseRequest = &LeaseRequest{
			InodeHandle: InodeHandle{
				MountID:     mountByAccountNameReply.MountID,
				InodeNumber: testRpcLeaseSingleInodeNumber,
			},
			LeaseRequestType: LeaseRequestTypeRelease,
		}
		leaseReply = &LeaseReply{}

		err = retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
		if nil != err {
			b.Fatalf("retryRPCClient.Send(\"RpcLease\",LeaseRequestTypeRelease) failed: %v", err)
		}

		if LeaseReplyTypeReleased != leaseReply.LeaseReplyType {
			b.Fatalf("RpcLease() returned LeaseReplyType %v... expected LeaseReplyTypeReleased", leaseReply.LeaseReplyType)
		}
	}

	b.StopTimer()

	unmountRequest = &UnmountRequest{
		MountID: mountByAccountNameReply.MountID,
	}
	unmountReply = &Reply{}

	err = retryRPCClient.Send("RpcUnmount", unmountRequest, unmountReply)
	if nil != err {
		b.Fatalf("retryRPCClient.Send(\"RpcUnmount\",,) failed: %v", err)
	}

	retryRPCClient.Close()
}

func TestRpcLease(t *testing.T) {
	var (
		instance           int
		testRpcLeaseClient []*testRpcLeaseClientStruct
		wg                 sync.WaitGroup
	)

	// Setup Single Lease instances

	wg.Add(testRpcLeaseSingleNumInstances)

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
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("2 Shared", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("3 Shared", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then Demote", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeDemoted)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 1 Shared leading to Demotion", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeDemote)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeDemoted, RPCInterruptTypeDemote)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Shared then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("2 Shared then Promotion leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypePromoted)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("1 Exclusive then 2 Shared leading to Demotion", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeDemote)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeDemote)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeDemoted, RPCInterruptTypeDemote)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("2 Shared then 1 Exclusive leading to Release", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase("2 Exclusives leading to Release that Expires", true)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseClient[0].alreadyUnmounted = true

	testRpcLeaseLogTestCase("2 Shared then 2 Promotions leading to Release", true)

	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeShared)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[2].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypePromote)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeDenied, RPCInterruptTypeRelease)
	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypePromoted)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	testRpcLeaseLogTestCase(fmt.Sprintf("%v Shared", testRpcLeaseSingleNumInstances-1), false)

	for instance = 1; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeShared)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseReplyType(LeaseReplyTypeShared)
	}
	for instance = 1; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeRelease)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	}

	testRpcLeaseLogTestCase(fmt.Sprintf("%v Exclusives", testRpcLeaseSingleNumInstances-1), false)

	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeExclusive)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	for instance = 2; instance < testRpcLeaseSingleNumInstances; instance++ {
		testRpcLeaseClient[instance].sendLeaseRequest(LeaseRequestTypeExclusive)
		testRpcLeaseClient[(instance - 1)].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
		testRpcLeaseClient[(instance - 1)].sendLeaseRequest(LeaseRequestTypeRelease)
		testRpcLeaseClient[(instance-1)].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
		testRpcLeaseClient[instance].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	}
	testRpcLeaseClient[(testRpcLeaseSingleNumInstances - 1)].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[(testRpcLeaseSingleNumInstances - 1)].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	// Shutdown Single Lease instances

	for instance = 0; instance < testRpcLeaseSingleNumInstances; instance++ {
		close(testRpcLeaseClient[instance].chIn)
	}

	wg.Wait()

	// Setup Multi Lease instances

	wg.Add(testRpcLeaseMultiNumInstances)

	testRpcLeaseClient = make([]*testRpcLeaseClientStruct, testRpcLeaseMultiNumInstances)

	for instance = 0; instance < testRpcLeaseMultiNumInstances; instance++ {
		testRpcLeaseClient[instance] = &testRpcLeaseClientStruct{
			instance:         instance,
			inodeNumber:      (testRpcLeaseMultiFirstInodeNumber + int64(instance)),
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

	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)
	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)

	testRpcLeaseClient[3].sendLeaseRequest(LeaseRequestTypeExclusive)

	testRpcLeaseClient[0].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsRPCInterruptType(RPCInterruptTypeRelease)

	testRpcLeaseClient[0].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[1].sendLeaseRequest(LeaseRequestTypeRelease)

	testRpcLeaseClient[0].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)
	testRpcLeaseClient[1].validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(LeaseReplyTypeReleased, RPCInterruptTypeRelease)

	testRpcLeaseClient[3].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)

	testRpcLeaseClient[4].sendLeaseRequest(LeaseRequestTypeExclusive)

	testRpcLeaseClient[4].validateChOutValueIsLeaseReplyType(LeaseReplyTypeExclusive)

	testRpcLeaseClient[2].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[3].sendLeaseRequest(LeaseRequestTypeRelease)
	testRpcLeaseClient[4].sendLeaseRequest(LeaseRequestTypeRelease)

	testRpcLeaseClient[2].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[3].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)
	testRpcLeaseClient[4].validateChOutValueIsLeaseReplyType(LeaseReplyTypeReleased)

	// Shutdown Multi Lease instances

	for instance = 0; instance < testRpcLeaseMultiNumInstances; instance++ {
		close(testRpcLeaseClient[instance].chIn)
	}

	wg.Wait()
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) instanceGoroutine() {
	var (
		deadlineIO                time.Duration
		err                       error
		keepAlivePeriod           time.Duration
		leaseReply                *LeaseReply
		leaseRequest              *LeaseRequest
		leaseRequestType          LeaseRequestType
		mountByAccountNameRequest *MountByAccountNameRequest
		mountByAccountNameReply   *MountByAccountNameReply
		ok                        bool
		retryRPCClient            *retryrpc.Client
		retryrpcClientConfig      *retryrpc.ClientConfig
		unmountReply              *Reply
		unmountRequest            *UnmountRequest
	)

	deadlineIO, err = time.ParseDuration(testRpcLeaseRetryRPCDeadlineIO)
	if nil != err {
		testRpcLeaseClient.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRpcLeaseRetryRPCDeadlineIO, err)
	}
	keepAlivePeriod, err = time.ParseDuration(testRpcLeaseRetryRPCKeepAlivePeriod)
	if nil != err {
		testRpcLeaseClient.Fatalf("time.ParseDuration(\"%s\") failed: %v", testRpcLeaseRetryRPCKeepAlivePeriod, err)
	}

	retryrpcClientConfig = &retryrpc.ClientConfig{
		IPAddr:                   globals.publicIPAddr,
		Port:                     int(globals.retryRPCPort),
		RootCAx509CertificatePEM: testTLSCerts.caCertPEMBlock,
		Callbacks:                testRpcLeaseClient,
		DeadlineIO:               deadlineIO,
		KeepAlivePeriod:          keepAlivePeriod,
	}

	retryRPCClient, err = retryrpc.NewClient(retryrpcClientConfig)
	if nil != err {
		testRpcLeaseClient.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

	mountByAccountNameRequest = &MountByAccountNameRequest{
		AccountName: testAccountName,
		AuthToken:   "",
	}
	mountByAccountNameReply = &MountByAccountNameReply{}

	err = retryRPCClient.Send("RpcMountByAccountName", mountByAccountNameRequest, mountByAccountNameReply)
	if nil != err {
		testRpcLeaseClient.Fatalf("retryRPCClient.Send(\"RpcMountByAccountName\",,) failed: %v", err)
	}

	for {
		leaseRequestType, ok = <-testRpcLeaseClient.chIn

		if ok {
			leaseRequest = &LeaseRequest{
				InodeHandle: InodeHandle{
					MountID:     mountByAccountNameReply.MountID,
					InodeNumber: testRpcLeaseClient.inodeNumber,
				},
				LeaseRequestType: leaseRequestType,
			}
			leaseReply = &LeaseReply{}

			testRpcLeaseClient.logEvent(leaseRequest.LeaseRequestType)

			err = retryRPCClient.Send("RpcLease", leaseRequest, leaseReply)
			if nil != err {
				testRpcLeaseClient.Fatalf("retryRPCClient.Send(\"RpcLease\",LeaseRequestType=%d) failed: %v", leaseRequestType, err)
			}

			testRpcLeaseClient.logEvent(leaseReply.LeaseReplyType)

			testRpcLeaseClient.chOut <- leaseReply.LeaseReplyType
		} else {
			unmountRequest = &UnmountRequest{
				MountID: mountByAccountNameReply.MountID,
			}
			unmountReply = &Reply{}

			err = retryRPCClient.Send("RpcUnmount", unmountRequest, unmountReply)
			if testRpcLeaseClient.alreadyUnmounted {
				if nil == err {
					testRpcLeaseClient.Fatalf("retryRPCClient.Send(\"RpcUnmount\",,) should have failed")
				}
			} else {
				if nil != err {
					testRpcLeaseClient.Fatalf("retryRPCClient.Send(\"RpcUnmount\",,) failed: %v", err)
				}
			}

			retryRPCClient.Close()

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

func (testRpcLeaseClient *testRpcLeaseClientStruct) validateChOutValueIsLeaseReplyType(expectedLeaseReplyType LeaseReplyType) {
	var (
		chOutValueAsInterface      interface{}
		chOutValueAsLeaseReplyType LeaseReplyType
		ok                         bool
	)

	chOutValueAsInterface = <-testRpcLeaseClient.chOut

	chOutValueAsLeaseReplyType, ok = chOutValueAsInterface.(LeaseReplyType)
	if !ok {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return a LeaseReplyType")
	}
	if chOutValueAsLeaseReplyType != expectedLeaseReplyType {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut returned LeaseReplyType %v... expected %v", chOutValueAsLeaseReplyType, expectedLeaseReplyType)
	}
}

func (testRpcLeaseClient *testRpcLeaseClientStruct) validateChOutValueIsLeaseReplyTypeIgnoringRPCInterruptType(expectedLeaseReplyType LeaseReplyType, ignoredRPCInterruptType RPCInterruptType) {
	var (
		chOutValueAsInterface        interface{}
		chOutValueAsRPCInterruptType RPCInterruptType
		chOutValueAsLeaseReplyType   LeaseReplyType
		ok                           bool
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

	chOutValueAsLeaseReplyType, ok = chOutValueAsInterface.(LeaseReplyType)
	if !ok {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut did not return a LeaseReplyType or ignored RPCInterruptType")
	}
	if chOutValueAsLeaseReplyType != expectedLeaseReplyType {
		testRpcLeaseClient.t.Fatalf("<-testRpcLeaseClient.chOut returned LeaseReplyType %v... expected %v", chOutValueAsLeaseReplyType, expectedLeaseReplyType)
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
			fmt.Printf("%v      %s%s-> \n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", testRpcLeaseClient.instance), testRpcLeaseRequestLetters[ev.(LeaseRequestType)])
		case LeaseReplyType:
			fmt.Printf("%v      %s <-%s\n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", testRpcLeaseClient.instance), testRpcLeaseReplyLetters[ev.(LeaseReplyType)])
		case RPCInterruptType:
			fmt.Printf("%v      %s ^^%s\n", time.Now().Format(testRpcLeaseTimeFormat), strings.Repeat("           ", testRpcLeaseClient.instance), testRpcLeaseInterruptLetters[ev.(RPCInterruptType)])
		}
	}
}
