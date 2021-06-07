// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"fmt"
	"strings"
	"testing"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/iswift/iswiftpkg"
	"github.com/NVIDIA/proxyfs/retryrpc"
)

type testRetryRPCClientCallbacksStruct struct {
	interruptPayloadChan chan []byte
}

func (retryrpcClientCallbacks *testRetryRPCClientCallbacksStruct) Interrupt(payload []byte) {
	retryrpcClientCallbacks.interruptPayloadChan <- payload
}

func TestRetryRPC(t *testing.T) {
	var (
		adjustInodeTableEntryOpenCountRequest  *AdjustInodeTableEntryOpenCountRequestStruct
		adjustInodeTableEntryOpenCountResponse *AdjustInodeTableEntryOpenCountResponseStruct
		deleteInodeTableEntryRequest           *DeleteInodeTableEntryRequestStruct
		deleteInodeTableEntryResponse          *DeleteInodeTableEntryResponseStruct
		err                                    error
		fetchNonceRangeRequest                 *FetchNonceRangeRequestStruct
		fetchNonceRangeResponse                *FetchNonceRangeResponseStruct
		fileInodeNumber                        uint64
		fileInodeObjectA                       uint64
		fileInodeObjectB                       uint64
		fileInodeObjectC                       uint64
		flushRequest                           *FlushRequestStruct
		flushResponse                          *FlushResponseStruct
		getInodeTableEntryRequest              *GetInodeTableEntryRequestStruct
		getInodeTableEntryResponse             *GetInodeTableEntryResponseStruct
		leaseRequest                           *LeaseRequestStruct
		leaseResponse                          *LeaseResponseStruct
		mountRequest                           *MountRequestStruct
		mountResponse                          *MountResponseStruct
		postRequestBody                        string
		putInodeTableEntriesRequest            *PutInodeTableEntriesRequestStruct
		putInodeTableEntriesResponse           *PutInodeTableEntriesResponseStruct
		putRequestBody                         string
		renewMountRequest                      *RenewMountRequestStruct
		renewMountResponse                     *RenewMountResponseStruct
		retryrpcClient                         *retryrpc.Client
		retryrpcClientCallbacks                *testRetryRPCClientCallbacksStruct
		unmountRequest                         *UnmountRequestStruct
		unmountResponse                        *UnmountResponseStruct
	)

	// Setup RetryRPC Client

	retryrpcClientCallbacks = &testRetryRPCClientCallbacksStruct{
		interruptPayloadChan: make(chan []byte),
	}

	// Setup test environment

	testSetup(t, retryrpcClientCallbacks)

	retryrpcClient, err = retryrpc.NewClient(testGlobals.retryrpcClientConfig)
	if nil != err {
		t.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

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

	// Attempt a FetchNonceRange() without a prior Mount()... which should fail (no Mount)

	fetchNonceRangeRequest = &FetchNonceRangeRequestStruct{
		MountID: "", // An invalid MountID... so will not be found in globals.mountMap
	}
	fetchNonceRangeResponse = &FetchNonceRangeResponseStruct{}

	err = retryrpcClient.Send("FetchNonceRange", fetchNonceRangeRequest, fetchNonceRangeResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"FetchNonceRange()\",,) should have failed")
	}

	// Perform a Mount()

	mountRequest = &MountRequestStruct{
		VolumeName: testVolume,
		AuthToken:  testGlobals.authToken,
	}
	mountResponse = &MountResponseStruct{}

	err = retryrpcClient.Send("Mount", mountRequest, mountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Mount(,)\",,) failed: %v", err)
	}

	// Perform a FetchNonceRange()

	fetchNonceRangeRequest = &FetchNonceRangeRequestStruct{
		MountID: mountResponse.MountID,
	}
	fetchNonceRangeResponse = &FetchNonceRangeResponseStruct{}

	err = retryrpcClient.Send("FetchNonceRange", fetchNonceRangeRequest, fetchNonceRangeResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"FetchNonceRange()\",,) failed: %v", err)
	}

	// Attempt a GetInodeTableEntry() for RootDirInode... which should fail (no Lease)

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) should have failed")
	}

	// Force a need for a re-auth

	iswiftpkg.ForceReAuth()

	// Attempt a GetInodeTableEntry() for RootDirInode... which should fail (re-auth required)

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) should have failed")
	}

	// Perform a RenewMount()

	err = testDoAuth()
	if nil != err {
		t.Fatalf("testDoAuth() failed: %v", err)
	}

	renewMountRequest = &RenewMountRequestStruct{
		MountID:   mountResponse.MountID,
		AuthToken: testGlobals.authToken,
	}
	renewMountResponse = &RenewMountResponseStruct{}

	err = retryrpcClient.Send("RenewMount", renewMountRequest, renewMountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"RenewMount(,)\",,) failed: %v", err)
	}

	// Fetch a Shared Lease on RootDirInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      ilayout.RootDirInodeNumber,
		LeaseRequestType: LeaseRequestTypeShared,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,ilayout.RootDirInodeNumber,LeaseRequestTypeShared)\",,) failed: %v", err)
	}

	// Perform a GetInodeTableEntry() for RootDirInode

	getInodeTableEntryRequest = &GetInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: ilayout.RootDirInodeNumber,
	}
	getInodeTableEntryResponse = &GetInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("GetInodeTableEntry", getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"GetInodeTableEntry(,ilayout.RootDirInodeNumber)\",,) failed: %v", err)
	}

	// Attempt a PutInodeTableEntries() for RootDirInode... which should fail (only Shared Lease)

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           ilayout.RootDirInodeNumber,
				InodeHeadObjectNumber: getInodeTableEntryResponse.InodeHeadObjectNumber,
				InodeHeadLength:       getInodeTableEntryResponse.InodeHeadLength,
			},
		},
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) should have failed")
	}

	// Perform a Lease Promote on RootDirInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      ilayout.RootDirInodeNumber,
		LeaseRequestType: LeaseRequestTypePromote,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,ilayout.RootDirInodeNumber,LeaseRequestTypePromote)\",,) failed: %v", err)
	}

	// Perform a PutInodeTableEntries() on RootDirInode

	putInodeTableEntriesRequest = &PutInodeTableEntriesRequestStruct{
		MountID: mountResponse.MountID,
		UpdatedInodeTableEntryArray: []PutInodeTableEntryStruct{
			{
				InodeNumber:           ilayout.RootDirInodeNumber,
				InodeHeadObjectNumber: getInodeTableEntryResponse.InodeHeadObjectNumber,
				InodeHeadLength:       getInodeTableEntryResponse.InodeHeadLength,
			},
		},
	}
	putInodeTableEntriesResponse = &PutInodeTableEntriesResponseStruct{}

	err = retryrpcClient.Send("PutInodeTableEntries", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"PutInodeTableEntries(,{ilayout.RootDirInodeNumber,,})\",,) failed: %v", err)
	}

	// TODO: Remove this early exit skipping of following TODOs

	if nil == err {
		t.Logf("Exiting TestRetryRPC() early to skip following TODOs")
		return
	}

	// TODO: Create a FileInode (set LinkCount to 0 & no dir entry)... with small amount of data

	if 2 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeNumber = fetchNonceRangeResponse.NextNonce
	fileInodeObjectA = fileInodeNumber + 1

	fetchNonceRangeResponse.NextNonce += 2
	fetchNonceRangeResponse.NumNoncesFetched -= 2

	t.Logf(" fileInodeNumber: %016X", fileInodeNumber)
	t.Logf("fileInodeObjectA: %016X", fileInodeObjectA)

	// Fetch an Exclusive Lease on FileInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      fileInodeNumber,
		LeaseRequestType: LeaseRequestTypeExclusive,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,fileInodeNumber,LeaseRequestTypeExclusive)\",,) failed: %v", err)
	}

	// TODO: Perform a PutInodeTableEntries() for FileInode

	// TODO: Append some data (to new Object) for FileInode... and new stat

	if 1 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeObjectB = fileInodeObjectA + 1

	fetchNonceRangeResponse.NextNonce++
	fetchNonceRangeResponse.NumNoncesFetched--

	t.Logf("fileInodeObjectB: %016X", fileInodeObjectB)

	// TODO: Perform a PutInodeTableEntries() for FileInode

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// TODO: Overwrite the original data in FileInode (to new Object)... and new stat dereferencing 1st Object

	if 1 > fetchNonceRangeResponse.NumNoncesFetched {
		t.Fatalf("fetchNonceRangeResponse contained insufficient NumNoncesFetched")
	}

	fileInodeObjectC = fileInodeObjectB + 1

	fetchNonceRangeResponse.NextNonce++
	fetchNonceRangeResponse.NumNoncesFetched--

	t.Logf("fileInodeObjectC: %016X", fileInodeObjectC)

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// TODO: Verify that 1st Object for FileInode gets deleted... but not 2nd nor 3rd

	// Perform an AdjustInodeTableEntryOpenCount(+1) for FileInode

	adjustInodeTableEntryOpenCountRequest = &AdjustInodeTableEntryOpenCountRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
		Adjustment:  +1,
	}
	adjustInodeTableEntryOpenCountResponse = &AdjustInodeTableEntryOpenCountResponseStruct{}

	err = retryrpcClient.Send("AdjustInodeTableEntryOpenCount", adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"AdjustInodeTableEntryOpenCount(,fileInodeNumber,+1)\",,) failed: %v", err)
	}

	// Perform a DeleteInodeTableEntry() on FileInode

	deleteInodeTableEntryRequest = &DeleteInodeTableEntryRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
	}
	deleteInodeTableEntryResponse = &DeleteInodeTableEntryResponseStruct{}

	err = retryrpcClient.Send("DeleteInodeTableEntry", deleteInodeTableEntryRequest, deleteInodeTableEntryResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"DeleteInodeTableEntry(,fileInodeNumber)\",,) failed: %v", err)
	}

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// TODO: Verify that FileInode is still in InodeTable

	// Perform an AdjustInodeTableEntryOpenCount(-1) for FileInode

	adjustInodeTableEntryOpenCountRequest = &AdjustInodeTableEntryOpenCountRequestStruct{
		MountID:     mountResponse.MountID,
		InodeNumber: fileInodeNumber,
		Adjustment:  -1,
	}
	adjustInodeTableEntryOpenCountResponse = &AdjustInodeTableEntryOpenCountResponseStruct{}

	err = retryrpcClient.Send("AdjustInodeTableEntryOpenCount", adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"AdjustInodeTableEntryOpenCount(,fileInodeNumber,-1)\",,) failed: %v", err)
	}

	// Perform a Flush()

	flushRequest = &FlushRequestStruct{
		MountID: mountResponse.MountID,
	}
	flushResponse = &FlushResponseStruct{}

	err = retryrpcClient.Send("Flush", flushRequest, flushResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Flush()\",,) failed: %v", err)
	}

	// TODO: Verify that FileInode is no longer in InodeTable and 2nd and 3rd Objects are deleted

	// Perform a Lease Release on FileInode

	leaseRequest = &LeaseRequestStruct{
		MountID:          mountResponse.MountID,
		InodeNumber:      fileInodeNumber,
		LeaseRequestType: LeaseRequestTypeRelease,
	}
	leaseResponse = &LeaseResponseStruct{}

	err = retryrpcClient.Send("Lease", leaseRequest, leaseResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Lease(,fileInodeNumber,LeaseRequestTypeRelease)\",,) failed: %v", err)
	}

	// Perform an Unmount()... without first releasing Exclusive Lease on RootDirInode

	unmountRequest = &UnmountRequestStruct{
		MountID: mountResponse.MountID,
	}
	unmountResponse = &UnmountResponseStruct{}

	err = retryrpcClient.Send("Unmount", unmountRequest, unmountResponse)
	if nil != err {
		t.Fatalf("retryrpcClient.Send(\"Unmount()\",,) failed: %v", err)
	}

	// TODO: Verify that Exclusive Lease on RootDirInode is implicitly released

	// Teardown RetryRPC Client

	retryrpcClient.Close()

	// And teardown test environment

	testTeardown(t)
}
