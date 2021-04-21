// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"testing"

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
		err                     error
		fetchNonceRangeRequest  *FetchNonceRangeRequestStruct
		fetchNonceRangeResponse *FetchNonceRangeResponseStruct
		retryrpcClient          *retryrpc.Client
		retryrpcClientCallbacks *testRetryRPCClientCallbacksStruct
	)

	retryrpcClientCallbacks = &testRetryRPCClientCallbacksStruct{
		interruptPayloadChan: make(chan []byte),
	}

	testSetup(t, retryrpcClientCallbacks)

	retryrpcClient, err = retryrpc.NewClient(testGlobals.retryrpcClientConfig)
	if nil != err {
		t.Fatalf("retryrpc.NewClient() failed: %v", err)
	}

	// Attempt a FetchNonceRange() without a prior Mount()... which should fail (no Mount)

	fetchNonceRangeRequest = &FetchNonceRangeRequestStruct{
		MountID: "", // An invalid MountID... so will not be found in globals.mountMap
	}
	fetchNonceRangeResponse = &FetchNonceRangeResponseStruct{}

	err = retryrpcClient.Send("FetchNonceRange", fetchNonceRangeRequest, fetchNonceRangeResponse)
	if nil == err {
		t.Fatalf("retryrpcClient.Send(\"FetchNonceRange\",,) should have failed")
	}

	// TODO: Perform a Mount()
	// TODO: Attempt a GetInodeTableEntry() for RootDirInode... which should fail (no Lease)
	// TODO: Force a need for a re-auth
	// TODO: Attempt a GetInodeTableEntry() for RootDirInode... which should fail (re-auth required)
	// TODO: Perform a RenewMount()
	// TODO: Fetch a Shared Lease on RootDirInode
	// TODO: Perform a GetInodeTableEntry() for RootDirInode
	// TODO: Attempt a PutInodeTableEntries() for RootDirInode... which should fail (only Shared Lease)
	// TODO: Perform a Lease Promote on RootDirInode
	// TODO: Perform a PutInodeTableEntries() on RootDirInode
	// TODO: Create a FileInode (set LinkCount to 0 & no dir entry)... with small amount of data
	// TODO: Fetch an Exclusive Lease on FileInode
	// TODO: Perform a PutInodeTableEntries() for FileInode
	// TODO: Append some data (to new Object) for FileInode... and new stat
	// TODO: Perform a PutInodeTableEntries() for FileInode
	// TODO: Perform a Flush()
	// TODO: Overwrite the original data in FileInode (to new Object)... and new stat dereferencing 1st Object
	// TODO: Perform a Flush()
	// TODO: Verify that 1st Object for FileInode gets deleted... but not 2nd nor 3rd
	// TODO: Perform a AdjustInodeTableEntryOpenCount(+1) for FileInode
	// TODO: Perform a DeleteInodeTableEntry() on FileInode
	// TODO: Perform a Flush()
	// TODO: Verify that FileInode is still in InodeTable
	// TODO: Perform a AdjustInodeTableEntryOpenCount(-1) for FileInode
	// TODO: Perform a Flush()
	// TODO: Verify that FileInode is no longer in InodeTable and 2nd and 3rd Objects are deleted
	// TODO: Perform a Lease Release on FileInode
	// TODO: Perform a Unmount()... without first releasing Exclusive Lease on RootDirInode
	// TODO: Verify that Exclusive Lease on RootDirInode is implicitly released

	retryrpcClient.Close()

	testTeardown(t)
}
