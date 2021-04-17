// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"testing"
)

func TestRetryRPC(t *testing.T) {
	testSetup(t)

	// TODO: Attempt a FetchNonceRange() without a prior Mount()... which should fail (no Mount)
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

	testTeardown(t)
}
