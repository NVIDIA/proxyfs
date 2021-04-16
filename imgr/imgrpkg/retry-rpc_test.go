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
	// TODO: Fetch a Shared Lease

	testTeardown(t)
}
