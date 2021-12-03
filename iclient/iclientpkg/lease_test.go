// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"testing"
)

func TestLocks(t *testing.T) {
	var (
		err            error
		fissionErrChan chan error
	)

	testSetup(t)

	fissionErrChan = make(chan error, 1)

	err = initializeGlobals(testGlobals.confMap, fissionErrChan)
	if nil != err {
		t.Fatalf("initializeGlobals(testGlobals.confMap, fissionErrChan) failed: %v", err)
	}

	err = startRPCHandler()
	if nil != err {
		t.Fatalf("startRPCHandler() failed: %v", err)
	}

	t.Logf("TODO... actually test locks in func TestLocks()")

	err = stopRPCHandler()
	if nil != err {
		t.Fatalf("stopRPCHandler() failed: %v", err)
	}

	err = uninitializeGlobals()
	if nil != err {
		t.Fatalf("uninitializeGlobals() failed: %v", err)
	}

	testTeardown(t)
}
