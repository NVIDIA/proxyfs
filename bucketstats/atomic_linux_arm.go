// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Per bugs in the 32-bit versions of 64-bit sync/atomic API implementations,
// this file implements those operations by means of a global lock.

package bucketstats

import "sync"

var (
	atomicMutex sync.Mutex
)

func atomicAddUint64(addr *uint64, val uint64) {
	atomicMutex.Lock()
	prevVal := *addr
	*addr = prevVal + val
	atomicMutex.Unlock()
}

func atomicIncUint64(addr *uint64) {
	atomicMutex.Lock()
	prevVal := *addr
	*addr = prevVal + 1
	atomicMutex.Unlock()
}

func atomicLoadUint64(addr *uint64) (val uint64) {
	atomicMutex.Lock()
	val = *addr
	atomicMutex.Unlock()
	return
}

func atomicStoreUint64(addr *uint64, val uint64) {
	atomicMutex.Lock()
	*addr = val
	atomicMutex.Unlock()
}
