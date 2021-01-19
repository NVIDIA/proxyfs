// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package bucketstats

import (
	"sync/atomic"
)

func atomicAddUint64(addr *uint64, val uint64) {
	atomic.AddUint64(addr, val)
}

func atomicIncUint64(addr *uint64) {
	atomic.AddUint64(addr, 1)
}

func atomicLoadUint64(addr *uint64) (val uint64) {
	val = atomic.LoadUint64(addr)
	return
}

func atomicStoreUint64(addr *uint64, val uint64) {
	atomic.StoreUint64(addr, val)
}
