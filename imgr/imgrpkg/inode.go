// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"github.com/NVIDIA/sortedmap"
)

func startInodeTableManagement() (err error) {
	globals.inodeTableCache = sortedmap.NewBPlusTreeCache(globals.config.InodeTableCacheEvictLowLimit, globals.config.InodeTableCacheEvictLowLimit)

	err = nil
	return
}

func stopInodeTableManagement() (err error) {
	globals.inodeTableCache = nil

	err = nil
	return
}
