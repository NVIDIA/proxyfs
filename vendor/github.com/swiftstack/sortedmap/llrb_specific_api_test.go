// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package sortedmap

import (
	"testing"
)

func TestLLRBTreeReset(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	context.tree.Reset()
}
