// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package sortedmap

import "fmt"

func (tree *llrbTreeStruct) Validate() (err error) {
	tree.Lock()
	defer tree.Unlock()

	err = tree.root.validate()

	return
}

func (node *llrbNodeStruct) validate() (err error) {
	if nil == node {
		return nil
	}

	computedHeight := 1

	if nil != node.left {
		computedHeight += node.left.len

		err = node.left.validate()

		if nil != err {
			return
		}
	}

	if nil != node.right {
		computedHeight += node.right.len

		err = node.right.validate()

		if nil != err {
			return
		}
	}

	if computedHeight != node.len {
		err = fmt.Errorf("For node.Key == %v, computedHeight(%v) != node.len(%v)", node.Key, computedHeight, node.len)
		return
	}

	return nil
}
