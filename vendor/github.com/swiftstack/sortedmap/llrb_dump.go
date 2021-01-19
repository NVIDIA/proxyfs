// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package sortedmap

import (
	"fmt"
	"strings"
)

func (tree *llrbTreeStruct) Dump() (err error) {
	tree.Lock()
	defer tree.Unlock()

	err = nil

	err = tree.dumpInFlatForm(tree.root)
	if nil != err {
		err = fmt.Errorf("dumpInFlatForm() failed: %v\n", err)
		fmt.Printf("\n%v\n", err)
		return
	}

	err = tree.dumpInTreeForm()
	if nil != err {
		err = fmt.Errorf("dumpInTreeForm() failed: %v\n", err)
		fmt.Printf("\n%v\n", err)
		return
	}

	err = nil
	return
}

func (tree *llrbTreeStruct) dumpInFlatForm(node *llrbNodeStruct) (err error) {
	if nil == node {
		err = nil
		return
	}

	nodeLeftKey := "nil"
	if nil != node.left {
		nodeLeftKey, err = tree.DumpKey(node.left.Key)
		if nil != err {
			return
		}
	}

	nodeRightKey := "nil"
	if nil != node.right {
		nodeRightKey, err = tree.DumpKey(node.right.Key)
		if nil != err {
			return
		}
	}

	var colorString string
	if RED == node.color {
		colorString = "RED"
	} else { // BLACK == node.color
		colorString = "BLACK"
	}

	nodeThisKey, err := tree.DumpKey(node.Key)
	if nil != err {
		return
	}

	fmt.Printf("%v Node Key == %v Node.left.Key == %v Node.right.Key == %v len == %v\n", colorString, nodeThisKey, nodeLeftKey, nodeRightKey, node.len)

	err = tree.dumpInFlatForm(node.left)
	if nil != err {
		return
	}

	err = tree.dumpInFlatForm(node.right)
	if nil != err {
		return
	}

	err = nil
	return
}

func (tree *llrbTreeStruct) dumpInTreeForm() (err error) {
	if nil == tree.root {
		err = nil
		return
	}

	if nil != tree.root.right {
		err = tree.dumpInTreeFormNode(tree.root.right, true, "")
		if nil != err {
			return
		}
	}

	rootKey, err := tree.DumpKey(tree.root.Key)
	if nil != err {
		return
	}

	fmt.Printf("%v\n", rootKey)

	if nil != tree.root.left {
		err = tree.dumpInTreeFormNode(tree.root.left, false, "")
		if nil != err {
			return
		}
	}

	err = nil
	return
}

func (tree *llrbTreeStruct) dumpInTreeFormNode(node *llrbNodeStruct, isRight bool, indent string) (err error) {
	var indentAppendage string
	var nextIndent string

	if nil != node.right {
		if isRight {
			indentAppendage = "        "
		} else {
			indentAppendage = " |      "
		}
		nextIndent = strings.Join([]string{indent, indentAppendage}, "")
		err = tree.dumpInTreeFormNode(node.right, true, nextIndent)
		if nil != err {
			return
		}
	}

	fmt.Printf("%v", indent)
	if isRight {
		fmt.Printf(" /")
	} else {
		fmt.Printf(" \\")
	}

	nodeKey, err := tree.DumpKey(node.Key)
	if nil != err {
		return
	}

	fmt.Printf("----- %v\n", nodeKey)

	if nil != node.left {
		if isRight {
			indentAppendage = " |      "
		} else {
			indentAppendage = "        "
		}
		nextIndent = strings.Join([]string{indent, indentAppendage}, "")
		err = tree.dumpInTreeFormNode(node.left, false, nextIndent)
		if nil != err {
			return
		}
	}

	err = nil
	return
}
