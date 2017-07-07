package sortedmap

import "fmt"

func (tree *btreeTreeStruct) Dump() (err error) {
	tree.Lock()
	defer tree.Unlock()

	if nil == tree.clonedFromTree {
		fmt.Printf("B+Tree @ %p has Root Node @ %p with %v activeClones\n", tree, tree.root, tree.activeClones)
	} else {
		fmt.Printf("B+Tree @ %p has Root Node @ %p with %v activeClones (clone of B+Tree @ %p)\n", tree, tree.root, tree.activeClones, tree.clonedFromTree)
	}

	err = tree.dumpNode(tree.root, "")

	return
}

func (tree *btreeTreeStruct) dumpNode(node *btreeNodeStruct, indent string) (err error) {
	if !node.loaded {
		err = node.tree.loadNode(node)
		if nil != err {
			return
		}
	}

	fmt.Printf("%v  .objectNumber        = 0x%016x\n", indent, node.objectNumber)
	fmt.Printf("%v  .objectOffset        = 0x%016x\n", indent, node.objectOffset)
	fmt.Printf("%v  .objectLength        = 0x%016x\n", indent, node.objectLength)
	fmt.Printf("%v  .items               = %v\n", indent, node.items)
	fmt.Printf("%v  .loaded              = %v\n", indent, node.loaded)
	fmt.Printf("%v  .dirty               = %v\n", indent, node.dirty)
	fmt.Printf("%v  .root                = %v\n", indent, node.root)
	fmt.Printf("%v  .leaf                = %v\n", indent, node.leaf)

	if nil == node.clonedFromNode {
		fmt.Printf("%v  .clonedFromNode      = nil\n", indent)
	} else {
		fmt.Printf("%v  .clonedFromNode      = %p\n", indent, node.clonedFromNode)
	}

	if nil == node.parentNode {
		fmt.Printf("%v  .parentNode          = nil\n", indent)
	} else {
		fmt.Printf("%v  .parentNode          = %p\n", indent, node.parentNode)
	}

	if !node.leaf {
		if nil == node.rootPrefixSumChild {
			fmt.Printf("%v  .rootPrefixSumChild  = nil\n", indent)
		} else {
			fmt.Printf("%v  .rootPrefixSumChild  = %p\n", indent, node.rootPrefixSumChild)
		}
	}

	if !node.root {
		fmt.Printf("%v  .prefixSumItems      = %v\n", indent, node.prefixSumItems)
		fmt.Printf("%v  .prefixSumKVIndex    = %v\n", indent, node.prefixSumKVIndex)
		if nil == node.prefixSumParent {
			fmt.Printf("%v  .prefixSumParent     = nil\n", indent)
		} else {
			fmt.Printf("%v  .prefixSumParent     = %p\n", indent, node.prefixSumParent)
		}
		if nil == node.prefixSumLeftChild {
			fmt.Printf("%v  .prefixSumLeftChild  = nil\n", indent)
		} else {
			fmt.Printf("%v  .prefixSumLeftChild  = %p\n", indent, node.prefixSumLeftChild)
		}
		if nil == node.prefixSumRightChild {
			fmt.Printf("%v  .prefixSumRightChild = nil\n", indent)
		} else {
			fmt.Printf("%v  .prefixSumRightChild = %p\n", indent, node.prefixSumRightChild)
		}
	}

	if !node.leaf {
		if nil != node.nonLeafLeftChild {
			fmt.Printf("%v  .nonLeafLeftChild    = %p\n", indent, node.nonLeafLeftChild)
			tree.dumpNode(node.nonLeafLeftChild, "    "+indent)
		}
	}

	numKVentries, lenErr := node.kvLLRB.Len()
	if nil != lenErr {
		err = lenErr
		return
	}
	for i := 0; i < numKVentries; i++ {
		key, value, _, getByIndexErr := node.kvLLRB.GetByIndex(i)
		if nil != getByIndexErr {
			err = getByIndexErr
			return
		}
		keyAsString, nonShadowingErr := tree.DumpKey(key)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		fmt.Printf("%v  .kvLLRB[%v].Key       = %v\n", indent, i, keyAsString)
		if node.leaf {
			valueAsString, nonShadowingErr := tree.DumpValue(value)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			fmt.Printf("%v  .kvLLRB[%v].Value     = %v\n", indent, i, valueAsString)
		} else {
			childNode := value.(*btreeNodeStruct)
			fmt.Printf("%v  .kvLLRB[%v].Value     = %p\n", indent, i, childNode)
			tree.dumpNode(childNode, "    "+indent)
		}
	}

	err = nil

	return
}
