package sortedmap

import "fmt"

func (tree *btreeTreeStruct) Validate() (err error) {
	tree.Lock()
	defer tree.Unlock()

	err = tree.root.validate()

	return
}

func (node *btreeNodeStruct) validate() (err error) {
	if !node.loaded {
		err = node.tree.loadNode(node)
		if nil != err {
			return
		}
	}

	if node.leaf {
		err = node.kvLLRB.Validate()
		if nil != err {
			return
		}

		numKeysInLLRB, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		if numKeysInLLRB != int(node.items) {
			err = fmt.Errorf("Leaf node @%p items [%d] != node.kvLLRB.Len() [%d]", node, numKeysInLLRB, node.items)
			return
		}

		if !node.root && (uint64(numKeysInLLRB) < node.tree.minKeysPerNode) {
			err = fmt.Errorf("Non-Root Leaf node @%p kvLLRB.Len() [%d] < node.tree.minKeysPerNode [%d]", node, numKeysInLLRB, node.tree.minKeysPerNode)
			return
		}
		if uint64(numKeysInLLRB) > node.tree.maxKeysPerNode {
			err = fmt.Errorf("Leaf node @%p kvLLRB.Len() [%d] > node.tree.maxKeysPerNode [%d]", node, numKeysInLLRB, node.tree.maxKeysPerNode)
			return
		}
	} else {
		childItems := uint64(0)

		if nil != node.nonLeafLeftChild {
			if node.nonLeafLeftChild.parentNode != node {
				err = fmt.Errorf("Node @%p had nonLeafLeftChild @%p with unexpected .parentNode %p", node, node.nonLeafLeftChild, node.nonLeafLeftChild.parentNode)
				return
			}

			err = node.nonLeafLeftChild.validate()
			if nil != err {
				return
			}

			childItems += node.nonLeafLeftChild.items
		}

		err = node.kvLLRB.Validate()
		if nil != err {
			return
		}

		numChildrenInLLRB, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		if !node.root && (uint64(numChildrenInLLRB) < node.tree.minKeysPerNode) {
			err = fmt.Errorf("Non-Root Non-Leaf node @%p kvLLRB.Len() [%d] < node.tree.minKeysPerNode [%d]", node, numChildrenInLLRB, node.tree.minKeysPerNode)
			return
		}
		if uint64(numChildrenInLLRB) > node.tree.maxKeysPerNode {
			err = fmt.Errorf("Non-Leaf node @%p kvLLRB.Len() [%d] > node.tree.maxKeysPerNode [%d]", node, numChildrenInLLRB, node.tree.maxKeysPerNode)
			return
		}

		for i := 0; i < numChildrenInLLRB; i++ {
			_, childNodeAsValue, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: validate() had indexing problem in kvLLRB")
				return
			}

			childNode := childNodeAsValue.(*btreeNodeStruct)

			if childNode.parentNode != node {
				err = fmt.Errorf("Node @%p had childNode @%p with unexpected .parentNode %p", node, childNode, childNode.parentNode)
				return
			}

			err = childNode.validate()
			if nil != err {
				return
			}

			childItems += childNode.items
		}

		if childItems != node.items {
			err = fmt.Errorf("Non-Leaf node @%p items [%d] != sum of childNode's .items [%d]", node, childItems, node.items)
			return
		}

		if nil == node.rootPrefixSumChild {
			err = fmt.Errorf("Non-Leaf node @%p rootPrefixSumChild == nil", node)
			return
		}

		if nil != node.rootPrefixSumChild.prefixSumParent {
			err = fmt.Errorf("Non-Leaf node @%p rootPrefixSumChild.prefixSumParent != nil", node)
			return
		}

		if childItems != node.rootPrefixSumChild.prefixSumItems {
			err = fmt.Errorf("Non-Leaf node @%p rootPrefixSumChild.prefixSumItems (%v) expected to be %v [case 1]", node, node.rootPrefixSumChild.prefixSumItems, childItems)
			return
		}

		err = node.rootPrefixSumChild.validatePrefixSum()
		if nil != err {
			return
		}
	}

	err = nil

	return
}

func (node *btreeNodeStruct) validatePrefixSum() (err error) {
	expectedPrefixSumItems := node.items

	if nil != node.prefixSumLeftChild {
		expectedPrefixSumItems += node.prefixSumLeftChild.prefixSumItems

		if node != node.prefixSumLeftChild.prefixSumParent {
			err = fmt.Errorf("Non-Leaf node @%p non-rootPrefixSumChild.prefixSumParent mismatch [case 1]", node)
			return
		}

		err = node.prefixSumLeftChild.validatePrefixSum()
	}

	if nil != node.prefixSumRightChild {
		expectedPrefixSumItems += node.prefixSumRightChild.prefixSumItems

		if node != node.prefixSumRightChild.prefixSumParent {
			err = fmt.Errorf("Non-Leaf node @%p non-rootPrefixSumChild.prefixSumParent mismatch [case 2]", node)
			return
		}

		err = node.prefixSumLeftChild.validatePrefixSum()
		if nil != err {
			return
		}
	}

	if expectedPrefixSumItems != node.prefixSumItems {
		err = fmt.Errorf("Non-Leaf node @%p rootPrefixSumChild.prefixSumItems (%v) expected to be %v [case 2]", node, node.prefixSumItems, expectedPrefixSumItems)
		return
	}

	err = nil

	return
}
