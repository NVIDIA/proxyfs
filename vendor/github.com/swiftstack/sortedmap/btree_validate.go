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
			err = fmt.Errorf("Leaf node.items != node.kvLLRB.Len()")
			return
		}
	} else {
		childItems := uint64(0)

		if nil != node.nonLeafLeftChild {
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

			err = childNode.validate()
			if nil != err {
				return
			}

			childItems += childNode.items
		}

		if childItems != node.items {
			err = fmt.Errorf("Non-leaf node.items != sum of childNode's .items")
			return
		}

		if nil == node.rootPrefixSumChild {
			err = fmt.Errorf("Non-leaf node.rootPrefixSumChild == nil")
			return
		}

		if nil != node.rootPrefixSumChild.prefixSumParent {
			err = fmt.Errorf("Non-leaf node.rootPrefixSumChild.prefixSumParent != nil")
			return
		}

		if childItems != node.rootPrefixSumChild.prefixSumItems {
			err = fmt.Errorf("Non-leaf node.rootPrefixSumChild.prefixSumItems (%v) expected to be %v [case 1]", node.rootPrefixSumChild.prefixSumItems, childItems)
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
			err = fmt.Errorf("Non-leaf node's non-rootPrefixSumChild.prefixSumParent mismatch [case 1]")
			return
		}

		err = node.prefixSumLeftChild.validatePrefixSum()
	}

	if nil != node.prefixSumRightChild {
		expectedPrefixSumItems += node.prefixSumRightChild.prefixSumItems

		if node != node.prefixSumRightChild.prefixSumParent {
			err = fmt.Errorf("Non-leaf node's non-rootPrefixSumChild.prefixSumParent mismatch [case 2]")
			return
		}

		err = node.prefixSumLeftChild.validatePrefixSum()
		if nil != err {
			return
		}
	}

	if expectedPrefixSumItems != node.prefixSumItems {
		err = fmt.Errorf("Non-leaf node.rootPrefixSumChild.prefixSumItems (%v) expected to be %v [case 2]", node.prefixSumItems, expectedPrefixSumItems)
		return
	}

	err = nil

	return
}
