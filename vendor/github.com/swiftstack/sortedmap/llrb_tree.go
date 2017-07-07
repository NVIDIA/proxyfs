package sortedmap

// Left-Leaning Red-Black (LLRB) described here:
//
//   http://www.cs.princeton.edu/~rs/talks/LLRB/08Penn.pdf
//   http://www.cs.princeton.edu/~rs/talks/LLRB/LLRB.pdf
//   http://www.cs.princeton.edu/~rs/talks/LLRB/Java/RedBlackBST.java
//
// Red-Black Trees are implemented by "coloring" nodes of a Binary Search Tree ("BST")
// so as to map a 2-3-4 Tree isomorphically. A 2-3 Tree may also be used with this
// same coloring scheme and will simplify the algorithms even more (with only a negligible
// performance impact). The implementation herein utilizes this 2-3 Tree basis.

import "sync"

const (
	RED   = true
	BLACK = false
)

type llrbNodeStruct struct {
	Key
	Value
	left  *llrbNodeStruct // Pointer to Left Child (or nil)
	right *llrbNodeStruct // Pointer to Right Child (or nil)
	color bool            // Color of parent link
	len   int             // Number of nodes (including this node) in a tree "rooted" by this node
}

type llrbTreeStruct struct {
	sync.Mutex
	Compare
	LLRBTreeCallbacks
	root *llrbNodeStruct
}

// API functions (see api.go)

func (tree *llrbTreeStruct) BisectLeft(key Key) (index int, found bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	if nil == node {
		index = -1
		found = false
		err = nil

		return
	}

	nodeIndex := 0 // computed index of current node
	if nil != node.left {
		nodeIndex = node.left.len
	}

	compareResult, compareErr := tree.Compare(key, node.Key) // Pre-load recursion test value
	if nil != compareErr {
		err = compareErr
		return
	}

	for 0 != compareResult {
		if compareResult < 0 { // key < node.Key
			node = node.left

			if nil == node {
				// key not found, nodeIndex points to key:value just after where key would go

				index = nodeIndex - 1
				found = false
				err = nil

				return
			} else { // nil != node, so recurse from here
				if nil == node.right {
					nodeIndex = nodeIndex - 1
				} else { // nil != node.right
					nodeIndex = nodeIndex - node.right.len - 1
				}
			}
		} else { // compareResult > 0 (key > node.Key)
			node = node.right

			if nil == node {
				// key not found, nodeIndex points to key:value just before where key would go

				index = nodeIndex
				found = false
				err = nil

				return
			} else { // nil != node, so recurse from here
				if nil == node.left {
					nodeIndex = nodeIndex + 1
				} else { // nil != node.left
					nodeIndex = nodeIndex + node.left.len + 1
				}
			}
		}

		compareResult, compareErr = tree.Compare(key, node.Key) // next recursion step's test value
		if nil != compareErr {
			err = compareErr
			return
		}
	}

	// If we reach here, nodeIndex is to matching key

	index = nodeIndex
	found = true
	err = nil

	return
}

func (tree *llrbTreeStruct) BisectRight(key Key) (index int, found bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	if nil == node {
		index = 0
		found = false
		err = nil

		return
	}

	nodeIndex := 0 // computed index of current node
	if nil != node.left {
		nodeIndex = node.left.len
	}

	compareResult, compareErr := tree.Compare(key, node.Key) // Pre-load recursion test value
	if nil != compareErr {
		err = compareErr
		return
	}

	for 0 != compareResult {
		if compareResult < 0 { // key < node.Key
			node = node.left

			if nil == node {
				// key not found, nodeIndex points to key:value just after where key would go

				index = nodeIndex
				found = false
				err = nil

				return
			} else { // nil != node, so recurse from here
				if nil == node.right {
					nodeIndex = nodeIndex - 1
				} else { // nil != node.right
					nodeIndex = nodeIndex - node.right.len - 1
				}
			}
		} else { // compareResult > 0 (key > node.Key)
			node = node.right

			if nil == node {
				// key not found, nodeIndex points to key:value just before where key would go

				index = nodeIndex + 1
				found = false
				err = nil

				return
			} else { // nil != node, so recurse from here
				if nil == node.left {
					nodeIndex = nodeIndex + 1
				} else { // nil != node.left
					nodeIndex = nodeIndex + node.left.len + 1
				}
			}
		}

		compareResult, compareErr = tree.Compare(key, node.Key) // next recursion step's test value
		if nil != compareErr {
			err = compareErr
			return
		}
	}

	// If we reach here, nodeIndex is to matching key

	index = nodeIndex
	found = true
	err = nil

	return
}

func (tree *llrbTreeStruct) DeleteByIndex(index int) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	if (index < 0) || (nil == tree.root) || (index >= tree.root.len) {
		ok = false
		err = nil

		return
	}

	ok = true // index is within [0,# nodes), so we know we will succeed

	key := tree.preDeleteByIndexAdjustLen(tree.root, index)

	tree.root, err = tree.delete(tree.root, key)
	if nil != err {
		return
	}

	if nil != tree.root {
		tree.root.color = BLACK
	}

	err = nil

	return
}

func (tree *llrbTreeStruct) DeleteByKey(key Key) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	ok, err = tree.preDeleteByKeyAdjustLen(tree.root, key)
	if nil != err {
		return
	}
	if !ok {
		err = nil
		return
	}

	tree.root, err = tree.delete(tree.root, key)
	if nil != err {
		return
	}
	if nil != tree.root {
		tree.root.color = BLACK
	}

	return
}

func (tree *llrbTreeStruct) GetByIndex(index int) (key Key, value Value, ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	err = nil

	node := tree.root

	if (index < 0) || (nil == node) || (index >= node.len) {
		key = nil
		value = nil
		ok = false

		return
	}

	ok = true // index is within [0,# nodes), so we know we will succeed

	nodeIndex := 0 // computed index of current node
	if nil != node.left {
		nodeIndex = node.left.len
	}

	for nodeIndex != index {
		if nodeIndex > index {
			node = node.left

			if nil == node.right {
				nodeIndex = nodeIndex - 1
			} else { // nil != node.right
				nodeIndex = nodeIndex - node.right.len - 1
			}
		} else { // nodeIndex < index
			node = node.right

			if nil == node.left {
				nodeIndex = nodeIndex + 1
			} else { // nil != node.left
				nodeIndex = nodeIndex + node.left.len + 1
			}
		}
	}

	key = node.Key
	value = node.Value

	return
}

func (tree *llrbTreeStruct) GetByKey(key Key) (value Value, ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	for nil != node {
		compareResult, compareErr := tree.Compare(key, node.Key)
		if nil != compareErr {
			err = compareErr
			return
		}

		switch {
		case compareResult < 0: // key < node.Key
			node = node.left
		case compareResult > 0: // key > node.Key
			node = node.right
		default: // compareResult == 0 (key == node.Key)
			value = node.Value
			ok = true
			err = nil

			return
		}
	}

	// If we reach here, key was not found

	value = nil
	ok = false
	err = nil

	return
}

func (tree *llrbTreeStruct) Len() (numberOfItems int, err error) {
	tree.Lock()
	defer tree.Unlock()

	err = nil

	if nil == tree.root {
		numberOfItems = 0
	} else {
		numberOfItems = tree.root.len
	}

	return
}

func (tree *llrbTreeStruct) PatchByIndex(index int, value Value) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	err = nil

	node := tree.root

	if (index < 0) || (nil == node) || (index >= node.len) {
		ok = false

		return
	}

	ok = true // index is within [0,# nodes), so we know we will succeed

	nodeIndex := 0 // computed index of current node
	if nil != node.left {
		nodeIndex = node.left.len
	}

	for nodeIndex != index {
		if nodeIndex > index {
			node = node.left

			if nil == node.right {
				nodeIndex = nodeIndex - 1
			} else { // nil != node.right
				nodeIndex = nodeIndex - node.right.len - 1
			}
		} else { // nodeIndex < index
			node = node.right

			if nil == node.left {
				nodeIndex = nodeIndex + 1
			} else { // nil != node.left
				nodeIndex = nodeIndex + node.left.len + 1
			}
		}
	}

	node.Value = value

	return
}

func (tree *llrbTreeStruct) PatchByKey(key Key, value Value) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	for nil != node {
		compareResult, compareErr := tree.Compare(key, node.Key)
		if nil != compareErr {
			err = compareErr
			return
		}

		switch {
		case compareResult < 0: // key < node.Key
			node = node.left
		case compareResult > 0: // key > node.Key
			node = node.right
		default: // compareResult == 0 (key == node.Key)
			node.Value = value
			ok = true
			err = nil

			return
		}
	}

	// If we reach here, key was not found

	ok = false
	err = nil

	return
}

func (tree *llrbTreeStruct) Put(key Key, value Value) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	updatedRoot, ok, err := tree.insert(tree.root, key, value)
	if nil != err {
		return
	}

	if ok {
		tree.root = updatedRoot
		tree.postInsertAdjustLen(tree.root, key)
		tree.root.color = BLACK
	}

	err = nil

	return
}

// Recursive functions

func (tree *llrbTreeStruct) insert(oldNexusNode *llrbNodeStruct, key Key, value Value) (newNexusNode *llrbNodeStruct, ok bool, err error) {
	if nil == oldNexusNode {
		// Add new leaf node - .len will be updated by later call to postInsertAdjustLen()

		newNexusNode = &llrbNodeStruct{Key: key, Value: value, left: nil, right: nil, color: RED, len: 0}
		ok = true
		err = nil

		return
	}

	newNexusNode = oldNexusNode

	compareResult, compareErr := tree.Compare(key, newNexusNode.Key)
	if nil != compareErr {
		err = compareErr
		return
	}

	switch {
	case compareResult < 0: // key < newNexusNode.Key
		updatedNewNexusNodeLeft, nonShadowingOk, insertErr := tree.insert(newNexusNode.left, key, value)
		if nil != insertErr {
			err = insertErr
			return
		}
		if nonShadowingOk {
			newNexusNode.left = updatedNewNexusNodeLeft
		} else { // !nonShadowingOk
			ok = false
			err = nil

			return
		}
	case compareResult > 0: // key > newNexusNode.Key
		updatedNewNexusNodeRight, nonShadowingOk, insertErr := tree.insert(newNexusNode.right, key, value)
		if nil != insertErr {
			err = insertErr
			return
		}
		if nonShadowingOk {
			newNexusNode.right = updatedNewNexusNodeRight
		} else { // !nonShadowingOk
			ok = false
			err = nil

			return
		}
	default: // compareResult == 0 (key == newNexusNode.Key)
		ok = false // Duplicate Key not supported
		err = nil

		return
	}

	ok = true // if we reach here, we will have succeeded
	err = nil

	newNexusNode = tree.fixUp(newNexusNode)

	return
}

func (tree *llrbTreeStruct) postInsertAdjustLen(node *llrbNodeStruct, key Key) (err error) {
	node.len++

	compareResult, compareErr := tree.Compare(key, node.Key)
	if nil != compareErr {
		err = compareErr
		return
	}

	switch {
	case compareResult < 0: // key < node.Key
		tree.postInsertAdjustLen(node.left, key)
	case compareResult > 0: // key > node.Key
		tree.postInsertAdjustLen(node.right, key)
	default: // compareResult == 0 (key == node.Key)
		// nothing to do additionally here
	}

	err = nil

	return
}

func (tree *llrbTreeStruct) preDeleteByIndexAdjustLen(node *llrbNodeStruct, index int) (key Key) {
	node.len--

	nodesOnLeft := 0
	if nil != node.left {
		nodesOnLeft = node.left.len
	}

	switch {
	case index < nodesOnLeft: // index indicates proceed down node.left
		key = tree.preDeleteByIndexAdjustLen(node.left, index)
	case index > nodesOnLeft: // index indicates proceed down node.right
		key = tree.preDeleteByIndexAdjustLen(node.right, (index - nodesOnLeft - 1))
	default: // index == nodesOnLeft
		key = node.Key // return node.Key so that subsequent recursion step can use it
	}

	return
}

func (tree *llrbTreeStruct) preDeleteByKeyAdjustLen(node *llrbNodeStruct, key Key) (ok bool, err error) {
	if nil == node {
		ok = false
		err = nil

		return
	}

	compareResult, compareErr := tree.Compare(key, node.Key)
	if nil != compareErr {
		err = compareErr
		return
	}

	switch {
	case compareResult < 0: // key < node.Key
		ok, err = tree.preDeleteByKeyAdjustLen(node.left, key)
		if nil != err {
			return
		}
		if ok {
			node.len--
		}
	case compareResult > 0: // key > node.Key
		ok, err = tree.preDeleteByKeyAdjustLen(node.right, key)
		if nil != err {
			return
		}
		if ok {
			node.len--
		}
	default: // compareResult == 0 (key == node.Key)
		node.len--
		ok = true
	}

	err = nil

	return
}

func (tree *llrbTreeStruct) delete(oldNexusNode *llrbNodeStruct, key Key) (newNexusNode *llrbNodeStruct, err error) {
	newNexusNode = oldNexusNode

	compareResult, compareErr := tree.Compare(key, newNexusNode.Key)
	if nil != compareErr {
		err = compareErr
		return
	}

	if compareResult < 0 {
		if isBlack(newNexusNode.left) && isBlack(newNexusNode.left.left) {
			newNexusNode = tree.moveRedLeft(newNexusNode)
		}

		newNexusNode.left, err = tree.delete(newNexusNode.left, key)
		if nil != err {
			return
		}
	} else { // compareResult >= 0
		if isRed(newNexusNode.left) {
			newNexusNode = tree.rotateRight(newNexusNode)

			compareResult, compareErr = tree.Compare(key, newNexusNode.Key)
			if nil != compareErr {
				err = compareErr
				return
			}
		}
		if (0 == compareResult) && (nil == newNexusNode.right) {
			newNexusNode = nil
			err = nil

			return
		}
		if isBlack(newNexusNode.right) && isBlack(newNexusNode.right.left) {
			newNexusNode = tree.moveRedRight(newNexusNode)

			compareResult, compareErr = tree.Compare(key, newNexusNode.Key)
			if nil != compareErr {
				err = compareErr
				return
			}
		}
		if 0 == compareResult {
			newNexusNode.right, newNexusNode.Key, newNexusNode.Value = tree.deleteMin(newNexusNode.right)
		} else { // 0 != compareResult
			newNexusNode.right, err = tree.delete(newNexusNode.right, key)
			if nil != err {
				return
			}
		}
	}

	newNexusNode = tree.fixUp(newNexusNode)
	err = nil

	return
}

func (tree *llrbTreeStruct) deleteMin(oldNexusNode *llrbNodeStruct) (newNexusNode *llrbNodeStruct, minKey Key, minValue Value) {
	newNexusNode = oldNexusNode

	if nil == newNexusNode.left {
		minKey = newNexusNode.Key
		minValue = newNexusNode.Value
		newNexusNode = nil

		return
	}

	if isBlack(newNexusNode.left) && isBlack(newNexusNode.left.left) {
		newNexusNode = tree.moveRedLeft(newNexusNode)
	}

	newNexusNode.left, minKey, minValue = tree.deleteMin(newNexusNode.left)

	newNexusNode.len--

	newNexusNode = tree.fixUp(newNexusNode)

	return
}

// Helper functions

func isRed(node *llrbNodeStruct) bool {
	if nil == node {
		return false
	}

	return (RED == node.color)
}

func isBlack(node *llrbNodeStruct) bool {
	if nil == node {
		return true
	}

	return (BLACK == node.color)
}

func colorFlip(node *llrbNodeStruct) {
	node.color = !node.color
	node.left.color = !node.left.color
	node.right.color = !node.right.color
}

func (tree *llrbTreeStruct) rotateLeft(oldParentNode *llrbNodeStruct) (newParentNode *llrbNodeStruct) {
	// Adjust children fields

	newParentNode = oldParentNode.right
	oldParentNode.right = newParentNode.left
	newParentNode.left = oldParentNode

	// Adjust len fields

	nodesTransferred := 0
	if nil != oldParentNode.right {
		nodesTransferred = oldParentNode.right.len
	}

	oldParentNode.len = oldParentNode.len - newParentNode.len + nodesTransferred
	newParentNode.len = newParentNode.len - nodesTransferred + oldParentNode.len

	// Adjust color field

	newParentNode.color = oldParentNode.color
	oldParentNode.color = RED

	// All done

	return
}

func (tree *llrbTreeStruct) rotateRight(oldParentNode *llrbNodeStruct) (newParentNode *llrbNodeStruct) {
	// Adjust children fields

	newParentNode = oldParentNode.left
	oldParentNode.left = newParentNode.right
	newParentNode.right = oldParentNode

	// Adjust len fields

	nodesTransferred := 0
	if nil != oldParentNode.left {
		nodesTransferred = oldParentNode.left.len
	}

	oldParentNode.len = oldParentNode.len - newParentNode.len + nodesTransferred
	newParentNode.len = newParentNode.len - nodesTransferred + oldParentNode.len

	// Adjust color field

	newParentNode.color = oldParentNode.color
	oldParentNode.color = RED

	// All done

	return
}

func (tree *llrbTreeStruct) moveRedLeft(oldNexusNode *llrbNodeStruct) (newNexusNode *llrbNodeStruct) {
	newNexusNode = oldNexusNode

	colorFlip(newNexusNode)

	if isRed(newNexusNode.right.left) {
		newNexusNode.right = tree.rotateRight(newNexusNode.right)
		newNexusNode = tree.rotateLeft(newNexusNode)
		colorFlip(newNexusNode)
	}

	return
}

func (tree *llrbTreeStruct) moveRedRight(oldNexusNode *llrbNodeStruct) (newNexusNode *llrbNodeStruct) {
	newNexusNode = oldNexusNode

	colorFlip(newNexusNode)

	if isRed(newNexusNode.left.left) {
		newNexusNode = tree.rotateRight(newNexusNode)
		colorFlip(newNexusNode)
	}

	return
}

func (tree *llrbTreeStruct) fixUp(oldNexusNode *llrbNodeStruct) (newNexusNode *llrbNodeStruct) {
	newNexusNode = oldNexusNode

	if isRed(newNexusNode.right) {
		newNexusNode = tree.rotateLeft(newNexusNode)
	}
	if isRed(newNexusNode.left) && isRed(newNexusNode.left.left) {
		newNexusNode = tree.rotateRight(newNexusNode)
	}
	if isRed(newNexusNode.left) && isRed(newNexusNode.right) {
		colorFlip(newNexusNode)
	}

	return
}
