package sortedmap

import (
	"fmt"
	"sync"

	"github.com/swiftstack/cstruct"
)

type btreeNodeStruct struct {
	objectNumber uint64 //                  if != 0, log* fields identify on-disk copy of this btreeNodeStruct
	objectOffset uint64
	objectLength uint64
	items        uint64 //                  number of item's (Keys & Values) at all leaf btreeNodeStructs at or below this btreeNodeStruct
	loaded       bool
	dirty        bool
	root         bool
	leaf         bool
	tree         *btreeTreeStruct
	parentNode   *btreeNodeStruct //        if root == true,  Value == nil
	kvLLRB       LLRBTree         //        if leaf == true,  Key == item's' Key, Value == item's Value
	//                                      if leaf == false, Key == minimum item's Key, Value = ptr to child btreeNodeStruct
	nonLeafLeftChild   *btreeNodeStruct //                    Value == ptr to btreeNodeStruct to the left of kvLLRB's 0th element
	rootPrefixSumChild *btreeNodeStruct //                    Value == ptr to root of binary tree of child btreeNodeStruct's sorted by prefixSumItems
	prefixSumItems     uint64           //  if root == false, sort key for prefix sum binary tree of child btreeNodeStruct's
	prefixSumKVIndex   int              //                    if node == parentNode.nonLeafLeftChild, value will be -1
	//                                                        if node != parentNode.nonLeafLeftChild, value will be index into parentNode.kvLLRB where Value == node
	prefixSumParent     *btreeNodeStruct //                   nil if this is also the rootPrefixSumChild
	prefixSumLeftChild  *btreeNodeStruct //                   nil if no left  child btreeNodeStruct
	prefixSumRightChild *btreeNodeStruct //                   nil if no right child btreeNodeStruct
}

type internalFlushedList map[*btreeNodeStruct]bool // Key == pointer to btreeNodeStruct in the "list"; Value is ignored

type onDiskUint64Struct struct {
	U64 uint64
}

type onDiskReferenceToNodeStruct struct {
	ObjectNumber uint64
	ObjectOffset uint64
	ObjectLength uint64
	Items        uint64
}

type onDiskNodeStruct struct {
	Items   uint64
	Root    bool
	Leaf    bool
	Payload []byte // if root == true,  maxKeysPerNode
	//                if leaf == true,  counted number <N> of Key:Value pairs
	//                if leaf == false, counted <N> number of children including (if present) nonLeafLeftChild
	//                                  nonLeafLeftChild's onDiskReferentToNodeStruct (if <N> > 0)
	//                                  counted <N-1> number of Key:onDiskReferenceToNodeStruct pairs
}

type onDiskReferenceKeyStruct struct { // Used as Key for staleOndiskReferences LLRBTree below
	objectNumber uint64
	objectOffset uint64
}

type onDiskReferencesContext struct { // Used as context for LLRBTreeCallbacks for staleOndiskReferences LLRBTree below
	LLRBTreeCallbacks
}

type btreeTreeStruct struct {
	sync.Mutex
	minKeysPerNode uint64 //                                only applies to non-Root nodes
	//                                                      "order" according to Bayer & McCreight (1972) & Comer (1979)
	maxKeysPerNode uint64 //                                "order" according to Knuth (1998)
	Compare
	BPlusTreeCallbacks
	root                  *btreeNodeStruct //               should never be nil
	activeClones          uint64
	clonedFromTree        *btreeTreeStruct
	staleOnDiskReferences LLRBTree //                       previously posted node locations yet to be discarded
	//                                                        key   is an onDiskReferenceKeyStruct objectNumber, objectOffset tuple
	//                                                        value is a simple objectLength value
}

// API functions (see api.go)

func (tree *btreeTreeStruct) BisectLeft(key Key) (index int, found bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root
	indexDelta := uint64(0)

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			netIndex, nonShadowingFound, nonShadowingErr := node.kvLLRB.BisectLeft(key)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}

			index = int(indexDelta) + netIndex
			found = nonShadowingFound

			err = nil
			return
		}

		minKey, _, ok, nonShadowingErr := node.kvLLRB.GetByIndex(0)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if ok {
			compareResult, nonShadowingErr := tree.Compare(key, minKey)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if 0 > compareResult {
				node = node.nonLeafLeftChild
			} else {
				nextIndex, _, nonShadowingErr := node.kvLLRB.BisectLeft(key)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				_, childNodeAsValue, _, nonShadowingErr := node.kvLLRB.GetByIndex(nextIndex)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				childNode := childNodeAsValue.(*btreeNodeStruct)

				if childNode == node.rootPrefixSumChild {
					if nil != childNode.prefixSumLeftChild {
						indexDelta += childNode.prefixSumLeftChild.prefixSumItems
					}
				} else {
					llrbLen, nonShadowingErr := node.kvLLRB.Len()
					if nil != nonShadowingErr {
						err = nonShadowingErr
						return
					}

					rightChildBoolStack := make([]bool, 0, (1 + llrbLen)) // actually only needed log-base-2 of node.kvLLRB.Len() (rounded up)... the height of Prefix Sum tree

					for {
						parentNode := childNode.prefixSumParent
						if parentNode.prefixSumLeftChild == childNode {
							rightChildBoolStack = append(rightChildBoolStack, false)
						} else { // parentNode.prefixSumRightChild == childNode
							rightChildBoolStack = append(rightChildBoolStack, true)
						}

						childNode = parentNode

						if nil == parentNode.prefixSumParent {
							break
						}
					}

					for i := (len(rightChildBoolStack) - 1); i >= 0; i-- {
						if rightChildBoolStack[i] {
							if nil != childNode.prefixSumLeftChild {
								indexDelta += childNode.prefixSumLeftChild.prefixSumItems
							}

							indexDelta += childNode.items

							childNode = childNode.prefixSumRightChild
						} else {
							childNode = childNode.prefixSumLeftChild
						}
					}

					if nil != childNode.prefixSumLeftChild {
						indexDelta += childNode.prefixSumLeftChild.prefixSumItems
					}
				}

				node = childNode
			}
		} else {
			node = node.nonLeafLeftChild
		}
	}
}

func (tree *btreeTreeStruct) BisectRight(key Key) (index int, found bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root
	indexDelta := uint64(0)

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			netIndex, nonShadowingFound, nonShadowingErr := node.kvLLRB.BisectRight(key)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}

			index = int(indexDelta) + netIndex
			found = nonShadowingFound

			err = nil
			return
		}

		minKey, _, ok, nonShadowingErr := node.kvLLRB.GetByIndex(0)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if ok {
			compareResult, nonShadowingErr := tree.Compare(key, minKey)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if 0 > compareResult {
				node = node.nonLeafLeftChild
			} else {
				nextIndex, _, nonShadowingErr := node.kvLLRB.BisectLeft(key)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				_, childNodeAsValue, _, nonShadowingErr := node.kvLLRB.GetByIndex(nextIndex)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				childNode := childNodeAsValue.(*btreeNodeStruct)

				if childNode == node.rootPrefixSumChild {
					if nil != childNode.prefixSumLeftChild {
						indexDelta += childNode.prefixSumLeftChild.prefixSumItems
					}
				} else {
					llrbLen, nonShadowingErr := node.kvLLRB.Len()
					if nil != nonShadowingErr {
						err = nonShadowingErr
						return
					}

					rightChildBoolStack := make([]bool, 0, (1 + llrbLen)) // actually only needed log-base-2 of this quantity (rounded up)... the height of Prefix Sum tree

					for {
						parentNode := childNode.prefixSumParent
						if parentNode.prefixSumLeftChild == childNode {
							rightChildBoolStack = append(rightChildBoolStack, false)
						} else { // parentNode.prefixSumRightChild == childNode
							rightChildBoolStack = append(rightChildBoolStack, true)
						}

						childNode = parentNode

						if nil == parentNode.prefixSumParent {
							break
						}
					}

					for i := (len(rightChildBoolStack) - 1); i >= 0; i-- {
						if rightChildBoolStack[i] {
							if nil != childNode.prefixSumLeftChild {
								indexDelta += childNode.prefixSumLeftChild.prefixSumItems
							}

							indexDelta += childNode.items

							childNode = childNode.prefixSumRightChild
						} else {
							childNode = childNode.prefixSumLeftChild
						}
					}

					if nil != childNode.prefixSumLeftChild {
						indexDelta += childNode.prefixSumLeftChild.prefixSumItems
					}
				}

				node = childNode
			}
		} else {
			node = node.nonLeafLeftChild
		}
	}
}

func (tree *btreeTreeStruct) DeleteByIndex(index int) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	parentIndexStack := []int{} // when not at the root,
	//                             let i == parentIndexStack[len(parentIndexStack) - 1] (i.e. the last element "pushed" on parentIndexStack)
	//                                 if i == -1 indicates we followed ParentNode's nonLeafLeftChild to get to this node
	//                                 if i >=  0 indicates we followed ParentNode's kvLLRB.GetByIndex(i)'s Value

	if (0 > index) || (uint64(index) >= node.items) {
		ok = false
		err = nil
		return
	}

	netIndex := uint64(index)

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			_, err = node.kvLLRB.DeleteByIndex(int(netIndex))
			if nil != err {
				return
			}
			tree.updatePrefixSumTreeLeafToRoot(node)
			tree.rebalanceHere(node, parentIndexStack)
			ok = true
			err = nil
			return
		}

		node = node.rootPrefixSumChild

		var leftChildPrefixSumItems uint64

		for {
			if nil == node.prefixSumLeftChild {
				leftChildPrefixSumItems = 0
			} else {
				leftChildPrefixSumItems = node.prefixSumLeftChild.prefixSumItems
			}

			if netIndex < leftChildPrefixSumItems {
				node = node.prefixSumLeftChild
			} else if netIndex < (leftChildPrefixSumItems + node.items) {
				netIndex -= leftChildPrefixSumItems
				parentIndexStack = append(parentIndexStack, node.prefixSumKVIndex)
				break
			} else {
				netIndex -= (leftChildPrefixSumItems + node.items)
				node = node.prefixSumRightChild
			}
		}
	}
}

func (tree *btreeTreeStruct) DeleteByKey(key Key) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	parentIndexStack := []int{} // when not at the root,
	//                             let i == parentIndexStack[len(parentIndexStack) - 1] (i.e. the last element "pushed" on parentIndexStack)
	//                                 if i == -1 indicates we followed ParentNode's nonLeafLeftChild to get to this node
	//                                 if i >=  0 indicates we followed ParentNode's kvLLRB.GetByIndex(i)'s Value

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			ok, err = node.kvLLRB.DeleteByKey(key)
			if nil != err {
				return
			}
			if ok {
				tree.updatePrefixSumTreeLeafToRoot(node)
				tree.rebalanceHere(node, parentIndexStack)
			}
			err = nil
			return
		}

		minKey, _, nonShadowingOK, nonShadowingErr := node.kvLLRB.GetByIndex(0)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if nonShadowingOK {
			compareResult, nonShadowingErr := tree.Compare(key, minKey)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if 0 > compareResult {
				parentIndexStack = append(parentIndexStack, -1)

				node = node.nonLeafLeftChild
			} else {
				kvIndex, _, nonShadowingErr := node.kvLLRB.BisectLeft(key)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				parentIndexStack = append(parentIndexStack, kvIndex)

				_, childNodeAsValue, _, nonShadowingErr := node.kvLLRB.GetByIndex(kvIndex)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				node = childNodeAsValue.(*btreeNodeStruct)
			}
		} else {
			node = node.nonLeafLeftChild
		}
	}
}

func (tree *btreeTreeStruct) GetByIndex(index int) (key Key, value Value, ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	if (0 > index) || (uint64(index) >= node.items) {
		ok = false
		err = nil
		return
	}

	netIndex := uint64(index)

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			key, value, _, err = node.kvLLRB.GetByIndex(int(netIndex))
			if nil != err {
				return
			}
			ok = true
			err = nil
			return
		}

		node = node.rootPrefixSumChild

		var leftChildPrefixSumItems uint64

		for {
			if nil == node.prefixSumLeftChild {
				leftChildPrefixSumItems = 0
			} else {
				leftChildPrefixSumItems = node.prefixSumLeftChild.prefixSumItems
			}

			if netIndex < leftChildPrefixSumItems {
				node = node.prefixSumLeftChild
			} else if netIndex < (leftChildPrefixSumItems + node.items) {
				netIndex -= leftChildPrefixSumItems
				break
			} else {
				netIndex -= (leftChildPrefixSumItems + node.items)
				node = node.prefixSumRightChild
			}
		}
	}
}

func (tree *btreeTreeStruct) GetByKey(key Key) (value Value, ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			value, ok, err = node.kvLLRB.GetByKey(key)
			return
		}

		minKey, _, nonShadowingOK, nonShadowingErr := node.kvLLRB.GetByIndex(0)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if nonShadowingOK {
			compareResult, nonShadowingErr := tree.Compare(key, minKey)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if 0 > compareResult {
				node = node.nonLeafLeftChild
			} else {
				nextIndex, _, nonShadowingErr := node.kvLLRB.BisectLeft(key)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				_, childNodeAsValue, _, nonShadowingErr := node.kvLLRB.GetByIndex(nextIndex)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				node = childNodeAsValue.(*btreeNodeStruct)
			}
		} else {
			node = node.nonLeafLeftChild
		}
	}
}

func (tree *btreeTreeStruct) Len() (numberOfItems int, err error) {
	tree.Lock()
	defer tree.Unlock()

	if !tree.root.loaded {
		err = tree.loadNode(tree.root)
		if nil != err {
			return
		}
	}

	numberOfItems = int(tree.root.items)

	err = nil
	return
}

func (tree *btreeTreeStruct) PatchByIndex(index int, value Value) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	if (0 > index) || (uint64(index) >= node.items) {
		ok = false
		err = nil
		return
	}

	netIndex := uint64(index)

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			tree.touchLoadedNodeToRoot(node)
			_, err = node.kvLLRB.PatchByIndex(int(netIndex), value)
			ok = true
			return
		}

		node = node.rootPrefixSumChild

		var leftChildPrefixSumItems uint64

		for {
			if nil == node.prefixSumLeftChild {
				leftChildPrefixSumItems = 0
			} else {
				leftChildPrefixSumItems = node.prefixSumLeftChild.prefixSumItems
			}

			if netIndex < leftChildPrefixSumItems {
				node = node.prefixSumLeftChild
			} else if netIndex < (leftChildPrefixSumItems + node.items) {
				netIndex -= leftChildPrefixSumItems
				break
			} else {
				netIndex -= (leftChildPrefixSumItems + node.items)
				node = node.prefixSumRightChild
			}
		}
	}
}

func (tree *btreeTreeStruct) PatchByKey(key Key, value Value) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			tree.touchLoadedNodeToRoot(node)
			ok, err = node.kvLLRB.PatchByKey(key, value)
			return
		}

		minKey, _, nonShadowingOK, nonShadowingErr := node.kvLLRB.GetByIndex(0)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if nonShadowingOK {
			compareResult, nonShadowingOK := tree.Compare(key, minKey)
			if nil != nonShadowingOK {
				err = nonShadowingErr
				return
			}
			if 0 > compareResult {
				node = node.nonLeafLeftChild
			} else {
				nextIndex, _, nonShadowingErr := node.kvLLRB.BisectLeft(key)
				if nil != nonShadowingOK {
					err = nonShadowingErr
					return
				}

				_, childNodeAsValue, _, nonShadowingErr := node.kvLLRB.GetByIndex(nextIndex)
				if nil != nonShadowingOK {
					err = nonShadowingErr
					return
				}

				node = childNodeAsValue.(*btreeNodeStruct)
			}
		} else {
			node = node.nonLeafLeftChild
		}
	}
}

func (tree *btreeTreeStruct) Put(key Key, value Value) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				return
			}
		}

		if node.leaf {
			_, keyAlreadyPresent, nonShadowingErr := node.kvLLRB.GetByKey(key)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}

			if keyAlreadyPresent {
				ok = false
			} else {
				err = tree.insertHere(node, key, value)
				ok = true
				return
			}

			err = nil
			return
		}

		minKey, _, nonShadowingOK, nonShadowingErr := node.kvLLRB.GetByIndex(0)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if nonShadowingOK {
			compareResult, nonShadowingErr := tree.Compare(key, minKey)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if 0 > compareResult {
				node = node.nonLeafLeftChild
			} else {
				nextIndex, _, nonShadowingErr := node.kvLLRB.BisectLeft(key)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				_, childNodeAsValue, _, nonShadowingErr := node.kvLLRB.GetByIndex(nextIndex)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}

				node = childNodeAsValue.(*btreeNodeStruct)
			}
		} else {
			node = node.nonLeafLeftChild
		}
	}
}

func (tree *btreeTreeStruct) FetchLayoutReport() (layoutReport LayoutReport, err error) {
	tree.Lock()
	defer tree.Unlock()

	layoutReport = make(map[uint64]uint64)

	err = tree.updateLayoutReport(layoutReport, tree.root)

	return
}

func (tree *btreeTreeStruct) TouchFlushedList(flushedList FlushedList) (err error) {
	iFL, ok := flushedList.(internalFlushedList)
	if !ok {
		err = fmt.Errorf("TouchFlushedList() could not parse flushedList")
		return
	}

	for node := range iFL {
		node.dirty = true
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) Flush(andPurge bool) (rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64, flushedList FlushedList, err error) {
	var (
		iFL internalFlushedList
	)

	tree.Lock()
	defer tree.Unlock()

	iFL = make(map[*btreeNodeStruct]bool)

	// First flush (and optionally purge) B+Tree

	err = tree.flushNode(tree.root, andPurge, iFL)
	if nil != err {
		return
	}

	rootObjectNumber = tree.root.objectNumber
	rootObjectOffset = tree.root.objectOffset
	rootObjectLength = tree.root.objectLength

	// All done

	flushedList = iFL
	err = nil
	return
}

func (tree *btreeTreeStruct) Purge(full bool) (err error) {
	tree.Lock()
	defer tree.Unlock()

	err = tree.purgeNode(tree.root, full)

	return
}

func (tree *btreeTreeStruct) Touch() (err error) {
	tree.Lock()
	defer tree.Unlock()

	err = tree.touchNode(tree.root)

	return
}

func (tree *btreeTreeStruct) TouchItem(thisItemIndexToTouch uint64) (nextItemIndexToTouch uint64, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	if 0 == node.items {
		// Special case where tree is empty... just return
		nextItemIndexToTouch = 0
		err = nil
		return
	}

	if thisItemIndexToTouch >= node.items {
		// Apparently tree has shrunk since last TouchItem() call,
		// so simply wrap back to the zeroth element
		thisItemIndexToTouch = 0
	}

	netIndex := uint64(thisItemIndexToTouch)

	for {
		if !node.loaded {
			err = tree.loadNode(node)
			if nil != err {
				// Upon detected corruption, just return
				nextItemIndexToTouch = 0
				return
			}
		}

		if node.leaf {
			// Touch this node up to root

			tree.touchLoadedNodeToRoot(node)

			// Return nextItemIndexToTouch as index beyond this leaf node

			itemsInLeafNode, nonShadowingErr := node.kvLLRB.Len()
			if nil != nonShadowingErr {
				// Upon detected corruption, just return
				nextItemIndexToTouch = 0
				err = nonShadowingErr
				return
			}

			nextItemIndexToTouch = thisItemIndexToTouch + (uint64(itemsInLeafNode) - netIndex)

			err = nil
			return
		}

		node = node.rootPrefixSumChild

		var leftChildPrefixSumItems uint64

		for {
			if nil == node.prefixSumLeftChild {
				leftChildPrefixSumItems = 0
			} else {
				leftChildPrefixSumItems = node.prefixSumLeftChild.prefixSumItems
			}

			if netIndex < leftChildPrefixSumItems {
				node = node.prefixSumLeftChild
			} else if netIndex < (leftChildPrefixSumItems + node.items) {
				netIndex -= leftChildPrefixSumItems
				break
			} else {
				netIndex -= (leftChildPrefixSumItems + node.items)
				node = node.prefixSumRightChild
			}
		}
	}
}

func (tree *btreeTreeStruct) Prune() (err error) {
	var (
		keyAsKey                      Key
		keyAsOnDiskReferenceKeyStruct *onDiskReferenceKeyStruct
		objectLengthAsValue           Value
		objectLengthAsUint64          uint64
		ok                            bool
	)

	tree.Lock()
	defer tree.Unlock()

	if nil != tree.clonedFromTree {
		err = fmt.Errorf("Prune() called on B+Tree that is a clone of another B+Tree")
		return
	}

	if 0 != tree.activeClones {
		err = fmt.Errorf("Prune() called on B+Tree with active clones")
		return
	}

	// Discard all stale OnDisk node references

	keyAsKey, objectLengthAsValue, ok, err = tree.staleOnDiskReferences.GetByIndex(0)

	for ok {
		keyAsOnDiskReferenceKeyStruct, ok = keyAsKey.(*onDiskReferenceKeyStruct)
		if !ok {
			err = fmt.Errorf("Logic error... tree.staleOnDiskReferences contained bad Key")
			return
		}
		objectLengthAsUint64, ok = objectLengthAsValue.(uint64)
		if !ok {
			err = fmt.Errorf("Logic error... tree.staleOnDiskReferences contained bad Value")
			return
		}
		err = tree.BPlusTreeCallbacks.DiscardNode(keyAsOnDiskReferenceKeyStruct.objectNumber, keyAsOnDiskReferenceKeyStruct.objectOffset, objectLengthAsUint64)
		if nil != err {
			return
		}
		ok, err = tree.staleOnDiskReferences.DeleteByIndex(0)
		if nil != err {
			return
		}
		if !ok {
			err = fmt.Errorf("Logic error... unable to delete previously fetched first element of tree.staleOnDiskReferences")
			return
		}
		keyAsKey, objectLengthAsValue, ok, err = tree.staleOnDiskReferences.GetByIndex(0)
	}

	// All done

	err = nil
	return
}

func (tree *btreeTreeStruct) Discard() (err error) {
	var (
		keyAsKey                      Key
		keyAsOnDiskReferenceKeyStruct *onDiskReferenceKeyStruct
		objectLengthAsValue           Value
		objectLengthAsUint64          uint64
		ok                            bool
	)

	tree.Lock()
	defer tree.Unlock()

	// Following logic dependent upon whether or not tree is a clone of some other B+Tree

	if nil == tree.clonedFromTree {
		// Make sure there are no active clones

		if 0 != tree.activeClones {
			err = fmt.Errorf("Discard() called on B+Tree with active clones")
			return
		}

		// Discard all nodes recursively

		err = tree.discardNode(tree.root)
		if nil != err {
			return
		}

		// Discard all stale OnDisk node references

		keyAsKey, objectLengthAsValue, ok, err = tree.staleOnDiskReferences.GetByIndex(0)

		for ok {
			keyAsOnDiskReferenceKeyStruct, ok = keyAsKey.(*onDiskReferenceKeyStruct)
			if !ok {
				err = fmt.Errorf("Logic error... tree.staleOnDiskReferences contained bad Key")
				return
			}
			objectLengthAsUint64, ok = objectLengthAsValue.(uint64)
			if !ok {
				err = fmt.Errorf("Logic error... tree.staleOnDiskReferences contained bad Value")
				return
			}
			err = tree.BPlusTreeCallbacks.DiscardNode(keyAsOnDiskReferenceKeyStruct.objectNumber, keyAsOnDiskReferenceKeyStruct.objectOffset, objectLengthAsUint64)
			if nil != err {
				return
			}
			ok, err = tree.staleOnDiskReferences.DeleteByIndex(0)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error... unable to delete previously fetched first element of tree.staleOnDiskReferences")
				return
			}
			keyAsKey, objectLengthAsValue, ok, err = tree.staleOnDiskReferences.GetByIndex(0)
		}
	} else {
		// This is a cloned B+Tree, so merely update clonedFromTree's activeClones count

		tree.clonedFromTree.Lock()
		tree.clonedFromTree.activeClones--
		tree.clonedFromTree.Unlock()
	}

	// Reset btreeTreeStruct to trigger Golang Garbage Collection now (and prevent further use)

	tree.Compare = nil
	tree.BPlusTreeCallbacks = nil
	tree.root = nil
	tree.clonedFromTree = nil

	// All done

	err = nil
	return
}

// Helper functions

func compareOnDiskReferenceKey(key1 Key, key2 Key) (result int, err error) {
	key1AsOnDiskReferenceKeyStructPtr, ok := key1.(*onDiskReferenceKeyStruct)
	if !ok {
		err = fmt.Errorf("compareOnDiskReferenceKey(non-*onDiskReferenceKeyStruct,) not supported")
		return
	}
	key2AsOnDiskReferenceKeyStructPtr, ok := key2.(*onDiskReferenceKeyStruct)
	if !ok {
		err = fmt.Errorf("compareOnDiskReferenceKey(,non-*onDiskReferenceKeyStruct) not supported")
		return
	}

	if key1AsOnDiskReferenceKeyStructPtr.objectNumber < key2AsOnDiskReferenceKeyStructPtr.objectNumber {
		result = -1
	} else if key1AsOnDiskReferenceKeyStructPtr.objectNumber == key2AsOnDiskReferenceKeyStructPtr.objectNumber {
		if key1AsOnDiskReferenceKeyStructPtr.objectOffset < key2AsOnDiskReferenceKeyStructPtr.objectOffset {
			result = -1
		} else if key1AsOnDiskReferenceKeyStructPtr.objectOffset == key2AsOnDiskReferenceKeyStructPtr.objectOffset {
			result = 0
		} else { // key1AsOnDiskReferenceKeyStructPtr.objectOffset > key2AsOnDiskReferenceKeyStructPtr.objectOffset
			result = 1
		}
	} else { // key1AsOnDiskReferenceKeyStructPtr.objectNumber > key2AsOnDiskReferenceKeyStructPtr.objectNumber
		result = 1
	}

	err = nil
	return
}

func (context *onDiskReferencesContext) DumpKey(key Key) (keyAsString string, err error) {
	keyAsStruct, ok := key.(*onDiskReferenceKeyStruct)
	if !ok {
		err = fmt.Errorf("DumpKey() argument not a *onDiskReferenceKeyStruct")
		return
	}
	keyAsString = fmt.Sprintf("0x%016X 0x%016X", keyAsStruct.objectNumber, keyAsStruct.objectOffset)
	err = nil
	return
}

func (context *onDiskReferencesContext) DumpValue(value Value) (valueAsString string, err error) {
	valueAsUint64, ok := value.(uint64)
	if !ok {
		err = fmt.Errorf("DumpValue() argument not a uint64")
		return
	}
	valueAsString = fmt.Sprintf("0x%016X", valueAsUint64)
	err = nil
	return
}

func (tree *btreeTreeStruct) discardNode(node *btreeNodeStruct) (err error) {
	if !node.loaded {
		err = tree.loadNode(node)
		if nil != err {
			return
		}
	}

	if !node.leaf {
		if nil != node.nonLeafLeftChild {
			err = tree.discardNode(node.nonLeafLeftChild)
			if nil != err {
				return
			}
		}

		numIndices, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		for i := 0; i < numIndices; i++ {
			_, childNodeAsValue, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: destroyNode() had indexing problem in kvLLRB")
				return
			}
			childNode := childNodeAsValue.(*btreeNodeStruct)

			err = tree.discardNode(childNode)
			if nil != err {
				return
			}
		}
	}

	if 0 == node.objectLength {
		err = nil
	} else {
		err = tree.BPlusTreeCallbacks.DiscardNode(node.objectNumber, node.objectOffset, node.objectLength)
	}

	return
}

func (tree *btreeTreeStruct) insertHere(insertNode *btreeNodeStruct, key Key, value Value) (err error) {
	insertNode.kvLLRB.Put(key, value)

	if insertNode.leaf {
		tree.updatePrefixSumTreeLeafToRoot(insertNode)
	}

	llrbLen, err := insertNode.kvLLRB.Len()
	if nil != err {
		return
	}

	if tree.maxKeysPerNode < uint64(llrbLen) {
		newRightSiblingNode := &btreeNodeStruct{
			objectNumber:        0, //                                               To be filled in once node is posted
			objectOffset:        0, //                                               To be filled in once node is posted
			objectLength:        0, //                                               To be filled in once node is posted
			items:               0,
			loaded:              true, //                                            Special case in that objectNumber == 0 means it has no onDisk copy
			dirty:               true,
			root:                false, //                                           Note: insertNode.root will also (at least eventually) be false
			leaf:                insertNode.leaf,
			tree:                tree,
			parentNode:          insertNode.parentNode,
			kvLLRB:              NewLLRBTree(tree.Compare, tree.BPlusTreeCallbacks),
			nonLeafLeftChild:    nil,
			rootPrefixSumChild:  nil,
			prefixSumItems:      0,   //                                             Not applicable to root node
			prefixSumParent:     nil, //                                             Not applicable to root node
			prefixSumLeftChild:  nil, //                                             Not applicable to root node
			prefixSumRightChild: nil, //                                             Not applicable to root node
		}

		var splitKey Key
		var splitValue Value

		for {
			llrbLen, nonShadowingErr := insertNode.kvLLRB.Len()
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if tree.minKeysPerNode >= uint64(llrbLen) {
				break
			}
			splitKey, splitValue, _, err = insertNode.kvLLRB.GetByIndex(llrbLen - 1)
			if nil != err {
				return
			}
			_, err = insertNode.kvLLRB.DeleteByIndex(llrbLen - 1)
			if nil != err {
				return
			}
			_, err = newRightSiblingNode.kvLLRB.Put(splitKey, splitValue)
			if nil != err {
				return
			}

			if insertNode.leaf {
				insertNode.items--
				newRightSiblingNode.items++
			}
		}

		if !insertNode.leaf {
			llrbLen, nonShadowingErr := insertNode.kvLLRB.Len()
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			splitKey, splitValue, _, err = insertNode.kvLLRB.GetByIndex(llrbLen - 1)
			if nil != err {
				return
			}
			_, err = insertNode.kvLLRB.DeleteByIndex(llrbLen - 1)
			if nil != err {
				return
			}
			newRightSiblingNode.nonLeafLeftChild = splitValue.(*btreeNodeStruct)

			tree.arrangePrefixSumTree(insertNode)
			tree.arrangePrefixSumTree(newRightSiblingNode)
		}

		if insertNode.root {
			insertNode.root = false

			tree.root = &btreeNodeStruct{
				objectNumber:        0, //                                               To be filled in once new root node is posted
				objectOffset:        0, //                                               To be filled in once new root node is posted
				objectLength:        0, //                                               To be filled in once new root node is posted
				items:               insertNode.items + newRightSiblingNode.items,
				loaded:              true, //                                            Special case in that objectNumber == 0 means it has no onDisk copy
				dirty:               true,
				root:                true,
				leaf:                false,
				tree:                tree,
				parentNode:          nil,
				kvLLRB:              NewLLRBTree(tree.Compare, tree.BPlusTreeCallbacks),
				nonLeafLeftChild:    insertNode,
				rootPrefixSumChild:  nil,
				prefixSumItems:      0,   //                                             Not applicable to root node
				prefixSumParent:     nil, //                                             Not applicable to root node
				prefixSumLeftChild:  nil, //                                             Not applicable to root node
				prefixSumRightChild: nil, //                                             Not applicable to root node
			}

			insertNode.parentNode = tree.root
			newRightSiblingNode.parentNode = tree.root

			tree.root.kvLLRB.Put(splitKey, newRightSiblingNode)
		} else {
			_, err = insertNode.parentNode.kvLLRB.Put(splitKey, newRightSiblingNode)
			if nil != err {
				return
			}
		}

		tree.rearrangePrefixSumTreeToRoot(insertNode.parentNode)
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) rebalanceHere(rebalanceNode *btreeNodeStruct, parentIndexStack []int) (err error) {
	if rebalanceNode.root {
		err = nil
		return
	}

	llrbLen, err := rebalanceNode.kvLLRB.Len()
	if nil != err {
		return
	}

	if uint64(llrbLen) >= tree.minKeysPerNode {
		err = nil
		return
	}

	parentNode := rebalanceNode.parentNode

	var leftSiblingNode *btreeNodeStruct
	var rightSiblingNode *btreeNodeStruct

	parentIndexStackTailIndex := len(parentIndexStack) - 1
	parentNodeIndex := parentIndexStack[parentIndexStackTailIndex]
	parentIndexStackPruned := parentIndexStack[:parentIndexStackTailIndex]

	if -1 == parentNodeIndex {
		leftSiblingNode = nil
	} else {
		if 0 == parentNodeIndex {
			leftSiblingNode = parentNode.nonLeafLeftChild
		} else {
			_, leftSiblingNodeAsValue, _, nonShadowingErr := parentNode.kvLLRB.GetByIndex(parentNodeIndex - 1)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			leftSiblingNode = leftSiblingNodeAsValue.(*btreeNodeStruct)
		}

		if !leftSiblingNode.loaded {
			err = tree.loadNode(leftSiblingNode)
			if nil != err {
				return
			}
		}

		llrbLen, nonShadowingErr := leftSiblingNode.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		if uint64(llrbLen) > tree.minKeysPerNode {
			// leftSiblingNode can give up a key

			leftSiblingNode.items--
			rebalanceNode.items++

			if rebalanceNode.leaf {
				// move one key from leftSiblingNode to rebalanceNode

				leftSiblingNodeKVIndex := llrbLen - 1
				movedKey, movedValue, _, nonShadowingErr := leftSiblingNode.kvLLRB.GetByIndex(leftSiblingNodeKVIndex)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}
				_, err = leftSiblingNode.kvLLRB.DeleteByIndex(leftSiblingNodeKVIndex)
				if nil != err {
					return
				}
				_, err = rebalanceNode.kvLLRB.Put(movedKey, movedValue)
				if nil != err {
					return
				}
				_, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex)
				if nil != err {
					return
				}
				_, err = parentNode.kvLLRB.Put(movedKey, rebalanceNode)
				if nil != err {
					return
				}
			} else {
				// rotate one key from leftSiblingNode to parentNode & one key from parentNode to rebalanceNode

				leftSiblingNodeKVIndex := llrbLen - 1
				newParentKey, movedValue, _, nonShadowingErr := leftSiblingNode.kvLLRB.GetByIndex(leftSiblingNodeKVIndex)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}
				_, err = leftSiblingNode.kvLLRB.DeleteByIndex(leftSiblingNodeKVIndex)
				if nil != err {
					return
				}
				oldParentKey, _, _, nonShadowingErr := parentNode.kvLLRB.GetByIndex(parentNodeIndex)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}
				_, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex)
				if nil != err {
					return
				}
				oldRebalanceNodeNonLeafLeftChild := rebalanceNode.nonLeafLeftChild
				rebalanceNode.nonLeafLeftChild = movedValue.(*btreeNodeStruct)
				_, err = rebalanceNode.kvLLRB.Put(oldParentKey, oldRebalanceNodeNonLeafLeftChild)
				if nil != err {
					return
				}
				_, err = parentNode.kvLLRB.Put(newParentKey, rebalanceNode)
				if nil != err {
					return
				}

				tree.arrangePrefixSumTree(leftSiblingNode)
				tree.arrangePrefixSumTree(rebalanceNode)
			}

			tree.arrangePrefixSumTree(parentNode)

			tree.markNodeDirty(leftSiblingNode)

			err = nil
			return
		}
	}

	llrbLen, err = parentNode.kvLLRB.Len()
	if nil != err {
		return
	}

	if (llrbLen - 1) == parentNodeIndex {
		rightSiblingNode = nil
	} else {
		_, rightSiblingNodeAsValue, _, nonShadowingErr := parentNode.kvLLRB.GetByIndex(parentNodeIndex + 1)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		rightSiblingNode = rightSiblingNodeAsValue.(*btreeNodeStruct)

		if !rightSiblingNode.loaded {
			err = tree.loadNode(rightSiblingNode)
			if nil != err {
				return
			}
		}

		llrbLen, nonShadowingErr := rightSiblingNode.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		if uint64(llrbLen) > tree.minKeysPerNode {
			// rightSiblingNode can give up a key

			rebalanceNode.items++
			rightSiblingNode.items--

			if rebalanceNode.leaf {
				// move one key from rightSiblingNode to rebalanceNode

				movedKey, movedValue, _, nonShadowingErr := rightSiblingNode.kvLLRB.GetByIndex(0)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}
				_, err = rightSiblingNode.kvLLRB.DeleteByIndex(0)
				if nil != err {
					return
				}
				_, err = rebalanceNode.kvLLRB.Put(movedKey, movedValue)
				if nil != err {
					return
				}
				newParentKey, _, _, nonShadowingErr := rightSiblingNode.kvLLRB.GetByIndex(0)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}
				_, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex + 1)
				if nil != err {
					return
				}
				_, err = parentNode.kvLLRB.Put(newParentKey, rightSiblingNode)
				if nil != err {
					return
				}
			} else {
				// rotate one key from rightSiblingNode to parentNode & one key from parentNode to rebalanceNode

				movedValue := rightSiblingNode.nonLeafLeftChild
				newParentKey, newRightSiblingNodeNonLeafLeftChild, _, nonShadowingErr := rightSiblingNode.kvLLRB.GetByIndex(0)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}
				_, err = rightSiblingNode.kvLLRB.DeleteByIndex(0)
				if nil != err {
					return
				}
				oldParentKey, _, _, nonShadowingErr := parentNode.kvLLRB.GetByIndex(parentNodeIndex + 1)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}
				_, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex + 1)
				if nil != err {
					return
				}
				rebalanceNode.kvLLRB.Put(oldParentKey, movedValue)
				rightSiblingNode.nonLeafLeftChild = newRightSiblingNodeNonLeafLeftChild.(*btreeNodeStruct)
				_, err = parentNode.kvLLRB.Put(newParentKey, rightSiblingNode)
				if nil != err {
					return
				}

				tree.arrangePrefixSumTree(rebalanceNode)
				tree.arrangePrefixSumTree(rightSiblingNode)
			}

			tree.arrangePrefixSumTree(parentNode)

			tree.markNodeDirty(rightSiblingNode)

			err = nil
			return
		}
	}

	// no simple move was possible, so we have to merge sibling nodes (always possible since we are not at the root)

	if nil != leftSiblingNode {
		// move keys from rebalanceNode to leftSiblingNode (along with former splitKey for non-leaf case)

		leftSiblingNode.items += rebalanceNode.items

		oldSplitKey, _, _, nonShadowingErr := parentNode.kvLLRB.GetByIndex(parentNodeIndex)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if !rebalanceNode.leaf {
			leftSiblingNode.kvLLRB.Put(oldSplitKey, rebalanceNode.nonLeafLeftChild)
		}
		numItemsToMove, nonShadowingErr := rebalanceNode.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		for i := 0; i < numItemsToMove; i++ {
			movedKey, movedValue, _, nonShadowingErr := rebalanceNode.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			leftSiblingNode.kvLLRB.Put(movedKey, movedValue)
		}

		llrbLen, nonShadowingErr := parentNode.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		if parentNode.root && (1 == llrbLen) {
			// height will reduce by one, so make leftSiblingNode the new root

			leftSiblingNode.root = true
			leftSiblingNode.parentNode = nil
			tree.root = leftSiblingNode

			if !leftSiblingNode.leaf {
				tree.arrangePrefixSumTree(leftSiblingNode)
			}
		} else {
			// height will remain the same, so just delete oldSplitKey from parentNode and recurse

			_, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex)
			if nil != err {
				return
			}

			if !leftSiblingNode.leaf {
				tree.arrangePrefixSumTree(leftSiblingNode)
			}

			tree.arrangePrefixSumTree(parentNode)

			tree.rebalanceHere(parentNode, parentIndexStackPruned)
		}

		tree.markNodeDirty(leftSiblingNode)
	} else if nil != rightSiblingNode {
		// move keys from rightSiblingNode to rebalanceNode (along with former splitKey for non-leaf case)

		rebalanceNode.items += rightSiblingNode.items

		oldSplitKey, _, _, nonShadowingErr := parentNode.kvLLRB.GetByIndex(parentNodeIndex + 1)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if !rebalanceNode.leaf {
			_, err = rebalanceNode.kvLLRB.Put(oldSplitKey, rightSiblingNode.nonLeafLeftChild)
			if nil != err {
				return
			}
		}
		numItemsToMove, nonShadowingErr := rightSiblingNode.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		for i := 0; i < numItemsToMove; i++ {
			movedKey, movedValue, _, nonShadowingErr := rightSiblingNode.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			_, err = rebalanceNode.kvLLRB.Put(movedKey, movedValue)
			if nil != err {
				return
			}
		}

		llrbLen, nonShadowingErr := parentNode.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		if parentNode.root && (1 == llrbLen) {
			// height will reduce by one, so make rebalanceNode the new root

			rebalanceNode.root = true
			rebalanceNode.parentNode = nil
			tree.root = rebalanceNode

			if !rebalanceNode.leaf {
				tree.arrangePrefixSumTree(rebalanceNode)
			}
		} else {
			// height will remain the same, so just delete oldSplitKey from parentNode and recurse

			_, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex + 1)
			if nil != err {
				return
			}

			if !rebalanceNode.leaf {
				tree.arrangePrefixSumTree(rebalanceNode)
			}

			tree.arrangePrefixSumTree(parentNode)

			tree.rebalanceHere(parentNode, parentIndexStackPruned)
		}

		tree.markNodeDirty(rightSiblingNode)
	} else {
		// non-root node must have had a sibling, so if we reach here, we have a logic problem

		err = fmt.Errorf("Logic error: rebalanceHere() found non-leaf node with no sibling in parentNode.kvLLRB")
		return
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) flushNode(node *btreeNodeStruct, andPurge bool, iFL internalFlushedList) (err error) {
	if !node.loaded {
		err = nil
		return
	}

	if !node.leaf {
		if nil != node.nonLeafLeftChild {
			err = tree.flushNode(node.nonLeafLeftChild, andPurge, iFL)
			if nil != err {
				return
			}
		}

		numIndices, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		for i := 0; i < numIndices; i++ {
			_, childNodeAsValue, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: flushNode() had indexing problem in kvLLRB")
				return
			}
			childNode := childNodeAsValue.(*btreeNodeStruct)

			err = tree.flushNode(childNode, andPurge, iFL)
			if nil != err {
				return
			}
		}
	}

	if node.dirty {
		tree.postNode(node)
		iFL[node] = true
	}

	if andPurge {
		node.kvLLRB = nil
		node.nonLeafLeftChild = nil
		node.rootPrefixSumChild = nil

		node.loaded = false
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) purgeNode(node *btreeNodeStruct, full bool) (err error) {
	if !node.loaded {
		err = nil
		return
	}

	if full && node.dirty {
		err = fmt.Errorf("Logic error: purgeNode(,full==true) shouldn't have found a dirty node")
		return
	}

	if !node.leaf {
		if nil != node.nonLeafLeftChild {
			err = tree.purgeNode(node.nonLeafLeftChild, full)
			if nil != err {
				return
			}
		}

		numIndices, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		for i := 0; i < numIndices; i++ {
			_, childNodeAsValue, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: purgeNode() had indexing problem in kvLLRB")
				return
			}
			childNode := childNodeAsValue.(*btreeNodeStruct)

			err = tree.purgeNode(childNode, full)
			if nil != err {
				return
			}
		}
	}

	if !node.dirty {
		node.kvLLRB = nil
		node.nonLeafLeftChild = nil
		node.rootPrefixSumChild = nil

		node.loaded = false
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) markNodeDirty(node *btreeNodeStruct) {
	node.dirty = true

	// Did node come from a now-stale copy on disk?

	if 0 != node.objectLength {
		// Schedule stale on-disk reference to be reclaimed in (this or top-level ancestor) non-clone B+Tree

		thisTree := tree
		for nil != thisTree.clonedFromTree {
			parentTree := thisTree.clonedFromTree
			parentTree.Lock()
			if tree != thisTree {
				thisTree.Unlock()
			}
			thisTree = parentTree
		}

		// Mark node's prior on-disk reference as stale

		staleOnDiskReferenceKey := &onDiskReferenceKeyStruct{
			objectNumber: node.objectNumber,
			objectOffset: node.objectOffset,
		}

		_, err := thisTree.staleOnDiskReferences.Put(staleOnDiskReferenceKey, node.objectLength)
		if nil != err {
			err = fmt.Errorf("Logic error inserting into staleOnDiskReferences LLRB Tree: %v", err)
			panic(err)
		}
		// if ok were false, that just means it was already there which is just fine

		if tree != thisTree {
			thisTree.Unlock()
		}

		// Zero-out on-disk reference so that the above is only done once for this now dirty node

		node.objectNumber = 0
		node.objectOffset = 0
		node.objectLength = 0
	}
}

func (tree *btreeTreeStruct) touchNode(node *btreeNodeStruct) (err error) {
	if !node.loaded {
		err = tree.loadNode(node)
		if nil != err {
			return
		}
	}

	tree.markNodeDirty(node)

	if !node.leaf {
		if nil != node.nonLeafLeftChild {
			err = tree.touchNode(node.nonLeafLeftChild)
			if nil != err {
				return
			}
		}

		numIndices, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		for i := 0; i < numIndices; i++ {
			_, childNodeAsValue, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: touchNode() had indexing problem in kvLLRB")
				return
			}
			childNode := childNodeAsValue.(*btreeNodeStruct)

			err = tree.touchNode(childNode)
			if nil != err {
				return
			}
		}
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) touchLoadedNodeToRoot(node *btreeNodeStruct) {
	for {
		tree.markNodeDirty(node)

		if node.root {
			return
		}

		node = node.parentNode
	}
}

func (tree *btreeTreeStruct) arrangePrefixSumTreeRecursively(prefixSumSlice []*btreeNodeStruct) (midPointNode *btreeNodeStruct) {
	midPointIndex := int(len(prefixSumSlice) / 2)

	midPointNode = prefixSumSlice[midPointIndex]

	if 0 < midPointIndex {
		midPointNode.prefixSumLeftChild = tree.arrangePrefixSumTreeRecursively(prefixSumSlice[:midPointIndex])
		midPointNode.prefixSumLeftChild.prefixSumParent = midPointNode
		midPointNode.prefixSumItems += midPointNode.prefixSumLeftChild.prefixSumItems
	}
	if (midPointIndex + 1) < len(prefixSumSlice) {
		midPointNode.prefixSumRightChild = tree.arrangePrefixSumTreeRecursively(prefixSumSlice[(midPointIndex + 1):])
		midPointNode.prefixSumRightChild.prefixSumParent = midPointNode
		midPointNode.prefixSumItems += midPointNode.prefixSumRightChild.prefixSumItems
	}

	return
}

func (tree *btreeTreeStruct) arrangePrefixSumTree(node *btreeNodeStruct) (err error) {
	numChildrenInLLRB, err := node.kvLLRB.Len()
	if nil != err {
		return
	}

	prefixSumSlice := make([]*btreeNodeStruct, (1 + numChildrenInLLRB))

	node.nonLeafLeftChild.prefixSumItems = node.nonLeafLeftChild.items
	node.nonLeafLeftChild.prefixSumKVIndex = -1
	node.nonLeafLeftChild.prefixSumParent = nil
	node.nonLeafLeftChild.prefixSumLeftChild = nil
	node.nonLeafLeftChild.prefixSumRightChild = nil

	prefixSumSlice[0] = node.nonLeafLeftChild

	for i := 0; i < numChildrenInLLRB; i++ {
		_, childNodeAsValue, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i)
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}
		if !ok {
			err = fmt.Errorf("Logic error: arrangePrefixSumTree() had indexing problem in kvLLRB")
			return
		}
		childNode := childNodeAsValue.(*btreeNodeStruct)

		childNode.prefixSumItems = childNode.items
		childNode.prefixSumKVIndex = i
		childNode.prefixSumParent = nil
		childNode.prefixSumLeftChild = nil
		childNode.prefixSumRightChild = nil

		prefixSumSlice[i+1] = childNode
	}

	node.rootPrefixSumChild = tree.arrangePrefixSumTreeRecursively(prefixSumSlice)
	node.items = node.rootPrefixSumChild.prefixSumItems

	err = nil
	return
}

func (tree *btreeTreeStruct) rearrangePrefixSumTreeToRoot(node *btreeNodeStruct) {
	tree.arrangePrefixSumTree(node)

	tree.markNodeDirty(node)

	if !node.root {
		tree.rearrangePrefixSumTreeToRoot(node.parentNode)
	}
}

func (tree *btreeTreeStruct) updatePrefixSumTreeLeafToRootRecursively(updatedChildNode *btreeNodeStruct, delta int) {
	if delta < 0 {
		updatedChildNode.items -= uint64(-delta)
	} else {
		updatedChildNode.items += uint64(delta)
	}

	tree.markNodeDirty(updatedChildNode)

	if updatedChildNode.root {
		return
	}

	prefixSumNode := updatedChildNode

	for {
		if delta < 0 {
			prefixSumNode.prefixSumItems -= uint64(-delta)
		} else {
			prefixSumNode.prefixSumItems += uint64(delta)
		}

		if nil == prefixSumNode.prefixSumParent {
			break
		} else {
			prefixSumNode = prefixSumNode.prefixSumParent
		}
	}

	tree.updatePrefixSumTreeLeafToRootRecursively(updatedChildNode.parentNode, delta)
}

func (tree *btreeTreeStruct) updatePrefixSumTreeLeafToRoot(leafNode *btreeNodeStruct) (err error) {
	if !leafNode.leaf {
		err = fmt.Errorf("Logic error: updatePrefixSumTreeToRoot called for non-leaf node")
		return
	}

	llrbLen, err := leafNode.kvLLRB.Len()
	if nil != err {
		return
	}

	delta := llrbLen - int(leafNode.items)
	if delta == 0 {
		return
	}

	tree.updatePrefixSumTreeLeafToRootRecursively(leafNode, delta)

	err = nil
	return
}

func (tree *btreeTreeStruct) loadNode(node *btreeNodeStruct) (err error) {
	nodeByteSlice, err := tree.BPlusTreeCallbacks.GetNode(node.objectNumber, node.objectOffset, node.objectLength)
	if nil != err {
		return
	}

	node.kvLLRB = NewLLRBTree(node.tree.Compare, node.tree.BPlusTreeCallbacks)

	var onDiskNode onDiskNodeStruct

	_, err = cstruct.Unpack(nodeByteSlice, &onDiskNode, OnDiskByteOrder)
	if nil != err {
		return
	}

	node.items = onDiskNode.Items
	node.root = onDiskNode.Root
	node.leaf = onDiskNode.Leaf

	payload := onDiskNode.Payload

	if node.root {
		var maxKeysPerNodeStruct onDiskUint64Struct

		bytesConsumed, unpackErr := cstruct.Unpack(payload, &maxKeysPerNodeStruct, OnDiskByteOrder)
		if nil != unpackErr {
			err = unpackErr
			return
		}

		payload = payload[bytesConsumed:]

		tree.minKeysPerNode = maxKeysPerNodeStruct.U64 >> 1
		tree.maxKeysPerNode = maxKeysPerNodeStruct.U64
	}

	if node.leaf {
		var numKeysStruct onDiskUint64Struct

		bytesConsumed, unpackErr := cstruct.Unpack(payload, &numKeysStruct, OnDiskByteOrder)
		if nil != unpackErr {
			err = unpackErr
			return
		}

		payload = payload[bytesConsumed:]
		for i := uint64(0); i < numKeysStruct.U64; i++ {
			key, bytesConsumed, unpackKeyErr := tree.BPlusTreeCallbacks.UnpackKey(payload)
			if nil != unpackKeyErr {
				err = unpackKeyErr
				return
			}
			payload = payload[bytesConsumed:]
			value, bytesConsumed, unpackValueErr := tree.BPlusTreeCallbacks.UnpackValue(payload)
			if nil != unpackValueErr {
				err = unpackValueErr
				return
			}
			payload = payload[bytesConsumed:]

			ok, nonShadowingErr := node.kvLLRB.Put(key, value)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: loadNode() call to Put() should have worked")
				return
			}
		}

		node.rootPrefixSumChild = nil
	} else {
		var numChildrenStruct onDiskUint64Struct

		bytesConsumed, unpackErr := cstruct.Unpack(payload, &numChildrenStruct, OnDiskByteOrder)
		if nil != unpackErr {
			err = unpackErr
			return
		}

		payload = payload[bytesConsumed:]

		if 0 == numChildrenStruct.U64 {
			node.nonLeafLeftChild = nil
		} else {
			var onDiskReferenceToNode onDiskReferenceToNodeStruct

			bytesConsumed, unpackErr := cstruct.Unpack(payload, &onDiskReferenceToNode, OnDiskByteOrder)
			if nil != unpackErr {
				err = unpackErr
				return
			}

			payload = payload[bytesConsumed:]

			childNode := &btreeNodeStruct{
				objectNumber: onDiskReferenceToNode.ObjectNumber,
				objectOffset: onDiskReferenceToNode.ObjectOffset,
				objectLength: onDiskReferenceToNode.ObjectLength,
				items:        onDiskReferenceToNode.Items,
				loaded:       false,
				tree:         node.tree,
				parentNode:   node,
				kvLLRB:       nil,
			}

			node.nonLeafLeftChild = childNode

			for i := uint64(1); i < numChildrenStruct.U64; i++ {
				key, bytesConsumed, unpackKeyErr := node.tree.BPlusTreeCallbacks.UnpackKey(payload)
				if nil != unpackKeyErr {
					err = unpackKeyErr
					return
				}

				payload = payload[bytesConsumed:]

				bytesConsumed, unpackErr = cstruct.Unpack(payload, &onDiskReferenceToNode, OnDiskByteOrder)
				if nil != unpackErr {
					err = unpackErr
					return
				}

				payload = payload[bytesConsumed:]

				childNode := &btreeNodeStruct{
					objectNumber: onDiskReferenceToNode.ObjectNumber,
					objectOffset: onDiskReferenceToNode.ObjectOffset,
					objectLength: onDiskReferenceToNode.ObjectLength,
					items:        onDiskReferenceToNode.Items,
					loaded:       false,
					tree:         node.tree,
					parentNode:   node,
					kvLLRB:       nil,
				}

				node.kvLLRB.Put(key, childNode)
			}

			tree.arrangePrefixSumTree(node)
		}
	}

	if 0 != len(payload) {
		err = fmt.Errorf("Logic error: load() should have exhausted payload")
		return
	}

	node.loaded = true
	node.dirty = false

	err = nil
	return
}

func (tree *btreeTreeStruct) postNode(node *btreeNodeStruct) (err error) {
	if !node.dirty {
		err = nil
		return
	}

	onDiskNode := onDiskNodeStruct{
		Items:   node.items,
		Root:    node.root,
		Leaf:    node.leaf,
		Payload: []byte{},
	}

	if node.root {
		maxKeysPerNodeStruct := onDiskUint64Struct{U64: tree.maxKeysPerNode}

		maxKeysPerNodeBuf, packErr := cstruct.Pack(maxKeysPerNodeStruct, OnDiskByteOrder)
		if nil != packErr {
			err = packErr
			return
		}

		onDiskNode.Payload = append(onDiskNode.Payload, maxKeysPerNodeBuf...)
	}

	if node.leaf {
		kvLLRBLen, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		kvLLRBLenStruct := onDiskUint64Struct{U64: uint64(kvLLRBLen)}

		kvLLRBLenBuf, packErr := cstruct.Pack(kvLLRBLenStruct, OnDiskByteOrder)
		if nil != packErr {
			err = packErr
			return
		}

		onDiskNode.Payload = append(onDiskNode.Payload, kvLLRBLenBuf...)

		for i := 0; i < kvLLRBLen; i++ {
			key, value, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: postNode() call to GetByIndex() should have worked")
				return
			}

			packedKey, packKeyErr := tree.BPlusTreeCallbacks.PackKey(key)
			if nil != packKeyErr {
				err = packKeyErr
				return
			}
			onDiskNode.Payload = append(onDiskNode.Payload, packedKey...)
			packedValue, packValueErr := tree.BPlusTreeCallbacks.PackValue(value)
			if nil != packValueErr {
				err = packValueErr
				return
			}
			onDiskNode.Payload = append(onDiskNode.Payload, packedValue...)
		}
	} else {
		var numChildren int

		llrbLen, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		if nil == node.nonLeafLeftChild {
			numChildren = 0

			if 0 != llrbLen {
				err = fmt.Errorf("Logic error: postNode() found no nonLeafLeftChild but elements in kvLLRB")
				return
			}
		} else {
			numChildren = 1 + llrbLen
		}

		numChildrenStruct := onDiskUint64Struct{U64: uint64(numChildren)}

		numChildrenBuf, packErr := cstruct.Pack(numChildrenStruct, OnDiskByteOrder)
		if nil != packErr {
			err = packErr
			return
		}

		onDiskNode.Payload = append(onDiskNode.Payload, numChildrenBuf...)

		var onDiskReferenceToNode onDiskReferenceToNodeStruct

		for i := 0; i < numChildren; i++ {
			if 0 == i {
				if node.nonLeafLeftChild.dirty {
					err = fmt.Errorf("Logic error: postNode() found nonLeafLeftChild dirty")
					return
				}

				onDiskReferenceToNode.ObjectNumber = node.nonLeafLeftChild.objectNumber
				onDiskReferenceToNode.ObjectOffset = node.nonLeafLeftChild.objectOffset
				onDiskReferenceToNode.ObjectLength = node.nonLeafLeftChild.objectLength
				onDiskReferenceToNode.Items = node.nonLeafLeftChild.items

				onDiskReferenceToNodeBuf, packErr := cstruct.Pack(onDiskReferenceToNode, OnDiskByteOrder)
				if nil != packErr {
					err = packErr
					return
				}

				onDiskNode.Payload = append(onDiskNode.Payload, onDiskReferenceToNodeBuf...)
			} else {
				key, value, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i - 1)
				if nil != nonShadowingErr {
					err = nonShadowingErr
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: postNode() call to GetByIndex() should have worked")
					return
				}

				packedKey, packKeyErr := tree.BPlusTreeCallbacks.PackKey(key)
				if nil != packKeyErr {
					err = packKeyErr
					return
				}
				onDiskNode.Payload = append(onDiskNode.Payload, packedKey...)

				childNode := value.(*btreeNodeStruct)

				if childNode.dirty {
					err = fmt.Errorf("Logic error: postNode() found childNode dirty")
					return
				}

				onDiskReferenceToNode.ObjectNumber = childNode.objectNumber
				onDiskReferenceToNode.ObjectOffset = childNode.objectOffset
				onDiskReferenceToNode.ObjectLength = childNode.objectLength
				onDiskReferenceToNode.Items = childNode.items

				onDiskReferenceToNodeBuf, packErr := cstruct.Pack(onDiskReferenceToNode, OnDiskByteOrder)
				if nil != packErr {
					err = packErr
					return
				}

				onDiskNode.Payload = append(onDiskNode.Payload, onDiskReferenceToNodeBuf...)
			}
		}
	}

	onDiskNodeBuf, err := cstruct.Pack(onDiskNode, OnDiskByteOrder)
	if nil != err {
		return
	}

	objectNumber, objectOffset, err := tree.BPlusTreeCallbacks.PutNode(onDiskNodeBuf)
	if nil != err {
		return
	}

	node.objectNumber = objectNumber
	node.objectOffset = objectOffset
	node.objectLength = uint64(len(onDiskNodeBuf))

	node.dirty = false

	err = nil
	return
}

func (tree *btreeTreeStruct) updateLayoutReport(layoutReport LayoutReport, node *btreeNodeStruct) (err error) {
	wasLoaded := node.loaded

	if !wasLoaded {
		err = tree.loadNode(node)
		if nil != err {
			return
		}
	}

	if 0 != node.objectNumber {
		prevObjectBytes, ok := layoutReport[node.objectNumber]
		if !ok {
			prevObjectBytes = 0
		}
		layoutReport[node.objectNumber] = prevObjectBytes + node.objectLength
	}

	if !node.leaf {
		if nil == node.nonLeafLeftChild {
			err = fmt.Errorf("Logic error: non-Leaf node found to not have a nonLeafLeftChild")
			return
		}

		err = tree.updateLayoutReport(layoutReport, node.nonLeafLeftChild)
		if nil != err {
			return
		}

		llrbLen, nonShadowingErr := node.kvLLRB.Len()
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		for i := 0; i < llrbLen; i++ {
			_, childNodeAsValue, ok, nonShadowingErr := node.kvLLRB.GetByIndex(i)
			if nil != nonShadowingErr {
				err = nonShadowingErr
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: childNode lookup by index not found")
				return
			}
			childNode := childNodeAsValue.(*btreeNodeStruct)
			err = tree.updateLayoutReport(layoutReport, childNode)
			if nil != err {
				return
			}
		}
	}

	if !wasLoaded {
		err = tree.purgeNode(node, true)
		if nil != err {
			return
		}
	}

	err = nil
	return
}
