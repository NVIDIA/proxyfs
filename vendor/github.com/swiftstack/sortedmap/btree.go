package sortedmap

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/swiftstack/cstruct"
)

type btreeNodeCacheTag uint32

const (
	noLRU btreeNodeCacheTag = iota // must be zero
	cleanLRU
	dirtyLRU
)

type btreeNodeCacheElement struct { //    only accessed while holding btreeNodeCacheStruct.Mutex
	btreeNodeCacheTag                  // default value of zero indicates not on either cleanLRU or dirtyLRU
	nextBTreeNode     *btreeNodeStruct // nil if at tail of LRU
	prevBTreeNode     *btreeNodeStruct // nil if at head of LRU
}

type btreeNodeCacheStruct struct {
	sync.Mutex //            protects both this btreeNodeCacheStruct & every btreeNodeCacheElement
	//                         {clean|dirty}LRUs are populated while holding a btreeTreeStruct.Mutex
	//                         {clean|dirty}LRUs are drained in a separate goroutine to avoid deadlock
	evictLowLimit  uint64 // evictions continue until evictLowLimit  >= cleanLRUItems + dirtyLRUItems
	evictHighLimit uint64 // evictions begin    when  evictHighLimit <  cleanLRUItems + dirtyLRUItems
	cleanLRUHead   *btreeNodeStruct
	cleanLRUTail   *btreeNodeStruct
	cleanLRUItems  uint64
	dirtyLRUHead   *btreeNodeStruct
	dirtyLRUTail   *btreeNodeStruct
	dirtyLRUItems  uint64
	drainerActive  bool // if true, btreeNodeCacheDrainer() is already attempting to evict cleanLRU elements
}

type btreeNodeStruct struct {
	btreeNodeCacheElement
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

type staleOnDiskReferenceStruct struct {
	objectNumber uint64
	objectOffset uint64
	objectLength uint64
}

type btreeTreeStruct struct {
	sync.Mutex
	minKeysPerNode uint64 //                           only applies to non-Root nodes
	//                                                 "order" according to Bayer & McCreight (1972) & Comer (1979)
	maxKeysPerNode uint64 //                           "order" according to Knuth (1998)
	Compare
	BPlusTreeCallbacks
	root                      *btreeNodeStruct                        // should never be nil
	staleOnDiskReferencesList map[staleOnDiskReferenceStruct]struct{} // previously posted node locations (staleOnDiskReferenceStruct) yet to be discarded
	nodeCache                 *btreeNodeCacheStruct                   // likely shared with other btreeTreeStruct's
}

// API functions (see api.go)

func (tree *btreeTreeStruct) BisectLeft(key Key) (index int, found bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root
	indexDelta := uint64(0)

	for {
		if node.loaded {
			tree.markNodeUsed(node)
		} else {
			err = tree.loadNode(node) // will also mark node clean/used in LRU
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
		if node.loaded {
			tree.markNodeUsed(node)
		} else {
			err = tree.loadNode(node) // will also mark node clean/used in LRU
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
	var (
		leftChildPrefixSumItems uint64
	)

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
		if node.loaded {
			tree.markNodeUsed(node)
		} else {
			err = tree.loadNode(node) // will also mark node clean/used in LRU
			if nil != err {
				return
			}
		}

		if node.leaf {
			_, err = node.kvLLRB.DeleteByIndex(int(netIndex))
			if nil != err {
				return
			}
			tree.markNodeDirty(node)
			tree.updatePrefixSumTreeLeafToRoot(node)
			err = tree.rebalanceHere(node, parentIndexStack) // will also mark affected nodes dirty/used in LRU
			if nil != err {
				return
			}
			ok = true
			err = nil
			return
		}

		node = node.rootPrefixSumChild

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
		if node.loaded {
			tree.markNodeUsed(node)
		} else {
			err = tree.loadNode(node) // will also mark node clean/used in LRU
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
				tree.markNodeDirty(node)
				tree.updatePrefixSumTreeLeafToRoot(node)
				err = tree.rebalanceHere(node, parentIndexStack) // will also mark affected nodes dirty/used in LRU
				if nil != err {
					return
				}
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
	var (
		leftChildPrefixSumItems uint64
	)

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
		if node.loaded {
			tree.markNodeUsed(node)
		} else {
			err = tree.loadNode(node) // will also mark node clean/used in LRU
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
		if node.loaded {
			tree.markNodeUsed(node)
		} else {
			err = tree.loadNode(node) // will also mark node clean/used in LRU
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
	var (
		leftChildPrefixSumItems uint64
	)

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
		if node.loaded {
			tree.markNodeUsed(node)
		} else {
			err = tree.loadNode(node) // will also mark node clean/used in LRU
			if nil != err {
				return
			}
		}

		if node.leaf {
			tree.touchLoadedNodeToRoot(node) // will also mark node dirty/used in LRU
			_, err = node.kvLLRB.PatchByIndex(int(netIndex), value)
			ok = true
			return
		}

		node = node.rootPrefixSumChild

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
			err = tree.loadNode(node) // will also mark node clean/used in LRU
			if nil != err {
				return
			}
		}

		if node.leaf {
			tree.touchLoadedNodeToRoot(node) // will also mark node dirty/used in LRU
			ok, err = node.kvLLRB.PatchByKey(key, value)
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

func (tree *btreeTreeStruct) Put(key Key, value Value) (ok bool, err error) {
	tree.Lock()
	defer tree.Unlock()

	node := tree.root

	for {
		if !node.loaded {
			err = tree.loadNode(node) // will also mark node clean/used in LRU
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
				err = tree.insertHere(node, key, value) // will also mark affected nodes dirty/used in LRU
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

func (tree *btreeTreeStruct) FetchLocation() (rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64) {
	rootObjectNumber = tree.root.objectNumber
	rootObjectOffset = tree.root.objectOffset
	rootObjectLength = tree.root.objectLength

	return
}

func (tree *btreeTreeStruct) FetchLayoutReport() (layoutReport LayoutReport, err error) {
	tree.Lock()
	defer tree.Unlock()

	layoutReport = make(map[uint64]uint64)

	err = tree.updateLayoutReport(layoutReport, tree.root)

	return
}

func (tree *btreeTreeStruct) Flush(andPurge bool) (rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64, err error) {
	tree.Lock()
	defer tree.Unlock()

	// First flush (and optionally purge) B+Tree

	err = tree.flushNode(tree.root, andPurge) // will also mark node clean/used or evicted in LRU
	if nil != err {
		return
	}

	rootObjectNumber = tree.root.objectNumber
	rootObjectOffset = tree.root.objectOffset
	rootObjectLength = tree.root.objectLength

	// All done

	err = nil
	return
}

func (tree *btreeTreeStruct) Purge(full bool) (err error) {
	tree.Lock()
	defer tree.Unlock()

	err = tree.purgeNode(tree.root, full) // will also mark node evicted in LRU

	return
}

func (tree *btreeTreeStruct) Touch() (err error) {
	tree.Lock()
	defer tree.Unlock()

	err = tree.touchNode(tree.root) // will also mark node dirty/used in LRU

	return
}

func (tree *btreeTreeStruct) TouchItem(thisItemIndexToTouch uint64) (nextItemIndexToTouch uint64, err error) {
	var (
		leftChildPrefixSumItems uint64
	)

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
			err = tree.loadNode(node) // will also mark node clean/used in LRU
			if nil != err {
				// Upon detected corruption, just return
				nextItemIndexToTouch = 0
				return
			}
		}

		if node.leaf {
			// Touch this node up to root

			tree.touchLoadedNodeToRoot(node) // will also mark node dirty/used in LRU

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
	tree.Lock()
	err = tree.pruneWhileLocked()
	tree.Unlock()
	return
}

func (tree *btreeTreeStruct) Discard() (err error) {
	tree.Lock()
	defer tree.Unlock()

	// Discard every node in tree

	tree.discardNode(tree.root)

	// Prune again to pick up now stale nodes added in discardNode()

	err = tree.pruneWhileLocked()
	if nil != err {
		return
	}

	// Reset btreeTreeStruct to trigger Golang Garbage Collection now (and prevent further use)

	tree.Compare = nil
	tree.BPlusTreeCallbacks = nil
	tree.root = nil
	tree.staleOnDiskReferencesList = nil
	tree.nodeCache = nil

	// All done

	err = nil
	return
}

// Helper functions

func (tree *btreeTreeStruct) pruneWhileLocked() (err error) {
	var (
		staleOnDiskReference staleOnDiskReferenceStruct
	)

	// Discard all stale OnDisk node references

	if nil != tree.staleOnDiskReferencesList {
		for staleOnDiskReference = range tree.staleOnDiskReferencesList {
			err = tree.BPlusTreeCallbacks.DiscardNode(staleOnDiskReference.objectNumber, staleOnDiskReference.objectOffset, staleOnDiskReference.objectLength)
			if nil != err {
				return
			}
		}

		tree.staleOnDiskReferencesList = nil
	}

	// All done

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
				err = fmt.Errorf("Logic error: discardNode() had indexing problem in kvLLRB")
				return
			}
			childNode := childNodeAsValue.(*btreeNodeStruct)

			err = tree.discardNode(childNode)
			if nil != err {
				return
			}
		}
	}

	tree.markNodeToBeDiscarded(node)

	err = nil
	return
}

func (tree *btreeTreeStruct) insertHere(insertNode *btreeNodeStruct, key Key, value Value) (err error) {
	var (
		llrbLen             int
		newRightSiblingNode *btreeNodeStruct
		ok                  bool
		splitKey            Key
		splitValue          Value
		splitValueAsNode    *btreeNodeStruct
	)

	insertNode.kvLLRB.Put(key, value)

	if insertNode.leaf {
		tree.updatePrefixSumTreeLeafToRoot(insertNode) // will also mark affected nodes dirty/used in LRU
	}

	llrbLen, err = insertNode.kvLLRB.Len()
	if nil != err {
		return
	}

	if tree.maxKeysPerNode < uint64(llrbLen) {
		newRightSiblingNode = &btreeNodeStruct{
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

		for {
			llrbLen, err = insertNode.kvLLRB.Len()
			if nil != err {
				return
			}
			if tree.minKeysPerNode >= uint64(llrbLen) {
				break
			}
			splitKey, splitValue, ok, err = insertNode.kvLLRB.GetByIndex(llrbLen - 1)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: insertHere() failed to fetch insertNode's splitKey:splitValue")
				return
			}
			ok, err = insertNode.kvLLRB.DeleteByIndex(llrbLen - 1)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: insertHere() failed to delete insertNode's splitKey")
				return
			}
			ok, err = newRightSiblingNode.kvLLRB.Put(splitKey, splitValue)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: insertHere() failed to put newRightSiblingNode's splitKey:splitValue")
				return
			}

			if insertNode.leaf {
				insertNode.items--
				newRightSiblingNode.items++
			} else {
				splitValueAsNode = splitValue.(*btreeNodeStruct)
				insertNode.items -= splitValueAsNode.items
				newRightSiblingNode.items += splitValueAsNode.items
				splitValueAsNode.parentNode = newRightSiblingNode
			}
		}

		if !insertNode.leaf {
			llrbLen, err = insertNode.kvLLRB.Len()
			if nil != err {
				return
			}
			splitKey, splitValue, ok, err = insertNode.kvLLRB.GetByIndex(llrbLen - 1)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: insertHere() failed to fetch insertNode's splitKey:splitValue")
				return
			}
			ok, err = insertNode.kvLLRB.DeleteByIndex(llrbLen - 1)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: insertHere() failed to delete insertNode's splitKey")
				return
			}
			newRightSiblingNode.nonLeafLeftChild = splitValue.(*btreeNodeStruct)

			insertNode.items -= newRightSiblingNode.nonLeafLeftChild.items
			newRightSiblingNode.items += newRightSiblingNode.nonLeafLeftChild.items
			newRightSiblingNode.nonLeafLeftChild.parentNode = newRightSiblingNode

			err = tree.arrangePrefixSumTree(insertNode)
			if nil != err {
				return
			}
			err = tree.arrangePrefixSumTree(newRightSiblingNode)
			if nil != err {
				return
			}
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

			tree.initNodeAsEvicted(newRightSiblingNode)
			tree.markNodeDirty(newRightSiblingNode)

			tree.initNodeAsEvicted(tree.root)
			tree.markNodeDirty(tree.root)
		} else {
			err = tree.insertHere(insertNode.parentNode, splitKey, newRightSiblingNode)
			if nil != err {
				return
			}

			tree.initNodeAsEvicted(newRightSiblingNode)
			tree.markNodeDirty(newRightSiblingNode)
		}

		err = tree.rearrangePrefixSumTreeToRoot(insertNode.parentNode)
		if nil != err {
			return
		}
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) rebalanceHere(rebalanceNode *btreeNodeStruct, parentIndexStack []int) (err error) {
	var (
		i                                          int
		leftSiblingNode                            *btreeNodeStruct
		leftSiblingNodeAsValue                     Value
		leftSiblingNodeKVIndex                     int
		llrbLen                                    int
		movedKeyFromParent                         Key
		movedKeyFromSibling                        Key
		movedNodeFromSibling                       *btreeNodeStruct
		movedValueFromSibling                      Value
		newRightSiblingNodeNonLeafLeftChildAsValue Value
		newSplitKey                                Key
		numItemsToMove                             int
		ok                                         bool
		oldSplitKey                                Key
		parentIndexStackPruned                     []int
		parentIndexStackTailIndex                  int
		parentNode                                 *btreeNodeStruct
		parentNodeIndex                            int
		rightSiblingNode                           *btreeNodeStruct
		rightSiblingNodeAsValue                    Value
	)

	if rebalanceNode.root {
		err = nil
		return
	}

	llrbLen, err = rebalanceNode.kvLLRB.Len()
	if nil != err {
		return
	}

	if uint64(llrbLen) >= tree.minKeysPerNode {
		err = nil
		return
	}

	parentNode = rebalanceNode.parentNode

	parentIndexStackTailIndex = len(parentIndexStack) - 1
	parentNodeIndex = parentIndexStack[parentIndexStackTailIndex]
	parentIndexStackPruned = parentIndexStack[:parentIndexStackTailIndex]

	if -1 == parentNodeIndex {
		leftSiblingNode = nil
	} else {
		if 0 == parentNodeIndex {
			leftSiblingNode = parentNode.nonLeafLeftChild
		} else {
			_, leftSiblingNodeAsValue, ok, err = parentNode.kvLLRB.GetByIndex(parentNodeIndex - 1)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch leftSiblingNode")
				return
			}
			leftSiblingNode = leftSiblingNodeAsValue.(*btreeNodeStruct)
		}

		if leftSiblingNode.loaded {
			tree.markNodeUsed(leftSiblingNode)
		} else {
			err = tree.loadNode(leftSiblingNode) // will also mark leftSiblingNode clean/used in LRU
			if nil != err {
				return
			}
		}

		llrbLen, err = leftSiblingNode.kvLLRB.Len()
		if nil != err {
			return
		}

		if uint64(llrbLen) > tree.minKeysPerNode {
			// leftSiblingNode can give up a key

			leftSiblingNode.items--
			rebalanceNode.items++

			if rebalanceNode.leaf {
				// move one key from leftSiblingNode to rebalanceNode and update splitKey in parentNode

				leftSiblingNodeKVIndex = llrbLen - 1
				movedKeyFromSibling, movedValueFromSibling, ok, err = leftSiblingNode.kvLLRB.GetByIndex(leftSiblingNodeKVIndex)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch leftSiblingNode's movedKeyFromSibling:movedValueFromSibling")
					return
				}
				ok, err = leftSiblingNode.kvLLRB.DeleteByIndex(leftSiblingNodeKVIndex)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete leftSiblingNode's movedKeyFromSibling")
					return
				}
				ok, err = rebalanceNode.kvLLRB.Put(movedKeyFromSibling, movedValueFromSibling)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to put leftSiblingNode's movedKeyFromSibling:movedValueFromSibling")
					return
				}
				ok, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete parentNode's oldSplitKey")
					return
				}
				ok, err = parentNode.kvLLRB.Put(movedKeyFromSibling, rebalanceNode)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete parentNode's movedKeyFromSibling:rebalanceNode")
					return
				}
			} else {
				// rotate one key from leftSiblingNode to parentNode & one key from parentNode to rebalanceNode

				leftSiblingNodeKVIndex = llrbLen - 1
				movedKeyFromSibling, movedValueFromSibling, ok, err = leftSiblingNode.kvLLRB.GetByIndex(leftSiblingNodeKVIndex)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch leftSiblingNode's movedKeyFromSibling:movedValueFromSibling")
					return
				}
				movedNodeFromSibling = movedValueFromSibling.(*btreeNodeStruct)
				ok, err = leftSiblingNode.kvLLRB.DeleteByIndex(leftSiblingNodeKVIndex)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete leftSiblingNode's movedKeyFromSibling")
					return
				}
				movedKeyFromParent, _, ok, err = parentNode.kvLLRB.GetByIndex(parentNodeIndex)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch parentNode's movedKeyFromParent")
					return
				}
				ok, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete parentNode's movedKeyFromParent")
					return
				}
				ok, err = rebalanceNode.kvLLRB.Put(movedKeyFromParent, rebalanceNode.nonLeafLeftChild)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to put rebalanceNode's movedKeyFromParent:rebalanceNode.nonLeafLeftChild")
					return
				}
				movedNodeFromSibling.parentNode = rebalanceNode
				rebalanceNode.nonLeafLeftChild = movedNodeFromSibling
				ok, err = parentNode.kvLLRB.Put(movedKeyFromSibling, rebalanceNode)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to put parentNode's movedKeyFromSibling:rebalanceNode")
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
		_, rightSiblingNodeAsValue, ok, err = parentNode.kvLLRB.GetByIndex(parentNodeIndex + 1)
		if nil != err {
			return
		}
		if !ok {
			err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch rightSiblingNode")
			return
		}
		rightSiblingNode = rightSiblingNodeAsValue.(*btreeNodeStruct)

		if rightSiblingNode.loaded {
			tree.markNodeUsed(rightSiblingNode)
		} else {
			err = tree.loadNode(rightSiblingNode) // will also mark rightSiblingNode clean/used in LRU
			if nil != err {
				return
			}
		}

		llrbLen, err = rightSiblingNode.kvLLRB.Len()
		if nil != err {
			return
		}

		if uint64(llrbLen) > tree.minKeysPerNode {
			// rightSiblingNode can give up a key

			rebalanceNode.items++
			rightSiblingNode.items--

			if rebalanceNode.leaf {
				// move one key from rightSiblingNode to rebalanceNode and update splitKey in parentNode

				movedKeyFromSibling, movedValueFromSibling, ok, err = rightSiblingNode.kvLLRB.GetByIndex(0)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch rightSiblingNode's movedKeyFromSibling:movedValueFromSibling")
					return
				}
				ok, err = rightSiblingNode.kvLLRB.DeleteByIndex(0)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete rightSiblingNode's movedKeyFromSibling")
					return
				}
				ok, err = rebalanceNode.kvLLRB.Put(movedKeyFromSibling, movedValueFromSibling)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to put rebalanceNode's movedKeyFromSibling:movedValueFromSibling")
					return
				}
				newSplitKey, _, ok, err = rightSiblingNode.kvLLRB.GetByIndex(0)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch newSplitKey from rightSiblingNode")
					return
				}
				ok, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex + 1)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete parentNode's oldSplitKey")
					return
				}
				ok, err = parentNode.kvLLRB.Put(newSplitKey, rightSiblingNode)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete parentNode's newSplitKey:rightSiblingNode")
					return
				}
			} else {
				// rotate one key from rightSiblingNode to parentNode & one key from parentNode to rebalanceNode

				newSplitKey, newRightSiblingNodeNonLeafLeftChildAsValue, ok, err = rightSiblingNode.kvLLRB.GetByIndex(0)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch rightSiblingNode's newSplitKey:newRightSiblingNodeNonLeafLeftChildAsValue")
					return
				}
				ok, err = rightSiblingNode.kvLLRB.DeleteByIndex(0)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete rightSiblingNode's kvLLRB[0]")
					return
				}
				movedKeyFromParent, _, ok, err = parentNode.kvLLRB.GetByIndex(parentNodeIndex + 1)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch movedKeyFromParent from parentNode")
					return
				}
				ok, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex + 1)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to delete parentNode's movedKeyFromParent")
					return
				}
				movedNodeFromSibling = rightSiblingNode.nonLeafLeftChild
				rightSiblingNode.nonLeafLeftChild = newRightSiblingNodeNonLeafLeftChildAsValue.(*btreeNodeStruct)
				movedNodeFromSibling.parentNode = rebalanceNode
				ok, err = rebalanceNode.kvLLRB.Put(movedKeyFromParent, movedNodeFromSibling)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to put rebalanceNode's movedKeyFromParent:movedNodeFromSibling")
					return
				}
				ok, err = parentNode.kvLLRB.Put(newSplitKey, rightSiblingNode)
				if nil != err {
					return
				}
				if !ok {
					err = fmt.Errorf("Logic error: rebalanceHere() failed to put parentNode's newSplitKey:rightSiblingNode")
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

		oldSplitKey, _, ok, err = parentNode.kvLLRB.GetByIndex(parentNodeIndex)
		if nil != err {
			return
		}
		if !ok {
			err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch oldSplitKey from parentNode")
			return
		}
		if !rebalanceNode.leaf {
			rebalanceNode.nonLeafLeftChild.parentNode = leftSiblingNode
			leftSiblingNode.kvLLRB.Put(oldSplitKey, rebalanceNode.nonLeafLeftChild)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to put leftSiblingNode's oldSplitKey:rebalanceNode.nonLeafLeftChild")
				return
			}
		}
		numItemsToMove, err = rebalanceNode.kvLLRB.Len()
		if nil != err {
			return
		}
		for i = 0; i < numItemsToMove; i++ {
			movedKeyFromSibling, movedValueFromSibling, ok, err = rebalanceNode.kvLLRB.GetByIndex(i)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch rebalanceNode's movedKeyFromSibling:movedValueFromSibling")
				return
			}
			movedNodeFromSibling, ok = movedValueFromSibling.(*btreeNodeStruct)
			if ok {
				movedNodeFromSibling.parentNode = leftSiblingNode
			}
			ok, err = leftSiblingNode.kvLLRB.Put(movedKeyFromSibling, movedValueFromSibling)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to put leftSiblingNode's movedKeyFromSibling:movedValueFromSibling")
				return
			}
		}

		llrbLen, err = parentNode.kvLLRB.Len()
		if nil != err {
			return
		}

		if parentNode.root && (1 == llrbLen) {
			// height will reduce by one, so make leftSiblingNode the new root

			tree.markNodeToBeDiscarded(tree.root)

			leftSiblingNode.root = true
			leftSiblingNode.parentNode = nil
			tree.root = leftSiblingNode

			if !leftSiblingNode.leaf {
				tree.arrangePrefixSumTree(leftSiblingNode)
			}
		} else {
			// height will remain the same (at this recursion level),
			// so just delete oldSplitKey from parentNode and recurse

			ok, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to delete parentNode's oldSplitKey")
				return
			}

			if !leftSiblingNode.leaf {
				tree.arrangePrefixSumTree(leftSiblingNode)
			}

			tree.arrangePrefixSumTree(parentNode)

			err = tree.rebalanceHere(parentNode, parentIndexStackPruned)
			if nil != err {
				return
			}
		}

		tree.markNodeToBeDiscarded(rebalanceNode)
		tree.markNodeDirty(leftSiblingNode)
	} else if nil != rightSiblingNode {
		// move keys from rightSiblingNode to rebalanceNode (along with former splitKey for non-leaf case)

		rebalanceNode.items += rightSiblingNode.items

		oldSplitKey, _, ok, err = parentNode.kvLLRB.GetByIndex(parentNodeIndex + 1)
		if nil != err {
			return
		}
		if !ok {
			err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch oldSplitKey from parentNode")
			return
		}
		if !rebalanceNode.leaf {
			rightSiblingNode.nonLeafLeftChild.parentNode = rebalanceNode
			ok, err = rebalanceNode.kvLLRB.Put(oldSplitKey, rightSiblingNode.nonLeafLeftChild)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to put rebalanceNode's oldSplitKey:rightSiblingNode.nonLeafLeftChild")
				return
			}
		}
		numItemsToMove, err = rightSiblingNode.kvLLRB.Len()
		if nil != err {
			return
		}
		for i = 0; i < numItemsToMove; i++ {
			movedKeyFromSibling, movedValueFromSibling, ok, err = rightSiblingNode.kvLLRB.GetByIndex(i)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to fetch movedKeyFromSibling:movedValueFromSibling from rightSiblingNode")
				return
			}
			movedNodeFromSibling, ok = movedValueFromSibling.(*btreeNodeStruct)
			if ok {
				movedNodeFromSibling.parentNode = rebalanceNode
			}
			ok, err = rebalanceNode.kvLLRB.Put(movedKeyFromSibling, movedValueFromSibling)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to put rebalanceNode's movedKeyFromSibling:movedValueFromSibling")
				return
			}
		}

		llrbLen, err = parentNode.kvLLRB.Len()
		if nil != err {
			return
		}

		if parentNode.root && (1 == llrbLen) {
			// height will reduce by one, so make rebalanceNode the new root

			tree.markNodeToBeDiscarded(tree.root)

			rebalanceNode.root = true
			rebalanceNode.parentNode = nil
			tree.root = rebalanceNode

			if !rebalanceNode.leaf {
				tree.arrangePrefixSumTree(rebalanceNode)
			}
		} else {
			// height will remain the same (at this recursion level),
			// so just delete oldSplitKey from parentNode and recurse

			ok, err = parentNode.kvLLRB.DeleteByIndex(parentNodeIndex + 1)
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("Logic error: rebalanceHere() failed to delete parentNode's oldSplitKey")
				return
			}

			if !rebalanceNode.leaf {
				tree.arrangePrefixSumTree(rebalanceNode)
			}

			tree.arrangePrefixSumTree(parentNode)

			err = tree.rebalanceHere(parentNode, parentIndexStackPruned)
			if nil != err {
				return
			}
		}

		tree.markNodeToBeDiscarded(rightSiblingNode)
		tree.markNodeDirty(rebalanceNode)
	} else {
		// since non-root minKeysPerNode >= 2, this node was required to have had a sibling,
		// so if we reach here, we have a logic problem

		err = fmt.Errorf("Logic error: rebalanceHere() found non-leaf node with no sibling in parentNode.kvLLRB")
		return
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) flushNode(node *btreeNodeStruct, andPurge bool) (err error) {
	if !node.loaded {
		err = nil
		return
	}

	if !node.leaf {
		if nil != node.nonLeafLeftChild {
			err = tree.flushNode(node.nonLeafLeftChild, andPurge)
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

			err = tree.flushNode(childNode, andPurge)
			if nil != err {
				return
			}
		}
	}

	if node.dirty {
		tree.postNode(node) // will also mark node clean/used in LRU
	}

	if andPurge {
		tree.markNodeEvicted(node)

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
		tree.markNodeEvicted(node)

		node.kvLLRB = nil
		node.nonLeafLeftChild = nil
		node.rootPrefixSumChild = nil

		node.loaded = false
	}

	err = nil
	return
}

func (tree *btreeTreeStruct) initNodeAsEvicted(node *btreeNodeStruct) {
	if nil != tree.nodeCache {
		node.btreeNodeCacheElement.btreeNodeCacheTag = noLRU
		node.btreeNodeCacheElement.nextBTreeNode = nil
		node.btreeNodeCacheElement.prevBTreeNode = nil
	}
}

func (tree *btreeTreeStruct) markNodeUsed(node *btreeNodeStruct) {
	if nil != tree.nodeCache {
		tree.nodeCache.Lock()
		switch node.btreeNodeCacheTag {
		case noLRU:
			err := fmt.Errorf("Logic error in markNodeUsed() with node.btreeNodeCacheTag == noLRU (%v)", noLRU)
			panic(err)
		case cleanLRU:
			// Move node to the MRU end of tree.nodeCache's cleanLRU (if necessary)
			if node != tree.nodeCache.cleanLRUTail {
				if node == tree.nodeCache.cleanLRUHead {
					tree.nodeCache.cleanLRUHead = node.nextBTreeNode
					tree.nodeCache.cleanLRUHead.prevBTreeNode = nil

					node.prevBTreeNode = tree.nodeCache.cleanLRUTail
					node.nextBTreeNode = nil

					tree.nodeCache.cleanLRUTail.nextBTreeNode = node
					tree.nodeCache.cleanLRUTail = node
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode

					node.nextBTreeNode = nil
					node.prevBTreeNode = tree.nodeCache.cleanLRUTail

					tree.nodeCache.cleanLRUTail.nextBTreeNode = node
					tree.nodeCache.cleanLRUTail = node
				}
			}
		case dirtyLRU:
			// Move node to the MRU end of tree.nodeCache's dirtyLRU (if necessary)
			if node != tree.nodeCache.dirtyLRUTail {
				if node == tree.nodeCache.dirtyLRUHead {
					tree.nodeCache.dirtyLRUHead = node.nextBTreeNode
					tree.nodeCache.dirtyLRUHead.prevBTreeNode = nil

					node.prevBTreeNode = tree.nodeCache.dirtyLRUTail
					node.nextBTreeNode = nil

					tree.nodeCache.dirtyLRUTail.nextBTreeNode = node
					tree.nodeCache.dirtyLRUTail = node
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode

					node.nextBTreeNode = nil
					node.prevBTreeNode = tree.nodeCache.dirtyLRUTail

					tree.nodeCache.dirtyLRUTail.nextBTreeNode = node
					tree.nodeCache.dirtyLRUTail = node
				}
			}
		}
		tree.nodeCache.Unlock()
	}
}

func (tree *btreeTreeStruct) markNodeClean(node *btreeNodeStruct) {
	node.dirty = false

	if nil != tree.nodeCache {
		tree.nodeCache.Lock()
		switch node.btreeNodeCacheTag {
		case noLRU:
			// Place node at the MRU end of tree.nodeCache's cleanLRU
			if 0 == tree.nodeCache.cleanLRUItems {
				tree.nodeCache.cleanLRUHead = node
				tree.nodeCache.cleanLRUTail = node
				tree.nodeCache.cleanLRUItems = 1

				node.btreeNodeCacheTag = cleanLRU
			} else {
				node.prevBTreeNode = tree.nodeCache.cleanLRUTail
				node.prevBTreeNode.nextBTreeNode = node

				tree.nodeCache.cleanLRUTail = node
				tree.nodeCache.cleanLRUItems++

				node.btreeNodeCacheTag = cleanLRU
			}
		case cleanLRU:
			// Move node to the MRU end of tree.nodeCache's cleanLRU (if necessary)
			if node != tree.nodeCache.cleanLRUTail {
				if node == tree.nodeCache.cleanLRUHead {
					tree.nodeCache.cleanLRUHead = node.nextBTreeNode
					tree.nodeCache.cleanLRUHead.prevBTreeNode = nil

					node.prevBTreeNode = tree.nodeCache.cleanLRUTail
					node.nextBTreeNode = nil

					tree.nodeCache.cleanLRUTail.nextBTreeNode = node
					tree.nodeCache.cleanLRUTail = node
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode

					node.nextBTreeNode = nil
					node.prevBTreeNode = tree.nodeCache.cleanLRUTail

					tree.nodeCache.cleanLRUTail.nextBTreeNode = node
					tree.nodeCache.cleanLRUTail = node
				}
			}
		case dirtyLRU:
			// Move node from dirtyLRU to the MRU end of tree.nodeCache's cleanLRU
			if node == tree.nodeCache.dirtyLRUHead {
				if node == tree.nodeCache.dirtyLRUTail {
					tree.nodeCache.dirtyLRUHead = nil
					tree.nodeCache.dirtyLRUTail = nil
					tree.nodeCache.dirtyLRUItems = 0
				} else {
					tree.nodeCache.dirtyLRUHead = node.nextBTreeNode
					tree.nodeCache.dirtyLRUHead.prevBTreeNode = nil
					tree.nodeCache.dirtyLRUItems--

					node.nextBTreeNode = nil
				}
			} else {
				if node == tree.nodeCache.dirtyLRUTail {
					tree.nodeCache.dirtyLRUTail = node.prevBTreeNode
					tree.nodeCache.dirtyLRUTail.nextBTreeNode = nil
					tree.nodeCache.dirtyLRUItems--
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode
					tree.nodeCache.dirtyLRUItems--

					node.nextBTreeNode = nil
				}
			}

			if 0 == tree.nodeCache.cleanLRUItems {
				node.btreeNodeCacheTag = cleanLRU
				node.prevBTreeNode = nil

				tree.nodeCache.cleanLRUHead = node
				tree.nodeCache.cleanLRUTail = node
				tree.nodeCache.cleanLRUItems = 1
			} else {
				node.btreeNodeCacheTag = cleanLRU
				node.prevBTreeNode = tree.nodeCache.cleanLRUTail

				tree.nodeCache.cleanLRUTail.nextBTreeNode = node
				tree.nodeCache.cleanLRUTail = node
				tree.nodeCache.cleanLRUItems++
			}
		}
		if !tree.nodeCache.drainerActive && (tree.nodeCache.evictHighLimit < (tree.nodeCache.cleanLRUItems + tree.nodeCache.dirtyLRUItems)) {
			tree.nodeCache.drainerActive = true
			go tree.nodeCache.btreeNodeCacheDrainer()
		}
		tree.nodeCache.Unlock()
	}
}

func (tree *btreeTreeStruct) markNodeDirty(node *btreeNodeStruct) {
	node.dirty = true

	tree.placeNodeOnStaleOnDiskReferenceList(node)

	if nil != tree.nodeCache {
		tree.nodeCache.Lock()
		switch node.btreeNodeCacheTag {
		case noLRU:
			// Place node at the MRU end of tree.nodeCache's dirtyLRU
			if 0 == tree.nodeCache.dirtyLRUItems {
				tree.nodeCache.dirtyLRUHead = node
				tree.nodeCache.dirtyLRUTail = node
				tree.nodeCache.dirtyLRUItems = 1

				node.btreeNodeCacheTag = dirtyLRU
			} else {
				node.prevBTreeNode = tree.nodeCache.dirtyLRUTail
				node.prevBTreeNode.nextBTreeNode = node

				tree.nodeCache.dirtyLRUTail = node
				tree.nodeCache.dirtyLRUItems++

				node.btreeNodeCacheTag = dirtyLRU
			}
		case cleanLRU:
			// Move node from cleanLRU to the MRU end of tree.nodeCache's dirtyLRU
			if node == tree.nodeCache.cleanLRUHead {
				if node == tree.nodeCache.cleanLRUTail {
					tree.nodeCache.cleanLRUHead = nil
					tree.nodeCache.cleanLRUTail = nil
					tree.nodeCache.cleanLRUItems = 0
				} else {
					tree.nodeCache.cleanLRUHead = node.nextBTreeNode
					tree.nodeCache.cleanLRUHead.prevBTreeNode = nil
					tree.nodeCache.cleanLRUItems--

					node.nextBTreeNode = nil
				}
			} else {
				if node == tree.nodeCache.cleanLRUTail {
					tree.nodeCache.cleanLRUTail = node.prevBTreeNode
					tree.nodeCache.cleanLRUTail.nextBTreeNode = nil
					tree.nodeCache.cleanLRUItems--
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode
					tree.nodeCache.cleanLRUItems--

					node.nextBTreeNode = nil
				}
			}

			if 0 == tree.nodeCache.dirtyLRUItems {
				node.btreeNodeCacheTag = dirtyLRU
				node.prevBTreeNode = nil

				tree.nodeCache.dirtyLRUHead = node
				tree.nodeCache.dirtyLRUTail = node
				tree.nodeCache.dirtyLRUItems = 1
			} else {
				node.btreeNodeCacheTag = dirtyLRU
				node.prevBTreeNode = tree.nodeCache.dirtyLRUTail

				tree.nodeCache.dirtyLRUTail.nextBTreeNode = node
				tree.nodeCache.dirtyLRUTail = node
				tree.nodeCache.dirtyLRUItems++
			}
		case dirtyLRU:
			// Move node to the MRU end of tree.nodeCache's dirtyLRU (if necessary)
			if node != tree.nodeCache.dirtyLRUTail {
				if node == tree.nodeCache.dirtyLRUHead {
					tree.nodeCache.dirtyLRUHead = node.nextBTreeNode
					tree.nodeCache.dirtyLRUHead.prevBTreeNode = nil

					node.prevBTreeNode = tree.nodeCache.dirtyLRUTail
					node.nextBTreeNode = nil

					tree.nodeCache.dirtyLRUTail.nextBTreeNode = node
					tree.nodeCache.dirtyLRUTail = node
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode

					node.nextBTreeNode = nil
					node.prevBTreeNode = tree.nodeCache.dirtyLRUTail

					tree.nodeCache.dirtyLRUTail.nextBTreeNode = node
					tree.nodeCache.dirtyLRUTail = node
				}
			}
		}
		tree.nodeCache.Unlock()
	}
}

func (tree *btreeTreeStruct) markNodeEvicted(node *btreeNodeStruct) {
	if nil != tree.nodeCache {
		tree.nodeCache.Lock()
		switch node.btreeNodeCacheTag {
		case noLRU:
			err := fmt.Errorf("Logic error in markNodeEvicted() with node.btreeNodeCacheTag == noLRU (%v)", noLRU)
			panic(err)
		case cleanLRU:
			// Remove node from tree.nodeCache's cleanLRU
			if node == tree.nodeCache.cleanLRUHead {
				if node == tree.nodeCache.cleanLRUTail {
					tree.nodeCache.cleanLRUHead = nil
					tree.nodeCache.cleanLRUTail = nil
					tree.nodeCache.cleanLRUItems--

					node.btreeNodeCacheTag = noLRU
				} else {
					tree.nodeCache.cleanLRUHead = node.nextBTreeNode
					tree.nodeCache.cleanLRUHead.prevBTreeNode = nil
					tree.nodeCache.cleanLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.nextBTreeNode = nil
				}
			} else {
				if node == tree.nodeCache.cleanLRUTail {
					tree.nodeCache.cleanLRUTail = node.prevBTreeNode
					tree.nodeCache.cleanLRUTail.nextBTreeNode = nil
					tree.nodeCache.cleanLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.prevBTreeNode = nil
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode
					tree.nodeCache.cleanLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.nextBTreeNode = nil
					node.prevBTreeNode = nil
				}
			}
		case dirtyLRU:
			// Remove node from tree.nodeCache's dirtyLRU
			if node == tree.nodeCache.dirtyLRUHead {
				if node == tree.nodeCache.dirtyLRUTail {
					tree.nodeCache.dirtyLRUHead = nil
					tree.nodeCache.dirtyLRUTail = nil
					tree.nodeCache.dirtyLRUItems--

					node.btreeNodeCacheTag = noLRU
				} else {
					tree.nodeCache.dirtyLRUHead = node.nextBTreeNode
					tree.nodeCache.dirtyLRUHead.prevBTreeNode = nil
					tree.nodeCache.dirtyLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.nextBTreeNode = nil
				}
			} else {
				if node == tree.nodeCache.dirtyLRUTail {
					tree.nodeCache.dirtyLRUTail = node.prevBTreeNode
					tree.nodeCache.dirtyLRUTail.nextBTreeNode = nil
					tree.nodeCache.dirtyLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.prevBTreeNode = nil
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode
					tree.nodeCache.dirtyLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.nextBTreeNode = nil
					node.prevBTreeNode = nil
				}
			}
		}
		tree.nodeCache.Unlock()
	}
}

func (tree *btreeTreeStruct) markNodeToBeDiscarded(node *btreeNodeStruct) {
	tree.placeNodeOnStaleOnDiskReferenceList(node)

	if nil != tree.nodeCache {
		tree.nodeCache.Lock()
		switch node.btreeNodeCacheTag {
		case noLRU:
			err := fmt.Errorf("Logic error in markNodeToBeDiscarded() with node.btreeNodeCacheTag == noLRU (%v)", noLRU)
			panic(err)
		case cleanLRU:
			// Remove node from tree.nodeCache's cleanLRU
			if node == tree.nodeCache.cleanLRUHead {
				if node == tree.nodeCache.cleanLRUTail {
					tree.nodeCache.cleanLRUHead = nil
					tree.nodeCache.cleanLRUTail = nil
					tree.nodeCache.cleanLRUItems--

					node.btreeNodeCacheTag = noLRU
				} else {
					tree.nodeCache.cleanLRUHead = node.nextBTreeNode
					tree.nodeCache.cleanLRUHead.prevBTreeNode = nil
					tree.nodeCache.cleanLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.nextBTreeNode = nil
				}
			} else {
				if node == tree.nodeCache.cleanLRUTail {
					tree.nodeCache.cleanLRUTail = node.prevBTreeNode
					tree.nodeCache.cleanLRUTail.nextBTreeNode = nil
					tree.nodeCache.cleanLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.prevBTreeNode = nil
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode
					tree.nodeCache.cleanLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.nextBTreeNode = nil
					node.prevBTreeNode = nil
				}
			}
		case dirtyLRU:
			// Remove node from tree.nodeCache's dirtyLRU
			if node == tree.nodeCache.dirtyLRUHead {
				if node == tree.nodeCache.dirtyLRUTail {
					tree.nodeCache.dirtyLRUHead = nil
					tree.nodeCache.dirtyLRUTail = nil
					tree.nodeCache.dirtyLRUItems--

					node.btreeNodeCacheTag = noLRU
				} else {
					tree.nodeCache.dirtyLRUHead = node.nextBTreeNode
					tree.nodeCache.dirtyLRUHead.prevBTreeNode = nil
					tree.nodeCache.dirtyLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.nextBTreeNode = nil
				}
			} else {
				if node == tree.nodeCache.dirtyLRUTail {
					tree.nodeCache.dirtyLRUTail = node.prevBTreeNode
					tree.nodeCache.dirtyLRUTail.nextBTreeNode = nil
					tree.nodeCache.dirtyLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.prevBTreeNode = nil
				} else {
					node.prevBTreeNode.nextBTreeNode = node.nextBTreeNode
					node.nextBTreeNode.prevBTreeNode = node.prevBTreeNode
					tree.nodeCache.dirtyLRUItems--

					node.btreeNodeCacheTag = noLRU
					node.nextBTreeNode = nil
					node.prevBTreeNode = nil
				}
			}
		}
		tree.nodeCache.Unlock()
	}
}

func (tree *btreeTreeStruct) placeNodeOnStaleOnDiskReferenceList(node *btreeNodeStruct) {
	if 0 != node.objectLength {
		// Node came from a now-stale copy on disk...
		//   so schedule stale on-disk reference to be reclaimed in a subsequent Prune() call

		if nil == tree.staleOnDiskReferencesList {
			tree.staleOnDiskReferencesList = make(map[staleOnDiskReferenceStruct]struct{})
		}

		staleOnDiskReference := staleOnDiskReferenceStruct{
			objectNumber: node.objectNumber,
			objectOffset: node.objectOffset,
			objectLength: node.objectLength,
		}

		// Duplicate insertions, since staleOnDiskReference is "by value" will be merged

		tree.staleOnDiskReferencesList[staleOnDiskReference] = struct{}{}

		// Zero-out on-disk reference so that the above is only done once for this node

		node.objectNumber = 0
		node.objectOffset = 0
		node.objectLength = 0
	}
}

// btreeNodeCacheDrainer is a goroutine used to reduce the number of clean B+Tree Nodes
// so that the combined number of clean and dirty B+Tree Nodes is at or below the
// evictLowLimit specified in the associated btreeNodeCacheStruct.
//
// Note that the btreeNodeCacheStruct's sync.Mutex is obtained by the above markNode...()
// functions while their callers hold the B+Tree's btreeTreeStruct sync.Mutex. Hence, it
// would be a deadlock-inducing activity to grab these sync.Mutex's in the reverse order.
// Care must be taken to avoid this. The challenge is that a given btreeNodeCacheStruct
// is likely to be shared among multiple btreeTreeStruct's. So processing the cleanLRU
// doubly-linked list, while only holding the btreeNodeCacheStruct's sync.Mutex, will
// ultimately require holding both that sync.Mutex and the associated btreeTreeStruct's
// sync.Mutex in order to "evict" it. This will necessitate the following sequence:
//
//   1 - release the btreeNodeCacheStruct's sync.Mutex
//   2 - obtain the selected btreeNodeStruct's btreeTreeStruct's sync.Mutex
//   3 - verifying the selected btreeNodeStruct is still appropriate for eviction
//   4 - performing the eviction
//
// Note also that non-leaf btreeNodeStruct's with loaded children cannot be evicted.
// Instead, all loaded descendants that are themselves evictable, should first be evicted.
// This will be accomplished by simply invoking purgeNode(,full==true) which should not
// find any dirty nodes at or beneath the btreeNodeStruct selected for eviction.
func (bPlusTreeCache *btreeNodeCacheStruct) btreeNodeCacheDrainer() {
	var (
		err                  error
		nodeToEvict          *btreeNodeStruct
		treeBeingEvictedFrom *btreeTreeStruct
	)

	for {
		bPlusTreeCache.Lock()
		if (0 == bPlusTreeCache.cleanLRUItems) || (bPlusTreeCache.evictLowLimit >= (bPlusTreeCache.cleanLRUItems + bPlusTreeCache.dirtyLRUItems)) {
			bPlusTreeCache.drainerActive = false
			bPlusTreeCache.Unlock()
			runtime.Goexit()
		}

		nodeToEvict = bPlusTreeCache.cleanLRUHead
		if nil == nodeToEvict {
			// No nodes to evict... try again next time
			bPlusTreeCache.drainerActive = false
			bPlusTreeCache.Unlock()
			runtime.Goexit()
		}

		treeBeingEvictedFrom = nodeToEvict.tree
		bPlusTreeCache.Unlock()
		treeBeingEvictedFrom.Lock()

		if cleanLRU != nodeToEvict.btreeNodeCacheTag {
			// Between bPlusTreeCache.Unlock() & nodeToEvict.tree.Lock(), nodeToEvict no longer evictable
			treeBeingEvictedFrom.Unlock()
			continue
		}

		err = treeBeingEvictedFrom.purgeNode(nodeToEvict, true)
		if nil != err {
			panic(err)
		}

		treeBeingEvictedFrom.Unlock()
	}
}

func (bPlusTreeCache *btreeNodeCacheStruct) UpdateLimits(evictLowLimit uint64, evictHighLimit uint64) {
	bPlusTreeCache.Lock()
	bPlusTreeCache.evictLowLimit = evictLowLimit
	bPlusTreeCache.evictHighLimit = evictHighLimit
	if !bPlusTreeCache.drainerActive && (0 < bPlusTreeCache.cleanLRUItems) && (bPlusTreeCache.evictHighLimit < (bPlusTreeCache.cleanLRUItems + bPlusTreeCache.dirtyLRUItems)) {
		bPlusTreeCache.drainerActive = true
		go bPlusTreeCache.btreeNodeCacheDrainer()
	}
	bPlusTreeCache.Unlock()
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

func (tree *btreeTreeStruct) rearrangePrefixSumTreeToRoot(node *btreeNodeStruct) (err error) {
	err = tree.arrangePrefixSumTree(node)
	if nil != err {
		return
	}

	tree.markNodeDirty(node)

	if !node.root {
		err = tree.rearrangePrefixSumTreeToRoot(node.parentNode)
	}

	return
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
	var (
		maxKeysPerNodeStruct  onDiskUint64Struct
		numChildrenStruct     onDiskUint64Struct
		numKeysStruct         onDiskUint64Struct
		onDiskNode            onDiskNodeStruct
		onDiskReferenceToNode onDiskReferenceToNodeStruct
	)

	nodeByteSlice, err := tree.BPlusTreeCallbacks.GetNode(node.objectNumber, node.objectOffset, node.objectLength)
	if nil != err {
		return
	}

	node.kvLLRB = NewLLRBTree(node.tree.Compare, node.tree.BPlusTreeCallbacks)

	_, err = cstruct.Unpack(nodeByteSlice, &onDiskNode, OnDiskByteOrder)
	if nil != err {
		return
	}

	node.items = onDiskNode.Items
	node.root = onDiskNode.Root
	node.leaf = onDiskNode.Leaf

	payload := onDiskNode.Payload

	if node.root {
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
		bytesConsumed, unpackErr := cstruct.Unpack(payload, &numChildrenStruct, OnDiskByteOrder)
		if nil != unpackErr {
			err = unpackErr
			return
		}

		payload = payload[bytesConsumed:]

		if 0 == numChildrenStruct.U64 {
			node.nonLeafLeftChild = nil
		} else {
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

			tree.initNodeAsEvicted(childNode)

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

				tree.initNodeAsEvicted(childNode)
			}

			tree.arrangePrefixSumTree(node)
		}
	}

	if 0 != len(payload) {
		err = fmt.Errorf("Logic error: load() should have exhausted payload")
		return
	}

	node.loaded = true

	tree.markNodeClean(node)

	err = nil
	return
}

func (tree *btreeTreeStruct) postNode(node *btreeNodeStruct) (err error) {
	var (
		numChildren           int
		onDiskReferenceToNode onDiskReferenceToNodeStruct
	)

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

	tree.markNodeClean(node)

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
		err = tree.purgeNode(node, true) // will also mark node evicted in LRU
		if nil != err {
			return
		}
	}

	err = nil
	return
}
