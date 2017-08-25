package sortedmap

import (
	"fmt"

	"github.com/swiftstack/cstruct"
)

// OnDiskByteOrder specifies the endian-ness expected to be used to persist B+Tree data structures
var OnDiskByteOrder = cstruct.LittleEndian

// LayoutReport is a map where key is an objectNumber and value is objectBytes used in that objectNumber
type LayoutReport map[uint64]uint64

// FlushedList is an opaque list of flushed B+Tree nodes to pass to TouchFlushedList() if necessary
type FlushedList interface{}

// BPlusTree interface declares the available methods available for a B+Tree
type BPlusTree interface {
	SortedMap
	FetchLayoutReport() (layoutReport LayoutReport, err error)
	Flush(andPurge bool) (rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64, flushedList FlushedList, err error)
	Purge(full bool) (err error)
	Touch() (err error)
	TouchItem(thisItemIndexToTouch uint64) (nextItemIndexToTouch uint64, err error)
	TouchFlushedList(flushedList FlushedList) (err error)
	Prune() (err error)
	Discard() (err error)
}

// BPlusTreeCallbacks specifies the interface to a set of callbacks provided by the client
type BPlusTreeCallbacks interface {
	DumpCallbacks
	GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error)
	PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error)
	DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error)
	PackKey(key Key) (packedKey []byte, err error)
	UnpackKey(payloadData []byte) (key Key, bytesConsumed uint64, err error)
	PackValue(value Value) (packedValue []byte, err error)
	UnpackValue(payloadData []byte) (value Value, bytesConsumed uint64, err error)
}

// NewBPlusTree is used to construct an in-memory B+Tree supporting the Tree interface
//
// Note that if the B+Tree will reside only in memory, callback argument may be nil.
// That said, there is no advantage to using a B+Tree for an in-memory collection over
// the llrb-provided collection implementing the same APIs.
func NewBPlusTree(maxKeysPerNode uint64, compare Compare, callbacks BPlusTreeCallbacks) (tree BPlusTree) {
	minKeysPerNode := maxKeysPerNode >> 1
	if (0 == maxKeysPerNode) != ((2 * minKeysPerNode) != maxKeysPerNode) {
		err := fmt.Errorf("maxKeysPerNode (%v) invalid - must be a positive number that is a multiple of 2", maxKeysPerNode)
		panic(err)
	}

	rootNode := &btreeNodeStruct{
		objectNumber:        0, //                            To be filled in once root node is posted
		objectOffset:        0, //                            To be filled in once root node is posted
		objectLength:        0, //                            To be filled in once root node is posted
		items:               0,
		loaded:              true, //                         Special case in that objectNumber == 0 means it has no onDisk copy
		dirty:               true,
		root:                true,
		leaf:                true,
		tree:                nil, //                          To be set just below
		parentNode:          nil,
		kvLLRB:              NewLLRBTree(compare, callbacks),
		nonLeafLeftChild:    nil,
		rootPrefixSumChild:  nil,
		prefixSumItems:      0,   //                          Not applicable to root node
		prefixSumParent:     nil, //                          Not applicable to root node
		prefixSumLeftChild:  nil, //                          Not applicable to root node
		prefixSumRightChild: nil, //                          Not applicable to root node
	}

	staleOnDiskReferencesContext := &onDiskReferencesContext{}

	treePtr := &btreeTreeStruct{
		minKeysPerNode:        minKeysPerNode,
		maxKeysPerNode:        maxKeysPerNode,
		Compare:               compare,
		BPlusTreeCallbacks:    callbacks,
		root:                  rootNode,
		activeClones:          0,
		clonedFromTree:        nil,
		staleOnDiskReferences: NewLLRBTree(compareOnDiskReferenceKey, staleOnDiskReferencesContext),
	}

	rootNode.tree = treePtr

	tree = treePtr

	return
}

// OldBPlusTree is used to re-construct a B+Tree previously persisted
func OldBPlusTree(rootObjectNumber uint64, rootObjectOffset uint64, rootObjectLength uint64, compare Compare, callbacks BPlusTreeCallbacks) (tree BPlusTree, err error) {
	rootNode := &btreeNodeStruct{
		objectNumber:        rootObjectNumber,
		objectOffset:        rootObjectOffset,
		objectLength:        rootObjectLength,
		items:               0, //             To be filled in once root node is loaded
		loaded:              false,
		dirty:               false,
		root:                true,
		leaf:                true, //          To be updated once root node is loaded
		tree:                nil,  //          To be set just below
		parentNode:          nil,
		kvLLRB:              nil, //           To be filled in once root node is loaded
		nonLeafLeftChild:    nil, //           To be filled in once root node is loaded
		rootPrefixSumChild:  nil, //           To be filled in once root node is loaded (nil if root is also leaf)
		prefixSumItems:      0,   //           Not applicable to root node
		prefixSumParent:     nil, //           Not applicable to root node
		prefixSumLeftChild:  nil, //           Not applicable to root node
		prefixSumRightChild: nil, //           Not applicable to root node
	}

	staleOnDiskReferencesContext := &onDiskReferencesContext{}

	treePtr := &btreeTreeStruct{
		minKeysPerNode:        0, //              To be filled in once root node is loaded
		maxKeysPerNode:        0, //              To be filled in once root node is loaded
		Compare:               compare,
		BPlusTreeCallbacks:    callbacks,
		root:                  rootNode,
		activeClones:          0,
		clonedFromTree:        nil,
		staleOnDiskReferences: NewLLRBTree(compareOnDiskReferenceKey, staleOnDiskReferencesContext),
	}

	rootNode.tree = treePtr

	tree = treePtr

	err = treePtr.loadNode(rootNode)

	return
}
