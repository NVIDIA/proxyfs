package sortedmap

import (
	"fmt"

	"github.com/swiftstack/cstruct"
)

func (tree *btreeTreeStruct) Dump() (err error) {
	tree.Lock()
	defer tree.Unlock()

	if nil != tree.nodeCache {
		fmt.Printf("B+Tree @ %p has Node Cache @ %p\n", tree, tree.nodeCache)
		fmt.Printf("  .evictLowLimit  = %v\n", tree.nodeCache.evictLowLimit)
		fmt.Printf("  .evictHighLimit = %v\n", tree.nodeCache.evictHighLimit)
		if nil == tree.nodeCache.cleanLRUHead {
			fmt.Printf("  .cleanLRUHead   = nil\n")
		} else {
			fmt.Printf("  .cleanLRUHead   = %p\n", tree.nodeCache.cleanLRUHead)
		}
		if nil == tree.nodeCache.cleanLRUTail {
			fmt.Printf("  .cleanLRUTail   = nil\n")
		} else {
			fmt.Printf("  .cleanLRUTail   = %p\n", tree.nodeCache.cleanLRUTail)
		}
		fmt.Printf("  .cleanLRUItems  = %v\n", tree.nodeCache.cleanLRUItems)
		if nil == tree.nodeCache.dirtyLRUHead {
			fmt.Printf("  .dirtyLRUHead   = nil\n")
		} else {
			fmt.Printf("  .dirtyLRUHead   = %p\n", tree.nodeCache.dirtyLRUHead)
		}
		if nil == tree.nodeCache.dirtyLRUTail {
			fmt.Printf("  .dirtyLRUTail   = nil\n")
		} else {
			fmt.Printf("  .dirtyLRUTail   = %p\n", tree.nodeCache.dirtyLRUTail)
		}
		fmt.Printf("  .dirtyLRUItems  = %v\n", tree.nodeCache.dirtyLRUItems)
		fmt.Printf("  .drainerActive  = %v\n", tree.nodeCache.drainerActive)
	}

	fmt.Printf("B+Tree @ %p has Root Node @ %p\n", tree, tree.root)
	fmt.Printf("  .minKeysPerNode      = %v\n", tree.minKeysPerNode)
	fmt.Printf("  .maxKeysPerNode      = %v\n", tree.maxKeysPerNode)

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

	if nil != tree.nodeCache {
		switch node.btreeNodeCacheElement.btreeNodeCacheTag {
		case noLRU:
			fmt.Printf("%v  .btreeNodeCacheTag   = noLRU (%v)\n", indent, noLRU)
		case cleanLRU:
			fmt.Printf("%v  .btreeNodeCacheTag   = cleanLRU (%v)\n", indent, cleanLRU)
		case dirtyLRU:
			fmt.Printf("%v  .btreeNodeCacheTag   = dirtyLRU (%v)\n", indent, dirtyLRU)
		default:
			fmt.Printf("%v  .btreeNodeCacheTag   = <unknown> (%v)\n", indent, node.btreeNodeCacheElement.btreeNodeCacheTag)
		}

		if noLRU != node.btreeNodeCacheElement.btreeNodeCacheTag {
			if nil == node.btreeNodeCacheElement.nextBTreeNode {
				fmt.Printf("%v  .nextBTreeNode       = nil\n", indent)
			} else {
				fmt.Printf("%v  .nextBTreeNode       = %p\n", indent, node.btreeNodeCacheElement.nextBTreeNode)
			}
			if nil == node.btreeNodeCacheElement.prevBTreeNode {
				fmt.Printf("%v  .prevBTreeNode       = nil\n", indent)
			} else {
				fmt.Printf("%v  .prevBTreeNode       = %p\n", indent, node.btreeNodeCacheElement.prevBTreeNode)
			}
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

func (tree *btreeTreeStruct) DumpRaw() (lines []string) {
	lines = make([]string, 0)

	lines = append(lines, fmt.Sprintf("B+Tree @ %p has Root Node @ %p", tree, tree.root))
	lines = append(lines, fmt.Sprintf("  .minKeysPerNode = %v", tree.minKeysPerNode))
	lines = append(lines, fmt.Sprintf("  .maxKeysPerNode = %v", tree.maxKeysPerNode))

	lines = append(lines, tree.dumpRawNode(tree.root, "")...)

	return
}

func (tree *btreeTreeStruct) dumpRawNode(node *btreeNodeStruct, indent string) (lines []string) {
	var (
		bytesConsumed         uint64
		childNode             *btreeNodeStruct
		err                   error
		i                     uint64
		key                   Key
		keyAsString           string
		nodeByteSlice         []byte
		numKeysStruct         onDiskUint64Struct
		onDiskNode            onDiskNodeStruct
		onDiskReferenceToNode onDiskReferenceToNodeStruct
		remainingPayload      []byte
		value                 Value
		valueAsString         string
	)

	lines = make([]string, 0)

	lines = append(lines, fmt.Sprintf("%sNode @ %p", indent, node))
	lines = append(lines, fmt.Sprintf("%s  .objectNumber   = %016X", indent, node.objectNumber))
	lines = append(lines, fmt.Sprintf("%s  .objectOffset   = %016X", indent, node.objectOffset))
	lines = append(lines, fmt.Sprintf("%s  .objectLength   = %016X", indent, node.objectLength))

	nodeByteSlice, err = tree.BPlusTreeCallbacks.GetNode(node.objectNumber, node.objectOffset, node.objectLength)
	if nil != err {
		lines = append(lines, fmt.Sprintf("%s  UNABLE TO READ onDiskNode: %v", indent, err))
		return
	}

	_, err = cstruct.Unpack(nodeByteSlice, &onDiskNode, OnDiskByteOrder)
	if nil != err {
		lines = append(lines, fmt.Sprintf("%s  UNABLE TO UNPACK onDiskNode: %v", indent, err))
		return
	}

	lines = append(lines, fmt.Sprintf("%s  .items          = %016X", indent, onDiskNode.Items))
	lines = append(lines, fmt.Sprintf("%s  .root           = %v", indent, onDiskNode.Root))
	lines = append(lines, fmt.Sprintf("%s  .leaf           = %v", indent, onDiskNode.Leaf))

	if onDiskNode.Root {
		bytesConsumed, err = cstruct.Unpack(onDiskNode.Payload, &numKeysStruct, OnDiskByteOrder)
		if nil != err {
			lines = append(lines, fmt.Sprintf("%s  UNABLE TO UNPACK numKeysStruct (maxKeysPerNode): %v", indent, err))
			return
		}
		remainingPayload = onDiskNode.Payload[bytesConsumed:]

		lines = append(lines, fmt.Sprintf("%s  .maxKeysPerNode = %v", indent, numKeysStruct.U64))
	} else {
		remainingPayload = onDiskNode.Payload
	}

	bytesConsumed, err = cstruct.Unpack(remainingPayload, &numKeysStruct, OnDiskByteOrder)
	if nil != err {
		lines = append(lines, fmt.Sprintf("%s  UNABLE TO UNPACK numKeysStruct (# Keys): %v", indent, err))
		return
	}
	remainingPayload = remainingPayload[bytesConsumed:]

	if onDiskNode.Leaf {
		for i = uint64(0); i < numKeysStruct.U64; i++ {
			key, bytesConsumed, err = tree.BPlusTreeCallbacks.UnpackKey(remainingPayload)
			if nil != err {
				lines = append(lines, fmt.Sprintf("%s  UNABLE TO UNPACK key[%v]: %v", indent, i, err))
				return
			}
			remainingPayload = remainingPayload[bytesConsumed:]

			keyAsString, err = tree.DumpKey(key)
			if nil == err {
				lines = append(lines, fmt.Sprintf("%s  .key  [%016X] = %v", indent, i, keyAsString))
			} else {
				lines = append(lines, fmt.Sprintf("%s  .key  [%016X] UNKNOWN", indent, i))
			}

			value, bytesConsumed, err = tree.BPlusTreeCallbacks.UnpackValue(remainingPayload)
			if nil != err {
				lines = append(lines, fmt.Sprintf("%s  UNABLE TO UNPACK value[%v]: %v", indent, i, err))
				return
			}
			remainingPayload = remainingPayload[bytesConsumed:]

			valueAsString, err = tree.DumpValue(value)
			if nil == err {
				lines = append(lines, fmt.Sprintf("%s  .value[%016X] = %v", indent, i, valueAsString))
			} else {
				lines = append(lines, fmt.Sprintf("%s  .value[%016X] UNKNOWN", indent, i))
			}
		}
	} else {
		if uint64(0) < numKeysStruct.U64 {
			bytesConsumed, err = cstruct.Unpack(remainingPayload, &onDiskReferenceToNode, OnDiskByteOrder)
			if nil != err {
				lines = append(lines, fmt.Sprintf("%s  UNABLE TO UNPACK value[%v]: %v", indent, i, err))
				return
			}
			remainingPayload = remainingPayload[bytesConsumed:]

			childNode = &btreeNodeStruct{
				objectNumber: onDiskReferenceToNode.ObjectNumber,
				objectOffset: onDiskReferenceToNode.ObjectOffset,
				objectLength: onDiskReferenceToNode.ObjectLength,
			}

			lines = append(lines, tree.dumpRawNode(childNode, indent+"  ")...)

			for i = uint64(1); i < numKeysStruct.U64; i++ {
				key, bytesConsumed, err = tree.BPlusTreeCallbacks.UnpackKey(remainingPayload)
				if nil != err {
					lines = append(lines, fmt.Sprintf("%s  UNABLE TO UNPACK key[%v]: %v", indent, i, err))
					return
				}
				remainingPayload = remainingPayload[bytesConsumed:]

				keyAsString, err = tree.DumpKey(key)
				if nil == err {
					lines = append(lines, fmt.Sprintf("%s  .key  [%016X] = %v", indent, i, keyAsString))
				} else {
					lines = append(lines, fmt.Sprintf("%s  .key  [%016X] UNKNOWN", indent, i))
				}

				bytesConsumed, err = cstruct.Unpack(remainingPayload, &onDiskReferenceToNode, OnDiskByteOrder)
				if nil != err {
					lines = append(lines, fmt.Sprintf("%s  UNABLE TO UNPACK value[%v]: %v", indent, i, err))
					return
				}
				remainingPayload = remainingPayload[bytesConsumed:]

				childNode = &btreeNodeStruct{
					objectNumber: onDiskReferenceToNode.ObjectNumber,
					objectOffset: onDiskReferenceToNode.ObjectOffset,
					objectLength: onDiskReferenceToNode.ObjectLength,
				}

				lines = append(lines, fmt.Sprintf("%s  .value[%016X] @ %p", indent, i, childNode))

				lines = append(lines, tree.dumpRawNode(childNode, indent+"  ")...)
			}
		}
	}

	if 0 != len(remainingPayload) {
		lines = append(lines, fmt.Sprintf("%s  LEN(remainingPayload) == %v (should have been zero)", indent))
	}

	return
}
