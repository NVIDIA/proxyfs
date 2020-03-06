package sortedmap

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

const (
	testBPlusTreeCacheDelay = 100 * time.Millisecond
)

type cacheBPlusTreeTestContextStruct struct {
	sync.Mutex
	nextObjectNumber uint64
	objectMap        map[uint64][]byte
}

func (tree *cacheBPlusTreeTestContextStruct) DumpKey(key Key) (keyAsString string, err error) {
	var (
		keyAsUint16 uint16
		ok          bool
	)

	keyAsUint16, ok = key.(uint16)
	if !ok {
		err = fmt.Errorf("DumpKey() expected key of type uint16... instead it was of type %v", reflect.TypeOf(key))
		return
	}

	keyAsString = fmt.Sprintf("0x%04X", keyAsUint16)

	err = nil
	return
}

func (tree *cacheBPlusTreeTestContextStruct) DumpValue(value Value) (valueAsString string, err error) {
	var (
		ok            bool
		valueAsUint32 uint32
	)

	valueAsUint32, ok = value.(uint32)
	if !ok {
		err = fmt.Errorf("DumpValue() expected value of type uint32... instead it was of type %v", reflect.TypeOf(value))
		return
	}

	valueAsString = fmt.Sprintf("0x%08X", valueAsUint32)

	err = nil
	return
}

func (tree *cacheBPlusTreeTestContextStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	var (
		ok bool
	)

	tree.Lock()
	defer tree.Unlock()

	nodeByteSlice, ok = tree.objectMap[objectNumber]
	if !ok {
		err = fmt.Errorf("GetNode() called for non-existent objectNumber 0x%016X", objectNumber)
		return
	}
	if uint64(0) != objectOffset {
		err = fmt.Errorf("GetNode() called for non-zero objectOffset 0x%016X", objectOffset)
		return
	}
	if uint64(len(nodeByteSlice)) != objectLength {
		err = fmt.Errorf("GetNode() called for objectLength 0x%016X... for node of length 0x%016X", objectLength, uint64(len(nodeByteSlice)))
		return
	}

	err = nil
	return
}

func (tree *cacheBPlusTreeTestContextStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	tree.Lock()
	defer tree.Unlock()

	objectNumber = tree.nextObjectNumber
	tree.nextObjectNumber++
	tree.objectMap[objectNumber] = nodeByteSlice

	objectOffset = 0

	err = nil
	return
}

func (tree *cacheBPlusTreeTestContextStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	var (
		ok            bool
		nodeByteSlice []byte
	)

	tree.Lock()
	defer tree.Unlock()

	nodeByteSlice, ok = tree.objectMap[objectNumber]
	if !ok {
		err = fmt.Errorf("DiscardNode() called for non-existent objectNumber 0x%016X", objectNumber)
		return
	}
	if uint64(0) != objectOffset {
		err = fmt.Errorf("DiscardNode() called for non-zero objectOffset 0x%016X", objectOffset)
		return
	}
	if uint64(len(nodeByteSlice)) != objectLength {
		err = fmt.Errorf("DiscardNode() called for objectLength 0x%016X... for node of length 0x%016X", objectLength, uint64(len(nodeByteSlice)))
		return
	}

	err = nil
	return
}

func (tree *cacheBPlusTreeTestContextStruct) PackKey(key Key) (packedKey []byte, err error) {
	var (
		ok          bool
		keyAsUint16 uint16
	)

	keyAsUint16, ok = key.(uint16)
	if !ok {
		err = fmt.Errorf("PackKey() expected key of type uint16... instead it was of type %v", reflect.TypeOf(key))
		return
	}

	packedKey = make([]byte, 2)
	packedKey[0] = uint8(keyAsUint16 & uint16(0x00FF) >> 0)
	packedKey[1] = uint8(keyAsUint16 & uint16(0xFF00) >> 8)

	err = nil
	return
}

func (tree *cacheBPlusTreeTestContextStruct) UnpackKey(payloadData []byte) (key Key, bytesConsumed uint64, err error) {
	if len(payloadData) < 2 {
		err = fmt.Errorf("UnpackKey() called for length %v... expected length of at least 2", len(payloadData))
		return
	}

	key = uint16(payloadData[0]) | (uint16(payloadData[1]) << 8)

	bytesConsumed = 2

	err = nil
	return
}

func (tree *cacheBPlusTreeTestContextStruct) PackValue(value Value) (packedValue []byte, err error) {
	var (
		ok            bool
		valueAsUint32 uint32
	)

	valueAsUint32, ok = value.(uint32)
	if !ok {
		err = fmt.Errorf("PackValue() expected value of type uint32... instead it was of type %v", reflect.TypeOf(value))
		return
	}

	packedValue = make([]byte, 4)
	packedValue[0] = uint8(valueAsUint32 & uint32(0x000000FF) >> 0)
	packedValue[1] = uint8(valueAsUint32 & uint32(0x0000FF00) >> 8)
	packedValue[2] = uint8(valueAsUint32 & uint32(0x00FF0000) >> 16)
	packedValue[3] = uint8(valueAsUint32 & uint32(0xFF000000) >> 24)

	err = nil
	return
}

func (tree *cacheBPlusTreeTestContextStruct) UnpackValue(payloadData []byte) (value Value, bytesConsumed uint64, err error) {
	if len(payloadData) < 4 {
		err = fmt.Errorf("UnpackValue() called for length %v... expected length of at least 4", len(payloadData))
		return
	}

	value = uint32(payloadData[0]) | (uint32(payloadData[1]) << 8) | (uint32(payloadData[2]) << 16) | (uint32(payloadData[3]) << 24)

	bytesConsumed = 4

	err = nil
	return
}

func TestBPlusTreeCache(t *testing.T) {
	var (
		treeA           BPlusTree // map[uint16]uint32
		treeB           BPlusTree // map[uint16]uint32
		treeCache       BPlusTreeCache
		treeCacheStruct *btreeNodeCacheStruct
		treeCacheStats  *BPlusTreeCacheStats
		treeContext     *cacheBPlusTreeTestContextStruct
	)

	treeContext = &cacheBPlusTreeTestContextStruct{
		nextObjectNumber: uint64(0),
		objectMap:        make(map[uint64][]byte),
	}

	treeCache = NewBPlusTreeCache(1, 4)
	treeCacheStruct = treeCache.(*btreeNodeCacheStruct)

	// Consume 1 node for treeA

	treeA = NewBPlusTree(4, CompareUint16, treeContext, treeCache)

	_, _ = treeA.Put(uint16(0x0000), uint32(0x00000000))

	if 0 != treeCacheStruct.cleanLRUItems {
		t.Fatalf("Expected treeCacheStruct.cleanLRUItems to be 0 (was %v)", treeCacheStruct.cleanLRUItems)
	}
	if 1 != treeCacheStruct.dirtyLRUItems {
		t.Fatalf("Expected treeCacheStruct.dirtyLRUItems to be 1 (was %v)", treeCacheStruct.dirtyLRUItems)
	}

	treeA.Flush(false)

	if 1 != treeCacheStruct.cleanLRUItems {
		t.Fatalf("Expected treeCacheStruct.cleanLRUItems to be 1 (was %v)", treeCacheStruct.cleanLRUItems)
	}
	if 0 != treeCacheStruct.dirtyLRUItems {
		t.Fatalf("Expected treeCacheStruct.dirtyLRUItems to be 0 (was %v)", treeCacheStruct.dirtyLRUItems)
	}

	treeA.Flush(true)

	if 0 != treeCacheStruct.cleanLRUItems {
		t.Fatalf("Expected treeCacheStruct.cleanLRUItems to be 0 (was %v)", treeCacheStruct.cleanLRUItems)
	}
	if 0 != treeCacheStruct.dirtyLRUItems {
		t.Fatalf("Expected treeCacheStruct.dirtyLRUItems to be 0 (was %v)", treeCacheStruct.dirtyLRUItems)
	}

	// Consume 4 nodes for treeB

	treeB = NewBPlusTree(4, CompareUint16, treeContext, treeCache)

	_, _ = treeB.Put(uint16(0x0000), uint32(0x00000000))
	_, _ = treeB.Put(uint16(0x0001), uint32(0x00000001))
	_, _ = treeB.Put(uint16(0x0002), uint32(0x00000002))
	_, _ = treeB.Put(uint16(0x0003), uint32(0x00000003))
	_, _ = treeB.Put(uint16(0x0004), uint32(0x00000004))
	_, _ = treeB.Put(uint16(0x0005), uint32(0x00000005))
	_, _ = treeB.Put(uint16(0x0006), uint32(0x00000006))

	if 0 != treeCacheStruct.cleanLRUItems {
		t.Fatalf("Expected treeCacheStruct.cleanLRUItems to be 0 (was %v)", treeCacheStruct.cleanLRUItems)
	}
	if 4 != treeCacheStruct.dirtyLRUItems {
		t.Fatalf("Expected treeCacheStruct.dirtyLRUItems to be 4 (was %v)", treeCacheStruct.dirtyLRUItems)
	}

	treeB.Flush(false)

	if 4 != treeCacheStruct.cleanLRUItems {
		t.Fatalf("Expected treeCacheStruct.cleanLRUItems to be 4 (was %v)", treeCacheStruct.cleanLRUItems)
	}
	if 0 != treeCacheStruct.dirtyLRUItems {
		t.Fatalf("Expected treeCacheStruct.dirtyLRUItems to be 0 (was %v)", treeCacheStruct.dirtyLRUItems)
	}

	treeB.Flush(true)

	if 0 != treeCacheStruct.cleanLRUItems {
		t.Fatalf("Expected treeCacheStruct.cleanLRUItems to be 0 (was %v)", treeCacheStruct.cleanLRUItems)
	}
	if 0 != treeCacheStruct.dirtyLRUItems {
		t.Fatalf("Expected treeCacheStruct.dirtyLRUItems to be 0 (was %v)", treeCacheStruct.dirtyLRUItems)
	}

	// Read entire treeB filling all 4 treeCacheStruct.cleanLRUItems

	_, _, _ = treeB.GetByKey(uint16(0x0000))
	_, _, _ = treeB.GetByKey(uint16(0x0001))
	_, _, _ = treeB.GetByKey(uint16(0x0002))
	_, _, _ = treeB.GetByKey(uint16(0x0003))
	_, _, _ = treeB.GetByKey(uint16(0x0004))
	_, _, _ = treeB.GetByKey(uint16(0x0005))
	_, _, _ = treeB.GetByKey(uint16(0x0006))

	if 4 != treeCacheStruct.cleanLRUItems {
		t.Fatalf("Expected treeCacheStruct.cleanLRUItems to be 4 (was %v)", treeCacheStruct.cleanLRUItems)
	}
	if 0 != treeCacheStruct.dirtyLRUItems {
		t.Fatalf("Expected treeCacheStruct.dirtyLRUItems to be 0 (was %v)", treeCacheStruct.dirtyLRUItems)
	}

	// Read lone key from treeA expecting treeCache to have to purge down to evictLowLimit

	_, _, _ = treeA.GetByKey(uint16(0x0000))

	for treeCacheStruct.drainerActive {
		time.Sleep(testBPlusTreeCacheDelay)
	}

	if 1 != treeCacheStruct.cleanLRUItems {
		t.Fatalf("Expected treeCacheStruct.cleanLRUItems to be 1 (was %v)", treeCacheStruct.cleanLRUItems)
	}
	if 0 != treeCacheStruct.dirtyLRUItems {
		t.Fatalf("Expected treeCacheStruct.dirtyLRUItems to be 0 (was %v)", treeCacheStruct.dirtyLRUItems)
	}

	// Finally, query treeCacheStruct Stats()

	treeCacheStats = treeCacheStruct.Stats()

	if 1 != treeCacheStats.EvictLowLimit {
		t.Fatalf("Expected EvictLowLimit to be 1 (was %v)", treeCacheStats.EvictLowLimit)
	}
	if 4 != treeCacheStats.EvictHighLimit {
		t.Fatalf("Expected EvictHighLimit to be 4 (was %v)", treeCacheStats.EvictHighLimit)
	}
	if 1 != treeCacheStats.CleanLRUItems {
		t.Fatalf("Expected CleanLRUItems to be 1 (was %v)", treeCacheStats.CleanLRUItems)
	}
	if 0 != treeCacheStats.DirtyLRUItems {
		t.Fatalf("Expected DirtyLRUItems to be 0 (was %v)", treeCacheStats.DirtyLRUItems)
	}
	if 20 != treeCacheStats.CacheHits {
		t.Fatalf("Expected CacheHits to be 20 (was %v)", treeCacheStats.CacheHits)
	}
	if 5 != treeCacheStats.CacheMisses {
		t.Fatalf("Expected CacheMisses to be 5 (was %v)", treeCacheStats.CacheMisses)
	}
}
