package sortedmap

import (
	"fmt"
	"reflect"
	"testing"
)

type balanceBPlusTreeTestContextStruct struct{}

func TestBPlusTreeBalance(t *testing.T) {
	const (
		maxKeysPerNode = 32
		numKeys        = int(10000)
	)
	var (
		err             error
		key             int
		keyIndex        int
		keysToDelete    []int
		keysToPut       []int
		ok              bool
		tree            BPlusTree // map[int]struct{}
		treeContext     *balanceBPlusTreeTestContextStruct
		treeLen         int
		treeLenExpected int
	)

	treeContext = &balanceBPlusTreeTestContextStruct{}

	tree = NewBPlusTree(maxKeysPerNode, CompareInt, treeContext, nil)

	keysToPut, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		t.Fatalf("testKnuthShuffledIntSlice() [Case A] failed: %v", err)
	}

	for keyIndex, key = range keysToPut {
		ok, err = tree.Put(key, struct{}{})
		if nil != err {
			t.Fatalf("tree.Put(%d,) [Case A] failed: %v", key, err)
		}
		if !ok {
			t.Fatalf("tree.Put(%d,) [Case A] returned !ok", key)
		}
		treeLen, err = tree.Len()
		if nil != err {
			t.Fatalf("tree.Len() [Case A] failed: %v", err)
		}
		treeLenExpected = keyIndex + 1
		if treeLen != treeLenExpected {
			t.Fatalf("tree.Len() [Case A] returned %d...expected %d", treeLen, treeLenExpected)
		}
		err = tree.Validate()
		if nil != err {
			t.Fatalf("tree.Validate() [Case A] failed: %v", err)
		}
	}

	keysToDelete, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		t.Fatalf("testKnuthShuffledIntSlice() [Case B] failed: %v", err)
	}

	for keyIndex, key = range keysToDelete {
		ok, err = tree.DeleteByKey(key)
		if nil != err {
			t.Fatalf("tree.DeleteByKey(%d) failed: %v", key, err)
		}
		if !ok {
			t.Fatalf("tree.DeleteByKey(%d) returned !ok", key)
		}
		treeLen, err = tree.Len()
		if nil != err {
			t.Fatalf("tree.Len() [Case B] failed: %v", err)
		}
		treeLenExpected = numKeys - keyIndex - 1
		if treeLen != treeLenExpected {
			t.Fatalf("tree.Len() [Case B] returned %d...expected %d", treeLen, treeLenExpected)
		}
		err = tree.Validate()
		if nil != err {
			t.Fatalf("tree.Validate() [Case B] failed: %v", err)
		}
	}

	keysToPut, err = testKnuthShuffledIntSlice(numKeys)
	if nil != err {
		t.Fatalf("testKnuthShuffledIntSlice() [Case C] failed: %v", err)
	}

	for keyIndex, key = range keysToPut {
		ok, err = tree.Put(key, struct{}{})
		if nil != err {
			t.Fatalf("tree.Put(%d,) [Case B] failed: %v", key, err)
		}
		if !ok {
			t.Fatalf("tree.Put(%d,) [Case B] returned !ok", key)
		}
		treeLen, err = tree.Len()
		if nil != err {
			t.Fatalf("tree.Len() [Case C] failed: %v", err)
		}
		treeLenExpected = keyIndex + 1
		if treeLen != treeLenExpected {
			t.Fatalf("tree.Len() [Case C] returned %d...expected %d", treeLen, treeLenExpected)
		}
		err = tree.Validate()
		if nil != err {
			t.Fatalf("tree.Validate() [Case C] failed: %v", err)
		}
	}

	for keyIndex = (numKeys - 1); keyIndex >= 0; keyIndex-- {
		ok, err = tree.DeleteByIndex(keyIndex)
		if nil != err {
			t.Fatalf("tree.DeleteByIndex(%d) failed: %v", key, err)
		}
		if !ok {
			t.Fatalf("tree.DeleteByIndex(%d) returned !ok", key)
		}
		treeLen, err = tree.Len()
		if nil != err {
			t.Fatalf("tree.Len() [Case D] failed: %v", err)
		}
		treeLenExpected = keyIndex
		if treeLen != treeLenExpected {
			t.Fatalf("tree.Len() [Case D] returned %d...expected %d", treeLen, treeLenExpected)
		}
		err = tree.Validate()
		if nil != err {
			t.Fatalf("tree.Validate() [Case D] failed: %v", err)
		}
	}
}

func (tree *balanceBPlusTreeTestContextStruct) DumpKey(key Key) (keyAsString string, err error) {
	var (
		keyAsInt int
		ok       bool
	)

	keyAsInt, ok = key.(int)
	if !ok {
		err = fmt.Errorf("DumpKey() expected key of type int... instead it was of type %v", reflect.TypeOf(key))
		return
	}

	keyAsString = fmt.Sprintf("%d", keyAsInt)

	err = nil
	return
}

func (tree *balanceBPlusTreeTestContextStruct) DumpValue(value Value) (valueAsString string, err error) {
	valueAsString = "<nil>"
	err = nil
	return
}

func (tree *balanceBPlusTreeTestContextStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("GetNode() not supported")
	return
}

func (tree *balanceBPlusTreeTestContextStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("PutNode() not supported")
	return
}

func (tree *balanceBPlusTreeTestContextStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = fmt.Errorf("DiscardNode() not supported")
	return
}

func (tree *balanceBPlusTreeTestContextStruct) PackKey(key Key) (packedKey []byte, err error) {
	err = fmt.Errorf("PackKey() not supported")
	return
}

func (tree *balanceBPlusTreeTestContextStruct) UnpackKey(payloadData []byte) (key Key, bytesConsumed uint64, err error) {
	err = fmt.Errorf("UnpackKey() not supported")
	return
}

func (tree *balanceBPlusTreeTestContextStruct) PackValue(value Value) (packedValue []byte, err error) {
	err = fmt.Errorf("PackValue() not supported")
	return
}

func (tree *balanceBPlusTreeTestContextStruct) UnpackValue(payloadData []byte) (value Value, bytesConsumed uint64, err error) {
	err = fmt.Errorf("UnpackValue() not supported")
	return
}
