package sortedmap

import (
	"encoding/binary"
	"fmt"
	"testing"
)

const (
	commonBPlusTreeTestNumKeysMaxSmall   = uint64(2)
	commonBPlusTreeTestNumKeysMaxModest  = uint64(10)
	commonBPlusTreeTestNumKeysMaxTypical = uint64(100)
	commonBPlusTreeTestNumKeysMaxLarge   = uint64(1000)

	commonBPlusTreeBenchmarkNumKeys = 10000
)

type commonBPlusTreeTestContextStruct struct {
	t    *testing.T
	tree BPlusTree
}

func (context *commonBPlusTreeTestContextStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("GetNode() not implemented")
	return
}

func (context *commonBPlusTreeTestContextStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("PutNode() not implemented")
	return
}

func (context *commonBPlusTreeTestContextStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = fmt.Errorf("DiscardNode() not implemented")
	return
}

func (context *commonBPlusTreeTestContextStruct) DumpKey(key Key) (keyAsString string, err error) {
	keyAsInt, ok := key.(int)
	if !ok {
		context.t.Fatalf("DumpKey() argument not an int")
	}
	keyAsString = fmt.Sprintf("%v", keyAsInt)
	err = nil
	return
}

func (context *commonBPlusTreeTestContextStruct) PackKey(key Key) (packedKey []byte, err error) {
	keyAsInt, ok := key.(int)
	if !ok {
		context.t.Fatalf("PackKey() argument not an int")
	}
	keyAsUint64 := uint64(keyAsInt)
	packedKey = make([]byte, 8)
	binary.LittleEndian.PutUint64(packedKey, keyAsUint64)
	err = nil
	return
}

func (context *commonBPlusTreeTestContextStruct) UnpackKey(payloadData []byte) (key Key, bytesConsumed uint64, err error) {
	context.t.Fatalf("UnpackKey() not implemented")
	return
}

func (context *commonBPlusTreeTestContextStruct) DumpValue(value Value) (valueAsString string, err error) {
	valueAsString, ok := value.(string)
	if !ok {
		context.t.Fatalf("PackValue() argument not a string")
	}
	err = nil
	return
}

func (context *commonBPlusTreeTestContextStruct) PackValue(value Value) (packedValue []byte, err error) {
	valueAsString, ok := value.(string)
	if !ok {
		context.t.Fatalf("PackValue() argument not a string")
	}
	packedValue = []byte(valueAsString)
	err = nil
	return
}

func (context *commonBPlusTreeTestContextStruct) UnpackValue(payloadData []byte) (value Value, bytesConsumed uint64, err error) {
	context.t.Fatalf("UnpackValue() not implemented")
	return
}

type commonBPlusTreeBenchmarkContextStruct struct {
	b    *testing.B
	tree BPlusTree
}

func (context *commonBPlusTreeBenchmarkContextStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("GetNode() not implemented")
	return
}

func (context *commonBPlusTreeBenchmarkContextStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("PutNode() not implemented")
	return
}

func (context *commonBPlusTreeBenchmarkContextStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = fmt.Errorf("GetNode() not implemented")
	return
}

func (context *commonBPlusTreeBenchmarkContextStruct) DumpKey(key Key) (keyAsString string, err error) {
	err = fmt.Errorf("DumpKey() not implemented")
	return
}

func (context *commonBPlusTreeBenchmarkContextStruct) PackKey(key Key) (packedKey []byte, err error) {
	err = fmt.Errorf("PackKey() not implemented")
	return
}

func (context *commonBPlusTreeBenchmarkContextStruct) UnpackKey(payloadData []byte) (key Key, bytesConsumed uint64, err error) {
	err = fmt.Errorf("UnpackKey() not implemented")
	return
}

func (context *commonBPlusTreeBenchmarkContextStruct) DumpValue(value Value) (valueAsString string, err error) {
	err = fmt.Errorf("DumpValue() not implemented")
	return
}

func (context *commonBPlusTreeBenchmarkContextStruct) PackValue(value Value) (packedValue []byte, err error) {
	err = fmt.Errorf("PackValue() not implemented")
	return
}

func (context *commonBPlusTreeBenchmarkContextStruct) UnpackValue(payloadData []byte) (value Value, bytesConsumed uint64, err error) {
	err = fmt.Errorf("UnpackValue() not implemented")
	return
}

func TestBPlusTreeAllButDeleteSimple(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestAllButDeleteSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestAllButDeleteSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestAllButDeleteSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestAllButDeleteSimple(t, context.tree)
}

func TestBPlusTreeDeleteByIndexSimple(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestDeleteByIndexSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestDeleteByIndexSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestDeleteByIndexSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestDeleteByIndexSimple(t, context.tree)
}

func TestBPlusTreeDeleteByKeySimple(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestDeleteByKeySimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestDeleteByKeySimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestDeleteByKeySimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestDeleteByKeySimple(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByIndexTrivial(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByIndexSmall(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByIndexLarge(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByIndexHuge(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByKeyTrivial(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByKeySmall(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByKeyLarge(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByKeyHuge(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
}

func TestBPlusTreeBisect(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context)
	metaTestBisect(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context)
	metaTestBisect(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaTestBisect(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context)
	metaTestBisect(t, context.tree)
}

func BenchmarkBPlusTreePut(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkPut(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeGetByIndex(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkGetByIndex(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreePatchByIndex(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkPatchByIndex(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeDeleteByIndex(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkDeleteByIndex(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeGetByKey(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkGetByKey(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeBisectLeft(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkBisectLeft(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeBisectRight(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkBisectRight(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreePatchByKey(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkPatchByKey(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeDeleteByKey(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context)
	metaBenchmarkDeleteByKey(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}
