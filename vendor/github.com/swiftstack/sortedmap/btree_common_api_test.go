package sortedmap

import (
	"encoding/binary"
	"fmt"
	"testing"
)

const (
	commonBPlusTreeTestNumKeysMaxSmall   = uint64(4)
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
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestAllButDeleteSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestAllButDeleteSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestAllButDeleteSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestAllButDeleteSimple(t, context.tree)
}

func TestBPlusTreeDeleteByIndexSimple(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestDeleteByIndexSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestDeleteByIndexSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestDeleteByIndexSimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestDeleteByIndexSimple(t, context.tree)
}

func TestBPlusTreeDeleteByKeySimple(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestDeleteByKeySimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestDeleteByKeySimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestDeleteByKeySimple(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestDeleteByKeySimple(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByIndexTrivial(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByIndexSmall(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByIndexLarge(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByIndexHuge(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByKeyTrivial(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByKeySmall(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByKeyLarge(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
}

func TestBPlusTreeInsertGetDeleteByKeyHuge(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
}

func TestBPlusTreeBisect(t *testing.T) {
	context := &commonBPlusTreeTestContextStruct{t: t}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxSmall, CompareInt, context, nil)
	metaTestBisect(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxModest, CompareInt, context, nil)
	metaTestBisect(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaTestBisect(t, context.tree)
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxLarge, CompareInt, context, nil)
	metaTestBisect(t, context.tree)
}

func BenchmarkBPlusTreePut(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkPut(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeGetByIndex(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkGetByIndex(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreePatchByIndex(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkPatchByIndex(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeDeleteByIndex(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkDeleteByIndex(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeGetByKey(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkGetByKey(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeBisectLeft(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkBisectLeft(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeBisectRight(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkBisectRight(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreePatchByKey(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkPatchByKey(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}

func BenchmarkBPlusTreeDeleteByKey(b *testing.B) {
	context := &commonBPlusTreeBenchmarkContextStruct{b: b}
	context.tree = NewBPlusTree(commonBPlusTreeTestNumKeysMaxTypical, CompareInt, context, nil)
	metaBenchmarkDeleteByKey(b, context.tree, commonBPlusTreeBenchmarkNumKeys)
}
