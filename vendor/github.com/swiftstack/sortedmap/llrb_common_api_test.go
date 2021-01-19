// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package sortedmap

import (
	"fmt"
	"testing"
)

const (
	commonLLRBTreeBenchmarkNumKeys = 1000
)

type commonLLRBTreeTestContextStruct struct {
	t    *testing.T
	tree LLRBTree
}

func (context *commonLLRBTreeTestContextStruct) DumpKey(key Key) (keyAsString string, err error) {
	keyAsInt, ok := key.(int)
	if !ok {
		context.t.Fatalf("DumpKey() argument not an int")
	}
	keyAsString = fmt.Sprintf("%v", keyAsInt)
	err = nil
	return
}

func (context *commonLLRBTreeTestContextStruct) DumpValue(value Value) (valueAsString string, err error) {
	valueAsString, ok := value.(string)
	if !ok {
		context.t.Fatalf("PackValue() argument not a string")
	}
	err = nil
	return
}

type commonLLRBTreeBenchmarkContextStruct struct {
	b    *testing.B
	tree LLRBTree
}

func (context *commonLLRBTreeBenchmarkContextStruct) DumpKey(key Key) (keyAsString string, err error) {
	err = fmt.Errorf("DumpKey() not implemented")
	return
}

func (context *commonLLRBTreeBenchmarkContextStruct) DumpValue(value Value) (valueAsString string, err error) {
	err = fmt.Errorf("DumpValue() not implemented")
	return
}

func TestLLRBTreeAllButDeleteSimple(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestAllButDeleteSimple(t, context.tree)
}

func TestLLRBTreeDeleteByIndexSimple(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestDeleteByIndexSimple(t, context.tree)
}

func TestLLRBTreeDeleteByKeySimple(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestDeleteByKeySimple(t, context.tree)
}

func TestLLRBTreeInsertGetDeleteByIndexTrivial(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestInsertGetDeleteByIndexTrivial(t, context.tree)
}

func TestLLRBTreeInsertGetDeleteByIndexSmall(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestInsertGetDeleteByIndexSmall(t, context.tree)
}

func TestLLRBTreeInsertGetDeleteByIndexLarge(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestInsertGetDeleteByIndexLarge(t, context.tree)
}

func TestLLRBTreeInsertGetDeleteByIndexHuge(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestInsertGetDeleteByIndexHuge(t, context.tree)
}

func TestLLRBTreeInsertGetDeleteByKeyTrivial(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestInsertGetDeleteByKeyTrivial(t, context.tree)
}

func TestLLRBTreeInsertGetDeleteByKeySmall(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestInsertGetDeleteByKeySmall(t, context.tree)
}

func TestLLRBTreeInsertGetDeleteByKeyLarge(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestInsertGetDeleteByKeyLarge(t, context.tree)
}

func TestLLRBTreeInsertGetDeleteByKeyHuge(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestInsertGetDeleteByKeyHuge(t, context.tree)
}

func TestLLRBTreeBisect(t *testing.T) {
	context := &commonLLRBTreeTestContextStruct{t: t}
	context.tree = NewLLRBTree(CompareInt, context)
	metaTestBisect(t, context.tree)
}

func BenchmarkLLRBTreePut(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkPut(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}

func BenchmarkLLRBTreeGetByIndex(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkGetByIndex(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}

func BenchmarkLLRBTreePatchByIndex(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkPatchByIndex(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}

func BenchmarkLLRBTreeDeleteByIndex(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkDeleteByIndex(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}

func BenchmarkLLRBTreeGetByKey(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkGetByKey(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}

func BenchmarkLLRBTreeBisectLeft(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkBisectLeft(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}

func BenchmarkLLRBTreeBisectRight(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkBisectRight(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}

func BenchmarkLLRBTreePatchByKey(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkPatchByKey(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}

func BenchmarkLLRBTreeDeleteByKey(b *testing.B) {
	context := &commonLLRBTreeBenchmarkContextStruct{b: b}
	context.tree = NewLLRBTree(CompareInt, context)
	metaBenchmarkDeleteByKey(b, context.tree, commonLLRBTreeBenchmarkNumKeys)
}
