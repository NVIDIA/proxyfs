// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package cstruct

import (
	"bytes"
	"reflect"
	"testing"
)

type ChildStruct struct {
	Btrue  bool
	Bfalse bool
	I8pos  int8
	I8neg  int8
	U8     uint8
	I16pos int16
	I16neg int16
	U16    uint16
	I32pos int32
	I32neg int32
	U32    uint32
	I64pos int64
	I64neg int64
	U64    uint64
}

type ParentStruct struct {
	U32      uint32
	Child    ChildStruct
	U16Array [3]uint16
}

type DynamicStruct struct {
	U16       uint16
	ByteSlice []uint8
}

func TestExamine(t *testing.T) {
	parent := ParentStruct{}

	parentStructBytesNeeded, parentTrailingByteSlice, err := Examine(parent)

	if nil != err {
		t.Fatalf("Examine(parent) returned unexpected error: %v", err)
	}

	if 57 != parentStructBytesNeeded {
		t.Fatalf("Examine(parent) returned unexpected bytesNeeded: %v (should have been 57)", parentStructBytesNeeded)
	}

	if parentTrailingByteSlice {
		t.Fatalf("Examine(parent) returned unexpected trailingByteSlice: true (should have been false)")
	}

	dynamic := DynamicStruct{}

	dynamicStructBytesNeeded, dynamicTrailingByteSlice, err := Examine(dynamic)

	if nil != err {
		t.Fatalf("Examine(dynamic) returned unexpected error: %v", err)
	}

	if 2 != dynamicStructBytesNeeded {
		t.Fatalf("Examine(dynamic) returned unexpected bytesNeeded: %v (should have been 2)", dynamicStructBytesNeeded)
	}

	if !dynamicTrailingByteSlice {
		t.Fatalf("Examine(dynamic) returned unexpected trailingByteSlice: false (should have been true)")
	}
}

func TestPack(t *testing.T) {
	parent := ParentStruct{}

	parent.U32 = 0x01020304
	parent.Child.Btrue = true
	parent.Child.Bfalse = false
	parent.Child.I8pos = 0x10
	parent.Child.I8neg = -0x10
	parent.Child.U8 = 0x10
	parent.Child.I16pos = 0x1122
	parent.Child.I16neg = -0x1122
	parent.Child.U16 = 0x1122
	parent.Child.I32pos = 0x01020304
	parent.Child.I32neg = -0x01020304
	parent.Child.U32 = 0x01020304
	parent.Child.I64pos = 0x1020304050607080
	parent.Child.I64neg = -0x1020304050607080
	parent.Child.U64 = 0x1020304050607080
	parent.U16Array[0] = 0x1122
	parent.U16Array[1] = 0x3344
	parent.U16Array[2] = 0x5566

	parentPackedLittleEndianExpected := []byte{
		0x04, 0x03, 0x02, 0x01,
		0x01,
		0x00,
		0x10,
		0xF0,
		0x10,
		0x22, 0x11,
		0xDE, 0xEE,
		0x22, 0x11,
		0x04, 0x03, 0x02, 0x01,
		0xFC, 0xFC, 0xFD, 0xFE,
		0x04, 0x03, 0x02, 0x01,
		0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10,
		0x80, 0x8F, 0x9F, 0xAF, 0xBF, 0xCF, 0xDF, 0xEF,
		0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10,
		0x22, 0x11,
		0x44, 0x33,
		0x66, 0x55,
	}

	parentPackedBigEndianExpected := []byte{
		0x01, 0x02, 0x03, 0x04,
		0x01,
		0x00,
		0x10,
		0xF0,
		0x10,
		0x11, 0x22,
		0xEE, 0xDE,
		0x11, 0x22,
		0x01, 0x02, 0x03, 0x04,
		0xFE, 0xFD, 0xFC, 0xFC,
		0x01, 0x02, 0x03, 0x04,
		0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		0xEF, 0xDF, 0xCF, 0xBF, 0xAF, 0x9F, 0x8F, 0x80,
		0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		0x11, 0x22,
		0x33, 0x44,
		0x55, 0x66,
	}

	parentPackedLittleEndianReturned, err := Pack(parent, LittleEndian)
	if nil != err {
		t.Fatalf("Pack(parent, LittleEndian) returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(parentPackedLittleEndianExpected, parentPackedLittleEndianReturned) {
		t.Fatalf("Pack(parent, LittleEndian) returned unexpected dst []byte")
	}

	parentPackedBigEndianReturned, err := Pack(parent, BigEndian)
	if nil != err {
		t.Fatalf("Pack(parent, BigEndian) returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(parentPackedBigEndianExpected, parentPackedBigEndianReturned) {
		t.Fatalf("Pack(parent, BigEndian) returned unexpected dst []byte")
	}

	dynamic := DynamicStruct{}

	dynamic.U16 = 0x1122
	dynamic.ByteSlice = []byte{0x33, 0x44, 0x55}

	dynamicPackedLittleEndianExpected := []byte{
		0x22, 0x11,
		0x33, 0x44, 0x55,
	}

	dynamicPackedBigEndianExpected := []byte{
		0x11, 0x22,
		0x33, 0x44, 0x55,
	}

	dynamicPackedLittleEndianReturned, err := Pack(dynamic, LittleEndian)
	if nil != err {
		t.Fatalf("Pack(dynamic, LittleEndian) returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(dynamicPackedLittleEndianExpected, dynamicPackedLittleEndianReturned) {
		t.Fatalf("Pack(dynamic, LittleEndian) returned unexpected dst []byte")
	}

	dynamicPackedBigEndianReturned, err := Pack(dynamic, BigEndian)
	if nil != err {
		t.Fatalf("Pack(dynamic, BigEndian) returned unexpected error: %v", err)
	}
	if 0 != bytes.Compare(dynamicPackedBigEndianExpected, dynamicPackedBigEndianReturned) {
		t.Fatalf("Pack(dynamic, BigEndian) returned unexpected dst []byte")
	}
}

func TestUnpack(t *testing.T) {
	parentPackedLittleEndian := []byte{
		0x04, 0x03, 0x02, 0x01,
		0x01,
		0x00,
		0x10,
		0xF0,
		0x10,
		0x22, 0x11,
		0xDE, 0xEE,
		0x22, 0x11,
		0x04, 0x03, 0x02, 0x01,
		0xFC, 0xFC, 0xFD, 0xFE,
		0x04, 0x03, 0x02, 0x01,
		0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10,
		0x80, 0x8F, 0x9F, 0xAF, 0xBF, 0xCF, 0xDF, 0xEF,
		0x80, 0x70, 0x60, 0x50, 0x40, 0x30, 0x20, 0x10,
		0x22, 0x11,
		0x44, 0x33,
		0x66, 0x55,
	}

	parentPackedBigEndian := []byte{
		0x01, 0x02, 0x03, 0x04,
		0x01,
		0x00,
		0x10,
		0xF0,
		0x10,
		0x11, 0x22,
		0xEE, 0xDE,
		0x11, 0x22,
		0x01, 0x02, 0x03, 0x04,
		0xFE, 0xFD, 0xFC, 0xFC,
		0x01, 0x02, 0x03, 0x04,
		0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		0xEF, 0xDF, 0xCF, 0xBF, 0xAF, 0x9F, 0x8F, 0x80,
		0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		0x11, 0x22,
		0x33, 0x44,
		0x55, 0x66,
	}

	parentExpected := ParentStruct{}

	parentExpected.U32 = 0x01020304
	parentExpected.Child.Btrue = true
	parentExpected.Child.Bfalse = false
	parentExpected.Child.I8pos = 0x10
	parentExpected.Child.I8neg = -0x10
	parentExpected.Child.U8 = 0x10
	parentExpected.Child.I16pos = 0x1122
	parentExpected.Child.I16neg = -0x1122
	parentExpected.Child.U16 = 0x1122
	parentExpected.Child.I32pos = 0x01020304
	parentExpected.Child.I32neg = -0x01020304
	parentExpected.Child.U32 = 0x01020304
	parentExpected.Child.I64pos = 0x1020304050607080
	parentExpected.Child.I64neg = -0x1020304050607080
	parentExpected.Child.U64 = 0x1020304050607080
	parentExpected.U16Array[0] = 0x1122
	parentExpected.U16Array[1] = 0x3344
	parentExpected.U16Array[2] = 0x5566

	parentReturnedFromUnpackParentPackedLittleEndian := ParentStruct{}

	bytesConsumedByUnpackParentPackedLittleEndian, err := Unpack(parentPackedLittleEndian, &parentReturnedFromUnpackParentPackedLittleEndian, LittleEndian)
	if nil != err {
		t.Fatalf("Unpack(parentPackedLittleEndian, &parentReturnedFromUnpackParentPackedLittleEndian, LittleEndian) returned unexpected error: %v", err)
	}
	if uint64(len(parentPackedLittleEndian)) != bytesConsumedByUnpackParentPackedLittleEndian {
		t.Fatalf("Unpack(parentPackedLittleEndian, &parentReturnedFromUnpackParentPackedLittleEndian, LittleEndian) bytesConsumed unexpected")
	}
	if !reflect.DeepEqual(parentExpected, parentReturnedFromUnpackParentPackedLittleEndian) {
		t.Fatalf("Unpack(parentPackedLittleEndian, &parentReturnedFromUnpackParentPackedLittleEndian, LittleEndian) returned unexpected ParentStruct")
	}

	parentReturnedFromUnpackParentPackedBigEndian := ParentStruct{}

	bytesConsumedByUnpackParentPackedBigEndian, err := Unpack(parentPackedBigEndian, &parentReturnedFromUnpackParentPackedBigEndian, BigEndian)
	if nil != err {
		t.Fatalf("Unpack(parentPackedBigEndian, &parentReturnedFromUnpackParentPackedBigEndian, BigEndian) returned unexpected error: %v", err)
	}
	if uint64(len(parentPackedBigEndian)) != bytesConsumedByUnpackParentPackedBigEndian {
		t.Fatalf("Unpack(parentPackedBigEndian, &parentReturnedFromUnpackParentPackedBigEndian, BigEndian) bytesConsumed unexpected")
	}
	if !reflect.DeepEqual(parentExpected, parentReturnedFromUnpackParentPackedBigEndian) {
		t.Fatalf("Unpack(parentPackedBigEndian, &parentReturnedFromUnpackParentPackedBigEndian, BigEndian) returned unexpected ParentStruct")
	}

	dynamicPackedLittleEndian := []byte{
		0x22, 0x11,
		0x33, 0x44, 0x55,
	}

	dynamicPackedBigEndian := []byte{
		0x11, 0x22,
		0x33, 0x44, 0x55,
	}

	dynamicExpected := DynamicStruct{}

	dynamicExpected.U16 = 0x1122
	dynamicExpected.ByteSlice = []byte{0x33, 0x44, 0x55}

	dynamicReturnedFromUnpackDynamicPackedLittleEndian := DynamicStruct{}

	bytesConsumedByUnpackDynamicPackedLittleEndian, err := Unpack(dynamicPackedLittleEndian, &dynamicReturnedFromUnpackDynamicPackedLittleEndian, LittleEndian)
	if nil != err {
		t.Fatalf("Unpack(dynamicPackedLittleEndian, &dynamicReturnedFromUnpackDynamicPackedLittleEndian, LittleEndian) returned unexpected error: %v", err)
	}
	if uint64(len(dynamicPackedLittleEndian)) != bytesConsumedByUnpackDynamicPackedLittleEndian {
		t.Fatalf("Unpack(dynamicPackedLittleEndian, &dynamicReturnedFromUnpackDynamicPackedLittleEndian, LittleEndian) bytesConsumed unexpected")
	}
	if !reflect.DeepEqual(dynamicExpected, dynamicReturnedFromUnpackDynamicPackedLittleEndian) {
		t.Fatalf("Unpack(parentPackedLittleEndian, &parentReturnedFromUnpackParentPackedLittleEndian, LittleEndian) returned unexpected DynamicStruct")
	}

	dynamicReturnedFromUnpackDynamicPackedBigEndian := DynamicStruct{}

	bytesConsumedByUnpackDynamicPackedBigEndian, err := Unpack(dynamicPackedBigEndian, &dynamicReturnedFromUnpackDynamicPackedBigEndian, BigEndian)
	if nil != err {
		t.Fatalf("Unpack(dynamicPackedBigEndian, &dynamicReturnedFromUnpackDynamicPackedBigEndian, BigEndian) returned unexpected error: %v", err)
	}
	if uint64(len(dynamicPackedBigEndian)) != bytesConsumedByUnpackDynamicPackedBigEndian {
		t.Fatalf("Unpack(dynamicPackedBigEndian, &dynamicReturnedFromUnpackDynamicPackedBigEndian, BigEndian) bytesConsumed unexpected")
	}
	if !reflect.DeepEqual(dynamicExpected, dynamicReturnedFromUnpackDynamicPackedBigEndian) {
		t.Fatalf("Unpack(parentPackedBigEndian, &parentReturnedFromUnpackParentPackedBigEndian, BigEndian) returned unexpected DynamicStruct")
	}
}
