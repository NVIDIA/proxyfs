// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package cstruct

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

func examineRecursive(obj reflect.Value) (bytesNeeded uint64, trailingByteSlice bool, err error) {
	// First check for "encapsulating" obj.Kind()'s

	if (obj.Kind() == reflect.Interface) || (obj.Kind() == reflect.Ptr) {
		bytesNeeded, trailingByteSlice, err = examineRecursive(obj.Elem())
		return
	}

	// Setup defaults for "non-encapsulating" obj.Kind()'s

	bytesNeeded = 0
	trailingByteSlice = false
	err = nil

	// Handle specific "non-encapsulating" obj.Kind()

	switch obj.Kind() {
	case reflect.Bool:
		bytesNeeded = 1
	case reflect.Int8:
		bytesNeeded = 1
	case reflect.Uint8:
		bytesNeeded = 1
	case reflect.Int16:
		bytesNeeded = 2
	case reflect.Uint16:
		bytesNeeded = 2
	case reflect.Int32:
		bytesNeeded = 4
	case reflect.Uint32:
		bytesNeeded = 4
	case reflect.Int64:
		bytesNeeded = 8
	case reflect.Uint64:
		bytesNeeded = 8
	case reflect.Array:
		for i := 0; i < obj.Len(); i++ {
			elementBytesNeeded, elementTrailingByteSlice, elementErr := examineRecursive(obj.Index(i))
			if nil != elementErr {
				err = elementErr
				return
			}
			if elementTrailingByteSlice {
				err = fmt.Errorf("Array of slices not unsupported")
				return
			}
			bytesNeeded += elementBytesNeeded
		}
	case reflect.Struct:
		for i := 0; i < obj.NumField(); i++ {
			fieldBytesNeeded, fieldTrailingByteSlice, fieldErr := examineRecursive(obj.Field(i))
			if nil != fieldErr {
				err = fieldErr
				return
			}
			if fieldTrailingByteSlice {
				if i < (obj.NumField() - 1) {
					err = fmt.Errorf("[]byte field only supported at end of object")
					return
				}

				trailingByteSlice = true
			}
			bytesNeeded += fieldBytesNeeded
		}
	case reflect.Slice:
		sliceType := obj.Type().Elem()
		if "uint8" == sliceType.String() {
			bytesNeeded = 0 // Indeterminate by just looking at obj... but obj will consume remainder of buffer (i.e. obj is always last)
			trailingByteSlice = true
		} else {
			err = fmt.Errorf("obj is %#v; obj.Type == %v unsupported", obj, obj.Type())
		}
	default:
		err = fmt.Errorf("obj is %#v; obj.Kind == %v unsupported", obj, obj.Kind())
	}

	return
}

func packRecursive(srcObj reflect.Value, oldDst []byte, oldOffset uint64, byteOrder binary.ByteOrder) (newDst []byte, newOffset uint64) {
	var i64 int64
	var u64 uint64

	switch srcObj.Kind() {
	case reflect.Interface:
		newDst, newOffset = packRecursive(srcObj.Elem(), oldDst, oldOffset, byteOrder)
	case reflect.Ptr:
		newDst, newOffset = packRecursive(srcObj.Elem(), oldDst, oldOffset, byteOrder)
	case reflect.Bool:
		newDst = oldDst
		newOffset = oldOffset + 1
		if srcObj.Bool() {
			u64 = 0x01
		} else {
			u64 = 0x00
		}
		newDst[oldOffset+0] = byte((u64 >> 0) & 0xFF)
	case reflect.Int8:
		newDst = oldDst
		newOffset = oldOffset + 1
		i64 = srcObj.Int()
		if 0 <= i64 {
			u64 = uint64(i64)
		} else {
			u64 = ^uint64(-i64) + 1
		}
		newDst[oldOffset+0] = byte((u64 >> 0) & 0xFF)
	case reflect.Uint8:
		newDst = oldDst
		newOffset = oldOffset + 1
		u64 = srcObj.Uint()
		newDst[oldOffset+0] = byte((u64 >> 0) & 0xFF)
	case reflect.Int16:
		newDst = oldDst
		newOffset = oldOffset + 2
		i64 = srcObj.Int()
		if 0 <= i64 {
			u64 = uint64(i64)
		} else {
			u64 = ^uint64(-i64) + 1
		}
		if LittleEndian == byteOrder {
			newDst[oldOffset+0] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x08) & 0xFF)
		} else { // BigEndian == byteOrder
			newDst[oldOffset+1] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+0] = byte((u64 >> 0x08) & 0xFF)
		}
	case reflect.Uint16:
		newDst = oldDst
		newOffset = oldOffset + 2
		u64 = srcObj.Uint()
		if LittleEndian == byteOrder {
			newDst[oldOffset+0] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x08) & 0xFF)
		} else { // BigEndian == byteOrder
			newDst[oldOffset+1] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+0] = byte((u64 >> 0x08) & 0xFF)
		}
	case reflect.Int32:
		newDst = oldDst
		newOffset = oldOffset + 4
		i64 = srcObj.Int()
		if 0 <= i64 {
			u64 = uint64(i64)
		} else {
			u64 = ^uint64(-i64) + 1
		}
		if LittleEndian == byteOrder {
			newDst[oldOffset+0] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x08) & 0xFF)
			newDst[oldOffset+2] = byte((u64 >> 0x10) & 0xFF)
			newDst[oldOffset+3] = byte((u64 >> 0x18) & 0xFF)
		} else { // BigEndian == byteOrder
			newDst[oldOffset+3] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+2] = byte((u64 >> 0x08) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x10) & 0xFF)
			newDst[oldOffset+0] = byte((u64 >> 0x18) & 0xFF)
		}
	case reflect.Uint32:
		newDst = oldDst
		newOffset = oldOffset + 4
		u64 = srcObj.Uint()
		if LittleEndian == byteOrder {
			newDst[oldOffset+0] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x08) & 0xFF)
			newDst[oldOffset+2] = byte((u64 >> 0x10) & 0xFF)
			newDst[oldOffset+3] = byte((u64 >> 0x18) & 0xFF)
		} else { // BigEndian == byteOrder
			newDst[oldOffset+3] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+2] = byte((u64 >> 0x08) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x10) & 0xFF)
			newDst[oldOffset+0] = byte((u64 >> 0x18) & 0xFF)
		}
	case reflect.Int64:
		newDst = oldDst
		newOffset = oldOffset + 8
		i64 = srcObj.Int()
		if 0 <= i64 {
			u64 = uint64(i64)
		} else {
			u64 = ^uint64(-i64) + 1
		}
		if LittleEndian == byteOrder {
			newDst[oldOffset+0] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x08) & 0xFF)
			newDst[oldOffset+2] = byte((u64 >> 0x10) & 0xFF)
			newDst[oldOffset+3] = byte((u64 >> 0x18) & 0xFF)
			newDst[oldOffset+4] = byte((u64 >> 0x20) & 0xFF)
			newDst[oldOffset+5] = byte((u64 >> 0x28) & 0xFF)
			newDst[oldOffset+6] = byte((u64 >> 0x30) & 0xFF)
			newDst[oldOffset+7] = byte((u64 >> 0x38) & 0xFF)
		} else { // BigEndian == byteOrder
			newDst[oldOffset+7] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+6] = byte((u64 >> 0x08) & 0xFF)
			newDst[oldOffset+5] = byte((u64 >> 0x10) & 0xFF)
			newDst[oldOffset+4] = byte((u64 >> 0x18) & 0xFF)
			newDst[oldOffset+3] = byte((u64 >> 0x20) & 0xFF)
			newDst[oldOffset+2] = byte((u64 >> 0x28) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x30) & 0xFF)
			newDst[oldOffset+0] = byte((u64 >> 0x38) & 0xFF)
		}
	case reflect.Uint64:
		newDst = oldDst
		newOffset = oldOffset + 8
		u64 = srcObj.Uint()
		if LittleEndian == byteOrder {
			newDst[oldOffset+0] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x08) & 0xFF)
			newDst[oldOffset+2] = byte((u64 >> 0x10) & 0xFF)
			newDst[oldOffset+3] = byte((u64 >> 0x18) & 0xFF)
			newDst[oldOffset+4] = byte((u64 >> 0x20) & 0xFF)
			newDst[oldOffset+5] = byte((u64 >> 0x28) & 0xFF)
			newDst[oldOffset+6] = byte((u64 >> 0x30) & 0xFF)
			newDst[oldOffset+7] = byte((u64 >> 0x38) & 0xFF)
		} else { // BigEndian == byteOrder
			newDst[oldOffset+7] = byte((u64 >> 0x00) & 0xFF)
			newDst[oldOffset+6] = byte((u64 >> 0x08) & 0xFF)
			newDst[oldOffset+5] = byte((u64 >> 0x10) & 0xFF)
			newDst[oldOffset+4] = byte((u64 >> 0x18) & 0xFF)
			newDst[oldOffset+3] = byte((u64 >> 0x20) & 0xFF)
			newDst[oldOffset+2] = byte((u64 >> 0x28) & 0xFF)
			newDst[oldOffset+1] = byte((u64 >> 0x30) & 0xFF)
			newDst[oldOffset+0] = byte((u64 >> 0x38) & 0xFF)
		}
	case reflect.Array:
		newDst = oldDst
		newOffset = oldOffset
		for i := 0; i < srcObj.Len(); i++ {
			newDst, newOffset = packRecursive(srcObj.Index(i), newDst, newOffset, byteOrder)
		}
	case reflect.Struct:
		newDst = oldDst
		newOffset = oldOffset
		for i := 0; i < srcObj.NumField(); i++ {
			newDst, newOffset = packRecursive(srcObj.Field(i), newDst, newOffset, byteOrder)
		}
	case reflect.Slice:
		newDst = append(oldDst, srcObj.Bytes()...)
		newOffset = uint64(len(newDst))
	}

	return
}

func unpackRecursive(src []byte, oldOffset uint64, dstObj reflect.Value, byteOrder binary.ByteOrder) (newOffset uint64) {
	var i64 int64
	var u64 uint64

	switch dstObj.Kind() {
	case reflect.Interface:
		newOffset = unpackRecursive(src, oldOffset, dstObj.Elem(), byteOrder)
	case reflect.Ptr:
		newOffset = unpackRecursive(src, oldOffset, dstObj.Elem(), byteOrder)
	case reflect.Bool:
		u64 = uint64(src[oldOffset+0])
		if 0x00 == u64 {
			dstObj.SetBool(false)
		} else {
			dstObj.SetBool(true)
		}
		newOffset = oldOffset + 1
	case reflect.Int8:
		u64 = uint64(src[oldOffset+0])
		if 0 == ((u64 >> 0x07) & 0x01) {
			i64 = int64(u64)
		} else {
			i64 = -int64(^((u64 - 1) | uint64(0xFFFFFFFFFFFFFF00)))
		}
		dstObj.SetInt(i64)
		newOffset = oldOffset + 1
	case reflect.Uint8:
		u64 = uint64(src[oldOffset+0])
		dstObj.SetUint(u64)
		newOffset = oldOffset + 1
	case reflect.Int16:
		if LittleEndian == byteOrder {
			u64 = uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+0])
		} else { // BigEndian == byteOrder
			u64 = uint64(src[oldOffset+0])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
		}
		if 0 == ((u64 >> 0x0F) & 0x01) {
			i64 = int64(u64)
		} else {
			i64 = -int64(^((u64 - 1) | uint64(0xFFFFFFFFFFFF0000)))
		}
		dstObj.SetInt(i64)
		newOffset = oldOffset + 2
	case reflect.Uint16:
		if LittleEndian == byteOrder {
			u64 = uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+0])
		} else { // BigEndian == byteOrder
			u64 = uint64(src[oldOffset+0])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
		}
		dstObj.SetUint(u64)
		newOffset = oldOffset + 2
	case reflect.Int32:
		if LittleEndian == byteOrder {
			u64 = uint64(src[oldOffset+3])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+2])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+0])
		} else { // BigEndian == byteOrder
			u64 = uint64(src[oldOffset+0])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+2])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+3])
		}
		if 0 == ((u64 >> 0x1F) & 0x01) {
			i64 = int64(u64)
		} else {
			i64 = -int64(^((u64 - 1) | uint64(0xFFFFFFFF00000000)))
		}
		dstObj.SetInt(i64)
		newOffset = oldOffset + 4
	case reflect.Uint32:
		if LittleEndian == byteOrder {
			u64 = uint64(src[oldOffset+3])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+2])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+0])
		} else { // BigEndian == byteOrder
			u64 = uint64(src[oldOffset+0])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+2])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+3])
		}
		dstObj.SetUint(u64)
		newOffset = oldOffset + 4
	case reflect.Int64:
		if LittleEndian == byteOrder {
			u64 = uint64(src[oldOffset+7])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+6])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+5])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+4])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+3])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+2])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+0])
		} else { // BigEndian == byteOrder
			u64 = uint64(src[oldOffset+0])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+2])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+3])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+4])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+5])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+6])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+7])
		}
		if 0 == ((u64 >> 0x3F) & 0x01) {
			i64 = int64(u64)
		} else {
			i64 = -int64(^(u64 - 1))
		}
		dstObj.SetInt(i64)
		newOffset = oldOffset + 8
	case reflect.Uint64:
		if LittleEndian == byteOrder {
			u64 = uint64(src[oldOffset+7])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+6])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+5])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+4])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+3])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+2])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+0])
		} else { // BigEndian == byteOrder
			u64 = uint64(src[oldOffset+0])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+1])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+2])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+3])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+4])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+5])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+6])
			u64 = (u64 << 0x08) + uint64(src[oldOffset+7])
		}
		dstObj.SetUint(u64)
		newOffset = oldOffset + 8
	case reflect.Array:
		newOffset = oldOffset
		for i := 0; i < dstObj.Len(); i++ {
			newOffset = unpackRecursive(src, newOffset, dstObj.Index(i), byteOrder)
		}
	case reflect.Struct:
		newOffset = oldOffset
		for i := 0; i < dstObj.NumField(); i++ {
			newOffset = unpackRecursive(src, newOffset, dstObj.Field(i), byteOrder)
		}
	case reflect.Slice:
		newOffset = uint64(len(src))
		dstObj.SetBytes(src[oldOffset:newOffset])
	}

	return
}
