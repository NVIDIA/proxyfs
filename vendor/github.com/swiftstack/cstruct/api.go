// Package cstruct supports the serialization/deserialization of Go structs
//
// Supported Go struct field types are:
//   bool - stored as 1 Byte value with 0x01 indicating true and 0x00 indicating false
//   int8
//   uint8 - equivalent to byte
//   int16
//   uint16
//   int32
//   uint32
//   int64
//   uint64
//   [<n>]<type> - a counted array of <type>
//   struct - an embedded struct
//   []byte - a trailing arbitrarily long slice of bytes
package cstruct

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

// LittleEndian byte order (i.e. least signficant byte first) for {|u}int{16|32|64} types
var LittleEndian = binary.LittleEndian

// BigEndian byte order (i.e. most signficant byte first) for {|u}int{16|32|64} types
var BigEndian = binary.BigEndian

// Examine may be used to determine the size of a []byte needed by Pack()
func Examine(objIF interface{}) (bytesNeeded uint64, trailingByteSlice bool, err error) {
	obj := reflect.ValueOf(objIF)

	bytesNeeded, trailingByteSlice, err = examineRecursive(obj)

	return
}

// Pack is used to serialize the supplied struct (passed by value or reference) in the desired byte order
func Pack(srcObjIF interface{}, byteOrder binary.ByteOrder) (dst []byte, err error) {
	srcObj := reflect.ValueOf(srcObjIF)

	bytesNeeded, _, err := examineRecursive(srcObj)
	if nil != err {
		dst = nil
		return
	}

	dst = make([]byte, bytesNeeded)

	dst, _ = packRecursive(srcObj, dst, 0, byteOrder)

	return
}

// Unpack is used to deserialize into the supplied struct (passed by reference) in the desired byte order
//
// Note that if the supplied struct contains a trailing byte slice, bytesConsumed will equal len(src)
func Unpack(src []byte, dstObjIF interface{}, byteOrder binary.ByteOrder) (bytesConsumed uint64, err error) {
	dstObj := reflect.ValueOf(dstObjIF)

	bytesNeeded, _, err := examineRecursive(dstObj)
	if nil != err {
		bytesConsumed = 0
		return
	}

	if bytesNeeded > uint64(len(src)) {
		bytesConsumed = bytesNeeded
		err = fmt.Errorf("src only contains %v bytes, need %v", len(src), bytesNeeded)
		return
	}

	bytesConsumed = unpackRecursive(src, 0, dstObj, byteOrder)

	return
}
