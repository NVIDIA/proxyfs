// Package sortedmap provides sorted maps implemented as either a Left-Leaning Red-Black Tree (in memory) or a B+Tree (pageable)
package sortedmap

import (
	"bytes"
	"fmt"
	"strings"
)

type Key interface{}
type Value interface{}

type Compare func(key1 Key, key2 Key) (result int, err error) // returns <0 if key1 < key2, 0 if key1 == key2, >0 if key1 > key2

func CompareInt(key1 Key, key2 Key) (result int, err error) {
	key1Int, ok := key1.(int)
	if !ok {
		err = fmt.Errorf("CompareInt(non-int,) not supported")
		return
	}
	key2Int, ok := key2.(int)
	if !ok {
		err = fmt.Errorf("CompareInt(int, non-int) not supported")
		return
	}

	result = key1Int - key2Int
	err = nil

	return
}

func CompareUint16(key1 Key, key2 Key) (result int, err error) {
	key1Uint16, ok := key1.(uint16)
	if !ok {
		err = fmt.Errorf("CompareUint16(non-uint16,) not supported")
		return
	}
	key2Uint16, ok := key2.(uint16)
	if !ok {
		err = fmt.Errorf("CompareUint16(uint16, non-uint16) not supported")
		return
	}

	if key1Uint16 < key2Uint16 {
		result = -1
	} else if key1Uint16 == key2Uint16 {
		result = 0
	} else { // key1Uint16 > key2Uint16
		result = 1
	}

	err = nil

	return
}

func CompareUint32(key1 Key, key2 Key) (result int, err error) {
	key1Uint32, ok := key1.(uint32)
	if !ok {
		err = fmt.Errorf("CompareUint32(non-uint32,) not supported")
		return
	}
	key2Uint32, ok := key2.(uint32)
	if !ok {
		err = fmt.Errorf("CompareUint32(uint32, non-uint32) not supported")
		return
	}

	if key1Uint32 < key2Uint32 {
		result = -1
	} else if key1Uint32 == key2Uint32 {
		result = 0
	} else { // key1Uint32 > key2Uint32
		result = 1
	}

	err = nil

	return
}

func CompareUint64(key1 Key, key2 Key) (result int, err error) {
	key1Uint64, ok := key1.(uint64)
	if !ok {
		err = fmt.Errorf("CompareUint64(non-uint64,) not supported")
		return
	}
	key2Uint64, ok := key2.(uint64)
	if !ok {
		err = fmt.Errorf("CompareUint64(uint64, non-uint64) not supported")
		return
	}

	if key1Uint64 < key2Uint64 {
		result = -1
	} else if key1Uint64 == key2Uint64 {
		result = 0
	} else { // key1Uint64 > key2Uint64
		result = 1
	}

	err = nil

	return
}

func CompareString(key1 Key, key2 Key) (result int, err error) {
	key1String, ok := key1.(string)
	if !ok {
		err = fmt.Errorf("CompareString(non-string,) not supported")
		return
	}
	key2String, ok := key2.(string)
	if !ok {
		err = fmt.Errorf("CompareString(string, non-string) not supported")
		return
	}

	result = strings.Compare(key1String, key2String)
	err = nil

	return
}

func CompareByteSlice(key1 Key, key2 Key) (result int, err error) {
	key1Slice, ok := key1.([]byte)
	if !ok {
		err = fmt.Errorf("CompareByteSlice(non-[]byte,) not supported")
		return
	}
	key2Slice, ok := key2.([]byte)
	if !ok {
		err = fmt.Errorf("CompareByteSlice([]byte, non-[]byte) not supported")
		return
	}

	result = bytes.Compare(key1Slice, key2Slice)
	err = nil

	return
}

type SortedMap interface {
	BisectLeft(key Key) (index int, found bool, err error)  // Returns index of matching key:value pair or, if no match, index is to key:value just before where this key would go
	BisectRight(key Key) (index int, found bool, err error) // Returns index of matching key:value pair or, if no match, index is to key:value just after where this key would go
	DeleteByIndex(index int) (ok bool, err error)
	DeleteByKey(key Key) (ok bool, err error)
	Dump() (err error)
	GetByIndex(index int) (key Key, value Value, ok bool, err error)
	GetByKey(key Key) (value Value, ok bool, err error)
	Len() (numberOfItems int, err error)
	PatchByIndex(index int, value Value) (ok bool, err error)
	PatchByKey(key Key, value Value) (ok bool, err error)
	Put(key Key, value Value) (ok bool, err error)
	Validate() (err error)
}

// DumpCallbacks specifies the interface to a set of callbacks provided by the client
type DumpCallbacks interface {
	DumpKey(key Key) (keyAsString string, err error)
	DumpValue(value Value) (valueAsString string, err error)
}
