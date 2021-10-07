// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"encoding/json"
	"fmt"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/ilayout"
)

func (inode *inodeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		fileOffset uint64
		ok         bool
	)

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		keyAsString, ok = key.(string)
		if ok {
			err = nil
		} else {
			err = fmt.Errorf("key.(string) returned !ok")
		}
	case ilayout.InodeTypeFile:
		fileOffset, ok = key.(uint64)
		if ok {
			keyAsString = fmt.Sprintf("%016X", fileOffset)
			err = nil
		} else {
			err = fmt.Errorf("key.(uint64) returned !ok")
		}
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		directoryEntryValueV1     *ilayout.DirectoryEntryValueV1Struct
		directoryEntryValueV1JSON []byte
		extentMapEntryValueV1     *ilayout.ExtentMapEntryValueV1Struct
		extentMapEntryValueV1JSON []byte
		ok                        bool
	)

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		directoryEntryValueV1, ok = value.(*ilayout.DirectoryEntryValueV1Struct)
		if ok {
			directoryEntryValueV1JSON, err = json.Marshal(directoryEntryValueV1)
			if nil == err {
				valueAsString = string(directoryEntryValueV1JSON[:])
				err = nil
			} else {
				err = fmt.Errorf("json.Marshal(directoryEntryValueV1) failed: %v", err)
			}
		} else {
			err = fmt.Errorf("value.(*ilayout.DirectoryEntryValueV1Struct) returned !ok")
		}
	case ilayout.InodeTypeFile:
		extentMapEntryValueV1, ok = value.(*ilayout.ExtentMapEntryValueV1Struct)
		if ok {
			extentMapEntryValueV1JSON, err = json.Marshal(extentMapEntryValueV1)
			if nil == err {
				valueAsString = string(extentMapEntryValueV1JSON[:])
				err = nil
			} else {
				err = fmt.Errorf("json.Marshal(extentMapEntryValueV1) failed: %v", err)
			}
		} else {
			err = fmt.Errorf("value.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
		}
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	if objectNumber == inode.putObjectNumber {
		if (objectOffset + objectLength) <= uint64(len(inode.putObjectBuffer)) {
			nodeByteSlice = make([]byte, objectLength)
			_ = copy(nodeByteSlice, inode.putObjectBuffer[objectOffset:(objectOffset+objectLength)])
			err = nil
		} else {
			err = fmt.Errorf("(objectOffset + objectLength) > uint64(len(inode.putObjectBuffer))")
		}
	} else {
		nodeByteSlice, err = objectGETRange(objectNumber, objectOffset, objectLength)

		if (nil == err) && (uint64(len(nodeByteSlice)) < objectLength) {
			err = fmt.Errorf("uint64(len(nodeByteSlice)) < objectLength")
		}
	}

	return
}

func (inode *inodeStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inode *inodeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inode *inodeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inode *inodeStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inode *inodeStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inode *inodeStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inode *inodeStruct) newPayload() (err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		inode.payload = sortedmap.NewBPlusTree(
			globals.config.DirInodeMaxKeysPerBPlusTreePage,
			sortedmap.CompareString,
			inode,
			globals.inodePayloadCache)
		err = nil
	case ilayout.InodeTypeFile:
		inode.payload = sortedmap.NewBPlusTree(
			globals.config.FileInodeMaxKeysPerBPlusTreePage,
			sortedmap.CompareUint64,
			inode,
			globals.inodePayloadCache)
		err = nil
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) oldPayload() (err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		inode.payload, err = sortedmap.OldBPlusTree(
			inode.inodeHeadV1.PayloadObjectNumber,
			inode.inodeHeadV1.PayloadObjectOffset,
			inode.inodeHeadV1.PayloadObjectLength,
			sortedmap.CompareString,
			inode,
			globals.inodePayloadCache)
		if nil != err {
			inode.payload = nil
		}
	case ilayout.InodeTypeFile:
		inode.payload, err = sortedmap.OldBPlusTree(
			inode.inodeHeadV1.PayloadObjectNumber,
			inode.inodeHeadV1.PayloadObjectOffset,
			inode.inodeHeadV1.PayloadObjectLength,
			sortedmap.CompareUint64,
			inode,
			globals.inodePayloadCache)
		if nil != err {
			inode.payload = nil
		}
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) convertInodeHeadV1LayoutToLayoutMap() {
	var (
		ilayoutInodeHeadLayoutEntryV1 ilayout.InodeHeadLayoutEntryV1Struct
	)

	inode.layoutMap = make(map[uint64]layoutMapEntryStruct)

	for _, ilayoutInodeHeadLayoutEntryV1 = range inode.inodeHeadV1.Layout {
		inode.layoutMap[ilayoutInodeHeadLayoutEntryV1.ObjectNumber] = layoutMapEntryStruct{
			objectSize:      ilayoutInodeHeadLayoutEntryV1.ObjectSize,
			bytesReferenced: ilayoutInodeHeadLayoutEntryV1.BytesReferenced,
		}
	}
}

func (inode *inodeStruct) convertLayoutMapToInodeHeadV1Layout() {
	var (
		ilayoutInodeHeadV1LayoutIndex uint64 = 0
		layoutMapEntry                layoutMapEntryStruct
		objectNumber                  uint64
	)

	// TODO
	inode.inodeHeadV1.Layout = make([]ilayout.InodeHeadLayoutEntryV1Struct, len(inode.layoutMap))

	for objectNumber, layoutMapEntry = range inode.layoutMap {
		inode.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].ObjectNumber = objectNumber
		inode.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].ObjectSize = layoutMapEntry.objectSize
		inode.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].BytesReferenced = layoutMapEntry.bytesReferenced
	}
}
