// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"encoding/json"
	"fmt"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/ilayout"
)

func (inodeLease *inodeLeaseStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		fileOffset uint64
		ok         bool
	)

	switch inodeLease.inodeHeadV1.InodeType {
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
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inodeLease *inodeLeaseStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		directoryEntryValueV1     *ilayout.DirectoryEntryValueV1Struct
		directoryEntryValueV1JSON []byte
		extentMapEntryValueV1     *ilayout.ExtentMapEntryValueV1Struct
		extentMapEntryValueV1JSON []byte
		ok                        bool
	)

	switch inodeLease.inodeHeadV1.InodeType {
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
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inodeLease *inodeLeaseStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	if objectNumber == inodeLease.putObjectNumber {
		if (objectOffset + objectLength) <= uint64(len(inodeLease.putObjectBuffer)) {
			nodeByteSlice = make([]byte, objectLength)
			_ = copy(nodeByteSlice, inodeLease.putObjectBuffer[objectOffset:(objectOffset+objectLength)])
			err = nil
		} else {
			err = fmt.Errorf("(objectOffset + objectLength) > uint64(len(inodeLease.putObjectBuffer))")
		}
	} else {
		nodeByteSlice, err = objectGETRange(objectNumber, objectOffset, objectLength)

		if (nil == err) && (uint64(len(nodeByteSlice)) < objectLength) {
			err = fmt.Errorf("uint64(len(nodeByteSlice)) < objectLength")
		}
	}

	return
}

func (inodeLease *inodeLeaseStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inodeLease *inodeLeaseStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inodeLease *inodeLeaseStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inodeLease *inodeLeaseStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inodeLease *inodeLeaseStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inodeLease *inodeLeaseStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
	case ilayout.InodeTypeFile:
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}
	return // TODO
}

func (inodeLease *inodeLeaseStruct) newPayload() (err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		inodeLease.payload = sortedmap.NewBPlusTree(
			globals.config.DirInodeMaxKeysPerBPlusTreePage,
			sortedmap.CompareString,
			inodeLease,
			globals.inodeLeasePayloadCache)
		err = nil
	case ilayout.InodeTypeFile:
		inodeLease.payload = sortedmap.NewBPlusTree(
			globals.config.FileInodeMaxKeysPerBPlusTreePage,
			sortedmap.CompareUint64,
			inodeLease,
			globals.inodeLeasePayloadCache)
		err = nil
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inodeLease *inodeLeaseStruct) oldPayload() (err error) {
	switch inodeLease.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		inodeLease.payload, err = sortedmap.OldBPlusTree(
			inodeLease.inodeHeadV1.PayloadObjectNumber,
			inodeLease.inodeHeadV1.PayloadObjectOffset,
			inodeLease.inodeHeadV1.PayloadObjectLength,
			sortedmap.CompareString,
			inodeLease,
			globals.inodeLeasePayloadCache)
		if nil != err {
			inodeLease.payload = nil
		}
	case ilayout.InodeTypeFile:
		inodeLease.payload, err = sortedmap.OldBPlusTree(
			inodeLease.inodeHeadV1.PayloadObjectNumber,
			inodeLease.inodeHeadV1.PayloadObjectOffset,
			inodeLease.inodeHeadV1.PayloadObjectLength,
			sortedmap.CompareUint64,
			inodeLease,
			globals.inodeLeasePayloadCache)
		if nil != err {
			inodeLease.payload = nil
		}
	default:
		err = fmt.Errorf("inodeLease.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inodeLease.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inodeLease *inodeLeaseStruct) convertInodeHeadV1LayoutToLayoutMap() {
	var (
		ilayoutInodeHeadLayoutEntryV1 ilayout.InodeHeadLayoutEntryV1Struct
	)

	inodeLease.layoutMap = make(map[uint64]layoutMapEntryStruct)

	for _, ilayoutInodeHeadLayoutEntryV1 = range inodeLease.inodeHeadV1.Layout {
		inodeLease.layoutMap[ilayoutInodeHeadLayoutEntryV1.ObjectNumber] = layoutMapEntryStruct{
			objectSize:      ilayoutInodeHeadLayoutEntryV1.ObjectSize,
			bytesReferenced: ilayoutInodeHeadLayoutEntryV1.BytesReferenced,
		}
	}
}

func (inodeLease *inodeLeaseStruct) convertLayoutMapToInodeHeadV1Layout() {
	var (
		ilayoutInodeHeadV1LayoutIndex uint64 = 0
		layoutMapEntry                layoutMapEntryStruct
		objectNumber                  uint64
	)

	// TODO
	inodeLease.inodeHeadV1.Layout = make([]ilayout.InodeHeadLayoutEntryV1Struct, len(inodeLease.layoutMap))

	for objectNumber, layoutMapEntry = range inodeLease.layoutMap {
		inodeLease.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].ObjectNumber = objectNumber
		inodeLease.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].ObjectSize = layoutMapEntry.objectSize
		inodeLease.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].BytesReferenced = layoutMapEntry.bytesReferenced
	}
}
