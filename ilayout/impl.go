// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ilayout

import (
	"fmt"
	"math"
	"time"
)

func unmarshalCheckPointVersion(checkpointString string) (checkPointVersion uint64, err error) {
	_, err = fmt.Sscanf(checkpointString, "%016X", &checkPointVersion)

	return
}

func (checkPointV1 *CheckPointV1Struct) marshalCheckPointV1() (checkPointV1String string, err error) {
	checkPointV1String = fmt.Sprintf("%016X %016X %016X %016X", checkPointV1.Version, checkPointV1.SuperBlockObjectNumber, checkPointV1.SuperBlockLength, checkPointV1.ReservedToNonce)

	err = nil
	return
}

func unmarshalCheckPointV1(checkPointV1String string) (checkPointV1 *CheckPointV1Struct, err error) {
	checkPointV1 = &CheckPointV1Struct{}

	_, err = fmt.Sscanf(checkPointV1String, "%016X %016X %016X %016X", &checkPointV1.Version, &checkPointV1.SuperBlockObjectNumber, &checkPointV1.SuperBlockLength, &checkPointV1.ReservedToNonce)
	if (nil == err) && (CheckPointVersionV1 != checkPointV1.Version) {
		err = fmt.Errorf("version mismatch... found %016X... expected %016X", checkPointV1.Version, CheckPointVersionV1)
	}

	return
}

func (objectTrailer *ObjectTrailerStruct) marshalObjectTrailer() (objectTrailerBuf []byte, err error) {
	var (
		curPos int
	)

	objectTrailerBuf = make([]byte, 2+2+4)

	curPos = 0

	curPos, err = putLEUint16ToBuf(objectTrailerBuf, curPos, objectTrailer.ObjType)
	if nil != err {
		return
	}

	curPos, err = putLEUint16ToBuf(objectTrailerBuf, curPos, objectTrailer.Version)
	if nil != err {
		return
	}

	curPos, err = putLEUint32ToBuf(objectTrailerBuf, curPos, objectTrailer.Length)
	if nil != err {
		return
	}

	if curPos != len(objectTrailerBuf) {
		err = fmt.Errorf("curPos != len(objectTrailerBuf)")
		return
	}

	err = nil
	return
}

func unmarshalObjectTrailer(objectTrailerBuf []byte) (objectTrailer *ObjectTrailerStruct, err error) {
	var (
		curPos         int
		expectedLength uint32
	)

	curPos = len(objectTrailerBuf) - (2 + 2 + 4)
	if curPos < 0 {
		err = fmt.Errorf("no room for ObjectTrailerStruct at end of objectTrailerBuf")
		return
	}
	if curPos > math.MaxUint32 {
		err = fmt.Errorf("cannot parse an objectTrailerBuf with > math.MaxUint32 (0x%8X) payload preceeding ObjectTrailerStruct", math.MaxUint32)
		return
	}

	expectedLength = uint32(curPos)

	objectTrailer = &ObjectTrailerStruct{}

	objectTrailer.ObjType, curPos, err = getLEUint16FromBuf(objectTrailerBuf, curPos)
	if nil != err {
		return
	}

	objectTrailer.Version, curPos, err = getLEUint16FromBuf(objectTrailerBuf, curPos)
	if nil != err {
		return
	}

	objectTrailer.Length, curPos, err = getLEUint32FromBuf(objectTrailerBuf, curPos)
	if nil != err {
		return
	}

	if curPos != len(objectTrailerBuf) {
		err = fmt.Errorf("logic error extracting ObjectTrailerStruct from end of objectTrailerBuf")
		return
	}

	if objectTrailer.Length != expectedLength {
		err = fmt.Errorf("payload of objectTrailerBuf preceeding ObjectTrailerStruct length mismatch")
		return
	}

	err = nil
	return
}

func (superBlockV1 *SuperBlockV1Struct) marshalSuperBlockV1() (superBlockV1Buf []byte, err error) {
	var (
		curPos                              int
		inodeTableLayoutIndex               int
		objectTrailer                       *ObjectTrailerStruct
		objectTrailerBuf                    []byte
		pendingDeleteObjectNumberArrayIndex int
	)

	superBlockV1Buf = make([]byte, 8+8+8+8+(len(superBlockV1.InodeTableLayout)*(8+8+8))+8+8+8+8+(len(superBlockV1.PendingDeleteObjectNumberArray)*8)+(2+2+4))

	curPos = 0

	curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableRootObjectNumber)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableRootObjectOffset)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableRootObjectLength)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, uint64(len(superBlockV1.InodeTableLayout)))
	if nil != err {
		return
	}

	for inodeTableLayoutIndex = 0; inodeTableLayoutIndex < len(superBlockV1.InodeTableLayout); inodeTableLayoutIndex++ {
		curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectNumber)
		if nil != err {
			return
		}

		curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectSize)
		if nil != err {
			return
		}

		curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableLayout[inodeTableLayoutIndex].BytesReferenced)
		if nil != err {
			return
		}
	}

	curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeObjectCount)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeObjectSize)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeBytesReferenced)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, uint64(len(superBlockV1.PendingDeleteObjectNumberArray)))
	if nil != err {
		return
	}

	for pendingDeleteObjectNumberArrayIndex = 0; pendingDeleteObjectNumberArrayIndex < len(superBlockV1.PendingDeleteObjectNumberArray); pendingDeleteObjectNumberArrayIndex++ {
		curPos, err = putLEUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.PendingDeleteObjectNumberArray[pendingDeleteObjectNumberArrayIndex])
		if nil != err {
			return
		}
	}

	if curPos > math.MaxUint32 {
		err = fmt.Errorf("cannot marshal an superBlockV1Buf with > math.MaxUint32 (0x%8X) payload preceeding ObjectTrailerStruct", math.MaxUint32)
		return
	}

	objectTrailer = &ObjectTrailerStruct{
		ObjType: SuperBlockType,
		Version: SuperBlockVersionV1,
		Length:  uint32(curPos),
	}

	objectTrailerBuf, err = objectTrailer.MarshalObjectTrailer()
	if nil != err {
		return
	}

	curPos, err = putFixedByteSliceToBuf(superBlockV1Buf, curPos, objectTrailerBuf)
	if nil != err {
		return
	}

	if curPos != len(superBlockV1Buf) {
		err = fmt.Errorf("curPos != len(superBlockV1Buf)")
		return
	}

	err = nil
	return
}

func unmarshalSuperBlockV1(superBlockV1Buf []byte) (superBlockV1 *SuperBlockV1Struct, err error) {
	var (
		curPos                              int
		inodeTableLayoutIndex               uint64
		inodeTableLayoutLen                 uint64
		objectTrailer                       *ObjectTrailerStruct
		pendingDeleteObjectNumberArrayIndex uint64
		pendingDeleteObjectNumberArrayLen   uint64
	)

	objectTrailer, err = unmarshalObjectTrailer(superBlockV1Buf)
	if nil != err {
		return
	}
	if objectTrailer.ObjType != SuperBlockType {
		err = fmt.Errorf("superBlockV1Buf does not contain a SuperBlockV1Struct - wrong ObjType")
		return
	}
	if objectTrailer.Version != SuperBlockVersionV1 {
		err = fmt.Errorf("superBlockV1Buf does not contain a SuperBlockV1Struct - wrong Version")
		return
	}

	superBlockV1 = &SuperBlockV1Struct{}

	curPos = 0

	superBlockV1.InodeTableRootObjectNumber, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.InodeTableRootObjectOffset, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.InodeTableRootObjectLength, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	inodeTableLayoutLen, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.InodeTableLayout = make([]InodeTableLayoutEntryV1Struct, inodeTableLayoutLen)

	for inodeTableLayoutIndex = 0; inodeTableLayoutIndex < inodeTableLayoutLen; inodeTableLayoutIndex++ {
		superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectNumber, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}

		superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectSize, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}

		superBlockV1.InodeTableLayout[inodeTableLayoutIndex].BytesReferenced, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}
	}

	superBlockV1.InodeObjectCount, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.InodeObjectSize, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.InodeBytesReferenced, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	pendingDeleteObjectNumberArrayLen, curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.PendingDeleteObjectNumberArray = make([]uint64, pendingDeleteObjectNumberArrayLen)

	for pendingDeleteObjectNumberArrayIndex = 0; pendingDeleteObjectNumberArrayIndex < pendingDeleteObjectNumberArrayLen; pendingDeleteObjectNumberArrayIndex++ {
		superBlockV1.PendingDeleteObjectNumberArray[pendingDeleteObjectNumberArrayIndex], curPos, err = getLEUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}
	}

	if curPos != int(objectTrailer.Length) {
		err = fmt.Errorf("incorrect size for superBlockV1Buf")
		return
	}

	err = nil
	return
}

func unmarshalInodeTableEntryValueVersion(inodeTableEntryValueBuf []byte) (inodeTableEntryValueVersion uint64, err error) {
	inodeTableEntryValueVersion, _, err = getLEUint64FromBuf(inodeTableEntryValueBuf, 0)

	return
}

func (inodeTableEntryValueV1 *InodeTableEntryValueV1Struct) marshalInodeTableEntryValueV1() (inodeTableEntryValueV1Buf []byte, err error) {
	var (
		curPos int
	)

	inodeTableEntryValueV1Buf = make([]byte, 8+8+8)

	curPos = 0

	curPos, err = putLEUint64ToBuf(inodeTableEntryValueV1Buf, curPos, InodeTableEntryValueVersionV1)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeTableEntryValueV1Buf, curPos, inodeTableEntryValueV1.InodeHeadObjectNumber)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeTableEntryValueV1Buf, curPos, inodeTableEntryValueV1.InodeHeadLength)
	if nil != err {
		return
	}

	if curPos != len(inodeTableEntryValueV1Buf) {
		err = fmt.Errorf("curPos != len(inodeTableEntryValueV1Buf)")
		return
	}

	err = nil
	return
}

func unmarshalInodeTableEntryValueV1(inodeTableEntryValueV1Buf []byte) (inodeTableEntryValueV1 *InodeTableEntryValueV1Struct, bytesConsumed int, err error) {
	var (
		curPos                      int
		inodeTableEntryValueVersion uint64
	)

	curPos = 0

	inodeTableEntryValueVersion, curPos, err = getLEUint64FromBuf(inodeTableEntryValueV1Buf, curPos)
	if nil != err {
		return
	}
	if InodeTableEntryValueVersionV1 != inodeTableEntryValueVersion {
		err = fmt.Errorf("incorrect Version for inodeTableEntryValueV1Buf")
		return
	}

	inodeTableEntryValueV1 = &InodeTableEntryValueV1Struct{}

	inodeTableEntryValueV1.InodeHeadObjectNumber, curPos, err = getLEUint64FromBuf(inodeTableEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	inodeTableEntryValueV1.InodeHeadLength, curPos, err = getLEUint64FromBuf(inodeTableEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	bytesConsumed = curPos

	err = nil
	return
}

func (inodeHeadV1 *InodeHeadV1Struct) marshalInodeHeadV1() (inodeHeadV1Buf []byte, err error) {
	var (
		curPos            int
		inodeHeadV1BufLen int
		layoutIndex       int
		linkTableIndex    int
		objectTrailer     *ObjectTrailerStruct
		objectTrailerBuf  []byte
		streamTableIndex  int
	)

	inodeHeadV1BufLen = 8 + 1

	inodeHeadV1BufLen += 8

	for linkTableIndex = 0; linkTableIndex < len(inodeHeadV1.LinkTable); linkTableIndex++ {
		inodeHeadV1BufLen += 8 + 8 + len(inodeHeadV1.LinkTable[linkTableIndex].ParentDirEntryName)
	}

	inodeHeadV1BufLen += 8 + 8 + 8 + 2 + 8 + 8

	inodeHeadV1BufLen += 8

	for streamTableIndex = 0; streamTableIndex < len(inodeHeadV1.StreamTable); streamTableIndex++ {
		inodeHeadV1BufLen += 8 + len(inodeHeadV1.StreamTable[streamTableIndex].Name) + 8 + len(inodeHeadV1.StreamTable[streamTableIndex].Value)
	}

	inodeHeadV1BufLen += 8 + 8 + 8

	inodeHeadV1BufLen += 8 + len(inodeHeadV1.SymLinkTarget)

	inodeHeadV1BufLen += 8 + (len(inodeHeadV1.Layout) * (8 + 8 + 8))

	inodeHeadV1BufLen += (2 + 2 + 4)

	inodeHeadV1Buf = make([]byte, inodeHeadV1BufLen)

	curPos = 0

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.InodeNumber)
	if nil != err {
		return
	}

	curPos, err = putLEUint8ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.InodeType)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, uint64(len(inodeHeadV1.LinkTable)))
	if nil != err {
		return
	}

	for linkTableIndex = 0; linkTableIndex < len(inodeHeadV1.LinkTable); linkTableIndex++ {
		curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.LinkTable[linkTableIndex].ParentDirInodeNumber)
		if nil != err {
			return
		}

		curPos, err = putLEStringToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.LinkTable[linkTableIndex].ParentDirEntryName)
		if nil != err {
			return
		}
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Size)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, uint64(inodeHeadV1.ModificationTime.UnixNano()))
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, uint64(inodeHeadV1.StatusChangeTime.UnixNano()))
	if nil != err {
		return
	}

	curPos, err = putLEUint16ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Mode)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.UserID)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.GroupID)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, uint64(len(inodeHeadV1.StreamTable)))
	if nil != err {
		return
	}

	for streamTableIndex = 0; streamTableIndex < len(inodeHeadV1.StreamTable); streamTableIndex++ {
		curPos, err = putLEStringToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.StreamTable[streamTableIndex].Name)
		if nil != err {
			return
		}

		curPos, err = putLEByteSliceToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.StreamTable[streamTableIndex].Value)
		if nil != err {
			return
		}
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.PayloadObjectNumber)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.PayloadObjectOffset)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.PayloadObjectLength)
	if nil != err {
		return
	}

	curPos, err = putLEStringToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.SymLinkTarget)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, uint64(len(inodeHeadV1.Layout)))
	if nil != err {
		return
	}

	for layoutIndex = 0; layoutIndex < len(inodeHeadV1.Layout); layoutIndex++ {
		curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Layout[layoutIndex].ObjectNumber)
		if nil != err {
			return
		}

		curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Layout[layoutIndex].ObjectSize)
		if nil != err {
			return
		}

		curPos, err = putLEUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Layout[layoutIndex].BytesReferenced)
		if nil != err {
			return
		}
	}

	if curPos > math.MaxUint32 {
		err = fmt.Errorf("cannot marshal an inodeHeadV1Buf with > math.MaxUint32 (0x%8X) payload preceeding ObjectTrailerStruct", math.MaxUint32)
		return
	}

	objectTrailer = &ObjectTrailerStruct{
		ObjType: InodeHeadType,
		Version: InodeHeadVersionV1,
		Length:  uint32(curPos),
	}

	objectTrailerBuf, err = objectTrailer.MarshalObjectTrailer()
	if nil != err {
		return
	}

	curPos, err = putFixedByteSliceToBuf(inodeHeadV1Buf, curPos, objectTrailerBuf)
	if nil != err {
		return
	}

	if curPos != len(inodeHeadV1Buf) {
		err = fmt.Errorf("curPos != len(inodeHeadV1Buf)")
		return
	}

	err = nil
	return
}

func unmarshalInodeHeadV1(inodeHeadV1Buf []byte) (inodeHeadV1 *InodeHeadV1Struct, err error) {
	var (
		curPos                         int
		layoutIndex                    uint64
		layoutLen                      uint64
		linkTableIndex                 uint64
		linkTableLen                   uint64
		modificationTimeAsUnixTimeInNs uint64
		objectTrailer                  *ObjectTrailerStruct
		statusChangeTimeAsUnixTimeInNs uint64
		streamTableIndex               uint64
		streamTableLen                 uint64
	)

	objectTrailer, err = unmarshalObjectTrailer(inodeHeadV1Buf)
	if nil != err {
		return
	}
	if objectTrailer.ObjType != InodeHeadType {
		err = fmt.Errorf("inodeHeadV1Buf does not contain a InodeHeadV1Struct - wrong ObjType")
		return
	}
	if objectTrailer.Version != InodeHeadVersionV1 {
		err = fmt.Errorf("inodeHeadV1Buf does not contain a InodeHeadV1Struct - wrong Version")
		return
	}

	inodeHeadV1 = &InodeHeadV1Struct{}

	curPos = 0

	inodeHeadV1.InodeNumber, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.InodeType, curPos, err = getLEUint8FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	linkTableLen, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.LinkTable = make([]InodeLinkTableEntryStruct, linkTableLen)

	for linkTableIndex = 0; linkTableIndex < linkTableLen; linkTableIndex++ {
		inodeHeadV1.LinkTable[linkTableIndex].ParentDirInodeNumber, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
		if nil != err {
			return
		}

		inodeHeadV1.LinkTable[linkTableIndex].ParentDirEntryName, curPos, err = getLEStringFromBuf(inodeHeadV1Buf, curPos)
		if nil != err {
			return
		}
	}

	inodeHeadV1.Size, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	modificationTimeAsUnixTimeInNs, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.ModificationTime = time.Unix(0, int64(modificationTimeAsUnixTimeInNs))

	statusChangeTimeAsUnixTimeInNs, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.StatusChangeTime = time.Unix(0, int64(statusChangeTimeAsUnixTimeInNs))

	inodeHeadV1.Mode, curPos, err = getLEUint16FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.UserID, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.GroupID, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	streamTableLen, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.StreamTable = make([]InodeStreamTableEntryStruct, streamTableLen)

	for streamTableIndex = 0; streamTableIndex < streamTableLen; streamTableIndex++ {
		inodeHeadV1.StreamTable[streamTableIndex].Name, curPos, err = getLEStringFromBuf(inodeHeadV1Buf, curPos)
		if nil != err {
			return
		}

		inodeHeadV1.StreamTable[streamTableIndex].Value, curPos, err = getLEByteSliceFromBuf(inodeHeadV1Buf, curPos)
		if nil != err {
			return
		}
	}

	inodeHeadV1.PayloadObjectNumber, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.PayloadObjectOffset, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.PayloadObjectLength, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.SymLinkTarget, curPos, err = getLEStringFromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	layoutLen, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
	if nil != err {
		return
	}

	inodeHeadV1.Layout = make([]InodeHeadLayoutEntryV1Struct, layoutLen)

	for layoutIndex = 0; layoutIndex < layoutLen; layoutIndex++ {
		inodeHeadV1.Layout[layoutIndex].ObjectNumber, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
		if nil != err {
			return
		}

		inodeHeadV1.Layout[layoutIndex].ObjectSize, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
		if nil != err {
			return
		}

		inodeHeadV1.Layout[layoutIndex].BytesReferenced, curPos, err = getLEUint64FromBuf(inodeHeadV1Buf, curPos)
		if nil != err {
			return
		}
	}

	if curPos != int(objectTrailer.Length) {
		err = fmt.Errorf("incorrect size for inodeHeadV1Buf")
		return
	}

	err = nil
	return
}

func (directoryEntryValueV1 *DirectoryEntryValueV1Struct) marshalDirectoryEntryValueV1() (directoryEntryValueV1Buf []byte, err error) {
	var (
		curPos int
	)

	directoryEntryValueV1Buf = make([]byte, 8+1)

	curPos = 0

	curPos, err = putLEUint64ToBuf(directoryEntryValueV1Buf, curPos, directoryEntryValueV1.InodeNumber)
	if nil != err {
		return
	}

	curPos, err = putLEUint8ToBuf(directoryEntryValueV1Buf, curPos, directoryEntryValueV1.InodeType)
	if nil != err {
		return
	}

	if curPos != len(directoryEntryValueV1Buf) {
		err = fmt.Errorf("curPos != len(directoryEntryValueV1Buf)")
		return
	}

	err = nil
	return
}

func unmarshalDirectoryEntryValueV1(directoryEntryValueV1Buf []byte) (directoryEntryValueV1 *DirectoryEntryValueV1Struct, bytesConsumed int, err error) {
	var (
		curPos int
	)

	curPos = 0

	directoryEntryValueV1 = &DirectoryEntryValueV1Struct{}

	directoryEntryValueV1.InodeNumber, curPos, err = getLEUint64FromBuf(directoryEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	directoryEntryValueV1.InodeType, curPos, err = getLEUint8FromBuf(directoryEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	bytesConsumed = curPos

	err = nil
	return
}

func (extentMapEntryValueV1 *ExtentMapEntryValueV1Struct) marshalExtentMapEntryValueV1() (extentMapEntryValueV1Buf []byte, err error) {
	var (
		curPos int
	)

	extentMapEntryValueV1Buf = make([]byte, 8+8+8+8)

	curPos = 0

	curPos, err = putLEUint64ToBuf(extentMapEntryValueV1Buf, curPos, extentMapEntryValueV1.FileOffset)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(extentMapEntryValueV1Buf, curPos, extentMapEntryValueV1.Length)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(extentMapEntryValueV1Buf, curPos, extentMapEntryValueV1.ObjectNumber)
	if nil != err {
		return
	}

	curPos, err = putLEUint64ToBuf(extentMapEntryValueV1Buf, curPos, extentMapEntryValueV1.ObjectOffset)
	if nil != err {
		return
	}

	if curPos != len(extentMapEntryValueV1Buf) {
		err = fmt.Errorf("curPos != len(extentMapEntryValueV1Buf)")
		return
	}

	err = nil
	return
}

func unmarshalExtentMapEntryValueV1(extentMapEntryValueV1Buf []byte) (extentMapEntryValueV1 *ExtentMapEntryValueV1Struct, bytesConsumed int, err error) {
	var (
		curPos int
	)

	curPos = 0

	extentMapEntryValueV1 = &ExtentMapEntryValueV1Struct{}

	extentMapEntryValueV1.FileOffset, curPos, err = getLEUint64FromBuf(extentMapEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	extentMapEntryValueV1.Length, curPos, err = getLEUint64FromBuf(extentMapEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	extentMapEntryValueV1.ObjectNumber, curPos, err = getLEUint64FromBuf(extentMapEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	extentMapEntryValueV1.ObjectOffset, curPos, err = getLEUint64FromBuf(extentMapEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	bytesConsumed = curPos

	err = nil
	return
}

func getLEUint8FromBuf(buf []byte, curPos int) (u8 uint8, nextPos int, err error) {
	nextPos = curPos + 1

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for uint8")
		return
	}

	u8 = buf[curPos]

	err = nil
	return
}

func putLEUint8ToBuf(buf []byte, curPos int, u8 uint8) (nextPos int, err error) {
	nextPos = curPos + 1

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for uint8")
		return
	}

	buf[curPos] = u8

	err = nil
	return
}

func getLEUint16FromBuf(buf []byte, curPos int) (u16 uint16, nextPos int, err error) {
	nextPos = curPos + 2

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for uint16")
		return
	}

	u16 = uint16(buf[curPos]) + (uint16(buf[curPos+1]) << 8)
	err = nil
	return
}

func putLEUint16ToBuf(buf []byte, curPos int, u16 uint16) (nextPos int, err error) {
	nextPos = curPos + 2

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for uint16")
		return
	}

	buf[curPos] = uint8(u16 & 0xFF)
	buf[curPos+1] = uint8(u16 >> 8)

	err = nil
	return
}

func getLEUint32FromBuf(buf []byte, curPos int) (u32 uint32, nextPos int, err error) {
	nextPos = curPos + 4

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for uint32")
		return
	}

	u32 = uint32(buf[curPos]) + (uint32(buf[curPos+1]) << 8) + (uint32(buf[curPos+2]) << 16) + (uint32(buf[curPos+3]) << 24)

	err = nil
	return
}

func putLEUint32ToBuf(buf []byte, curPos int, u32 uint32) (nextPos int, err error) {
	nextPos = curPos + 4

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for uint32")
		return
	}

	buf[curPos] = uint8(u32 & 0xFF)
	buf[curPos+1] = uint8((u32 >> 8) & 0xFF)
	buf[curPos+2] = uint8((u32 >> 16) & 0xFF)
	buf[curPos+3] = uint8(u32 >> 24)

	err = nil
	return
}

func getLEUint64FromBuf(buf []byte, curPos int) (u64 uint64, nextPos int, err error) {
	nextPos = curPos + 8

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for uint64")
		return
	}

	u64 = uint64(buf[curPos]) + (uint64(buf[curPos+1]) << 8) + (uint64(buf[curPos+2]) << 16) + (uint64(buf[curPos+3]) << 24) + (uint64(buf[curPos+4]) << 32) + (uint64(buf[curPos+5]) << 40) + (uint64(buf[curPos+6]) << 48) + (uint64(buf[curPos+7]) << 56)
	err = nil

	return
}

func putLEUint64ToBuf(buf []byte, curPos int, u64 uint64) (nextPos int, err error) {
	nextPos = curPos + 8

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for uint64")
		return
	}

	buf[curPos] = uint8(u64 & 0xFF)
	buf[curPos+1] = uint8((u64 >> 8) & 0xFF)
	buf[curPos+2] = uint8((u64 >> 16) & 0xFF)
	buf[curPos+3] = uint8((u64 >> 24) & 0xFF)
	buf[curPos+4] = uint8((u64 >> 32) & 0xFF)
	buf[curPos+5] = uint8((u64 >> 40) & 0xFF)
	buf[curPos+6] = uint8((u64 >> 48) & 0xFF)
	buf[curPos+7] = uint8(u64 >> 56)

	err = nil
	return
}

func getLEStringFromBuf(buf []byte, curPos int) (str string, nextPos int, err error) {
	var (
		strLen uint64
	)

	strLen, nextPos, err = getLEUint64FromBuf(buf, curPos)
	if nil != err {
		return
	}

	if strLen > (uint64(len(buf)) - uint64(nextPos)) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for string of reported length")
		return
	}

	str = string(buf[nextPos : nextPos+int(strLen)])
	nextPos += int(strLen)

	err = nil
	return
}

func putLEStringToBuf(buf []byte, curPos int, str string) (nextPos int, err error) {
	nextPos, err = putLEUint64ToBuf(buf, curPos, uint64(len(str)))
	if nil != err {
		return
	}

	curPos = nextPos
	nextPos += len(str)

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for string")
		return
	}

	copy(buf[curPos:nextPos], []byte(str))

	err = nil
	return
}

func getLEByteSliceFromBuf(buf []byte, curPos int) (byteSlice []byte, nextPos int, err error) {
	var (
		byteSliceLen uint64
	)

	byteSliceLen, nextPos, err = getLEUint64FromBuf(buf, curPos)
	if nil != err {
		return
	}

	if byteSliceLen > (uint64(len(buf)) - uint64(nextPos)) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for []byte of reported length")
		return
	}

	byteSlice = make([]byte, byteSliceLen)
	copy(byteSlice, buf[nextPos:nextPos+int(byteSliceLen)])
	nextPos += int(byteSliceLen)

	err = nil
	return
}

func putLEByteSliceToBuf(buf []byte, curPos int, byteSlice []byte) (nextPos int, err error) {
	nextPos, err = putLEUint64ToBuf(buf, curPos, uint64(len(byteSlice)))
	if nil != err {
		return
	}

	curPos = nextPos
	nextPos += len(byteSlice)

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for []byte")
		return
	}

	copy(buf[curPos:nextPos], byteSlice)

	err = nil
	return
}

func getFixedByteSliceFromBuf(buf []byte, curPos int, byteSlice []byte) (nextPos int, err error) {
	var (
		byteSliceLen int = len(byteSlice)
	)

	if byteSliceLen < (len(buf) - curPos) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for []byte of len(byteSlice) length")
		return
	}

	copy(byteSlice, buf[nextPos:nextPos+byteSliceLen])
	nextPos = curPos + byteSliceLen

	err = nil
	return
}

func putFixedByteSliceToBuf(buf []byte, curPos int, byteSlice []byte) (nextPos int, err error) {
	var (
		byteSliceLen int = len(byteSlice)
	)

	nextPos = curPos + byteSliceLen

	if nextPos > len(buf) {
		err = fmt.Errorf("insufficient space in buf[curPos:] for []byte")
		return
	}

	copy(buf[curPos:nextPos], byteSlice)

	err = nil
	return
}
