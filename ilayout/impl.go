// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ilayout

import (
	"fmt"
	"time"
)

func unmarshalCheckPointHeaderVersion(checkpointHeaderString string) (checkPointHeaderVersion uint64, err error) {
	_, err = fmt.Sscanf(checkpointHeaderString, "%016X", &checkPointHeaderVersion)

	return
}

func (checkPointHeaderV1 *CheckPointHeaderV1Struct) marshalCheckPointHeaderV1() (checkPointHeaderV1String string, err error) {
	checkPointHeaderV1String = fmt.Sprintf("%016X %016X %016X %016X", checkPointHeaderV1.Version, checkPointHeaderV1.SuperBlockObjectNumber, checkPointHeaderV1.SuperBlockLength, checkPointHeaderV1.ReservedToNonce)

	err = nil
	return
}

func unmarshalCheckPointHeaderV1(checkPointHeaderV1String string) (checkPointHeaderV1 *CheckPointHeaderV1Struct, err error) {
	checkPointHeaderV1 = &CheckPointHeaderV1Struct{}

	_, err = fmt.Sscanf(checkPointHeaderV1String, "%016X %016X %016X %016X", &checkPointHeaderV1.Version, &checkPointHeaderV1.SuperBlockObjectNumber, &checkPointHeaderV1.SuperBlockLength, &checkPointHeaderV1.ReservedToNonce)
	if (nil == err) && (CheckPointHeaderVersionV1 != checkPointHeaderV1.Version) {
		err = fmt.Errorf("Version mismatch... found %016X... expected %016X", checkPointHeaderV1.Version, CheckPointHeaderVersionV1)
	}

	return
}

func unmarshalSuperBlockVersion(superBlockBuf []byte) (superBlockVersion uint64, err error) {
	var (
		superBlockBufLen int
	)

	superBlockBufLen = len(superBlockBuf)

	if superBlockBufLen < 8 {
		err = fmt.Errorf("Insufficient space in superBlockBuf for superBlockVersion")
		return
	}

	superBlockVersion, _, err = leGetUint64FromBuf(superBlockBuf, superBlockBufLen-8)

	return
}

func (superBlockV1 *SuperBlockV1Struct) marshalSuperBlockV1() (superBlockV1Buf []byte, err error) {
	var (
		curPos                int
		inodeTableLayoutIndex int
	)

	superBlockV1Buf = make([]byte, 8+8+8+8+(len(superBlockV1.InodeTableLayout)*(8+8+8))+8)

	curPos = 0

	curPos, err = lePutUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableRootObjectNumber)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableRootObjectOffset)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableRootObjectLength)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(superBlockV1Buf, curPos, uint64(len(superBlockV1.InodeTableLayout)))
	if nil != err {
		return
	}

	for inodeTableLayoutIndex = 0; inodeTableLayoutIndex < len(superBlockV1.InodeTableLayout); inodeTableLayoutIndex++ {
		curPos, err = lePutUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectNumber)
		if nil != err {
			return
		}

		curPos, err = lePutUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectSize)
		if nil != err {
			return
		}

		curPos, err = lePutUint64ToBuf(superBlockV1Buf, curPos, superBlockV1.InodeTableLayout[inodeTableLayoutIndex].BytesReferenced)
		if nil != err {
			return
		}
	}

	curPos, err = lePutUint64ToBuf(superBlockV1Buf, curPos, SuperBlockVersionV1)
	if nil != err {
		return
	}

	err = nil
	return
}

func unmarshalSuperBlockV1(superBlockV1Buf []byte) (superBlockV1 *SuperBlockV1Struct, err error) {
	var (
		curPos                int
		inodeTableLayoutIndex uint64
		inodeTableLayoutLen   uint64
		superBlockVersion     uint64
	)

	superBlockV1 = &SuperBlockV1Struct{}

	curPos = 0

	superBlockV1.InodeTableRootObjectNumber, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.InodeTableRootObjectOffset, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.InodeTableRootObjectLength, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	inodeTableLayoutLen, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}

	superBlockV1.InodeTableLayout = make([]InodeTableLayoutEntryV1Struct, inodeTableLayoutLen)

	for inodeTableLayoutIndex = 0; inodeTableLayoutIndex < inodeTableLayoutLen; inodeTableLayoutIndex++ {
		superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectNumber, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}

		superBlockV1.InodeTableLayout[inodeTableLayoutIndex].ObjectSize, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}

		superBlockV1.InodeTableLayout[inodeTableLayoutIndex].BytesReferenced, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}
	}

	superBlockVersion, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
	if nil != err {
		return
	}
	if superBlockVersion != SuperBlockVersionV1 {
		err = fmt.Errorf("Incorrect Version for superBlockV1Buf")
		return
	}

	if curPos != len(superBlockV1Buf) {
		err = fmt.Errorf("Incorrect size for superBlockV1Buf")
		return
	}

	err = nil
	return
}

func unmarshalInodeTableEntryValueVersion(inodeTableEntryValueBuf []byte) (inodeTableEntryValueVersion uint64, err error) {
	inodeTableEntryValueVersion, _, err = leGetUint64FromBuf(inodeTableEntryValueBuf, 0)

	return
}

func (inodeTableEntryValueV1 *InodeTableEntryValueV1Struct) marshalInodeTableEntryValueV1() (inodeTableEntryValueV1Buf []byte, err error) {
	var (
		curPos int
	)

	inodeTableEntryValueV1Buf = make([]byte, 8+8+8)

	curPos = 0

	curPos, err = lePutUint64ToBuf(inodeTableEntryValueV1Buf, curPos, InodeTableEntryValueVersionV1)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeTableEntryValueV1Buf, curPos, inodeTableEntryValueV1.InodeHeadObjectNumber)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeTableEntryValueV1Buf, curPos, inodeTableEntryValueV1.InodeHeadLength)
	if nil != err {
		return
	}

	err = nil
	return
}

func unmarshalInodeTableEntryValueV1(inodeTableEntryValueV1Buf []byte) (inodeTableEntryValueV1 *InodeTableEntryValueV1Struct, err error) {
	var (
		curPos                      int
		inodeTableEntryValueVersion uint64
	)

	curPos = 0

	inodeTableEntryValueVersion, curPos, err = leGetUint64FromBuf(inodeTableEntryValueV1Buf, curPos)
	if nil != err {
		return
	}
	if InodeTableEntryValueVersionV1 != inodeTableEntryValueVersion {
		err = fmt.Errorf("Incorrect Version for inodeTableEntryValueV1Buf")
		return
	}

	inodeTableEntryValueV1 = &InodeTableEntryValueV1Struct{}

	inodeTableEntryValueV1.InodeHeadObjectNumber, curPos, err = leGetUint64FromBuf(inodeTableEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	inodeTableEntryValueV1.InodeHeadLength, curPos, err = leGetUint64FromBuf(inodeTableEntryValueV1Buf, curPos)
	if nil != err {
		return
	}

	if curPos != len(inodeTableEntryValueV1Buf) {
		err = fmt.Errorf("Incorrect size for inodeTableEntryValueV1Buf")
		return
	}

	err = nil
	return
}

func unmarshalInodeHeadVersion(inodeHeadBuf []byte) (inodeHeadVersion uint64, err error) {
	var (
		inodeHeadBufLen int
	)

	inodeHeadBufLen = len(inodeHeadBuf)

	if inodeHeadBufLen < 8 {
		err = fmt.Errorf("Insufficient space in inodeHeadBuf for inodeHeadVersion")
		return
	}

	inodeHeadVersion, _, err = leGetUint64FromBuf(inodeHeadBuf, inodeHeadBufLen-8)

	return
}

func (inodeHeadV1 *InodeHeadV1Struct) marshalInodeHeadV1() (inodeHeadV1Buf []byte, err error) {
	var (
		accessTimeAsRFC3339String       string
		attrChangeTimeAsRFC3339String   string
		creationTimeAsRFC3339String     string
		curPos                          int
		inodeHeadV1BufLen               int
		layoutIndex                     int
		linkTableIndex                  int
		modificationTimeAsRFC3339String string
		streamTableIndex                int
	)

	creationTimeAsRFC3339String = inodeHeadV1.CreationTime.Format(time.RFC3339)
	modificationTimeAsRFC3339String = inodeHeadV1.ModificationTime.Format(time.RFC3339)
	accessTimeAsRFC3339String = inodeHeadV1.AccessTime.Format(time.RFC3339)
	attrChangeTimeAsRFC3339String = inodeHeadV1.AttrChangeTime.Format(time.RFC3339)

	inodeHeadV1BufLen = 8 + 1

	inodeHeadV1BufLen += 8

	for linkTableIndex = 0; linkTableIndex < len(inodeHeadV1.LinkTable); linkTableIndex++ {
		inodeHeadV1BufLen += 8 + 8 + len(inodeHeadV1.LinkTable[linkTableIndex].ParentDirEntryName)
	}

	inodeHeadV1BufLen += 8

	inodeHeadV1BufLen += (8 + len(creationTimeAsRFC3339String)) + (8 + len(modificationTimeAsRFC3339String)) + (8 + len(accessTimeAsRFC3339String)) + (8 + len(attrChangeTimeAsRFC3339String))

	inodeHeadV1BufLen += 2 + 8 + 8

	inodeHeadV1BufLen += 8

	for streamTableIndex = 0; streamTableIndex < len(inodeHeadV1.StreamTable); streamTableIndex++ {
		inodeHeadV1BufLen += 8 + len(inodeHeadV1.StreamTable[streamTableIndex].Name) + 8 + len(inodeHeadV1.StreamTable[streamTableIndex].Value)
	}

	inodeHeadV1BufLen += 8 + 8 + 8

	inodeHeadV1BufLen += 8 + len(inodeHeadV1.SymlinkTarget)

	inodeHeadV1BufLen += 8 + (len(inodeHeadV1.Layout) * (8 + 8 + 8))

	inodeHeadV1BufLen += 8

	inodeHeadV1Buf = make([]byte, inodeHeadV1BufLen)

	curPos = 0

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.InodeNumber)
	if nil != err {
		return
	}

	curPos, err = lePutUint8ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.InodeType)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, uint64(len(inodeHeadV1.LinkTable)))
	if nil != err {
		return
	}

	for linkTableIndex = 0; linkTableIndex < len(inodeHeadV1.LinkTable); linkTableIndex++ {
		curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.LinkTable[linkTableIndex].ParentDirInodeNumber)
		if nil != err {
			return
		}

		curPos, err = lePutStringToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.LinkTable[linkTableIndex].ParentDirEntryName)
		if nil != err {
			return
		}
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Size)
	if nil != err {
		return
	}

	curPos, err = lePutStringToBuf(inodeHeadV1Buf, curPos, creationTimeAsRFC3339String)
	if nil != err {
		return
	}

	curPos, err = lePutStringToBuf(inodeHeadV1Buf, curPos, modificationTimeAsRFC3339String)
	if nil != err {
		return
	}

	curPos, err = lePutStringToBuf(inodeHeadV1Buf, curPos, accessTimeAsRFC3339String)
	if nil != err {
		return
	}

	curPos, err = lePutStringToBuf(inodeHeadV1Buf, curPos, attrChangeTimeAsRFC3339String)
	if nil != err {
		return
	}

	curPos, err = lePutUint16ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Mode)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.UserID)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.GroupID)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, uint64(len(inodeHeadV1.StreamTable)))
	if nil != err {
		return
	}

	for streamTableIndex = 0; streamTableIndex < len(inodeHeadV1.StreamTable); streamTableIndex++ {
		curPos, err = lePutStringToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.StreamTable[streamTableIndex].Name)
		if nil != err {
			return
		}

		curPos, err = lePutByteSliceToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.StreamTable[streamTableIndex].Value)
		if nil != err {
			return
		}
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.PayloadObjectNumber)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.PayloadObjectOffset)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.PayloadObjectLength)
	if nil != err {
		return
	}

	curPos, err = lePutStringToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.SymlinkTarget)
	if nil != err {
		return
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, uint64(len(inodeHeadV1.Layout)))
	if nil != err {
		return
	}

	for layoutIndex = 0; layoutIndex < len(inodeHeadV1.Layout); layoutIndex++ {
		curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Layout[layoutIndex].ObjectNumber)
		if nil != err {
			return
		}

		curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Layout[layoutIndex].ObjectSize)
		if nil != err {
			return
		}

		curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, inodeHeadV1.Layout[layoutIndex].BytesReferenced)
		if nil != err {
			return
		}
	}

	curPos, err = lePutUint64ToBuf(inodeHeadV1Buf, curPos, InodeHeadVersionV1)
	if nil != err {
		return
	}

	err = nil
	return
}

func unmarshalInodeHeadV1(inodeHeadV1Buf []byte) (inodeHeadV1 *InodeHeadV1Struct, err error) {
	return nil, fmt.Errorf("TODO - unmarshalInodeHeadV1()")
}

func leGetUint8FromBuf(buf []byte, curPos int) (u8 uint8, nextPos int, err error) {
	nextPos = curPos + 1

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for uint8")
		return
	}

	u8 = buf[curPos]

	err = nil
	return
}

func lePutUint8ToBuf(buf []byte, curPos int, u8 uint8) (nextPos int, err error) {
	nextPos = curPos + 1

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for uint8")
		return
	}

	buf[curPos] = u8

	err = nil
	return
}

func leGetUint16FromBuf(buf []byte, curPos int) (u16 uint16, nextPos int, err error) {
	nextPos = curPos + 2

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for uint16")
		return
	}

	u16 = uint16(buf[curPos]) + (uint16(buf[curPos+1]) << 8)
	err = nil
	return
}

func lePutUint16ToBuf(buf []byte, curPos int, u16 uint16) (nextPos int, err error) {
	nextPos = curPos + 2

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for uint16")
		return
	}

	buf[curPos] = uint8(u16 & 0xFF)
	buf[curPos+1] = uint8(u16 >> 8)

	err = nil
	return
}

func leGetUint32FromBuf(buf []byte, curPos int) (u32 uint32, nextPos int, err error) {
	nextPos = curPos + 4

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for uint32")
		return
	}

	u32 = uint32(buf[curPos]) + (uint32(buf[curPos+1]) << 8) + (uint32(buf[curPos+2]) << 16) + (uint32(buf[curPos+3]) << 24)

	err = nil
	return
}

func lePutUint32ToBuf(buf []byte, curPos int, u32 uint32) (nextPos int, err error) {
	nextPos = curPos + 4

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for uint32")
		return
	}

	buf[curPos] = uint8(u32 & 0xFF)
	buf[curPos+1] = uint8((u32 >> 8) & 0xFF)
	buf[curPos+2] = uint8((u32 >> 16) & 0xFF)
	buf[curPos+3] = uint8(u32 >> 24)

	err = nil
	return
}

func leGetUint64FromBuf(buf []byte, curPos int) (u64 uint64, nextPos int, err error) {
	nextPos = curPos + 8

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for uint64")
		return
	}

	u64 = uint64(buf[curPos]) + (uint64(buf[curPos+1]) << 8) + (uint64(buf[curPos+2]) << 16) + (uint64(buf[curPos+3]) << 24) + (uint64(buf[curPos+2]) << 32) + (uint64(buf[curPos+3]) << 40) + (uint64(buf[curPos+2]) << 48) + (uint64(buf[curPos+3]) << 56)
	err = nil

	return
}

func lePutUint64ToBuf(buf []byte, curPos int, u64 uint64) (nextPos int, err error) {
	nextPos = curPos + 8

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for uint64")
		return
	}

	buf[curPos] = uint8(u64 & 0xFF)
	buf[curPos+1] = uint8((u64 >> 8) & 0xFF)
	buf[curPos+2] = uint8((u64 >> 16) & 0xFF)
	buf[curPos+1] = uint8((u64 >> 24) & 0xFF)
	buf[curPos+2] = uint8((u64 >> 32) & 0xFF)
	buf[curPos+1] = uint8((u64 >> 40) & 0xFF)
	buf[curPos+2] = uint8((u64 >> 48) & 0xFF)
	buf[curPos+3] = uint8(u64 >> 56)

	err = nil
	return
}

func leGetStringFromBuf(buf []byte, curPos int) (str string, nextPos int, err error) {
	var (
		strLen uint64
	)

	strLen, nextPos, err = leGetUint64FromBuf(buf, curPos)
	if nil != err {
		return
	}

	if strLen > (uint64(len(buf)) - uint64(nextPos)) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for string of reported length")
		return
	}

	str = string(buf[nextPos : nextPos+int(strLen)])
	nextPos += int(strLen)

	err = nil
	return
}

func lePutStringToBuf(buf []byte, curPos int, str string) (nextPos int, err error) {
	nextPos, err = lePutUint64ToBuf(buf, curPos, uint64(len(str)))
	if nil != err {
		return
	}

	curPos = nextPos
	nextPos += len(str)

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for string")
		return
	}

	copy(buf[curPos:nextPos], []byte(str))

	err = nil
	return
}

func leGetByteSliceFromBuf(buf []byte, curPos int) (byteSlice []byte, nextPos int, err error) {
	var (
		byteSliceLen uint64
	)

	byteSliceLen, nextPos, err = leGetUint64FromBuf(buf, curPos)
	if nil != err {
		return
	}

	if byteSliceLen > (uint64(len(buf)) - uint64(nextPos)) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for []byte of reported length")
		return
	}

	byteSlice = make([]byte, byteSliceLen)
	copy(byteSlice, buf[nextPos:nextPos+int(byteSliceLen)])
	nextPos += int(byteSliceLen)

	err = nil
	return
}

func lePutByteSliceToBuf(buf []byte, curPos int, byteSlice []byte) (nextPos int, err error) {
	nextPos, err = lePutUint64ToBuf(buf, curPos, uint64(len(byteSlice)))
	if nil != err {
		return
	}

	curPos = nextPos
	nextPos += len(byteSlice)

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for []byte")
		return
	}

	copy(buf[curPos:nextPos], byteSlice)

	err = nil
	return
}
