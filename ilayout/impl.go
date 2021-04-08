// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package ilayout

import (
	"fmt"
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

	superBlockV1Buf = make([]byte, 8+8+8+(len(superBlockV1.InodeTableLayout)*(8+8+8))+8)

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
		curPos                  int
		inodeTableLayoutEntryV1 InodeTableLayoutEntryV1Struct
		superBlockVersion       uint64
	)

	superBlockV1 = &SuperBlockV1Struct{
		InodeTableLayout: make([]InodeTableLayoutEntryV1Struct, 0),
	}

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

	for (len(superBlockV1Buf) - curPos) > 8 {
		inodeTableLayoutEntryV1.ObjectNumber, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}

		inodeTableLayoutEntryV1.ObjectSize, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}

		inodeTableLayoutEntryV1.BytesReferenced, curPos, err = leGetUint64FromBuf(superBlockV1Buf, curPos)
		if nil != err {
			return
		}

		superBlockV1.InodeTableLayout = append(superBlockV1.InodeTableLayout, inodeTableLayoutEntryV1)
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
	return nil, fmt.Errorf("TODO - marshalInodeHeadV1()")
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

func leGetStringFromBuf(buf []byte, curPos int) (s string, nextPos int, err error) {
	var (
		sLen uint64
	)

	sLen, nextPos, err = leGetUint64FromBuf(buf, curPos)
	if nil != err {
		return
	}

	if sLen > (uint64(len(buf)) - uint64(nextPos)) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for string of reported length")
		return
	}

	s = string(buf[nextPos : nextPos+int(sLen)])
	nextPos += int(sLen)

	err = nil
	return
}

func lePutStringToBuf(buf []byte, curPos int, s string) (nextPos int, err error) {
	nextPos, err = lePutUint64ToBuf(buf, curPos, uint64(len(s)))
	if nil != err {
		return
	}

	curPos = nextPos
	nextPos += len(s)

	if nextPos > len(buf) {
		err = fmt.Errorf("Insufficient space in buf[curPos:] for string")
		return
	}

	copy(buf[curPos:nextPos], []byte(s))

	err = nil
	return
}
