// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package fission

import (
	"bytes"
	"fmt"
	"unsafe"
)

func marshalAttr(attr *Attr, outPayload []byte, outPayloadOffset uint32) {
	*(*uint64)(unsafe.Pointer(&outPayload[outPayloadOffset+0])) = attr.Ino
	*(*uint64)(unsafe.Pointer(&outPayload[outPayloadOffset+8])) = attr.Size
	*(*uint64)(unsafe.Pointer(&outPayload[outPayloadOffset+16])) = attr.Blocks
	*(*uint64)(unsafe.Pointer(&outPayload[outPayloadOffset+24])) = attr.ATimeSec
	*(*uint64)(unsafe.Pointer(&outPayload[outPayloadOffset+32])) = attr.MTimeSec
	*(*uint64)(unsafe.Pointer(&outPayload[outPayloadOffset+40])) = attr.CTimeSec
	*(*uint64)(unsafe.Pointer(&outPayload[outPayloadOffset+48])) = attr.CrTimeSec
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+56])) = attr.ATimeNSec
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+60])) = attr.MTimeNSec
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+64])) = attr.CTimeNSec
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+68])) = attr.CrTimeNSec
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+72])) = attr.Mode
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+76])) = attr.NLink
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+80])) = attr.UID
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+84])) = attr.GID
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+88])) = attr.RDev
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+92])) = attr.BlkSize
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+96])) = attr.Padding
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+100])) = attr.Flags
}

func unmarshalSetAttrIn(devFuseFDReadBufPayload []byte) (setAttrIn *SetAttrIn, err error) {
	if len(devFuseFDReadBufPayload) != SetAttrInSize {
		err = fmt.Errorf("bad len(devFuseFDReadBufPayload) == %v", len(devFuseFDReadBufPayload))
		return
	}

	setAttrIn = &SetAttrIn{
		Valid:        *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[0])),
		Padding:      *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[4])),
		FH:           *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[8])),
		Size:         *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[16])),
		LockOwner:    *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[24])),
		ATimeSec:     *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[32])),
		MTimeSec:     *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[40])),
		Unused2:      *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[48])),
		ATimeNSec:    *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[56])),
		MTimeNSec:    *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[60])),
		Unused3:      *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[64])),
		Mode:         *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[68])),
		Unused4:      *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[72])),
		UID:          *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[76])),
		GID:          *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[80])),
		Unused5:      *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[84])),
		BkupTimeSec:  *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[88])),
		ChgTimeSec:   *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[96])),
		CrTimeSec:    *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[104])),
		BkupTimeNSec: *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[112])),
		ChgTimeNSec:  *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[116])),
		CrTimeNSec:   *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[120])),
		Flags:        *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[124])),
	}

	err = nil
	return
}

func unmarshalSetXAttrIn(devFuseFDReadBufPayload []byte) (setXAttrIn *SetXAttrIn, err error) {
	var (
		nameDataSplit  [][]byte
		setXAttrInSize int
	)

	if len(devFuseFDReadBufPayload) < SetXAttrInFixedPortionSize {
		err = fmt.Errorf("bad len(devFuseFDReadBufPayload) == %v", len(devFuseFDReadBufPayload))
		return
	}

	nameDataSplit = bytes.SplitN(devFuseFDReadBufPayload[SetXAttrInFixedPortionSize:], []byte{0}, 2)
	if len(nameDataSplit) != 2 {
		err = fmt.Errorf("bad devFuseFDReadBufPayload")
		return
	}

	setXAttrIn = &SetXAttrIn{
		Size:     *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[0])),
		Flags:    *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[4])),
		Position: *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[8])),
		Padding:  *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[12])),
		Name:     cloneByteSlice(nameDataSplit[0], false),
		Data:     cloneByteSlice(nameDataSplit[1], true),
	}

	setXAttrInSize = SetXAttrInFixedPortionSize + len(setXAttrIn.Name) + 1 + len(setXAttrIn.Data)

	if len(devFuseFDReadBufPayload) != setXAttrInSize {
		err = fmt.Errorf("bad Size == %v expected %v", setXAttrIn.Size, setXAttrInSize)
		return
	}

	err = nil
	return
}

func unmarshalGetXAttrIn(devFuseFDReadBufPayload []byte) (getXAttrIn *GetXAttrIn, err error) {
	if len(devFuseFDReadBufPayload) < GetXAttrInFixedPortionSize {
		err = fmt.Errorf("bad len(devFuseFDReadBufPayload) == %v", len(devFuseFDReadBufPayload))
		return
	}

	getXAttrIn = &GetXAttrIn{
		Size:     *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[0])),
		Padding:  *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[4])),
		Position: *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[8])),
		Padding2: *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[12])),
		Name:     cloneByteSlice(devFuseFDReadBufPayload[GetXAttrInFixedPortionSize:], true),
	}

	err = nil
	return
}
