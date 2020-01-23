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
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+48])) = attr.ATimeNSec
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+52])) = attr.MTimeNSec
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+56])) = attr.CTimeNSec
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+60])) = attr.Mode
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+64])) = attr.NLink
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+68])) = attr.UID
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+72])) = attr.GID
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+76])) = attr.RDev
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+80])) = attr.BlkSize
	*(*uint32)(unsafe.Pointer(&outPayload[outPayloadOffset+84])) = attr.Padding
}

func unmarshalSetAttrIn(devFuseFDReadBufPayload []byte) (setAttrIn *SetAttrIn, err error) {
	if len(devFuseFDReadBufPayload) != SetAttrInSize {
		err = fmt.Errorf("bad len(devFuseFDReadBufPayload) == %v", len(devFuseFDReadBufPayload))
		return
	}

	setAttrIn = &SetAttrIn{
		Valid:     *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[0])),
		Padding:   *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[4])),
		FH:        *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[8])),
		Size:      *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[16])),
		LockOwner: *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[24])),
		ATimeSec:  *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[32])),
		MTimeSec:  *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[40])),
		Unused2:   *(*uint64)(unsafe.Pointer(&devFuseFDReadBufPayload[48])),
		ATimeNSec: *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[56])),
		MTimeNSec: *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[60])),
		Unused3:   *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[64])),
		Mode:      *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[68])),
		Unused4:   *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[72])),
		UID:       *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[76])),
		GID:       *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[80])),
		Unused5:   *(*uint32)(unsafe.Pointer(&devFuseFDReadBufPayload[84])),
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
		Position: 0,
		Padding:  0,
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
		Position: 0,
		Padding2: 0,
		Name:     cloneByteSlice(devFuseFDReadBufPayload[GetXAttrInFixedPortionSize:], true),
	}

	err = nil
	return
}
