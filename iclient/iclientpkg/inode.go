// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"encoding/json"
	"fmt"
	"sync"
	"syscall"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/imgr/imgrpkg"
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
		err = fmt.Errorf("TODO")
	case ilayout.InodeTypeFile:
		err = fmt.Errorf("TODO")
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		err = fmt.Errorf("TODO")
	case ilayout.InodeTypeFile:
		err = fmt.Errorf("TODO")
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		err = fmt.Errorf("TODO")
	case ilayout.InodeTypeFile:
		err = fmt.Errorf("TODO")
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		nextPos int
	)

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		key, nextPos, err = ilayout.GetLEStringFromBuf(payloadData, 0)
		if nil == err {
			bytesConsumed = uint64(nextPos)
		}
	case ilayout.InodeTypeFile:
		key, nextPos, err = ilayout.GetLEUint64FromBuf(payloadData, 0)
		if nil == err {
			bytesConsumed = uint64(nextPos)
		}
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		err = fmt.Errorf("TODO")
	case ilayout.InodeTypeFile:
		err = fmt.Errorf("TODO")
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func (inode *inodeStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		bytesConsumedAsInt int
	)

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		value, bytesConsumedAsInt, err = ilayout.UnmarshalDirectoryEntryValueV1(payloadData)
		if nil == err {
			bytesConsumed = uint64(bytesConsumedAsInt)
		}
	case ilayout.InodeTypeFile:
		value, bytesConsumedAsInt, err = ilayout.UnmarshalExtentMapEntryValueV1(payloadData)
		if nil == err {
			bytesConsumed = uint64(bytesConsumedAsInt)
		}
	default:
		err = fmt.Errorf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	return
}

func lookupInode(inodeNumber uint64) (inode *inodeStruct) {
	var (
		ok bool
	)

	globals.Lock()

	inode, ok = globals.inodeTable[inodeNumber]
	if !ok {
		inode = nil
	}

	globals.Unlock()

	return
}

// createOpenHandle allocates an openHandleStruct and inserts it into the globals openHandle
// maps by both inodeNumber and fissionFH. Note that the fissionFlags* fields all default to
// false. Callers are expected to modify as necessary.
//
func createOpenHandle(inodeNumber uint64) (openHandle *openHandleStruct) {
	openHandle = &openHandleStruct{
		inodeNumber:        inodeNumber,
		fissionFH:          fetchNonce(),
		fissionFlagsAppend: false, // To be filled in by caller
		fissionFlagsRead:   false, // To be filled in by caller
		fissionFlagsWrite:  false, // To be filled in by caller
	}

	globals.Lock()

	globals.openHandleMapByInodeNumber[openHandle.inodeNumber] = openHandle
	globals.openHandleMapByFissionFH[openHandle.fissionFH] = openHandle

	globals.Unlock()

	return
}

func (openHandle *openHandleStruct) destroy() {
	var (
		ok bool
	)

	globals.Lock()

	_, ok = globals.openHandleMapByInodeNumber[openHandle.inodeNumber]
	if !ok {
		logFatalf("globals.openHandleMapByInodeNumber[openHandle.inodeNumber] returned !ok")
	}

	_, ok = globals.openHandleMapByFissionFH[openHandle.fissionFH]
	if !ok {
		logFatalf("globals.openHandleMapByFissionFH[openHandle.fissionFH] returned !ok")
	}

	delete(globals.openHandleMapByInodeNumber, openHandle.inodeNumber)
	delete(globals.openHandleMapByFissionFH, openHandle.fissionFH)

	globals.Unlock()
}

func lookupOpenHandleByInodeNumber(inodeNumber uint64) (openHandle *openHandleStruct) {
	var (
		ok bool
	)

	globals.Lock()

	openHandle, ok = globals.openHandleMapByInodeNumber[inodeNumber]
	if !ok {
		openHandle = nil
	}

	globals.Unlock()

	return
}

func lookupOpenHandleByFissionFH(fissionFH uint64) (openHandle *openHandleStruct) {
	var (
		ok bool
	)

	globals.Lock()

	openHandle, ok = globals.openHandleMapByFissionFH[fissionFH]
	if !ok {
		openHandle = nil
	}

	globals.Unlock()

	return
}

func fetchNonce() (nonceToReturn uint64) {
	var (
		err                     error
		fetchNonceRangeRequest  *imgrpkg.FetchNonceRangeRequestStruct
		fetchNonceRangeResponse *imgrpkg.FetchNonceRangeResponseStruct
		nonceWaitGroup          *sync.WaitGroup
	)

Retry:

	globals.Lock()

	if globals.noncesRemaining > 0 {
		nonceToReturn = globals.nextNonce
		globals.nextNonce++
		globals.noncesRemaining--
		globals.Unlock()
		return
	}

	nonceWaitGroup = globals.nonceWaitGroup
	if nil != nonceWaitGroup {
		globals.Unlock()
		nonceWaitGroup.Wait()
		goto Retry
	}

	globals.nonceWaitGroup = &sync.WaitGroup{}
	globals.nonceWaitGroup.Add(1)

	globals.Unlock()

	fetchNonceRangeRequest = &imgrpkg.FetchNonceRangeRequestStruct{
		MountID: globals.mountID,
	}
	fetchNonceRangeResponse = &imgrpkg.FetchNonceRangeResponseStruct{}

	err = rpcFetchNonceRange(fetchNonceRangeRequest, fetchNonceRangeResponse)
	if nil != err {
		logFatal(err)
	}

	globals.Lock()

	globals.nextNonce = fetchNonceRangeResponse.NextNonce
	globals.noncesRemaining = fetchNonceRangeResponse.NumNoncesFetched

	globals.nonceWaitGroup.Done()
	globals.nonceWaitGroup = nil

	globals.Unlock()

	goto Retry
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

	inode.inodeHeadV1.Layout = make([]ilayout.InodeHeadLayoutEntryV1Struct, len(inode.layoutMap))

	for objectNumber, layoutMapEntry = range inode.layoutMap {
		inode.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].ObjectNumber = objectNumber
		inode.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].ObjectSize = layoutMapEntry.objectSize
		inode.inodeHeadV1.Layout[ilayoutInodeHeadV1LayoutIndex].BytesReferenced = layoutMapEntry.bytesReferenced
	}
}

// ensureInodeHeadV1IsNonNil will ensure inode.inodeHeadV1 is non-nil. As
// such, if it is currently nil at entry, ensureInodeHeadV1IsNonNil() will
// need exclusive access to the inode. In this case, if the exclusive lock
// is not held, ensureInodeHeadV1IsNonNil() will return without error but
// indicate an exclusive lock is needed. Note that if inode.inodeHeadV1 was
// non-nil or an exclusive lock was already held, ensureInodeHeadV1IsNonNil()
// will return that it (no longer) needs an exclusive lock and without error.
//
func (inode *inodeStruct) ensureInodeHeadV1IsNonNil(exclusivelyLocked bool) (needExclusiveLock bool, errno syscall.Errno) {
	var (
		err                        error
		getInodeTableEntryRequest  *imgrpkg.GetInodeTableEntryRequestStruct
		getInodeTableEntryResponse *imgrpkg.GetInodeTableEntryResponseStruct
		inodeHeadV1Buf             []byte
	)

	if nil == inode.inodeHeadV1 {
		if exclusivelyLocked {
			needExclusiveLock = false

			getInodeTableEntryRequest = &imgrpkg.GetInodeTableEntryRequestStruct{
				MountID:     globals.mountID,
				InodeNumber: inode.inodeNumber,
			}
			getInodeTableEntryResponse = &imgrpkg.GetInodeTableEntryResponseStruct{}

			err = rpcGetInodeTableEntry(getInodeTableEntryRequest, getInodeTableEntryResponse)
			if nil != err {
				errno = syscall.ENOENT
				return
			}

			inodeHeadV1Buf, err = objectGETTail(getInodeTableEntryResponse.InodeHeadObjectNumber, getInodeTableEntryResponse.InodeHeadLength)
			if nil != err {
				logFatalf("objectGETTail(getInodeTableEntryResponse.InodeHeadObjectNumber: %v, getInodeTableEntryResponse.InodeHeadLength: %v) failed: %v", getInodeTableEntryResponse.InodeHeadObjectNumber, getInodeTableEntryResponse.InodeHeadLength, err)
			}

			inode.inodeHeadV1, err = ilayout.UnmarshalInodeHeadV1(inodeHeadV1Buf)
			if nil != err {
				logFatalf("ilayout.UnmarshalInodeHeadV1(inodeHeadV1Buf) failed: %v", err)
			}
		} else {
			needExclusiveLock = true
		}
	} else {
		needExclusiveLock = false
	}

	errno = 0
	return
}
