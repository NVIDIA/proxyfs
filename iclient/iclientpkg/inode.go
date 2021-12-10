// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

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
	var (
		layoutMapEntry layoutMapEntryStruct
		ok             bool
	)

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		// Fall through
	case ilayout.InodeTypeFile:
		// Fall through
	default:
		logFatalf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	inode.ensurePutObjectIsActive()

	layoutMapEntry, ok = inode.layoutMap[inode.putObjectNumber]
	if !ok {
		logFatalf("inode.layoutMap[inode.putObjectNumber] returned !ok")
	}

	objectNumber = inode.putObjectNumber
	objectOffset = uint64(len(inode.putObjectBuffer))

	layoutMapEntry.objectSize += uint64(len(nodeByteSlice))
	layoutMapEntry.bytesReferenced += uint64(len(nodeByteSlice))

	inode.layoutMap[inode.putObjectNumber] = layoutMapEntry

	inode.superBlockInodeObjectSizeAdjustment += int64(len(nodeByteSlice))
	inode.superBlockInodeBytesReferencedAdjustment += int64(len(nodeByteSlice))

	inode.putObjectBuffer = append(inode.putObjectBuffer, nodeByteSlice...)

	err = nil
	return
}

func (inode *inodeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	var (
		layoutMapEntry layoutMapEntryStruct
		ok             bool
	)

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		// Fall through
	case ilayout.InodeTypeFile:
		// Fall through
	default:
		logFatalf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	layoutMapEntry, ok = inode.layoutMap[objectNumber]
	if !ok {
		log.Fatalf("inode.layoutMap[old inode.putObjectNumber] returned !ok")
	}
	if objectLength > layoutMapEntry.bytesReferenced {
		log.Fatalf("objectLength > layoutMapEntry.bytesReferenced")
	}
	if (objectOffset + objectLength) > layoutMapEntry.objectSize {
		log.Fatalf("(objectOffset + objectLength) > layoutMapEntry.objectSize")
	}

	// It's ok to update lauoutMap... but note that the above checks don't protect against all double deallocations

	if (objectLength == layoutMapEntry.bytesReferenced) && (objectNumber != inode.putObjectNumber) {
		// Note that we skip the special case where we are currently
		// discarding the only referenced bytes in an active putObjectBuffer
		// since a subsequent flush will write (at least) the inodeHeadV1 there

		delete(inode.layoutMap, objectNumber)

		inode.superBlockInodeObjectCountAdjustment--
		inode.superBlockInodeObjectSizeAdjustment -= int64(layoutMapEntry.objectSize)
		inode.superBlockInodeBytesReferencedAdjustment -= int64(objectLength)

		inode.dereferencedObjectNumberArray = append(inode.dereferencedObjectNumberArray, objectNumber)
	} else {
		layoutMapEntry.bytesReferenced -= objectLength

		inode.layoutMap[objectNumber] = layoutMapEntry

		inode.superBlockInodeBytesReferencedAdjustment -= int64(objectLength)
	}

	err = nil
	return
}

func (inode *inodeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsString  string
		keyAsUint64  uint64
		nextPos      int
		ok           bool
		packedKeyLen int
	)

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		keyAsString, ok = key.(string)
		if ok {
			packedKeyLen = 8 + len(keyAsString)
			packedKey = make([]byte, packedKeyLen)
			nextPos, err = ilayout.PutLEStringToBuf(packedKey, 0, keyAsString)
			if (nil == err) && (nextPos != packedKeyLen) {
				err = fmt.Errorf("nextPos != packedKeyLen")
			}
		} else {
			err = fmt.Errorf("key.(string) returned !ok")
		}
	case ilayout.InodeTypeFile:
		keyAsUint64, ok = key.(uint64)
		if ok {
			packedKeyLen = 8
			packedKey = make([]byte, packedKeyLen)
			nextPos, err = ilayout.PutLEUint64ToBuf(packedKey, 0, keyAsUint64)
			if (nil == err) && (nextPos != packedKeyLen) {
				err = fmt.Errorf("nextPos != packedKeyLen")
			}
		} else {
			err = fmt.Errorf("key.(uint64) returned !ok")
		}
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
	var (
		valueAsDirectoryEntryValueV1 *ilayout.DirectoryEntryValueV1Struct
		valueAsExtentMapEntryValueV1 *ilayout.ExtentMapEntryValueV1Struct
		ok                           bool
	)

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		valueAsDirectoryEntryValueV1, ok = value.(*ilayout.DirectoryEntryValueV1Struct)
		if ok {
			packedValue, err = valueAsDirectoryEntryValueV1.MarshalDirectoryEntryValueV1()
		} else {
			err = fmt.Errorf("value.(*ilayout.DirectoryEntryValueV1Struct) returned !ok")
		}
	case ilayout.InodeTypeFile:
		valueAsExtentMapEntryValueV1, ok = value.(*ilayout.ExtentMapEntryValueV1Struct)
		if ok {
			packedValue, err = valueAsExtentMapEntryValueV1.MarshalExtentMapEntryValueV1()
		} else {
			err = fmt.Errorf("value.(*ilayout.DirectoryEntryValueV1Struct) returned !ok")
		}
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

// createOpenHandle allocates an openHandleStruct and inserts it into the globals.openHandleMap.
//
// Note that fissionFlags{Read|Write} are forced to be TRUE per the behavior of Linux VFS
// and/or fuse.ko choosing to mask these during Do{Create|Open|OpenDir}() upcalls.
//
func createOpenHandle(inodeNumber uint64, fissionFlagsAppend bool, fissionFlagsRead bool, fissionFlagsWrite bool) (openHandle *openHandleStruct) {
	openHandle = &openHandleStruct{
		inodeNumber:        inodeNumber,
		fissionFH:          fetchNonce(),
		fissionFlagsAppend: fissionFlagsAppend,
		fissionFlagsRead:   true, // Should be fissionFlagsRead  but Linux VFS and/or fuse.ko masks these
		fissionFlagsWrite:  true, // Should be fissionFlagsWrite but Linux VFS and/or fuse.ko masks these
	}

	globals.Lock()

	globals.openHandleMap[openHandle.fissionFH] = openHandle

	globals.Unlock()

	return
}

func (openHandle *openHandleStruct) destroy() {
	var (
		ok bool
	)

	globals.Lock()

	_, ok = globals.openHandleMap[openHandle.fissionFH]
	if !ok {
		logFatalf("globals.openHandleMap[openHandle.fissionFH] returned !ok")
	}

	delete(globals.openHandleMap, openHandle.fissionFH)

	globals.Unlock()
}

func lookupOpenHandle(fissionFH uint64) (openHandle *openHandleStruct) {
	var (
		ok bool
	)

	globals.Lock()

	openHandle, ok = globals.openHandleMap[fissionFH]
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

func (inode *inodeStruct) populateInodeHeadV1() (err error) {
	var (
		getInodeTableEntryRequest  *imgrpkg.GetInodeTableEntryRequestStruct
		getInodeTableEntryResponse *imgrpkg.GetInodeTableEntryResponseStruct
		inodeHeadV1Buf             []byte
	)

	getInodeTableEntryRequest = &imgrpkg.GetInodeTableEntryRequestStruct{
		MountID:     globals.mountID,
		InodeNumber: inode.inodeNumber,
	}
	getInodeTableEntryResponse = &imgrpkg.GetInodeTableEntryResponseStruct{}

	err = rpcGetInodeTableEntry(getInodeTableEntryRequest, getInodeTableEntryResponse)
	if nil != err {
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

	inode.convertInodeHeadV1LinkTableToLinkSet()
	inode.convertInodeHeadV1StreamTableToStreamMap()
	inode.convertInodeHeadV1LayoutToLayoutMap()

	return
}

func (inode *inodeStruct) convertInodeHeadV1LinkTableToLinkSet() {
	var (
		ilayoutInodeLinkTableEntry ilayout.InodeLinkTableEntryStruct
	)

	inode.linkSet = make(map[ilayout.InodeLinkTableEntryStruct]struct{})

	for _, ilayoutInodeLinkTableEntry = range inode.inodeHeadV1.LinkTable {
		inode.linkSet[ilayoutInodeLinkTableEntry] = struct{}{}
	}
}

func (inode *inodeStruct) convertLinkSetToInodeHeadV1LinkTable() {
	var (
		ilayoutInodeLinkTableEntry ilayout.InodeLinkTableEntryStruct
	)

	inode.inodeHeadV1.LinkTable = make([]ilayout.InodeLinkTableEntryStruct, 0, len(inode.linkSet))

	for ilayoutInodeLinkTableEntry = range inode.linkSet {
		inode.inodeHeadV1.LinkTable = append(inode.inodeHeadV1.LinkTable, ilayoutInodeLinkTableEntry)
	}
}

func (inode *inodeStruct) convertInodeHeadV1StreamTableToStreamMap() {
	var (
		ilayoutInodeStreamTableEntry ilayout.InodeStreamTableEntryStruct
	)

	inode.streamMap = make(map[string][]byte)

	for _, ilayoutInodeStreamTableEntry = range inode.inodeHeadV1.StreamTable {
		inode.streamMap[ilayoutInodeStreamTableEntry.Name] = ilayoutInodeStreamTableEntry.Value
	}
}

func (inode *inodeStruct) convertStreamMapToInodeHeadV1StreamTable() {
	var (
		name  string
		value []byte
	)

	inode.inodeHeadV1.StreamTable = make([]ilayout.InodeStreamTableEntryStruct, 0, len(inode.streamMap))

	for name, value = range inode.streamMap {
		inode.inodeHeadV1.StreamTable = append(inode.inodeHeadV1.StreamTable, ilayout.InodeStreamTableEntryStruct{
			Name:  name,
			Value: value,
		})
	}
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
		layoutMapEntry             layoutMapEntryStruct
		layoutMapEntryToDeleteList []uint64
		objectNumber               uint64
	)

	inode.inodeHeadV1.Layout = make([]ilayout.InodeHeadLayoutEntryV1Struct, 0, len(inode.layoutMap))

	for objectNumber, layoutMapEntry = range inode.layoutMap {
		if layoutMapEntry.objectSize == 0 {
			if layoutMapEntry.bytesReferenced != 0 {
				logFatalf("(layoutMapEntry.objectSize == 0) && (layoutMapEntry.bytesReferenced != 0)")
			}

			layoutMapEntryToDeleteList = append(layoutMapEntryToDeleteList, objectNumber)
		} else {
			if layoutMapEntry.bytesReferenced == 0 {
				logFatalf("(layoutMapEntry.objectSize != 0) && (layoutMapEntry.bytesReferenced == 0)")
			}

			inode.inodeHeadV1.Layout = append(inode.inodeHeadV1.Layout, ilayout.InodeHeadLayoutEntryV1Struct{
				ObjectNumber:    objectNumber,
				ObjectSize:      layoutMapEntry.objectSize,
				BytesReferenced: layoutMapEntry.bytesReferenced,
			})
		}
	}

	for _, objectNumber = range layoutMapEntryToDeleteList {
		delete(inode.layoutMap, objectNumber)
	}
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

func (inode *inodeStruct) ensurePutObjectIsActive() {
	if inode.putObjectNumber == 0 {
		inode.putObjectNumber = fetchNonce()
		inode.putObjectBuffer = make([]byte, 0)

		inode.superBlockInodeObjectCountAdjustment++

		inode.layoutMap[inode.putObjectNumber] = layoutMapEntryStruct{
			objectSize:      0,
			bytesReferenced: 0,
		}

		inode.superBlockInodeObjectCountAdjustment++
	}
}

func (fileInode *inodeStruct) launchFlusher() {
	fileInode.fileFlusher = &fileInodeFlusherStruct{
		inode:       fileInode,
		inodeNumber: fileInode.inodeNumber,
		timer:       time.NewTimer(globals.config.FileFlushTriggerDuration),
		cancelChan:  make(chan struct{}, 1),
	}

	go fileInode.fileFlusher.gor()
}

func (fileFlusher *fileInodeFlusherStruct) gor() {
	var (
		inodeLockRequest *inodeLockRequestStruct
	)

	select {
	case <-fileFlusher.cancelChan:
		// We need to cancel our timer safely

		if !fileFlusher.timer.Stop() {
			<-fileFlusher.timer.C
		}
	case <-fileFlusher.timer.C:
		// Our timer has expired - so we likely need to do a flush... but we need an Exclusive Lock to do so
		//
		// But note that "our" fileFlusher.putObjectNumber may have already been flushed...

	Retry:
		inodeLockRequest = newLockRequest()
		inodeLockRequest.inodeNumber = fileFlusher.inodeNumber
		inodeLockRequest.exclusive = true
		inodeLockRequest.addThisLock()
		if len(inodeLockRequest.locksHeld) == 0 {
			performInodeLockRetryDelay()
			goto Retry
		}

		if fileFlusher == fileFlusher.inode.fileFlusher {
			// We now know that fileFlusher.inode.dirty == true and
			// that we need to flush "our" fileFlusher.putObjectNumber

			flushInodesInSlice([]*inodeStruct{fileFlusher.inode})
		}

		inodeLockRequest.unlockAll()
	}
}

func (fileFlusher *fileInodeFlusherStruct) cancel() {
	// Prevent (*fileInodeFlusherStruct).gor() from case where it's .timer had already expired
	// and it is awaiting the ExclusiveLock before checking fileFlusher.inode.fileFlusher

	fileFlusher.inode.fileFlusher = nil

	// Now send "cancel" request to (*fileInodeFlusherStruct).gor()
	//
	// Note that it may never actually read the "cancel" request, but since .cancelChan
	// is buffered, and we close it here, the garbage collector will soon reclaim it

	fileFlusher.cancelChan <- struct{}{}

	close(fileFlusher.cancelChan)
}

// flush marshals the current state of an assumed to be dirty inode into its .putObjectBuffer.
// This .putObjectBuffer is then passed to objectPUT() following which the inode is marked clean.
//
// The return value indicates the size of the marshaled .inodeHeadV1 such that, along with the
// .superBlockInode*, .dereferencedObjectNumberArray, and .putObjectNumber fields, will be used
// to construct this inode's portion of an imgrpkg.PutInodeTableEntriesRequestStruct.
//
// The caller is assumed to mark the inode clean and reset the .superBlockInode*,
// .dereferencedObjectNumberArray, and .putObject{Number|Buffer} fields.
//
func (inode *inodeStruct) flush() (inodeHeadLength uint64) {
	var (
		err            error
		inodeHeadV1Buf []byte
	)

	inode.ensurePutObjectIsActive()

	if inode.inodeHeadV1.InodeType != ilayout.InodeTypeSymLink {
		if inode.payload != nil {
			inode.inodeHeadV1.PayloadObjectNumber, inode.inodeHeadV1.PayloadObjectOffset, inode.inodeHeadV1.PayloadObjectLength, err = inode.payload.Flush(false)
			if nil != err {
				logFatalf("inode.payload.Flush(false) failed: %v", err)
			}

			err = inode.payload.Prune()
			if nil != err {
				logFatalf("inode.payload.Prune() failed: %v", err)
			}
		}
	}

	if inode.inodeHeadV1.InodeType == ilayout.InodeTypeFile {
		if inode.fileFlusher != nil {
			inode.fileFlusher.cancel()
		}
	}

	inode.convertLinkSetToInodeHeadV1LinkTable()
	inode.convertStreamMapToInodeHeadV1StreamTable()
	inode.convertLayoutMapToInodeHeadV1Layout()

	inodeHeadV1Buf, err = inode.inodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		logFatalf("inode.inodeHeadV1.MarshalInodeHeadV1() failed: %v", err)
	}

	inode.putObjectBuffer = append(inode.putObjectBuffer, inodeHeadV1Buf...)

	err = objectPUT(inode.putObjectNumber, inode.putObjectBuffer)
	if nil != err {
		logFatalf("objectPUT(inode.putObjectNumber, inode.putObjectBuffer) failed: %v", err)
	}

	inodeHeadLength = uint64(len(inodeHeadV1Buf))
	return
}

func flushInodeNumbersInSlice(inodeNumberSlice []uint64) {
	var (
		inodeNumber     uint64
		inodeSlice      []*inodeStruct
		inodeSliceIndex int
		ok              bool
	)

	inodeSlice = make([]*inodeStruct, len(inodeNumberSlice))

	globals.Lock()

	for inodeSliceIndex, inodeNumber = range inodeNumberSlice {
		inodeSlice[inodeSliceIndex], ok = globals.inodeTable[inodeNumber]
		if !ok {
			logFatalf("globals.inodeTable[inodeNumber: %016X] returned !ok", inodeNumber)
		}
	}

	globals.Unlock()

	flushInodesInSlice(inodeSlice)
}

func flushInodesInSlice(inodeSlice []*inodeStruct) {
	var (
		err                          error
		inode                        *inodeStruct
		inodeHeadLength              uint64
		inodeSliceIndex              int
		putInodeTableEntriesRequest  *imgrpkg.PutInodeTableEntriesRequestStruct
		putInodeTableEntriesResponse *imgrpkg.PutInodeTableEntriesResponseStruct
	)

	putInodeTableEntriesRequest = &imgrpkg.PutInodeTableEntriesRequestStruct{
		MountID:                                  globals.mountID,
		UpdatedInodeTableEntryArray:              make([]imgrpkg.PutInodeTableEntryStruct, len(inodeSlice)),
		SuperBlockInodeObjectCountAdjustment:     0,
		SuperBlockInodeObjectSizeAdjustment:      0,
		SuperBlockInodeBytesReferencedAdjustment: 0,
		DereferencedObjectNumberArray:            make([]uint64, 0),
	}
	putInodeTableEntriesResponse = &imgrpkg.PutInodeTableEntriesResponseStruct{}

	for inodeSliceIndex, inode = range inodeSlice {
		if !inode.dirty {
			logFatalf("inode.dirty for inodeNumber %016X is false", inode.inodeNumber)
		}

		inodeHeadLength = inode.flush()

		putInodeTableEntriesRequest.UpdatedInodeTableEntryArray[inodeSliceIndex].InodeNumber = inode.inodeNumber
		putInodeTableEntriesRequest.UpdatedInodeTableEntryArray[inodeSliceIndex].InodeHeadObjectNumber = inode.putObjectNumber
		putInodeTableEntriesRequest.UpdatedInodeTableEntryArray[inodeSliceIndex].InodeHeadLength = inodeHeadLength

		putInodeTableEntriesRequest.SuperBlockInodeObjectCountAdjustment += inode.superBlockInodeObjectCountAdjustment
		putInodeTableEntriesRequest.SuperBlockInodeObjectSizeAdjustment += inode.superBlockInodeObjectSizeAdjustment
		putInodeTableEntriesRequest.SuperBlockInodeBytesReferencedAdjustment += inode.superBlockInodeBytesReferencedAdjustment

		putInodeTableEntriesRequest.DereferencedObjectNumberArray = append(putInodeTableEntriesRequest.DereferencedObjectNumberArray, inode.dereferencedObjectNumberArray...)

		inode.dirty = false

		inode.superBlockInodeObjectCountAdjustment = 0
		inode.superBlockInodeObjectSizeAdjustment = 0
		inode.superBlockInodeBytesReferencedAdjustment = 0

		inode.dereferencedObjectNumberArray = make([]uint64, 0)

		inode.putObjectNumber = 0
		inode.putObjectBuffer = nil
	}

	err = rpcPutInodeTableEntries(putInodeTableEntriesRequest, putInodeTableEntriesResponse)
	if nil != err {
		logFatalf("rpcPutInodeTableEntries(putInodeTableEntriesRequest, putInodeTableEntriesResponse) failed: %v", err)
	}
}

// recordExtent is called before appending to a fileInode's putObjectBuffer to update
// the ExtentMap to reference the about to be appended data. As it is possible that
// the extent being recorded overlaps with one or more existing extents, recordExtent()
// embeds a call to unmapExtent(). If the to be recorded extent is contiguous with an
// existing extent (both in terms of fileOffset and objectOffset), these will be
// combined. Thus, a sequentially written fileInode will have at most a single extent
// referencing each object. The fileInode's Size will not be adjusted as holes in the
// ExtentMap equate to "read as zero" and the fileInode's Size may have been established
// via a SetAttr (i.e. without actually writing data to extent the fileInode).
//
// The fileInode's layoutMap and, potentially, dereferencedObjectNumberArray will be
// updated to reflect the dereferenced extent. Similarly, the fileInode's pending
// updates for superBlockInode{BytesReferencedAdjustment|Object{Count|Size}} will
// be updated.
//
func (fileInode *inodeStruct) recordExtent(startingFileOffset uint64, length uint64) {
	var (
		err                          error
		extentMapEntryKeyV1          uint64
		extentMapEntryKeyV1AsKey     sortedmap.Key
		extentMapEntryValueV1        *ilayout.ExtentMapEntryValueV1Struct
		extentMapEntryValueV1AsValue sortedmap.Value
		index                        int
		layoutMapEntry               layoutMapEntryStruct
		ok                           bool
	)

	fileInode.ensurePutObjectIsActive()

	fileInode.unmapExtent(startingFileOffset, length)

	layoutMapEntry, ok = fileInode.layoutMap[fileInode.putObjectNumber]
	if !ok {
		logFatalf("fileInode.layoutMap[inode.putObjectNumber] returned !ok")
	}

	layoutMapEntry.objectSize += length
	layoutMapEntry.bytesReferenced += length

	fileInode.layoutMap[fileInode.putObjectNumber] = layoutMapEntry

	fileInode.superBlockInodeObjectSizeAdjustment += int64(length)
	fileInode.superBlockInodeBytesReferencedAdjustment += int64(length)

	index, _, err = fileInode.payload.BisectLeft(startingFileOffset)
	if nil != err {
		logFatalf("fileInode.payload.BisectLeft(startingFileOffset) failed: %v", err)
	}

	extentMapEntryKeyV1AsKey, extentMapEntryValueV1AsValue, ok, err = fileInode.payload.GetByIndex(index)
	if nil != err {
		logFatalf("fileInode.payload.GetByIndex(index) failed: %v", err)
	}

	if ok {
		extentMapEntryKeyV1, ok = extentMapEntryKeyV1AsKey.(uint64)
		if !ok {
			logFatalf("extentMapEntryKeyV1AsKey.(uint64) returned !ok")
		}
		extentMapEntryValueV1, ok = extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct)
		if !ok {
			logFatalf("extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
		}

		if (extentMapEntryKeyV1 + extentMapEntryValueV1.Length) == startingFileOffset {
			// The extent to be recorded starts right after an existing extent by fileOffset

			if extentMapEntryValueV1.ObjectNumber == fileInode.putObjectNumber {
				// And the just prior existing extent was recorded to the current putObject

				if (extentMapEntryValueV1.ObjectOffset + extentMapEntryValueV1.Length) == uint64(len(fileInode.putObjectBuffer)) {
					// And the extent to be recorded starts right after the just prior extent in the current putObject - so we can combine them

					extentMapEntryValueV1.Length += length

					ok, err = fileInode.payload.PatchByIndex(index, extentMapEntryValueV1)
					if nil != err {
						logFatalf("fileInode.payload.PatchByIndex(index, extentMapEntryValueV1) failed: %v", err)
					}
					if !ok {
						logFatalf("fileInode.payload.PatchByIndex(index, extentMapEntryValueV1) returned !ok")
					}

					// So we need not fall through to the un-optimized path

					return
				}
			}
		}
	}

	// Un-optimized path - just record the new extent

	extentMapEntryValueV1 = &ilayout.ExtentMapEntryValueV1Struct{
		Length:       length,
		ObjectNumber: fileInode.putObjectNumber,
		ObjectOffset: uint64(len(fileInode.putObjectBuffer)),
	}

	ok, err = fileInode.payload.Put(startingFileOffset, extentMapEntryValueV1)
	if nil != err {
		logFatalf("fileInode.payload.Put(startingFileOffset, extentMapEntryValueV1) failed: %v", err)
	}
	if !ok {
		logFatalf("fileInode.payload.Put(startingFileOffset, extentMapEntryValueV1) returned !ok")
	}
}

// unmapExtent releases references for a range of bytes in a fileInode. If length
// is zero, the unmapped extent is assumed to be to the end of the fileInode (i.e.
// a truncate opereration) as indicated by the fileInode's current Size. The
// fileInode's Size will not be adjusted as holes in the ExtentMap equate to
// "read as zero" and the fileInode's Size may have been established via SetAttr
// (i.e. without actually writing data to extend the fileInode).
//
// The fileInode's layoutMap and, potentially, dereferencedObjectNumberArray will be
// updated to reflect the dereferenced extent. Similarly, the fileInode's pending
// updates for superBlockInode{BytesReferencedAdjustment|Object{Count|Size}} will
// be updated.
//
func (fileInode *inodeStruct) unmapExtent(startingFileOffset uint64, length uint64) {
	var (
		err                          error
		extentLengthOfTail           uint64
		extentLengthToTrim           uint64
		extentMapEntryKeyV1          uint64
		extentMapEntryKeyV1AsKey     sortedmap.Key
		extentMapEntryValueV1        *ilayout.ExtentMapEntryValueV1Struct
		extentMapEntryValueV1AsValue sortedmap.Value
		extentObjectNumberOfTail     uint64
		extentObjectOffsetOfTail     uint64
		found                        bool
		index                        int
		layoutMapEntry               layoutMapEntryStruct
		ok                           bool
	)

	if length == 0 {
		length = fileInode.inodeHeadV1.Size - startingFileOffset
	}

	index, found, err = fileInode.payload.BisectLeft(startingFileOffset)
	if nil != err {
		logFatalf("fileInode.payload.BisectLeft(startingFileOffset) failed: %v", err)
	}

	if !found {
		// See if there is an extent starting just to the left of the unmap range

		extentMapEntryKeyV1AsKey, extentMapEntryValueV1AsValue, ok, err = fileInode.payload.GetByIndex(index)
		if nil != err {
			logFatalf("fileInode.payload.GetByIndex(index) failed: %v", err)
		}

		if ok {
			// There is an extent starting just to the left of the unmap range

			extentMapEntryKeyV1, ok = extentMapEntryKeyV1AsKey.(uint64)
			if !ok {
				logFatalf("extentMapEntryKeyV1AsKey.(uint64) returned !ok")
			}
			extentMapEntryValueV1, ok = extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct)
			if !ok {
				logFatalf("extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
			}

			layoutMapEntry, ok = fileInode.layoutMap[extentMapEntryValueV1.ObjectNumber]
			if !ok {
				logFatalf("fileInode.layoutMap[extentMapEntryValueV1.ObjectNumber] returned !ok")
			}

			if (extentMapEntryKeyV1 + extentMapEntryValueV1.Length) > startingFileOffset {
				// The extent starting just to the left of the unmap range either needs
				// to have a hole punched in it or it needs to be trimmed on the right

				if (extentMapEntryKeyV1 + extentMapEntryValueV1.Length) > (startingFileOffset + length) {
					// The extent starting just to the left of the unmap range needs to have a hole punched in it

					extentLengthOfTail = (extentMapEntryKeyV1 + extentMapEntryValueV1.Length) - (startingFileOffset + length)
					extentObjectNumberOfTail = extentMapEntryValueV1.ObjectNumber
					extentObjectOffsetOfTail = extentMapEntryValueV1.ObjectOffset + (extentMapEntryValueV1.Length - extentLengthOfTail)

					extentMapEntryValueV1.Length = startingFileOffset - extentMapEntryKeyV1

					ok, err = fileInode.payload.PatchByIndex(index, extentMapEntryValueV1)
					if nil != err {
						logFatalf("fileInode.payload.PatchByIndex() failed: %v", err)
					}
					if !ok {
						logFatalf("fileInode.payload.PatchByIndex() returned !ok")
					}

					extentMapEntryKeyV1 = startingFileOffset + length

					extentMapEntryValueV1 = &ilayout.ExtentMapEntryValueV1Struct{
						Length:       extentLengthOfTail,
						ObjectNumber: extentObjectNumberOfTail,
						ObjectOffset: extentObjectOffsetOfTail,
					}

					ok, err = fileInode.payload.Put(extentMapEntryKeyV1, extentMapEntryValueV1)
					if nil != err {
						logFatalf("fileInode.payload.Put() failed: %v", err)
					}
					if !ok {
						logFatalf("fileInode.payload.Put() returned !ok")
					}

					layoutMapEntry.bytesReferenced -= length

					fileInode.layoutMap[extentMapEntryValueV1.ObjectNumber] = layoutMapEntry

					fileInode.superBlockInodeBytesReferencedAdjustment -= int64(length)

					// And since we've now fully accounted for the unmap range, we are done

					return
				}

				// If we reach here, the extent starting just to the left of the unmap range needs to be trimmed on the right

				extentLengthToTrim = (extentMapEntryKeyV1 + extentMapEntryValueV1.Length) - startingFileOffset

				extentMapEntryValueV1.Length -= extentLengthToTrim

				ok, err = fileInode.payload.PatchByIndex(index, extentMapEntryValueV1)
				if nil != err {
					logFatalf("fileInode.payload.PatchByIndex() failed: %v", err)
				}
				if !ok {
					logFatalf("fileInode.payload.PatchByIndex() returned !ok")
				}

				layoutMapEntry.bytesReferenced -= extentLengthToTrim

				fileInode.layoutMap[extentMapEntryValueV1.ObjectNumber] = layoutMapEntry

				fileInode.superBlockInodeBytesReferencedAdjustment -= int64(extentLengthToTrim)

				// Adjust the unmap range to cover only what remains to be unmapped following the above

				startingFileOffset += extentLengthToTrim
				length -= extentLengthToTrim
			}
		}
	}

	// Now advance index to refer to the next (or first) extent known to start at or to the right of the start of the unmap range (if any)

	index++

	// Finally proceed deleting or trimming on the left the next extent (if any) until the unmap range is removed

	for {
		extentMapEntryKeyV1AsKey, extentMapEntryValueV1AsValue, ok, err = fileInode.payload.GetByIndex(index)
		if nil != err {
			logFatalf("fileInode.payload.GetByIndex(index) failed: %v", err)
		}

		if !ok {
			// We reached the end of the ExtentMap so we are done

			return
		}

		extentMapEntryKeyV1, ok = extentMapEntryKeyV1AsKey.(uint64)
		if !ok {
			logFatalf("extentMapEntryKeyV1AsKey.(uint64) returned !ok")
		}
		extentMapEntryValueV1, ok = extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct)
		if !ok {
			logFatalf("extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct) returned !ok")
		}

		layoutMapEntry, ok = fileInode.layoutMap[extentMapEntryValueV1.ObjectNumber]
		if !ok {
			logFatalf("fileInode.layoutMap[extentMapEntryValueV1.ObjectNumber] returned !ok")
		}

		// This extent either is completely consumed by the unmap range or needs to be trimmed on the left

		if (startingFileOffset + length) < (extentMapEntryKeyV1 + extentMapEntryValueV1.Length) {
			// This extent needs to be trimmed on the left

			extentLengthToTrim = (extentMapEntryKeyV1 + extentMapEntryValueV1.Length) - (startingFileOffset + length)

			ok, err = fileInode.payload.DeleteByIndex(index)
			if nil != err {
				logFatalf("fileInode.payload.DeleteByIndex() failed: %v", err)
			}
			if !ok {
				logFatalf("fileInode.payload.DeleteByIndex() returned !ok")
			}

			extentMapEntryKeyV1 += extentLengthToTrim

			extentMapEntryValueV1.Length -= extentLengthToTrim
			extentMapEntryValueV1.ObjectOffset += extentLengthToTrim

			ok, err = fileInode.payload.Put(extentMapEntryKeyV1, extentMapEntryValueV1)
			if nil != err {
				logFatalf("fileInode.payload.Put() failed: %v", err)
			}
			if !ok {
				logFatalf("fileInode.payload.Put() returned !ok")
			}

			layoutMapEntry.bytesReferenced -= extentLengthToTrim

			fileInode.layoutMap[extentMapEntryValueV1.ObjectNumber] = layoutMapEntry

			fileInode.superBlockInodeBytesReferencedAdjustment -= int64(extentLengthToTrim)

			// And since we've now fully accounted for the unmap range, we are done

			return
		}

		// If we reach here, the extent is completely consumed by the unmap range

		ok, err = fileInode.payload.DeleteByIndex(index)
		if nil != err {
			logFatalf("fileInode.payload.DeleteByIndex() failed: %v", err)
		}
		if !ok {
			logFatalf("fileInode.payload.DeleteByIndex() returned !ok")
		}

		layoutMapEntry.bytesReferenced -= extentMapEntryValueV1.Length

		if layoutMapEntry.bytesReferenced == 0 {
			delete(fileInode.layoutMap, extentMapEntryValueV1.ObjectNumber)

			fileInode.superBlockInodeObjectCountAdjustment--
			fileInode.superBlockInodeObjectSizeAdjustment -= int64(layoutMapEntry.objectSize)
			fileInode.superBlockInodeBytesReferencedAdjustment -= int64(extentMapEntryValueV1.Length)

			fileInode.dereferencedObjectNumberArray = append(fileInode.dereferencedObjectNumberArray, extentMapEntryValueV1.ObjectNumber)
		} else {
			layoutMapEntry.bytesReferenced -= extentMapEntryValueV1.Length

			fileInode.layoutMap[extentMapEntryValueV1.ObjectNumber] = layoutMapEntry

			fileInode.superBlockInodeBytesReferencedAdjustment -= int64(extentMapEntryValueV1.Length)
		}

		length = (startingFileOffset + length) - (extentMapEntryKeyV1 + extentMapEntryValueV1.Length)

		if length == 0 {
			// Since we've now fully accounted for the unmap range, we are done

			return
		}

		startingFileOffset = extentMapEntryKeyV1 + extentMapEntryValueV1.Length

		// Now loop back to fetch the next existing extent
	}
}
