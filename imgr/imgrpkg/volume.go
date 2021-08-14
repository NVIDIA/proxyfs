// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"container/list"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/ilayout"
)

func startVolumeManagement() (err error) {
	globals.inodeTableCache = sortedmap.NewBPlusTreeCache(globals.config.InodeTableCacheEvictLowLimit, globals.config.InodeTableCacheEvictLowLimit)
	globals.inodeLeaseLRU = list.New()
	globals.volumeMap = sortedmap.NewLLRBTree(sortedmap.CompareString, &globals)
	globals.mountMap = make(map[string]*mountStruct)

	err = nil
	return
}

func stopVolumeManagement() (err error) {
	globals.inodeTableCache = nil
	globals.inodeLeaseLRU = nil
	globals.volumeMap = nil
	globals.mountMap = nil

	err = nil
	return
}

func (dummy *globalsStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		ok bool
	)

	keyAsString, ok = key.(string)
	if ok {
		err = nil
	} else {
		err = fmt.Errorf("volumeMap's DumpKey(%v) called for non-string", key)
	}

	return
}

func (dummy *globalsStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		ok            bool
		valueAsVolume *volumeStruct
	)

	valueAsVolume, ok = value.(*volumeStruct)
	if ok {
		valueAsString = valueAsVolume.storageURL
		err = nil
	} else {
		err = fmt.Errorf("volumeMap's DumpValue(%v) called for non-*volumeStruct", value)
	}

	return
}

func deleteVolume(volumeName string) (err error) {
	var (
		ok             bool
		volumeAsStruct *volumeStruct
		volumeAsValue  sortedmap.Value
	)

	globals.Lock()

	volumeAsValue, ok, err = globals.volumeMap.GetByKey(volumeName)
	if nil != err {
		logFatal(err)
	}
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("volumeName \"%s\" does not exist", volumeName)
		return
	}

	volumeAsStruct, ok = volumeAsValue.(*volumeStruct)
	if !ok {
		logFatalf("globals.volumeMap[\"%s\"] was not a *volumeStruct", volumeName)
	}

	// The following is only temporary...
	// TODO: Actually gracefully unmount clients, block new mounts, and lazily remove it

	if len(volumeAsStruct.mountMap) != 0 {
		logFatalf("No support for deleting actively mounted volume \"%s\"", volumeName)
	}

	ok, err = globals.volumeMap.DeleteByKey(volumeAsStruct.name)
	if nil != err {
		logFatal(err)
	}
	if !ok {
		logFatalf("globals.volumeMap[\"%s\"] suddenly missing", volumeAsStruct.name)
	}

	globals.Unlock()

	err = nil
	return
}

type postVolumeRootDirDirectoryCallbacksStruct struct {
	io.ReadSeeker
	sortedmap.BPlusTreeCallbacks
	objectNumber uint64
	body         []byte
	readPos      int64
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) Read(p []byte) (n int, err error) {
	n = copy(p, postVolumeRootDirDirectoryCallbacks.body[postVolumeRootDirDirectoryCallbacks.readPos:])
	postVolumeRootDirDirectoryCallbacks.readPos += int64(n)

	if postVolumeRootDirDirectoryCallbacks.readPos == int64(len(postVolumeRootDirDirectoryCallbacks.body)) {
		err = io.EOF
	} else {
		err = nil
	}
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) Seek(offset int64, whence int) (int64, error) {
	var (
		newOffset int64
	)

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = postVolumeRootDirDirectoryCallbacks.readPos + offset
	case io.SeekEnd:
		newOffset = postVolumeRootDirDirectoryCallbacks.readPos + offset
	default:
		return 0, fmt.Errorf("invalid whence (%d)", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("resultant offset cannot be negative")
	}
	if newOffset > int64(len(postVolumeRootDirDirectoryCallbacks.body)) {
		return 0, fmt.Errorf("resultant offset cannot be beyond len(body)")
	}

	postVolumeRootDirDirectoryCallbacks.readPos = newOffset

	return newOffset, nil
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	objectNumber = postVolumeRootDirDirectoryCallbacks.objectNumber
	objectOffset = uint64(len(postVolumeRootDirDirectoryCallbacks.body))

	postVolumeRootDirDirectoryCallbacks.body = append(postVolumeRootDirDirectoryCallbacks.body, nodeByteSlice...)

	err = nil
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = nil
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsString string
		nextPos     int
		ok          bool
	)

	keyAsString, ok = key.(string)
	if !ok {
		err = fmt.Errorf("(*postVolumeRootDirDirectoryCallbacksStruct).PackKey(key:%v) called with non-string", key)
		return
	}

	packedKey = make([]byte, 8+len(keyAsString))

	nextPos, err = ilayout.PutLEStringToBuf(packedKey, 0, keyAsString)
	if nil != err {
		return
	}

	if len(packedKey) != nextPos {
		err = fmt.Errorf("(*postVolumeRootDirDirectoryCallbacksStruct).PackKey(key:%s) logic error", keyAsString)
		return
	}

	err = nil
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		nextPos                      int
		ok                           bool
		valueAsDirectoryEntryValueV1 ilayout.DirectoryEntryValueV1Struct
	)

	valueAsDirectoryEntryValueV1, ok = value.(ilayout.DirectoryEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("(*postVolumeRootDirDirectoryCallbacksStruct).PackValue(value:%v) called with non-DirectoryEntryValueV1Struct", value)
		return
	}

	packedValue = make([]byte, 8+1)

	nextPos, err = ilayout.PutLEUint64ToBuf(packedValue, 0, valueAsDirectoryEntryValueV1.InodeNumber)
	if nil != err {
		return
	}

	nextPos, err = ilayout.PutLEUint8ToBuf(packedValue, nextPos, valueAsDirectoryEntryValueV1.InodeType)
	if nil != err {
		return
	}

	if len(packedValue) != nextPos {
		err = fmt.Errorf("(*postVolumeRootDirDirectoryCallbacksStruct).PackKey(key:%#v) logic error", valueAsDirectoryEntryValueV1)
		return
	}

	err = nil
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

type postVolumeSuperBlockInodeTableCallbacksStruct struct {
	io.ReadSeeker
	sortedmap.BPlusTreeCallbacks
	objectNumber uint64
	body         []byte
	readPos      int64
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) Read(p []byte) (n int, err error) {
	n = copy(p, postVolumeSuperBlockInodeTableCallbacks.body[postVolumeSuperBlockInodeTableCallbacks.readPos:])
	postVolumeSuperBlockInodeTableCallbacks.readPos += int64(n)

	if postVolumeSuperBlockInodeTableCallbacks.readPos == int64(len(postVolumeSuperBlockInodeTableCallbacks.body)) {
		err = io.EOF
	} else {
		err = nil
	}
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) Seek(offset int64, whence int) (int64, error) {
	var (
		newOffset int64
	)

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = postVolumeSuperBlockInodeTableCallbacks.readPos + offset
	case io.SeekEnd:
		newOffset = postVolumeSuperBlockInodeTableCallbacks.readPos + offset
	default:
		return 0, fmt.Errorf("invalid whence (%d)", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("resultant offset cannot be negative")
	}
	if newOffset > int64(len(postVolumeSuperBlockInodeTableCallbacks.body)) {
		return 0, fmt.Errorf("resultant offset cannot be beyond len(body)")
	}

	postVolumeSuperBlockInodeTableCallbacks.readPos = newOffset

	return newOffset, nil
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	objectNumber = postVolumeSuperBlockInodeTableCallbacks.objectNumber
	objectOffset = uint64(len(postVolumeSuperBlockInodeTableCallbacks.body))

	postVolumeSuperBlockInodeTableCallbacks.body = append(postVolumeSuperBlockInodeTableCallbacks.body, nodeByteSlice...)

	err = nil
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = nil
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsUint64 uint64
		nextPos     int
		ok          bool
	)

	keyAsUint64, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("(*postVolumeSuperBlockInodeTableCallbacksStruct).PackKey(key:%v) called with non-uint64", key)
		return
	}

	packedKey = make([]byte, 8)

	nextPos, err = ilayout.PutLEUint64ToBuf(packedKey, 0, keyAsUint64)
	if nil != err {
		return
	}

	if len(packedKey) != nextPos {
		err = fmt.Errorf("(*postVolumeSuperBlockInodeTableCallbacksStruct).PackKey(key:%016X) logic error", keyAsUint64)
		return
	}

	err = nil
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		ok                            bool
		valueAsInodeTableEntryValueV1 ilayout.InodeTableEntryValueV1Struct
	)

	valueAsInodeTableEntryValueV1, ok = value.(ilayout.InodeTableEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("(*postVolumeSuperBlockInodeTableCallbacksStruct).PackValue(value:%v) called with non-InodeTableEntryValueV1Struct", value)
		return
	}

	packedValue, err = valueAsInodeTableEntryValueV1.MarshalInodeTableEntryValueV1()

	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func postVolume(storageURL string, authToken string) (err error) {
	var (
		checkPointV1                            *ilayout.CheckPointV1Struct
		checkPointV1String                      string
		inodeTable                              sortedmap.BPlusTree
		ok                                      bool
		postVolumeRootDirDirectoryCallbacks     *postVolumeRootDirDirectoryCallbacksStruct
		postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct
		reservedToNonce                         uint64
		rootDirDirectory                        sortedmap.BPlusTree
		rootDirInodeHeadV1                      *ilayout.InodeHeadV1Struct
		rootDirInodeHeadV1Buf                   []byte
		rootDirInodeObjectLength                uint64
		rootDirInodeObjectNumber                uint64
		rootDirInodeObjectOffset                uint64
		superBlockObjectLength                  uint64
		superBlockObjectNumber                  uint64
		superBlockObjectOffset                  uint64
		superBlockV1                            *ilayout.SuperBlockV1Struct
		superBlockV1Buf                         []byte
		timeNow                                 = time.Now()
	)

	// Reserve some Nonce values

	rootDirInodeObjectNumber = ilayout.RootDirInodeNumber + 1
	superBlockObjectNumber = rootDirInodeObjectNumber + 1

	reservedToNonce = superBlockObjectNumber

	// Create RootDirInode

	postVolumeRootDirDirectoryCallbacks = &postVolumeRootDirDirectoryCallbacksStruct{
		objectNumber: rootDirInodeObjectNumber,
		body:         make([]byte, 0),
		readPos:      0,
	}

	rootDirDirectory = sortedmap.NewBPlusTree(
		globals.config.RootDirMaxDirEntriesPerBPlusTreePage,
		sortedmap.CompareString,
		postVolumeRootDirDirectoryCallbacks,
		nil)

	ok, err = rootDirDirectory.Put(
		".",
		ilayout.DirectoryEntryValueV1Struct{
			InodeNumber: ilayout.RootDirInodeNumber,
			InodeType:   ilayout.InodeTypeDir,
		})
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("rootDirDirectory.Put(\".\",) returned !ok")
		return
	}

	ok, err = rootDirDirectory.Put(
		"..",
		ilayout.DirectoryEntryValueV1Struct{
			InodeNumber: ilayout.RootDirInodeNumber,
			InodeType:   ilayout.InodeTypeDir,
		})
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("rootDirDirectory.Put(\".\",) returned !ok")
		return
	}

	_, rootDirInodeObjectOffset, rootDirInodeObjectLength, err = rootDirDirectory.Flush(false)
	if nil != err {
		return
	}

	rootDirInodeHeadV1 = &ilayout.InodeHeadV1Struct{
		InodeNumber: ilayout.RootDirInodeNumber,
		InodeType:   ilayout.InodeTypeDir,
		LinkTable: []ilayout.InodeLinkTableEntryStruct{
			{
				ParentDirInodeNumber: ilayout.RootDirInodeNumber,
				ParentDirEntryName:   ".",
			},
			{
				ParentDirInodeNumber: ilayout.RootDirInodeNumber,
				ParentDirEntryName:   "..",
			},
		},
		Size:                0,
		ModificationTime:    timeNow,
		StatusChangeTime:    timeNow,
		Mode:                ilayout.InodeModeMask,
		UserID:              0,
		GroupID:             0,
		StreamTable:         []ilayout.InodeStreamTableEntryStruct{},
		PayloadObjectNumber: rootDirInodeObjectNumber,
		PayloadObjectOffset: rootDirInodeObjectOffset,
		PayloadObjectLength: rootDirInodeObjectLength,
		SymLinkTarget:       "",
		Layout: []ilayout.InodeHeadLayoutEntryV1Struct{
			{
				ObjectNumber:    rootDirInodeObjectNumber,
				ObjectSize:      uint64(len(postVolumeRootDirDirectoryCallbacks.body)),
				BytesReferenced: uint64(len(postVolumeRootDirDirectoryCallbacks.body)),
			},
		},
	}

	rootDirInodeHeadV1Buf, err = rootDirInodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		return
	}

	postVolumeRootDirDirectoryCallbacks.body = append(postVolumeRootDirDirectoryCallbacks.body, rootDirInodeHeadV1Buf...)

	err = swiftObjectPut(storageURL, authToken, rootDirInodeObjectNumber, postVolumeRootDirDirectoryCallbacks)
	if nil != err {
		return
	}

	// Create SuperBlock

	postVolumeSuperBlockInodeTableCallbacks = &postVolumeSuperBlockInodeTableCallbacksStruct{
		objectNumber: superBlockObjectNumber,
		body:         make([]byte, 0),
		readPos:      0,
	}

	inodeTable = sortedmap.NewBPlusTree(
		globals.config.InodeTableMaxInodesPerBPlusTreePage,
		sortedmap.CompareUint64,
		postVolumeSuperBlockInodeTableCallbacks,
		nil)

	ok, err = inodeTable.Put(
		ilayout.RootDirInodeNumber,
		ilayout.InodeTableEntryValueV1Struct{
			InodeHeadObjectNumber: rootDirInodeObjectNumber,
			InodeHeadLength:       uint64(len(rootDirInodeHeadV1Buf)),
		})
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("inodeTable.Put(RootDirInodeNumber,) returned !ok")
		return
	}

	_, superBlockObjectOffset, superBlockObjectLength, err = inodeTable.Flush(false)
	if nil != err {
		return
	}

	superBlockV1 = &ilayout.SuperBlockV1Struct{
		InodeTableRootObjectNumber: superBlockObjectNumber,
		InodeTableRootObjectOffset: superBlockObjectOffset,
		InodeTableRootObjectLength: superBlockObjectLength,
		InodeTableLayout: []ilayout.InodeTableLayoutEntryV1Struct{
			{
				ObjectNumber:    superBlockObjectNumber,
				ObjectSize:      uint64(len(postVolumeSuperBlockInodeTableCallbacks.body)),
				BytesReferenced: uint64(len(postVolumeSuperBlockInodeTableCallbacks.body)),
			},
		},
		InodeObjectCount:     1,
		InodeObjectSize:      uint64(len(postVolumeRootDirDirectoryCallbacks.body)),
		InodeBytesReferenced: uint64(len(postVolumeRootDirDirectoryCallbacks.body)),
	}

	superBlockV1Buf, err = superBlockV1.MarshalSuperBlockV1()
	if nil != err {
		return
	}

	postVolumeSuperBlockInodeTableCallbacks.body = append(postVolumeSuperBlockInodeTableCallbacks.body, superBlockV1Buf...)

	err = swiftObjectPut(storageURL, authToken, superBlockObjectNumber, postVolumeSuperBlockInodeTableCallbacks)
	if nil != err {
		return
	}

	// Create CheckPoint

	checkPointV1 = &ilayout.CheckPointV1Struct{
		Version:                ilayout.CheckPointVersionV1,
		SuperBlockObjectNumber: superBlockObjectNumber,
		SuperBlockLength:       uint64(len(superBlockV1Buf)),
		ReservedToNonce:        reservedToNonce,
	}

	checkPointV1String, err = checkPointV1.MarshalCheckPointV1()
	if nil != err {
		return
	}

	err = swiftObjectPut(storageURL, authToken, ilayout.CheckPointObjectNumber, strings.NewReader(checkPointV1String))
	if nil != err {
		return
	}

	err = nil
	return
}

func putVolume(name string, storageURL string, authToken string) (err error) {
	var (
		ok     bool
		volume *volumeStruct
	)

	volume = &volumeStruct{
		name:                          name,
		storageURL:                    storageURL,
		authToken:                     authToken,
		mountMap:                      make(map[string]*mountStruct),
		healthyMountList:              list.New(),
		leasesExpiredMountList:        list.New(),
		authTokenExpiredMountList:     list.New(),
		deleting:                      false,
		checkPoint:                    nil,
		superBlock:                    nil,
		inodeTable:                    nil,
		inodeTableLayout:              nil,
		activeObjectNumberDeleteList:  list.New(),
		pendingObjectNumberDeleteList: list.New(),
		checkPointControlChan:         nil,
		inodeLeaseMap:                 make(map[uint64]*inodeLeaseStruct),
	}

	globals.Lock()

	ok, err = globals.volumeMap.Put(volume.name, volume)
	if nil != err {
		logFatal(err)
	}

	globals.Unlock()

	if ok {
		err = nil
	} else {
		err = fmt.Errorf("volume \"%s\" already exists", name)
	}

	return
}

func (volume *volumeStruct) checkPointDaemon(checkPointControlChan chan chan error) {
	var (
		checkPointIntervalTimer *time.Timer
		checkPointResponseChan  chan error
		err                     error
		more                    bool
	)

	for {
		checkPointIntervalTimer = time.NewTimer(globals.config.CheckPointInterval)

		select {
		case <-checkPointIntervalTimer.C:
			err = volume.doCheckPoint()
			if nil != err {
				logWarnf("checkPointIntervalTimer-triggered doCheckPoint() failed: %v", err)
			}
		case checkPointResponseChan, more = <-checkPointControlChan:
			if !checkPointIntervalTimer.Stop() {
				<-checkPointIntervalTimer.C
			}

			if more {
				err = volume.doCheckPoint()
				if nil != err {
					logWarnf("requested doCheckPoint() failed: %v", err)
				}

				checkPointResponseChan <- err
			} else {
				volume.checkPointControlWG.Done()
			}
		}
	}
}

func (volume *volumeStruct) doCheckPoint() (err error) {
	var (
		startTime time.Time
	)

	globals.Lock()

	startTime = time.Now()

	err = nil // TODO: perform the actual doCheckPoint()

	globals.stats.VolumeCheckPointUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))

	globals.Unlock()

	return
}

func (volume *volumeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		keyAsInodeNumber uint64
		ok               bool
	)

	keyAsInodeNumber, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("key.(uint64) returned !ok")
		return
	}

	keyAsString = fmt.Sprintf("%016X", keyAsInodeNumber)

	err = nil
	return
}

func (volume *volumeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		ok                          bool
		valueAsInodeTableEntryValue *ilayout.InodeTableEntryValueV1Struct
	)

	valueAsInodeTableEntryValue, ok = value.(*ilayout.InodeTableEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("value.(*ilayout.InodeTableEntryValueV1Struct) returned !ok")
		return
	}

	valueAsString = fmt.Sprintf("[%016X %016X]", valueAsInodeTableEntryValue.InodeHeadObjectNumber, valueAsInodeTableEntryValue.InodeHeadLength)

	err = nil
	return
}

func (volume *volumeStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	var (
		authOK bool
	)

	nodeByteSlice, authOK, err = volume.swiftObjectGetRange(objectNumber, objectOffset, objectLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetRange(objectNumber==0x%016X, objectOffset==0x%016X, objectLength==0x%016X) failed: %v", objectNumber, objectOffset, objectLength, err)
		logError(err)
	} else if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetRange(objectNumber==0x%016X, objectOffset==0x%016X, objectLength==0x%016X) returned !authOK", objectNumber, objectOffset, objectLength)
		logWarn(err)
	}

	return
}

func (volume *volumeStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("%s %s", ETODO, "PutNode()")
	return
}

func (volume *volumeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = fmt.Errorf("%s %s", ETODO, "DiscardNode()")
	return
}

func (volume *volumeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsUint64 uint64
		nextPos     int
		ok          bool
	)

	keyAsUint64, ok = key.(uint64)
	if !ok {
		err = fmt.Errorf("(*volumeStruct).PackKey(key:%v) called with non-uint64", key)
		return
	}

	packedKey = make([]byte, 8)

	nextPos, err = ilayout.PutLEUint64ToBuf(packedKey, 0, keyAsUint64)
	if nil != err {
		return
	}

	if len(packedKey) != nextPos {
		err = fmt.Errorf("(*volumeStruct).PackKey(key:%016X) logic error", keyAsUint64)
		return
	}

	err = nil
	return
}

func (volume *volumeStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		nextPos int
	)

	key, nextPos, err = ilayout.GetLEUint64FromBuf(payloadData, 0)
	if (nil == err) && (nextPos != 8) {
		err = fmt.Errorf("ilayout.GetLEUint64FromBuf(payloadData, 0) consumed %v bytes (8 expected)", nextPos)
	}

	bytesConsumed = 8

	return
}

func (volume *volumeStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		ok                            bool
		valueAsInodeTableEntryValueV1 ilayout.InodeTableEntryValueV1Struct
	)

	valueAsInodeTableEntryValueV1, ok = value.(ilayout.InodeTableEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("(*volumeStruct).PackValue(value:%v) called with non-InodeTableEntryValueV1Struct", value)
		return
	}

	packedValue, err = valueAsInodeTableEntryValueV1.MarshalInodeTableEntryValueV1()

	return
}

func (volume *volumeStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		bytesConsumedAsInt int
	)

	value, bytesConsumedAsInt, err = ilayout.UnmarshalInodeTableEntryValueV1(payloadData)
	if (nil == err) && (bytesConsumedAsInt != 24) {
		err = fmt.Errorf("ilayout.UnmarshalInodeTableEntryValueV1(payloadData) consumed %v bytes (24 expected)", bytesConsumedAsInt)
	}

	bytesConsumed = 24

	return
}

func (volume *volumeStruct) fetchCheckPoint() (checkPointV1 *ilayout.CheckPointV1Struct, err error) {
	var (
		authOK          bool
		checkPointV1Buf []byte
	)

	checkPointV1Buf, authOK, err = volume.swiftObjectGet(ilayout.CheckPointObjectNumber)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGet() failed: %v", err)
		return
	}
	if !authOK {
		err = fmt.Errorf("volume.swiftObjectGet() returned !authOK")
		return
	}

	checkPointV1, err = ilayout.UnmarshalCheckPointV1(string(checkPointV1Buf[:]))

	return
}

func (volume *volumeStruct) fetchSuperBlock(superBlockObjectNumber uint64, superBlockLength uint64) (superBlockV1 *ilayout.SuperBlockV1Struct, err error) {
	var (
		authOK          bool
		superBlockV1Buf []byte
	)

	superBlockV1Buf, authOK, err = volume.swiftObjectGetTail(superBlockObjectNumber, superBlockLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetTail() failed: %v", err)
		return
	}
	if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetTail() returned !authOK")
		return
	}

	superBlockV1, err = ilayout.UnmarshalSuperBlockV1(superBlockV1Buf)

	return
}

func (volume *volumeStruct) fetchInodeHead(inodeHeadObjectNumber uint64, inodeHeadLength uint64) (inodeHeadV1 *ilayout.InodeHeadV1Struct, err error) {
	var (
		authOK         bool
		inodeHeadV1Buf []byte
	)

	inodeHeadV1Buf, authOK, err = volume.swiftObjectGetTail(inodeHeadObjectNumber, inodeHeadLength)
	if nil != err {
		err = fmt.Errorf("volume.swiftObjectGetTail() failed: %v", err)
		return
	}
	if !authOK {
		err = fmt.Errorf("volume.swiftObjectGetTail() returned !authOK")
		return
	}

	inodeHeadV1, err = ilayout.UnmarshalInodeHeadV1(inodeHeadV1Buf)

	return
}
