// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/sortedmap"
)

func startVolumeManagement() (err error) {
	globals.inodeTableCache = sortedmap.NewBPlusTreeCache(globals.config.InodeTableCacheEvictLowLimit, globals.config.InodeTableCacheEvictLowLimit)
	globals.volumeMap = sortedmap.NewLLRBTree(sortedmap.CompareString, &globals)
	globals.mountMap = make(map[string]*mountStruct)

	err = nil
	return
}

func stopVolumeManagement() (err error) {
	globals.inodeTableCache = nil
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
		valueAsString = fmt.Sprintf("%s", valueAsVolume.storageURL)
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
		logFatalf("[IMGR]globals.volumeMap[\"%s\"] was not a *volumeStruct", volumeName)
	}

	// The following is only temporary...
	// TODO: Actually gracefully unmount clients, block new mounts, and lazily remove it

	if 0 != len(volumeAsStruct.mountMap) {
		logFatalf("[IMGR]No support for deleting actively mounted volume \"%s\"", volumeName)
	}

	ok, err = globals.volumeMap.DeleteByKey(volumeAsStruct.name)
	if nil != err {
		logFatal(err)
	}
	if !ok {
		logFatalf("[IMGR]globals.volumeMap[\"%s\"] suddenly missing", volumeAsStruct.name)
	}

	globals.Unlock()

	err = nil
	return
}

type volumeGETStruct struct {
	Name          string
	StorageURL    string
	HealthyMounts uint64
	ExpiredMounts uint64
}

func getVolumeAsJSON(volumeName string) (volume []byte, err error) {
	var (
		ok             bool
		volumeAsStruct *volumeStruct
		volumeAsValue  sortedmap.Value
		volumeToReturn *volumeGETStruct
	)

	globals.RLock()

	volumeAsValue, ok, err = globals.volumeMap.GetByKey(volumeName)
	if nil != err {
		logFatal(err)
	}
	if !ok {
		globals.RUnlock()
		err = fmt.Errorf("volumeName \"%s\" does not exist", volumeName)
		return
	}

	volumeAsStruct, ok = volumeAsValue.(*volumeStruct)
	if !ok {
		logFatalf("[IMGR]globals.volumeMap[\"%s\"] was not a *volumeStruct", volumeName)
	}

	volumeToReturn = &volumeGETStruct{
		Name:          volumeAsStruct.name,
		StorageURL:    volumeAsStruct.storageURL,
		HealthyMounts: uint64(volumeAsStruct.healthyMountList.Len()),
		ExpiredMounts: uint64(volumeAsStruct.expiredMountList.Len()),
	}

	globals.RUnlock()

	volume, err = json.Marshal(volumeToReturn)
	if nil != err {
		logFatal(err)
	}

	err = nil
	return
}

func getVolumeListAsJSON() (volumeList []byte) {
	var (
		err                error
		ok                 bool
		volumeAsStruct     *volumeStruct
		volumeAsValue      sortedmap.Value
		volumeListIndex    int
		volumeListLen      int
		volumeListToReturn []*volumeGETStruct
	)

	globals.RLock()

	volumeListLen, err = globals.volumeMap.Len()
	if nil != err {
		logFatal(err)
	}

	volumeListToReturn = make([]*volumeGETStruct, volumeListLen)

	for volumeListIndex = 0; volumeListIndex < volumeListLen; volumeListIndex++ {
		_, volumeAsValue, ok, err = globals.volumeMap.GetByIndex(volumeListIndex)
		if nil != err {
			logFatal(err)
		}
		if !ok {
			logFatalf("[IMGR]globals.volumeMap[] len (%d) is wrong", volumeListLen)
		}

		volumeAsStruct, ok = volumeAsValue.(*volumeStruct)
		if !ok {
			logFatalf("[IMGR]globals.volumeMap[%d] was not a *volumeStruct", volumeListIndex)
		}

		volumeListToReturn[volumeListIndex] = &volumeGETStruct{
			Name:          volumeAsStruct.name,
			StorageURL:    volumeAsStruct.storageURL,
			HealthyMounts: uint64(volumeAsStruct.healthyMountList.Len()),
			ExpiredMounts: uint64(volumeAsStruct.expiredMountList.Len()),
		}
	}

	globals.RUnlock()

	volumeList, err = json.Marshal(volumeListToReturn)
	if nil != err {
		logFatal(err)
	}

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
	err = fmt.Errorf("Not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("Not implemented")
	return
}

func (postVolumeRootDirDirectoryCallbacks *postVolumeRootDirDirectoryCallbacksStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("Not implemented")
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
	err = fmt.Errorf("Not implemented")
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
	err = fmt.Errorf("Not implemented")
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
	err = fmt.Errorf("Not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("Not implemented")
	return
}

func (postVolumeSuperBlockInodeTableCallbacks *postVolumeSuperBlockInodeTableCallbacksStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("Not implemented")
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
	err = fmt.Errorf("Not implemented")
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
	err = fmt.Errorf("Not implemented")
	return
}

func postVolume(storageURL string, authToken string) (err error) {
	var (
		checkPointHeaderV1                      *ilayout.CheckPointHeaderV1Struct
		checkPointHeaderV1String                string
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
		CreationTime:        timeNow,
		ModificationTime:    timeNow,
		AccessTime:          timeNow,
		AttrChangeTime:      timeNow,
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
			InodeHeadObjectNumber: ilayout.RootDirInodeNumber,
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

	// Create CheckPointHeader

	checkPointHeaderV1 = &ilayout.CheckPointHeaderV1Struct{
		Version:                ilayout.CheckPointHeaderVersionV1,
		SuperBlockObjectNumber: superBlockObjectNumber,
		SuperBlockLength:       uint64(len(superBlockV1Buf)),
		ReservedToNonce:        reservedToNonce,
	}

	checkPointHeaderV1String, err = checkPointHeaderV1.MarshalCheckPointHeaderV1()
	if nil != err {
		return
	}

	err = swiftContainerHeaderSet(storageURL, authToken, ilayout.CheckPointHeaderName, checkPointHeaderV1String)
	if nil != err {
		return
	}

	err = nil
	return
}

func putVolume(name string, storageURL string) (err error) {
	var (
		ok     bool
		volume *volumeStruct
	)

	globals.Lock()

	volume = &volumeStruct{
		name:             name,
		storageURL:       storageURL,
		mountMap:         make(map[string]*mountStruct),
		healthyMountList: list.New(),
		expiredMountList: list.New(),
		deleting:         false,
		inodeTable:       nil,
	}

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
