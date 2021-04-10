// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"container/list"
	"encoding/json"
	"fmt"
	"time"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/utils"
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

type rootDirDirectoryCallbacksStruct struct {
	objectNumber uint64
	buf          []byte
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("Not implemented")
	return
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("Not implemented")
	return
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	err = fmt.Errorf("Not implemented")
	return
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	objectNumber = rootDirDirectoryCallbacks.objectNumber
	objectOffset = uint64(len(rootDirDirectoryCallbacks.buf))

	rootDirDirectoryCallbacks.buf = append(rootDirDirectoryCallbacks.buf, nodeByteSlice...)

	err = nil
	return
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = nil
	return
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsString string
		nextPos     int
		ok          bool
	)

	keyAsString, ok = key.(string)
	if !ok {
		err = fmt.Errorf("*rootDirDirectoryCallbacksStruct).PackKey(key:%v) called with non-string", key)
		return
	}

	packedKey = make([]byte, 8+len(keyAsString))

	nextPos, err = ilayout.PutLEStringToBuf(packedKey, 0, keyAsString)
	if nil != err {
		return
	}

	if len(packedKey) != nextPos {
		err = fmt.Errorf("*rootDirDirectoryCallbacksStruct).PackKey(key:%s) logic error", keyAsString)
		return
	}

	err = nil
	return
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	err = fmt.Errorf("Not implemented")
	return
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		nextPos                      int
		ok                           bool
		valueAsDirectoryEntryValueV1 ilayout.DirectoryEntryValueV1Struct
	)

	valueAsDirectoryEntryValueV1, ok = value.(ilayout.DirectoryEntryValueV1Struct)
	if !ok {
		err = fmt.Errorf("*rootDirDirectoryCallbacksStruct).PackValue(value:%v) called with non-DirectoryEntryValueV1Struct", value)
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
		err = fmt.Errorf("*rootDirDirectoryCallbacksStruct).PackKey(key:%#v) logic error", valueAsDirectoryEntryValueV1)
		return
	}

	err = nil
	return
}

func (rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	err = fmt.Errorf("Not implemented")
	return
}

func postVolume(name string, storageURL string, authToken string) (err error) {
	var (
		ok                        bool
		reservedToNonce           uint64
		rootDirDirectory          sortedmap.BPlusTree
		rootDirDirectoryCallbacks *rootDirDirectoryCallbacksStruct
		rootDirInodeHeadV1        *ilayout.InodeHeadV1Struct
		rootDirInodeHeadV1Buf     []byte
		rootDirInodeObjectLength  uint64
		rootDirInodeObjectNumber  uint64
		rootDirInodeObjectOffset  uint64
		superBlockObjectNumber    uint64
		timeNow                   = time.Now()
	)

	// Reserve some Nonce values

	rootDirInodeObjectNumber = ilayout.RootDirInodeNumber + 1
	superBlockObjectNumber = rootDirInodeObjectNumber + 1

	reservedToNonce = superBlockObjectNumber

	// Construct RootDirInode

	rootDirDirectoryCallbacks = &rootDirDirectoryCallbacksStruct{
		objectNumber: rootDirInodeObjectNumber,
		buf:          make([]byte, 0),
	}

	rootDirDirectory = sortedmap.NewBPlusTree(
		globals.config.InodeTableMaxInodesPerBPlusTreePage,
		sortedmap.CompareString,
		rootDirDirectoryCallbacks,
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
				ObjectSize:      uint64(len(rootDirDirectoryCallbacks.buf)),
				BytesReferenced: uint64(len(rootDirDirectoryCallbacks.buf)),
			},
		},
	}

	rootDirInodeHeadV1Buf, err = rootDirInodeHeadV1.MarshalInodeHeadV1()
	if nil != err {
		return
	}

	fmt.Printf("UNDO: reservedToNonce == %v\n", reservedToNonce)
	fmt.Printf("UNDO: rootDirInodeHeadV1:\n%s\n", utils.JSONify(rootDirInodeHeadV1, true))

	rootDirDirectoryCallbacks.buf = append(rootDirDirectoryCallbacks.buf, rootDirInodeHeadV1Buf...)

	err = swiftObjectPut(storageURL, authToken, rootDirInodeObjectNumber, rootDirDirectoryCallbacks.buf)
	if nil != err {
		return
	}

	// TODO:
	//   3 - Create InodeTable to include RootDirInode
	//   4 - Flush InodeTable to inodeTableCallbacks.buf
	//   5 - Craft SuperBlock to reference flushed InodeTable
	//   6 - Append marshaled SuperBlock to inodeTableCallbacks.buf
	//   7 - PUT inodeTableCallbacks.buf contents to <storageURL>/<superBlockObjectNumber>
	//   8 - Craft CheckPointHeader to reference superBlockObjectNumber & reservedToNonce
	//   9 - PUT X-Checkpoint-Header to <storageURL>

	// Finally, start serving the volume (and pass thru that err return)

	err = putVolume(name, storageURL)

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
