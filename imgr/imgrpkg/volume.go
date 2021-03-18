// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"encoding/json"
	"fmt"

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
		HealthyMounts: volumeAsStruct.numMountAuthTokenValid,
		ExpiredMounts: uint64(len(volumeAsStruct.mountMap)) - volumeAsStruct.numMountAuthTokenValid,
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
			HealthyMounts: volumeAsStruct.numMountAuthTokenValid,
			ExpiredMounts: uint64(len(volumeAsStruct.mountMap)) - volumeAsStruct.numMountAuthTokenValid,
		}
	}

	globals.RUnlock()

	volumeList, err = json.Marshal(volumeListToReturn)
	if nil != err {
		logFatal(err)
	}

	return
}

func putVolume(name string, storageURL string) (err error) {
	var (
		ok     bool
		volume *volumeStruct
	)

	globals.Lock()

	volume = &volumeStruct{
		name:                   name,
		storageURL:             storageURL,
		mountMap:               make(map[string]*mountStruct),
		numMountAuthTokenValid: 0,
		deleting:               false,
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
