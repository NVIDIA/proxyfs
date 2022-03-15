// Copyright (c) 2015-2022, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"container/list"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/retryrpc"
	"github.com/NVIDIA/proxyfs/utils"
)

func startRetryRPCServer() (err error) {
	var (
		retryrpcServerConfig *retryrpc.ServerConfig
		tlsCertificate       tls.Certificate
	)

	if globals.config.RetryRPCCertFilePath == "" { // && globals.config.RetryRPCKeyFilePath == ""
		tlsCertificate = tls.Certificate{}
	} else { // (globals.config.RetryRPCCertFilePath != "") && (globals.config.RetryRPCKeyFilePath != "")
		tlsCertificate, err = tls.LoadX509KeyPair(globals.config.RetryRPCCertFilePath, globals.config.RetryRPCKeyFilePath)
		if nil != err {
			return
		}
	}

	retryrpcServerConfig = &retryrpc.ServerConfig{
		LongTrim:        globals.config.RetryRPCTTLCompleted,
		ShortTrim:       globals.config.RetryRPCAckTrim,
		DNSOrIPAddr:     globals.config.PublicIPAddr,
		Port:            int(globals.config.RetryRPCPort),
		DeadlineIO:      globals.config.RetryRPCDeadlineIO,
		KeepAlivePeriod: globals.config.RetryRPCKeepAlivePeriod,
		TLSCertificate:  tlsCertificate,
		Logger:          logLoggerNew("RETRYRPC", globals.config.RetryRPCLogEnabled),
	}

	globals.retryrpcServer = retryrpc.NewServer(retryrpcServerConfig)

	retryRPCServer = &RetryRPCServerStruct{}

	err = globals.retryrpcServer.Register(retryRPCServer)
	if nil != err {
		return
	}

	err = globals.retryrpcServer.Start()
	if nil != err {
		return
	}

	globals.retryrpcServer.Run()

	err = nil
	return
}

func stopRetryRPCServer() (err error) {
	globals.retryrpcServer.Close()

	retryRPCServer = nil

	err = nil
	return nil
}

func mount(retryRPCClientID uint64, mountRequest *MountRequestStruct, mountResponse *MountResponseStruct) (err error) {
	var (
		alreadyInGlobalsMountMap            bool
		inodeTableEntryInMemory             *inodeTableLayoutElementStruct
		inodeTableEntryOnDisk               ilayout.InodeTableLayoutEntryV1Struct
		lastCheckPoint                      *ilayout.CheckPointV1Struct
		lastCheckPointAsByteSlice           []byte
		lastCheckPointAsString              string
		mount                               *mountStruct
		mountIDAsByteArray                  []byte
		mountIDAsString                     string
		ok                                  bool
		superBlockPendingDeleteObjectNumber uint64
		startTime                           time.Time = time.Now()
		superBlockAsByteSlice               []byte
		volume                              *volumeStruct
		volumeAsValue                       sortedmap.Value
	)

	logTracef("==> [RPC] Mount(retryRPCClientID: 0x%016X, mountRequest: %+v,)", retryRPCClientID, mountRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] Mount(retryRPCClientID: 0x%016X, mountRequest: %+v,mountResponse: %+v)", retryRPCClientID, mountRequest, mountResponse)
		} else {
			logTracef("<== [RPC] Mount(retryRPCClientID: 0x%016X, mountRequest: %+v,) failed: %v", retryRPCClientID, mountRequest, err)
		}
	}()

	defer func() {
		globals.stats.MountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	volumeAsValue, ok, err = globals.volumeMap.GetByKey(mountRequest.VolumeName)
	if nil != err {
		logFatalf("globals.volumeMap.GetByKey() failed: %v", err)
	}
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownVolumeName, mountRequest.VolumeName)
		return
	}

	volume, ok = volumeAsValue.(*volumeStruct)
	if !ok {
		logFatalf("volumeAsValue.(*volumeStruct) returned !ok")
	}

	if volume.deleting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EVolumeBeingDeleted, mountRequest.VolumeName)
		return
	}

	lastCheckPointAsByteSlice, err = checkPointRead(volume.storageURL, mountRequest.AuthToken)
	if nil != err {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mountRequest.AuthToken)
		return
	}
	lastCheckPointAsString = string(lastCheckPointAsByteSlice[:])

	lastCheckPoint, err = ilayout.UnmarshalCheckPointV1(lastCheckPointAsString)
	if nil != err {
		logFatalf("ilayout.UnmarshalCheckPointV1(lastCheckPointAsString==\"%s\") failed: %v", lastCheckPointAsString, err)
	}

	superBlockAsByteSlice, err = swiftObjectGetTail(volume.storageURL, mountRequest.AuthToken, lastCheckPoint.SuperBlockObjectNumber, lastCheckPoint.SuperBlockLength)
	if nil != err {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mountRequest.AuthToken)
		return
	}

retryGenerateMountID:

	mountIDAsByteArray = utils.FetchRandomByteSlice(mountIDByteArrayLen)
	mountIDAsString = base64.StdEncoding.EncodeToString(mountIDAsByteArray[:])

	_, alreadyInGlobalsMountMap = globals.mountMap[mountIDAsString]
	if alreadyInGlobalsMountMap {
		goto retryGenerateMountID
	}

	mount = &mountStruct{
		volume:              volume,
		mountID:             mountIDAsString,
		retryRPCClientID:    retryRPCClientID,
		unmounting:          false,
		leaseRequestMap:     make(map[uint64]*leaseRequestStruct),
		authToken:           mountRequest.AuthToken,
		lastAuthTime:        startTime,
		mountListMembership: onHealthyMountList,
		inodeOpenMap:        make(map[uint64]uint64),
	}

	mount.volume.mountMapWG.Add(1)

	volume.mountMap[mountIDAsString] = mount
	mount.mountListElement = volume.healthyMountList.PushBack(mount)
	globals.mountMap[mountIDAsString] = mount

	if nil == volume.checkPointControlChan {
		volume.checkPoint = lastCheckPoint

		volume.superBlock, err = ilayout.UnmarshalSuperBlockV1(superBlockAsByteSlice)
		if nil != err {
			logFatalf("ilayout.UnmarshalSuperBlockV1(superBlockAsByteSlice) failed: %v", err)
		}

		volume.inodeTable, err = sortedmap.OldBPlusTree(volume.superBlock.InodeTableRootObjectNumber, volume.superBlock.InodeTableRootObjectOffset, volume.superBlock.InodeTableRootObjectLength, sortedmap.CompareUint64, volume, globals.inodeTableCache)
		if nil != err {
			logFatalf("sortedmap.OldBPlusTree(volume.superBlock.InodeTableRootObjectNumber, volume.superBlock.InodeTableRootObjectOffset, volume.superBlock.InodeTableRootObjectLength, sortedmap.CompareUint64, volume, globals.inodeTableCache) failed: %v", err)
		}

		volume.inodeTableLayout = make(map[uint64]*inodeTableLayoutElementStruct)

		for _, inodeTableEntryOnDisk = range volume.superBlock.InodeTableLayout {
			inodeTableEntryInMemory = &inodeTableLayoutElementStruct{
				bytesWritten:    inodeTableEntryOnDisk.BytesWritten,
				bytesReferenced: inodeTableEntryOnDisk.BytesReferenced,
			}

			volume.inodeTableLayout[inodeTableEntryOnDisk.ObjectNumber] = inodeTableEntryInMemory
		}

		for _, superBlockPendingDeleteObjectNumber = range volume.superBlock.PendingDeleteObjectNumberArray {
			volume.pendingDeleteObjectNumberList.PushBack(superBlockPendingDeleteObjectNumber)
		}

		volume.checkPointControlChan = make(chan chan error)

		volume.checkPointObjectNumber = lastCheckPoint.SuperBlockObjectNumber

		volume.checkPointControlWG.Add(1)

		go volume.checkPointDaemon()
	}

	globals.Unlock()

	mountResponse.MountID = mountIDAsString

	err = nil
	return
}

func renewMount(renewMountRequest *RenewMountRequestStruct, renewMountResponse *RenewMountResponseStruct) (err error) {
	var (
		mount     *mountStruct
		ok        bool
		startTime time.Time = time.Now()
	)

	logTracef("==> [RPC] RenewMount(renewMountRequest: %+v,)", renewMountRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] RenewMount(renewMountRequest: %+v, renewMountResponse: %+v)", renewMountRequest, renewMountResponse)
		} else {
			logTracef("<== [RPC] RenewMount(renewMountRequest: %+v,) failed: %v", renewMountRequest, err)
		}
	}()

	defer func() {
		globals.stats.RenewMountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[renewMountRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, renewMountRequest.MountID)
		return
	}

	mount.authToken = renewMountRequest.AuthToken

	if checkAuthToken(mount.volume.storageURL, mount.authToken) {
		switch mount.mountListMembership {
		case onHealthyMountList:
			mount.volume.healthyMountList.MoveToBack(mount.mountListElement)
		case onAuthTokenExpiredMountList:
			_ = mount.volume.authTokenExpiredMountList.Remove(mount.mountListElement)
			mount.mountListElement = mount.volume.healthyMountList.PushBack(mount)
			mount.mountListMembership = onHealthyMountList
		default:
			logFatalf("mount.mountListMembership (%v) not one of on{Healthy|AuthTokenExpired}MountList")
		}

		err = nil
	} else {
		switch mount.mountListMembership {
		case onHealthyMountList:
			_ = mount.volume.healthyMountList.Remove(mount.mountListElement)
			mount.mountListElement = mount.volume.authTokenExpiredMountList.PushBack(mount)
			mount.mountListMembership = onAuthTokenExpiredMountList
		case onAuthTokenExpiredMountList:
			mount.volume.authTokenExpiredMountList.MoveToBack(mount.mountListElement)
		default:
			logFatalf("mount.mountListMembership (%v) not one of on{Healthy|AuthTokenExpired}MountList")
		}

		err = fmt.Errorf("%s %s", EAuthTokenRejected, renewMountRequest.AuthToken)
	}

	globals.Unlock()

	return
}

func unmount(unmountRequest *UnmountRequestStruct, unmountResponse *UnmountResponseStruct) (err error) {
	var (
		mount             *mountStruct
		ok                bool
		startTime         time.Time = time.Now()
		unmountFinishedWG sync.WaitGroup
	)

	logTracef("==> [RPC] Unmount(unmountRequest: %+v,)", unmountRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] Unmount(unmountRequest: %+v, unmountResponse: %+v)", unmountRequest, unmountResponse)
		} else {
			logTracef("<== [RPC] Unmount(unmountRequest: %+v,) failed: %v", unmountRequest, err)
		}
	}()

	defer func() {
		globals.stats.UnmountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[unmountRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, unmountRequest.MountID)
		return
	}

	mount.unmounting = true

	unmountFinishedWG.Add(1)

	go mount.performUnmount(&unmountFinishedWG)

	globals.Unlock()

	unmountFinishedWG.Wait()

	err = nil
	return
}

func volumeStatus(volumeStatusRequest *VolumeStatusRequestStruct, volumeStatusResponse *VolumeStatusResponseStruct) (err error) {
	var (
		bytesReferenced uint64
		mount           *mountStruct
		numInodes       uint64
		objectCount     uint64
		bytesWritten    uint64
		ok              bool
		startTime       time.Time = time.Now()
	)

	logTracef("==> [RPC] VolumeStatus(volumeStatusRequest: %+v,)", volumeStatusRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] VolumeStatus(volumeStatusRequest: %+v, volumeStatusResponse: %+v)", volumeStatusRequest, volumeStatusResponse)
		} else {
			logTracef("<== [RPC] VolumeStatus(volumeStatusRequest: %+v,) failed: %v", volumeStatusRequest, err)
		}
	}()

	defer func() {
		globals.stats.VolumeStatusUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[volumeStatusRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, volumeStatusRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	numInodes, objectCount, bytesWritten, bytesReferenced = mount.volume.statusWhileLocked()

	globals.Unlock()

	volumeStatusResponse.NumInodes = numInodes
	volumeStatusResponse.ObjectCount = objectCount
	volumeStatusResponse.BytesWritten = bytesWritten
	volumeStatusResponse.BytesReferenced = bytesReferenced

	return
}

func fetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeResponse *FetchNonceRangeResponseStruct) (err error) {
	var (
		mount     *mountStruct
		ok        bool
		startTime time.Time = time.Now()
	)

	logTracef("==> [RPC] FetchNonceRange(renewMountRequest: %+v,)", fetchNonceRangeRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] FetchNonceRange(fetchNonceRangeRequest: %+v, fetchNonceRangeResponse: %+v)", fetchNonceRangeRequest, fetchNonceRangeResponse)
		} else {
			logTracef("<== [RPC] FetchNonceRange(fetchNonceRangeRequest: %+v,) failed: %v", fetchNonceRangeRequest, err)
		}
	}()

	defer func() {
		globals.stats.FetchNonceRangeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[fetchNonceRangeRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, fetchNonceRangeRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	fetchNonceRangeResponse.NextNonce, fetchNonceRangeResponse.NumNoncesFetched, err = mount.volume.fetchNonceRangeWhileLocked()

	globals.Unlock()

	return
}

func getInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryResponse *GetInodeTableEntryResponseStruct) (err error) {
	var (
		inodeTableEntryValue    ilayout.InodeTableEntryValueV1Struct
		inodeTableEntryValueRaw sortedmap.Value
		leaseRequest            *leaseRequestStruct
		mount                   *mountStruct
		ok                      bool
		startTime               time.Time = time.Now()
		volume                  *volumeStruct
	)

	logTracef("==> [RPC] GetInodeTableEntry(getInodeTableEntryRequest: %+v,)", getInodeTableEntryRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] GetInodeTableEntry(getInodeTableEntryRequest: %+v, getInodeTableEntryResponse: %+v)", getInodeTableEntryRequest, getInodeTableEntryResponse)
		} else {
			logTracef("<== [RPC] GetInodeTableEntry(getInodeTableEntryRequest: %+v,) failed: %v", getInodeTableEntryRequest, err)
		}
	}()

	defer func() {
		globals.stats.GetInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[getInodeTableEntryRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, getInodeTableEntryRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	leaseRequest, ok = mount.leaseRequestMap[getInodeTableEntryRequest.InodeNumber]
	if !ok || !leaseRequest.okToRead() {
		globals.Unlock()
		err = fmt.Errorf("%s %016X", EMissingLease, getInodeTableEntryRequest.InodeNumber)
		return
	}

	volume = mount.volume

	inodeTableEntryValueRaw, ok, err = volume.inodeTable.GetByKey(getInodeTableEntryRequest.InodeNumber)
	if nil != err {
		logFatalf("volume.inodeTable.GetByKey(getInodeTableEntryRequest.InodeNumber) failed: %v", err)
	}
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("%s %016X", EUnknownInodeNumber, getInodeTableEntryRequest.InodeNumber)
		return
	}

	inodeTableEntryValue, ok = inodeTableEntryValueRaw.(ilayout.InodeTableEntryValueV1Struct)
	if !ok {
		logFatalf("inodeTableEntryValueRaw.(ilayout.InodeTableEntryValueV1Struct) returned !ok")
	}

	getInodeTableEntryResponse.InodeHeadObjectNumber = inodeTableEntryValue.InodeHeadObjectNumber
	getInodeTableEntryResponse.InodeHeadLength = inodeTableEntryValue.InodeHeadLength

	globals.Unlock()

	err = nil
	return
}

func putInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesResponse *PutInodeTableEntriesResponseStruct) (err error) {
	var (
		dereferencedObjectNumber uint64
		inodeTableEntryValue     ilayout.InodeTableEntryValueV1Struct
		leaseRequest             *leaseRequestStruct
		mount                    *mountStruct
		ok                       bool
		putInodeTableEntry       PutInodeTableEntryStruct
		startTime                time.Time = time.Now()
		volume                   *volumeStruct
	)

	logTracef("==> [RPC] PutInodeTableEntries(putInodeTableEntriesRequest: %+v,)", putInodeTableEntriesRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] PutInodeTableEntries(putInodeTableEntriesRequest: %+v, putInodeTableEntriesResponse: %+v)", putInodeTableEntriesRequest, putInodeTableEntriesResponse)
		} else {
			logTracef("<== [RPC] PutInodeTableEntries(putInodeTableEntriesRequest: %+v,) failed: %v", putInodeTableEntriesRequest, err)
		}
	}()

	defer func() {
		globals.stats.PutInodeTableEntriesUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[putInodeTableEntriesRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, putInodeTableEntriesRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	for _, putInodeTableEntry = range putInodeTableEntriesRequest.UpdatedInodeTableEntryArray {
		leaseRequest, ok = mount.leaseRequestMap[putInodeTableEntry.InodeNumber]
		if !ok || !leaseRequest.okToWrite() {
			globals.Unlock()
			err = fmt.Errorf("%s %016X", EMissingLease, putInodeTableEntry.InodeNumber)
			return
		}
	}

	volume = mount.volume

	for _, putInodeTableEntry = range putInodeTableEntriesRequest.UpdatedInodeTableEntryArray {
		inodeTableEntryValue = ilayout.InodeTableEntryValueV1Struct{
			InodeHeadObjectNumber: putInodeTableEntry.InodeHeadObjectNumber,
			InodeHeadLength:       putInodeTableEntry.InodeHeadLength,
		}

		ok, err = volume.inodeTable.PatchByKey(putInodeTableEntry.InodeNumber, inodeTableEntryValue)
		if nil != err {
			logFatalf("volume.inodeTable.PatchByKey(putInodeTableEntry.InodeNumber,) failed: %v", err)
		}
		if !ok {
			ok, err = volume.inodeTable.Put(putInodeTableEntry.InodeNumber, inodeTableEntryValue)
			if nil != err {
				logFatalf("volume.inodeTable.Put(putInodeTableEntry.InodeNumber,) failed: %v", err)
			}
			if !ok {
				logFatalf("volume.inodeTable.Put(putInodeTableEntry.InodeNumber,) returned !ok")
			}
		}
	}

	volume.superBlock.InodeObjectCount = uint64(int64(volume.superBlock.InodeObjectCount) + putInodeTableEntriesRequest.SuperBlockInodeObjectCountAdjustment)
	volume.superBlock.InodeBytesWritten = uint64(int64(volume.superBlock.InodeBytesWritten) + putInodeTableEntriesRequest.SuperBlockInodeBytesWrittenAdjustment)
	volume.superBlock.InodeBytesReferenced = uint64(int64(volume.superBlock.InodeBytesReferenced) + putInodeTableEntriesRequest.SuperBlockInodeBytesReferencedAdjustment)

	for _, dereferencedObjectNumber = range putInodeTableEntriesRequest.DereferencedObjectNumberArray {
		_ = volume.pendingDeleteObjectNumberList.PushBack(dereferencedObjectNumber)
	}

	volume.dirty = true

	globals.Unlock()

	err = nil
	return
}

func deleteInodeTableEntry(deleteInodeTableEntryRequest *DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryResponse *DeleteInodeTableEntryResponseStruct) (err error) {
	var (
		inodeOpenMapElement *inodeOpenMapElementStruct
		leaseRequest        *leaseRequestStruct
		mount               *mountStruct
		ok                  bool
		startTime           time.Time = time.Now()
	)

	logTracef("==> [RPC] DeleteInodeTableEntry(deleteInodeTableEntryRequest: %+v,)", deleteInodeTableEntryRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] DeleteInodeTableEntry(deleteInodeTableEntryRequest: %+v, deleteInodeTableEntryResponse: %+v)", deleteInodeTableEntryRequest, deleteInodeTableEntryResponse)
		} else {
			logTracef("<== [RPC] DeleteInodeTableEntry(deleteInodeTableEntryRequest: %+v,) failed: %v", deleteInodeTableEntryRequest, err)
		}
	}()

	defer func() {
		globals.stats.DeleteInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[deleteInodeTableEntryRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, deleteInodeTableEntryRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	leaseRequest, ok = mount.leaseRequestMap[deleteInodeTableEntryRequest.InodeNumber]
	if !ok || !leaseRequest.okToWrite() {
		globals.Unlock()
		err = fmt.Errorf("%s %016X", EMissingLease, deleteInodeTableEntryRequest.InodeNumber)
		return
	}

	inodeOpenMapElement, ok = mount.volume.inodeOpenMap[deleteInodeTableEntryRequest.InodeNumber]
	if ok {
		inodeOpenMapElement.markedForDeletion = true
	} else {
		mount.volume.removeInodeWhileLocked(deleteInodeTableEntryRequest.InodeNumber)
	}

	globals.Unlock()

	err = nil
	return
}

func adjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountResponse *AdjustInodeTableEntryOpenCountResponseStruct) (err error) {
	var (
		inodeOpenCount      uint64
		inodeOpenMapElement *inodeOpenMapElementStruct
		leaseRequest        *leaseRequestStruct
		mount               *mountStruct
		ok                  bool
		startTime           time.Time = time.Now()
	)

	logTracef("==> [RPC] AdjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest: %+v,)", adjustInodeTableEntryOpenCountRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] AdjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest: %+v, adjustInodeTableEntryOpenCountResponse: %+v)", adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)
		} else {
			logTracef("<== [RPC] AdjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest: %+v,) failed: %v", adjustInodeTableEntryOpenCountRequest, err)
		}
	}()

	defer func() {
		globals.stats.AdjustInodeTableEntryOpenCountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	if adjustInodeTableEntryOpenCountRequest.Adjustment == 0 {
		err = fmt.Errorf("%s %016X %v", EBadOpenCountAdjustment, adjustInodeTableEntryOpenCountRequest.InodeNumber, adjustInodeTableEntryOpenCountRequest.Adjustment)
		return
	}

	globals.Lock()

	mount, ok = globals.mountMap[adjustInodeTableEntryOpenCountRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, adjustInodeTableEntryOpenCountRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	leaseRequest, ok = mount.leaseRequestMap[adjustInodeTableEntryOpenCountRequest.InodeNumber]
	if !ok || !leaseRequest.okToRead() {
		globals.Unlock()
		err = fmt.Errorf("%s %016X", EMissingLease, adjustInodeTableEntryOpenCountRequest.InodeNumber)
		return
	}

	inodeOpenCount, ok = mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber]
	if ok {
		if inodeOpenCount == 0 {
			logFatalf("mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] returned 0")
		}
		if (adjustInodeTableEntryOpenCountRequest.Adjustment < 0) && (uint64(-adjustInodeTableEntryOpenCountRequest.Adjustment) > inodeOpenCount) {
			globals.Unlock()
			err = fmt.Errorf("%s %016X %v", EBadOpenCountAdjustment, adjustInodeTableEntryOpenCountRequest.InodeNumber, adjustInodeTableEntryOpenCountRequest.Adjustment)
			return
		}
	} else {
		if adjustInodeTableEntryOpenCountRequest.Adjustment < 0 {
			globals.Unlock()
			err = fmt.Errorf("%s %016X %v", EBadOpenCountAdjustment, adjustInodeTableEntryOpenCountRequest.InodeNumber, adjustInodeTableEntryOpenCountRequest.Adjustment)
			return
		}
		inodeOpenCount = 0
	}

	inodeOpenMapElement, ok = mount.volume.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber]
	if ok {
		if inodeOpenMapElement.numMounts == 0 {
			logFatalf("mount.volume.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] returned inodeOpenMapElement.numMounts == 0")
		}
	} else {
		if inodeOpenCount != 0 {
			logFatalf("inodeOpenCount can't be != 0 if inodeOpenMapElement is missing")
		}
		if adjustInodeTableEntryOpenCountRequest.Adjustment < 0 {
			logFatalf("adjustInodeTableEntryOpenCountRequest.Adjustment can't be < 0 if inodeOpenMapElement is missing")
		}
		if globals.inodeOpenCount >= globals.config.OpenFileLimit {
			globals.Unlock()
			err = fmt.Errorf("%s", ETooManyOpens)
			return
		}
		inodeOpenMapElement = &inodeOpenMapElementStruct{
			numMounts:         0,
			markedForDeletion: false,
		}
		mount.volume.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] = inodeOpenMapElement
		globals.inodeOpenCount++
	}

	if adjustInodeTableEntryOpenCountRequest.Adjustment > 0 {
		if inodeOpenCount == 0 {
			mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] = uint64(adjustInodeTableEntryOpenCountRequest.Adjustment)
			inodeOpenMapElement.numMounts++
		} else {
			mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] = inodeOpenCount + uint64(adjustInodeTableEntryOpenCountRequest.Adjustment)
		}
	} else { // adjustInodeTableEntryOpenCountRequest.Adjustment < 0 [we already know it is != 0]
		inodeOpenCount -= uint64(-adjustInodeTableEntryOpenCountRequest.Adjustment)
		if inodeOpenCount == 0 {
			delete(mount.inodeOpenMap, adjustInodeTableEntryOpenCountRequest.InodeNumber)
			inodeOpenMapElement.numMounts--
			if inodeOpenMapElement.numMounts == 0 {
				delete(mount.volume.inodeOpenMap, adjustInodeTableEntryOpenCountRequest.InodeNumber)
				globals.inodeOpenCount--
				if inodeOpenMapElement.markedForDeletion {
					mount.volume.removeInodeWhileLocked(adjustInodeTableEntryOpenCountRequest.InodeNumber)
				}
			}
		} else { // [adjusted] inodeOpenCount > 0
			mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] = inodeOpenCount
		}
	}

	globals.Unlock()

	err = nil
	return
}

func flush(flushRequest *FlushRequestStruct, flushResponse *FlushResponseStruct) (err error) {
	var (
		checkPointResponseChan chan error
		mount                  *mountStruct
		ok                     bool
		startTime              time.Time = time.Now()
		volume                 *volumeStruct
	)

	logTracef("==> [RPC] Flush(flushRequest: %+v,)", flushRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] Flush(flushRequest: %+v, flushResponse: %+v)", flushRequest, flushResponse)
		} else {
			logTracef("<== [RPC] Flush(flushRequest: %+v,) failed: %v", flushRequest, err)
		}
	}()

	defer func() {
		globals.stats.FlushUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	checkPointResponseChan = make(chan error)

	globals.Lock()

	mount, ok = globals.mountMap[flushRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, flushRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	volume = mount.volume

	if nil == volume.checkPointControlChan {
		globals.Unlock()
		err = nil
		return
	}

	volume.checkPointControlChan <- checkPointResponseChan

	globals.Unlock()

	err = <-checkPointResponseChan

	return
}

func lease(leaseRequest *LeaseRequestStruct, leaseResponse *LeaseResponseStruct) (err error) {
	var (
		inodeLease            *inodeLeaseStruct
		leaseRequestOperation *leaseRequestOperationStruct
		mount                 *mountStruct
		ok                    bool
		startTime             time.Time = time.Now()
		volume                *volumeStruct
	)

	logTracef("==> [RPC] Lease(leaseRequest: %+v,)", leaseRequest)
	defer func() {
		if err == nil {
			logTracef("<== [RPC] Lease(leaseRequest: %+v, leaseResponse: %+v)", leaseRequest, leaseResponse)
		} else {
			logTracef("<== [RPC] Lease(leaseRequest: %+v,) failed: %v", leaseRequest, err)
		}
	}()

	defer func() {
		globals.stats.LeaseUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	switch leaseRequest.LeaseRequestType {
	case LeaseRequestTypeShared:
		defer func() {
			globals.stats.SharedLeaseRequestUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()
	case LeaseRequestTypePromote:
		defer func() {
			globals.stats.PromoteLeaseRequestUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()
	case LeaseRequestTypeExclusive:
		defer func() {
			globals.stats.ExclusiveLeaseRequestUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()
	case LeaseRequestTypeDemote:
		defer func() {
			globals.stats.DemoteLeaseRequestUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()
	case LeaseRequestTypeRelease:
		defer func() {
			globals.stats.ReleaseLeaseRequestUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
		}()
	default:
		leaseResponse.LeaseResponseType = LeaseResponseTypeDenied
		err = fmt.Errorf("%s LeaseRequestType %v not supported", ELeaseRequestDenied, leaseRequest.LeaseRequestType)
		return
	}

	globals.Lock()

	mount, ok = globals.mountMap[leaseRequest.MountID]
	if !ok || mount.unmounting {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, leaseRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	volume = mount.volume

	if (leaseRequest.LeaseRequestType == LeaseRequestTypeShared) || (leaseRequest.LeaseRequestType == LeaseRequestTypeExclusive) {
		inodeLease, ok = volume.inodeLeaseMap[leaseRequest.InodeNumber]
		if !ok {
			inodeLease = &inodeLeaseStruct{
				volume:               volume,
				inodeNumber:          leaseRequest.InodeNumber,
				leaseState:           inodeLeaseStateNone,
				requestChan:          make(chan *leaseRequestOperationStruct),
				stopChan:             make(chan struct{}),
				stopping:             false,
				sharedHoldersList:    list.New(),
				promotingHolder:      nil,
				exclusiveHolder:      nil,
				releasingHoldersList: list.New(),
				requestedList:        list.New(),
				lastGrantTime:        time.Time{},
				lastInterruptTime:    time.Time{},
				interruptsSent:       0,
				longAgoTimer:         &time.Timer{},
				interruptTimer:       &time.Timer{},
			}

			volume.inodeLeaseMap[leaseRequest.InodeNumber] = inodeLease
			inodeLease.lruElement = globals.inodeLeaseLRU.PushBack(inodeLease)

			volume.leaseHandlerWG.Add(1)
			go inodeLease.handler()

			if (globals.inodeLeaseExpirerWG == nil) && (globals.inodeLeaseLRU.Len() > int(globals.config.LeaseEvictHighLimit)) {
				globals.inodeLeaseExpirerWG = &sync.WaitGroup{}
				globals.inodeLeaseExpirerWG.Add(1)
				go inodeLeaseExpirer()
			}
		}
	} else { // in.LeaseRequestType is one of LeaseRequestType{Promote|Demote|Release}
		inodeLease, ok = volume.inodeLeaseMap[leaseRequest.InodeNumber]
		if !ok {
			globals.Unlock()
			leaseResponse.LeaseResponseType = LeaseResponseTypeDenied
			err = fmt.Errorf("%s LeaseRequestType %v not allowed for non-existent Lease", ELeaseRequestDenied, leaseRequest.LeaseRequestType)
			return
		}
	}

	// Send Lease Request Operation to *inodeLeaseStruct.handler()
	//
	// Note that we still hold the globals.Lock, so inodeLease can't disappear out from under us

	leaseRequestOperation = &leaseRequestOperationStruct{
		mount:            mount,
		inodeLease:       inodeLease,
		LeaseRequestType: leaseRequest.LeaseRequestType,
		replyChan:        make(chan LeaseResponseType),
	}

	inodeLease.requestChan <- leaseRequestOperation

	globals.Unlock()

	leaseResponse.LeaseResponseType = <-leaseRequestOperation.replyChan

	return
}

func (mount *mountStruct) authTokenHasExpired() (authTokenExpired bool) {
	var (
		authTokenIsAuthorized bool
		startTime             time.Time = time.Now()
	)

	if mount.mountListMembership == onAuthTokenExpiredMountList {
		return true
	}

	if startTime.Sub(mount.lastAuthTime) < globals.config.AuthTokenCheckInterval {
		return false
	}

	authTokenIsAuthorized = checkAuthToken(mount.volume.storageURL, mount.authToken)

	globals.stats.AuthTokenCheckUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))

	// if nil == err {
	if authTokenIsAuthorized {
		mount.lastAuthTime = startTime
		return false
	} else {
		return true
	}
}
