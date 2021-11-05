// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"container/list"
	"crypto/tls"
	"encoding/base64"
	"fmt"
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

	lastCheckPointAsByteSlice, err = swiftObjectGet(volume.storageURL, mountRequest.AuthToken, ilayout.CheckPointObjectNumber)
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
		volume:                 volume,
		mountID:                mountIDAsString,
		retryRPCClientID:       retryRPCClientID,
		acceptingLeaseRequests: true,
		leaseRequestMap:        make(map[uint64]*leaseRequestStruct),
		leasesExpired:          false,
		authTokenExpired:       false,
		authToken:              mountRequest.AuthToken,
		lastAuthTime:           startTime,
		inodeOpenMap:           make(map[uint64]uint64),
	}

	volume.mountMap[mountIDAsString] = mount
	mount.listElement = volume.healthyMountList.PushBack(mount)
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
				objectSize:      inodeTableEntryOnDisk.ObjectSize,
				bytesReferenced: inodeTableEntryOnDisk.BytesReferenced,
			}

			volume.inodeTableLayout[inodeTableEntryOnDisk.ObjectNumber] = inodeTableEntryInMemory
		}

		for _, superBlockPendingDeleteObjectNumber = range volume.superBlock.PendingDeleteObjectNumberArray {
			volume.pendingDeleteObjectNumberList.PushBack(superBlockPendingDeleteObjectNumber)
		}

		volume.checkPointControlChan = make(chan chan error)

		volume.checkPointControlWG.Add(1)

		go volume.checkPointDaemon(volume.checkPointControlChan)
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

	defer func() {
		globals.stats.RenewMountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[renewMountRequest.MountID]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, renewMountRequest.MountID)
		return
	}

	mount.authToken = renewMountRequest.AuthToken

	_, err = swiftObjectGet(mount.volume.storageURL, mount.authToken, ilayout.CheckPointObjectNumber)
	if nil == err {
		if mount.leasesExpired {
			mount.volume.leasesExpiredMountList.MoveToBack(mount.listElement)
		} else {
			if mount.authTokenExpired {
				_ = mount.volume.authTokenExpiredMountList.Remove(mount.listElement)
				mount.listElement = mount.volume.healthyMountList.PushBack(mount)
			} else {
				mount.volume.healthyMountList.MoveToBack(mount.listElement)
			}
		}
	} else {
		err = fmt.Errorf("%s %s", EAuthTokenRejected, renewMountRequest.AuthToken)
	}

	globals.Unlock()

	return
}

func unmount(unmountRequest *UnmountRequestStruct, unmountResponse *UnmountResponseStruct) (err error) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.UnmountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf(ETODO + " unmount")
}

func volumeStatus(volumeStatusRequest *VolumeStatusRequestStruct, volumeStatusResponse *VolumeStatusResponseStruct) (err error) {
	var (
		bytesReferenced uint64
		mount           *mountStruct
		numInodes       uint64
		objectCount     uint64
		objectSize      uint64
		ok              bool
		startTime       time.Time = time.Now()
	)

	defer func() {
		globals.stats.VolumeStatusUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[volumeStatusRequest.MountID]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, volumeStatusRequest.MountID)
		return
	}

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	numInodes, objectCount, objectSize, bytesReferenced = mount.volume.statusWhileLocked()

	globals.Unlock()

	volumeStatusResponse.NumInodes = numInodes
	volumeStatusResponse.ObjectCount = objectCount
	volumeStatusResponse.ObjectSize = objectSize
	volumeStatusResponse.BytesReferenced = bytesReferenced

	return
}

func fetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeResponse *FetchNonceRangeResponseStruct) (err error) {
	var (
		mount     *mountStruct
		ok        bool
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.FetchNonceRangeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[fetchNonceRangeRequest.MountID]
	if !ok {
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

	defer func() {
		globals.stats.GetInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[getInodeTableEntryRequest.MountID]
	if !ok {
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
	if !ok || ((leaseRequestStateSharedGranted != leaseRequest.requestState) && (leaseRequestStateExclusiveGranted != leaseRequest.requestState)) {
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

	defer func() {
		globals.stats.PutInodeTableEntriesUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[putInodeTableEntriesRequest.MountID]
	if !ok {
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
		if !ok || (leaseRequestStateExclusiveGranted != leaseRequest.requestState) {
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
	volume.superBlock.InodeObjectSize = uint64(int64(volume.superBlock.InodeObjectSize) + putInodeTableEntriesRequest.SuperBlockInodeObjectSizeAdjustment)
	volume.superBlock.InodeBytesReferenced = uint64(int64(volume.superBlock.InodeBytesReferenced) + putInodeTableEntriesRequest.SuperBlockInodeBytesReferencedAdjustment)

	for _, dereferencedObjectNumber = range putInodeTableEntriesRequest.DereferencedObjectNumberArray {
		_ = volume.pendingDeleteObjectNumberList.PushBack(dereferencedObjectNumber)
	}

	globals.Unlock()

	err = nil
	return
}

func deleteInodeTableEntry(deleteInodeTableEntryRequest *DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryResponse *DeleteInodeTableEntryResponseStruct) (err error) {
	var (
		leaseRequest *leaseRequestStruct
		mount        *mountStruct
		ok           bool
		startTime    time.Time = time.Now()
	)

	defer func() {
		globals.stats.DeleteInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[deleteInodeTableEntryRequest.MountID]
	if !ok {
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
	if !ok || (leaseRequestStateExclusiveGranted != leaseRequest.requestState) {
		globals.Unlock()
		err = fmt.Errorf("%s %016X", EMissingLease, deleteInodeTableEntryRequest.InodeNumber)
		return
	}

	// TODO: Need to actually clean up the Inode... but for now, just remove it

	ok, err = mount.volume.inodeTable.DeleteByKey(deleteInodeTableEntryRequest.InodeNumber)
	if nil != err {
		logFatalf("volume.inodeTable.DeleteByKey(n.InodeNumber) failed: %v", err)
	}
	if !ok {
		logFatalf("volume.inodeTable.DeleteByKey(n.InodeNumber) returned !ok")
	}

	globals.Unlock()

	err = nil
	return
}

// TODO - The thinking is that some "to-be-deleted-upon-last-close" logic will substitute
// for the InodeTable entry... and that some background garbage collector will do the work.

func adjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountResponse *AdjustInodeTableEntryOpenCountResponseStruct) (err error) {
	var (
		leaseRequest       *leaseRequestStruct
		mount              *mountStruct
		ok                 bool
		newMountOpenCount  uint64
		newVolumeOpenCount uint64
		oldMountOpenCount  uint64
		oldVolumeOpenCount uint64
		startTime          time.Time = time.Now()
	)

	defer func() {
		globals.stats.AdjustInodeTableEntryOpenCountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.Lock()

	mount, ok = globals.mountMap[adjustInodeTableEntryOpenCountRequest.MountID]
	if !ok {
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
	if !ok || ((leaseRequestStateSharedGranted != leaseRequest.requestState) && (leaseRequestStateExclusiveGranted != leaseRequest.requestState)) {
		globals.Unlock()
		err = fmt.Errorf("%s %016X", EMissingLease, adjustInodeTableEntryOpenCountRequest.InodeNumber)
		return
	}

	if adjustInodeTableEntryOpenCountRequest.Adjustment >= 0 {
		oldMountOpenCount, ok = mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber]
		if ok {
			oldVolumeOpenCount, ok = mount.volume.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber]
			if !ok {
				logFatalf("mount.volumeinodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] returned !ok after mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] returned ok")
			}
		} else {
			oldMountOpenCount = 0
			oldVolumeOpenCount, ok = mount.volume.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber]
			if !ok {
				oldVolumeOpenCount = 0
			}
		}

		if adjustInodeTableEntryOpenCountRequest.Adjustment == 0 {
			newMountOpenCount = oldMountOpenCount
			newVolumeOpenCount = oldVolumeOpenCount
		} else {
			newMountOpenCount = oldMountOpenCount + uint64(adjustInodeTableEntryOpenCountRequest.Adjustment)
			newVolumeOpenCount = oldVolumeOpenCount + uint64(adjustInodeTableEntryOpenCountRequest.Adjustment)

			mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] = newMountOpenCount
			mount.volume.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] = newVolumeOpenCount
		}
	} else { // adjustInodeTableEntryOpenCountRequest.Adjustment < 0
		oldMountOpenCount, ok = mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber]
		if !ok || (oldMountOpenCount < uint64(-adjustInodeTableEntryOpenCountRequest.Adjustment)) {
			globals.Unlock()
			err = fmt.Errorf("%s %016X %v", EBadOpenCountAdjustment, adjustInodeTableEntryOpenCountRequest.InodeNumber, adjustInodeTableEntryOpenCountRequest.Adjustment)
			return
		}
		oldVolumeOpenCount, ok = mount.volume.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber]
		if !ok || (oldVolumeOpenCount < uint64(-adjustInodeTableEntryOpenCountRequest.Adjustment)) {
			logFatalf("mount.volumeinodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] returned !ok || oldVolumeOpenCount < uint64(-adjustInodeTableEntryOpenCountRequest.Adjustment)")
		}

		newMountOpenCount = oldMountOpenCount - uint64(-adjustInodeTableEntryOpenCountRequest.Adjustment)
		newVolumeOpenCount = oldVolumeOpenCount - uint64(-adjustInodeTableEntryOpenCountRequest.Adjustment)

		if newMountOpenCount == 0 {
			delete(mount.inodeOpenMap, adjustInodeTableEntryOpenCountRequest.InodeNumber)
		} else {
			mount.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] = newMountOpenCount
		}

		if newVolumeOpenCount == 0 {
			delete(mount.volume.inodeOpenMap, adjustInodeTableEntryOpenCountRequest.InodeNumber)

			// TODO: Check for pending delete... and, if so, actuially delete the inode
		} else {
			mount.volume.inodeOpenMap[adjustInodeTableEntryOpenCountRequest.InodeNumber] = newVolumeOpenCount
		}
	}

	globals.Unlock()

	adjustInodeTableEntryOpenCountResponse.CurrentOpenCountThisMount = newMountOpenCount
	adjustInodeTableEntryOpenCountResponse.CurrentOpenCountAllMounts = newVolumeOpenCount

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

	defer func() {
		globals.stats.FlushUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	checkPointResponseChan = make(chan error)

	globals.Lock()

	mount, ok = globals.mountMap[flushRequest.MountID]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, flushRequest.MountID)
		return
	}

	volume = mount.volume

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

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
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, leaseRequest.MountID)
		return
	}

	volume = mount.volume

	if mount.authTokenHasExpired() {
		globals.Unlock()
		err = fmt.Errorf("%s %s", EAuthTokenRejected, mount.authToken)
		return
	}

	if (leaseRequest.LeaseRequestType == LeaseRequestTypeShared) || (leaseRequest.LeaseRequestType == LeaseRequestTypeExclusive) {
		if !mount.acceptingLeaseRequests {
			globals.Unlock()
			leaseResponse.LeaseResponseType = LeaseResponseTypeDenied
			err = fmt.Errorf("%s LeaseRequestType %v not currently being accepted", ELeaseRequestDenied, leaseRequest.LeaseRequestType)
			return
		}
		inodeLease, ok = volume.inodeLeaseMap[leaseRequest.InodeNumber]
		if !ok {
			inodeLease = &inodeLeaseStruct{
				volume:               volume,
				inodeNumber:          leaseRequest.InodeNumber,
				leaseState:           inodeLeaseStateNone,
				requestChan:          make(chan *leaseRequestOperationStruct),
				stopChan:             make(chan struct{}),
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
	// Note that we still hold the volumesLock, so inodeLease can't disappear out from under us

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
		err       error
		startTime time.Time = time.Now()
	)

	if mount.authTokenExpired {
		return true
	}

	if startTime.Sub(mount.lastAuthTime) < globals.config.AuthTokenCheckInterval {
		return false
	}

	_, err = swiftObjectGet(mount.volume.storageURL, mount.authToken, ilayout.CheckPointObjectNumber)

	globals.stats.AuthTokenCheckUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))

	if nil == err {
		mount.lastAuthTime = startTime
		return false
	} else {
		return true
	}
}
