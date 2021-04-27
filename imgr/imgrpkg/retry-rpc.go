// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/blunder"
	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/retryrpc"
	"github.com/NVIDIA/proxyfs/utils"
)

func startRetryRPCServer() (err error) {
	var (
		retryrpcServerConfig *retryrpc.ServerConfig
		tlsCertificate       tls.Certificate
	)

	if "" == globals.config.RetryRPCCertFilePath { // && "" == globals.config.RetryRPCKeyFilePath
		tlsCertificate = tls.Certificate{}
	} else { // ("" != globals.config.RetryRPCCertFilePath) && ("" != globals.config.RetryRPCKeyFilePath)
		tlsCertificate, err = tls.LoadX509KeyPair(globals.config.RetryRPCCertFilePath, globals.config.RetryRPCKeyFilePath)
		if nil != err {
			return
		}
	}

	retryrpcServerConfig = &retryrpc.ServerConfig{
		LongTrim:        globals.config.RetryRPCTTLCompleted,
		ShortTrim:       globals.config.RetryRPCAckTrim,
		IPAddr:          globals.config.PublicIPAddr,
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
		alreadyInGlobalsMountMap     bool
		lastCheckPointHeader         *ilayout.CheckPointHeaderV1Struct
		lastCheckPointHeaderAsString string
		mount                        *mountStruct
		mountIDAsByteArray           []byte
		mountIDAsString              string
		ok                           bool
		startTime                    time.Time
		volume                       *volumeStruct
		volumeAsValue                sortedmap.Value
	)

	startTime = time.Now()

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

	volume.Lock()

	if volume.deleting {
		volume.Unlock()
		globals.Unlock()
		err = fmt.Errorf("%s %s", EVolumeBeingDeleted, mountRequest.VolumeName)
		return
	}

	lastCheckPointHeaderAsString, err = swiftContainerHeaderGet(volume.storageURL, mountRequest.AuthToken, ilayout.CheckPointHeaderName)
	if nil != err {
		volume.Unlock()
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
		volume:           volume,
		mountID:          mountIDAsString,
		leasesExpired:    false,
		authTokenExpired: false,
		authToken:        mountRequest.AuthToken,
		lastAuthTime:     time.Now(),
	}

	volume.mountMap[mountIDAsString] = mount
	mount.listElement = volume.healthyMountList.PushBack(mount)
	globals.mountMap[mountIDAsString] = mount

	if nil == volume.checkPointControlChan {
		lastCheckPointHeader, err = ilayout.UnmarshalCheckPointHeaderV1(lastCheckPointHeaderAsString)
		if nil != err {
			logFatalf("ilayout.UnmarshalCheckPointHeaderV1(lastCheckPointHeaderAsString==\"%s\") failed: %v", lastCheckPointHeaderAsString, err)
		}

		volume.checkPointHeader = lastCheckPointHeader

		volume.checkPointControlChan = make(chan chan error)

		volume.checkPointControlWG.Add(1)

		go volume.checkPointDaemon(volume.checkPointControlChan)
	}

	volume.Unlock()
	globals.Unlock()

	mountResponse.MountID = mountIDAsString

	err = nil
	return
}

func renewMount(renewMountRequest *RenewMountRequestStruct, renewMountResponse *RenewMountResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.RenewMountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf(ETODO + " renewMount")
}

func unmount(unmountRequest *UnmountRequestStruct, unmountResponse *UnmountResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.UnmountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf(ETODO + " unmount")
}

func fetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeResponse *FetchNonceRangeResponseStruct) (err error) {
	var (
		mount     *mountStruct
		ok        bool
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.FetchNonceRangeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	globals.RLock()

	mount, ok = globals.mountMap[fetchNonceRangeRequest.MountID]
	if !ok {
		globals.RUnlock()
		err = fmt.Errorf("MountID not recognized")
		return
	}

	fmt.Printf("TODO: Perform fetchNonceRange() for mountStruct @ %p\n", mount)

	globals.RUnlock()

	return fmt.Errorf(ETODO + " fetchNonceRange")
}

func getInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryResponse *GetInodeTableEntryResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.GetInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf(ETODO + " getInodeTableEntry")
}

func putInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesResponse *PutInodeTableEntriesResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.PutInodeTableEntriesUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf(ETODO + " putInodeTableEntries")
}

func deleteInodeTableEntry(deleteInodeTableEntryRequest *DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryResponse *DeleteInodeTableEntryResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.DeleteInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf(ETODO + " deleteInodeTableEntry")
}

func adjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountResponse *AdjustInodeTableEntryOpenCountResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.AdjustInodeTableEntryOpenCountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf(ETODO + " adjustInodeTableEntryOpenCount")
}

func flush(flushRequest *FlushRequestStruct, flushResponse *FlushResponseStruct) (err error) {
	var (
		checkPointResponseChan chan error
		mount                  *mountStruct
		ok                     bool
		startTime              time.Time
		volume                 *volumeStruct
	)

	startTime = time.Now()

	defer func() {
		globals.stats.FlushUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	checkPointResponseChan = make(chan error)

	globals.RLock()

	mount, ok = globals.mountMap[flushRequest.MountID]
	if !ok {
		globals.RUnlock()
		err = fmt.Errorf("%s %s", EUnknownMountID, flushRequest.MountID)
		return
	}

	volume = mount.volume

	volume.RLock()
	globals.RUnlock()

	if nil == volume.checkPointControlChan {
		volume.RUnlock()
		err = nil
		return
	}

	volume.checkPointControlChan <- checkPointResponseChan

	volume.RUnlock()

	err = <-checkPointResponseChan

	return
}

func lease(leaseRequest *LeaseRequestStruct, leaseResponse *LeaseResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

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
		err = fmt.Errorf("LeaseRequestType %v not supported", leaseRequest.LeaseRequestType)
		err = blunder.AddError(err, blunder.BadLeaseRequest)
		return
	}

	return fmt.Errorf(ETODO + " lease")
}
