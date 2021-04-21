// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/NVIDIA/proxyfs/blunder"
	"github.com/NVIDIA/proxyfs/retryrpc"
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
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.MountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf("TODO")
}

func renewMount(renewMountRequest *RenewMountRequestStruct, renewMountResponse *RenewMountResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.RenewMountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf("TODO")
}

func unmount(unmountRequest *UnmountRequestStruct, unmountResponse *UnmountResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.UnmountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf("TODO")
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

	return fmt.Errorf("TODO")
}

func getInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryResponse *GetInodeTableEntryResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.GetInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf("TODO")
}

func putInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesResponse *PutInodeTableEntriesResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.PutInodeTableEntriesUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf("TODO")
}

func deleteInodeTableEntry(deleteInodeTableEntryRequest *DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryResponse *DeleteInodeTableEntryResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.DeleteInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf("TODO")
}

func adjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountResponse *AdjustInodeTableEntryOpenCountResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.AdjustInodeTableEntryOpenCountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf("TODO")
}

func flush(flushRequest *FlushRequestStruct, flushResponse *FlushResponseStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.FlushUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return fmt.Errorf("TODO")
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

	return fmt.Errorf("TODO")
}
