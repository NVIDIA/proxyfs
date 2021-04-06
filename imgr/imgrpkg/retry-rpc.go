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

func mount(retryRPCClientID uint64, mountRequest *MountRequestStruct, mountReply *MountReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.MountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func renewMount(renewMountRequest *RenewMountRequestStruct, renewMountReply *RenewMountReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.RenewMountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func unmount(unmountRequest *UnmountRequestStruct, unmountReply *UnmountReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.UnmountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func fetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeReply *FetchNonceRangeReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.FetchNonceRangeUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func getInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryReply *GetInodeTableEntryReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.GetInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func putInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesReply *PutInodeTableEntriesReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.PutInodeTableEntriesUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func deleteInodeTableEntry(deleteInodeTableEntryRequest *DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryReply *DeleteInodeTableEntryReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.DeleteInodeTableEntryUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func adjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountReply *AdjustInodeTableEntryOpenCountReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.AdjustInodeTableEntryOpenCountUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func flush(flushRequest *FlushRequestStruct, flushReply *FlushReplyStruct) (err error) {
	var (
		startTime time.Time
	)

	startTime = time.Now()

	defer func() {
		globals.stats.FlushUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	return nil // TODO
}

func lease(leaseRequest *LeaseRequestStruct, leaseReply *LeaseReplyStruct) (err error) {
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
		leaseReply.LeaseReplyType = LeaseReplyTypeDenied
		err = fmt.Errorf("LeaseRequestType %v not supported", leaseRequest.LeaseRequestType)
		err = blunder.AddError(err, blunder.BadLeaseRequest)
		return
	}

	return nil // TODO
}
