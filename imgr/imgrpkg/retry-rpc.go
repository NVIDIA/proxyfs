// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"crypto/tls"

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
	return nil // TODO
}

func renewMount(renewMountRequest *RenewMountRequestStruct, renewMountReply *RenewMountReplyStruct) (err error) {
	return nil // TODO
}

func unmount(unmountRequest *UnmountRequestStruct, unmountReply *UnmountReplyStruct) (err error) {
	return nil // TODO
}

func fetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeReply *FetchNonceRangeReplyStruct) (err error) {
	return nil // TODO
}

func getInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryReply *GetInodeTableEntryReplyStruct) (err error) {
	return nil // TODO
}

func putInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesReply *PutInodeTableEntriesReplyStruct) (err error) {
	return nil // TODO
}

func deleteInodeTableEntry(deleteInodeTableEntryRequest *DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryReply *DeleteInodeTableEntryReplyStruct) (err error) {
	return nil // TODO
}

func adjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountReply *AdjustInodeTableEntryOpenCountReplyStruct) (err error) {
	return nil // TODO
}

func flush(flushRequest *FlushRequestStruct, flushReply *FlushReplyStruct) (err error) {
	return nil // TODO
}

func lease(leaseRequest *LeaseRequestStruct, leaseReply *LeaseReplyStruct) (err error) {
	return nil // TODO
}
