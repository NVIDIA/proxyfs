// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package imgrpkg implements the server side Inode Management for ProxyFS volumes.
// The package supports to communications protocols. The balance of the package
// documentation will describe the Go-callable API. Note that func's listed under
// type RetryRPCServerStruct are the RPCs issued by the client side via package
// retryrpc connections (and, thus, not intended to be called directly).
//
// To configure an imgrpkg instance, Start() is called passing, as the sole
// argument, a package conf ConfMap. Here is a sample .conf file:
//
//  [IMGR]
//  PublicIPAddr:                  172.28.128.2
//  PrivateIPAddr:                 172.28.128.2
//  RetryRPCPort:                  32356
//  HTTPServerPort:                15346
//
//  RetryRPCTTLCompleted:          10m
//  RetryRPCAckTrim:               100ms
//  RetryRPCDeadlineIO:            60s
//  RetryRPCKeepAlivePeriod:       60s
//
//  RetryRPCCertFilePath:                       # If both RetryRPC{Cert|Key}FilePath are missing or empty,
//  RetryRPCKeyFilePath:                        #   non-TLS RetryRPC will be selected; otherwise TLS will be used
//
//  FetchNonceRangeTpReturn:       100
//
//  MinLeaseDuration:              250ms
//  LeaseInterruptInterval:        250ms
//  LeaseInterruptLimit:           20
//
//  SwiftRetryDelay:               1s
//  SwiftRetryExpBackoff:          1.5
//  SwiftRetryLimit:               11
//
//  SwiftConnectionPoolSize:       128
//
//  InodeTableCacheEvictLowLimit:  10000
//  InodeTableCacheEvictHighLimit: 10010
//
//  LogFilePath:                                # imgr.log
//  LogToConsole:                  true         # false
//  TraceEnabled:                  false
//
// Most of the config keys are required and must have values. One exception
// is LogFilePath that will default to "" and, hence, cause logging to not
// go to a file. This might typically be used when LogToConsole is set to true.
//
// The RetryRPC{Cert|Key}FilePath keys are optional and, if provided may be
// empty. In such cases, the retryrpc package will be configured to use TCP.
// If, however, they are present and provide a path or paths to valid Cert|Key
// files, the retryrpc package will be configured to use TLS. In any event,
// the RPCs will be available via <PublicIPAddr>:<RetryRPCPort>.
//
// In addition to the package retryrpc-exposed RPCs, the package also includes
// an embedded HTTP Server (at URL http://<PrivateIPAddr>:<HTTPServerPort>)
// responses to the following.
//
//  DELETE /volume/<volumeName>
//   - removes the specified <volumeName> from being served
//  GET /config
//   - returns a JSON document of the supplied ConfMap passed to Start()
//  GET /stats
//   - returns a package bucketstats dump
//  GET /volume
//   - returns a JSON document describing details of the specified <volumeName>
//  GET /volume/<volumeName>
//   - returns a JSON document describing details of all volumes
//  PUT /volume/<volumeName>
//   - content should be a StorageURL (including the Container)
//
package imgrpkg

import (
	"github.com/NVIDIA/proxyfs/conf"
)

// Start is called to start serving.
//
func Start(confMap conf.ConfMap) (err error) {
	err = start(confMap)
	return
}

// Stop is called to stop serving.
//
func Stop() (err error) {
	err = stop()
	return
}

// Signal is called to interrupt the server for performing operations such as log rotation.
//
func Signal() (err error) {
	err = signal()
	return
}

// LogWarnf is a wrapper around the internal logWarnf() func called by imgr/main.go::main().
//
func LogWarnf(format string, args ...interface{}) {
	logWarnf(format, args...)
}

// LogInfof is a wrapper around the internal logInfof() func called by imgr/main.go::main().
//
func LogInfof(format string, args ...interface{}) {
	logInfof(format, args...)
}

type RetryRPCServerStruct struct{}

var retryRPCServer *RetryRPCServerStruct

// MountRequestStruct is the request object for Mount.
//
type MountRequestStruct struct {
	VolumeName string
	AuthToken  string
}

// MountReplyStruct is the reply object for Mount.
//
type MountReplyStruct struct {
	MountID string
}

// Mount performs a mount of the specified Volume and returns a MountID to be used
// in all subsequent RPCs to reference this Volume by this Client.
//
func (dummy *RetryRPCServerStruct) Mount(retryRPCClientID uint64, mountRequest *MountRequestStruct, mountReply *MountReplyStruct) (err error) {
	return mount(retryRPCClientID, mountRequest, mountReply)
}

// RenewMountRequestStruct is the request object for RenewMount.
//
type RenewMountRequestStruct struct {
	MountID   string
	AuthToken string
}

// RenewMountReplyStruct is the reply object for RenewMount.
//
type RenewMountReplyStruct struct{}

// RenewMount updates the AuthToken for the specified MountID.
//
func (dummy *RetryRPCServerStruct) RenewMount(renewMountRequest *RenewMountRequestStruct, renewMountReply *RenewMountReplyStruct) (err error) {
	return renewMount(renewMountRequest, renewMountReply)
}

// UnmountRequestStruct is the request object for Unmount.
//
type UnmountRequestStruct struct {
	MountID string
}

// UnmountReplyStruct is the reply object for Unmount.
//
type UnmountReplyStruct struct{}

// Unmount requests that the given MountID be released (and implicitly releases
// any Leases held by the MountID).
//
func (dummy *RetryRPCServerStruct) Unmount(unmountRequest *UnmountRequestStruct, unmountReply *UnmountReplyStruct) (err error) {
	return unmount(unmountRequest, unmountReply)
}

// FetchNonceRangeRequestStruct is the request object for FetchNonceRange.
//
type FetchNonceRangeRequestStruct struct {
	MountID string
}

// FetchNonceRangeReplyStruct is the reply object for FetchNonceRange.
//
type FetchNonceRangeReplyStruct struct {
	NextNonce        uint64
	NumNoncesFetched uint64
}

// FetchNonceRange requests a range of uint64 nonce values (i.e. values that will
// never be reused).
//
func (dummy *RetryRPCServerStruct) FetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeReply *FetchNonceRangeReplyStruct) (err error) {
	return fetchNonceRange(fetchNonceRangeRequest, fetchNonceRangeReply)
}

// GetInodeTableEntryRequestStruct is the request object for GetInodeTableEntry.
//
type GetInodeTableEntryRequestStruct struct {
	MountID     string
	InodeNumber uint64
}

// GetInodeTableEntryReplyStruct is the reply object for GetInodeTableEntry.
//
type GetInodeTableEntryReplyStruct struct {
	// TODO
}

// GetInodeTableEntry requests the Inode information for the specified Inode
// (which must have an active Shared or Exclusive Lease granted to the MountID).
//
func (dummy *RetryRPCServerStruct) GetInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryReply *GetInodeTableEntryReplyStruct) (err error) {
	return getInodeTableEntry(getInodeTableEntryRequest, getInodeTableEntryReply)
}

// PutInodeTableEntriesRequestStruct is the request object for PutInodeTableEntries.
//
type PutInodeTableEntriesRequestStruct struct {
	MountID string
	// TODO
}

// PutInodeTableEntriesReplyStruct is the reply object for PutInodeTableEntries.
//
type PutInodeTableEntriesReplyStruct struct{}

// PutInodeTableEntries requests an atomic update of the listed Inodes (which must
// each have an active Exclusive Lease granted to the MountID).
//
func (dummy *RetryRPCServerStruct) PutInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesReply *PutInodeTableEntriesReplyStruct) (err error) {
	return putInodeTableEntries(putInodeTableEntriesRequest, putInodeTableEntriesReply)
}

// DeleteInodeTableEntryRequestStruct is the request object for DeleteInodeTableEntry.
//
type DeleteInodeTableEntryRequestStruct struct {
	MountID     string
	InodeNumber uint64
}

// DeleteInodeTableEntryReplyStruct is the reply object for DeleteInodeTableEntry.
//
type DeleteInodeTableEntryReplyStruct struct {
	// TODO
}

// DeleteInodeTableEntry requests the specified Inode information be deleted.
// An active Exclusive Lease must be granted to the MountID. Note that
// unless/until the OpenCount for the Inode drops to zero, the Inode will
// still exist.
//
func (dummy *RetryRPCServerStruct) DeleteInodeTableEntry(deleteInodeTableEntryRequest *DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryReply *DeleteInodeTableEntryReplyStruct) (err error) {
	return deleteInodeTableEntry(deleteInodeTableEntryRequest, deleteInodeTableEntryReply)
}

// AdjustInodeTableEntryOpenCountRequestStruct is the request object for AdjustInodeTableEntryOpenCount.
//
type AdjustInodeTableEntryOpenCountRequestStruct struct {
	MountID     string
	InodeNumber uint64
}

// AdjustInodeTableEntryOpenCountReplyStruct is the reply object for AdjustInodeTableEntryOpenCount.
//
type AdjustInodeTableEntryOpenCountReplyStruct struct {
	// TODO
}

// AdjustInodeTableEntryOpenCount requests the specified Inode's OpenCount be
// adjusted. A (Shared or Exclusive) Lease must be granted to the MountID. If
// the adjustment results in an OpenCount of zero and the Inode has been marked
// for deletion by a prior call to DeleteInodeTableEntry, the Inode will be
// deleted.
//
func (dummy *RetryRPCServerStruct) AdjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountReply *AdjustInodeTableEntryOpenCountReplyStruct) (err error) {
	return adjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountReply)
}

// FlushRequestStruct is the request object for Flush.
//
type FlushRequestStruct struct {
	MountID string
	// TODO
}

// FlushReplyStruct is the reply object for Flush.
//
type FlushReplyStruct struct{}

// Flush that the results of prior PutInodeTableEntries requests be persisted.
//
func (dummy *RetryRPCServerStruct) Flush(flushRequest *FlushRequestStruct, flushReply *FlushReplyStruct) (err error) {
	return flush(flushRequest, flushReply)
}

// LeaseRequestType specifies the requested lease operation.
//
type LeaseRequestType uint32

const (
	LeaseRequestTypeShared    LeaseRequestType = iota // Currently unleased, requesting SharedLease
	LeaseRequestTypePromote                           // Currently SharedLease held, requesting promoting to ExclusiveLease
	LeaseRequestTypeExclusive                         // Currently unleased, requesting ExclusiveLease
	LeaseRequestTypeDemote                            // Currently ExclusiveLease held, requesting demotion to SharedLease
	LeaseRequestTypeRelease                           // Currently SharedLease or ExclusiveLease held, releasing it
)

// LeaseRequestStruct is the request object for Lease.
//
type LeaseRequestStruct struct {
	MountID          string
	InodeNumber      uint64
	LeaseRequestType // One of LeaseRequestType*
}

// LeaseReplyType specifies the acknowledgement that the requested lease operation
// has been completed or denied (e.g. when a Promotion request cannot be satisfied
// and the client will soon be receiving a LeaseInterruptTypeRelease).
//
type LeaseReplyType uint32

const (
	LeaseReplyTypeDenied    LeaseReplyType = iota // Request denied (e.g. Promotion deadlock avoidance)
	LeaseReplyTypeShared                          // SharedLease granted
	LeaseReplyTypePromoted                        // SharedLease promoted to ExclusiveLease
	LeaseReplyTypeExclusive                       // ExclusiveLease granted
	LeaseReplyTypeDemoted                         // ExclusiveLease demoted to SharedLease
	LeaseReplyTypeReleased                        // SharedLease or ExclusiveLease released
)

// LeaseReplyStruct is the reply object for Lease.
//
type LeaseReplyStruct struct {
	LeaseReplyType // One of LeaseReplyType*
}

// Lease is a blocking Lease Request.
//
func (dummy *RetryRPCServerStruct) Lease(leaseRequest *LeaseRequestStruct, leaseReply *LeaseReplyStruct) (err error) {
	return lease(leaseRequest, leaseReply)
}

// RPCInterruptType specifies the action (unmount, demotion, or release) requested by ProxyFS
// of the client in an RPCInterrupt "upcall" to indicate that a lease or leases must be demoted
// or released.
//
type RPCInterruptType uint32

const (
	// RPCInterruptTypeUnmount indicates all Leases should be released (after performing necessary
	// state saving RPCs) and the client should unmount.
	//
	RPCInterruptTypeUnmount RPCInterruptType = iota

	// RPCInterruptTypeDemote indicates the specified LeaseHandle should (at least) be demoted
	// from Exclusive to Shared (after performing necessary state saving RPCs).
	//
	RPCInterruptTypeDemote

	// RPCInterruptTypeRelease indicates the specified LeaseHandle should be released (after
	// performing state saving RPCs and invalidating such cached state).
	//
	RPCInterruptTypeRelease
)

// RPCInterrupt is the "upcall" mechanism used by ProxyFS to interrupt the client.
//
type RPCInterrupt struct {
	RPCInterruptType        // One of RPCInterruptType*
	InodeNumber      uint64 // if RPCInterruptType == RPCInterruptTypeUnmount, InodeNumber == 0 (ignored)
}
