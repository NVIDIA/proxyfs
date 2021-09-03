// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package imgrpkg implements the server side Inode Management for ProxyFS volumes.
// While the package provides a small set of Go-callable APIs, the bulk of its
// functionality is accessed via package retryrpc-exposed RPCs. While these RPCs
// reference active volumes known to an imgrpkg instance, a RESTful API is provided
// to specify those active volumes.
//
// Note that func's listed under type RetryRPCServerStruct are the RPCs issued by
// the client side via package retryrpc connections (and, thus, not intended to be
// called directly).
//
// To configure an imgrpkg instance, Start() is called passing, as the sole
// argument, a package conf ConfMap. Here is a sample .conf file:
//
//  [IMGR]
//  PublicIPAddr:                         127.0.0.1
//  PrivateIPAddr:                        127.0.0.1
//  RetryRPCPort:                         32356
//  HTTPServerPort:                       15346
//
//  RetryRPCTTLCompleted:                 10m
//  RetryRPCAckTrim:                      100ms
//  RetryRPCDeadlineIO:                   60s
//  RetryRPCKeepAlivePeriod:              60s
//
//  RetryRPCCertFilePath:                              # If both RetryRPC{Cert|Key}FilePath are missing or empty,
//  RetryRPCKeyFilePath:                               #   non-TLS RetryRPC will be selected; otherwise TLS will be used
//
//  CheckPointInterval:                   10s
//
//  AuthTokenCheckInterval:               1m
//
//  FetchNonceRangeToReturn:              100
//
//  MinLeaseDuration:                     250ms
//  LeaseInterruptInterval:               250ms
//  LeaseInterruptLimit:                  20
//  LeaseEvictLowLimit:                   100000
//  LeaseEvictHighLimit:                  100010
//
//  SwiftRetryDelay:                      100ms
//  SwiftRetryExpBackoff:                 2
//  SwiftRetryLimit:                      4
//
//  SwiftTimeout:                         10m
//  SwiftConnectionPoolSize:              128
//
//  ParallelObjectDeleteMax:              10
//
//  InodeTableCacheEvictLowLimit:         10000
//  InodeTableCacheEvictHighLimit:        10010
//
//  InodeTableMaxInodesPerBPlusTreePage:  2048
//  RootDirMaxDirEntriesPerBPlusTreePage: 1024
//
//  LogFilePath:                                       # imgr.log
//  LogToConsole:                         true         # false
//  TraceEnabled:                         false
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
// The RESTful API is provided by an embedded HTTP Server
// (at URL http://<PrivateIPAddr>:<HTTPServerPort>) responsing to the following:
//
//  DELETE /volume/<volumeName>
//
// This will cause the specified <volumeName> to no longer be served. Note that
// this does not actually affect the contents of the associated Container.
//
//  GET /config
//
// This will return a JSON document that matches the conf.ConfMap used to
// launch this package.
//
//  GET /stats
//
// This will return a raw bucketstats dump.
//
//  GET /volume
//
// This will return a JSON document containing an array of volumes currently
// being served with details about each.
//
//  GET /volume/<volumeName>
//
// This will return a JSON document containing only the specified
// <volumeName> details (assuming it is currently being served).
//
//  POST /volume
//  Content-Type: application/json
//
//  {
//     "StorageURL": "http://172.28.128.2:8080/v1/AUTH_test/con",
//     "AuthToken" : "AUTH_tk0123456789abcde0123456789abcdef0"
//  }
//
// This will cause the specified StorageURL to be formatted. The StorageURL
// specified in the JSON document content identifies the Container for format.
// The AuthToken in the JSON document content provides the authentication to
// use during the formatting process.
//
//  PUT /volume/<volumeName>
//  Content-Type: application/json
//
//  {
//     "StorageURL": "http://172.28.128.2:8080/v1/AUTH_test/con"
//  }
//
// This will cause the specified <volumeName> to be served. The StorageURL
// specified in the JSON document content identifies the Container to serve.
// Clients will each supply an AuthToken in their Mount/RenewMount requests
// that will be used to access the Container.
//
//  PUT /volume/<volumeName>
//  Content-Type: application/json
//
//  {
//     "StorageURL": "http://172.28.128.2:8080/v1/AUTH_test/con",
//     "AuthToken" : "AUTH_tk0123456789abcde0123456789abcdef0"
//  }
//
// This will cause the specified <volumeName> to be served. The StorageURL
// specified in the JSON document content identifies the Container to serve.
// Clients will each supply an AuthToken in their Mount/RenewMount requests
// that will be used to access the Container. As a debugging aid, and in the
// case where no Clients have <volumeName> mounted, the AuthToken in the JSON
// document content will be used to access the Container.
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

// E* specifies the prefix of an error string returned by any RetryRPC API
//
const (
	EAuthTokenRejected  = "EAuthTokenRejected:"
	ELeaseRequestDenied = "ELeaseRequestDenied:"
	EMissingLease       = "EMissingLease:"
	EVolumeBeingDeleted = "EVolumeBeingDeleted:"
	EUnknownInodeNumber = "EUnknownInodeNumber:"
	EUnknownMountID     = "EUnknownMountID:"
	EUnknownVolumeName  = "EUnknownVolumeName:"

	ETODO = "ETODO:"
)

type RetryRPCServerStruct struct{}

var retryRPCServer *RetryRPCServerStruct

// MountRequestStruct is the request object for Mount.
//
type MountRequestStruct struct {
	VolumeName string
	AuthToken  string
}

// MountResponseStruct is the response object for Mount.
//
type MountResponseStruct struct {
	MountID string
}

// Mount performs a mount of the specified Volume and returns a MountID to be used
// in all subsequent RPCs to reference this Volume by this Client.
//
// Possible errors: EAuthTokenRejected EVolumeBeingDeleted EUnknownVolumeName
//
func (dummy *RetryRPCServerStruct) Mount(retryRPCClientID uint64, mountRequest *MountRequestStruct, mountResponse *MountResponseStruct) (err error) {
	return mount(retryRPCClientID, mountRequest, mountResponse)
}

// RenewMountRequestStruct is the request object for RenewMount.
//
type RenewMountRequestStruct struct {
	MountID   string
	AuthToken string
}

// RenewMountResponseStruct is the response object for RenewMount.
//
type RenewMountResponseStruct struct{}

// RenewMount updates the AuthToken for the specified MountID.
//
func (dummy *RetryRPCServerStruct) RenewMount(renewMountRequest *RenewMountRequestStruct, renewMountResponse *RenewMountResponseStruct) (err error) {
	return renewMount(renewMountRequest, renewMountResponse)
}

// UnmountRequestStruct is the request object for Unmount.
//
type UnmountRequestStruct struct {
	MountID string
}

// UnmountResponseStruct is the response object for Unmount.
//
type UnmountResponseStruct struct{}

// Unmount requests that the given MountID be released (and implicitly releases
// any Leases held by the MountID).
//
func (dummy *RetryRPCServerStruct) Unmount(unmountRequest *UnmountRequestStruct, unmountResponse *UnmountResponseStruct) (err error) {
	return unmount(unmountRequest, unmountResponse)
}

// FetchNonceRangeRequestStruct is the request object for FetchNonceRange.
//
// Possible errors: EAuthTokenRejected EUnknownMountID
//
type FetchNonceRangeRequestStruct struct {
	MountID string
}

// FetchNonceRangeResponseStruct is the response object for FetchNonceRange.
//
type FetchNonceRangeResponseStruct struct {
	NextNonce        uint64
	NumNoncesFetched uint64
}

// FetchNonceRange requests a range of uint64 nonce values (i.e. values that will
// never be reused).
//
func (dummy *RetryRPCServerStruct) FetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeResponse *FetchNonceRangeResponseStruct) (err error) {
	return fetchNonceRange(fetchNonceRangeRequest, fetchNonceRangeResponse)
}

// GetInodeTableEntryRequestStruct is the request object for GetInodeTableEntry.
//
type GetInodeTableEntryRequestStruct struct {
	MountID     string
	InodeNumber uint64
}

// GetInodeTableEntryResponseStruct is the response object for GetInodeTableEntry.
//
type GetInodeTableEntryResponseStruct struct {
	InodeHeadObjectNumber uint64
	InodeHeadLength       uint64
}

// GetInodeTableEntry requests the Inode information for the specified Inode
// (which must have an active Shared or Exclusive Lease granted to the MountID).
//
func (dummy *RetryRPCServerStruct) GetInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryResponse *GetInodeTableEntryResponseStruct) (err error) {
	return getInodeTableEntry(getInodeTableEntryRequest, getInodeTableEntryResponse)
}

// PutInodeTableEntryStruct is used to indicate the change to an individual
// InodeTableEntry as part of the collection of changes in a PutInodeTablesEntries
// request (which must have an active Exclusive Lease granted to the MountID).
//
type PutInodeTableEntryStruct struct {
	InodeNumber           uint64
	InodeHeadObjectNumber uint64
	InodeHeadLength       uint64
}

// PutInodeTableEntriesRequestStruct is the request object for PutInodeTableEntries
// (which must have an active Exclusive Lease for every PutInodeTableEntryStruct.InodeNumber
// granted to the MountID).
//
// The SuperBlockInode{ObjectCount|ObjectSize|BytesReferenced}Adjustment fields
// are used to update the corresponding fields in the volume's SuperBlock.
//
// Note that dereferenced objects listed in the DereferencedObjectNumberArray will
// not be deleted until the next CheckPoint is performed.
//
type PutInodeTableEntriesRequestStruct struct {
	MountID                                  string
	UpdatedInodeTableEntryArray              []PutInodeTableEntryStruct
	SuperBlockInodeObjectCountAdjustment     int64
	SuperBlockInodeObjectSizeAdjustment      int64
	SuperBlockInodeBytesReferencedAdjustment int64
	DereferencedObjectNumberArray            []uint64
}

// PutInodeTableEntriesResponseStruct is the response object for PutInodeTableEntries.
//
type PutInodeTableEntriesResponseStruct struct{}

// PutInodeTableEntries requests an atomic update of the listed Inodes (which must
// each have an active Exclusive Lease granted to the MountID).
//
func (dummy *RetryRPCServerStruct) PutInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesResponse *PutInodeTableEntriesResponseStruct) (err error) {
	return putInodeTableEntries(putInodeTableEntriesRequest, putInodeTableEntriesResponse)
}

// DeleteInodeTableEntryRequestStruct is the request object for DeleteInodeTableEntry.
//
type DeleteInodeTableEntryRequestStruct struct {
	MountID     string
	InodeNumber uint64
}

// DeleteInodeTableEntryResponseStruct is the response object for DeleteInodeTableEntry.
//
type DeleteInodeTableEntryResponseStruct struct{}

// DeleteInodeTableEntry requests the specified Inode information be deleted.
// An active Exclusive Lease must be granted to the MountID. Note that
// unless/until the OpenCount for the Inode drops to zero, the Inode will
// still exist.
//
func (dummy *RetryRPCServerStruct) DeleteInodeTableEntry(deleteInodeTableEntryRequest *DeleteInodeTableEntryRequestStruct, deleteInodeTableEntryResponse *DeleteInodeTableEntryResponseStruct) (err error) {
	return deleteInodeTableEntry(deleteInodeTableEntryRequest, deleteInodeTableEntryResponse)
}

// AdjustInodeTableEntryOpenCountRequestStruct is the request object for AdjustInodeTableEntryOpenCount.
//
type AdjustInodeTableEntryOpenCountRequestStruct struct {
	MountID     string
	InodeNumber uint64
	Adjustment  int64
}

// AdjustInodeTableEntryOpenCountResponseStruct is the response object for AdjustInodeTableEntryOpenCount.
//
type AdjustInodeTableEntryOpenCountResponseStruct struct {
	CurrentOpenCountThisMount uint64
	CurrentOpenCountAllMounts uint64
}

// AdjustInodeTableEntryOpenCount requests the specified Inode's OpenCount be
// adjusted. A (Shared or Exclusive) Lease must be granted to the MountID. If
// the adjustment results in an OpenCount of zero and the Inode has been marked
// for deletion by a prior call to DeleteInodeTableEntry, the Inode will be
// deleted.
//
func (dummy *RetryRPCServerStruct) AdjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest *AdjustInodeTableEntryOpenCountRequestStruct, adjustInodeTableEntryOpenCountResponse *AdjustInodeTableEntryOpenCountResponseStruct) (err error) {
	return adjustInodeTableEntryOpenCount(adjustInodeTableEntryOpenCountRequest, adjustInodeTableEntryOpenCountResponse)
}

// FlushRequestStruct is the request object for Flush.
//
type FlushRequestStruct struct {
	MountID string
}

// FlushResponseStruct is the response object for Flush.
//
type FlushResponseStruct struct{}

// Flush that the results of prior PutInodeTableEntries requests be persisted.
//
// Possible errors: EUnknownMountID
//
func (dummy *RetryRPCServerStruct) Flush(flushRequest *FlushRequestStruct, flushResponse *FlushResponseStruct) (err error) {
	return flush(flushRequest, flushResponse)
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

// LeaseResponseType specifies the acknowledgement that the requested lease operation
// has been completed or denied (e.g. when a Promotion request cannot be satisfied
// and the client will soon be receiving a LeaseInterruptTypeRelease).
//
type LeaseResponseType uint32

const (
	LeaseResponseTypeDenied    LeaseResponseType = iota // Request denied (e.g. Promotion deadlock avoidance)
	LeaseResponseTypeShared                             // SharedLease granted
	LeaseResponseTypePromoted                           // SharedLease promoted to ExclusiveLease
	LeaseResponseTypeExclusive                          // ExclusiveLease granted
	LeaseResponseTypeDemoted                            // ExclusiveLease demoted to SharedLease
	LeaseResponseTypeReleased                           // SharedLease or ExclusiveLease released
)

// LeaseResponseStruct is the response object for Lease.
//
type LeaseResponseStruct struct {
	LeaseResponseType // One of LeaseResponseType*
}

// Lease is a blocking Lease Request.
//
func (dummy *RetryRPCServerStruct) Lease(leaseRequest *LeaseRequestStruct, leaseResponse *LeaseResponseStruct) (err error) {
	return lease(leaseRequest, leaseResponse)
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
