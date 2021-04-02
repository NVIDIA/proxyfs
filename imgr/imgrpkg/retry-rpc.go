// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

type retryRPCServerStruct struct{}

var retryRPCServer *retryRPCServerStruct

func startRetryRPCServer() (err error) {
	retryRPCServer = &retryRPCServerStruct{}

	return nil // TODO
}

func stopRetryRPCServer() (err error) {
	retryRPCServer = nil

	return nil // TODO
}

// MountRequestStruct is the request object for Mount
//
type MountRequestStruct struct {
	VolumeName string
	AuthToken  string
}

// MountReplyStruct is the reply object for Mount
//
type MountReplyStruct struct {
	MountID string
}

// Mount performs a mount of the specified Volume and returns a MountID to be used
// in all subsequent RPCs to reference this Volume by this Client
//
func (dummy *retryRPCServerStruct) Mount(retryRPCClientID uint64, mountRequest *MountRequestStruct, mountReply *MountReplyStruct) (err error) {
	return nil // TODO
}

// RenewMountRequestStruct is the request object for RenewMount
//
type RenewMountRequestStruct struct {
	MountID   string
	AuthToken string
}

// RenewMountReplyStruct is the reply object for RenewMount
//
type RenewMountReplyStruct struct{}

// RenewMount updates the AuthToken for the specified MountID
//
func (dummy *retryRPCServerStruct) RenewMount(renewMountRequest *RenewMountRequestStruct, renewMountReply *RenewMountReplyStruct) (err error) {
	return nil // TODO
}

// UnmountRequestStruct is the request object for Unmount
//
type UnmountRequestStruct struct {
	MountID string
}

// UnmountReplyStruct is the reply object for Unmount
//
type UnmountReplyStruct struct{}

// Unmount requests that the given MountID be released (and implicitly releases
// any Leases held by the MountID)
//
func (dummy *retryRPCServerStruct) Unmount(unmountRequest *UnmountRequestStruct, unmountReply *UnmountReplyStruct) (err error) {
	return nil // TODO
}

// FetchNonceRangeRequestStruct is the request object for FetchNonceRange
//
type FetchNonceRangeRequestStruct struct {
	MountID string
}

// FetchNonceRangeReplyStruct is the reply object for FetchNonceRange
//
type FetchNonceRangeReplyStruct struct {
	NextNonce        uint64
	NumNoncesFetched uint64
}

// FetchNonceRange requests a range of uint64 nonce values (i.e. values that will
// never be reused)
//
func (dummy *retryRPCServerStruct) FetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeReply *FetchNonceRangeReplyStruct) (err error) {
	return nil // TODO
}

// GetInodeTableEntryRequestStruct is the request object for GetInodeTableEntry
//
type GetInodeTableEntryRequestStruct struct {
	MountID     string
	InodeNumber uint64
}

// GetInodeTableEntryReplyStruct is the reply object for GetInodeTableEntry
//
type GetInodeTableEntryReplyStruct struct {
	// TODO
}

// GetInodeTableEntry requests the Inode information for the specified Inode
// (which must have an active Shared or Exclusive Lease granted to the MountID)
//
func (dummy *retryRPCServerStruct) GetInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryReply *GetInodeTableEntryReplyStruct) (err error) {
	return nil // TODO
}

// PutInodeTableEntriesRequestStruct is the request object for PutInodeTableEntries
//
type PutInodeTableEntriesRequestStruct struct {
	MountID string
	// TODO
}

// PutInodeTableEntriesReplyStruct is the reply object for PutInodeTableEntries
//
type PutInodeTableEntriesReplyStruct struct{}

// PutInodeTableEntries requests an atomic update of the listed Inodes (which must
// each have an active Exclusive Lease granted to the MountID)
//
func (dummy *retryRPCServerStruct) PutInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesReply *PutInodeTableEntriesReplyStruct) (err error) {
	return nil // TODO
}

// FlushRequestStruct is the request object for Flush
//
type FlushRequestStruct struct {
	MountID string
	// TODO
}

// FlushReplyStruct is the reply object for Flush
//
type FlushReplyStruct struct{}

// Flush that the results of prior PutInodeTableEntries requests be persisted
//
func (dummy *retryRPCServerStruct) Flush(flushRequest *FlushRequestStruct, flushReply *FlushReplyStruct) (err error) {
	return nil // TODO
}

// LeaseRequestType specifies the requested lease operation
//
type LeaseRequestType uint32

const (
	LeaseRequestTypeShared    LeaseRequestType = iota // Currently unleased, requesting SharedLease
	LeaseRequestTypePromote                           // Currently SharedLease held, requesting promoting to ExclusiveLease
	LeaseRequestTypeExclusive                         // Currently unleased, requesting ExclusiveLease
	LeaseRequestTypeDemote                            // Currently ExclusiveLease held, requesting demotion to SharedLease
	LeaseRequestTypeRelease                           // Currently SharedLease or ExclusiveLease held, releasing it
)

// LeaseRequestStruct is the request object for Lease
//
type LeaseRequestStruct struct {
	MountID          string
	InodeNumber      uint64
	LeaseRequestType // One of LeaseRequestType*
}

// LeaseReplyType specifies the acknowledgement that the requested lease operation
// has been completed or denied (e.g. when a Promotion request cannot be satisfied
// and the client will soon be receiving a LeaseInterruptTypeRelease)
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

// LeaseReplyStruct is the reply object for Lease
//
type LeaseReplyStruct struct {
	LeaseReplyType // One of LeaseReplyType*
}

// Lease is a blocking Lease Request
func (dummy *retryRPCServerStruct) Lease(leaseRequest *LeaseRequestStruct, leaseReply *LeaseReplyStruct) (err error) {
	return nil // TODO
}

// RPCInterruptType specifies the action (unmount, demotion, or release) requested by ProxyFS
// of the client in an RPCInterrupt "upcall" to indicate that a lease or leases must be demoted
// or released
//
type RPCInterruptType uint32

const (
	// RPCInterruptTypeUnmount indicates all Leases should be released (after performing necessary
	// state saving RPCs) and the client should unmount
	//
	RPCInterruptTypeUnmount RPCInterruptType = iota

	// RPCInterruptTypeDemote indicates the specified LeaseHandle should (at least) be demoted
	// from Exclusive to Shared (after performing necessary state saving RPCs)
	//
	RPCInterruptTypeDemote

	// RPCInterruptTypeRelease indicates the specified LeaseHandle should be released (after
	// performing state saving RPCs and invalidating such cached state)
	//
	RPCInterruptTypeRelease
)

// RPCInterrupt is the "upcall" mechanism used by ProxyFS to interrupt the client
//
type RPCInterrupt struct {
	RPCInterruptType        // One of RPCInterruptType*
	InodeNumber      uint64 // if RPCInterruptType == RPCInterruptTypeUnmount, InodeNumber == 0 (ignored)
}
