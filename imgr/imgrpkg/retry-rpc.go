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

// MountRequestStruct is the request object for RpcMount
//
type MountRequestStruct struct {
	VolumeName string
	AuthToken  string
}

// MountReplyStruct is the reply object for RpcMount
//
type MountReplyStruct struct {
	MountID string
}

// RpcMount performs a mount of the specified Volume and returns a MountID to be used
// in all subsequent RPCs to reference this Volume by this Client
//
func (dummy *retryRPCServerStruct) RpcMount(retryRPCClientID uint64, mountRequest *MountRequestStruct, mountReply *MountReplyStruct) (err error) {
	return nil // TODO
}

// RenewMountRequestStruct is the request object for RpcRenewMount
//
type RenewMountRequestStruct struct {
	MountID   string
	AuthToken string
}

// RenewMountReplyStruct is the reply object for RpcRenewMount
//
type RenewMountReplyStruct struct{}

// RpcRenewMount updates the AuthToken for the specified MountID
//
func (dummy *retryRPCServerStruct) RpcRenewMount(renewMountRequest *RenewMountRequestStruct, renewMountReply *RenewMountReplyStruct) (err error) {
	return nil // TODO
}

// UnmountRequestStruct is the request object for RpcUnmount
//
type UnmountRequestStruct struct {
	MountID string
}

// UnmountReplyStruct is the reply object for RpcUnmount
//
type UnmountReplyStruct struct{}

// RpcUnmount requests that the given MountID be released (and implicitly releases
// any Leases held by the MountID)
//
func (dummy *retryRPCServerStruct) RpcUnmount(unmountRequest *UnmountRequestStruct, unmountReply *UnmountReplyStruct) (err error) {
	return nil // TODO
}

// FetchNonceRangeRequestStruct is the request object for RpcFetchNonceRange
//
type FetchNonceRangeRequestStruct struct {
	MountID string
}

// FetchNonceRangeReplyStruct is the reply object for RpcFetchNonceRange
//
type FetchNonceRangeReplyStruct struct {
	NextNonce        uint64
	NumNoncesFetched uint64
}

// RpcFetchNonceRange requests a range of uint64 nonce values (i.e. values that will
// never be reused)
//
func (dummy *retryRPCServerStruct) RpcFetchNonceRange(fetchNonceRangeRequest *FetchNonceRangeRequestStruct, fetchNonceRangeReply *FetchNonceRangeReplyStruct) (err error) {
	return nil // TODO
}

// GetInodeTableEntryRequestStruct is the request object for RpcGetInodeTableEntry
//
type GetInodeTableEntryRequestStruct struct {
	MountID     string
	InodeNumber uint64
}

// GetInodeTableEntryReplyStruct is the reply object for RpcGetInodeTableEntry
//
type GetInodeTableEntryReplyStruct struct {
	// TODO
}

// RpcGetInodeTableEntry requests the Inode information for the specified Inode
// (which must have an active Shared or Exclusive Lease granted to the MountID)
//
func (dummy *retryRPCServerStruct) RpcGetInodeTableEntry(getInodeTableEntryRequest *GetInodeTableEntryRequestStruct, getInodeTableEntryReply *GetInodeTableEntryReplyStruct) (err error) {
	return nil // TODO
}

// PutInodeTableEntriesRequestStruct is the request object for RpcPutInodeTableEntries
//
type PutInodeTableEntriesRequestStruct struct {
	MountID string
	// TODO
}

// PutInodeTableEntriesReplyStruct is the reply object for RpcPutInodeTableEntries
//
type PutInodeTableEntriesReplyStruct struct{}

// RpcPutInodeTableEntries requests an atomic update of the listed Inodes (which must
// each have an active Exclusive Lease granted to the MountID)
//
func (dummy *retryRPCServerStruct) RpcPutInodeTableEntries(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesReply *PutInodeTableEntriesReplyStruct) (err error) {
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

// LeaseRequest is the request object for RpcLease
//
type LeaseRequest struct {
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

// LeaseReply is the reply object for RpcLease
//
type LeaseReply struct {
	LeaseReplyType // One of LeaseReplyType*
}

// RpcLease is a blocking Lease Request
func (dummy *retryRPCServerStruct) RpcLease(putInodeTableEntriesRequest *PutInodeTableEntriesRequestStruct, putInodeTableEntriesReply *PutInodeTableEntriesReplyStruct) (err error) {
	return nil // TODO
}

// RPCInterruptType specifies the action (unmount, demotion, or release) requested by ProxyFS
// of the client in an RpcInterrupt "upcall" to indicate that a lease or leases must be demoted
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
