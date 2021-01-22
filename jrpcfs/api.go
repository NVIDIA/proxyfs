// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Package jrpcfs implements a JSON RPC interface to package fs.
//
// The structs in this file are used as parameters to the Server methods
// found in filesystem.go and middleware.go.
//
// NOTE: Please try to keep the definitions in this file in alphabetical order.
//
//
// Using the JSON RPC APIs from outside of proxyfsd:
//
//  - The types defined here and in filesystem.go are not available outside of proxyfs,
//    so one must use the JSON RPC, which is defined here:
//    https://en.wikipedia.org/wiki/JSON-RPC
//
//  - JSON RPC expects a request with the following format:
//        "method":  <string that identifies the RPC method>
//        "params":  [<args for the particular method>]
//        "jsonrpc": "2.0"
//        "id":      <my RPC request id; client-defined>
//
//    NOTE: the "id" here is just used to match request/response.
//
//  - A JSON RPC response will look like this:
//        "id":     <id of request>
//        "error":  <error string or nil if no error>
//        "result": <encoded result data>
//
//
// EXAMPLE:
//
// As an example, let's look at doing a mount.
//
// The Go-side definitions of interest are:
//
// From this file:
//   type MountRequestByVolumeName struct {
//       VolumeName   string
//       MountOptions uint64
//   }
//
//   type MountReply struct {
//       MountID            MountIDAsString
//       RootDirInodeNumber uint64
//   }
//
// From filesystem.go:
//   func (s *Server) RpcMount(in *MountRequest, reply *MountReply) error
//
//
//  It may be easiest to represent what needs to be sent to the proxyfs RPC
//  server in Python:
//
//    # The args for RpcMount are defined in MountRequest. The arg names used
//    # must be exactly the same as the names in the Go-side struct.
//    #
//    # For the expected type for each argument, see the definition of the
//    # appropriate request/response struct in this file.
//    #
//    args = {'VolumeName' : "CommonVolume", 'MountOptions': 0}
//
//    # Start our client-side request numbering at 0
//    #
//    id = 0
//
//    # This will become the JSON request once we encode it
//    #
//    payload = {
//        "method": "Server.RpcMountByVolumeName", # This will always be "Server."<method name from filesystem.go>
//        "params": [args],                        # Args must be encoded in an array here!
//        "jsonrpc": "2.0",                        # JSON RPC version
//        "id": id,                                # Client request id
//    }
//
//    # Encode payload into JSON
//    #
//    data = json.dumps((payload))
//
//    # Send request over socket. Ignore socket specifics here.
//    #
//    s = socket.create_connection(("localhost", 12345))
//    s.sendall(data)
//
//  Now we receive and decode the response from the proxyfsd RPC server.
//
//
//    # Read response from socket. Ignore socket specifics here.
//    #
//    # (Note that the size of 1024 here will not work for all calls; this
//    # is just a simple example).
//    #
//    rdata = s.recv(1024)
//
//    # Decode response out of JSON
//    #
//    resp = json.loads(rdata)
//
//    # Check that id from response is the same as our request
//    #
//    if resp["id"] != id:
//        raise Exception("expected id=%s, received id=%s: %s" %(id, resp["id"], resp["error"]))
//
//    # Check whether there was an error in handling the request
//    #
//    if resp["error"] is not None:
//        raise Exception(resp["error"])
//
//    # If we got this far, we can check out the response. The response
//    # contents will consist of the content of the response struct for
//    # this particular request. In this case, that is the contents of
//    # the MountReply structure.
//    #
//    # Note that it's generally good practice to check for the presence
//    # of the key before using the value...
//    #
//    print "Returned MountID:  ", resp["result"]["MountID"]
//    print "Returned rootInode:", resp["result"]["RootDirInodeNumber"]
//
//  On the C-side, jrpcclient leverages the popular json-c library. One glaring ommission of json-c is
//  support for unsigned integers... specifically uint64_t's. The parser actually substitutes for any
//  "number" that is bigger than math.MaxInt64 with math.MaxInt64 - rather than converting to the
//  bit-compatible uint64_t interpretation. It's a mystery why... but this choice has spurred several
//  to request json-c expand to directly support (particularly) uint64_t. Efforts have been started but,
//  alas, never completed.
//
//  The ProxyFS work-around will be to pass vulnerable uint64's that have practical cases where the
//  upper-most bit (the "sign" bit if it was an int64) as int64's. The int64 value will be the equivalent
//  such that casting between int64_t's and uint64_t's will result in the desired value. It just so
//  happens that jrpcclient is already doing the casting back and forth, so making that possible on the
//  (here) Go side resolves the issue.
//
//  The impact on this change for the other JSON RPC client, pfs_middleware, should not be noticable so
//  long as the cases where a uint64 comes across as a negative int64 are opaque to pfs_middleware.
//
//  It turns out that all uint64's previously in the jrpcfs-specified RPC (i.e. those in api.go) fall
//  into three categories:
//
//    Practically never > math.MaxInt64 - e.g. Stat.Size
//    Possibly          > math.MaxInt64 - specifically SnapShotIDs adorning InodeNumbers
//
//  In the "Possibly" category, the InodeNumbers are the worry here. Fortunately, InodeNumbers are
//  considered "opaque" handles to ProxyFS resources and, as such, only need to preserve this identity
//  property (whether signed or unsigned). For this reason, all InodeNumbers in the following API are
//  passed as int64's rather than uint64's. In the case where the InodeNumber > math.MaxInt64, care
//  is taken such that the negative value passed via the int64 is cast to the proper (large) uint64
//  on each side of the RPC consistently.
//
package jrpcfs

import (
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
)

// The first section of this file defines the structs used by protocols like Samba.
// The API for this section is implemented in filesystem.go.
//
// Structs used by Swift middleware are defined below.

// MountID is embedded in a number of request objects (as well as InodeHandle & PathHandle)
//
// For Reads/Writes, the binary form of MountID will be used
// For Non-Reads/Writes, a base64.StdEncoding.Encode() of the binary form of MountID will be used
//
type MountIDAsByteArray [16]byte
type MountIDAsString string

// InodeHandle is embedded in a number of the request objects.
type InodeHandle struct {
	MountID     MountIDAsString
	InodeNumber int64
}

// PathHandle is embedded in a number of the request objects.
type PathHandle struct {
	MountID  MountIDAsString
	Fullpath string
}

// ChmodRequest is the request object for RpcChmod.
type ChmodRequest struct {
	InodeHandle
	FileMode uint32
}

// ChmodPathRequest is the request object for RpcChmodPath.
type ChmodPathRequest struct {
	PathHandle
	FileMode uint32
}

// ChownRequest is the request object for RpcChown.
type ChownRequest struct {
	InodeHandle
	UserID  int32
	GroupID int32
}

// ChownPathRequest is the request object for RpcChownPath.
type ChownPathRequest struct {
	PathHandle
	UserID  int32
	GroupID int32
}

// CreateRequest is the request object for RpcCreate.
type CreateRequest struct {
	InodeHandle
	Basename string
	UserID   int32
	GroupID  int32
	FileMode uint32
}

// CreatePathRequest is the request object for RpcCreatePath.
type CreatePathRequest struct {
	PathHandle
	UserID   int32
	GroupID  int32
	FileMode uint32
}

// DirEntry is used as part of ReaddirReply and ReaddirPlusReply.
//
// FileType here will be a uint16 containing DT_DIR|DT_REG|DT_LNK.
//
type DirEntry struct {
	InodeNumber     int64
	FileType        uint16
	Basename        string
	NextDirLocation int64
}

// FetchExtentMapChunkRequest is the request object for RpcFetchExtentMapChunk.
type FetchExtentMapChunkRequest struct {
	InodeHandle
	FileOffset                 uint64
	MaxEntriesFromFileOffset   int64
	MaxEntriesBeforeFileOffset int64
}

// FetchExtentMapChunkReply is the response object for RpcFetchExtentMapChunk.
type FetchExtentMapChunkReply struct {
	FileOffsetRangeStart uint64                       // Holes in [FileOffsetRangeStart:FileOffsetRangeEnd)
	FileOffsetRangeEnd   uint64                       //   not covered in ExtentMapEntry slice should "read-as-zero"
	FileSize             uint64                       //   up to the end-of-file as indicated by FileSize
	ExtentMapEntry       []inode.ExtentMapEntryStruct // All will be in [FileOffsetRangeStart:FileOffsetRangeEnd)
}

// FlushRequest is the request object for RpcFlush.
type FlushRequest struct {
	InodeHandle
	SendTimeSec  int64
	SendTimeNsec int64
}

// GetStatRequest is the request object for RpcGetStat.
type GetStatRequest struct {
	InodeHandle
}

// GetStatPathRequest is the request object for RpcGetStatPath.
type GetStatPathRequest struct {
	PathHandle
}

type GetXAttrRequest struct {
	InodeHandle
	AttrName string
}

type GetXAttrPathRequest struct {
	PathHandle
	AttrName string
}

type GetXAttrReply struct {
	AttrValueSize uint64
	AttrValue     []byte
}

type FlockRequest struct {
	InodeHandle
	FlockCmd    int32
	FlockType   int32
	FlockWhence int32
	FlockStart  uint64
	FlockLen    uint64
	FlockPid    uint64
}

type FlockReply struct {
	FlockType   int32
	FlockWhence int32
	FlockStart  uint64
	FlockLen    uint64
	FlockPid    uint64
}

// InodeReply is the reply object for requests that return an inode number.
// This response object is used by a number of the methods.
type InodeReply struct {
	InodeNumber int64
}

// LogRequest is the request object for RpcLog.
type LogRequest struct {
	Message string
}

// LookupPathRequest is the request object for RpcLookupPath.
type LookupPathRequest struct {
	MountID  MountIDAsString
	Fullpath string
}

// LinkRequest is the request object for RpcLink.
type LinkRequest struct {
	InodeHandle
	Basename          string
	TargetInodeNumber int64
}

// LinkPathRequest is the request object for RpcLinkPath.
type LinkPathRequest struct {
	PathHandle
	TargetFullpath string
}

type ListXAttrRequest struct {
	InodeHandle
}

type ListXAttrPathRequest struct {
	PathHandle
}

type ListXAttrReply struct {
	AttrNames []string
}

// LookupRequest is the request object for RpcLookup.
type LookupRequest struct {
	InodeHandle
	Basename string
}

// LookupPlusRequest is the request object for RpcLookupPlus.
type LookupPlusRequest struct {
	InodeHandle
	Basename string
}

// LookupPlusReply is the reply object for RpcLookupPlus.
type LookupPlusReply struct {
	InodeNumber int64
	StatStruct
}

// AccessRequest is the request object for RpcAccess.
type AccessRequest struct {
	InodeHandle
	UserID     int32
	GroupID    int32
	AccessMode uint32
}

// MkdirRequest is the request object for RpcMkdir.
type MkdirRequest struct {
	InodeHandle
	Basename string
	UserID   int32
	GroupID  int32
	FileMode uint32
}

// MkdirPathRequest is the request object for RpcMkdirPath.
type MkdirPathRequest struct {
	PathHandle
	UserID   int32
	GroupID  int32
	FileMode uint32
}

// MountByAccountNameRequest is the request object for RpcMountByAccountName.
type MountByAccountNameRequest struct {
	AccountName  string
	MountOptions uint64
	AuthUserID   uint64
	AuthGroupID  uint64
}

// MountByAccountNameReply is the reply object for RpcMountByAccountName.
type MountByAccountNameReply struct {
	MountID                  MountIDAsString
	RootDirInodeNumber       int64
	RetryRPCPublicIPAddr     string
	RetryRPCPort             uint16
	RootCAx509CertificatePEM []byte
}

// MountByVolumeNameRequest is the request object for RpcMountByVolumeName.
type MountByVolumeNameRequest struct {
	VolumeName   string
	MountOptions uint64
	AuthUserID   uint64
	AuthGroupID  uint64
}

// MountByVolumeNameReply is the reply object for RpcMountByVolumeName.
type MountByVolumeNameReply struct {
	MountID                  MountIDAsString
	RootDirInodeNumber       int64
	RetryRPCPublicIPAddr     string
	RetryRPCPort             uint16
	RootCAx509CertificatePEM []byte
}

// UnmountRequest is the request object for RpcUnmount.
//
// Note that all leases are implicitly released as part of servicing this request.
type UnmountRequest struct {
	MountID MountIDAsString
}

// ReaddirRequest is the request object for RpcReaddir.
type ReaddirRequest struct {
	InodeHandle
	MaxEntries     uint64
	PrevDirEntName string
}

// ReaddirByLocRequest is the request object for RpcReaddirByLoc.
type ReaddirByLocRequest struct {
	InodeHandle
	MaxEntries         uint64
	PrevDirEntLocation int64
}

// ReaddirReply is the reply object for RpcReaddir and RpcReaddirByLoc.
type ReaddirReply struct {
	DirEnts []DirEntry
}

// ReaddirPlusRequest is the request object for RpcReaddirPlus.
type ReaddirPlusRequest struct {
	InodeHandle
	MaxEntries     uint64
	PrevDirEntName string
}

// ReaddirPlusByLocRequest is the request object for RpcReaddirPlusByLoc.
type ReaddirPlusByLocRequest struct {
	InodeHandle
	MaxEntries         uint64
	PrevDirEntLocation int64
}

// ReaddirPlusReply is the reply object for RpcReaddirPlus and RpcReaddirPlusByLoc.
type ReaddirPlusReply struct {
	DirEnts  []DirEntry
	StatEnts []StatStruct
}

// ReadSymlinkRequest is the request object for RpcReadSymlink.
type ReadSymlinkRequest struct {
	InodeHandle
}

// ReadSymlinkPathRequest is the request object for RpcReadSymlinkPath.
type ReadSymlinkPathRequest struct {
	PathHandle
}

// ReadSymlinkReply is the reply object for RpcReadSymlink and RpcReadSymlinkPath.
type ReadSymlinkReply struct {
	Target string
}

type RemoveXAttrRequest struct {
	InodeHandle
	AttrName string
}

type RemoveXAttrPathRequest struct {
	PathHandle
	AttrName string
}

// RenameRequest is the request object for RpcRename.
type RenameRequest struct {
	MountID           MountIDAsString
	SrcDirInodeNumber int64
	SrcBasename       string
	DstDirInodeNumber int64
	DstBasename       string
}

// RenamePathRequest is the request object for RpcRenamePath.
type RenamePathRequest struct {
	PathHandle
	DstFullpath string
}

// MoveRequest is the request object for RpcMove.
//
// Note the similarity with RenameRequest except that implicit Destroy of an Inode
// when a replacement DirEntry reduces the prior DirEntry's Inode LinkCount to zero
// is not performed... instead leaving it up to the client to do so.
//
type MoveRequest struct {
	MountID           MountIDAsString
	SrcDirInodeNumber int64
	SrcBasename       string
	DstDirInodeNumber int64
	DstBasename       string
}

// MoveReply is the reply object for RpcMove.
type MoveReply struct {
	ToDestroyInodeNumber int64
}

// DestroyRequest is teh request object for RpcDestroy.
type DestroyRequest struct {
	InodeHandle
}

// Reply is a generic response object used when no values need to be returned.
// This response object is used by a number of the methods.
type Reply struct {
	RequestTimeSec  int64
	RequestTimeNsec int64
	SendTimeSec     int64
	SendTimeNsec    int64
}

// ResizeRequest is the request object for RpcResize.
type ResizeRequest struct {
	InodeHandle
	NewSize uint64
}

// SetstatRequest is the request object for RpcSetstat.
type SetstatRequest struct {
	InodeHandle
	StatStruct
}

// SetTimeRequest is the request object for RpcSetTime.
type SetTimeRequest struct {
	InodeHandle
	StatStruct
}

// SetTimePathRequest is the request object for RpcSetTimePath.
type SetTimePathRequest struct {
	PathHandle
	StatStruct
}

type SetXAttrRequest struct {
	InodeHandle
	AttrName  string
	AttrValue []byte
	AttrFlags int
}

type SetXAttrPathRequest struct {
	PathHandle
	AttrName  string
	AttrValue []byte
	AttrFlags int
}

// StatVFSRequest is the request object for RpcStatVFS.
type StatVFSRequest struct {
	MountID MountIDAsString
}

// StatVFS is used when filesystem stats need to be conveyed. It is used by RpcStatVFS.
type StatVFS struct {
	BlockSize      uint64
	FragmentSize   uint64
	TotalBlocks    uint64
	FreeBlocks     uint64
	AvailBlocks    uint64
	TotalInodes    uint64
	FreeInodes     uint64
	AvailInodes    uint64
	FileSystemID   uint64
	MountFlags     uint64
	MaxFilenameLen uint64
}

// StatStruct is used when stats need to be conveyed. It is used as the response to RpcGetStat and RpcGetStatPath,
// as well as in RpcSetStat and RpcReaddirPlus.
//
// Note that times are conveyed as nanoseconds since epoch.
//
type StatStruct struct {
	CTimeNs         uint64
	CRTimeNs        uint64
	MTimeNs         uint64
	ATimeNs         uint64
	Size            uint64
	NumLinks        uint64
	StatInodeNumber int64
	FileMode        uint32
	UserID          uint32
	GroupID         uint32
}

// SymlinkRequest is the request object for RpcSymlink.
type SymlinkRequest struct {
	InodeHandle
	Basename string
	Target   string
	UserID   int32
	GroupID  int32
}

// SymlinkPathRequest is the request object for RpcSymlinkPath.
type SymlinkPathRequest struct {
	PathHandle
	TargetFullpath string
	UserID         int32
	GroupID        int32
}

// TypeRequest is the request object for RpcType.
type TypeRequest struct {
	InodeHandle
}

// TypeReply is the reply object for RpcType.
//
// FileType here will be a uint16 containing DT_DIR|DT_REG|DT_LNK.
//
type TypeReply struct {
	FileType uint16
}

// UnlinkRequest is the request object for RpcUnlink & RpcRmdir.
type UnlinkRequest struct {
	InodeHandle
	Basename string
}

// UnlinkPathRequest is the request object for RpcUnlinkPath & RpcRmdirPath.
type UnlinkPathRequest struct {
	PathHandle
}

// This section of the file contains RPC data structures for Swift middleware bimodal support.
//
// The API for this section is implemented in middleware.go.
//
// TODO - Prefix all of the structs with "Middleware" to distinguish from other RPCs.

// CreateContainerRequest is the request object for RpcCreateContainer.
type CreateContainerRequest struct {
	VirtPath string
}

// CreateContainerReply is the reply object for RpcCreateContainer.
type CreateContainerReply struct {
}

// DeleteReply is the response object for RpcDelete
type DeleteReply struct {
}

// DeleteReq is the request object for RpcDelete
type DeleteReq struct {
	VirtPath string
}

type HeadReply struct {
	FileSize         uint64
	IsDir            bool
	ModificationTime uint64 // nanoseconds since epoch
	AttrChangeTime   uint64 // nanoseconds since epoch
	InodeNumber      int64
	NumWrites        uint64
	Metadata         []byte // entity metadata, serialized
}

type HeadReq struct {
	VirtPath string // virtual entity path, e.g. /v1/AUTH_acc/some-dir[/some-file]
}

// GetContainerReply is the response object for RpcGetContainer
type GetContainerReply struct {
	ContainerEntries []fs.ContainerEntry
	ModificationTime uint64
	AttrChangeTime   uint64
	Metadata         []byte // container metadata, serialized
}

// GetContainerReq is the request object for RpcGetContainer
type GetContainerReq struct {
	VirtPath   string // virtual container path, e.g. /v1/AUTH_acc/some-dir
	Marker     string // marker from query string, used in pagination
	EndMarker  string // endmarker from query string, used in pagination
	Prefix     string // only look at entries starting with this
	MaxEntries uint64 // maximum number of entries to return
	Delimiter  string // only match up to the first occurrence of delimiter (excluding prefix)
}

// Response object for RpcGetAccount
type GetAccountReply struct {
	AccountEntries   []fs.AccountEntry
	ModificationTime uint64
	AttrChangeTime   uint64
}

// Request object for RpcGetAccount
type GetAccountReq struct {
	VirtPath   string // account path, e.g. /v1/AUTH_acc
	Marker     string // marker from query string, used in pagination
	EndMarker  string // endmarker from query string, used in pagination
	MaxEntries uint64 // maximum number of entries to return
}

// GetObjectReply is the response object for RpcGetObject
type GetObjectReply struct {
	FileSize         uint64               // size of the file, in bytes
	IsDir            bool                 // true if directory
	ReadEntsOut      []inode.ReadPlanStep // object/length/offset triples where the data is found
	InodeNumber      uint64
	NumWrites        uint64
	Metadata         []byte // serialized object metadata (previously set by middleware empty if absent)
	ModificationTime uint64 // file's mtime in nanoseconds since the epoch
	AttrChangeTime   uint64
	LeaseId          string
}

// GetObjectReq is the request object for RpcGetObject
type GetObjectReq struct {
	// Virtual path to be read. Refers to an object, e.g.
	// /v1/AUTH_acc/a-container/an-object
	VirtPath string

	// Ranges to be read from virtual path. Note: these are
	// offset/length pairs, not HTTP byte ranges; please remember
	// to convert the values. To obtain a read plan for the entire
	// object, leave ReadEntsIn empty.
	ReadEntsIn []fs.ReadRangeIn
}

// MiddlewarePostReply is the reply object for RpcPost
type MiddlewarePostReply struct {
}

// MiddlewarePostReq is the request object for RpcPost
type MiddlewarePostReq struct {

	// Virtual path to be read.  This could be account, account/container or account/container/object
	VirtPath string

	// New or updated HTTP metadata to be stored
	NewMetaData []byte

	// Last MetaData known by caller - used to resolve races between clients by doing read/modify/write
	OldMetaData []byte
}

type MiddlewareMkdirReply struct {
	ModificationTime uint64
	AttrChangeTime   uint64
	InodeNumber      int64
	NumWrites        uint64
}

type MiddlewareMkdirReq struct {

	// Virtual path of the directory to be created
	VirtPath string

	// HTTP metadata to be stored
	Metadata []byte
}

// PutCompleteReq is the request object for RpcPutComplete
type PutCompleteReq struct {
	VirtPath    string
	PhysPaths   []string
	PhysLengths []uint64
	Metadata    []byte
}

// PutCompleteReply is the response object for RpcPutComplete
type PutCompleteReply struct {
	ModificationTime uint64
	AttrChangeTime   uint64
	InodeNumber      int64
	NumWrites        uint64
}

// PutLocationReq is the request object for RpcPutLocation
type PutLocationReq struct {
	VirtPath string
}

// PutLocationReply is the response object for RpcPutLocation
type PutLocationReply struct {
	PhysPath string
}

// PingReq is the request object for RpcPing
type PingReq struct {
	Message string
}

// PingReply is the response object for RpcPutLocation
type PingReply struct {
	Message string
}

// IsAccountBimodalReq is the request object for RpcIsAccountBimodal
type IsAccountBimodalReq struct {
	AccountName string
}

// IsAccountBimodalReply is the response object for RpcPutLocation
type IsAccountBimodalReply struct {
	IsBimodal               bool
	ActivePeerPrivateIPAddr string
}

// Types for RpcPutContainer
type PutContainerReq struct {
	VirtPath    string
	NewMetadata []byte
	OldMetadata []byte
}

type PutContainerReply struct {
}

type CoalesceReq struct {
	VirtPath                    string
	ElementAccountRelativePaths []string
	// New or updated HTTP metadata to be stored
	NewMetaData []byte
}

type CoalesceReply struct {
	ModificationTime uint64
	AttrChangeTime   uint64
	InodeNumber      int64
	NumWrites        uint64
}

type ProvisionObjectRequest struct {
	MountID MountIDAsString
}

type ProvisionObjectReply struct {
	PhysPath string
}

type WroteRequest struct {
	InodeHandle
	ContainerName string
	ObjectName    string
	FileOffset    []uint64
	ObjectOffset  []uint64
	Length        []uint64
	WroteTimeNs   uint64 // New value for File's Stat.CTimeNs & Stat.MTimeNs
}

type WroteReply struct {
}

type RenewLeaseReq struct {
	LeaseId string
}
type RenewLeaseReply struct{}

type ReleaseLeaseReq struct {
	LeaseId string
}
type ReleaseLeaseReply struct{}

// SnapShotCreateRequest is the request object for RpcSnapShotCreate
type SnapShotCreateRequest struct {
	MountID MountIDAsString
	Name    string
}

// SnapShotCreateReply is the reply object for RpcSnapShotCreate
type SnapShotCreateReply struct {
	SnapShotID uint64
}

// SnapShotDeleteRequest is the request object for RpcSnapShotDelete
type SnapShotDeleteRequest struct {
	MountID    MountIDAsString
	SnapShotID uint64
}

// SnapShotDeleteReply is the reply object for RpcSnapShotDelete
type SnapShotDeleteReply struct{}

// SnapShotListRequest is the request object for RpcSnapShotListBy{ID|Name|Time}
type SnapShotListRequest struct {
	MountID  MountIDAsString
	Reversed bool
}

// SnapShotListReply is the reply object for RpcSnapShotListBy{ID|Name|Time}
type SnapShotListReply struct {
	List []headhunter.SnapShotStruct
}

// SnapShotLookupByNameRequest is the request object for RpcSnapShotLookupByName
type SnapShotLookupByNameRequest struct {
	MountID MountIDAsString
	Name    string
}

// SnapShotLookupByNameReply is the reply object for RpcSnapShotLookupByName
type SnapShotLookupByNameReply struct {
	SnapShot headhunter.SnapShotStruct
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
	InodeHandle
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
type RPCInterrupt struct {
	RPCInterruptType       // One of RPCInterruptType*
	InodeNumber      int64 // if RPCInterruptType == RPCInterruptTypeUnmount, InodeNumber == 0 (ignored)
}

// LeaseReportElementStruct describes the state of a particular Lease. Any SharedLease holders or
// a singular ExclusiveLease holder will be reported (if any) as well as a list of any clients
// awaiting Shared or Exclusive Leases.
//
type LeaseReportElementStruct struct {
	InodeNumber          string
	SharedHoldersList    []MountIDAsString
	PromotingHolder      MountIDAsString
	ExclusiveHolder      MountIDAsString
	DemotingHolder       MountIDAsString
	ReleasingHoldersList []MountIDAsString
	RequestedList        []string // Time-ordered (i.e. 1st element is next to receive Lease)
	//                                 SharedLease    waiters will be of the form "S[<MountIDAsString>]"
	//                                 ExclusiveLease waiters will be of the form "E[<MountIDAsString>]"
}

// FetchLeaseReport returns a JSON-marshalable dump of the state of Leases for the specified Volume.
//
func FetchLeaseReport(volumeName string) (leaseReport []*LeaseReportElementStruct, err error) {
	leaseReport, err = fetchLeaseReport(volumeName)
	return
}
