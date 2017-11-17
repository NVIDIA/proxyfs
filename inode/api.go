// Package inode provides inode-management functionality for ProxyFS.
package inode

import (
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/utils"
)

type InodeNumber uint64
type InodeType uint16
type InodeMode uint32
type InodeUserID uint32
type InodeGroupID uint32
type InodeDirLocation int64

const (
	InodeRootUserID  = InodeUserID(0)
	InodeRootGroupID = InodeGroupID(0)
)

// NOTE: Using unix.DT_* constants for these types makes it easier
//       to expose this information in a standardized way with our RPC APIs.
const (
	DirType     InodeType = unix.DT_DIR
	FileType    InodeType = unix.DT_REG
	SymlinkType InodeType = unix.DT_LNK
)

// The following are used in calls to Access()... either F_OK or bitwise or of R_OK, W_OK, and X_OK

const (
	F_OK = InodeMode(unix.F_OK)                               //         check for existence
	R_OK = InodeMode(unix.R_OK)                               // UID:GID check for read    permission
	W_OK = InodeMode(unix.W_OK)                               // UID:GID check for write   permission
	X_OK = InodeMode(unix.X_OK)                               // UID:GID check for execute permission
	P_OK = InodeMode((unix.R_OK | unix.W_OK | unix.X_OK) + 1) //         check for ownership
)

// The following line of code is a directive to go generate that tells it to create a
// file called inodetype_string.go that implements the .String() method for InodeType.
//go:generate stringer -type=InodeType

type MetadataStruct struct {
	InodeType
	LinkCount            uint64
	Size                 uint64
	CreationTime         time.Time
	ModificationTime     time.Time
	AccessTime           time.Time
	AttrChangeTime       time.Time // aka ctime; This field is intended to be changed by writing or by setting inode information (i.e., owner, group, link count, mode, etc.).
	NumWrites            uint64    // only maintained for FileType inodes
	InodeStreamNameSlice []string
	Mode                 InodeMode
	UserID               InodeUserID
	GroupID              InodeGroupID
}

type FragmentationReport struct {
	NumberOfFragments uint64 // used with BytesInFragments to compute average fragment size
	BytesInFragments  uint64 // equivalent to size of file for FileInode that is not sparse
	BytesTrapped      uint64 // unreferenced bytes trapped in referenced log segments
}

type DirEntry struct {
	InodeNumber
	Basename        string
	Type            InodeType
	NextDirLocation InodeDirLocation
}

type CoalesceElement struct {
	ContainingDirectoryInodeNumber InodeNumber
	ElementInodeNumber             InodeNumber
	ElementName                    string
}

func (de *DirEntry) Size() int {
	// sizeof(InodeNumber) + sizeof(InodeType) + sizeof(DirLocation) + string data + null byte delimiter
	return int(unsafe.Sizeof(de.InodeNumber)) + int(unsafe.Sizeof(de.Type)) + int(unsafe.Sizeof(de.NextDirLocation)) + len(de.Basename) + 1
}

type ReadPlanStep struct {
	LogSegmentNumber uint64 // If == 0, Length specifies zero-file size
	Offset           uint64 // If zero-fill case, == 0
	Length           uint64 // Must != 0
	AccountName      string // If == "", Length specifies a zero-fill size
	ContainerName    string // If == "", Length specifies a zero-fill size
	ObjectName       string // If == "", Length specifies a zero-fill size
	ObjectPath       string // If == "", Length specifies a zero-fill size
}

const (
	RootDirInodeNumber = InodeNumber(1)
)

// AccountNameToVolumeName returns the corresponding volumeName for the supplied accountName (if any).
func AccountNameToVolumeName(accountName string) (volumeName string, ok bool) {
	volumeName, ok = accountNameToVolumeName(accountName)
	return
}

// VolumeNameToActivePeerPrivateIPAddr returns the Peer IP Address serving the specified VolumeName.
func VolumeNameToActivePeerPrivateIPAddr(volumeName string) (activePeerPrivateIPAddr string, ok bool) {
	activePeerPrivateIPAddr, ok = volumeNameToActivePeerPrivateIPAddr(volumeName)
	return
}

// FetchVolumeHandle returns a the VolumeHandle corresponding to the name VolumeName.
//
// Note: The method should be considered a write operation on the RoodDirInodeNumber.
//       As such, an exclusive lock should be held around a call to FetchVolumeHandle().
func FetchVolumeHandle(volumeName string) (volumeHandle VolumeHandle, err error) {
	volumeHandle, err = fetchVolumeHandle(volumeName)
	return
}

type VolumeHandle interface {
	// Generic methods, implemented volume.go

	GetFSID() (fsid uint64)

	// Common Inode methods, implemented in inode.go

	Access(inodeNumber InodeNumber, userID InodeUserID, groupID InodeGroupID, otherGroupIDs []InodeGroupID, accessMode InodeMode) (accessReturn bool)
	Purge(inodeNumber InodeNumber) (err error)
	Destroy(inodeNumber InodeNumber) (err error)
	GetMetadata(inodeNumber InodeNumber) (metadata *MetadataStruct, err error)
	GetType(inodeNumber InodeNumber) (inodeType InodeType, err error)
	GetLinkCount(inodeNumber InodeNumber) (linkCount uint64, err error)
	SetLinkCount(inodeNumber InodeNumber, linkCount uint64) (err error)
	SetCreationTime(inodeNumber InodeNumber, creationTime time.Time) (err error)
	SetModificationTime(inodeNumber InodeNumber, modificationTime time.Time) (err error)
	SetAccessTime(inodeNumber InodeNumber, accessTime time.Time) (err error)
	SetPermMode(inodeNumber InodeNumber, filePerm InodeMode) (err error)
	SetOwnerUserID(inodeNumber InodeNumber, userID InodeUserID) (err error)
	SetOwnerUserIDGroupID(inodeNumber InodeNumber, userID InodeUserID, groupID InodeGroupID) (err error)
	SetOwnerGroupID(inodeNumber InodeNumber, groupID InodeGroupID) (err error)
	GetStream(inodeNumber InodeNumber, inodeStreamName string) (buf []byte, err error)
	PutStream(inodeNumber InodeNumber, inodeStreamName string, buf []byte) (err error)
	DeleteStream(inodeNumber InodeNumber, inodeStreamName string) (err error)
	GetFragmentationReport(inodeNumber InodeNumber) (fragmentationReport FragmentationReport, err error)
	Optimize(inodeNumber InodeNumber, maxDuration time.Duration) (err error)
	Validate(inodeNumber InodeNumber) (err error)

	// Directory Inode specific methods, implemented in dir.go

	CreateDir(filePerm InodeMode, userID InodeUserID, groupID InodeGroupID) (dirInodeNumber InodeNumber, err error)
	Link(dirInodeNumber InodeNumber, basename string, targetInodeNumber InodeNumber) (err error)
	Unlink(dirInodeNumber InodeNumber, basename string) (err error)
	Move(srcDirInodeNumber InodeNumber, srcBasename string, dstDirInodeNumber InodeNumber, dstBasename string) (err error)
	Lookup(dirInodeNumber InodeNumber, basename string) (targetInodeNumber InodeNumber, err error)
	NumDirEntries(dirInodeNumber InodeNumber) (numEntries uint64, err error)
	ReadDir(dirInodeNumber InodeNumber, maxEntries uint64, maxBufSize uint64, prevReturned ...interface{}) (dirEntrySlice []DirEntry, moreEntries bool, err error)

	// File Inode specific methods, implemented in file.go

	CreateFile(filePerm InodeMode, userID InodeUserID, groupID InodeGroupID) (fileInodeNumber InodeNumber, err error)
	Read(inodeNumber InodeNumber, offset uint64, length uint64, profiler *utils.Profiler) (buf []byte, err error)
	GetReadPlan(fileInodeNumber InodeNumber, offset *uint64, length *uint64) (readPlan []ReadPlanStep, err error)
	Write(fileInodeNumber InodeNumber, offset uint64, buf []byte, profiler *utils.Profiler) (err error)
	ProvisionObject() (objectPath string, err error)
	Wrote(fileInodeNumber InodeNumber, fileOffset uint64, objectPath string, objectOffset uint64, length uint64, patchOnly bool) (err error)
	SetSize(fileInodeNumber InodeNumber, Size uint64) (err error)
	Flush(fileInodeNumber InodeNumber, andPurge bool) (err error)
	Coalesce(containingDirInode InodeNumber, combinationName string, elements []CoalesceElement) (combinationInodeNumber InodeNumber, modificationTime time.Time, numWrites uint64, err error)

	// Symlink Inode specific methods, implemented in symlink.go

	CreateSymlink(target string, filePerm InodeMode, userID InodeUserID, groupID InodeGroupID) (symlinkInodeNumber InodeNumber, err error)
	GetSymlink(symlinkInodeNumber InodeNumber) (target string, err error)
}
