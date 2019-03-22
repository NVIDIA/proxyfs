// Package fs, sitting on top of the inode manager, defines the filesystem exposed by ProxyFS.
package fs

// #include <limits.h>
import "C"

import (
	"time"

	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/utils"
)

type MountID uint64

// ReadRangeIn is the ReadPlan range requested
//
// Either Offset or Len can be omitted, but not both. Those correspond
// to HTTP byteranges "bytes=N-" (no Len; asks for byte N to the end
// of the file) and "bytes=-N" (no Offset; asks for the last N bytes
// of the file).
type ReadRangeIn struct {
	Offset *uint64
	Len    *uint64
}

// Returned by MiddlewareGetAccount
type AccountEntry struct {
	Basename         string
	ModificationTime uint64 // nanoseconds since epoch
	AttrChangeTime   uint64 // nanoseconds since epoch
}

// Returned by MiddlewareGetContainer
//
type ContainerEntry struct {
	Basename         string
	FileSize         uint64
	ModificationTime uint64 // nanoseconds since epoch
	AttrChangeTime   uint64 // nanoseconds since epoch
	IsDir            bool
	NumWrites        uint64
	InodeNumber      uint64
	Metadata         []byte
}

type HeadResponse struct {
	Metadata         []byte
	FileSize         uint64
	ModificationTime uint64 // nanoseconds since epoch
	AttrChangeTime   uint64 // nanoseconds since epoch
	IsDir            bool
	InodeNumber      inode.InodeNumber
	NumWrites        uint64
}

// The following constants are used to ensure that the length of file fullpath and basenames are POSIX-compliant
const (
	FilePathMax = C.PATH_MAX
	FileNameMax = C.NAME_MAX
)

// The maximum number of symlinks we will follow
const MaxSymlinks = 8 // same as Linux; see include/linux/namei.h in Linux's Git repository

// Constant defining the name of the alternate data stream used by Swift Middleware
const MiddlewareStream = "middleware"

// Base-2 constants
const (
	Kibi = 1024
	Mebi = Kibi * 1024
	Gibi = Mebi * 1024
	Tebi = Gibi * 1024
)

// StatVfs defaults
const (
	DefaultReportedBlockSize    uint64 = 64 * Kibi
	DefaultReportedFragmentSize uint64 = 64 * Kibi
	DefaultReportedNumBlocks    uint64 = 100 * Tebi / DefaultReportedBlockSize
	DefaultReportedNumInodes    uint64 = 100 * Gibi
)

type FlockStruct struct {
	Type   int32
	Whence int32
	Start  uint64
	Len    uint64
	Pid    uint64
}

type MountOptions uint64

const (
	MountReadOnly MountOptions = 1 << iota
)

type StatKey uint64

const (
	StatCTime     StatKey = iota + 1 // time of last inode attribute change (ctime in posix stat)
	StatCRTime                       // time of inode creation              (crtime in posix stat)
	StatMTime                        // time of last data modification      (mtime in posix stat)
	StatATime                        // time of last data access            (atime in posix stat)
	StatSize                         // inode data size in bytes
	StatNLink                        // Number of hard links to the inode
	StatFType                        // file type of inode
	StatINum                         // inode number
	StatMode                         // file mode
	StatUserID                       // file userid
	StatGroupID                      // file groupid
	StatNumWrites                    // number of writes to inode
)

// XXX TODO: StatMode, StatUserID, and StatGroupID are really
//           uint32, not uint64. How to expose a stat map with
//           values of different types?
type Stat map[StatKey]uint64 // key is one of StatKey consts

// Whole-filesystem stats for StatVfs calls
//
type StatVFSKey uint64

const (
	StatVFSBlockSize      StatVFSKey = iota + 1 // statvfs.f_bsize - Filesystem block size
	StatVFSFragmentSize                         // statvfs.f_frsize - Filesystem fragment size, smallest addressable data size in the filesystem
	StatVFSTotalBlocks                          // statvfs.f_blocks - Filesystem size in StatVFSFragmentSize units
	StatVFSFreeBlocks                           // statvfs.f_bfree - number of free blocks
	StatVFSAvailBlocks                          // statvfs.f_bavail - number of free blocks for unprivileged users
	StatVFSTotalInodes                          // statvfs.f_files - number of inodes in the filesystem
	StatVFSFreeInodes                           // statvfs.f_ffree - number of free inodes in the filesystem
	StatVFSAvailInodes                          // statvfs.f_favail - number of free inodes for unprivileged users
	StatVFSFilesystemID                         // statvfs.f_fsid  - Our filesystem ID
	StatVFSMountFlags                           // statvfs.f_flag  - mount flags
	StatVFSMaxFilenameLen                       // statvfs.f_namemax - maximum filename length
)

type StatVFS map[StatVFSKey]uint64 // key is one of StatVFSKey consts

type JobHandle interface {
	Active() (active bool)
	Wait()
	Cancel()
	Error() (err []string)
	Info() (info []string)
}

// Mount handle interface

func MountByAccountName(accountName string, mountOptions MountOptions) (mountHandle MountHandle, err error) {
	mountHandle, err = mountByAccountName(accountName, mountOptions)
	return
}

func MountByVolumeName(volumeName string, mountOptions MountOptions) (mountHandle MountHandle, err error) {
	mountHandle, err = mountByVolumeName(volumeName, mountOptions)
	return
}

type MountHandle interface {
	Access(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, accessMode inode.InodeMode) (accessReturn bool)
	CallInodeToProvisionObject() (pPath string, err error)
	Create(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, dirInodeNumber inode.InodeNumber, basename string, filePerm inode.InodeMode) (fileInodeNumber inode.InodeNumber, err error)
	FetchExtentMapChunk(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, fileInodeNumber inode.InodeNumber, fileOffset uint64, maxEntriesFromFileOffset int64, maxEntriesBeforeFileOffset int64) (extentMapChunk *inode.ExtentMapChunkStruct, err error)
	Flush(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (err error)
	Flock(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, lockCmd int32, inFlockStruct *FlockStruct) (outFlockStruct *FlockStruct, err error)
	Getstat(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (stat Stat, err error)
	GetType(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (inodeType inode.InodeType, err error)
	GetXAttr(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, streamName string) (value []byte, err error)
	IsDir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (inodeIsDir bool, err error)
	IsFile(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (inodeIsFile bool, err error)
	IsSymlink(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (inodeIsSymlink bool, err error)
	Link(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, dirInodeNumber inode.InodeNumber, basename string, targetInodeNumber inode.InodeNumber) (err error)
	ListXAttr(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (streamNames []string, err error)
	Lookup(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, dirInodeNumber inode.InodeNumber, basename string) (inodeNumber inode.InodeNumber, err error)
	LookupPath(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, fullpath string) (inodeNumber inode.InodeNumber, err error)
	MiddlewareCoalesce(destPath string, elementPaths []string) (ino uint64, numWrites uint64, modificationTime uint64, err error)
	MiddlewareDelete(parentDir string, baseName string) (err error)
	MiddlewareGetAccount(maxEntries uint64, marker string, endmarker string) (accountEnts []AccountEntry, mtime uint64, ctime uint64, err error)
	MiddlewareGetContainer(vContainerName string, maxEntries uint64, marker string, endmarker string, prefix string, delimiter string) (containerEnts []ContainerEntry, err error)
	MiddlewareGetObject(containerObjectPath string, readRangeIn []ReadRangeIn, readRangeOut *[]inode.ReadPlanStep) (fileSize uint64, lastModified uint64, lastChanged uint64, ino uint64, numWrites uint64, serializedMetadata []byte, err error)
	MiddlewareHeadResponse(entityPath string) (response HeadResponse, err error)
	MiddlewareMkdir(vContainerName string, vObjectPath string, metadata []byte) (mtime uint64, ctime uint64, inodeNumber inode.InodeNumber, numWrites uint64, err error)
	MiddlewarePost(parentDir string, baseName string, newMetaData []byte, oldMetaData []byte) (err error)
	MiddlewarePutComplete(vContainerName string, vObjectPath string, pObjectPaths []string, pObjectLengths []uint64, pObjectMetadata []byte) (mtime uint64, ctime uint64, fileInodeNumber inode.InodeNumber, numWrites uint64, err error)
	MiddlewarePutContainer(containerName string, oldMetadata []byte, newMetadata []byte) (err error)
	Mkdir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, basename string, filePerm inode.InodeMode) (newDirInodeNumber inode.InodeNumber, err error)
	RemoveXAttr(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, streamName string) (err error)
	Rename(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, srcDirInodeNumber inode.InodeNumber, srcBasename string, dstDirInodeNumber inode.InodeNumber, dstBasename string) (err error)
	Read(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, offset uint64, length uint64, profiler *utils.Profiler) (buf []byte, err error)
	Readdir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, maxEntries uint64, prevReturned ...interface{}) (entries []inode.DirEntry, numEntries uint64, areMoreEntries bool, err error)
	ReaddirPlus(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, maxEntries uint64, prevReturned ...interface{}) (dirEntries []inode.DirEntry, statEntries []Stat, numEntries uint64, areMoreEntries bool, err error)
	Readsymlink(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber) (target string, err error)
	Resize(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, newSize uint64) (err error)
	Rmdir(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, basename string) (err error)
	Setstat(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, stat Stat) (err error)
	SetXAttr(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, streamName string, value []byte, flags int) (err error)
	StatVfs() (statVFS StatVFS, err error)
	Symlink(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, basename string, target string) (symlinkInodeNumber inode.InodeNumber, err error)
	Unlink(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, basename string) (err error)
	VolumeName() (volumeName string)
	Write(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, offset uint64, buf []byte, profiler *utils.Profiler) (size uint64, err error)
	Wrote(userID inode.InodeUserID, groupID inode.InodeGroupID, otherGroupIDs []inode.InodeGroupID, inodeNumber inode.InodeNumber, objectPath string, fileOffset []uint64, objectOffset []uint64, length []uint64) (err error)
}

// ValidateVolume performs an "FSCK" on the specified volumeName.
func ValidateVolume(volumeName string) (validateVolumeHandle JobHandle) {
	var (
		vVS *validateVolumeStruct
	)
	startTime := time.Now()
	defer func() {
		globals.ValidateVolumeUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	vVS = &validateVolumeStruct{}

	vVS.jobType = "FSCK"
	vVS.volumeName = volumeName
	vVS.active = true
	vVS.stopFlag = false
	vVS.err = make([]string, 0)
	vVS.info = make([]string, 0)

	vVS.globalWaitGroup.Add(1)
	go vVS.validateVolume()

	validateVolumeHandle = vVS

	return
}

// ScrubVolume performs a "SCRUB" on the specified volumeName.
func ScrubVolume(volumeName string) (scrubVolumeHandle JobHandle) {
	var (
		sVS *scrubVolumeStruct
	)
	startTime := time.Now()
	defer func() {
		globals.ScrubVolumeUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	sVS = &scrubVolumeStruct{}

	sVS.jobType = "SCRUB"
	sVS.volumeName = volumeName
	sVS.active = true
	sVS.stopFlag = false
	sVS.err = make([]string, 0)
	sVS.info = make([]string, 0)

	sVS.globalWaitGroup.Add(1)
	go sVS.scrubVolume()

	scrubVolumeHandle = sVS

	return
}

// Utility functions

func ValidateBaseName(baseName string) (err error) {
	startTime := time.Now()
	defer func() {
		globals.ValidateBaseNameUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.ValidateBaseNameErrors.Add(1)
		}
	}()

	err = validateBaseName(baseName)
	return
}

func ValidateFullPath(fullPath string) (err error) {
	startTime := time.Now()
	defer func() {
		globals.ValidateFullPathUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.ValidateFullPathErrors.Add(1)
		}
	}()

	err = validateFullPath(fullPath)
	return
}

func AccountNameToVolumeName(accountName string) (volumeName string, ok bool) {
	startTime := time.Now()
	defer func() {
		globals.AccountNameToVolumeNameUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	volumeName, ok = inode.AccountNameToVolumeName(accountName)
	return
}

func VolumeNameToActivePeerPrivateIPAddr(volumeName string) (activePeerPrivateIPAddr string, ok bool) {
	startTime := time.Now()
	defer func() {
		globals.VolumeNameToActivePeerPrivateIPAddrUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	activePeerPrivateIPAddr, ok = inode.VolumeNameToActivePeerPrivateIPAddr(volumeName)
	return
}
