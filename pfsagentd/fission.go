package main

import (
	"container/list"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/swiftstack/fission"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
)

const (
	initOutFlagsMaskReadDirPlusDisabled = uint32(0) |
		fission.InitFlagsAsyncRead |
		fission.InitFlagsFileOps |
		fission.InitFlagsAtomicOTrunc |
		fission.InitFlagsBigWrites |
		fission.InitFlagsAutoInvalData |
		fission.InitFlagsParallelDirops |
		fission.InitFlagsMaxPages |
		fission.InitFlagsExplicitInvalData

	initOutFlagsMaskReadDirPlusEnabled = initOutFlagsMaskReadDirPlusDisabled |
		fission.InitFlagsDoReadDirPlus |
		fission.InitFlagsReaddirplusAuto

	pfsagentFuseSubtype = "PFSAgent"
)

func performMountFUSE() {
	var (
		err error
	)

	globals.fissionVolume = fission.NewVolume(
		globals.config.FUSEVolumeName,     // volumeName        string
		globals.config.FUSEMountPointPath, // mountpointDirPath string
		pfsagentFuseSubtype,               // fuseSubtype       string
		globals.config.FUSEMaxWrite,       // initOutMaxWrite   uint32
		&globals,                          // callbacks         fission.Callbacks
		newLogger(),                       // logger            *log.Logger
		globals.fissionErrChan)            // errChan           chan error

	err = globals.fissionVolume.DoMount()
	if nil != err {
		log.Fatalf("fissionVolume.DoMount() failed: %v", err)
	}
}

func performUnmountFUSE() {
	var (
		err error
	)

	err = globals.fissionVolume.DoUnmount()
	if nil != err {
		log.Fatalf("fissionVolume.DoUnmount() failed: %v", err)
	}
}

func convertErrToErrno(err error, defaultErrno syscall.Errno) (errno syscall.Errno) {
	var (
		convertErr    error
		possibleErrno syscall.Errno
	)

	_, convertErr = fmt.Sscanf(err.Error(), "errno: %v", &possibleErrno)
	if nil == convertErr {
		errno = possibleErrno
	} else {
		errno = defaultErrno
	}

	return
}

func fixAttrSizes(attr *fission.Attr) {
	if syscall.S_IFREG == (attr.Mode & syscall.S_IFMT) {
		attr.Blocks = attr.Size + (globals.config.AttrBlockSize - 1)
		attr.Blocks /= globals.config.AttrBlockSize
		attr.BlkSize = uint32(globals.config.AttrBlockSize)
	} else {
		attr.Size = 0
		attr.Blocks = 0
		attr.BlkSize = 0
	}
}

func nsToUnixTime(ns uint64) (sec uint64, nsec uint32) {
	sec = ns / 1e9
	nsec = uint32(ns - (sec * 1e9))
	return
}

func unixTimeToNs(sec uint64, nsec uint32) (ns uint64) {
	ns = (sec * 1e9) + uint64(nsec)
	return
}

func (dummy *globalsStruct) DoLookup(inHeader *fission.InHeader, lookupIn *fission.LookupIn) (lookupOut *fission.LookupOut, errno syscall.Errno) {
	var (
		aTimeNSec         uint32
		aTimeSec          uint64
		cTimeNSec         uint32
		cTimeSec          uint64
		err               error
		lookupPlusReply   *jrpcfs.LookupPlusReply
		lookupPlusRequest *jrpcfs.LookupPlusRequest
		mTimeNSec         uint32
		mTimeSec          uint64
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoLookup_calls, 1)

	lookupPlusRequest = &jrpcfs.LookupPlusRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		Basename: string(lookupIn.Name[:]),
	}

	lookupPlusReply = &jrpcfs.LookupPlusReply{}

	err = globals.retryRPCClient.Send("RpcLookupPlus", lookupPlusRequest, lookupPlusReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.ENOENT)
		return
	}

	aTimeSec, aTimeNSec = nsToUnixTime(lookupPlusReply.ATimeNs)
	mTimeSec, mTimeNSec = nsToUnixTime(lookupPlusReply.MTimeNs)
	cTimeSec, cTimeNSec = nsToUnixTime(lookupPlusReply.CTimeNs)

	lookupOut = &fission.LookupOut{
		EntryOut: fission.EntryOut{
			NodeID:         uint64(lookupPlusReply.InodeNumber),
			Generation:     0,
			EntryValidSec:  globals.entryValidSec,
			AttrValidSec:   globals.attrValidSec,
			EntryValidNSec: globals.entryValidNSec,
			AttrValidNSec:  globals.attrValidNSec,
			Attr: fission.Attr{
				Ino:       uint64(lookupPlusReply.InodeNumber),
				Size:      lookupPlusReply.Size,
				Blocks:    0, // fixAttrSizes() will compute this
				ATimeSec:  aTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  cTimeSec,
				ATimeNSec: aTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: cTimeNSec,
				Mode:      lookupPlusReply.FileMode,
				NLink:     uint32(lookupPlusReply.NumLinks),
				UID:       lookupPlusReply.UserID,
				GID:       lookupPlusReply.GroupID,
				RDev:      0,
				BlkSize:   0, // fixAttrSizes() will set this
				Padding:   0,
			},
		},
	}

	fixAttrSizes(&lookupOut.EntryOut.Attr)

	errno = 0
	return
}

func (dummy *globalsStruct) DoForget(inHeader *fission.InHeader, forgetIn *fission.ForgetIn) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoForget_calls, 1)
	return
}

func (dummy *globalsStruct) DoGetAttr(inHeader *fission.InHeader, getAttrIn *fission.GetAttrIn) (getAttrOut *fission.GetAttrOut, errno syscall.Errno) {
	var (
		aTimeNSec      uint32
		aTimeSec       uint64
		cTimeNSec      uint32
		cTimeSec       uint64
		err            error
		fileInode      *fileInodeStruct
		getStatReply   *jrpcfs.StatStruct
		getStatRequest *jrpcfs.GetStatRequest
		mTimeNSec      uint32
		mTimeSec       uint64
		ok             bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoGetAttr_calls, 1)

	// fileInode = lockInodeWithSharedLease()

	globals.Lock()

	fileInode, ok = globals.fileInodeMap[inode.InodeNumber(inHeader.NodeID)]
	if ok {
		aTimeSec, aTimeNSec = nsToUnixTime(fileInode.cachedStat.ATimeNs)
		mTimeSec, mTimeNSec = nsToUnixTime(fileInode.cachedStat.MTimeNs)
		cTimeSec, cTimeNSec = nsToUnixTime(fileInode.cachedStat.CTimeNs)

		getAttrOut = &fission.GetAttrOut{
			AttrValidSec:  globals.attrValidSec,
			AttrValidNSec: globals.attrValidNSec,
			Dummy:         0,
			Attr: fission.Attr{
				Ino:       inHeader.NodeID,
				Size:      fileInode.cachedStat.Size,
				Blocks:    0, // fixAttrSizes() will compute this
				ATimeSec:  aTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  cTimeSec,
				ATimeNSec: aTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: cTimeNSec,
				Mode:      fileInode.cachedStat.FileMode,
				NLink:     uint32(fileInode.cachedStat.NumLinks),
				UID:       fileInode.cachedStat.UserID,
				GID:       fileInode.cachedStat.GroupID,
				RDev:      0,
				BlkSize:   0, // fixAttrSizes() will set this
				Padding:   0,
			},
		}

		globals.Unlock()
	} else {
		globals.Unlock()

		getStatRequest = &jrpcfs.GetStatRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(inHeader.NodeID),
			},
		}

		getStatReply = &jrpcfs.StatStruct{}

		err = globals.retryRPCClient.Send("RpcGetStat", getStatRequest, getStatReply)
		if nil != err {
			errno = convertErrToErrno(err, syscall.EIO)
			return
		}

		aTimeSec, aTimeNSec = nsToUnixTime(getStatReply.ATimeNs)
		mTimeSec, mTimeNSec = nsToUnixTime(getStatReply.MTimeNs)
		cTimeSec, cTimeNSec = nsToUnixTime(getStatReply.CTimeNs)

		getAttrOut = &fission.GetAttrOut{
			AttrValidSec:  globals.attrValidSec,
			AttrValidNSec: globals.attrValidNSec,
			Dummy:         0,
			Attr: fission.Attr{
				Ino:       inHeader.NodeID,
				Size:      getStatReply.Size,
				Blocks:    0, // fixAttrSizes() will compute this
				ATimeSec:  aTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  cTimeSec,
				ATimeNSec: aTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: cTimeNSec,
				Mode:      getStatReply.FileMode,
				NLink:     uint32(getStatReply.NumLinks),
				UID:       getStatReply.UserID,
				GID:       getStatReply.GroupID,
				RDev:      0,
				BlkSize:   0, // fixAttrSizes() will set this
				Padding:   0,
			},
		}
	}

	fixAttrSizes(&getAttrOut.Attr)

	errno = 0
	return
}

func setMode(nodeID uint64, mode uint32) (errno syscall.Errno) {
	var (
		chmodReply   *jrpcfs.Reply
		chmodRequest *jrpcfs.ChmodRequest
		err          error
	)

	chmodRequest = &jrpcfs.ChmodRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(nodeID),
		},
		FileMode: mode & uint32(os.ModePerm),
	}

	chmodReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcChmod", chmodRequest, chmodReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func setUIDAndOrGID(nodeID uint64, settingUID bool, uid uint32, settingGID bool, gid uint32) (errno syscall.Errno) {
	var (
		chownReply   *jrpcfs.Reply
		chownRequest *jrpcfs.ChownRequest
		err          error
	)

	chownRequest = &jrpcfs.ChownRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(nodeID),
		},
	}

	if settingUID {
		chownRequest.UserID = int32(uid)
	} else {
		chownRequest.UserID = -1
	}

	if settingGID {
		chownRequest.GroupID = int32(gid)
	} else {
		chownRequest.GroupID = -1
	}

	chownReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcChown", chownRequest, chownReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func setSize(nodeID uint64, size uint64) (errno syscall.Errno) {
	var (
		chunkedPutContext        *chunkedPutContextStruct
		chunkedPutContextElement *list.Element
		err                      error
		fileInode                *fileInodeStruct
		grantedLock              *fileInodeLockRequestStruct
		ok                       bool
		resizeReply              *jrpcfs.Reply
		resizeRequest            *jrpcfs.ResizeRequest
	)

	fileInode = referenceFileInode(inode.InodeNumber(nodeID))
	if nil == fileInode {
		errno = syscall.ENOENT
		return
	}

	fileInode.doFlushIfNecessary()

	grantedLock = fileInode.getExclusiveLock()

	fileInode.cachedStat.Size = size

	pruneExtentMap(fileInode.extentMap, size)

	chunkedPutContextElement = fileInode.chunkedPutList.Front()

	for nil != chunkedPutContextElement {
		chunkedPutContext, ok = chunkedPutContextElement.Value.(*chunkedPutContextStruct)
		if !ok {
			logFatalf("chunkedPutContextElement.Value.(*chunkedPutContextStruct) returned !ok")
		}

		pruneExtentMap(chunkedPutContext.extentMap, size)

		chunkedPutContextElement = chunkedPutContextElement.Next()
	}

	resizeRequest = &jrpcfs.ResizeRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(nodeID),
		},
		NewSize: size,
	}

	resizeReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcResize", resizeRequest, resizeReply)
	if nil != err {
		grantedLock.release()
		fileInode.dereference()
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	grantedLock.release()

	fileInode.dereference()

	errno = 0
	return
}

func setMTimeAndOrATime(nodeID uint64, settingMTime bool, settingMTimeNow bool, mTimeSec uint64, mTimeNSec uint32, settingATime bool, settingATimeNow bool, aTimeSec uint64, aTimeNSec uint32) (errno syscall.Errno) {
	var (
		err            error
		setTimeReply   *jrpcfs.Reply
		setTimeRequest *jrpcfs.SetTimeRequest
		timeNow        time.Time
	)

	setTimeRequest = &jrpcfs.SetTimeRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(nodeID),
		},
		StatStruct: jrpcfs.StatStruct{
			MTimeNs: uint64(0), // Updated below if settingMTime
			ATimeNs: uint64(0), // Updated below if settingATime
		},
	}

	timeNow = time.Now()

	if settingMTime {
		if settingMTimeNow {
			setTimeRequest.MTimeNs = uint64(timeNow.UnixNano())
		} else {
			setTimeRequest.MTimeNs = unixTimeToNs(mTimeSec, mTimeNSec)
		}
	}
	if settingATime {
		if settingATimeNow {
			setTimeRequest.ATimeNs = uint64(timeNow.UnixNano())
		} else {
			setTimeRequest.ATimeNs = unixTimeToNs(aTimeSec, aTimeNSec)
		}
	}

	setTimeReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcSetTime", setTimeRequest, setTimeReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoSetAttr(inHeader *fission.InHeader, setAttrIn *fission.SetAttrIn) (setAttrOut *fission.SetAttrOut, errno syscall.Errno) {
	var (
		aTimeNSec              uint32
		aTimeSec               uint64
		cachedFileInodeCase    bool
		cTimeNSec              uint32
		cTimeSec               uint64
		err                    error
		fileInode              *fileInodeStruct
		getStatReply           *jrpcfs.StatStruct
		getStatRequest         *jrpcfs.GetStatRequest
		mTimeNSec              uint32
		mTimeSec               uint64
		settingATime           bool
		settingATimeNow        bool
		settingGID             bool
		settingMode            bool
		settingMTime           bool
		settingMTimeAndOrATime bool
		settingMTimeNow        bool
		settingSize            bool
		settingUID             bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoSetAttr_calls, 1)

	if setAttrIn.Valid != (setAttrIn.Valid & (fission.SetAttrInValidMode | fission.SetAttrInValidUID | fission.SetAttrInValidGID | fission.SetAttrInValidSize | fission.SetAttrInValidATime | fission.SetAttrInValidMTime | fission.SetAttrInValidFH | fission.SetAttrInValidATimeNow | fission.SetAttrInValidMTimeNow | fission.SetAttrInValidLockOwner)) {
		errno = syscall.ENOSYS
		return
	}

	settingMode = (0 != (setAttrIn.Valid & fission.SetAttrInValidMode))

	settingUID = (0 != (setAttrIn.Valid & fission.SetAttrInValidUID))
	settingGID = (0 != (setAttrIn.Valid & fission.SetAttrInValidGID))

	settingSize = (0 != (setAttrIn.Valid & fission.SetAttrInValidSize))

	settingMTime = (0 != (setAttrIn.Valid & fission.SetAttrInValidMTime)) || (0 != (setAttrIn.Valid & fission.SetAttrInValidMTimeNow))
	settingATime = (0 != (setAttrIn.Valid & fission.SetAttrInValidATime)) || (0 != (setAttrIn.Valid & fission.SetAttrInValidATimeNow))

	settingMTimeNow = settingMTime && (0 != (setAttrIn.Valid & fission.SetAttrInValidMTimeNow))
	settingATimeNow = settingATime && (0 != (setAttrIn.Valid & fission.SetAttrInValidATimeNow))

	settingMTimeAndOrATime = settingATime || settingMTime

	// TODO: Verify we can accept but ignore fission.SetAttrInValidFH        in setAttrIn.Valid
	// TODO: Verify we can accept but ignore fission.SetAttrInValidLockOwner in setAttrIn.Valid

	// Check for cached FileInode case

	globals.Lock()
	fileInode, cachedFileInodeCase = globals.fileInodeMap[inode.InodeNumber(inHeader.NodeID)]
	globals.Unlock()

	if cachedFileInodeCase {
		// Calling referenceFileInode() here even though we "know" fileInode to avoid the
		// sanity check otherwise valid where fileInode.references should not be zero.
		//
		// Note that, to avoid any race ambiguity, we will use the fileInode returned
		// from referenceFileInode() moving forward on the off-chance the one we fetched
		// from globals.fileInodeMap was removed just after the globals.Unlock() call.

		fileInode = referenceFileInode(inode.InodeNumber(inHeader.NodeID))

		fileInode.doFlushIfNecessary()
	}

	// Now perform requested setAttrIn.Mode operations

	if settingMode {
		errno = setMode(inHeader.NodeID, setAttrIn.Mode)
		if 0 != errno {
			return
		}
	}

	if settingUID || settingGID {
		errno = setUIDAndOrGID(inHeader.NodeID, settingUID, setAttrIn.UID, settingGID, setAttrIn.GID)
		if 0 != errno {
			return
		}
	}

	if settingSize {
		errno = setSize(inHeader.NodeID, setAttrIn.Size)
		if 0 != errno {
			return
		}
	}

	if settingMTimeAndOrATime {
		errno = setMTimeAndOrATime(inHeader.NodeID, settingMTime, settingMTimeNow, setAttrIn.MTimeSec, setAttrIn.MTimeNSec, settingATime, settingATimeNow, setAttrIn.ATimeSec, setAttrIn.ATimeNSec)
		if 0 != errno {
			return
		}
	}

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
	}

	getStatReply = &jrpcfs.StatStruct{}

	err = globals.retryRPCClient.Send("RpcGetStat", getStatRequest, getStatReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	if cachedFileInodeCase {
		fileInode.cachedStat = getStatReply

		fileInode.dereference()
	}

	aTimeSec, aTimeNSec = nsToUnixTime(getStatReply.ATimeNs)
	mTimeSec, mTimeNSec = nsToUnixTime(getStatReply.MTimeNs)
	cTimeSec, cTimeNSec = nsToUnixTime(getStatReply.CTimeNs)

	setAttrOut = &fission.SetAttrOut{
		AttrValidSec:  globals.attrValidSec,
		AttrValidNSec: globals.attrValidNSec,
		Dummy:         0,
		Attr: fission.Attr{
			Ino:       inHeader.NodeID,
			Size:      getStatReply.Size,
			Blocks:    0, // fixAttrSizes() will compute this
			ATimeSec:  aTimeSec,
			MTimeSec:  mTimeSec,
			CTimeSec:  cTimeSec,
			ATimeNSec: aTimeNSec,
			MTimeNSec: mTimeNSec,
			CTimeNSec: cTimeNSec,
			Mode:      getStatReply.FileMode,
			NLink:     uint32(getStatReply.NumLinks),
			UID:       getStatReply.UserID,
			GID:       getStatReply.GroupID,
			RDev:      0,
			BlkSize:   0, // fixAttrSizes() will set this
			Padding:   0,
		},
	}

	fixAttrSizes(&setAttrOut.Attr)

	errno = 0
	return
}

func (dummy *globalsStruct) DoReadLink(inHeader *fission.InHeader) (readLinkOut *fission.ReadLinkOut, errno syscall.Errno) {
	var (
		err                error
		readSymlinkReply   *jrpcfs.ReadSymlinkReply
		readSymlinkRequest *jrpcfs.ReadSymlinkRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoReadLink_calls, 1)

	readSymlinkRequest = &jrpcfs.ReadSymlinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
	}

	readSymlinkReply = &jrpcfs.ReadSymlinkReply{}

	err = globals.retryRPCClient.Send("RpcReadSymlink", readSymlinkRequest, readSymlinkReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	readLinkOut = &fission.ReadLinkOut{
		Data: []byte(readSymlinkReply.Target),
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoSymLink(inHeader *fission.InHeader, symLinkIn *fission.SymLinkIn) (symLinkOut *fission.SymLinkOut, errno syscall.Errno) {
	var (
		aTimeNSec      uint32
		aTimeSec       uint64
		cTimeNSec      uint32
		cTimeSec       uint64
		err            error
		getStatReply   *jrpcfs.StatStruct
		getStatRequest *jrpcfs.GetStatRequest
		mTimeNSec      uint32
		mTimeSec       uint64
		symlinkReply   *jrpcfs.InodeReply
		symlinkRequest *jrpcfs.SymlinkRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoSymLink_calls, 1)

	symlinkRequest = &jrpcfs.SymlinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		Basename: string(symLinkIn.Name[:]),
		Target:   string(symLinkIn.Data[:]),
		UserID:   int32(inHeader.UID),
		GroupID:  int32(inHeader.GID),
	}

	symlinkReply = &jrpcfs.InodeReply{}

	err = globals.retryRPCClient.Send("RpcSymlink", symlinkRequest, symlinkReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: symlinkReply.InodeNumber,
		},
	}

	getStatReply = &jrpcfs.StatStruct{}

	err = globals.retryRPCClient.Send("RpcGetStat", getStatRequest, getStatReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	aTimeSec, aTimeNSec = nsToUnixTime(getStatReply.ATimeNs)
	mTimeSec, mTimeNSec = nsToUnixTime(getStatReply.MTimeNs)
	cTimeSec, cTimeNSec = nsToUnixTime(getStatReply.CTimeNs)

	symLinkOut = &fission.SymLinkOut{
		EntryOut: fission.EntryOut{
			NodeID:         uint64(symlinkReply.InodeNumber),
			Generation:     0,
			EntryValidSec:  globals.entryValidSec,
			AttrValidSec:   globals.attrValidSec,
			EntryValidNSec: globals.entryValidNSec,
			AttrValidNSec:  globals.attrValidNSec,
			Attr: fission.Attr{
				Ino:       uint64(symlinkReply.InodeNumber),
				Size:      0,
				Blocks:    0,
				ATimeSec:  aTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  cTimeSec,
				ATimeNSec: aTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: cTimeNSec,
				Mode:      getStatReply.FileMode,
				NLink:     uint32(getStatReply.NumLinks),
				UID:       getStatReply.UserID,
				GID:       getStatReply.GroupID,
				RDev:      0,
				BlkSize:   0,
				Padding:   0,
			},
		},
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoMkNod(inHeader *fission.InHeader, mkNodIn *fission.MkNodIn) (mkNodOut *fission.MkNodOut, errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoMkNod_calls, 1)
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoMkDir(inHeader *fission.InHeader, mkDirIn *fission.MkDirIn) (mkDirOut *fission.MkDirOut, errno syscall.Errno) {
	var (
		aTimeNSec      uint32
		aTimeSec       uint64
		cTimeNSec      uint32
		cTimeSec       uint64
		err            error
		getStatReply   *jrpcfs.StatStruct
		getStatRequest *jrpcfs.GetStatRequest
		mkdirReply     *jrpcfs.InodeReply
		mkdirRequest   *jrpcfs.MkdirRequest
		mTimeNSec      uint32
		mTimeSec       uint64
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoMkDir_calls, 1)

	mkdirRequest = &jrpcfs.MkdirRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		Basename: string(mkDirIn.Name[:]),
		UserID:   int32(inHeader.UID),
		GroupID:  int32(inHeader.GID),
		FileMode: mkDirIn.Mode & uint32(os.ModePerm),
	}

	mkdirReply = &jrpcfs.InodeReply{}

	err = globals.retryRPCClient.Send("RpcMkdir", mkdirRequest, mkdirReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: mkdirReply.InodeNumber,
		},
	}

	getStatReply = &jrpcfs.StatStruct{}

	err = globals.retryRPCClient.Send("RpcGetStat", getStatRequest, getStatReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	aTimeSec, aTimeNSec = nsToUnixTime(getStatReply.ATimeNs)
	mTimeSec, mTimeNSec = nsToUnixTime(getStatReply.MTimeNs)
	cTimeSec, cTimeNSec = nsToUnixTime(getStatReply.CTimeNs)

	mkDirOut = &fission.MkDirOut{
		EntryOut: fission.EntryOut{
			NodeID:         uint64(mkdirReply.InodeNumber),
			Generation:     0,
			EntryValidSec:  globals.entryValidSec,
			AttrValidSec:   globals.attrValidSec,
			EntryValidNSec: globals.entryValidNSec,
			AttrValidNSec:  globals.attrValidNSec,
			Attr: fission.Attr{
				Ino:       uint64(mkdirReply.InodeNumber),
				Size:      0,
				Blocks:    0,
				ATimeSec:  aTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  cTimeSec,
				ATimeNSec: aTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: cTimeNSec,
				Mode:      getStatReply.FileMode,
				NLink:     uint32(getStatReply.NumLinks),
				UID:       getStatReply.UserID,
				GID:       getStatReply.GroupID,
				RDev:      0,
				BlkSize:   0,
				Padding:   0,
			},
		},
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoUnlink(inHeader *fission.InHeader, unlinkIn *fission.UnlinkIn) (errno syscall.Errno) {
	var (
		err           error
		unlinkReply   *jrpcfs.Reply
		unlinkRequest *jrpcfs.UnlinkRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoUnlink_calls, 1)

	doFlushIfNecessary(inode.InodeNumber(inHeader.NodeID), unlinkIn.Name)

	unlinkRequest = &jrpcfs.UnlinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		Basename: string(unlinkIn.Name[:]),
	}

	unlinkReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcUnlink", unlinkRequest, unlinkReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoRmDir(inHeader *fission.InHeader, rmDirIn *fission.RmDirIn) (errno syscall.Errno) {
	var (
		err           error
		unlinkReply   *jrpcfs.Reply
		unlinkRequest *jrpcfs.UnlinkRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoRmDir_calls, 1)

	unlinkRequest = &jrpcfs.UnlinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		Basename: string(rmDirIn.Name[:]),
	}

	unlinkReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcRmdir", unlinkRequest, unlinkReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoRename(inHeader *fission.InHeader, renameIn *fission.RenameIn) (errno syscall.Errno) {
	var (
		err           error
		renameReply   *jrpcfs.Reply
		renameRequest *jrpcfs.RenameRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoRename_calls, 1)

	// TODO: Remove this once Lease Management makes this unnecessary
	doFlushIfNecessary(inode.InodeNumber(renameIn.NewDir), renameIn.NewName)

	renameRequest = &jrpcfs.RenameRequest{
		MountID:           globals.mountID,
		SrcDirInodeNumber: int64(inHeader.NodeID),
		SrcBasename:       string(renameIn.OldName[:]),
		DstDirInodeNumber: int64(renameIn.NewDir),
		DstBasename:       string(renameIn.NewName[:]),
	}

	renameReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcRename", renameRequest, renameReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoLink(inHeader *fission.InHeader, linkIn *fission.LinkIn) (linkOut *fission.LinkOut, errno syscall.Errno) {
	var (
		aTimeNSec      uint32
		aTimeSec       uint64
		cTimeNSec      uint32
		cTimeSec       uint64
		err            error
		getStatReply   *jrpcfs.StatStruct
		getStatRequest *jrpcfs.GetStatRequest
		linkReply      *jrpcfs.Reply
		linkRequest    *jrpcfs.LinkRequest
		mTimeNSec      uint32
		mTimeSec       uint64
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoLink_calls, 1)

	linkRequest = &jrpcfs.LinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		Basename:          string(linkIn.Name[:]),
		TargetInodeNumber: int64(linkIn.OldNodeID),
	}

	linkReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcLink", linkRequest, linkReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(linkIn.OldNodeID),
		},
	}

	getStatReply = &jrpcfs.StatStruct{}

	err = globals.retryRPCClient.Send("RpcGetStat", getStatRequest, getStatReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	aTimeSec, aTimeNSec = nsToUnixTime(getStatReply.ATimeNs)
	mTimeSec, mTimeNSec = nsToUnixTime(getStatReply.MTimeNs)
	cTimeSec, cTimeNSec = nsToUnixTime(getStatReply.CTimeNs)

	linkOut = &fission.LinkOut{
		EntryOut: fission.EntryOut{
			NodeID:         linkIn.OldNodeID,
			Generation:     0,
			EntryValidSec:  globals.entryValidSec,
			AttrValidSec:   globals.attrValidSec,
			EntryValidNSec: globals.entryValidNSec,
			AttrValidNSec:  globals.attrValidNSec,
			Attr: fission.Attr{
				Ino:       linkIn.OldNodeID,
				Size:      getStatReply.Size,
				Blocks:    0, // fixAttrSizes() will compute this
				ATimeSec:  aTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  cTimeSec,
				ATimeNSec: aTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: cTimeNSec,
				Mode:      getStatReply.FileMode,
				NLink:     uint32(getStatReply.NumLinks),
				UID:       getStatReply.UserID,
				GID:       getStatReply.GroupID,
				RDev:      0,
				BlkSize:   0, // fixAttrSizes() will set this
				Padding:   0,
			},
		},
	}

	fixAttrSizes(&linkOut.EntryOut.Attr)

	errno = 0
	return
}

func (dummy *globalsStruct) DoOpen(inHeader *fission.InHeader, openIn *fission.OpenIn) (openOut *fission.OpenOut, errno syscall.Errno) {
	var (
		err            error
		fhSet          fhSetType
		getStatReply   *jrpcfs.StatStruct
		getStatRequest *jrpcfs.GetStatRequest
		ok             bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoOpen_calls, 1)

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
	}

	getStatReply = &jrpcfs.StatStruct{}

	err = globals.retryRPCClient.Send("RpcGetStat", getStatRequest, getStatReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	if syscall.S_IFREG != (getStatReply.FileMode & syscall.S_IFMT) {
		errno = syscall.EINVAL
		return
	}

	globals.Lock()

	globals.lastFH++

	globals.fhToInodeNumberMap[globals.lastFH] = inHeader.NodeID

	fhSet, ok = globals.inodeNumberToFHMap[inHeader.NodeID]
	if !ok {
		fhSet = make(fhSetType)
	}
	fhSet[globals.lastFH] = struct{}{}
	globals.inodeNumberToFHMap[inHeader.NodeID] = fhSet

	openOut = &fission.OpenOut{
		FH:        globals.lastFH,
		OpenFlags: 0,
		Padding:   0,
	}

	globals.Unlock()

	if 0 != (openIn.Flags & fission.FOpenRequestTRUNC) {
		errno = setSize(inHeader.NodeID, 0)
		if 0 != errno {
			return
		}
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoRead(inHeader *fission.InHeader, readIn *fission.ReadIn) (readOut *fission.ReadOut, errno syscall.Errno) {
	var (
		curObjectOffset                           uint64
		err                                       error
		fhInodeNumber                             uint64
		fileInode                                 *fileInodeStruct
		grantedLock                               *fileInodeLockRequestStruct
		logSegmentCacheElement                    *logSegmentCacheElementStruct
		logSegmentCacheElementBufEndingPosition   uint64
		logSegmentCacheElementBufRemainingLen     uint64
		logSegmentCacheElementBufSelectedLen      uint64
		logSegmentCacheElementBufStartingPosition uint64
		ok                                        bool
		readPlan                                  []interface{}
		readPlanSpan                              uint64
		readPlanStepAsInterface                   interface{}
		readPlanStepAsMultiObjectExtentStruct     *multiObjectExtentStruct
		readPlanStepAsSingleObjectExtentWithLink  *singleObjectExtentWithLinkStruct
		readPlanStepRemainingLength               uint64
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoRead_calls, 1)
	globals.stats.FUSEDoReadBytes.Add(uint64(readIn.Size))

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[readIn.FH]
	if !ok {
		logFatalf("DoRead(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, readIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoRead(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, readIn.FH, fhInodeNumber)
	}

	globals.Unlock()

	fileInode = referenceFileInode(inode.InodeNumber(inHeader.NodeID))
	if nil == fileInode {
		logFatalf("DoRead(NodeID=%v,FH=%v) called for non-FileInode", inHeader.NodeID, readIn.FH)
	}
	defer fileInode.dereference()

	grantedLock = fileInode.getSharedLock()
	defer grantedLock.release()

	err = fileInode.populateExtentMap(uint64(readIn.Offset), uint64(readIn.Size))
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	readPlan, readPlanSpan = fileInode.getReadPlan(uint64(readIn.Offset), uint64(readIn.Size))

	if (nil == readPlan) || (0 == readPlanSpan) {
		readOut = &fission.ReadOut{
			Data: make([]byte, 0),
		}
	} else {
		readOut = &fission.ReadOut{
			Data: make([]byte, 0, readPlanSpan),
		}

		for _, readPlanStepAsInterface = range readPlan {
			switch readPlanStepAsInterface.(type) {
			case *multiObjectExtentStruct:
				readPlanStepAsMultiObjectExtentStruct = readPlanStepAsInterface.(*multiObjectExtentStruct)

				if "" == readPlanStepAsMultiObjectExtentStruct.objectName {
					// Zero-fill for readPlanStep.length

					readOut.Data = append(readOut.Data, make([]byte, readPlanStepAsMultiObjectExtentStruct.length)...)
				} else {
					// Fetch LogSegment data... possibly crossing LogSegmentCacheLine boundaries

					curObjectOffset = readPlanStepAsMultiObjectExtentStruct.objectOffset
					readPlanStepRemainingLength = readPlanStepAsMultiObjectExtentStruct.length

					for readPlanStepRemainingLength > 0 {
						logSegmentCacheElement = fetchLogSegmentCacheLine(readPlanStepAsMultiObjectExtentStruct.containerName, readPlanStepAsMultiObjectExtentStruct.objectName, curObjectOffset)

						if logSegmentCacheElementStateGetFailed == logSegmentCacheElement.state {
							errno = syscall.EIO
							return
						}

						logSegmentCacheElementBufStartingPosition = curObjectOffset - logSegmentCacheElement.startingOffset
						logSegmentCacheElementBufRemainingLen = uint64(len(logSegmentCacheElement.buf)) - logSegmentCacheElementBufStartingPosition

						if logSegmentCacheElementBufRemainingLen <= readPlanStepRemainingLength {
							logSegmentCacheElementBufSelectedLen = logSegmentCacheElementBufRemainingLen
						} else {
							logSegmentCacheElementBufSelectedLen = readPlanStepRemainingLength
						}

						logSegmentCacheElementBufEndingPosition = logSegmentCacheElementBufStartingPosition + logSegmentCacheElementBufSelectedLen

						readOut.Data = append(readOut.Data, logSegmentCacheElement.buf[logSegmentCacheElementBufStartingPosition:logSegmentCacheElementBufEndingPosition]...)

						curObjectOffset += logSegmentCacheElementBufSelectedLen
						readPlanStepRemainingLength -= logSegmentCacheElementBufSelectedLen
					}
				}
			case *singleObjectExtentWithLinkStruct:
				readPlanStepAsSingleObjectExtentWithLink = readPlanStepAsInterface.(*singleObjectExtentWithLinkStruct)

				if nil == readPlanStepAsSingleObjectExtentWithLink.chunkedPutContext {
					// Zero-fill for readPlanStep.length

					readOut.Data = append(readOut.Data, make([]byte, readPlanStepAsSingleObjectExtentWithLink.length)...)
				} else {
					// Fetch LogSegment data... from readPlanStepAsSingleObjectExtentWithLink.chunkedPutContextStruct

					_ = atomic.AddUint64(&globals.metrics.LogSegmentPUTReadHits, 1)

					readOut.Data = append(readOut.Data, readPlanStepAsSingleObjectExtentWithLink.chunkedPutContext.buf[readPlanStepAsSingleObjectExtentWithLink.objectOffset:readPlanStepAsSingleObjectExtentWithLink.objectOffset+readPlanStepAsSingleObjectExtentWithLink.length]...)
				}
			default:
				logFatalf("getReadPlan() returned an invalid readPlanStep: %v", readPlanStepAsInterface)
			}
		}
	}

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoRead_bytes, uint64(len(readOut.Data)))

	errno = 0
	return
}

func (dummy *globalsStruct) DoWrite(inHeader *fission.InHeader, writeIn *fission.WriteIn) (writeOut *fission.WriteOut, errno syscall.Errno) {
	var (
		chunkedPutContext        *chunkedPutContextStruct
		chunkedPutContextElement *list.Element
		fhInodeNumber            uint64
		fileInode                *fileInodeStruct
		grantedLock              *fileInodeLockRequestStruct
		ok                       bool
		singleObjectExtent       *singleObjectExtentStruct
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoWrite_calls, 1)

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[writeIn.FH]
	if !ok {
		logFatalf("DoWrite(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, writeIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoWrite(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, writeIn.FH, fhInodeNumber)
	}

	globals.Unlock()

	fileInode = referenceFileInode(inode.InodeNumber(inHeader.NodeID))
	if nil == fileInode {
		logFatalf("DoWrite(NodeID=%v,FH=%v) called for non-FileInode", inHeader.NodeID, writeIn.FH)
	}

	// Grab quota to start a fresh chunkedPutContext before calling fileInode.getExclusiveLock()
	// since, in the pathologic case where only fileInode has any outstanding chunkedPutContext's,
	// they can only be complete()'d while holding fileInode.getExclusiveLock() and we would
	// deadlock.

	_ = <-globals.fileInodeDirtyLogSegmentChan

	grantedLock = fileInode.getExclusiveLock()

	if 0 == fileInode.chunkedPutList.Len() {
		// No chunkedPutContext is present (so none can be open), so open one

		_ = atomic.AddUint64(&globals.metrics.LogSegmentPUTs, 1)

		chunkedPutContext = &chunkedPutContextStruct{
			buf:            make([]byte, 0),
			fileInode:      fileInode,
			state:          chunkedPutContextStateOpen,
			sendChan:       make(chan struct{}, 1),
			wakeChan:       make(chan struct{}, 1),
			inRead:         false,
			flushRequested: false,
		}

		chunkedPutContext.extentMap = sortedmap.NewLLRBTree(sortedmap.CompareUint64, chunkedPutContext)
		chunkedPutContext.chunkedPutListElement = fileInode.chunkedPutList.PushBack(chunkedPutContext)

		fileInode.reference()

		pruneFileInodeDirtyListIfNecessary()

		globals.Lock()
		fileInode.dirtyListElement = globals.fileInodeDirtyList.PushBack(fileInode)
		globals.Unlock()

		go chunkedPutContext.sendDaemon()
	} else {
		globals.Lock()
		globals.fileInodeDirtyList.MoveToBack(fileInode.dirtyListElement)
		globals.Unlock()

		chunkedPutContextElement = fileInode.chunkedPutList.Back()
		chunkedPutContext = chunkedPutContextElement.Value.(*chunkedPutContextStruct)

		if chunkedPutContextStateOpen == chunkedPutContext.state {
			// Use this most recent (and open) chunkedPutContext... so we can give back our chunkedPutContext quota

			globals.fileInodeDirtyLogSegmentChan <- struct{}{}
		} else {
			// Most recent chunkedPutContext is closed, so open a new one

			_ = atomic.AddUint64(&globals.metrics.LogSegmentPUTs, 1)

			chunkedPutContext = &chunkedPutContextStruct{
				buf:            make([]byte, 0),
				fileInode:      fileInode,
				state:          chunkedPutContextStateOpen,
				sendChan:       make(chan struct{}, 1),
				wakeChan:       make(chan struct{}, 1),
				inRead:         false,
				flushRequested: false,
			}

			chunkedPutContext.extentMap = sortedmap.NewLLRBTree(sortedmap.CompareUint64, chunkedPutContext)
			chunkedPutContext.chunkedPutListElement = fileInode.chunkedPutList.PushBack(chunkedPutContext)

			fileInode.reference()

			go chunkedPutContext.sendDaemon()
		}
	}

	singleObjectExtent = &singleObjectExtentStruct{
		fileOffset:   uint64(writeIn.Offset),
		objectOffset: uint64(len(chunkedPutContext.buf)),
		length:       uint64(len(writeIn.Data)),
	}

	chunkedPutContext.mergeSingleObjectExtent(singleObjectExtent)

	if (singleObjectExtent.fileOffset + singleObjectExtent.length) > fileInode.cachedStat.Size {
		fileInode.cachedStat.Size = singleObjectExtent.fileOffset + singleObjectExtent.length
	}

	chunkedPutContext.buf = append(chunkedPutContext.buf, writeIn.Data...)

	select {
	case chunkedPutContext.sendChan <- struct{}{}:
		// We just notified sendDaemon()
	default:
		// We didn't need to notify sendDaemon()
	}

	if uint64(len(chunkedPutContext.buf)) >= globals.config.MaxFlushSize {
		// Time to do a Flush
		chunkedPutContext.state = chunkedPutContextStateClosing
		close(chunkedPutContext.sendChan)
	}

	grantedLock.release()

	fileInode.dereference()

	writeOut = &fission.WriteOut{
		Size:    uint32(len(writeIn.Data)),
		Padding: 0,
	}

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoWrite_bytes, uint64(writeOut.Size))
	globals.stats.FUSEDoWriteBytes.Add(uint64(writeOut.Size))

	errno = 0
	return
}

func (dummy *globalsStruct) DoStatFS(inHeader *fission.InHeader) (statFSOut *fission.StatFSOut, errno syscall.Errno) {
	var (
		err            error
		statVFSRequest *jrpcfs.StatVFSRequest
		statVFSReply   *jrpcfs.StatVFS
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoStatFS_calls, 1)

	statVFSRequest = &jrpcfs.StatVFSRequest{
		MountID: globals.mountID,
	}

	statVFSReply = &jrpcfs.StatVFS{}

	err = globals.retryRPCClient.Send("RpcStatVFS", statVFSRequest, statVFSReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	statFSOut = &fission.StatFSOut{
		KStatFS: fission.KStatFS{
			Blocks:  statVFSReply.TotalBlocks,
			BFree:   statVFSReply.FreeBlocks,
			BAvail:  statVFSReply.AvailBlocks,
			Files:   statVFSReply.TotalInodes,
			FFree:   statVFSReply.FreeInodes,
			BSize:   uint32(statVFSReply.BlockSize),
			NameLen: uint32(statVFSReply.MaxFilenameLen),
			FRSize:  uint32(statVFSReply.FragmentSize),
			Padding: 0,
			Spare:   [6]uint32{0, 0, 0, 0, 0, 0},
		},
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoRelease(inHeader *fission.InHeader, releaseIn *fission.ReleaseIn) (errno syscall.Errno) {
	var (
		fhInodeNumber uint64
		fhSet         fhSetType
		ok            bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoRelease_calls, 1)

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[releaseIn.FH]
	if !ok {
		logFatalf("DoRelease(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, releaseIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoRelease(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, releaseIn.FH, fhInodeNumber)
	}

	delete(globals.fhToInodeNumberMap, releaseIn.FH)

	fhSet, ok = globals.inodeNumberToFHMap[inHeader.NodeID]
	if !ok {
		logFatalf("DoRelease(NodeID=%v,FH=%v) called for unknown NodeID", inHeader.NodeID, releaseIn.FH)
	}

	_, ok = fhSet[releaseIn.FH]
	if !ok {
		logFatalf("DoRelease(NodeID=%v,FH=%v) called for FH missing from fhSet: %v", inHeader.NodeID, releaseIn.FH, fhSet)
	}

	delete(fhSet, releaseIn.FH)

	if 0 == len(fhSet) {
		delete(globals.inodeNumberToFHMap, inHeader.NodeID)
	} else {
		globals.inodeNumberToFHMap[inHeader.NodeID] = fhSet
	}

	globals.Unlock()

	errno = 0
	return
}

func (dummy *globalsStruct) DoFSync(inHeader *fission.InHeader, fSyncIn *fission.FSyncIn) (errno syscall.Errno) {
	var (
		fhInodeNumber uint64
		fileInode     *fileInodeStruct
		ok            bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoFSync_calls, 1)

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[fSyncIn.FH]
	if !ok {
		logFatalf("DoFSync(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, fSyncIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoFSync(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, fSyncIn.FH, fhInodeNumber)
	}

	globals.Unlock()

	fileInode = referenceFileInode(inode.InodeNumber(inHeader.NodeID))
	if nil == fileInode {
		logFatalf("DoFSync(NodeID=%v,FH=%v) called for non-FileInode", inHeader.NodeID, fSyncIn.FH)
	}

	fileInode.doFlushIfNecessary()

	fileInode.dereference()

	errno = 0
	return
}

func (dummy *globalsStruct) DoSetXAttr(inHeader *fission.InHeader, setXAttrIn *fission.SetXAttrIn) (errno syscall.Errno) {
	var (
		err             error
		setXAttrReply   *jrpcfs.Reply
		setXAttrRequest *jrpcfs.SetXAttrRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoSetXAttr_calls, 1)

	if !globals.config.XAttrEnabled {
		errno = syscall.ENOSYS
		return
	}

	setXAttrRequest = &jrpcfs.SetXAttrRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		AttrName:  string(setXAttrIn.Name[:]),
		AttrValue: setXAttrIn.Data[:],
		AttrFlags: fs.SetXAttrCreateOrReplace,
	}

	setXAttrReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcSetXAttr", setXAttrRequest, setXAttrReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoSetXAttr_bytes, uint64(len(setXAttrIn.Data)))

	errno = 0
	return
}

func (dummy *globalsStruct) DoGetXAttr(inHeader *fission.InHeader, getXAttrIn *fission.GetXAttrIn) (getXAttrOut *fission.GetXAttrOut, errno syscall.Errno) {
	var (
		err             error
		getXAttrReply   *jrpcfs.GetXAttrReply
		getXAttrRequest *jrpcfs.GetXAttrRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoGetXAttr_calls, 1)

	if !globals.config.XAttrEnabled {
		errno = syscall.ENOSYS
		return
	}

	getXAttrRequest = &jrpcfs.GetXAttrRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		AttrName: string(getXAttrIn.Name[:]),
	}

	getXAttrReply = &jrpcfs.GetXAttrReply{}

	err = globals.retryRPCClient.Send("RpcGetXAttr", getXAttrRequest, getXAttrReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	if 0 == getXAttrIn.Size {
		getXAttrOut = &fission.GetXAttrOut{
			Size:    uint32(len(getXAttrReply.AttrValue)),
			Padding: 0,
			Data:    make([]byte, 0),
		}
		errno = 0
		return
	}

	if uint32(len(getXAttrReply.AttrValue)) > getXAttrIn.Size {
		errno = syscall.ERANGE
		return
	}

	getXAttrOut = &fission.GetXAttrOut{
		Size:    uint32(len(getXAttrReply.AttrValue)),
		Padding: 0,
		Data:    getXAttrReply.AttrValue,
	}

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoGetXAttr_bytes, uint64(getXAttrOut.Size))

	errno = 0
	return
}

func (dummy *globalsStruct) DoListXAttr(inHeader *fission.InHeader, listXAttrIn *fission.ListXAttrIn) (listXAttrOut *fission.ListXAttrOut, errno syscall.Errno) {
	var (
		err              error
		listXAttrReply   *jrpcfs.ListXAttrReply
		listXAttrRequest *jrpcfs.ListXAttrRequest
		totalSize        uint32
		xAttrIndex       int
		xAttrName        string
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoListXAttr_calls, 1)

	if !globals.config.XAttrEnabled {
		errno = syscall.ENOSYS
		return
	}

	listXAttrRequest = &jrpcfs.ListXAttrRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
	}

	listXAttrReply = &jrpcfs.ListXAttrReply{}

	err = globals.retryRPCClient.Send("RpcListXAttr", listXAttrRequest, listXAttrReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	totalSize = 0

	for _, xAttrName = range listXAttrReply.AttrNames {
		totalSize += uint32(len(xAttrName) + 1)
	}

	if 0 == listXAttrIn.Size {
		listXAttrOut = &fission.ListXAttrOut{
			Size:    totalSize,
			Padding: 0,
			Name:    make([][]byte, 0),
		}
		errno = 0
		return
	}

	listXAttrOut = &fission.ListXAttrOut{
		Size:    totalSize, // unnecessary
		Padding: 0,
		Name:    make([][]byte, len(listXAttrReply.AttrNames)),
	}

	for xAttrIndex, xAttrName = range listXAttrReply.AttrNames {
		listXAttrOut.Name[xAttrIndex] = []byte(xAttrName)
	}

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoListXAttr_names, uint64(len(listXAttrOut.Name)))

	errno = 0
	return
}

func (dummy *globalsStruct) DoRemoveXAttr(inHeader *fission.InHeader, removeXAttrIn *fission.RemoveXAttrIn) (errno syscall.Errno) {
	var (
		err                error
		removeXAttrReply   *jrpcfs.Reply
		removeXAttrRequest *jrpcfs.RemoveXAttrRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoRemoveXAttr_calls, 1)

	if !globals.config.XAttrEnabled {
		errno = syscall.ENOSYS
		return
	}

	removeXAttrRequest = &jrpcfs.RemoveXAttrRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		AttrName: string(removeXAttrIn.Name[:]),
	}

	removeXAttrReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcRemoveXAttr", removeXAttrRequest, removeXAttrReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoFlush(inHeader *fission.InHeader, flushIn *fission.FlushIn) (errno syscall.Errno) {
	var (
		fhInodeNumber uint64
		ok            bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoFlush_calls, 1)

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[flushIn.FH]
	if !ok {
		logFatalf("DoFlush(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, flushIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoFlush(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, flushIn.FH, fhInodeNumber)
	}

	globals.Unlock()

	errno = 0
	return
}

func (dummy *globalsStruct) DoInit(inHeader *fission.InHeader, initIn *fission.InitIn) (initOut *fission.InitOut, errno syscall.Errno) {
	var (
		initOutFlags uint32
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoInit_calls, 1)

	if globals.config.ReadDirPlusEnabled {
		initOutFlags = initOutFlagsMaskReadDirPlusEnabled
	} else {
		initOutFlags = initOutFlagsMaskReadDirPlusDisabled
	}

	initOut = &fission.InitOut{
		Major:                initIn.Major,
		Minor:                initIn.Minor,
		MaxReadAhead:         initIn.MaxReadAhead,
		Flags:                initOutFlags,
		MaxBackground:        globals.config.FUSEMaxBackground,
		CongestionThreshhold: globals.config.FUSECongestionThreshhold,
		MaxWrite:             globals.config.FUSEMaxWrite,
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoOpenDir(inHeader *fission.InHeader, openDirIn *fission.OpenDirIn) (openDirOut *fission.OpenDirOut, errno syscall.Errno) {
	var (
		err            error
		fhSet          fhSetType
		getStatReply   *jrpcfs.StatStruct
		getStatRequest *jrpcfs.GetStatRequest
		ok             bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoOpenDir_calls, 1)

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
	}

	getStatReply = &jrpcfs.StatStruct{}

	err = globals.retryRPCClient.Send("RpcGetStat", getStatRequest, getStatReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	if syscall.S_IFDIR != (getStatReply.FileMode & syscall.S_IFMT) {
		errno = syscall.ENOTDIR
		return
	}

	globals.Lock()

	globals.lastFH++

	globals.fhToInodeNumberMap[globals.lastFH] = inHeader.NodeID

	fhSet, ok = globals.inodeNumberToFHMap[inHeader.NodeID]
	if !ok {
		fhSet = make(fhSetType)
	}
	fhSet[globals.lastFH] = struct{}{}
	globals.inodeNumberToFHMap[inHeader.NodeID] = fhSet

	openDirOut = &fission.OpenDirOut{
		FH:        globals.lastFH,
		OpenFlags: 0,
		Padding:   0,
	}

	globals.Unlock()

	errno = 0
	return
}

func (dummy *globalsStruct) DoReadDir(inHeader *fission.InHeader, readDirIn *fission.ReadDirIn) (readDirOut *fission.ReadDirOut, errno syscall.Errno) {
	var (
		curSize              uint32
		dirEntIndex          uint64
		dirEntNameLenAligned uint32
		dirEnt               fission.DirEnt
		dirEntSize           uint32
		dirEntry             *jrpcfs.DirEntry
		err                  error
		fhInodeNumber        uint64
		maxEntries           uint64
		numEntries           uint64
		ok                   bool
		readdirByLocRequest  *jrpcfs.ReaddirByLocRequest
		readdirReply         *jrpcfs.ReaddirReply
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoReadDir_calls, 1)

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[readDirIn.FH]
	if !ok {
		logFatalf("DoReadDir(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, readDirIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoReadDir(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, readDirIn.FH, fhInodeNumber)
	}

	globals.Unlock()

	maxEntries = (uint64(readDirIn.Size) + fission.DirEntFixedPortionSize + 1 - 1) / (fission.DirEntFixedPortionSize + 1)

	readdirByLocRequest = &jrpcfs.ReaddirByLocRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		MaxEntries:         maxEntries,
		PrevDirEntLocation: int64(readDirIn.Offset) - 1,
	}

	readdirReply = &jrpcfs.ReaddirReply{}

	err = globals.retryRPCClient.Send("RpcReaddirByLoc", readdirByLocRequest, readdirReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	numEntries = uint64(len(readdirReply.DirEnts))

	readDirOut = &fission.ReadDirOut{
		DirEnt: make([]fission.DirEnt, 0, numEntries),
	}

	curSize = 0

	for dirEntIndex = 0; dirEntIndex < numEntries; dirEntIndex++ {
		dirEntry = &readdirReply.DirEnts[dirEntIndex]

		dirEntNameLenAligned = (uint32(len(dirEntry.Basename)) + (fission.DirEntAlignment - 1)) & ^uint32(fission.DirEntAlignment-1)
		dirEntSize = fission.DirEntFixedPortionSize + dirEntNameLenAligned

		if (curSize + dirEntSize) > readDirIn.Size {
			break
		}

		dirEnt = fission.DirEnt{
			Ino:     uint64(dirEntry.InodeNumber),
			Off:     uint64(dirEntry.NextDirLocation),
			NameLen: uint32(len(dirEntry.Basename)), // unnecessary
			Type:    uint32(dirEntry.FileType),
			Name:    []byte(dirEntry.Basename),
		}

		readDirOut.DirEnt = append(readDirOut.DirEnt, dirEnt)

		curSize += dirEntSize
	}

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoReadDir_entries, uint64(len(readDirOut.DirEnt)))

	errno = 0
	return
}

func (dummy *globalsStruct) DoReleaseDir(inHeader *fission.InHeader, releaseDirIn *fission.ReleaseDirIn) (errno syscall.Errno) {
	var (
		fhInodeNumber uint64
		fhSet         fhSetType
		ok            bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoReleaseDir_calls, 1)

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[releaseDirIn.FH]
	if !ok {
		logFatalf("DoReleaseDir(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, releaseDirIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoReleaseDir(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, releaseDirIn.FH, fhInodeNumber)
	}

	delete(globals.fhToInodeNumberMap, releaseDirIn.FH)

	fhSet, ok = globals.inodeNumberToFHMap[inHeader.NodeID]
	if !ok {
		logFatalf("DoReleaseDir(NodeID=%v,FH=%v) called for unknown NodeID", inHeader.NodeID, releaseDirIn.FH)
	}

	_, ok = fhSet[releaseDirIn.FH]
	if !ok {
		logFatalf("DoReleaseDir(NodeID=%v,FH=%v) called for FH missing from fhSet: %v", inHeader.NodeID, releaseDirIn.FH, fhSet)
	}

	delete(fhSet, releaseDirIn.FH)

	if 0 == len(fhSet) {
		delete(globals.inodeNumberToFHMap, inHeader.NodeID)
	} else {
		globals.inodeNumberToFHMap[inHeader.NodeID] = fhSet
	}

	globals.Unlock()

	errno = 0
	return
}

func (dummy *globalsStruct) DoFSyncDir(inHeader *fission.InHeader, fSyncDirIn *fission.FSyncDirIn) (errno syscall.Errno) {
	var (
		fhInodeNumber uint64
		ok            bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoFSyncDir_calls, 1)

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[fSyncDirIn.FH]
	if !ok {
		logFatalf("DoFSync(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, fSyncDirIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoFSync(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, fSyncDirIn.FH, fhInodeNumber)
	}

	globals.Unlock()

	errno = 0
	return
}

func (dummy *globalsStruct) DoGetLK(inHeader *fission.InHeader, getLKIn *fission.GetLKIn) (getLKOut *fission.GetLKOut, errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoGetLK_calls, 1)
	errno = syscall.ENOSYS
	return
}
func (dummy *globalsStruct) DoSetLK(inHeader *fission.InHeader, setLKIn *fission.SetLKIn) (errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoSetLK_calls, 1)
	errno = syscall.ENOSYS
	return
}
func (dummy *globalsStruct) DoSetLKW(inHeader *fission.InHeader, setLKWIn *fission.SetLKWIn) (errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoSetLKW_calls, 1)
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoAccess(inHeader *fission.InHeader, accessIn *fission.AccessIn) (errno syscall.Errno) {
	var (
		err           error
		accessReply   *jrpcfs.Reply
		accessRequest *jrpcfs.AccessRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoAccess_calls, 1)

	accessRequest = &jrpcfs.AccessRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		UserID:     int32(inHeader.UID),
		GroupID:    int32(inHeader.GID),
		AccessMode: accessIn.Mask,
	}

	accessReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcAccess", accessRequest, accessReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoCreate(inHeader *fission.InHeader, createIn *fission.CreateIn) (createOut *fission.CreateOut, errno syscall.Errno) {
	var (
		aTimeNSec      uint32
		aTimeSec       uint64
		cTimeNSec      uint32
		cTimeSec       uint64
		createReply    *jrpcfs.InodeReply
		createRequest  *jrpcfs.CreateRequest
		err            error
		fhSet          fhSetType
		getStatReply   *jrpcfs.StatStruct
		getStatRequest *jrpcfs.GetStatRequest
		mTimeNSec      uint32
		mTimeSec       uint64
		ok             bool
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoCreate_calls, 1)

	createRequest = &jrpcfs.CreateRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		Basename: string(createIn.Name[:]),
		UserID:   int32(inHeader.UID),
		GroupID:  int32(inHeader.GID),
		FileMode: createIn.Mode & uint32(os.ModePerm),
	}

	createReply = &jrpcfs.InodeReply{}

	err = globals.retryRPCClient.Send("RpcCreate", createRequest, createReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: createReply.InodeNumber,
		},
	}

	getStatReply = &jrpcfs.StatStruct{}

	err = globals.retryRPCClient.Send("RpcGetStat", getStatRequest, getStatReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	aTimeSec, aTimeNSec = nsToUnixTime(getStatReply.ATimeNs)
	mTimeSec, mTimeNSec = nsToUnixTime(getStatReply.MTimeNs)
	cTimeSec, cTimeNSec = nsToUnixTime(getStatReply.CTimeNs)

	globals.Lock()

	globals.lastFH++

	globals.fhToInodeNumberMap[globals.lastFH] = uint64(createReply.InodeNumber)

	fhSet, ok = globals.inodeNumberToFHMap[uint64(createReply.InodeNumber)]
	if !ok {
		fhSet = make(fhSetType)
	}
	fhSet[globals.lastFH] = struct{}{}
	globals.inodeNumberToFHMap[uint64(createReply.InodeNumber)] = fhSet

	createOut = &fission.CreateOut{
		EntryOut: fission.EntryOut{
			NodeID:         uint64(createReply.InodeNumber),
			Generation:     0,
			EntryValidSec:  globals.entryValidSec,
			AttrValidSec:   globals.attrValidSec,
			EntryValidNSec: globals.entryValidNSec,
			AttrValidNSec:  globals.attrValidNSec,
			Attr: fission.Attr{
				Ino:       uint64(createReply.InodeNumber),
				Size:      getStatReply.Size,
				Blocks:    0, // fixAttrSizes() will compute this
				ATimeSec:  aTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  cTimeSec,
				ATimeNSec: aTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: cTimeNSec,
				Mode:      getStatReply.FileMode,
				NLink:     uint32(getStatReply.NumLinks),
				UID:       getStatReply.UserID,
				GID:       getStatReply.GroupID,
				RDev:      0,
				BlkSize:   0, // fixAttrSizes() will set this
				Padding:   0,
			},
		},
		FH:        globals.lastFH,
		OpenFlags: fission.FOpenResponseDirectIO,
		Padding:   0,
	}

	globals.Unlock()

	fixAttrSizes(&createOut.Attr)

	errno = 0
	return
}

func (dummy *globalsStruct) DoInterrupt(inHeader *fission.InHeader, interruptIn *fission.InterruptIn) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoInterrupt_calls, 1)
	return
}

func (dummy *globalsStruct) DoBMap(inHeader *fission.InHeader, bMapIn *fission.BMapIn) (bMapOut *fission.BMapOut, errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoBMap_calls, 1)
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoDestroy(inHeader *fission.InHeader) (errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoDestroy_calls, 1)
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoPoll(inHeader *fission.InHeader, pollIn *fission.PollIn) (pollOut *fission.PollOut, errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoPoll_calls, 1)
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoBatchForget(inHeader *fission.InHeader, batchForgetIn *fission.BatchForgetIn) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoBatchForget_calls, 1)
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoBatchForget_nodes, uint64(len(batchForgetIn.Forget)))
	return
}

func (dummy *globalsStruct) DoFAllocate(inHeader *fission.InHeader, fAllocateIn *fission.FAllocateIn) (errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoFAllocate_calls, 1)
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReadDirPlus(inHeader *fission.InHeader, readDirPlusIn *fission.ReadDirPlusIn) (readDirPlusOut *fission.ReadDirPlusOut, errno syscall.Errno) {
	var (
		aTimeNSec               uint32
		aTimeSec                uint64
		cTimeNSec               uint32
		cTimeSec                uint64
		curSize                 uint32
		dirEntIndex             uint64
		dirEntNameLenAligned    uint32
		dirEntPlus              fission.DirEntPlus
		dirEntPlusSize          uint32
		dirEntry                *jrpcfs.DirEntry
		err                     error
		fhInodeNumber           uint64
		mTimeNSec               uint32
		mTimeSec                uint64
		maxEntries              uint64
		numEntries              uint64
		ok                      bool
		readdirPlusByLocRequest *jrpcfs.ReaddirPlusByLocRequest
		readdirPlusReply        *jrpcfs.ReaddirPlusReply
		statStruct              *jrpcfs.StatStruct
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoReadDirPlus_calls, 1)

	globals.Lock()

	fhInodeNumber, ok = globals.fhToInodeNumberMap[readDirPlusIn.FH]
	if !ok {
		logFatalf("DoReadDirPlus(NodeID=%v,FH=%v) called for unknown FH", inHeader.NodeID, readDirPlusIn.FH)
	}
	if fhInodeNumber != inHeader.NodeID {
		logFatalf("DoReadDirPlus(NodeID=%v,FH=%v) called for FH associated with NodeID=%v", inHeader.NodeID, readDirPlusIn.FH, fhInodeNumber)
	}

	globals.Unlock()

	maxEntries = (uint64(readDirPlusIn.Size) + fission.DirEntPlusFixedPortionSize + 1 - 1) / (fission.DirEntPlusFixedPortionSize + 1)

	readdirPlusByLocRequest = &jrpcfs.ReaddirPlusByLocRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(inHeader.NodeID),
		},
		MaxEntries:         maxEntries,
		PrevDirEntLocation: int64(readDirPlusIn.Offset) - 1,
	}

	readdirPlusReply = &jrpcfs.ReaddirPlusReply{}

	err = globals.retryRPCClient.Send("RpcReaddirPlusByLoc", readdirPlusByLocRequest, readdirPlusReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	numEntries = uint64(len(readdirPlusReply.DirEnts))
	if numEntries != uint64(len(readdirPlusReply.StatEnts)) {
		logFatalf("DoReadDirPlus(NodeID=%v,FH=%v) fetched mismatched number of DirEnts (%v) & StatEnts (%v)", inHeader.NodeID, readDirPlusIn.FH, len(readdirPlusReply.DirEnts), len(readdirPlusReply.StatEnts))
	}

	readDirPlusOut = &fission.ReadDirPlusOut{
		DirEntPlus: make([]fission.DirEntPlus, 0, numEntries),
	}

	curSize = 0

	for dirEntIndex = 0; dirEntIndex < numEntries; dirEntIndex++ {
		dirEntry = &readdirPlusReply.DirEnts[dirEntIndex]
		statStruct = &readdirPlusReply.StatEnts[dirEntIndex]

		dirEntNameLenAligned = (uint32(len(dirEntry.Basename)) + (fission.DirEntAlignment - 1)) & ^uint32(fission.DirEntAlignment-1)
		dirEntPlusSize = fission.DirEntPlusFixedPortionSize + dirEntNameLenAligned

		if (curSize + dirEntPlusSize) > readDirPlusIn.Size {
			break
		}

		aTimeSec, aTimeNSec = nsToUnixTime(statStruct.ATimeNs)
		mTimeSec, mTimeNSec = nsToUnixTime(statStruct.MTimeNs)
		cTimeSec, cTimeNSec = nsToUnixTime(statStruct.CTimeNs)

		dirEntPlus = fission.DirEntPlus{
			EntryOut: fission.EntryOut{
				NodeID:         uint64(dirEntry.InodeNumber),
				Generation:     0,
				EntryValidSec:  globals.entryValidSec,
				AttrValidSec:   globals.attrValidSec,
				EntryValidNSec: globals.entryValidNSec,
				AttrValidNSec:  globals.attrValidNSec,
				Attr: fission.Attr{
					Ino:       uint64(dirEntry.InodeNumber),
					Size:      statStruct.Size,
					Blocks:    0, // fixAttrSizes() will compute this
					ATimeSec:  aTimeSec,
					MTimeSec:  mTimeSec,
					CTimeSec:  cTimeSec,
					ATimeNSec: aTimeNSec,
					MTimeNSec: mTimeNSec,
					CTimeNSec: cTimeNSec,
					Mode:      statStruct.FileMode,
					NLink:     uint32(statStruct.NumLinks),
					UID:       statStruct.UserID,
					GID:       statStruct.GroupID,
					RDev:      0,
					BlkSize:   0, // fixAttrSizes() will set this
					Padding:   0,
				},
			},
			DirEnt: fission.DirEnt{
				Ino:     uint64(dirEntry.InodeNumber),
				Off:     uint64(dirEntry.NextDirLocation),
				NameLen: uint32(len(dirEntry.Basename)), // unnecessary
				Type:    uint32(dirEntry.FileType),
				Name:    []byte(dirEntry.Basename),
			},
		}

		fixAttrSizes(&dirEntPlus.EntryOut.Attr)

		readDirPlusOut.DirEntPlus = append(readDirPlusOut.DirEntPlus, dirEntPlus)

		curSize += dirEntPlusSize
	}

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoReadDirPlus_entries, uint64(len(readDirPlusOut.DirEntPlus)))

	errno = 0
	return
}

func (dummy *globalsStruct) DoRename2(inHeader *fission.InHeader, rename2In *fission.Rename2In) (errno syscall.Errno) {
	var (
		err           error
		renameReply   *jrpcfs.Reply
		renameRequest *jrpcfs.RenameRequest
	)

	_ = atomic.AddUint64(&globals.metrics.FUSE_DoRename2_calls, 1)

	// TODO: Remove this once Lease Management makes this unnecessary
	doFlushIfNecessary(inode.InodeNumber(rename2In.NewDir), rename2In.NewName)

	renameRequest = &jrpcfs.RenameRequest{
		MountID:           globals.mountID,
		SrcDirInodeNumber: int64(inHeader.NodeID),
		SrcBasename:       string(rename2In.OldName[:]),
		DstDirInodeNumber: int64(rename2In.NewDir),
		DstBasename:       string(rename2In.NewName[:]),
	}

	renameReply = &jrpcfs.Reply{}

	err = globals.retryRPCClient.Send("RpcRename", renameRequest, renameReply)
	if nil != err {
		errno = convertErrToErrno(err, syscall.EIO)
		return
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoLSeek(inHeader *fission.InHeader, lSeekIn *fission.LSeekIn) (lSeekOut *fission.LSeekOut, errno syscall.Errno) {
	_ = atomic.AddUint64(&globals.metrics.FUSE_DoLSeek_calls, 1)
	errno = syscall.ENOSYS
	return
}
