// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"syscall"
	"time"

	"github.com/NVIDIA/fission"
	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/imgr/imgrpkg"
)

const (
	attrBlockSize = uint32(512)
	attrRDev      = uint32(0)

	fuseSubtype = "ProxyFS"

	initOutFlags = uint32(0) |
		fission.InitFlagsAsyncRead |
		fission.InitFlagsFileOps |
		fission.InitFlagsAtomicOTrunc |
		fission.InitFlagsBigWrites |
		fission.InitFlagsAutoInvalData |
		fission.InitFlagsDoReadDirPlus |
		fission.InitFlagsReaddirplusAuto |
		fission.InitFlagsParallelDirops |
		fission.InitFlagsMaxPages |
		fission.InitFlagsExplicitInvalData
)

func performMountFUSE() (err error) {
	globals.fissionVolume = fission.NewVolume(
		globals.config.VolumeName,
		globals.config.MountPointDirPath,
		fuseSubtype,
		globals.config.FUSEMaxWrite,
		globals.config.FUSEAllowOther,
		&globals,
		newLogger(),
		globals.fissionErrChan,
	)

	globals.fuseEntryValidDurationSec, globals.fuseEntryValidDurationNSec = nsToUnixTime(uint64(globals.config.FUSEEntryValidDuration))
	globals.fuseAttrValidDurationSec, globals.fuseAttrValidDurationNSec = nsToUnixTime(uint64(globals.config.FUSEAttrValidDuration))

	err = globals.fissionVolume.DoMount()

	return
}

func performUnmountFUSE() (err error) {
	err = globals.fissionVolume.DoUnmount()

	globals.fissionVolume = nil

	globals.fuseEntryValidDurationSec, globals.fuseEntryValidDurationNSec = 0, 0
	globals.fuseAttrValidDurationSec, globals.fuseAttrValidDurationNSec = 0, 0

	return
}

func (dummy *globalsStruct) DoLookup(inHeader *fission.InHeader, lookupIn *fission.LookupIn) (lookupOut *fission.LookupOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoLookup(inHeader: %+v, lookupIn: %+v)", inHeader, lookupIn)
	defer func() {
		logTracef("<== DoLookup(lookupOut: %+v, errno: %v)", lookupOut, errno)
	}()

	defer func() {
		globals.stats.DoLookupUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	lookupOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoForget(inHeader *fission.InHeader, forgetIn *fission.ForgetIn) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoForget(inHeader: %+v, forgetIn: %+v)", inHeader, forgetIn)
	defer func() {
		logTracef("<== DoForget()")
	}()

	defer func() {
		globals.stats.DoForgetUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	return
}

func (dummy *globalsStruct) DoGetAttr(inHeader *fission.InHeader, getAttrIn *fission.GetAttrIn) (getAttrOut *fission.GetAttrOut, errno syscall.Errno) {
	var (
		err                        error
		getInodeTableEntryRequest  *imgrpkg.GetInodeTableEntryRequestStruct
		getInodeTableEntryResponse *imgrpkg.GetInodeTableEntryResponseStruct
		inode                      *inodeStruct
		inodeHeadV1Buf             []byte
		inodeLockRequest           *inodeLockRequestStruct
		modificationTimeNSec       uint32
		modificationTimeSec        uint64
		startTime                  time.Time = time.Now()
		statusChangeTimeNSec       uint32
		statusChangeTimeSec        uint64
	)

	logTracef("==> DoGetAttr(inHeader: %+v, getAttrIn: %+v)", inHeader, getAttrIn)
	defer func() {
		logTracef("<== DoGetAttr(getAttrOut: %+v, errno: %v)", getAttrOut, errno)
	}()

	defer func() {
		globals.stats.DoGetAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

Retry:
	inodeLockRequest = newLockRequest()
	inodeLockRequest.inodeNumber = uint64(inHeader.NodeID)
	inodeLockRequest.exclusive = false
	inodeLockRequest.addThisLock()
	if len(inodeLockRequest.locksHeld) == 0 {
		goto Retry
	}

	inode = lookupInode(uint64(inHeader.NodeID))
	if nil == inode {
		inodeLockRequest.unlockAll()
		getAttrOut = nil
		errno = syscall.ENOENT
		return
	}

	if nil == inode.inodeHeadV1 {
		getInodeTableEntryRequest = &imgrpkg.GetInodeTableEntryRequestStruct{
			MountID:     globals.mountID,
			InodeNumber: uint64(inHeader.NodeID),
		}
		getInodeTableEntryResponse = &imgrpkg.GetInodeTableEntryResponseStruct{}

		err = rpcGetInodeTableEntry(getInodeTableEntryRequest, getInodeTableEntryResponse)
		if nil != err {
			inodeLockRequest.unlockAll()
			getAttrOut = nil
			errno = syscall.ENOENT
			return
		}

		inodeHeadV1Buf, err = objectGETTail(getInodeTableEntryResponse.InodeHeadObjectNumber, getInodeTableEntryResponse.InodeHeadLength)
		if nil != err {
			logFatalf("objectGETTail(getInodeTableEntryResponse.InodeHeadObjectNumber: %v, getInodeTableEntryResponse.InodeHeadLength: %v) failed: %v", getInodeTableEntryResponse.InodeHeadObjectNumber, getInodeTableEntryResponse.InodeHeadLength, err)
		}

		inode.inodeHeadV1, err = ilayout.UnmarshalInodeHeadV1(inodeHeadV1Buf)
		if nil != err {
			logFatalf("ilayout.UnmarshalInodeHeadV1(inodeHeadV1Buf) failed: %v", err)
		}
	}

	modificationTimeSec, modificationTimeNSec = nsToUnixTime(uint64(inode.inodeHeadV1.ModificationTime.UnixNano()))
	statusChangeTimeSec, statusChangeTimeNSec = nsToUnixTime(uint64(inode.inodeHeadV1.StatusChangeTime.UnixNano()))

	getAttrOut = &fission.GetAttrOut{
		AttrValidSec:  globals.fuseAttrValidDurationSec,
		AttrValidNSec: globals.fuseAttrValidDurationNSec,
		Dummy:         0,
		Attr: fission.Attr{
			Ino:       inode.inodeHeadV1.InodeNumber,
			Size:      inode.inodeHeadV1.Size, // Possibly overwritten by fixAttrSizes()
			Blocks:    0,                      // Computed by fixAttrSizes()
			ATimeSec:  modificationTimeSec,
			MTimeSec:  modificationTimeSec,
			CTimeSec:  statusChangeTimeSec,
			ATimeNSec: modificationTimeNSec,
			MTimeNSec: modificationTimeNSec,
			CTimeNSec: statusChangeTimeNSec,
			Mode:      computeAttrMode(inode.inodeHeadV1.InodeType, inode.inodeHeadV1.Mode),
			NLink:     uint32(len(inode.inodeHeadV1.LinkTable)),
			UID:       uint32(inode.inodeHeadV1.UserID),
			GID:       uint32(inode.inodeHeadV1.GroupID),
			RDev:      attrRDev,
			BlkSize:   attrBlockSize, // Possibly overwritten by fixAttrSizes()
			Padding:   0,
		},
	}

	inodeLockRequest.unlockAll()

	fixAttrSizes(&getAttrOut.Attr)

	errno = 0
	return
}

func (dummy *globalsStruct) DoSetAttr(inHeader *fission.InHeader, setAttrIn *fission.SetAttrIn) (setAttrOut *fission.SetAttrOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoSetAttr(inHeader: %+v, setAttrIn: %+v)", inHeader, setAttrIn)
	defer func() {
		logTracef("<== DoSetAttr(setAttrOut: %+v, errno: %v)", setAttrOut, errno)
	}()

	defer func() {
		globals.stats.DoSetAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	setAttrOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReadLink(inHeader *fission.InHeader) (readLinkOut *fission.ReadLinkOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoReadLink(inHeader: %+v)", inHeader)
	defer func() {
		logTracef("<== DoReadLink(readLinkOut: %+v, errno: %v)", readLinkOut, errno)
	}()

	defer func() {
		globals.stats.DoReadLinkUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	readLinkOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSymLink(inHeader *fission.InHeader, symLinkIn *fission.SymLinkIn) (symLinkOut *fission.SymLinkOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoSymLink(inHeader: %+v, symLinkIn: %+v)", inHeader, symLinkIn)
	defer func() {
		logTracef("<== DoSymLink(symLinkOut: %+v, errno: %v)", symLinkOut, errno)
	}()

	defer func() {
		globals.stats.DoSymLinkUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	symLinkOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoMkNod(inHeader *fission.InHeader, mkNodIn *fission.MkNodIn) (mkNodOut *fission.MkNodOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoMkNod(inHeader: %+v, mkNodIn: %+v)", inHeader, mkNodIn)
	defer func() {
		logTracef("<== DoMkNod(mkNodOut: %+v, errno: %v)", mkNodOut, errno)
	}()

	defer func() {
		globals.stats.DoMkNodUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	mkNodOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoMkDir(inHeader *fission.InHeader, mkDirIn *fission.MkDirIn) (mkDirOut *fission.MkDirOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoMkDir(inHeader: %+v, mkDirIn: %+v)", inHeader, mkDirIn)
	defer func() {
		logTracef("<== DoMkDir(mkDirOut: %+v, errno: %v)", mkDirOut, errno)
	}()

	defer func() {
		globals.stats.DoMkDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	mkDirOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoUnlink(inHeader *fission.InHeader, unlinkIn *fission.UnlinkIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoUnlink(inHeader: %+v, unlinkIn: %+v)", inHeader, unlinkIn)
	defer func() {
		logTracef("<== DoUnlink(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoUnlinkUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRmDir(inHeader *fission.InHeader, rmDirIn *fission.RmDirIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoRmDir(inHeader: %+v, rmDirIn: %+v)", inHeader, rmDirIn)
	defer func() {
		logTracef("<== DoRmDir(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoRmDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRename(inHeader *fission.InHeader, renameIn *fission.RenameIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoRename(inHeader: %+v, renameIn: %+v)", inHeader, renameIn)
	defer func() {
		logTracef("<== DoRename(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoRenameUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoLink(inHeader *fission.InHeader, linkIn *fission.LinkIn) (linkOut *fission.LinkOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoLink(inHeader: %+v, linkIn: %+v)", inHeader, linkIn)
	defer func() {
		logTracef("<== DoLink(linkOut: %+v, errno: %v)", linkOut, errno)
	}()

	defer func() {
		globals.stats.DoLinkUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	linkOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoOpen(inHeader *fission.InHeader, openIn *fission.OpenIn) (openOut *fission.OpenOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoOpen(inHeader: %+v, openIn: %+v)", inHeader, openIn)
	defer func() {
		logTracef("<== DoOpen(openOut: %+v, errno: %v)", openOut, errno)
	}()

	defer func() {
		globals.stats.DoOpenUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	openOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRead(inHeader *fission.InHeader, readIn *fission.ReadIn) (readOut *fission.ReadOut, errno syscall.Errno) {
	var (
		curOffset                    uint64
		err                          error
		extentMapEntryIndexV1        int
		extentMapEntryIndexV1Max     int // Entry entry at or just after  where readIn.Offset+readIn.Size may reside
		extentMapEntryIndexV1Min     int // First entry at or just before where readIn.Offset may reside
		extentMapEntryKeyV1          uint64
		extentMapEntryKeyV1AsKey     sortedmap.Key
		extentMapEntryValueV1        *ilayout.ExtentMapEntryValueV1Struct
		extentMapEntryValueV1AsValue sortedmap.Value
		inode                        *inodeStruct
		inodeLockRequest             *inodeLockRequestStruct
		ok                           bool
		openHandle                   *openHandleStruct
		readPlan                     []*ilayout.ExtentMapEntryValueV1Struct
		readPlanEntry                *ilayout.ExtentMapEntryValueV1Struct // If .ObjectNumber == 0, .ObjectOffset is ignored... .Length is the number of zero fill bytes
		remainingSize                uint64
		startTime                    time.Time = time.Now()
	)

	logTracef("==> DoRead(inHeader: %+v, readIn: %+v)", inHeader, readIn)
	defer func() {
		if errno == 0 {
			logTracef("<== DoRead(readOut: &{len(Data):%v}, errno: %v)", len(readOut.Data), errno)
		} else {
			logTracef("<== DoRead(readOut: %+v, errno: %v)", readOut, errno)
		}
	}()

	defer func() {
		globals.stats.DoReadUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

Retry:
	inodeLockRequest = newLockRequest()
	inodeLockRequest.inodeNumber = uint64(inHeader.NodeID)
	inodeLockRequest.exclusive = true
	inodeLockRequest.addThisLock()
	if len(inodeLockRequest.locksHeld) == 0 {
		goto Retry
	}

	openHandle = lookupOpenHandleByInodeNumber(readIn.FH)
	if nil == openHandle {
		inodeLockRequest.unlockAll()
		readOut = nil
		errno = syscall.EBADF
		return
	}
	if openHandle.inodeNumber != uint64(inHeader.NodeID) {
		inodeLockRequest.unlockAll()
		readOut = nil
		errno = syscall.EBADF
		return
	}
	if !openHandle.fissionFlagsRead {
		inodeLockRequest.unlockAll()
		readOut = nil
		errno = syscall.EBADF
		return
	}

	inode = lookupInode(openHandle.inodeNumber)
	if nil == inode {
		inodeLockRequest.unlockAll()
		readOut = nil
		errno = syscall.ENOENT
		return
	}

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		inodeLockRequest.unlockAll()
		readOut = nil
		errno = syscall.EISDIR
		return
	case ilayout.InodeTypeFile:
		// Fall through
	case ilayout.InodeTypeSymLink:
		inodeLockRequest.unlockAll()
		readOut = nil
		errno = syscall.EBADF
		return
	default:
		logFatalf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	curOffset = readIn.Offset
	if curOffset >= inode.inodeHeadV1.Size {
		inodeLockRequest.unlockAll()
		readOut = &fission.ReadOut{
			Data: make([]byte, 0, 0),
		}
		errno = 0
		return
	}

	remainingSize = uint64(readIn.Size)
	if (curOffset + remainingSize) > inode.inodeHeadV1.Size {
		remainingSize = inode.inodeHeadV1.Size - curOffset
	}

	readOut = &fission.ReadOut{
		Data: make([]byte, 0, remainingSize),
	}

	if nil == inode.payload {
		err = inode.oldPayload()
		if nil != err {
			logFatal(err)
		}
	}

	extentMapEntryIndexV1Min, _, err = inode.payload.BisectLeft(curOffset)
	if nil != err {
		logFatal(err)
	}

	if extentMapEntryIndexV1Min < 0 {
		extentMapEntryIndexV1Min = 0
	}

	extentMapEntryIndexV1Max, _, err = inode.payload.BisectLeft(readIn.Offset + remainingSize)
	if nil != err {
		logFatal(err)
	}

	readPlan = make([]*ilayout.ExtentMapEntryValueV1Struct, 0, 2*(extentMapEntryIndexV1Max-extentMapEntryIndexV1Min))

	for extentMapEntryIndexV1 = extentMapEntryIndexV1Min; extentMapEntryIndexV1 < extentMapEntryIndexV1Max; extentMapEntryIndexV1++ {
		extentMapEntryKeyV1AsKey, extentMapEntryValueV1AsValue, _, err = inode.payload.GetByIndex(extentMapEntryIndexV1)
		if nil != err {
			logFatal(err)
		}

		extentMapEntryKeyV1, ok = extentMapEntryKeyV1AsKey.(uint64)
		if !ok {
			logFatalf("extentMapEntryKeyV1AsKey.(uint64) returned !ok")
		}

		if curOffset < extentMapEntryKeyV1 {
			if remainingSize < (extentMapEntryKeyV1 - curOffset) {
				readPlan = append(readPlan, &ilayout.ExtentMapEntryValueV1Struct{
					Length:       remainingSize,
					ObjectNumber: 0,
					ObjectOffset: 0,
				})

				curOffset += remainingSize
				remainingSize = 0

				break
			} else {
				readPlan = append(readPlan, &ilayout.ExtentMapEntryValueV1Struct{
					Length:       extentMapEntryKeyV1 - curOffset,
					ObjectNumber: 0,
					ObjectOffset: 0,
				})

				remainingSize -= extentMapEntryKeyV1 - curOffset
				curOffset = extentMapEntryKeyV1
			}
		}

		extentMapEntryValueV1, ok = extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1Struct)
		if !ok {
			logFatalf("extentMapEntryValueV1AsValue.(*ilayout.ExtentMapEntryValueV1) returned !ok")
		}

		if remainingSize <= extentMapEntryValueV1.Length {
			readPlan = append(readPlan, &ilayout.ExtentMapEntryValueV1Struct{
				Length:       remainingSize,
				ObjectNumber: extentMapEntryValueV1.ObjectNumber,
				ObjectOffset: extentMapEntryValueV1.ObjectOffset,
			})

			curOffset += remainingSize
			remainingSize = 0

			break
		}

		readPlan = append(readPlan, &ilayout.ExtentMapEntryValueV1Struct{
			Length:       extentMapEntryValueV1.Length,
			ObjectNumber: extentMapEntryValueV1.ObjectNumber,
			ObjectOffset: extentMapEntryValueV1.ObjectOffset,
		})

		curOffset += extentMapEntryValueV1.Length
		remainingSize -= extentMapEntryValueV1.Length
	}

	if remainingSize > 0 {
		readPlan = append(readPlan, &ilayout.ExtentMapEntryValueV1Struct{
			Length:       remainingSize,
			ObjectNumber: 0,
			ObjectOffset: 0,
		})
	}

	for _, readPlanEntry = range readPlan {
		switch readPlanEntry.ObjectNumber {
		case 0:
			readOut.Data = append(readOut.Data, make([]byte, readPlanEntry.Length, readPlanEntry.Length)...)
		case inode.putObjectNumber:
			readOut.Data = append(readOut.Data, inode.putObjectBuffer[readPlanEntry.ObjectOffset:(readPlanEntry.ObjectOffset+readPlanEntry.Length)]...)
		default:
			// TODO - need to actually read from the cache (in a coherent/cooperative way)
			// TODO - need to handle case where extent crosses cache line boundary
			// UNDO - but for now, we will simply append zeroes
			readOut.Data = append(readOut.Data, make([]byte, readPlanEntry.Length, readPlanEntry.Length)...) // UNDO
		}
	}

	inodeLockRequest.unlockAll()

	errno = 0
	return
}

func (dummy *globalsStruct) DoWrite(inHeader *fission.InHeader, writeIn *fission.WriteIn) (writeOut *fission.WriteOut, errno syscall.Errno) {
	var (
		inode            *inodeStruct
		inodeLockRequest *inodeLockRequestStruct
		openHandle       *openHandleStruct
		startTime        time.Time = time.Now()
	)

	logTracef("==> DoWrite(inHeader: %+v, writeIn: &{FH:%v Offset:%v Size:%v: WriteFlags:%v LockOwner:%v Flags:%v Padding:%v len(Data):%v})", inHeader, writeIn.FH, writeIn.Offset, writeIn.Size, writeIn.WriteFlags, writeIn.LockOwner, writeIn.Flags, writeIn.Padding, len(writeIn.Data))
	defer func() {
		logTracef("<== DoWrite(writeOut: %+v, errno: %v)", writeOut, errno)
	}()

	defer func() {
		globals.stats.DoWriteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

Retry:
	inodeLockRequest = newLockRequest()
	inodeLockRequest.inodeNumber = uint64(inHeader.NodeID)
	inodeLockRequest.exclusive = true
	inodeLockRequest.addThisLock()
	if len(inodeLockRequest.locksHeld) == 0 {
		goto Retry
	}

	openHandle = lookupOpenHandleByInodeNumber(writeIn.FH)
	if nil == openHandle {
		inodeLockRequest.unlockAll()
		writeOut = nil
		errno = syscall.EBADF
		return
	}
	if openHandle.inodeNumber != uint64(inHeader.NodeID) {
		inodeLockRequest.unlockAll()
		writeOut = nil
		errno = syscall.EBADF
		return
	}
	if !openHandle.fissionFlagsWrite {
		inodeLockRequest.unlockAll()
		writeOut = nil
		errno = syscall.EBADF
		return
	}

	inode = lookupInode(openHandle.inodeNumber)
	if nil == inode {
		inodeLockRequest.unlockAll()
		writeOut = nil
		errno = syscall.ENOENT
		return
	}

	switch inode.inodeHeadV1.InodeType {
	case ilayout.InodeTypeDir:
		inodeLockRequest.unlockAll()
		writeOut = nil
		errno = syscall.EISDIR
		return
	case ilayout.InodeTypeFile:
		// Fall through
	case ilayout.InodeTypeSymLink:
		inodeLockRequest.unlockAll()
		writeOut = nil
		errno = syscall.EBADF
		return
	default:
		logFatalf("inode.inodeHeadV1.InodeType(%v) unexpected - must be either ilayout.InodeTypeDir(%v) or ilayout.InodeTypeFile(%v)", inode.inodeHeadV1.InodeType, ilayout.InodeTypeDir, ilayout.InodeTypeFile)
	}

	// TODO - need to patch ExtentMap
	// TODO - need to append to inode.putObjectBuffer
	// TODO - need to detect combinable ExtentMap entries
	// TODO - need to trigger a flush if len(payload.putObjectBuffer) >= globals.config.FileFlushTriggerSize
	// TODO - need to (possibly) trigger new timer after globals.config.FileFlushTriggerDuration

	inodeLockRequest.unlockAll()

	writeOut = nil // UNDO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoStatFS(inHeader *fission.InHeader) (statFSOut *fission.StatFSOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoStatFS(inHeader: %+v)", inHeader)
	defer func() {
		logTracef("<== DoStatFS(statFSOut: %+v, errno: %v)", statFSOut, errno)
	}()

	defer func() {
		globals.stats.DoStatFSUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	statFSOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRelease(inHeader *fission.InHeader, releaseIn *fission.ReleaseIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoRelease(inHeader: %+v, releaseIn: %+v)", inHeader, releaseIn)
	defer func() {
		logTracef("<== DoRelease(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoReleaseUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoFSync(inHeader *fission.InHeader, fSyncIn *fission.FSyncIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoFSync(inHeader: %+v, fSyncIn: %+v)", inHeader, fSyncIn)
	defer func() {
		logTracef("<== DoFSync(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoFSyncUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSetXAttr(inHeader *fission.InHeader, setXAttrIn *fission.SetXAttrIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoSetXAttr(inHeader: %+v, setXAttrIn: %+v)", inHeader, setXAttrIn)
	defer func() {
		logTracef("<== DoSetXAttr(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoSetXAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoGetXAttr(inHeader *fission.InHeader, getXAttrIn *fission.GetXAttrIn) (getXAttrOut *fission.GetXAttrOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoGetXAttr(inHeader: %+v, getXAttrIn: %+v)", inHeader, getXAttrIn)
	defer func() {
		logTracef("<== DoGetXAttr(getXAttrOut: %+v, errno: %v)", getXAttrOut, errno)
	}()

	defer func() {
		globals.stats.DoGetXAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	getXAttrOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoListXAttr(inHeader *fission.InHeader, listXAttrIn *fission.ListXAttrIn) (listXAttrOut *fission.ListXAttrOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoListXAttr(inHeader: %+v, listXAttrIn: %+v)", inHeader, listXAttrIn)
	defer func() {
		logTracef("<== DoListXAttr(listXAttrOut: %+v, errno: %v)", listXAttrOut, errno)
	}()

	defer func() {
		globals.stats.DoListXAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	listXAttrOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRemoveXAttr(inHeader *fission.InHeader, removeXAttrIn *fission.RemoveXAttrIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoRemoveXAttr(inHeader: %+v, removeXAttrIn: %+v)", inHeader, removeXAttrIn)
	defer func() {
		logTracef("<== DoRemoveXAttr(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoRemoveXAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoFlush(inHeader *fission.InHeader, flushIn *fission.FlushIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoFlush(inHeader: %+v, flushIn: %+v)", inHeader, flushIn)
	defer func() {
		logTracef("<== DoFlush(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoFlushUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoInit(inHeader *fission.InHeader, initIn *fission.InitIn) (initOut *fission.InitOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoInit(inHeader: %+v, initIn: %+v)", inHeader, initIn)
	defer func() {
		logTracef("<== DoInit(initOut: %+v, errno: %v)", initOut, errno)
	}()

	defer func() {
		globals.stats.DoInitUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO

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
		startTime time.Time = time.Now()
	)

	logTracef("==> DoOpenDir(inHeader: %+v, openDirIn: %+v)", inHeader, openDirIn)
	defer func() {
		logTracef("<== DoOpenDir(openDirOut: %+v, errno: %v)", openDirOut, errno)
	}()

	defer func() {
		globals.stats.DoOpenDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	openDirOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReadDir(inHeader *fission.InHeader, readDirIn *fission.ReadDirIn) (readDirOut *fission.ReadDirOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoReadDir(inHeader: %+v, readDirIn: %+v)", inHeader, readDirIn)
	defer func() {
		logTracef("<== DoReadDir(readDirOut: %+v, errno: %v)", readDirOut, errno)
	}()

	defer func() {
		globals.stats.DoReadDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	readDirOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReleaseDir(inHeader *fission.InHeader, releaseDirIn *fission.ReleaseDirIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoReleaseDir(inHeader: %+v, releaseDirIn: %+v)", inHeader, releaseDirIn)
	defer func() {
		logTracef("<== DoReleaseDir(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoReleaseDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoFSyncDir(inHeader *fission.InHeader, fSyncDirIn *fission.FSyncDirIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoFSyncDir(inHeader: %+v, fSyncDirIn: %+v)", inHeader, fSyncDirIn)
	defer func() {
		logTracef("<== DoFSyncDir(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoFSyncDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoGetLK(inHeader *fission.InHeader, getLKIn *fission.GetLKIn) (getLKOut *fission.GetLKOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoGetLK(inHeader: %+v, getLKIn: %+v)", inHeader, getLKIn)
	defer func() {
		logTracef("<== DoGetLK(getLKOut: %+v, errno: %v)", getLKOut, errno)
	}()

	defer func() {
		globals.stats.DoGetLKUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	getLKOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSetLK(inHeader *fission.InHeader, setLKIn *fission.SetLKIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoSetLK(inHeader: %+v, setLKIn: %+v)", inHeader, setLKIn)
	defer func() {
		logTracef("<== DoSetLK(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoSetLKUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSetLKW(inHeader *fission.InHeader, setLKWIn *fission.SetLKWIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoSetLKW(inHeader: %+v, setLKWIn: %+v)", inHeader, setLKWIn)
	defer func() {
		logTracef("<== DoSetLKW(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoSetLKWUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoAccess(inHeader *fission.InHeader, accessIn *fission.AccessIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoAccess(inHeader: %+v, accessIn: %+v)", inHeader, accessIn)
	defer func() {
		logTracef("<== DoAccess(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoAccessUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoCreate(inHeader *fission.InHeader, createIn *fission.CreateIn) (createOut *fission.CreateOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoCreate(inHeader: %+v, createIn: %+v)", inHeader, createIn)
	defer func() {
		logTracef("<== DoCreate(createOut: %+v, errno: %v)", createOut, errno)
	}()

	defer func() {
		globals.stats.DoCreateUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	createOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoInterrupt(inHeader *fission.InHeader, interruptIn *fission.InterruptIn) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoInterrupt(inHeader: %+v, interruptIn: %+v)", inHeader, interruptIn)
	defer func() {
		logTracef("<== DoInterrupt()")
	}()

	defer func() {
		globals.stats.DoInterruptUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	return
}

func (dummy *globalsStruct) DoBMap(inHeader *fission.InHeader, bMapIn *fission.BMapIn) (bMapOut *fission.BMapOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoBMap(inHeader: %+v, bMapIn: %+v)", inHeader, bMapIn)
	defer func() {
		logTracef("<== DoBMap(bMapOut: %+v, errno: %v)", bMapOut, errno)
	}()

	defer func() {
		globals.stats.DoBMapUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	bMapOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoDestroy(inHeader *fission.InHeader) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoDestroy(inHeader: %+v)", inHeader)
	defer func() {
		logTracef("<== DoDestroy(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoDestroyUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoPoll(inHeader *fission.InHeader, pollIn *fission.PollIn) (pollOut *fission.PollOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoPoll(inHeader: %+v, pollIn: %+v)", inHeader, pollIn)
	defer func() {
		logTracef("<== DoPoll(pollOut: %+v, errno: %v)", pollOut, errno)
	}()

	defer func() {
		globals.stats.DoPollUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	pollOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoBatchForget(inHeader *fission.InHeader, batchForgetIn *fission.BatchForgetIn) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoBatchForget(inHeader: %+v, batchForgetIn: %+v)", inHeader, batchForgetIn)
	defer func() {
		logTracef("<== DoBatchForget()")
	}()

	defer func() {
		globals.stats.DoBatchForgetUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	return
}

func (dummy *globalsStruct) DoFAllocate(inHeader *fission.InHeader, fAllocateIn *fission.FAllocateIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoFAllocate(inHeader: %+v, fAllocateIn: %+v)", inHeader, fAllocateIn)
	defer func() {
		logTracef("<== DoFAllocate(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoFAllocateUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReadDirPlus(inHeader *fission.InHeader, readDirPlusIn *fission.ReadDirPlusIn) (readDirPlusOut *fission.ReadDirPlusOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoReadDirPlus(inHeader: %+v, readDirPlusIn: %+v)", inHeader, readDirPlusIn)
	defer func() {
		logTracef("<== DoReadDirPlus(readDirPlusOut: %+v, errno: %v)", readDirPlusOut, errno)
	}()

	defer func() {
		globals.stats.DoReadDirPlusUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	readDirPlusOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRename2(inHeader *fission.InHeader, rename2In *fission.Rename2In) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoRename2(inHeader: %+v, rename2In: %+v)", inHeader, rename2In)
	defer func() {
		logTracef("<== DoRename2(errno: %v)", errno)
	}()

	defer func() {
		globals.stats.DoRename2Usecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoLSeek(inHeader *fission.InHeader, lSeekIn *fission.LSeekIn) (lSeekOut *fission.LSeekOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoLSeek(inHeader: %+v, lSeekIn: %+v)", inHeader, lSeekIn)
	defer func() {
		logTracef("<== DoLSeek(lSeekOut: %+v, errno: %v)", lSeekOut, errno)
	}()

	defer func() {
		globals.stats.DoLSeekUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	lSeekOut = nil
	errno = syscall.ENOSYS
	return
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

func computeAttrMode(iLayoutInodeType uint8, iLayoutMode uint16) (attrMode uint32) {
	attrMode = uint32(iLayoutMode)
	switch iLayoutInodeType {
	case ilayout.InodeTypeDir:
		attrMode |= syscall.S_IFDIR
	case ilayout.InodeTypeFile:
		attrMode |= syscall.S_IFREG
	case ilayout.InodeTypeSymLink:
		attrMode |= syscall.S_IFLNK
	default:
		logFatalf("iLayoutInodeType (%v) unknown", iLayoutInodeType)
	}
	return
}

func fixAttrSizes(attr *fission.Attr) {
	if syscall.S_IFREG == (attr.Mode & syscall.S_IFMT) {
		attr.Blocks = attr.Size + (uint64(attrBlockSize) - 1)
		attr.Blocks /= uint64(attrBlockSize)
		attr.BlkSize = attrBlockSize
	} else {
		attr.Size = 0
		attr.Blocks = 0
		attr.BlkSize = 0
	}
}
