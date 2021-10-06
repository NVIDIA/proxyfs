// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"fmt"
	"syscall"
	"time"

	"github.com/NVIDIA/fission"

	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/imgr/imgrpkg"
)

const (
	attrBlockSize = uint32(512)

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

	err = globals.fissionVolume.DoMount()

	return
}

func performUnmountFUSE() (err error) {
	err = globals.fissionVolume.DoUnmount()

	globals.fissionVolume = nil

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
		inodeLease                 *inodeLeaseStruct
		inodeLockRequest           *inodeLockRequestStruct
		startTime                  time.Time = time.Now()
	)

	logTracef("==> DoGetAttr(inHeader: %+v, getAttrIn: %+v)", inHeader, getAttrIn)
	defer func() {
		logTracef("<== DoGetAttr(getAttrOut: %+v, errno: %v)", getAttrOut, errno)
	}()

	defer func() {
		globals.stats.DoGetAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	inodeLockRequest = newLockRequest()
	inodeLockRequest.inodeNumber = uint64(inHeader.NodeID)
	inodeLockRequest.exclusive = false
	inodeLockRequest.addThisLock()

	inodeLease = lookupInodeLease(uint64(inHeader.NodeID))
	if nil == inodeLease {
		inodeLockRequest.unlockAll()
		getAttrOut = nil
		errno = syscall.ENOENT
		return
	}

	if nil == inodeLease.inodeHeadV1 {
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

		fmt.Printf("TODO: need to fetch .inodeHeadV1 via getInodeTableEntryResponse: %+v\n", getInodeTableEntryResponse)
		inodeLease.inodeHeadV1 = &ilayout.InodeHeadV1Struct{}
	}

	inodeLockRequest.unlockAll()
	getAttrOut = nil
	errno = syscall.ENOSYS
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
		startTime time.Time = time.Now()
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

	// TODO
	readOut = nil
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoWrite(inHeader *fission.InHeader, writeIn *fission.WriteIn) (writeOut *fission.WriteOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	logTracef("==> DoWrite(inHeader: %+v, writeIn: &{FH:%v Offset:%v Size:%v: WriteFlags:%v LockOwner:%v Flags:%v Padding:%v len(Data):%v})", inHeader, writeIn.FH, writeIn.Offset, writeIn.Size, writeIn.WriteFlags, writeIn.LockOwner, writeIn.Flags, writeIn.Padding, len(writeIn.Data))
	defer func() {
		logTracef("<== DoWrite(writeOut: %+v, errno: %v)", writeOut, errno)
	}()

	defer func() {
		globals.stats.DoWriteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	writeOut = nil
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

func fixAttrSizes(attr *fission.Attr) {
	// TODO
	// 	if syscall.S_IFREG == (attr.Mode & syscall.S_IFMT) {
	// 		attr.Blocks = attr.Size + (uint64(attrBlkSize) - 1)
	// 		attr.Blocks /= uint64(attrBlkSize)
	// 		attr.BlkSize = attrBlkSize
	// 	} else {
	// 		attr.Size = 0
	// 		attr.Blocks = 0
	// 		attr.BlkSize = 0
	// 	}
}
