// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"syscall"
	"time"

	"github.com/NVIDIA/fission"
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

	defer func() {
		globals.stats.DoLookupUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoForget(inHeader *fission.InHeader, forgetIn *fission.ForgetIn) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoForgetUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	return
}

func (dummy *globalsStruct) DoGetAttr(inHeader *fission.InHeader, getAttrIn *fission.GetAttrIn) (getAttrOut *fission.GetAttrOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoGetAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSetAttr(inHeader *fission.InHeader, setAttrIn *fission.SetAttrIn) (setAttrOut *fission.SetAttrOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoSetAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReadLink(inHeader *fission.InHeader) (readLinkOut *fission.ReadLinkOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoReadLinkUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSymLink(inHeader *fission.InHeader, symLinkIn *fission.SymLinkIn) (symLinkOut *fission.SymLinkOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoSymLinkUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoMkNod(inHeader *fission.InHeader, mkNodIn *fission.MkNodIn) (mkNodOut *fission.MkNodOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoMkNodUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoMkDir(inHeader *fission.InHeader, mkDirIn *fission.MkDirIn) (mkDirOut *fission.MkDirOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoMkDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoUnlink(inHeader *fission.InHeader, unlinkIn *fission.UnlinkIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoLinkUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoOpen(inHeader *fission.InHeader, openIn *fission.OpenIn) (openOut *fission.OpenOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoOpenUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRead(inHeader *fission.InHeader, readIn *fission.ReadIn) (readOut *fission.ReadOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoReadUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoWrite(inHeader *fission.InHeader, writeIn *fission.WriteIn) (writeOut *fission.WriteOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoWriteUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoStatFS(inHeader *fission.InHeader) (statFSOut *fission.StatFSOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoStatFSUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRelease(inHeader *fission.InHeader, releaseIn *fission.ReleaseIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoGetXAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoListXAttr(inHeader *fission.InHeader, listXAttrIn *fission.ListXAttrIn) (listXAttrOut *fission.ListXAttrOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoListXAttrUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRemoveXAttr(inHeader *fission.InHeader, removeXAttrIn *fission.RemoveXAttrIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoOpenDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReadDir(inHeader *fission.InHeader, readDirIn *fission.ReadDirIn) (readDirOut *fission.ReadDirOut, errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

	defer func() {
		globals.stats.DoReadDirUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReleaseDir(inHeader *fission.InHeader, releaseDirIn *fission.ReleaseDirIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoGetLKUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSetLK(inHeader *fission.InHeader, setLKIn *fission.SetLKIn) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoCreateUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoInterrupt(inHeader *fission.InHeader, interruptIn *fission.InterruptIn) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoBMapUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoDestroy(inHeader *fission.InHeader) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoPollUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoBatchForget(inHeader *fission.InHeader, batchForgetIn *fission.BatchForgetIn) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoReadDirPlusUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoRename2(inHeader *fission.InHeader, rename2In *fission.Rename2In) (errno syscall.Errno) {
	var (
		startTime time.Time = time.Now()
	)

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

	defer func() {
		globals.stats.DoLSeekUsecs.Add(uint64(time.Since(startTime) / time.Microsecond))
	}()

	// TODO
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
