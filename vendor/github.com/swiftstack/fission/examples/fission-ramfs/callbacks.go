package main

import (
	"os"
	"syscall"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/fission"
)

func (dummy *globalsStruct) DoLookup(inHeader *fission.InHeader, lookupIn *fission.LookupIn) (lookupOut *fission.LookupOut, errno syscall.Errno) {
	var (
		dirEntInoAsU64   uint64
		dirEntInoAsValue sortedmap.Value
		dirEntInode      *inodeStruct
		dirInode         *inodeStruct
		err              error
		granted          bool
		grantedLockSet   *grantedLockSetStruct = makeGrantedLockSet()
		ok               bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	grantedLockSet.free(globals.tryLock)

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	dirEntInoAsValue, ok, err = dirInode.dirEntryMap.GetByKey(lookupIn.Name)
	if nil != err {
		globals.logger.Printf("func DoLookup(NodeID==%v,Name=%s) failed on .dirEntryMap.GetByKey(): %v", inHeader.NodeID, string(lookupIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	dirEntInoAsU64 = dirEntInoAsValue.(uint64)

	dirEntInode, ok = globals.inodeMap[dirEntInoAsU64]
	if !ok {
		globals.logger.Printf("func DoLookup(NodeID==%v) failed fetching globals.inodeMap[%v]", inHeader.NodeID, dirEntInoAsU64)
		os.Exit(1)
	}

	granted = grantedLockSet.try(dirEntInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	lookupOut = &fission.LookupOut{
		EntryOut: fission.EntryOut{
			NodeID:         dirEntInode.attr.Ino,
			Generation:     0,
			EntryValidSec:  0,
			AttrValidSec:   0,
			EntryValidNSec: 0,
			AttrValidNSec:  0,
			Attr: fission.Attr{
				Ino:       dirEntInode.attr.Ino,
				Size:      dirEntInode.attr.Size,
				Blocks:    dirEntInode.attr.Blocks,
				ATimeSec:  dirEntInode.attr.ATimeSec,
				MTimeSec:  dirEntInode.attr.MTimeSec,
				CTimeSec:  dirEntInode.attr.CTimeSec,
				ATimeNSec: dirEntInode.attr.ATimeNSec,
				MTimeNSec: dirEntInode.attr.MTimeNSec,
				CTimeNSec: dirEntInode.attr.CTimeNSec,
				Mode:      dirEntInode.attr.Mode,
				NLink:     dirEntInode.attr.NLink,
				UID:       dirEntInode.attr.UID,
				GID:       dirEntInode.attr.GID,
				RDev:      dirEntInode.attr.RDev,
				BlkSize:   dirEntInode.attr.BlkSize,
				Padding:   dirEntInode.attr.Padding,
			},
		},
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoForget(inHeader *fission.InHeader, forgetIn *fission.ForgetIn) {
	return
}

func (dummy *globalsStruct) DoGetAttr(inHeader *fission.InHeader, getAttrIn *fission.GetAttrIn) (getAttrOut *fission.GetAttrOut, errno syscall.Errno) {
	var (
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		inode          *inodeStruct
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(inode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	grantedLockSet.free(globals.tryLock)

	getAttrOut = &fission.GetAttrOut{
		AttrValidSec:  0,
		AttrValidNSec: 0,
		Dummy:         0,
		Attr: fission.Attr{
			Ino:       inode.attr.Ino,
			Size:      inode.attr.Size,
			Blocks:    inode.attr.Blocks,
			ATimeSec:  inode.attr.ATimeSec,
			MTimeSec:  inode.attr.MTimeSec,
			CTimeSec:  inode.attr.CTimeSec,
			ATimeNSec: inode.attr.ATimeNSec,
			MTimeNSec: inode.attr.MTimeNSec,
			CTimeNSec: inode.attr.CTimeNSec,
			Mode:      inode.attr.Mode,
			NLink:     inode.attr.NLink,
			UID:       inode.attr.UID,
			GID:       inode.attr.GID,
			RDev:      inode.attr.RDev,
			BlkSize:   inode.attr.BlkSize,
			Padding:   inode.attr.Padding,
		},
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoSetAttr(inHeader *fission.InHeader, setAttrIn *fission.SetAttrIn) (setAttrOut *fission.SetAttrOut, errno syscall.Errno) {
	var (
		granted         bool
		grantedLockSet  *grantedLockSetStruct = makeGrantedLockSet()
		inode           *inodeStruct
		inodeAttrMode   uint32
		ok              bool
		setAttrInMode   uint32
		unixTimeNowNSec uint32
		unixTimeNowSec  uint64
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	if (0 != (setAttrIn.Valid & fission.SetAttrInValidFH)) && (0 != setAttrIn.FH) {
		if !globals.alreadyLoggedIgnoring.setAttrInValidFH {
			globals.logger.Printf("func DoSetAttr(,setAttrIn.Valid==0x%08X) ignoring FH bit (0x%08X)", setAttrIn.Valid, fission.SetAttrInValidFH)
			globals.alreadyLoggedIgnoring.setAttrInValidFH = true
		}
	}

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(inode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	grantedLockSet.free(globals.tryLock)

	unixTimeNowSec, unixTimeNowNSec = unixTimeNow()

	if 0 != (setAttrIn.Valid & fission.SetAttrInValidMode) {
		inodeAttrMode = inode.attr.Mode & ^uint32(syscall.S_IRWXU|syscall.S_IRWXG|syscall.S_IRWXO)
		setAttrInMode = setAttrIn.Mode & uint32(syscall.S_IRWXU|syscall.S_IRWXG|syscall.S_IRWXO)
		inodeAttrMode |= setAttrInMode

		inode.attr.Mode = inodeAttrMode
	}

	if 0 != (setAttrIn.Valid & fission.SetAttrInValidUID) {
		inode.attr.UID = setAttrIn.UID
	}
	if 0 != (setAttrIn.Valid & fission.SetAttrInValidGID) {
		inode.attr.GID = setAttrIn.GID
	}

	if 0 != (setAttrIn.Valid & fission.SetAttrInValidSize) {
		if syscall.S_IFREG != (inode.attr.Mode & syscall.S_IFMT) {
			grantedLockSet.freeAll(false)
			errno = syscall.EINVAL
			return
		}
		if setAttrIn.Size <= inode.attr.Size {
			inode.fileData = inode.fileData[:setAttrIn.Size]
		} else {
			inode.fileData = append(inode.fileData, make([]byte, (setAttrIn.Size-inode.attr.Size))...)
		}
		inode.attr.Size = setAttrIn.Size
	}

	if 0 != (setAttrIn.Valid & fission.SetAttrInValidATime) {
		inode.attr.ATimeSec = setAttrIn.ATimeSec
		inode.attr.ATimeNSec = setAttrIn.ATimeNSec
	}

	if 0 != (setAttrIn.Valid & fission.SetAttrInValidMTime) {
		inode.attr.MTimeSec = setAttrIn.MTimeSec
		inode.attr.MTimeNSec = setAttrIn.MTimeNSec
	}

	if 0 != (setAttrIn.Valid & fission.SetAttrInValidATimeNow) {
		inode.attr.ATimeSec = unixTimeNowSec
		inode.attr.ATimeNSec = unixTimeNowNSec
	}

	if 0 != (setAttrIn.Valid & fission.SetAttrInValidMTimeNow) {
		inode.attr.MTimeSec = unixTimeNowSec
		inode.attr.MTimeNSec = unixTimeNowNSec
	}

	if 0 != (setAttrIn.Valid & fission.SetAttrInValidLockOwner) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOSYS
	}

	setAttrOut = &fission.SetAttrOut{
		AttrValidSec:  0,
		AttrValidNSec: 0,
		Dummy:         0,
		Attr: fission.Attr{
			Ino:       inode.attr.Ino,
			Size:      inode.attr.Size,
			Blocks:    inode.attr.Blocks,
			ATimeSec:  inode.attr.ATimeSec,
			MTimeSec:  inode.attr.MTimeSec,
			CTimeSec:  inode.attr.CTimeSec,
			ATimeNSec: inode.attr.ATimeNSec,
			MTimeNSec: inode.attr.MTimeNSec,
			CTimeNSec: inode.attr.CTimeNSec,
			Mode:      inode.attr.Mode,
			NLink:     inode.attr.NLink,
			UID:       inode.attr.UID,
			GID:       inode.attr.GID,
			RDev:      inode.attr.RDev,
			BlkSize:   inode.attr.BlkSize,
			Padding:   inode.attr.Padding,
		},
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoReadLink(inHeader *fission.InHeader) (readLinkOut *fission.ReadLinkOut, errno syscall.Errno) {
	var (
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
		symInode       *inodeStruct
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	symInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(symInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFLNK != (symInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	readLinkOut = &fission.ReadLinkOut{
		Data: cloneByteSlice(symInode.symlinkData),
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoSymLink(inHeader *fission.InHeader, symLinkIn *fission.SymLinkIn) (symLinkOut *fission.SymLinkOut, errno syscall.Errno) {
	var (
		dirEntInode     *inodeStruct
		dirEntInodeMode uint32
		dirInode        *inodeStruct
		err             error
		granted         bool
		grantedLockSet  *grantedLockSetStruct = makeGrantedLockSet()
		ok              bool
		unixTimeNowNSec uint32
		unixTimeNowSec  uint64
	)

	dirEntInodeMode = uint32(syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO)
	dirEntInodeMode |= syscall.S_IFLNK

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	_, ok, err = dirInode.dirEntryMap.GetByKey(symLinkIn.Name)
	if nil != err {
		globals.logger.Printf("func DoSymLink(NodeID==%v,Name=%s,Data=%s) failed on .dirEntryMap.GetByKey(): %v", inHeader.NodeID, string(symLinkIn.Name[:]), string(symLinkIn.Data[:]), err)
		os.Exit(1)
	}

	if ok {
		grantedLockSet.freeAll(false)
		errno = syscall.EEXIST
		return
	}

	globals.lastNodeID++

	unixTimeNowSec, unixTimeNowNSec = unixTimeNow()

	dirEntInode = &inodeStruct{
		tryLock: makeTryLock(),
		attr: fission.Attr{
			Ino:       globals.lastNodeID,
			Size:      0,
			Blocks:    0,
			ATimeSec:  unixTimeNowSec,
			MTimeSec:  unixTimeNowSec,
			CTimeSec:  unixTimeNowSec,
			ATimeNSec: unixTimeNowNSec,
			MTimeNSec: unixTimeNowNSec,
			CTimeNSec: unixTimeNowNSec,
			Mode:      dirEntInodeMode,
			NLink:     1,
			UID:       inHeader.UID,
			GID:       inHeader.GID,
			RDev:      0,
			BlkSize:   attrBlkSize,
			Padding:   0,
		},
		xattrMap:    sortedmap.NewLLRBTree(sortedmap.CompareByteSlice, globals.xattrMapDummy),
		dirEntryMap: nil,
		fileData:    nil,
		symlinkData: symLinkIn.Data,
	}

	ok, err = dirInode.dirEntryMap.Put(symLinkIn.Name, dirEntInode.attr.Ino)
	if nil != err {
		globals.logger.Printf("func DoSymLink(NodeID==%v,Name=%s,Data=%s) failed on .dirEntryMap.Put(): %v", inHeader.NodeID, string(symLinkIn.Name[:]), string(symLinkIn.Data[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoSymLink(NodeID==%v,Name=%s,Data=%s) .dirEntryMap.Put() returned !ok", inHeader.NodeID, string(symLinkIn.Name[:]), string(symLinkIn.Data[:]))
		os.Exit(1)
	}

	globals.inodeMap[dirEntInode.attr.Ino] = dirEntInode

	symLinkOut = &fission.SymLinkOut{
		EntryOut: fission.EntryOut{
			NodeID:         dirEntInode.attr.Ino,
			Generation:     0,
			EntryValidSec:  0,
			AttrValidSec:   0,
			EntryValidNSec: 0,
			AttrValidNSec:  0,
			Attr: fission.Attr{
				Ino:       dirEntInode.attr.Ino,
				Size:      dirEntInode.attr.Size,
				Blocks:    dirEntInode.attr.Blocks,
				ATimeSec:  dirEntInode.attr.ATimeSec,
				MTimeSec:  dirEntInode.attr.MTimeSec,
				CTimeSec:  dirEntInode.attr.CTimeSec,
				ATimeNSec: dirEntInode.attr.ATimeNSec,
				MTimeNSec: dirEntInode.attr.MTimeNSec,
				CTimeNSec: dirEntInode.attr.CTimeNSec,
				Mode:      dirEntInode.attr.Mode,
				NLink:     dirEntInode.attr.NLink,
				UID:       dirEntInode.attr.UID,
				GID:       dirEntInode.attr.GID,
				RDev:      dirEntInode.attr.RDev,
				BlkSize:   dirEntInode.attr.BlkSize,
				Padding:   dirEntInode.attr.Padding,
			},
		},
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoMkNod(inHeader *fission.InHeader, mkNodIn *fission.MkNodIn) (mkNodOut *fission.MkNodOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoMkDir(inHeader *fission.InHeader, mkDirIn *fission.MkDirIn) (mkDirOut *fission.MkDirOut, errno syscall.Errno) {
	var (
		dirEntInode     *inodeStruct
		dirEntInodeMode uint32
		dirInode        *inodeStruct
		err             error
		granted         bool
		grantedLockSet  *grantedLockSetStruct = makeGrantedLockSet()
		ok              bool
		unixTimeNowNSec uint32
		unixTimeNowSec  uint64
	)

	dirEntInodeMode = uint32(syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO)
	dirEntInodeMode &= mkDirIn.Mode
	dirEntInodeMode &= ^mkDirIn.UMask
	dirEntInodeMode |= syscall.S_IFDIR

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	_, ok, err = dirInode.dirEntryMap.GetByKey(mkDirIn.Name)
	if nil != err {
		globals.logger.Printf("func DoMkDir(NodeID==%v,Name=%s) failed on .dirEntryMap.GetByKey(): %v", inHeader.NodeID, string(mkDirIn.Name[:]), err)
		os.Exit(1)
	}

	if ok {
		grantedLockSet.freeAll(false)
		errno = syscall.EEXIST
		return
	}

	globals.lastNodeID++

	unixTimeNowSec, unixTimeNowNSec = unixTimeNow()

	dirEntInode = &inodeStruct{
		tryLock: makeTryLock(),
		attr: fission.Attr{
			Ino:       globals.lastNodeID,
			Size:      0,
			Blocks:    0,
			ATimeSec:  unixTimeNowSec,
			MTimeSec:  unixTimeNowSec,
			CTimeSec:  unixTimeNowSec,
			ATimeNSec: unixTimeNowNSec,
			MTimeNSec: unixTimeNowNSec,
			CTimeNSec: unixTimeNowNSec,
			Mode:      dirEntInodeMode,
			NLink:     2,
			UID:       inHeader.UID,
			GID:       inHeader.GID,
			RDev:      0,
			BlkSize:   attrBlkSize,
			Padding:   0,
		},
		xattrMap:    sortedmap.NewLLRBTree(sortedmap.CompareByteSlice, globals.xattrMapDummy),
		dirEntryMap: sortedmap.NewLLRBTree(sortedmap.CompareByteSlice, globals.dirEntryMapDummy),
		fileData:    nil,
		symlinkData: nil,
	}

	ok, err = dirEntInode.dirEntryMap.Put([]byte("."), dirEntInode.attr.Ino)
	if nil != err {
		globals.logger.Printf("func DoMkDir(NodeID==%v,Name=%s) failed on dirEntInode.dirEntryMap.Put(\".\"): %v", inHeader.NodeID, string(mkDirIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoMkDir(NodeID==%v,Name=%s) dirEntInode.dirEntryMap.Put(\".\") returned !ok", inHeader.NodeID, string(mkDirIn.Name[:]))
		os.Exit(1)
	}
	ok, err = dirEntInode.dirEntryMap.Put([]byte(".."), dirInode.attr.Ino)
	if nil != err {
		globals.logger.Printf("func DoMkDir(NodeID==%v,Name=%s) failed on dirEntInode.dirEntryMap.Put(\"..\"): %v", inHeader.NodeID, string(mkDirIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoMkDir(NodeID==%v,Name=%s) dirEntInode.dirEntryMap.Put(\"..\") returned !ok", inHeader.NodeID, string(mkDirIn.Name[:]))
		os.Exit(1)
	}
	ok, err = dirInode.dirEntryMap.Put(mkDirIn.Name, dirEntInode.attr.Ino)
	if nil != err {
		globals.logger.Printf("func DoMkDir(NodeID==%v,Name=%s) failed on dirInode.dirEntryMap.Put(): %v", inHeader.NodeID, string(mkDirIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoMkDir(NodeID==%v,Name=%s) dirInode.dirEntryMap.Put() returned !ok", inHeader.NodeID, string(mkDirIn.Name[:]))
		os.Exit(1)
	}

	dirInode.attr.NLink++

	globals.inodeMap[dirEntInode.attr.Ino] = dirEntInode

	mkDirOut = &fission.MkDirOut{
		EntryOut: fission.EntryOut{
			NodeID:         dirEntInode.attr.Ino,
			Generation:     0,
			EntryValidSec:  0,
			AttrValidSec:   0,
			EntryValidNSec: 0,
			AttrValidNSec:  0,
			Attr: fission.Attr{
				Ino:       dirEntInode.attr.Ino,
				Size:      dirEntInode.attr.Size,
				Blocks:    dirEntInode.attr.Blocks,
				ATimeSec:  dirEntInode.attr.ATimeSec,
				MTimeSec:  dirEntInode.attr.MTimeSec,
				CTimeSec:  dirEntInode.attr.CTimeSec,
				ATimeNSec: dirEntInode.attr.ATimeNSec,
				MTimeNSec: dirEntInode.attr.MTimeNSec,
				CTimeNSec: dirEntInode.attr.CTimeNSec,
				Mode:      dirEntInode.attr.Mode,
				NLink:     dirEntInode.attr.NLink,
				UID:       dirEntInode.attr.UID,
				GID:       dirEntInode.attr.GID,
				RDev:      dirEntInode.attr.RDev,
				BlkSize:   dirEntInode.attr.BlkSize,
				Padding:   dirEntInode.attr.Padding,
			},
		},
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoUnlink(inHeader *fission.InHeader, unlinkIn *fission.UnlinkIn) (errno syscall.Errno) {
	var (
		dirEntInoAsU64   uint64
		dirEntInoAsValue sortedmap.Value
		dirEntInode      *inodeStruct
		dirInode         *inodeStruct
		err              error
		granted          bool
		grantedLockSet   *grantedLockSetStruct = makeGrantedLockSet()
		ok               bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	dirEntInoAsValue, ok, err = dirInode.dirEntryMap.GetByKey(unlinkIn.Name)
	if nil != err {
		globals.logger.Printf("func DoUnlink(NodeID==%v,Name=%s) failed on .dirEntryMap.GetByKey(): %v", inHeader.NodeID, string(unlinkIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	dirEntInoAsU64 = dirEntInoAsValue.(uint64)

	dirEntInode, ok = globals.inodeMap[dirEntInoAsU64]
	if !ok {
		globals.logger.Printf("func DoUnlink(NodeID==%v,Name==%s) failed fetching globals.inodeMap[%v]", inHeader.NodeID, unlinkIn.Name, dirEntInoAsU64)
		os.Exit(1)
	}

	granted = grantedLockSet.try(dirEntInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR == (dirEntInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.EISDIR
		return
	}

	ok, err = dirInode.dirEntryMap.DeleteByKey(unlinkIn.Name)
	if nil != err {
		globals.logger.Printf("func DoUnlink(NodeID==%v,Name=%s) failed on .dirEntryMap.DeleteByKey(): %v", inHeader.NodeID, string(unlinkIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoUnlink(NodeID==%v,Name=%s) .dirEntryMap.DeleteByKey() returned !ok", inHeader.NodeID, string(unlinkIn.Name[:]))
		os.Exit(1)
	}

	dirEntInode.attr.NLink--

	if 0 == dirEntInode.attr.NLink {
		delete(globals.inodeMap, dirEntInode.attr.Ino)
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoRmDir(inHeader *fission.InHeader, rmDirIn *fission.RmDirIn) (errno syscall.Errno) {
	var (
		dirEntInoAsU64            uint64
		dirEntInoAsValue          sortedmap.Value
		dirEntInode               *inodeStruct
		dirEntInodeDirEntryMapLen int
		dirInode                  *inodeStruct
		err                       error
		granted                   bool
		grantedLockSet            *grantedLockSetStruct = makeGrantedLockSet()
		ok                        bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	dirEntInoAsValue, ok, err = dirInode.dirEntryMap.GetByKey(rmDirIn.Name)
	if nil != err {
		globals.logger.Printf("func DoRmDir(NodeID==%v,Name=%s) failed on .dirEntryMap.GetByKey(): %v", inHeader.NodeID, string(rmDirIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	dirEntInoAsU64 = dirEntInoAsValue.(uint64)

	dirEntInode, ok = globals.inodeMap[dirEntInoAsU64]
	if !ok {
		globals.logger.Printf("func DoRmDir(NodeID==%v,Name==%s) failed fetching globals.inodeMap[%v]", inHeader.NodeID, rmDirIn.Name, dirEntInoAsU64)
		os.Exit(1)
	}

	granted = grantedLockSet.try(dirEntInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirEntInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	dirEntInodeDirEntryMapLen, err = dirEntInode.dirEntryMap.Len()
	if nil != err {
		globals.logger.Printf("func DoRmDir(NodeID==%v,Name=%s) failed on .dirEntryMap.Len(): %v", inHeader.NodeID, string(rmDirIn.Name[:]), err)
		os.Exit(1)
	}

	if 2 != dirEntInodeDirEntryMapLen {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTEMPTY
		return
	}

	ok, err = dirInode.dirEntryMap.DeleteByKey(rmDirIn.Name)
	if nil != err {
		globals.logger.Printf("func DoRmDir(NodeID==%v,Name=%s) failed on .dirEntryMap.DeleteByKey(): %v", inHeader.NodeID, string(rmDirIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoRmDir(NodeID==%v,Name=%s) .dirEntryMap.DeleteByKey() returned !ok", inHeader.NodeID, string(rmDirIn.Name[:]))
		os.Exit(1)
	}

	dirEntInode.attr.NLink--

	delete(globals.inodeMap, dirEntInode.attr.Ino)

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoRename(inHeader *fission.InHeader, renameIn *fission.RenameIn) (errno syscall.Errno) {
	var (
		err                         error
		granted                     bool
		grantedLockSet              *grantedLockSetStruct = makeGrantedLockSet()
		movedInode                  *inodeStruct
		movedInodeNodeIDAsU64       uint64
		movedInodeNodeIDAsValue     sortedmap.Value
		newDirInode                 *inodeStruct
		ok                          bool
		oldDirInode                 *inodeStruct
		replacedInode               *inodeStruct
		replacedInodeDirEntryMapLen int
		replacedInodeNodeIDAsU64    uint64
		replacedInodeNodeIDAsValue  sortedmap.Value
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	oldDirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(oldDirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (oldDirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	if inHeader.NodeID == renameIn.NewDir {
		newDirInode = oldDirInode
	} else {
		newDirInode, ok = globals.inodeMap[renameIn.NewDir]
		if !ok {
			grantedLockSet.freeAll(false)
			errno = syscall.ENOENT
			return
		}

		granted = grantedLockSet.try(newDirInode.tryLock)
		if !granted {
			grantedLockSet.freeAll(true)
			goto Restart
		}

		if syscall.S_IFDIR != (newDirInode.attr.Mode & syscall.S_IFMT) {
			grantedLockSet.freeAll(false)
			errno = syscall.ENOTDIR
			return
		}
	}

	movedInodeNodeIDAsValue, ok, err = oldDirInode.dirEntryMap.GetByKey(renameIn.OldName)
	if nil != err {
		globals.logger.Printf("func DoRename(,OldName=%s) failed on .dirEntryMap.GetByKey(): %v", string(renameIn.OldName[:]), err)
		os.Exit(1)
	}
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	movedInodeNodeIDAsU64 = movedInodeNodeIDAsValue.(uint64)

	movedInode, ok = globals.inodeMap[movedInodeNodeIDAsU64]
	if !ok {
		globals.logger.Printf("func DoRename(,OldName=%s) globals.inodeMap[movedInodeNodeIDAsU64] returned !ok", string(renameIn.OldName[:]))
		os.Exit(1)
	}

	granted = grantedLockSet.try(movedInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	replacedInodeNodeIDAsValue, ok, err = newDirInode.dirEntryMap.GetByKey(renameIn.NewName)
	if nil != err {
		globals.logger.Printf("func DoRename(,NewName=%s) failed on .dirEntryMap.GetByKey(): %v", string(renameIn.NewName[:]), err)
		os.Exit(1)
	}

	if ok {
		replacedInodeNodeIDAsU64 = replacedInodeNodeIDAsValue.(uint64)

		replacedInode, ok = globals.inodeMap[replacedInodeNodeIDAsU64]
		if !ok {
			globals.logger.Printf("func DoRename(,NewName=%s) globals.inodeMap[replacedInodeNodeIDAsU64] returned !ok", string(renameIn.NewName[:]))
			os.Exit(1)
		}

		granted = grantedLockSet.try(movedInode.tryLock)
		if !granted {
			grantedLockSet.freeAll(true)
			goto Restart
		}
	} else {
		replacedInode = nil
	}

	if syscall.S_IFDIR == (movedInode.attr.Mode & syscall.S_IFMT) {
		if nil != replacedInode {
			if syscall.S_IFDIR != (movedInode.attr.Mode & syscall.S_IFMT) {
				grantedLockSet.freeAll(false)
				errno = syscall.ENOTDIR
				return
			}

			replacedInodeDirEntryMapLen, err = replacedInode.dirEntryMap.Len()
			if nil != err {
				globals.logger.Printf("func DoRename(,NewName=%s) failed on .dirEntryMap.Len(): %v", string(renameIn.NewName[:]), err)
				os.Exit(1)
			}

			if 2 != replacedInodeDirEntryMapLen {
				grantedLockSet.freeAll(false)
				errno = syscall.EEXIST
				return
			}

			ok, err = newDirInode.dirEntryMap.DeleteByKey(renameIn.NewName)
			if nil != err {
				globals.logger.Printf("func DoRename(,[Dir]NewName=%s) failed on .dirEntryMap.DeleteByKey(): %v", string(renameIn.NewName[:]), err)
				os.Exit(1)
			}
			if !ok {
				globals.logger.Printf("func DoRename(,[Dir]NewName=%s) .dirEntryMap.DeleteByKey() returned !ok", string(renameIn.NewName[:]))
				os.Exit(1)
			}

			newDirInode.attr.NLink--

			delete(globals.inodeMap, replacedInode.attr.Ino)
		}

		oldDirInode.attr.NLink--
		newDirInode.attr.NLink++

		ok, err = movedInode.dirEntryMap.PatchByKey([]byte(".."), newDirInode.attr.Ino)
		if nil != err {
			globals.logger.Printf("func DoRename() failed on .dirEntryMap.PatchByKey(): %v", err)
			os.Exit(1)
		}
		if !ok {
			globals.logger.Printf("func DoRename() .dirEntryMap.PatchByKey() returned !ok")
			os.Exit(1)
		}
	} else {
		if nil != replacedInode {
			if syscall.S_IFDIR == (movedInode.attr.Mode & syscall.S_IFMT) {
				grantedLockSet.freeAll(false)
				errno = syscall.EISDIR
				return
			}

			ok, err = newDirInode.dirEntryMap.DeleteByKey(renameIn.NewName)
			if nil != err {
				globals.logger.Printf("func DoRename(,[Non-Dir]NewName=%s) failed on .dirEntryMap.DeleteByKey(): %v", string(renameIn.NewName[:]), err)
				os.Exit(1)
			}
			if !ok {
				globals.logger.Printf("func DoRename(,[Non-Dir]NewName=%s) .dirEntryMap.DeleteByKey() returned !ok", string(renameIn.NewName[:]))
				os.Exit(1)
			}

			replacedInode.attr.NLink--

			if 0 == replacedInode.attr.NLink {
				delete(globals.inodeMap, replacedInode.attr.Ino)
			}
		}
	}

	ok, err = oldDirInode.dirEntryMap.DeleteByKey(renameIn.OldName)
	if nil != err {
		globals.logger.Printf("func DoRename(,OldName=%s) failed on .dirEntryMap.DeleteByKey(): %v", string(renameIn.OldName[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoRename() .dirEntryMap.DeleteByKey(,OldName=%s) returned !ok", string(renameIn.OldName[:]))
		os.Exit(1)
	}

	ok, err = newDirInode.dirEntryMap.Put(renameIn.NewName, movedInode.attr.Ino)
	if nil != err {
		globals.logger.Printf("func DoRename(,OldName=%s) failed on .dirEntryMap.Put(): %v", string(renameIn.NewName[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoRename(,NewName=%s) .dirEntryMap.Put() returned !ok", string(renameIn.NewName[:]))
		os.Exit(1)
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoLink(inHeader *fission.InHeader, linkIn *fission.LinkIn) (linkOut *fission.LinkOut, errno syscall.Errno) {
	var (
		dirInode       *inodeStruct
		err            error
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
		oldInode       *inodeStruct
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	_, ok, err = dirInode.dirEntryMap.GetByKey(linkIn.Name)
	if nil != err {
		globals.logger.Printf("func DoLink(NodeID==%v,Name=%s) failed on .dirEntryMap.GetByKey(): %v", inHeader.NodeID, string(linkIn.Name[:]), err)
		os.Exit(1)
	}

	if ok {
		grantedLockSet.freeAll(false)
		errno = syscall.EEXIST
		return
	}

	oldInode, ok = globals.inodeMap[linkIn.OldNodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(oldInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR == (oldInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.EISDIR
		return
	}

	ok, err = dirInode.dirEntryMap.Put(linkIn.Name, oldInode.attr.Ino)
	if nil != err {
		globals.logger.Printf("func DoLink(NodeID==%v,Name=%s) failed on .dirEntryMap.Put(): %v", inHeader.NodeID, string(linkIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoLink(NodeID==%v,Name=%s) .dirEntryMap.Put() returned !ok", inHeader.NodeID, string(linkIn.Name[:]))
		os.Exit(1)
	}

	oldInode.attr.NLink++

	linkOut = &fission.LinkOut{
		EntryOut: fission.EntryOut{
			NodeID:         oldInode.attr.Ino,
			Generation:     0,
			EntryValidSec:  0,
			AttrValidSec:   0,
			EntryValidNSec: 0,
			AttrValidNSec:  0,
			Attr: fission.Attr{
				Ino:       oldInode.attr.Ino,
				Size:      oldInode.attr.Size,
				Blocks:    oldInode.attr.Blocks,
				ATimeSec:  oldInode.attr.ATimeSec,
				MTimeSec:  oldInode.attr.MTimeSec,
				CTimeSec:  oldInode.attr.CTimeSec,
				ATimeNSec: oldInode.attr.ATimeNSec,
				MTimeNSec: oldInode.attr.MTimeNSec,
				CTimeNSec: oldInode.attr.CTimeNSec,
				Mode:      oldInode.attr.Mode,
				NLink:     oldInode.attr.NLink,
				UID:       oldInode.attr.UID,
				GID:       oldInode.attr.GID,
				RDev:      oldInode.attr.RDev,
				BlkSize:   oldInode.attr.BlkSize,
				Padding:   oldInode.attr.Padding,
			},
		},
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoOpen(inHeader *fission.InHeader, openIn *fission.OpenIn) (openOut *fission.OpenOut, errno syscall.Errno) {
	var (
		fileInode      *inodeStruct
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	fileInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(fileInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFREG != (fileInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.EINVAL
		return
	}

	if 0 != (openIn.Flags & fission.FOpenRequestTRUNC) {
		fileInode.attr.Size = 0
		fileInode.fileData = make([]byte, 0)
	}

	openOut = &fission.OpenOut{
		FH:        0,
		OpenFlags: fission.FOpenResponseDirectIO,
		Padding:   0,
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoRead(inHeader *fission.InHeader, readIn *fission.ReadIn) (readOut *fission.ReadOut, errno syscall.Errno) {
	var (
		fileInode          *inodeStruct
		granted            bool
		grantedLockSet     *grantedLockSetStruct = makeGrantedLockSet()
		ok                 bool
		readOffsetPlusSize uint64
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	fileInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(fileInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFREG != (fileInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.EINVAL
		return
	}

	readOffsetPlusSize = readIn.Offset + uint64(readIn.Size)

	if readIn.Offset < fileInode.attr.Size {
		if readOffsetPlusSize <= fileInode.attr.Size {
			readOut = &fission.ReadOut{
				Data: cloneByteSlice(fileInode.fileData[readIn.Offset:readOffsetPlusSize]),
			}
		} else {
			readOut = &fission.ReadOut{
				Data: cloneByteSlice(fileInode.fileData[readIn.Offset:]),
			}
		}
	} else {
		readOut = &fission.ReadOut{
			Data: make([]byte, 0),
		}
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoWrite(inHeader *fission.InHeader, writeIn *fission.WriteIn) (writeOut *fission.WriteOut, errno syscall.Errno) {
	var (
		fileInode           *inodeStruct
		granted             bool
		grantedLockSet      *grantedLockSetStruct = makeGrantedLockSet()
		ok                  bool
		overwriteSize       uint64
		writeOffsetPlusSize uint64
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	fileInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(fileInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFREG != (fileInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.EINVAL
		return
	}

	writeOffsetPlusSize = writeIn.Offset + uint64(writeIn.Size)

	if writeIn.Offset < fileInode.attr.Size {
		if writeOffsetPlusSize <= fileInode.attr.Size {
			_ = copy(fileInode.fileData[writeIn.Offset:writeOffsetPlusSize], writeIn.Data)
		} else {
			overwriteSize = fileInode.attr.Size - writeIn.Offset

			_ = copy(fileInode.fileData[writeIn.Offset:], writeIn.Data[:overwriteSize])
			fileInode.fileData = append(fileInode.fileData, writeIn.Data[overwriteSize:]...)

			fileInode.attr.Size = writeOffsetPlusSize
		}
	} else {
		if writeIn.Offset > fileInode.attr.Size {
			fileInode.fileData = append(fileInode.fileData, make([]byte, (writeIn.Offset-fileInode.attr.Size))...)
		}

		fileInode.fileData = append(fileInode.fileData, writeIn.Data...)

		fileInode.attr.Size = writeOffsetPlusSize
	}

	grantedLockSet.freeAll(false)

	writeOut = &fission.WriteOut{
		Size:    writeIn.Size,
		Padding: 0,
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoStatFS(inHeader *fission.InHeader) (statFSOut *fission.StatFSOut, errno syscall.Errno) {
	statFSOut = &fission.StatFSOut{
		KStatFS: fission.KStatFS{
			Blocks:  0,
			BFree:   0,
			BAvail:  0,
			Files:   0,
			FFree:   0,
			BSize:   0,
			NameLen: 0,
			FRSize:  0,
			Padding: 0,
			Spare:   [6]uint32{0, 0, 0, 0, 0, 0},
		},
	}

	// TODO: Fill in the StatFSOut.KStatFS above correctly

	errno = 0
	return
}

func (dummy *globalsStruct) DoRelease(inHeader *fission.InHeader, releaseIn *fission.ReleaseIn) (errno syscall.Errno) {
	var (
		fileInode      *inodeStruct
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	fileInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(fileInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFREG != (fileInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.EINVAL
		return
	}

	if 0 == fileInode.attr.NLink {
		delete(globals.inodeMap, inHeader.NodeID)
		// Note: Other threads could still be blocked obtaining a lock on fileInode
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoFSync(inHeader *fission.InHeader, fSyncIn *fission.FSyncIn) (errno syscall.Errno) {
	var (
		fileInode      *inodeStruct
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	fileInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(fileInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFREG != (fileInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.EINVAL
		return
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoSetXAttr(inHeader *fission.InHeader, setXAttrIn *fission.SetXAttrIn) (errno syscall.Errno) {
	var (
		err            error
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		inode          *inodeStruct
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(inode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	grantedLockSet.free(globals.tryLock)

	ok, err = inode.xattrMap.PatchByKey(setXAttrIn.Name, setXAttrIn.Data)
	if nil != err {
		globals.logger.Printf("func DoSetXAttr(NodeID==%v, Name==%s) failed on .xattrMap.PatchByKey(): %v", inHeader.NodeID, string(setXAttrIn.Name[:]), err)
		os.Exit(1)
	}

	if !ok {
		ok, err = inode.xattrMap.Put(setXAttrIn.Name, setXAttrIn.Data)
		if nil != err {
			globals.logger.Printf("func DoSetXAttr(NodeID==%v, Name==%s) failed on .xattrMap.Put(): %v", inHeader.NodeID, string(setXAttrIn.Name[:]), err)
			os.Exit(1)
		}
		if !ok {
			globals.logger.Printf("func DoSetXAttr(NodeID==%v, Name==%s) .xattrMap.Put() returned !ok", inHeader.NodeID, string(setXAttrIn.Name[:]))
			os.Exit(1)
		}
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoGetXAttr(inHeader *fission.InHeader, getXAttrIn *fission.GetXAttrIn) (getXAttrOut *fission.GetXAttrOut, errno syscall.Errno) {
	var (
		dataAsByteSlice []byte
		dataAsValue     sortedmap.Value
		err             error
		granted         bool
		grantedLockSet  *grantedLockSetStruct = makeGrantedLockSet()
		inode           *inodeStruct
		ok              bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(inode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	grantedLockSet.free(globals.tryLock)

	dataAsValue, ok, err = inode.xattrMap.GetByKey(getXAttrIn.Name)
	if nil != err {
		globals.logger.Printf("func DoGetXAttr(NodeID==%v) failed on .xattrMap.GetByKey(): %v", inHeader.NodeID, err)
		os.Exit(1)
	}

	grantedLockSet.freeAll(false)

	if !ok {
		errno = syscall.ENODATA
		return
	}

	dataAsByteSlice = dataAsValue.([]byte)

	if 0 == getXAttrIn.Size {
		getXAttrOut = &fission.GetXAttrOut{
			Size:    uint32(len(dataAsByteSlice)),
			Padding: 0,
			Data:    make([]byte, 0),
		}
		errno = 0
		return
	}

	if uint32(len(dataAsByteSlice)) > getXAttrIn.Size {
		errno = syscall.ERANGE
		return
	}

	getXAttrOut = &fission.GetXAttrOut{
		Size:    uint32(len(dataAsByteSlice)),
		Padding: 0,
		Data:    cloneByteSlice(dataAsByteSlice),
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoListXAttr(inHeader *fission.InHeader, listXAttrIn *fission.ListXAttrIn) (listXAttrOut *fission.ListXAttrOut, errno syscall.Errno) {
	var (
		err                  error
		granted              bool
		grantedLockSet       *grantedLockSetStruct = makeGrantedLockSet()
		inode                *inodeStruct
		ok                   bool
		totalSize            uint32
		xattrCount           int
		xattrIndex           int
		xattrNameAsByteSlice []byte
		xattrNameAsKey       sortedmap.Key
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(inode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	grantedLockSet.free(globals.tryLock)

	listXAttrOut = &fission.ListXAttrOut{
		Size:    0,
		Padding: 0,
		Name:    make([][]byte, 0),
	}

	xattrCount, err = inode.xattrMap.Len()
	if nil != err {
		globals.logger.Printf("func DoListXAttr(NodeID==%v) failed on .dirEntryMap.Len(): %v", inHeader.NodeID, err)
		os.Exit(1)
	}

	totalSize = 0

	for xattrIndex = 0; xattrIndex < xattrCount; xattrIndex++ {
		xattrNameAsKey, _, ok, err = inode.xattrMap.GetByIndex(xattrIndex)
		if nil != err {
			globals.logger.Printf("func DoGetXAttr(NodeID==%v) failed on .xattrMap.GetByIndex(%d): %v", inHeader.NodeID, xattrIndex, err)
			os.Exit(1)
		}
		if !ok {
			globals.logger.Printf("func DoGetXAttr(NodeID==%v) .xattrMap.GetByIndex(%d) returned !ok", inHeader.NodeID, xattrIndex)
			os.Exit(1)
		}

		xattrNameAsByteSlice = xattrNameAsKey.([]byte)

		if 0 != listXAttrIn.Size {
			if (totalSize + uint32(len(xattrNameAsByteSlice)+1)) > listXAttrIn.Size {
				grantedLockSet.freeAll(false)
				errno = syscall.ERANGE
				return
			}
		}

		totalSize += uint32(len(xattrNameAsByteSlice) + 1)

		if 0 != listXAttrIn.Size {
			listXAttrOut.Name = append(listXAttrOut.Name, xattrNameAsByteSlice)
		}
	}

	grantedLockSet.freeAll(false)

	listXAttrOut.Size = totalSize

	errno = 0
	return
}

func (dummy *globalsStruct) DoRemoveXAttr(inHeader *fission.InHeader, removeXAttrIn *fission.RemoveXAttrIn) (errno syscall.Errno) {
	var (
		err            error
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		inode          *inodeStruct
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(inode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	grantedLockSet.free(globals.tryLock)

	ok, err = inode.xattrMap.DeleteByKey(removeXAttrIn.Name)
	if nil != err {
		globals.logger.Printf("func DoRemoveXAttr(NodeID==%v, Name==%s) failed on .xattrMap.DeleteByKey(): %v", inHeader.NodeID, string(removeXAttrIn.Name[:]), err)
		os.Exit(1)
	}

	grantedLockSet.freeAll(false)

	if !ok {
		errno = syscall.ENOENT
		return
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoFlush(inHeader *fission.InHeader, flushIn *fission.FlushIn) (errno syscall.Errno) {
	var (
		fileInode      *inodeStruct
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	fileInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(fileInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFREG != (fileInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.EINVAL
		return
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoInit(inHeader *fission.InHeader, initIn *fission.InitIn) (initOut *fission.InitOut, errno syscall.Errno) {
	initOut = &fission.InitOut{
		Major:                initIn.Major,
		Minor:                initIn.Minor,
		MaxReadAhead:         initIn.MaxReadAhead,
		Flags:                initOutFlagsNearlyAll,
		MaxBackground:        initOutMaxBackgound,
		CongestionThreshhold: initOutCongestionThreshhold,
		MaxWrite:             initOutMaxWrite,
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoOpenDir(inHeader *fission.InHeader, openDirIn *fission.OpenDirIn) (openDirOut *fission.OpenDirOut, errno syscall.Errno) {
	var (
		dirInode       *inodeStruct
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	openDirOut = &fission.OpenDirOut{
		FH:        0,
		OpenFlags: 0,
		Padding:   0,
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoReadDir(inHeader *fission.InHeader, readDirIn *fission.ReadDirIn) (readDirOut *fission.ReadDirOut, errno syscall.Errno) {
	var (
		dirEntCount           int
		dirEntIndex           int
		dirEntInoAsU64        uint64
		dirEntInoAsValue      sortedmap.Value
		dirEntInode           *inodeStruct
		dirEntNameAsByteSlice []byte
		dirEntNameAsKey       sortedmap.Key
		dirEntNameLenAligned  uint32
		dirEntSize            uint32
		dirInode              *inodeStruct
		err                   error
		granted               bool
		grantedLockSet        *grantedLockSetStruct = makeGrantedLockSet()
		totalSize             uint32
		ok                    bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	dirEntCount, err = dirInode.dirEntryMap.Len()
	if nil != err {
		globals.logger.Printf("func DoReadDir(NodeID==%v) failed on .dirEntryMap.Len(): %v", inHeader.NodeID, err)
		os.Exit(1)
	}

	if uint64(dirEntCount) < readDirIn.Offset {
		// Just return an empty ReadDirOut

		grantedLockSet.freeAll(false)

		readDirOut = &fission.ReadDirOut{
			DirEnt: make([]fission.DirEnt, 0),
		}

		errno = 0
		return
	}

	// Just compute the maximal ReadDirOut... we'll prune it later

	readDirOut = &fission.ReadDirOut{
		DirEnt: make([]fission.DirEnt, dirEntCount),
	}

	for dirEntIndex = 0; dirEntIndex < dirEntCount; dirEntIndex++ {
		dirEntNameAsKey, dirEntInoAsValue, ok, err = dirInode.dirEntryMap.GetByIndex(dirEntIndex)
		if nil != err {
			globals.logger.Printf("func DoReadDir(NodeID==%v) failed on .dirEntryMap.GetByIndex(): %v", inHeader.NodeID, err)
			os.Exit(1)
		}
		if !ok {
			globals.logger.Printf("func DoReadDir(NodeID==%v) .dirEntryMap.GetByIndex() returned !ok", inHeader.NodeID)
			os.Exit(1)
		}

		dirEntNameAsByteSlice = dirEntNameAsKey.([]byte)
		dirEntInoAsU64 = dirEntInoAsValue.(uint64)

		dirEntInode, ok = globals.inodeMap[dirEntInoAsU64]
		if !ok {
			globals.logger.Printf("func DoReadDir(NodeID==%v) failed fetching globals.inodeMap[%v]", inHeader.NodeID, dirEntInoAsU64)
			os.Exit(1)
		}

		granted = grantedLockSet.try(dirEntInode.tryLock)
		if !granted {
			grantedLockSet.freeAll(true)
			goto Restart
		}

		readDirOut.DirEnt[dirEntIndex] = fission.DirEnt{
			Ino:     dirEntInode.attr.Ino,
			Off:     uint64(dirEntIndex) + 1,
			NameLen: uint32(len(dirEntNameAsByteSlice)), // unnecessary
			Type:    dirEntInode.attr.Mode & syscall.S_IFMT,
			Name:    cloneByteSlice(dirEntNameAsByteSlice),
		}
	}

	grantedLockSet.freeAll(false)

	// Now prune on the left to readDirIn.Offset & on the right anything beyond readDirIn.Size

	readDirOut.DirEnt = readDirOut.DirEnt[readDirIn.Offset:]

	totalSize = 0

	for dirEntIndex = 0; dirEntIndex < len(readDirOut.DirEnt); dirEntIndex++ {
		dirEntNameLenAligned = (uint32(len(readDirOut.DirEnt[dirEntIndex].Name)) + (fission.DirEntAlignment - 1)) & ^uint32(fission.DirEntAlignment-1)
		dirEntSize = fission.DirEntFixedPortionSize + dirEntNameLenAligned

		if (totalSize + dirEntSize) > readDirIn.Size {
			// Truncate readDirOut here and return

			readDirOut.DirEnt = readDirOut.DirEnt[:dirEntIndex]

			errno = 0
			return
		}

		totalSize += dirEntSize
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoReleaseDir(inHeader *fission.InHeader, releaseDirIn *fission.ReleaseDirIn) (errno syscall.Errno) {
	var (
		dirInode       *inodeStruct
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	if 0 == dirInode.attr.NLink {
		delete(globals.inodeMap, inHeader.NodeID)
		// Note: Other threads could still be blocked obtaining a lock on dirInode
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoFSyncDir(inHeader *fission.InHeader, fSyncDirIn *fission.FSyncDirIn) (errno syscall.Errno) {
	var (
		fileInode      *inodeStruct
		granted        bool
		grantedLockSet *grantedLockSetStruct = makeGrantedLockSet()
		ok             bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	fileInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(fileInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (fileInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoGetLK(inHeader *fission.InHeader, getLKIn *fission.GetLKIn) (getLKOut *fission.GetLKOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSetLK(inHeader *fission.InHeader, setLKIn *fission.SetLKIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoSetLKW(inHeader *fission.InHeader, setLKWIn *fission.SetLKWIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoAccess(inHeader *fission.InHeader, accessIn *fission.AccessIn) (errno syscall.Errno) {
	var (
		executeGrantedOrNotRequested bool
		executeRequested             bool
		granted                      bool
		grantedLockSet               *grantedLockSetStruct = makeGrantedLockSet()
		inode                        *inodeStruct
		inodeAttrGID                 uint32
		inodeAttrMode                uint32
		inodeAttrModeGroup           uint32
		inodeAttrModeOther           uint32
		inodeAttrModeOwner           uint32
		inodeAttrUID                 uint32
		isInodeGroup                 bool
		isInodeOwner                 bool
		isRoot                       bool
		ok                           bool
		readGrantedOrNotRequested    bool
		readRequested                bool
		writeGrantedOrNotRequested   bool
		writeRequested               bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(inode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	inodeAttrUID = inode.attr.UID
	inodeAttrGID = inode.attr.GID

	inodeAttrMode = inode.attr.Mode

	inodeAttrModeOwner = inodeAttrMode >> accessOwnerShift
	inodeAttrModeGroup = inodeAttrMode >> accessGroupShift
	inodeAttrModeOther = inodeAttrMode >> accessOtherShift

	grantedLockSet.freeAll(false)

	isRoot = (inHeader.UID == uint32(0))

	isInodeOwner = (inHeader.UID == inodeAttrUID)
	isInodeGroup = (inHeader.GID == inodeAttrGID)

	readRequested = (0 != (accessIn.Mask & accessROK))
	writeRequested = (0 != (accessIn.Mask & accessWOK))
	executeRequested = (0 != (accessIn.Mask & accessXOK))

	if readRequested {
		if isRoot {
			readGrantedOrNotRequested = true
		} else {
			readGrantedOrNotRequested = false
			if isInodeOwner && (0 != (inodeAttrModeOwner & accessROK)) {
				readGrantedOrNotRequested = true
			}
			if isInodeGroup && (0 != (inodeAttrModeGroup & accessROK)) {
				readGrantedOrNotRequested = true
			}
			if 0 != (inodeAttrModeOther & accessROK) {
				readGrantedOrNotRequested = true
			}
		}
	} else {
		readGrantedOrNotRequested = true
	}

	if writeRequested {
		if isRoot {
			writeGrantedOrNotRequested = true
		} else {
			writeGrantedOrNotRequested = false
			if isInodeOwner && (0 != (inodeAttrModeOwner & accessWOK)) {
				writeGrantedOrNotRequested = true
			}
			if isInodeGroup && (0 != (inodeAttrModeGroup & accessWOK)) {
				writeGrantedOrNotRequested = true
			}
			if 0 != (inodeAttrModeOther & accessWOK) {
				writeGrantedOrNotRequested = true
			}
		}
	} else {
		writeGrantedOrNotRequested = true
	}

	if executeRequested {
		if isRoot {
			executeGrantedOrNotRequested = false
			if 0 != (inodeAttrModeOwner & accessXOK) {
				executeGrantedOrNotRequested = true
			}
			if 0 != (inodeAttrModeGroup & accessXOK) {
				executeGrantedOrNotRequested = true
			}
			if 0 != (inodeAttrModeOther & accessXOK) {
				executeGrantedOrNotRequested = true
			}
		} else {
			executeGrantedOrNotRequested = false
			if isInodeOwner && (0 != (inodeAttrModeOwner & accessXOK)) {
				executeGrantedOrNotRequested = true
			}
			if isInodeGroup && (0 != (inodeAttrModeGroup & accessXOK)) {
				executeGrantedOrNotRequested = true
			}
			if 0 != (inodeAttrModeOther & accessXOK) {
				executeGrantedOrNotRequested = true
			}
		}
	} else {
		executeGrantedOrNotRequested = true
	}

	if readGrantedOrNotRequested && writeGrantedOrNotRequested && executeGrantedOrNotRequested {
		errno = 0
	} else {
		errno = syscall.EACCES
	}

	return
}

func (dummy *globalsStruct) DoCreate(inHeader *fission.InHeader, createIn *fission.CreateIn) (createOut *fission.CreateOut, errno syscall.Errno) {
	var (
		dirInode        *inodeStruct
		err             error
		fileInode       *inodeStruct
		fileInodeMode   uint32
		granted         bool
		grantedLockSet  *grantedLockSetStruct = makeGrantedLockSet()
		ok              bool
		unixTimeNowNSec uint32
		unixTimeNowSec  uint64
	)

	fileInodeMode = uint32(syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO)
	fileInodeMode &= createIn.Mode
	fileInodeMode &= ^createIn.UMask
	fileInodeMode |= syscall.S_IFREG

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	_, ok, err = dirInode.dirEntryMap.GetByKey(createIn.Name)
	if nil != err {
		globals.logger.Printf("func DoCreate(NodeID==%v,Name=%s) failed on .dirEntryMap.GetByKey(): %v", inHeader.NodeID, string(createIn.Name[:]), err)
		os.Exit(1)
	}

	if ok {
		grantedLockSet.freeAll(false)
		errno = syscall.EEXIST
		return
	}

	globals.lastNodeID++

	unixTimeNowSec, unixTimeNowNSec = unixTimeNow()

	fileInode = &inodeStruct{
		tryLock: makeTryLock(),
		attr: fission.Attr{
			Ino:       globals.lastNodeID,
			Size:      0,
			Blocks:    0,
			ATimeSec:  unixTimeNowSec,
			MTimeSec:  unixTimeNowSec,
			CTimeSec:  unixTimeNowSec,
			ATimeNSec: unixTimeNowNSec,
			MTimeNSec: unixTimeNowNSec,
			CTimeNSec: unixTimeNowNSec,
			Mode:      fileInodeMode,
			NLink:     1,
			UID:       inHeader.UID,
			GID:       inHeader.GID,
			RDev:      0,
			BlkSize:   attrBlkSize,
			Padding:   0,
		},
		xattrMap:    sortedmap.NewLLRBTree(sortedmap.CompareByteSlice, globals.xattrMapDummy),
		dirEntryMap: nil,
		fileData:    make([]byte, 0),
		symlinkData: nil,
	}

	ok, err = dirInode.dirEntryMap.Put(createIn.Name, fileInode.attr.Ino)
	if nil != err {
		globals.logger.Printf("func DoCreate(NodeID==%v,Name=%s) failed on .dirEntryMap.Put(): %v", inHeader.NodeID, string(createIn.Name[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoCreate(NodeID==%v,Name=%s) .dirEntryMap.Put() returned !ok", inHeader.NodeID, string(createIn.Name[:]))
		os.Exit(1)
	}

	globals.inodeMap[fileInode.attr.Ino] = fileInode

	createOut = &fission.CreateOut{
		EntryOut: fission.EntryOut{
			NodeID:         fileInode.attr.Ino,
			Generation:     0,
			EntryValidSec:  0,
			AttrValidSec:   0,
			EntryValidNSec: 0,
			AttrValidNSec:  0,
			Attr: fission.Attr{
				Ino:       fileInode.attr.Ino,
				Size:      fileInode.attr.Size,
				Blocks:    fileInode.attr.Blocks,
				ATimeSec:  fileInode.attr.ATimeSec,
				MTimeSec:  fileInode.attr.MTimeSec,
				CTimeSec:  fileInode.attr.CTimeSec,
				ATimeNSec: fileInode.attr.ATimeNSec,
				MTimeNSec: fileInode.attr.MTimeNSec,
				CTimeNSec: fileInode.attr.CTimeNSec,
				Mode:      fileInode.attr.Mode,
				NLink:     fileInode.attr.NLink,
				UID:       fileInode.attr.UID,
				GID:       fileInode.attr.GID,
				RDev:      fileInode.attr.RDev,
				BlkSize:   fileInode.attr.BlkSize,
				Padding:   fileInode.attr.Padding,
			},
		},
		FH:        0,
		OpenFlags: fission.FOpenResponseDirectIO,
		Padding:   0,
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoInterrupt(inHeader *fission.InHeader, interruptIn *fission.InterruptIn) {
	return
}

func (dummy *globalsStruct) DoBMap(inHeader *fission.InHeader, bMapIn *fission.BMapIn) (bMapOut *fission.BMapOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoDestroy(inHeader *fission.InHeader) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoPoll(inHeader *fission.InHeader, pollIn *fission.PollIn) (pollOut *fission.PollOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoBatchForget(inHeader *fission.InHeader, batchForgetIn *fission.BatchForgetIn) {
	return
}

func (dummy *globalsStruct) DoFAllocate(inHeader *fission.InHeader, fAllocateIn *fission.FAllocateIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (dummy *globalsStruct) DoReadDirPlus(inHeader *fission.InHeader, readDirPlusIn *fission.ReadDirPlusIn) (readDirPlusOut *fission.ReadDirPlusOut, errno syscall.Errno) {
	var (
		dirEntInoAsU64        uint64
		dirEntInoAsValue      sortedmap.Value
		dirEntInode           *inodeStruct
		dirEntNameAsByteSlice []byte
		dirEntNameAsKey       sortedmap.Key
		dirEntNameLenAligned  uint32
		dirEntSize            uint32
		dirEntPlusCount       int
		dirEntPlusIndex       int
		dirInode              *inodeStruct
		err                   error
		granted               bool
		grantedLockSet        *grantedLockSetStruct = makeGrantedLockSet()
		totalSize             uint32
		ok                    bool
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	dirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(dirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (dirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	dirEntPlusCount, err = dirInode.dirEntryMap.Len()
	if nil != err {
		globals.logger.Printf("func DoReadDirPlus(NodeID==%v) failed on .dirEntryMap.Len(): %v", inHeader.NodeID, err)
		os.Exit(1)
	}

	if uint64(dirEntPlusCount) < readDirPlusIn.Offset {
		// Just return an empty ReadDirPlusOut

		grantedLockSet.freeAll(false)

		readDirPlusOut = &fission.ReadDirPlusOut{
			DirEntPlus: make([]fission.DirEntPlus, 0),
		}

		errno = 0
		return
	}

	// Just compute the maximal ReadDirPlusOut... we'll prune it later

	readDirPlusOut = &fission.ReadDirPlusOut{
		DirEntPlus: make([]fission.DirEntPlus, dirEntPlusCount),
	}

	for dirEntPlusIndex = 0; dirEntPlusIndex < dirEntPlusCount; dirEntPlusIndex++ {
		dirEntNameAsKey, dirEntInoAsValue, ok, err = dirInode.dirEntryMap.GetByIndex(dirEntPlusIndex)
		if nil != err {
			globals.logger.Printf("func DoReadDirPlus(NodeID==%v) failed on .dirEntryMap.GetByIndex(): %v", inHeader.NodeID, err)
			os.Exit(1)
		}
		if !ok {
			globals.logger.Printf("func DoReadDirPlus(NodeID==%v) .dirEntryMap.GetByIndex() returned !ok", inHeader.NodeID)
			os.Exit(1)
		}

		dirEntNameAsByteSlice = dirEntNameAsKey.([]byte)
		dirEntInoAsU64 = dirEntInoAsValue.(uint64)

		dirEntInode, ok = globals.inodeMap[dirEntInoAsU64]
		if !ok {
			globals.logger.Printf("func DoReadDirPlus(NodeID==%v) failed fetching globals.inodeMap[%v]", inHeader.NodeID, dirEntInoAsU64)
			os.Exit(1)
		}

		granted = grantedLockSet.try(dirEntInode.tryLock)
		if !granted {
			grantedLockSet.freeAll(true)
			goto Restart
		}

		readDirPlusOut.DirEntPlus[dirEntPlusIndex] = fission.DirEntPlus{
			EntryOut: fission.EntryOut{
				NodeID:         dirEntInode.attr.Ino,
				Generation:     0,
				EntryValidSec:  0,
				AttrValidSec:   0,
				EntryValidNSec: 0,
				AttrValidNSec:  0,
				Attr: fission.Attr{
					Ino:       dirEntInode.attr.Ino,
					Size:      dirEntInode.attr.Size,
					Blocks:    dirEntInode.attr.Blocks,
					ATimeSec:  dirEntInode.attr.ATimeSec,
					MTimeSec:  dirEntInode.attr.MTimeSec,
					CTimeSec:  dirEntInode.attr.CTimeSec,
					ATimeNSec: dirEntInode.attr.ATimeNSec,
					MTimeNSec: dirEntInode.attr.MTimeNSec,
					CTimeNSec: dirEntInode.attr.CTimeNSec,
					Mode:      dirEntInode.attr.Mode,
					NLink:     dirEntInode.attr.NLink,
					UID:       dirEntInode.attr.UID,
					GID:       dirEntInode.attr.GID,
					RDev:      dirEntInode.attr.RDev,
					BlkSize:   dirEntInode.attr.BlkSize,
					Padding:   dirEntInode.attr.Padding,
				},
			},
			DirEnt: fission.DirEnt{
				Ino:     dirEntInode.attr.Ino,
				Off:     uint64(dirEntPlusIndex) + 1,
				NameLen: uint32(len(dirEntNameAsByteSlice)), // unnecessary
				Type:    dirEntInode.attr.Mode & syscall.S_IFMT,
				Name:    cloneByteSlice(dirEntNameAsByteSlice),
			},
		}
	}

	grantedLockSet.freeAll(false)

	// Now prune on the left to readDirPlusIn.Offset & on the right anything beyond readDirPlusIn.Size

	readDirPlusOut.DirEntPlus = readDirPlusOut.DirEntPlus[readDirPlusIn.Offset:]

	totalSize = 0

	for dirEntPlusIndex = 0; dirEntPlusIndex < len(readDirPlusOut.DirEntPlus); dirEntPlusIndex++ {
		dirEntNameLenAligned = (uint32(len(readDirPlusOut.DirEntPlus[dirEntPlusIndex].Name)) + (fission.DirEntAlignment - 1)) & ^uint32(fission.DirEntAlignment-1)
		dirEntSize = fission.DirEntPlusFixedPortionSize + dirEntNameLenAligned

		if (totalSize + dirEntSize) > readDirPlusIn.Size {
			// Truncate readDirPlusOut here and return

			readDirPlusOut.DirEntPlus = readDirPlusOut.DirEntPlus[:dirEntPlusIndex]

			errno = 0
			return
		}

		totalSize += dirEntSize
	}

	errno = 0
	return
}

func (dummy *globalsStruct) DoRename2(inHeader *fission.InHeader, rename2In *fission.Rename2In) (errno syscall.Errno) {
	var (
		err                         error
		granted                     bool
		grantedLockSet              *grantedLockSetStruct = makeGrantedLockSet()
		movedInode                  *inodeStruct
		movedInodeNodeIDAsU64       uint64
		movedInodeNodeIDAsValue     sortedmap.Value
		newDirInode                 *inodeStruct
		ok                          bool
		oldDirInode                 *inodeStruct
		replacedInode               *inodeStruct
		replacedInodeDirEntryMapLen int
		replacedInodeNodeIDAsU64    uint64
		replacedInodeNodeIDAsValue  sortedmap.Value
	)

Restart:
	grantedLockSet.get(globals.tryLock)

	oldDirInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	granted = grantedLockSet.try(oldDirInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	if syscall.S_IFDIR != (oldDirInode.attr.Mode & syscall.S_IFMT) {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOTDIR
		return
	}

	if inHeader.NodeID == rename2In.NewDir {
		newDirInode = oldDirInode
	} else {
		newDirInode, ok = globals.inodeMap[rename2In.NewDir]
		if !ok {
			grantedLockSet.freeAll(false)
			errno = syscall.ENOENT
			return
		}

		granted = grantedLockSet.try(newDirInode.tryLock)
		if !granted {
			grantedLockSet.freeAll(true)
			goto Restart
		}

		if syscall.S_IFDIR != (newDirInode.attr.Mode & syscall.S_IFMT) {
			grantedLockSet.freeAll(false)
			errno = syscall.ENOTDIR
			return
		}
	}

	movedInodeNodeIDAsValue, ok, err = oldDirInode.dirEntryMap.GetByKey(rename2In.OldName)
	if nil != err {
		globals.logger.Printf("func DoRename2(,OldName=%s) failed on .dirEntryMap.GetByKey(): %v", string(rename2In.OldName[:]), err)
		os.Exit(1)
	}
	if !ok {
		grantedLockSet.freeAll(false)
		errno = syscall.ENOENT
		return
	}

	movedInodeNodeIDAsU64 = movedInodeNodeIDAsValue.(uint64)

	movedInode, ok = globals.inodeMap[movedInodeNodeIDAsU64]
	if !ok {
		globals.logger.Printf("func DoRename2(,OldName=%s) globals.inodeMap[movedInodeNodeIDAsU64] returned !ok", string(rename2In.OldName[:]))
		os.Exit(1)
	}

	granted = grantedLockSet.try(movedInode.tryLock)
	if !granted {
		grantedLockSet.freeAll(true)
		goto Restart
	}

	replacedInodeNodeIDAsValue, ok, err = newDirInode.dirEntryMap.GetByKey(rename2In.NewName)
	if nil != err {
		globals.logger.Printf("func DoRename2(,NewName=%s) failed on .dirEntryMap.GetByKey(): %v", string(rename2In.NewName[:]), err)
		os.Exit(1)
	}

	if ok {
		replacedInodeNodeIDAsU64 = replacedInodeNodeIDAsValue.(uint64)

		replacedInode, ok = globals.inodeMap[replacedInodeNodeIDAsU64]
		if !ok {
			globals.logger.Printf("func DoRename2(,NewName=%s) globals.inodeMap[replacedInodeNodeIDAsU64] returned !ok", string(rename2In.NewName[:]))
			os.Exit(1)
		}

		granted = grantedLockSet.try(movedInode.tryLock)
		if !granted {
			grantedLockSet.freeAll(true)
			goto Restart
		}
	} else {
		replacedInode = nil
	}

	if syscall.S_IFDIR == (movedInode.attr.Mode & syscall.S_IFMT) {
		if nil != replacedInode {
			if syscall.S_IFDIR != (movedInode.attr.Mode & syscall.S_IFMT) {
				grantedLockSet.freeAll(false)
				errno = syscall.ENOTDIR
				return
			}

			replacedInodeDirEntryMapLen, err = replacedInode.dirEntryMap.Len()
			if nil != err {
				globals.logger.Printf("func DoRename2(,NewName=%s) failed on .dirEntryMap.Len(): %v", string(rename2In.NewName[:]), err)
				os.Exit(1)
			}

			if 2 != replacedInodeDirEntryMapLen {
				grantedLockSet.freeAll(false)
				errno = syscall.EEXIST
				return
			}

			ok, err = newDirInode.dirEntryMap.DeleteByKey(rename2In.NewName)
			if nil != err {
				globals.logger.Printf("func DoRename2(,[Dir]NewName=%s) failed on .dirEntryMap.DeleteByKey(): %v", string(rename2In.NewName[:]), err)
				os.Exit(1)
			}
			if !ok {
				globals.logger.Printf("func DoRename2(,[Dir]NewName=%s) .dirEntryMap.DeleteByKey() returned !ok", string(rename2In.NewName[:]))
				os.Exit(1)
			}

			newDirInode.attr.NLink--

			delete(globals.inodeMap, replacedInode.attr.Ino)
		}

		oldDirInode.attr.NLink--
		newDirInode.attr.NLink++

		ok, err = movedInode.dirEntryMap.PatchByKey([]byte(".."), newDirInode.attr.Ino)
		if nil != err {
			globals.logger.Printf("func DoRename2() failed on .dirEntryMap.PatchByKey(): %v", err)
			os.Exit(1)
		}
		if !ok {
			globals.logger.Printf("func DoRename2() .dirEntryMap.PatchByKey() returned !ok")
			os.Exit(1)
		}
	} else {
		if nil != replacedInode {
			if syscall.S_IFDIR == (movedInode.attr.Mode & syscall.S_IFMT) {
				grantedLockSet.freeAll(false)
				errno = syscall.EISDIR
				return
			}

			ok, err = newDirInode.dirEntryMap.DeleteByKey(rename2In.NewName)
			if nil != err {
				globals.logger.Printf("func DoRename2(,[Non-Dir]NewName=%s) failed on .dirEntryMap.DeleteByKey(): %v", string(rename2In.NewName[:]), err)
				os.Exit(1)
			}
			if !ok {
				globals.logger.Printf("func DoRename2(,[Non-Dir]NewName=%s) .dirEntryMap.DeleteByKey() returned !ok", string(rename2In.NewName[:]))
				os.Exit(1)
			}

			replacedInode.attr.NLink--

			if 0 == replacedInode.attr.NLink {
				delete(globals.inodeMap, replacedInode.attr.Ino)
			}
		}
	}

	ok, err = oldDirInode.dirEntryMap.DeleteByKey(rename2In.OldName)
	if nil != err {
		globals.logger.Printf("func DoRename2(,OldName=%s) failed on .dirEntryMap.DeleteByKey(): %v", string(rename2In.OldName[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoRename2() .dirEntryMap.DeleteByKey(,OldName=%s) returned !ok", string(rename2In.OldName[:]))
		os.Exit(1)
	}

	ok, err = newDirInode.dirEntryMap.Put(rename2In.NewName, movedInode.attr.Ino)
	if nil != err {
		globals.logger.Printf("func DoRename2(,OldName=%s) failed on .dirEntryMap.Put(): %v", string(rename2In.NewName[:]), err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("func DoRename2(,NewName=%s) .dirEntryMap.Put() returned !ok", string(rename2In.NewName[:]))
		os.Exit(1)
	}

	grantedLockSet.freeAll(false)

	errno = 0
	return
}

func (dummy *globalsStruct) DoLSeek(inHeader *fission.InHeader, lSeekIn *fission.LSeekIn) (lSeekOut *fission.LSeekOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
