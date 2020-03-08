package inode

import (
	"fmt"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/utils"
)

func (vS *volumeStruct) createRootOrSubDir(filePerm InodeMode, userID InodeUserID, groupID InodeGroupID, isRootDir bool) (dirInodeNumber InodeNumber, err error) {
	// Create file mode out of file permissions plus inode type
	fileMode, err := determineMode(filePerm, DirType)
	if nil != err {
		return
	}

	var dirInode *inMemoryInodeStruct

	if isRootDir {
		dirInode = vS.makeInMemoryInodeWithThisInodeNumber(DirType, fileMode, userID, groupID, RootDirInodeNumber, true)
		dirInodeNumber = RootDirInodeNumber
	} else {
		dirInode, err = vS.makeInMemoryInode(DirType, fileMode, userID, groupID)
		if nil != err {
			return
		}
		dirInodeNumber = dirInode.InodeNumber
	}

	dirInode.dirty = true

	// sorted map from directory entry name (a string) to InodeNumber

	dirMapping :=
		sortedmap.NewBPlusTree(
			vS.maxEntriesPerDirNode,
			sortedmap.CompareString,
			&dirInodeCallbacks{treeNodeLoadable{inode: dirInode}},
			globals.dirEntryCache)

	ok, err := dirMapping.Put(".", dirInode.InodeNumber)
	if (nil != err) || (!ok) {
		panic(err)
	}

	if isRootDir {
		ok, err = dirMapping.Put("..", dirInode.InodeNumber)
		if (nil != err) || (!ok) {
			panic(err)
		}

		dirInode.LinkCount = 2
	} else {
		dirInode.LinkCount = 1
	}

	dirInode.payload = dirMapping

	if isRootDir {
		// If creating RootDir, since this must be atomic, caller already holds vS.Mutex
		ok, err = vS.inodeCacheInsertWhileLocked(dirInode)
	} else {
		ok, err = vS.inodeCacheInsert(dirInode)
	}
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("inodeCacheInsert(dirInode) failed")
		return
	}

	// If creating RootDir, force an immediate flush to ensure it is atomically created as well
	if isRootDir {
		err = vS.flushInode(dirInode)
		if nil != err {
			logger.ErrorfWithError(err, "createRootOrSubDir() call to flushInode() failed")
			return
		}
	}

	err = nil
	return
}

func (vS *volumeStruct) CreateDir(filePerm InodeMode, userID InodeUserID, groupID InodeGroupID) (dirInodeNumber InodeNumber, err error) {
	stats.IncrementOperations(&stats.DirCreateOps)

	dirInodeNumber, err = vS.createRootOrSubDir(filePerm, userID, groupID, false)

	if err == nil {
		stats.IncrementOperations(&stats.DirCreateSuccessOps)
	}

	return
}

func linkInMemory(dirInode *inMemoryInodeStruct, targetInode *inMemoryInodeStruct, basename string) error {
	dirInode.dirty = true
	targetInode.dirty = true

	dirMapping := dirInode.payload.(sortedmap.BPlusTree)

	ok, err := dirMapping.Put(basename, targetInode.InodeNumber)
	if nil != err {
		panic(err)
	}
	if !ok {
		err = fmt.Errorf("%s: failed to create link '%v' to inode %v in directory inode %v: entry exists",
			utils.GetFnName(), basename, targetInode.InodeNumber, dirInode.InodeNumber)
		return blunder.AddError(err, blunder.FileExistsError)
	}

	updateTime := time.Now()

	targetInode.LinkCount++
	targetInode.AttrChangeTime = updateTime

	if targetInode.InodeType == DirType && targetInode.InodeNumber != RootDirInodeNumber {
		subdirMapping := targetInode.payload.(sortedmap.BPlusTree)
		subdirMapping.Put("..", dirInode.InodeNumber)
		dirInode.LinkCount++
	}

	dirInode.AttrChangeTime = updateTime
	dirInode.ModificationTime = updateTime

	return nil
}

// Insert-only version of linkInMemory()
func linkInMemoryInsertOnly(dirInode *inMemoryInodeStruct, basename string, targetInodeNumber InodeNumber) (err error) {
	// TODO
	dirInode.dirty = true

	dirMapping := dirInode.payload.(sortedmap.BPlusTree)

	ok, err := dirMapping.Put(basename, targetInodeNumber)
	if nil != err {
		panic(err)
	}
	if !ok {
		err = fmt.Errorf("%s: failed to create link '%v' to inode %v in directory inode %v: entry exists",
			utils.GetFnName(), basename, targetInodeNumber, dirInode.InodeNumber)
		return blunder.AddError(err, blunder.FileExistsError)
	}

	updateTime := time.Now()

	dirInode.AttrChangeTime = updateTime
	dirInode.ModificationTime = updateTime

	return nil
}

// This is used by the link(2), create(2), and mkdir(2) operations
// (mountstruct.Link(), mountstruct.Create(), and mountstruct.Mkdir())
func (vS *volumeStruct) Link(dirInodeNumber InodeNumber, basename string, targetInodeNumber InodeNumber, insertOnly bool) (err error) {
	var (
		dirInode       *inMemoryInodeStruct
		flushInodeList []*inMemoryInodeStruct
		ok             bool
		snapShotIDType headhunter.SnapShotIDType
		targetInode    *inMemoryInodeStruct
	)

	if (RootDirInodeNumber == dirInodeNumber) && (SnapShotDirName == basename) {
		err = blunder.NewError(blunder.InvalidArgError, "Link() to /%v not allowed", SnapShotDirName)
		return
	}
	snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(dirInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = blunder.NewError(blunder.InvalidArgError, "Link() on non-LiveView dirInodeNumber not allowed")
		return
	}
	snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(targetInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = blunder.NewError(blunder.InvalidArgError, "Link() on non-LiveView targetInodeNumber not allowed")
		return
	}

	stats.IncrementOperations(&stats.DirLinkOps)

	dirInode, err = vS.fetchInodeType(dirInodeNumber, DirType)
	if err != nil {
		logger.ErrorfWithError(err, "dirInode error")
		return err
	}

	if insertOnly {
		err = linkInMemoryInsertOnly(dirInode, basename, targetInodeNumber)
		if err != nil {
			return err
		}

		flushInodeList = []*inMemoryInodeStruct{dirInode}
	} else {
		targetInode, ok, err = vS.fetchInode(targetInodeNumber)
		if err != nil {
			// the inode is locked so this should never happen (unless the inode
			// was evicted from the cache and it was corrupt when re-read from disk)
			// (err includes volume name and inode number)
			logger.ErrorfWithError(err, "%s: targetInode fetch error", utils.GetFnName())
			return err
		}
		if !ok {
			// this should never happen (see above)
			err = fmt.Errorf("%s: Link failing request to link to inode %d volume '%s' because it is unallocated",
				utils.GetFnName(), targetInode.InodeNumber, vS.volumeName)
			err = blunder.AddError(err, blunder.NotFoundError)
			logger.ErrorWithError(err)
			return err
		}

		err = linkInMemory(dirInode, targetInode, basename)
		if err != nil {
			return err
		}

		flushInodeList = []*inMemoryInodeStruct{dirInode, targetInode}
	}

	// REVIEW TODO: We think we need to do something more than just return an error here :-)

	err = vS.flushInodes(flushInodeList)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	stats.IncrementOperations(&stats.DirLinkSuccessOps)
	return nil
}

// Manipulate a directory to remove an an entry. Like Unlink(), but without any inode loading or flushing.
func unlinkInMemory(dirInode *inMemoryInodeStruct, untargetInode *inMemoryInodeStruct, basename string) (err error) {
	dirMapping := dirInode.payload.(sortedmap.BPlusTree)

	dirInode.dirty = true
	untargetInode.dirty = true

	ok, err := dirMapping.DeleteByKey(basename)
	if nil != err {
		panic(err)
	}
	if !ok {
		err = fmt.Errorf("Unlink(): dirInode DeleteByKey of \"%v\" should have returned ok == true", basename)
		panic(err)
	}

	untargetInode.LinkCount--

	if DirType == untargetInode.InodeType {
		untargetDirMapping := untargetInode.payload.(sortedmap.BPlusTree)

		ok, err = untargetDirMapping.DeleteByKey("..")
		if nil != err {
			panic(err)
		}
		if !ok {
			err = fmt.Errorf("Unlink(): untargetInode DeleteByKey of \"..\" should have returned ok == true")
			panic(err)
		}

		dirInode.LinkCount--
	}

	updateTime := time.Now()

	dirInode.AttrChangeTime = updateTime
	dirInode.ModificationTime = updateTime

	untargetInode.AttrChangeTime = updateTime

	return
}

// Remove-only version of unlinkInMemory()
func unlinkInMemoryRemoveOnly(dirInode *inMemoryInodeStruct, basename string) (err error) {
	dirMapping := dirInode.payload.(sortedmap.BPlusTree)

	dirInode.dirty = true

	ok, err := dirMapping.DeleteByKey(basename)
	if nil != err {
		panic(err)
	}
	if !ok {
		err = fmt.Errorf("Unlink(): dirInode DeleteByKey of \"%v\" should have returned ok == true", basename)
		panic(err)
	}

	updateTime := time.Now()

	dirInode.AttrChangeTime = updateTime
	dirInode.ModificationTime = updateTime

	return
}

func (vS *volumeStruct) Unlink(dirInodeNumber InodeNumber, basename string, removeOnly bool) (err error) {
	var (
		dirInode            *inMemoryInodeStruct
		flushInodeList      []*inMemoryInodeStruct
		ok                  bool
		snapShotIDType      headhunter.SnapShotIDType
		untargetInode       *inMemoryInodeStruct
		untargetInodeNumber InodeNumber
	)

	if (RootDirInodeNumber == dirInodeNumber) && (SnapShotDirName == basename) {
		err = blunder.NewError(blunder.InvalidArgError, "Unlink() of /%v not allowed", SnapShotDirName)
		return
	}
	snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(dirInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = blunder.NewError(blunder.InvalidArgError, "Unlink() on non-LiveView dirInodeNumber not allowed")
		return
	}

	stats.IncrementOperations(&stats.DirUnlinkOps)

	dirInode, err = vS.fetchInodeType(dirInodeNumber, DirType)
	if nil != err {
		return err
	}

	untargetInodeNumber, err = vS.lookup(dirInodeNumber, basename)
	if nil != err {
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	if removeOnly {
		err = unlinkInMemoryRemoveOnly(dirInode, basename)
		if err != nil {
			return err
		}

		flushInodeList = []*inMemoryInodeStruct{dirInode}
	} else {

		untargetInode, ok, err = vS.fetchInode(untargetInodeNumber)
		if nil != err {
			// the inode is locked so this should never happen (unless the inode
			// was evicted from the cache and it was corrupt when re-read from disk)
			// (err includes volume name and inode number)
			logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
			return err
		}
		if !ok {
			// this should never happen (see above)
			err = fmt.Errorf("%s: failing request to Unlink inode %d volume '%s' because it is unallocated",
				utils.GetFnName(), untargetInode.InodeNumber, vS.volumeName)
			err = blunder.AddError(err, blunder.NotFoundError)
			logger.ErrorWithError(err)
			return err
		}

		// Pre-flush untargetInode so that no time-based (implicit) flushes will occur during this transaction
		err = vS.flushInode(untargetInode)
		if err != nil {
			logger.ErrorfWithError(err, "Unlink(): untargetInode flush error")
			panic(err)
		}

		err = unlinkInMemory(dirInode, untargetInode, basename)
		if err != nil {
			return err
		}

		flushInodeList = []*inMemoryInodeStruct{dirInode, untargetInode}
	}

	err = vS.flushInodes(flushInodeList)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	stats.IncrementOperations(&stats.DirUnlinkSuccessOps)
	return
}

func (vS *volumeStruct) Move(srcDirInodeNumber InodeNumber, srcBasename string, dstDirInodeNumber InodeNumber, dstBasename string) (err error) {
	if (RootDirInodeNumber == srcDirInodeNumber) && (SnapShotDirName == srcBasename) {
		err = blunder.NewError(blunder.InvalidArgError, "Move() from /%v not allowed", SnapShotDirName)
		return
	}
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(srcDirInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = blunder.NewError(blunder.InvalidArgError, "Move() on non-LiveView srcDirInodeNumber not allowed")
		return
	}
	if (RootDirInodeNumber == dstDirInodeNumber) && (SnapShotDirName == dstBasename) {
		err = blunder.NewError(blunder.InvalidArgError, "Move() into /%v not allowed", SnapShotDirName)
		return
	}
	snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(dstDirInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = blunder.NewError(blunder.InvalidArgError, "Move() on non-LiveView dstDirInodeNumber not allowed")
		return
	}

	stats.IncrementOperations(&stats.DirRenameOps)

	srcDirInode, err := vS.fetchInodeType(srcDirInodeNumber, DirType)
	if nil != err {
		logger.ErrorfWithError(err, "Move(): srcDirInode fetch error")
		panic(err)
	}
	srcDirMapping := srcDirInode.payload.(sortedmap.BPlusTree)

	var dstDirInode *inMemoryInodeStruct
	var dstDirMapping sortedmap.BPlusTree
	if srcDirInodeNumber == dstDirInodeNumber {
		if srcBasename == dstBasename {
			err = fmt.Errorf("%v: Source & Target of Move() cannot be identical: %v/%v", utils.GetFnName(), srcDirInodeNumber, srcBasename)
			logger.ErrorWithError(err)
			err = blunder.AddError(err, blunder.FileExistsError)
			return
		}
		dstDirInode = srcDirInode
		dstDirMapping = srcDirMapping
	} else {
		dstDirInode, err = vS.fetchInodeType(dstDirInodeNumber, DirType)
		if nil != err {
			logger.ErrorfWithError(err, "Move(): dstDirInode fetch error")
			panic(err)
		}
		dstDirMapping = dstDirInode.payload.(sortedmap.BPlusTree)
	}

	srcInodeNumberAsValue, ok, err := srcDirMapping.GetByKey(srcBasename)
	if nil != err {
		panic(err)
	}
	if !ok {
		err = fmt.Errorf("%v: unable to find basename %v in dirInode %v", utils.GetFnName(), srcBasename, srcDirInodeNumber)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}
	srcInodeNumber := srcInodeNumberAsValue.(InodeNumber)

	srcInode, ok, err := vS.fetchInode(srcInodeNumber)
	if nil != err {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when re-read from disk)
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of src inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request because src inode %d volume '%s' is unallocated",
			utils.GetFnName(), srcInode.InodeNumber, vS.volumeName)
		err = blunder.AddError(err, blunder.NotDirError)
		logger.ErrorWithError(err)
		return err
	}

	var dstInodeNumber InodeNumber
	var dstInode *inMemoryInodeStruct
	dstInodeNumberAsValue, ok, err := dstDirMapping.GetByKey(dstBasename)
	if nil != err {
		// this indicates disk corruption or software bug
		logger.ErrorfWithError(err, "%s: dstDirInode GetByKey(%s) error inode %d volume '%s'",
			utils.GetFnName(), dstBasename, dstDirInode.InodeNumber, vS.volumeName)
		panic(err)
	}
	if ok {
		dstInodeNumber = dstInodeNumberAsValue.(InodeNumber)

		dstInode, ok, err = vS.fetchInode(dstInodeNumber)
		if nil != err {
			// this indicates disk corruption or software bug
			// (err includes volume name and inode number)
			logger.ErrorfWithError(err, "%s: dstInode fetch error", utils.GetFnName())
			panic(err)
		}
		if !ok {
			// disk corruption or software bug
			err = fmt.Errorf("%s: dstInode inode %d volume '%s' is unallocated",
				utils.GetFnName(), dstInode.InodeNumber, vS.volumeName)
			err = blunder.AddError(err, blunder.NotFoundError)
			logger.ErrorWithError(err)
			panic(err)
		}
	} else {
		dstInodeNumber = InodeNumber(0)
		dstInode = nil
	}

	// I believe this is allowed so long at the dstInode is empty --craig
	if (nil != dstInode) && (DirType == dstInode.InodeType) {
		err = fmt.Errorf("%v: Target of Move() is an existing directory: %v/%v", utils.GetFnName(), dstDirInodeNumber, dstBasename)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.FileExistsError)
		return
	}

	// All set to proceed

	if FileType == srcInode.InodeType {
		// Pre-flush srcInode so that no time-based (implicit) flushes will occur during this transaction
		err = vS.flushInode(srcInode)
		if err != nil {
			logger.ErrorfWithError(err, "Move(): srcInode flush error")
			panic(err)
		}
	}
	if (nil != dstInode) && (FileType == dstInode.InodeType) {
		// Pre-flush dstInode so that no time-based (implicit) flushes will occur during this transaction
		err = vS.flushInode(dstInode)
		if err != nil {
			logger.ErrorfWithError(err, "Move(): dstInode flush error")
			panic(err)
		}
	}

	updateTime := time.Now()

	inodes := make([]*inMemoryInodeStruct, 0, 4)

	srcDirInode.dirty = true
	srcDirInode.AttrChangeTime = updateTime
	srcDirInode.ModificationTime = updateTime
	inodes = append(inodes, srcDirInode)

	if srcDirInodeNumber != dstDirInodeNumber {
		dstDirInode.dirty = true
		dstDirInode.AttrChangeTime = updateTime
		dstDirInode.ModificationTime = updateTime
		inodes = append(inodes, dstDirInode)

		if DirType == srcInode.InodeType {
			srcDirInode.LinkCount--
			dstDirInode.LinkCount++

			srcInodeAsDirMapping := srcInode.payload.(sortedmap.BPlusTree)
			ok, err = srcInodeAsDirMapping.PatchByKey("..", dstDirInodeNumber)
			if nil != err {
				logger.ErrorfWithError(err, "Move(): srcInode PatchByKey error")
				panic(err)
			}
			if !ok {
				err = fmt.Errorf("Should have found \"..\" entry")
				logger.ErrorfWithError(err, "Move(): srcInode PatchByKey error")
				panic(err)
			}
		}
	}

	srcInode.dirty = true
	srcInode.AttrChangeTime = updateTime
	inodes = append(inodes, srcInode)

	ok, err = srcDirMapping.DeleteByKey(srcBasename)
	if nil != err {
		logger.ErrorfWithError(err, "Move(): srcDirInode DeleteByKey error")
		panic(err)
	}
	if !ok {
		err = fmt.Errorf("Should have found \"%v\" entry", srcBasename)
		logger.ErrorfWithError(err, "Move(): srcDirInode DeleteByKey error")
		panic(err)
	}

	if nil == dstInode {
		ok, err = dstDirMapping.Put(dstBasename, srcInodeNumber)
		if nil != err {
			logger.ErrorfWithError(err, "Move(): dstDirInode Put error")
			panic(err)
		}
		if !ok {
			err = fmt.Errorf("Should have been able to PUT \"%v\" entry", dstBasename)
			logger.ErrorfWithError(err, "Move(): dstDirInode Put error")
			panic(err)
		}
	} else {
		dstInode.dirty = true
		dstInode.AttrChangeTime = updateTime
		inodes = append(inodes, dstInode)

		dstInode.LinkCount--

		ok, err = dstDirMapping.PatchByKey(dstBasename, srcInodeNumber)
		if nil != err {
			logger.ErrorfWithError(err, "Move(): dstDirInode PatchByKey error")
			panic(err)
		}
		if !ok {
			err = fmt.Errorf("Should have been able to PatchByKey \"%v\" entry", dstBasename)
			logger.ErrorfWithError(err, "Move(): dstDirInode PatchByKey error")
			panic(err)
		}
	}

	// Flush the multi-inode transaction

	err = vS.flushInodes(inodes)
	if err != nil {
		logger.ErrorfWithError(err, "flushInodes(%v) error", inodes)
		panic(err)
	}

	// And, if we decremented dstInode.LinkCount to zero, destroy dstInode as well

	if (nil != dstInode) && (0 == dstInode.LinkCount) {
		err = vS.Destroy(dstInode.InodeNumber)
	}

	stats.IncrementOperations(&stats.DirRenameSuccessOps)
	return
}

func (vS *volumeStruct) lookup(dirInodeNumber InodeNumber, basename string) (targetInodeNumber InodeNumber, err error) {
	var (
		dirInode               *inMemoryInodeStruct
		dirInodeNonce          uint64
		dirInodeSnapShotID     uint64
		dirInodeSnapShotIDType headhunter.SnapShotIDType
		dirMapping             sortedmap.BPlusTree
		ok                     bool
		value                  sortedmap.Value
		snapShot               headhunter.SnapShotStruct
	)

	dirInodeSnapShotIDType, dirInodeSnapShotID, dirInodeNonce = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(dirInodeNumber))

	switch dirInodeSnapShotIDType {
	case headhunter.SnapShotIDTypeLive:
		if (uint64(RootDirInodeNumber) == dirInodeNonce) &&
			(SnapShotDirName == basename) &&
			(0 < vS.headhunterVolumeHandle.SnapShotCount()) {
			// Lookup should only succeed there are any active SnapShot's

			targetInodeNumber = InodeNumber(vS.headhunterVolumeHandle.SnapShotTypeDotSnapShotAndNonceEncode(dirInodeNonce))
			err = nil

			return
		}
		// Let normal processing perform the lookup below (SnapShotID == 0, so InodeNumber "adornment" is a no-op)
	case headhunter.SnapShotIDTypeSnapShot:
		// Let normal processing perform the lookup below but "adorn" resultant InodeNumber's with the dirSnapShotID
	case headhunter.SnapShotIDTypeDotSnapShot:
		// Currently, this is only supported in RootDirInodeNumber

		if uint64(RootDirInodeNumber) != dirInodeNonce {
			err = blunder.AddError(fmt.Errorf("%v other than in '/' not supported", SnapShotDirName), blunder.NotFoundError)
			return
		}

		switch basename {
		case ".":
			targetInodeNumber = dirInodeNumber
			err = nil
		case "..":
			targetInodeNumber = RootDirInodeNumber
			err = nil
		default:
			// See if basename is the name for an active SnapShot

			snapShot, ok = vS.headhunterVolumeHandle.SnapShotLookupByName(basename)

			if ok {
				targetInodeNumber = InodeNumber(vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(snapShot.ID, dirInodeNonce))
				err = nil
			} else {
				err = blunder.AddError(fmt.Errorf("/%v/%v SnapShot not found", SnapShotDirName, basename), blunder.NotFoundError)
			}
		}

		return
	}

	dirInode, err = vS.fetchInodeType(dirInodeNumber, DirType)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	dirMapping = dirInode.payload.(sortedmap.BPlusTree)
	value, ok, err = dirMapping.GetByKey(basename)
	if nil != err {
		panic(err)
	}
	if !ok {
		err = fmt.Errorf("unable to find basename %v in dirInode %v", basename, dirInodeNumber)
		// There are cases where failing to find an inode is not an error.
		// Not logging any errors here; let the caller decide if this is log-worthy
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}
	targetInodeNumber, ok = value.(InodeNumber)
	if !ok {
		err = fmt.Errorf("dirMapping for basename %v in dirInode %v not an InodeNumber", basename, dirInodeNumber)
		panic(err)
	}
	targetInodeNumber = InodeNumber(vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(dirInodeSnapShotID, uint64(targetInodeNumber)))

	return
}

func (vS *volumeStruct) Lookup(dirInodeNumber InodeNumber, basename string) (targetInodeNumber InodeNumber, err error) {
	stats.IncrementOperations(&stats.DirLookupOps)

	targetInodeNumber, err = vS.lookup(dirInodeNumber, basename)

	return
}

func (vS *volumeStruct) NumDirEntries(dirInodeNumber InodeNumber) (numEntries uint64, err error) {
	var (
		adjustNumEntriesForSnapShotSubDirInRootDirInode bool
		dirMapping                                      sortedmap.BPlusTree
		dirMappingLen                                   int
		inode                                           *inMemoryInodeStruct
		snapShotCount                                   uint64
		snapShotIDType                                  headhunter.SnapShotIDType
	)

	if RootDirInodeNumber == dirInodeNumber {
		// Account for .. in /<SnapShotDirName> if any SnapShot's exist
		snapShotCount = vS.headhunterVolumeHandle.SnapShotCount()
		adjustNumEntriesForSnapShotSubDirInRootDirInode = (0 != snapShotCount)
	} else {
		snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(dirInodeNumber))
		if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
			// numEntries == 1 ('.') + 1 ('..') + # SnapShot's
			snapShotCount = vS.headhunterVolumeHandle.SnapShotCount()
			numEntries = 1 + 1 + snapShotCount
			err = nil
			return
		}
		adjustNumEntriesForSnapShotSubDirInRootDirInode = false
	}

	inode, err = vS.fetchInodeType(dirInodeNumber, DirType)
	if nil != err {
		return
	}

	dirMapping = inode.payload.(sortedmap.BPlusTree)

	dirMappingLen, err = dirMapping.Len()
	if nil != err {
		err = blunder.AddError(err, blunder.IOError)
		return
	}

	if adjustNumEntriesForSnapShotSubDirInRootDirInode {
		numEntries = uint64(dirMappingLen) + 1
	} else {
		numEntries = uint64(dirMappingLen)
	}

	return
}

// A maxEntries or maxBufSize argument of zero is interpreted to mean "no maximum".
func (vS *volumeStruct) ReadDir(dirInodeNumber InodeNumber, maxEntries uint64, maxBufSize uint64, prevReturned ...interface{}) (dirEntries []DirEntry, moreEntries bool, err error) {
	var (
		bufSize                      uint64
		dirEntryBasename             string
		dirEntryBasenameAsKey        sortedmap.Key
		dirEntryInodeNumber          InodeNumber
		dirEntryInodeNumberAsValue   sortedmap.Value
		dirIndex                     int
		dirMapping                   sortedmap.BPlusTree
		dirMappingLen                int         // If snapShotDirToBeInserted, this is 1 + dirMapping.Len()
		dotDotInodeNumberReplacement InodeNumber // If == 0, do not replace ..'s InodeNumber
		foundPrevReturned            bool
		inode                        *inMemoryInodeStruct
		key                          sortedmap.Key
		nextEntry                    DirEntry
		nonce                        uint64
		okGetByIndex                 bool
		okKeyAsString                bool
		okPayloadBPlusTree           bool
		okPutToSnapShotListSorted    bool
		okValueAsInodeNumber         bool
		snapShotDirFound             bool
		snapShotDirIndex             int  // If snapShotDirToBeInserted, this is the index where it goes
		snapShotDirToBeInserted      bool // Only true in /<SnapShotDirName>
		snapShotID                   uint64
		snapShotIDType               headhunter.SnapShotIDType
		snapShotList                 []headhunter.SnapShotStruct
		snapShotListElement          headhunter.SnapShotStruct
		snapShotListSorted           sortedmap.LLRBTree // Will also include '.' & '..'
		snapShotListSortedLen        int
		value                        sortedmap.Value
	)

	// The following defer'd func() is useful for debugging... so leaving it in here as a comment
	/*
		defer func() {
			logger.Errorf("Executed inode.ReadDir()...")
			logger.Errorf("  dirInodeNumber: 0x%016X", dirInodeNumber)
			logger.Errorf("  maxEntries:     0x%016X", maxEntries)
			logger.Errorf("  maxBufSize:     0x%016X", maxBufSize)
			switch len(prevReturned) {
			case 0:
				// Nothing
			case 1:
				logger.Errorf("  prevReturned:   %v", prevReturned[0])
			default:
				logger.Errorf("  len(prevReturned) [%v] should have been 0 or 1", len(prevReturned))
			}
			if nil == err {
				for dirEntriesIndex, dirEntry := range dirEntries {
					logger.Errorf("  dirEntries[%v]:", dirEntriesIndex)
					logger.Errorf("    InodeNumber:     0x%016X", dirEntry.InodeNumber)
					logger.Errorf("    Basename:        %s", dirEntry.Basename)
					logger.Errorf("    InodeType:       %v", dirEntry.Type)
					logger.Errorf("    NextDirLocation: %v", dirEntry.NextDirLocation)
				}
			}
			logger.Errorf("  moreEntries:    %v", moreEntries)
			logger.Errorf("  err:            %v", err)
		}()
	*/

	stats.IncrementOperations(&stats.DirReaddirOps)

	dirEntries = make([]DirEntry, 0, int(maxEntries))
	moreEntries = false

	snapShotIDType, snapShotID, nonce = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(dirInodeNumber))

	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		if uint64(RootDirInodeNumber) != nonce {
			err = fmt.Errorf("ReadDir() to %v not in '/' not supported", SnapShotDirName)
			err = blunder.AddError(err, blunder.NotSupportedError)
			return
		}

		snapShotList = vS.headhunterVolumeHandle.SnapShotListByName(false)

		snapShotListSorted = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

		okPutToSnapShotListSorted, err = snapShotListSorted.Put(".", dirInodeNumber)
		if nil != err {
			err = fmt.Errorf("ReadDir() encountered error populating SnapShotListSorted: %v", err)
			err = blunder.AddError(err, blunder.IOError)
			return
		}
		if !okPutToSnapShotListSorted {
			err = fmt.Errorf("ReadDir() encountered !ok populating SnapShotListSorted")
			err = blunder.AddError(err, blunder.IOError)
			return
		}

		okPutToSnapShotListSorted, err = snapShotListSorted.Put("..", RootDirInodeNumber)
		if nil != err {
			err = fmt.Errorf("ReadDir() encountered error populating SnapShotListSorted: %v", err)
			err = blunder.AddError(err, blunder.IOError)
			return
		}
		if !okPutToSnapShotListSorted {
			err = fmt.Errorf("ReadDir() encountered !ok populating SnapShotListSorted")
			err = blunder.AddError(err, blunder.IOError)
			return
		}

		for _, snapShotListElement = range snapShotList {
			okPutToSnapShotListSorted, err = snapShotListSorted.Put(snapShotListElement.Name, InodeNumber(vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(snapShotListElement.ID, uint64(RootDirInodeNumber))))
			if nil != err {
				err = fmt.Errorf("ReadDir() encountered error populating SnapShotListSorted: %v", err)
				err = blunder.AddError(err, blunder.IOError)
				return
			}
			if !okPutToSnapShotListSorted {
				err = fmt.Errorf("ReadDir() encountered !ok populating SnapShotListSorted")
				err = blunder.AddError(err, blunder.IOError)
				return
			}
		}

		switch len(prevReturned) {
		case 0:
			dirIndex = int(0)
		case 1:
			var (
				okAsInodeDirLocation           bool
				okAsString                     bool
				prevReturnedAsInodeDirLocation InodeDirLocation
				prevReturnedAsString           string
			)

			prevReturnedAsInodeDirLocation, okAsInodeDirLocation = prevReturned[0].(InodeDirLocation)
			prevReturnedAsString, okAsString = prevReturned[0].(string)

			if okAsInodeDirLocation {
				if 0 > prevReturnedAsInodeDirLocation {
					dirIndex = int(0)
				} else {
					dirIndex = int(prevReturnedAsInodeDirLocation + 1)
				}
			} else if okAsString {
				dirIndex, foundPrevReturned, err = snapShotListSorted.BisectRight(prevReturnedAsString)
				if nil != err {
					err = fmt.Errorf("ReadDir() encountered error bisecting SnapShotListSorted: %v", err)
				}
				if foundPrevReturned {
					dirIndex++
				}
			} else {
				err = fmt.Errorf("ReadDir() accepts only zero or one (InodeDirLocation or string) trailing prevReturned argument")
				err = blunder.AddError(err, blunder.NotSupportedError)
				return
			}
		default:
			err = fmt.Errorf("ReadDir() accepts only zero or one (InodeDirLocation or string) trailing prevReturned argument")
			err = blunder.AddError(err, blunder.NotSupportedError)
			return
		}

		bufSize = 0

		snapShotListSortedLen, err = snapShotListSorted.Len()
		if nil != err {
			err = fmt.Errorf("ReadDir() encountered error accessing SnapShotListSorted: %v", err)
			err = blunder.AddError(err, blunder.IOError)
			return
		}

		for {
			if snapShotListSortedLen <= dirIndex {
				break
			}

			dirEntryBasenameAsKey, dirEntryInodeNumberAsValue, okGetByIndex, err = snapShotListSorted.GetByIndex(dirIndex)
			if nil != err {
				err = fmt.Errorf("ReadDir() encountered error accessing SnapShotListSorted: %v", err)
				err = blunder.AddError(err, blunder.IOError)
				return
			}
			if !okGetByIndex {
				err = fmt.Errorf("ReadDir() encountered !ok accessing SnapShotListSorted: %v", err)
				err = blunder.AddError(err, blunder.IOError)
				return
			}

			dirEntryBasename, okKeyAsString = dirEntryBasenameAsKey.(string)
			if !okKeyAsString {
				err = fmt.Errorf("ReadDir() encountered !ok accessing SnapShotListSorted: %v", err)
				err = blunder.AddError(err, blunder.IOError)
				return
			}

			dirEntryInodeNumber, okValueAsInodeNumber = dirEntryInodeNumberAsValue.(InodeNumber)
			if !okValueAsInodeNumber {
				err = fmt.Errorf("ReadDir() encountered !ok accessing SnapShotListSorted: %v", err)
				err = blunder.AddError(err, blunder.IOError)
				return
			}

			nextEntry = DirEntry{
				InodeNumber:     dirEntryInodeNumber,
				Basename:        dirEntryBasename,
				NextDirLocation: InodeDirLocation(dirIndex) + 1,
			}

			if (0 != maxEntries) && (uint64(len(dirEntries)+1) > maxEntries) {
				break
			}
			if (0 != maxBufSize) && ((bufSize + uint64(nextEntry.Size())) > maxBufSize) {
				break
			}

			dirEntries = append(dirEntries, nextEntry)
			bufSize += uint64(nextEntry.Size())
			dirIndex++
		}

		moreEntries = dirIndex < dirMappingLen

		stats.IncrementOperationsEntriesAndBytes(stats.DirRead, uint64(len(dirEntries)), bufSize)

		err = nil
		return
	}

	// If we reach here, snapShotIDType is one of headhunter.SnapShotIDType{Live|SnapShotIDTypeSnapShot}

	inode, err = vS.fetchInodeType(dirInodeNumber, DirType)
	if nil != err {
		return
	}

	dirMapping, okPayloadBPlusTree = inode.payload.(sortedmap.BPlusTree)
	if !okPayloadBPlusTree {
		err = fmt.Errorf("ReadDir() found unexpected Inode Payload")
		err = blunder.AddError(err, blunder.IOError)
		return
	}

	dirMappingLen, err = dirMapping.Len()
	if nil != err {
		err = blunder.AddError(err, blunder.IOError)
		return
	}

	snapShotDirToBeInserted = false               // By default, SnapShotDirName not to be inserted
	dotDotInodeNumberReplacement = InodeNumber(0) // By default, do not replace ..'s InodeNumber

	if headhunter.SnapShotIDTypeLive == snapShotIDType {
		if RootDirInodeNumber == dirInodeNumber {
			if uint64(0) < vS.headhunterVolumeHandle.SnapShotCount() {
				// Need to find a spot to insert SnapShotDirName

				snapShotDirIndex, snapShotDirFound, err = dirMapping.BisectRight(SnapShotDirName)
				if nil != err {
					err = blunder.AddError(err, blunder.IOError)
					return
				}
				if snapShotDirFound {
					err = fmt.Errorf("ReadDir() encountered pre-existing /%v dirEntry", SnapShotDirName)
					err = blunder.AddError(err, blunder.IOError)
					return
				}

				snapShotDirToBeInserted = true
				dirMappingLen++
			}
		}
	} else {
		if uint64(RootDirInodeNumber) == nonce {
			dotDotInodeNumberReplacement = InodeNumber(vS.headhunterVolumeHandle.SnapShotTypeDotSnapShotAndNonceEncode(uint64(RootDirInodeNumber)))
		}
	}

	switch len(prevReturned) {
	case 0:
		dirIndex = int(0)
	case 1:
		var (
			foundDoingBisectRight          bool
			okAsInodeDirLocation           bool
			okAsString                     bool
			prevReturnedAsInodeDirLocation InodeDirLocation
			prevReturnedAsString           string
		)

		prevReturnedAsInodeDirLocation, okAsInodeDirLocation = prevReturned[0].(InodeDirLocation)
		prevReturnedAsString, okAsString = prevReturned[0].(string)

		if okAsInodeDirLocation {
			if 0 > prevReturnedAsInodeDirLocation {
				dirIndex = int(0)
			} else {
				dirIndex = int(prevReturnedAsInodeDirLocation + 1)
			}
		} else if okAsString {
			if snapShotDirToBeInserted {
				if SnapShotDirName == prevReturnedAsString {
					dirIndex = snapShotDirIndex + 1
				} else {
					dirIndex, foundDoingBisectRight, err = dirMapping.BisectRight(prevReturnedAsString)
					if nil != err {
						err = blunder.AddError(err, blunder.IOError)
						return
					}
					if dirIndex >= snapShotDirIndex {
						dirIndex++
					}
					if foundDoingBisectRight {
						dirIndex++
					}
				}
			} else {
				dirIndex, foundDoingBisectRight, err = dirMapping.BisectRight(prevReturnedAsString)
				if nil != err {
					err = blunder.AddError(err, blunder.IOError)
					return
				}
				if foundDoingBisectRight {
					dirIndex++
				}
			}
		} else {
			err = fmt.Errorf("ReadDir() accepts only zero or one (InodeDirLocation or string) trailing prevReturned argument")
			err = blunder.AddError(err, blunder.NotSupportedError)
			return
		}
	default:
		err = fmt.Errorf("ReadDir() accepts only zero or one (InodeDirLocation or string) trailing prevReturned argument")
		err = blunder.AddError(err, blunder.NotSupportedError)
		return
	}

	bufSize = 0

	for {
		if snapShotDirToBeInserted && (dirIndex == snapShotDirIndex) {
			dirEntryBasename = SnapShotDirName
			dirEntryInodeNumber = InodeNumber(vS.headhunterVolumeHandle.SnapShotTypeDotSnapShotAndNonceEncode(uint64(RootDirInodeNumber)))
		} else {
			if snapShotDirToBeInserted {
				if dirIndex < snapShotDirIndex {
					key, value, okGetByIndex, err = dirMapping.GetByIndex(dirIndex)
				} else {
					key, value, okGetByIndex, err = dirMapping.GetByIndex(dirIndex - 1)
				}
			} else {
				key, value, okGetByIndex, err = dirMapping.GetByIndex(dirIndex)
			}
			if nil != err {
				err = blunder.AddError(err, blunder.IOError)
				return
			}
			if !okGetByIndex {
				break
			}

			dirEntryBasename, okKeyAsString = key.(string)
			if !okKeyAsString {
				err = fmt.Errorf("ReadDir() encountered dirEntry with non-string Key")
				err = blunder.AddError(err, blunder.IOError)
				return
			}

			if (InodeNumber(0) != dotDotInodeNumberReplacement) && (".." == dirEntryBasename) {
				dirEntryInodeNumber = dotDotInodeNumberReplacement
			} else {
				dirEntryInodeNumber = InodeNumber(vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(snapShotID, uint64(value.(InodeNumber))))
			}
		}

		nextEntry = DirEntry{
			InodeNumber:     dirEntryInodeNumber,
			Basename:        dirEntryBasename,
			NextDirLocation: InodeDirLocation(dirIndex) + 1,
		}

		if (0 != maxEntries) && (uint64(len(dirEntries)+1) > maxEntries) {
			break
		}
		if (0 != maxBufSize) && ((bufSize + uint64(nextEntry.Size())) > maxBufSize) {
			break
		}

		dirEntries = append(dirEntries, nextEntry)
		bufSize += uint64(nextEntry.Size())

		dirIndex++ // Consumed one dirEntry either manufactured (/<SnapShotDirName>) or from dirMapping
	}

	moreEntries = dirIndex < dirMappingLen

	stats.IncrementOperationsEntriesAndBytes(stats.DirRead, uint64(len(dirEntries)), bufSize)

	err = nil
	return
}
