package inode

import (
	"fmt"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
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

	// If creating RootDir, since this must be atomic, caller already holds vS.Mutex
	if !isRootDir {
		vS.Lock()
	}
	vS.inodeCache[dirInodeNumber] = dirInode
	if !isRootDir {
		vS.Unlock()
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

func addDirEntryInMemory(dirInode *inMemoryInodeStruct, targetInode *inMemoryInodeStruct, basename string) error {
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

// This is used by the link(2), create(2), and mkdir(2) operations
// (mountstruct.Link(), mountstruct.Create(), and mountstruct.Mkdir())
func (vS *volumeStruct) Link(dirInodeNumber InodeNumber, basename string, targetInodeNumber InodeNumber) (err error) {
	stats.IncrementOperations(&stats.DirLinkOps)

	dirInode, err := vS.fetchInodeType(dirInodeNumber, DirType)
	if err != nil {
		logger.ErrorfWithError(err, "dirInode error")
		return err
	}

	targetInode, ok, err := vS.fetchInode(targetInodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when re-read from disk)
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: targetInode fetch error", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: Link failing request to link to inode %d volume '%s' because its unallocated",
			utils.GetFnName(), targetInode.InodeNumber, vS.volumeName)
		err = blunder.AddError(err, blunder.NotFoundError)
		logger.ErrorWithError(err)
		return err
	}

	if FileType == targetInode.InodeType {
		// Pre-flush targetInode so that no time-based (implicit) flushes will occur during this transaction
		err = vS.flushInode(targetInode)
		if err != nil {
			logger.ErrorfWithError(err, "targetInode flush error")
			return err
		}
	}

	err = addDirEntryInMemory(dirInode, targetInode, basename)
	if err != nil {
		return err
	}

	err = vS.flushInodes([]*inMemoryInodeStruct{dirInode, targetInode})
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	stats.IncrementOperations(&stats.DirLinkSuccessOps)
	return nil
}

// Manipulate a directory to remove an an entry. Like Unlink(), but without any inode loading or flushing.
func removeDirEntryInMemory(dirInode *inMemoryInodeStruct, untargetInode *inMemoryInodeStruct, basename string) (err error) {
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

func (vS *volumeStruct) Unlink(dirInodeNumber InodeNumber, basename string) (err error) {
	stats.IncrementOperations(&stats.DirUnlinkOps)

	dirInode, err := vS.fetchInodeType(dirInodeNumber, DirType)
	if nil != err {
		return err
	}

	untargetInodeNumber, err := vS.Lookup(dirInodeNumber, basename)
	if nil != err {
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	untargetInode, ok, err := vS.fetchInode(untargetInodeNumber)
	if nil != err {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when re-read from disk)
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request to Unlink inode %d volume '%s' because its unallocated",
			utils.GetFnName(), untargetInode.InodeNumber, vS.volumeName)
		err = blunder.AddError(err, blunder.NotFoundError)
		logger.ErrorWithError(err)
		return err
	}

	// Pre-flush untargetInode so that no time-based (implicit) flushes will occur during this transaction
	err = vS.flushInode(untargetInode)
	if err != nil {
		logger.ErrorfWithError(err, "Move(): untargetInode flush error")
		panic(err)
	}

	err = removeDirEntryInMemory(dirInode, untargetInode, basename)
	if err != nil {
		return err
	}

	err = vS.flushInodes([]*inMemoryInodeStruct{dirInode, untargetInode})
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	stats.IncrementOperations(&stats.DirUnlinkSuccessOps)
	return
}

func (vS *volumeStruct) Move(srcDirInodeNumber InodeNumber, srcBasename string, dstDirInodeNumber InodeNumber, dstBasename string) (err error) {
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

	// Finally flush the multi-inode transaction

	err = vS.flushInodes(inodes)
	if err != nil {
		logger.ErrorfWithError(err, "flushInodes(%v) error", inodes)
		panic(err)
	}

	stats.IncrementOperations(&stats.DirRenameSuccessOps)
	return
}

func (vS *volumeStruct) Lookup(dirInodeNumber InodeNumber, basename string) (targetInodeNumber InodeNumber, err error) {
	stats.IncrementOperations(&stats.DirLookupOps)

	dirInode, err := vS.fetchInodeType(dirInodeNumber, DirType)
	if err != nil {
		logger.ErrorWithError(err)
		return 0, err
	}

	dirMapping := dirInode.payload.(sortedmap.BPlusTree)
	value, ok, err := dirMapping.GetByKey(basename)
	if nil != err {
		panic(err)
	}
	if !ok {
		err = fmt.Errorf("%v: unable to find basename %v in dirInode %v", utils.GetFnName(), basename, dirInodeNumber)
		// There are cases where failing to find an inode is not an error.
		// Not logging any errors here; let the caller decide if this is log-worthy
		return 0, blunder.AddError(err, blunder.NotFoundError)
	}
	targetInodeNumber = value.(InodeNumber)

	return targetInodeNumber, nil
}

func (vS *volumeStruct) NumDirEntries(dirInodeNumber InodeNumber) (numEntries uint64, err error) {
	inode, err := vS.fetchInodeType(dirInodeNumber, DirType)
	if nil != err {
		return
	}

	dirMapping := inode.payload.(sortedmap.BPlusTree)

	dirMappingLen, err := dirMapping.Len()
	if nil != err {
		err = blunder.AddError(err, blunder.IOError)
		return
	}

	numEntries = uint64(dirMappingLen)

	return
}

// A maxEntries or maxBufSize argument of zero is interpreted to mean "no maximum".
func (vS *volumeStruct) ReadDir(dirInodeNumber InodeNumber, maxEntries uint64, maxBufSize uint64, prevReturned ...interface{}) (dirEntries []DirEntry, moreEntries bool, err error) {
	var (
		bufSize              uint64
		dirIndex             int
		dirMapping           sortedmap.BPlusTree
		dirMappingLen        int
		atLeastOneEntryFound bool
		inode                *inMemoryInodeStruct
		key                  sortedmap.Key
		nextEntry            DirEntry
		okGetByIndex         bool
		okPayloadBPlusTree   bool
		value                sortedmap.Value
	)

	stats.IncrementOperations(&stats.DirReaddirOps)

	dirEntries = make([]DirEntry, 0, int(maxEntries))
	moreEntries = false

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
			dirIndex, foundDoingBisectRight, err = dirMapping.BisectRight(prevReturnedAsString)
			if nil != err {
				err = blunder.AddError(err, blunder.IOError)
				return
			}
			if foundDoingBisectRight {
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

	atLeastOneEntryFound = false
	bufSize = 0

	for {
		key, value, okGetByIndex, err = dirMapping.GetByIndex(dirIndex)
		if nil != err {
			err = blunder.AddError(err, blunder.IOError)
			return
		}
		if !okGetByIndex {
			break
		}

		atLeastOneEntryFound = true

		nextEntry = DirEntry{
			InodeNumber:     value.(InodeNumber),
			Basename:        key.(string),
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

	if 0 == len(dirEntries) {
		if atLeastOneEntryFound {
			// moreEntries will be true despite none fitting withing supplied constraints
		} else {
			err = fmt.Errorf("ReadDir() called for prevReturned at or beyond end of directory")
			err = blunder.AddError(err, blunder.NotFoundError)
		}
	}

	stats.IncrementOperationsEntriesAndBytes(stats.DirRead, uint64(len(dirEntries)), bufSize)

	return
}
