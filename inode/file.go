package inode

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

type fileExtentStruct struct {
	FileOffset       uint64
	Length           uint64
	LogSegmentNumber uint64
	LogSegmentOffset uint64
}

func (vS *volumeStruct) CreateFile(filePerm InodeMode, userID InodeUserID, groupID InodeGroupID) (fileInodeNumber InodeNumber, err error) {
	fileInode, err := vS.createFileInode(filePerm, userID, groupID)
	if err != nil {
		return 0, err
	}
	return fileInode.InodeNumber, nil
}

func (vS *volumeStruct) createFileInode(filePerm InodeMode, userID InodeUserID, groupID InodeGroupID) (fileInode *inMemoryInodeStruct, err error) {
	stats.IncrementOperations(&stats.FileCreateOps)

	// Create file mode out of file permissions plus inode type
	fileMode, err := determineMode(filePerm, FileType)
	if err != nil {
		return nil, err
	}

	fileInode, err = vS.makeInMemoryInode(FileType, fileMode, userID, groupID)
	if err != nil {
		return nil, err
	}

	fileInode.dirty = true

	// The payload of a file inode is a B+-tree map whose keys are uint64 file
	// offsets and whose values are `fileExtent`s.

	extents := sortedmap.NewBPlusTree(vS.maxExtentsPerFileNode, sortedmap.CompareUint64, &fileInodeCallbacks{treeNodeLoadable{inode: fileInode}})

	fileInode.payload = extents

	vS.Lock()
	vS.inodeCache[fileInode.InodeNumber] = fileInode
	vS.Unlock()

	stats.IncrementOperations(&stats.FileCreateSuccessOps)
	return
}

func fileLen(extents sortedmap.BPlusTree) uint64 {
	numExtents, err := extents.Len()
	if nil != err {
		panic(err)
	}
	if numExtents == 0 {
		return 0
	}

	_, value, ok, err := extents.GetByIndex(numExtents - 1)
	if nil != err {
		panic(err)
	}
	if !ok {
		panic("couldn't find last extent of nonempty file")
	}

	lastFileExtent := value.(*fileExtentStruct)

	return lastFileExtent.FileOffset + lastFileExtent.Length
}

// Update a file inode to have a new size, zero-padding as necessary.
//
// Doesn't flush anything.
func setSizeInMemory(fileInode *inMemoryInodeStruct, size uint64) (err error) {
	extents := fileInode.payload.(sortedmap.BPlusTree)
	extentIndex, found, err := extents.BisectLeft(size)
	if nil != err {
		panic(err)
	}

	if !found {
		if 0 <= extentIndex {
			// Potentially trim preceeding extent
			_, extentValue, ok, getByIndexErr := extents.GetByIndex(extentIndex)
			if nil != getByIndexErr {
				panic(getByIndexErr)
			}
			if !ok {
				unexpectedErr := fmt.Errorf("unexpected extents indexing problem")
				panic(unexpectedErr)
			}
			extent := extentValue.(*fileExtentStruct)
			if (extent.FileOffset + extent.Length) > size {
				// Yes, we need to trim the preceeding extent
				trimSize := extent.Length - (size - extent.FileOffset)
				extent.Length -= trimSize
				ok, patchByIndexErr := extents.PatchByIndex(extentIndex, extent)
				if nil != patchByIndexErr {
					panic(patchByIndexErr)
				}
				if !ok {
					unexpectedErr := fmt.Errorf("unexpected extents indexing problem")
					panic(unexpectedErr)
				}
				decrementLogSegmentMapFileData(fileInode, extent.LogSegmentNumber, trimSize)
			}
		}

		extentIndex++ // Step to next extent entry (beyond potentially truncated preceeding extent)
	}

	for {
		_, extentValue, ok, getByIndexErr := extents.GetByIndex(extentIndex)
		if nil != getByIndexErr {
			panic(getByIndexErr)
		}
		if !ok {
			break
		}
		extent := extentValue.(*fileExtentStruct)
		decrementLogSegmentMapFileData(fileInode, extent.LogSegmentNumber, extent.Length)
		ok, deleteByIndexErr := extents.DeleteByIndex(extentIndex)
		if nil != deleteByIndexErr {
			panic(deleteByIndexErr)
		}
		if !ok {
			deleteByIndexErr = fmt.Errorf("extent just indexed should have been delete-able")
			panic(deleteByIndexErr)
		}
	}

	fileInode.dirty = true
	fileInode.Size = size

	updateTime := time.Now()
	fileInode.ModificationTime = updateTime
	fileInode.AttrChangeTime = updateTime
	return
}

func (vS *volumeStruct) Read(fileInodeNumber InodeNumber, offset uint64, length uint64, profiler *utils.Profiler) (buf []byte, err error) {
	var (
		fileInode     *inMemoryInodeStruct
		readPlan      []ReadPlanStep
		readPlanBytes uint64
	)

	fileInode, err = vS.fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	readPlan, readPlanBytes, err = vS.getReadPlanHelper(fileInode, &offset, &length)
	if nil != err {
		logger.WarnWithError(err)
		return
	}

	buf, err = vS.doReadPlan(fileInode, readPlan, readPlanBytes)
	if nil != err {
		logger.WarnWithError(err)
		return
	}

	stats.IncrementOperationsAndBucketedBytes(stats.FileRead, uint64(len(buf)))

	err = nil
	return
}

func (vS *volumeStruct) GetReadPlan(fileInodeNumber InodeNumber, offset *uint64, length *uint64) (readPlan []ReadPlanStep, err error) {
	var (
		readPlanBytes uint64
	)

	fileInode, err := vS.fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	if fileInode.dirty {
		err = flush(fileInode, false)
		if nil != err {
			logger.ErrorWithError(err)
			return
		}
	}

	readPlan, readPlanBytes, err = vS.getReadPlanHelper(fileInode, offset, length)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	stats.IncrementOperationsAndBucketedBytes(stats.FileReadplan, readPlanBytes)
	return
}

func (vS *volumeStruct) getReadPlanHelper(fileInode *inMemoryInodeStruct, requestedOffset *uint64, requestedLength *uint64) (readPlan []ReadPlanStep, readPlanBytes uint64, err error) {
	var (
		offset uint64
	)

	readPlan = make([]ReadPlanStep, 0)

	if requestedOffset == nil && requestedLength == nil {
		err = fmt.Errorf("requestedOffset and requestedLength cannot both be nil")
		return
	} else if requestedOffset == nil {
		// Suffix request, e.g. "bytes=-10", the last 10 bytes of the file
		offset = fileInode.Size - *requestedLength
		readPlanBytes = *requestedLength
	} else if requestedLength == nil {
		// Prefix request, e.g. "bytes=25-", from byte 25 to the end
		offset = *requestedOffset
		readPlanBytes = fileInode.Size - *requestedOffset
	} else {
		offset = *requestedOffset
		readPlanBytes = *requestedLength
	}

	if (offset + readPlanBytes) > fileInode.Size {
		if fileInode.Size > offset {
			readPlanBytes = fileInode.Size - offset
		} else {
			readPlanBytes = 0
		}
	}

	if 0 == readPlanBytes {
		return
	}

	extents := fileInode.payload.(sortedmap.BPlusTree)

	curOffset := offset
	terminalOffset := offset + readPlanBytes

	curExtentIndex, _, err := extents.BisectLeft(offset)
	if nil != err {
		panic(err)
	}

	for {
		if 0 > curExtentIndex {
			// Skip to next (0th) extent
			curExtentIndex++
		}

		// Fetch curExtent
		_, curExtentValue, ok, getByIndexErr := extents.GetByIndex(curExtentIndex)
		if nil != getByIndexErr {
			panic(getByIndexErr)
		}
		if !ok {
			// We have reached the end of extents
			break
		}
		curExtent := curExtentValue.(*fileExtentStruct)

		if curExtent.FileOffset >= terminalOffset {
			// [curOffset:terminalOffset) ends before curExtent... so we need look no further
			break
		}

		if (curExtent.FileOffset + curExtent.Length) <= curOffset {
			// [curOffset:terminalOffset) starts beyond curExtent... so move to the next extent
			curExtentIndex++
			continue
		}

		// At this point, we know [curOffset:terminalOffset) intersects curExtent

		if curOffset < curExtent.FileOffset {
			// [curOffset:terminalOffset) starts before curExtent... so insert a zero-fill ReadPlanStep to get to curExtent
			step := ReadPlanStep{
				LogSegmentNumber: 0,
				Offset:           0,
				Length:           curExtent.FileOffset - curOffset,
				AccountName:      "",
				ContainerName:    "",
				ObjectName:       "",
				ObjectPath:       "",
			}
			readPlan = append(readPlan, step)
			curOffset = curExtent.FileOffset
		}

		skipSize := uint64(0)

		if curOffset > curExtent.FileOffset {
			// [curOffset:terminalOffset) starts after curExtent... so update skipSize appropriately
			skipSize = curOffset - curExtent.FileOffset
		}

		if terminalOffset <= (curExtent.FileOffset + curExtent.Length) {
			// [curOffset:terminalOffset) is completed by some or all of curExtent
			step := ReadPlanStep{
				LogSegmentNumber: curExtent.LogSegmentNumber,
				Offset:           curExtent.LogSegmentOffset + skipSize,
				Length:           terminalOffset - curOffset,
				AccountName:      vS.accountName,
			}
			step.ContainerName, step.ObjectName, step.ObjectPath, err = vS.getObjectLocationFromLogSegmentNumber(step.LogSegmentNumber)
			if nil != err {
				return
			}
			readPlan = append(readPlan, step)
			curOffset = terminalOffset
			break
		} else {
			// [curOffset:terminalOffset) extends beyond curExtent
			step := ReadPlanStep{
				LogSegmentNumber: curExtent.LogSegmentNumber,
				Offset:           curExtent.LogSegmentOffset + skipSize,
				Length:           curExtent.Length - skipSize,
				AccountName:      vS.accountName,
			}
			step.ContainerName, step.ObjectName, step.ObjectPath, err = vS.getObjectLocationFromLogSegmentNumber(step.LogSegmentNumber)
			if nil != err {
				return
			}
			readPlan = append(readPlan, step)
			curOffset += step.Length
			curExtentIndex++
		}
	}

	// Append trailing zero-fill ReadPlanStep if necessary

	if curOffset < terminalOffset {
		// We need a trailing zero-fill ReadPlanStep
		step := ReadPlanStep{
			LogSegmentNumber: 0,
			Offset:           0,
			Length:           terminalOffset - curOffset,
			AccountName:      "",
			ContainerName:    "",
			ObjectName:       "",
			ObjectPath:       "",
		}

		readPlan = append(readPlan, step)
	}

	err = nil
	return
}

func incrementLogSegmentMapFileData(fileInode *inMemoryInodeStruct, logSegmentNumber uint64, incrementAmount uint64) {
	logSegmentRecord, ok := fileInode.LogSegmentMap[logSegmentNumber]
	if ok {
		logSegmentRecord += incrementAmount
		fileInode.LogSegmentMap[logSegmentNumber] = logSegmentRecord
	} else {
		fileInode.LogSegmentMap[logSegmentNumber] = incrementAmount
	}
}

func decrementLogSegmentMapFileData(fileInode *inMemoryInodeStruct, logSegmentNumber uint64, decrementAmount uint64) {
	logSegmentRecord, ok := fileInode.LogSegmentMap[logSegmentNumber]
	if ok {
		if decrementAmount > logSegmentRecord {
			err := fmt.Errorf("Unexpected decrementLogSegmentMapFileData() call would make FileData \"go negative\"")
			panic(err)
		}
		logSegmentRecord -= decrementAmount
		fileInode.LogSegmentMap[logSegmentNumber] = logSegmentRecord
	} else {
		err := fmt.Errorf("Unexpected decrementLogSegmentMapFileData() call referenced non-existent logSegmentNumber")
		panic(err)
	}
}

// `recordWrite` is called by `Write` and `Wrote` to update the file inode
// payload's record of the extents that compose the file.
func recordWrite(fileInode *inMemoryInodeStruct, fileOffset uint64, length uint64, logSegmentNumber uint64, logSegmentOffset uint64) (err error) {
	extents := fileInode.payload.(sortedmap.BPlusTree)

	// First we need to eliminate extents or portions thereof that overlap the specified write

	extentIndex, found, err := extents.BisectLeft(fileOffset)
	if nil != err {
		panic(err)
	}

	if !found {
		if 0 <= extentIndex {
			// Potentially split preceeding extent
			_, extentValue, ok, getByIndexErr := extents.GetByIndex(extentIndex)
			if nil != getByIndexErr {
				panic(getByIndexErr)
			}
			if !ok {
				unexpectedErr := fmt.Errorf("unexpected extents indexing problem")
				panic(unexpectedErr)
			}
			leftExtent := extentValue.(*fileExtentStruct)
			if (leftExtent.FileOffset + leftExtent.Length) > fileOffset {
				// Yes, we need to split the preceeding extent
				splitOutSize := (leftExtent.FileOffset + leftExtent.Length) - fileOffset
				leftExtent.Length -= splitOutSize
				ok, patchByIndexErr := extents.PatchByIndex(extentIndex, leftExtent)
				if nil != patchByIndexErr {
					panic(patchByIndexErr)
				}
				if !ok {
					unexpectedErr := fmt.Errorf("unexpected extents indexing problem")
					panic(unexpectedErr)
				}
				rightExtent := &fileExtentStruct{
					FileOffset:       fileOffset,
					Length:           splitOutSize,
					LogSegmentNumber: leftExtent.LogSegmentNumber,
					LogSegmentOffset: leftExtent.LogSegmentOffset + leftExtent.Length,
				}
				ok, putErr := extents.Put(rightExtent.FileOffset, rightExtent)
				if nil != putErr {
					panic(putErr)
				}
				if !ok {
					unexpectedErr := fmt.Errorf("unexpected extents key problem")
					panic(unexpectedErr)
				}
			}
		}

		extentIndex++ // Step to next extent entry (beyond potentially truncated preceeding extent)
	}

	for {
		_, extentValue, ok, getByIndexErr := extents.GetByIndex(extentIndex)
		if nil != getByIndexErr {
			panic(getByIndexErr)
		}
		if !ok {
			// We have reached the end of extents
			break
		}
		leftExtent := extentValue.(*fileExtentStruct)
		if leftExtent.FileOffset >= (fileOffset + length) {
			// We are done pruning extents
			break
		}
		if (fileOffset + length) >= (leftExtent.FileOffset + leftExtent.Length) {
			// This extent entirely overwritten... just delete it
			decrementLogSegmentMapFileData(fileInode, leftExtent.LogSegmentNumber, leftExtent.Length)
			ok, deleteByIndexErr := extents.DeleteByIndex(extentIndex)
			if nil != deleteByIndexErr {
				panic(deleteByIndexErr)
			}
			if !ok {
				unexpectedErr := fmt.Errorf("unexpected extents indexing problem")
				panic(unexpectedErr)
			}
		} else {
			// This extent partially overwritten... trim it from the front and we will be done trimming
			ok, deleteByIndexErr := extents.DeleteByIndex(extentIndex)
			if nil != deleteByIndexErr {
				panic(deleteByIndexErr)
			}
			if !ok {
				unexpectedErr := fmt.Errorf("unexpected extents indexing problem")
				panic(unexpectedErr)
			}
			overlapSize := (fileOffset + length) - leftExtent.FileOffset
			rightExtent := &fileExtentStruct{
				FileOffset:       leftExtent.FileOffset + overlapSize,
				Length:           leftExtent.Length - overlapSize,
				LogSegmentNumber: leftExtent.LogSegmentNumber,
				LogSegmentOffset: leftExtent.LogSegmentOffset + overlapSize,
			}
			ok, putErr := extents.Put(rightExtent.FileOffset, rightExtent)
			if nil != putErr {
				panic(putErr)
			}
			if !ok {
				unexpectedErr := fmt.Errorf("unexpected extents key problem")
				panic(unexpectedErr)
			}
			decrementLogSegmentMapFileData(fileInode, leftExtent.LogSegmentNumber, overlapSize)
			break
		}
	}

	// Now that there will be no overlap, see if we can append to the preceding fileExtent

	prevIndex, found, err := extents.BisectLeft(fileOffset)
	if nil != err {
		panic(err)
	}
	if found {
		unexpectedErr := fmt.Errorf("unexpected to find fileOffset in extents at this point)")
		panic(unexpectedErr)
	}

	var prevExtent *fileExtentStruct

	prevExtent = nil

	if 0 <= prevIndex {
		_, prevExtentValue, ok, getByIndexErr := extents.GetByIndex(prevIndex)
		if nil != getByIndexErr {
			panic(getByIndexErr)
		}
		if !ok {
			unexpectedErr := fmt.Errorf("unexpected to not find predecessor in extents at this point")
			panic(unexpectedErr)
		}

		prevExtent = prevExtentValue.(*fileExtentStruct)
	}

	if (nil != prevExtent) && (prevExtent.LogSegmentNumber == logSegmentNumber) && ((prevExtent.FileOffset + prevExtent.Length) == fileOffset) && ((prevExtent.LogSegmentOffset + prevExtent.Length) == logSegmentOffset) {
		// APPEND Case: We are able to simply lengthen prevExtent

		prevExtent.Length += length
		ok, patchByIndexErr := extents.PatchByIndex(prevIndex, prevExtent)
		if nil != patchByIndexErr {
			panic(patchByIndexErr)
		}
		if !ok {
			unexpectedErr := fmt.Errorf("unexpected to not be able to PATCH at this point")
			panic(unexpectedErr)
		}
	} else {
		// Non-APPEND Case: We need to insert a new extent

		newExtent := &fileExtentStruct{
			FileOffset:       fileOffset,
			Length:           length,
			LogSegmentNumber: logSegmentNumber,
			LogSegmentOffset: logSegmentOffset,
		}

		ok, putErr := extents.Put(newExtent.FileOffset, newExtent)
		if nil != putErr {
			panic(putErr)
		}
		if !ok {
			unexpectedErr := fmt.Errorf("unexpected to not be able to PUT at this point")
			panic(unexpectedErr)
		}
	}

	incrementLogSegmentMapFileData(fileInode, logSegmentNumber, length)

	return nil
}

func (vS *volumeStruct) Write(fileInodeNumber InodeNumber, offset uint64, buf []byte, profiler *utils.Profiler) (err error) {
	fileInode, err := vS.fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	fileInode.dirty = true

	logSegmentNumber, logSegmentOffset, err := vS.doSendChunk(fileInode, buf)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	length := uint64(len(buf))

	err = recordWrite(fileInode, offset, length, logSegmentNumber, logSegmentOffset)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	startingSize := fileInode.Size

	offsetJustAfterWhereBufLogicallyWritten := offset + length

	if offsetJustAfterWhereBufLogicallyWritten > startingSize {
		fileInode.Size = offsetJustAfterWhereBufLogicallyWritten
	}

	appendedBytes := fileInode.Size - startingSize
	overwrittenBytes := length - appendedBytes

	stats.IncrementOperationsBucketedBytesAndAppendedOverwritten(stats.FileWrite, length, appendedBytes, overwrittenBytes)

	updateTime := time.Now()
	fileInode.AttrChangeTime = updateTime
	fileInode.ModificationTime = updateTime
	fileInode.NumWrites++

	return
}

func (vS *volumeStruct) Wrote(fileInodeNumber InodeNumber, fileOffset uint64, objectPath string, objectOffset uint64, length uint64, patchOnly bool) (err error) {
	fileInode, err := vS.fetchInodeType(fileInodeNumber, FileType)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	if fileInode.dirty {
		err = flush(fileInode, false)
		if nil != err {
			logger.ErrorWithError(err)
			return
		}
	}

	if !patchOnly {
		if 0 != fileOffset || 0 != objectOffset {
			err = fmt.Errorf("non-patch calls to Wrote() with non-zero offsets not supported")
			logger.ErrorWithError(err)
			return
		}
	}

	accountName, containerName, logSegmentNumeral, err := utils.PathToAcctContObj(objectPath)
	if err != nil {
		return
	}
	if accountName != fileInode.volume.accountName {
		err = fmt.Errorf("Wrote() called indicating a PUT was done to a different Account")
		logger.ErrorWithError(err)
		return
	}
	logSegmentNumber, err := strconv.ParseUint(logSegmentNumeral, 16, 64)
	if err != nil {
		return
	}

	err = fileInode.volume.setLogSegmentContainer(logSegmentNumber, containerName)
	if nil != err {
		return
	}

	fileInode.dirty = true

	err = recordWrite(fileInode, fileOffset, length, logSegmentNumber, objectOffset)
	if err != nil {
		logger.ErrorWithError(err)
		return
	}

	if patchOnly {
		if (fileOffset + length) > fileInode.Size {
			fileInode.Size = fileOffset + length
		}

		updateTime := time.Now()
		fileInode.ModificationTime = updateTime
		fileInode.AttrChangeTime = updateTime
	} else {
		err = setSizeInMemory(fileInode, length)
		if err != nil {
			logger.ErrorWithError(err)
			return
		}
	}

	fileInode.dirty = true
	err = fileInode.volume.flushInode(fileInode)
	if err != nil {
		logger.ErrorWithError(err)
		return
	}

	stats.IncrementOperationsAndBucketedBytes(stats.FileWrote, length)
	fileInode.NumWrites++

	return
}

func (vS *volumeStruct) SetSize(fileInodeNumber InodeNumber, size uint64) (err error) {
	// NOTE: Errors are logged by the caller

	fileInode, err := vS.fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		return err
	}

	fileInode.dirty = true

	err = setSizeInMemory(fileInode, size)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	err = fileInode.volume.flushInode(fileInode)
	if nil != err {
		logger.ErrorWithError(err)
		return err
	}

	stats.IncrementOperations(&stats.DirSetsizeOps)

	return
}

func (vS *volumeStruct) Flush(fileInodeNumber InodeNumber, andPurge bool) (err error) {

	fileInode, ok, err := vS.fetchInode(fileInodeNumber)
	if nil != err {
		// this indicates disk corruption or software bug
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: request to flush inode %d volume '%s' failed",
			utils.GetFnName(), fileInodeNumber, vS.volumeName)
		return
	}
	if !ok {
		// this can happen if background flush loses a race with unlink()
		logger.Infof("%s: request to flush free inode %d volume '%s' ignored",
			utils.GetFnName(), fileInodeNumber, vS.volumeName)
		return
	}
	if fileInode.InodeType != FileType {
		// this should never happen unless there's disk corruption
		logger.Errorf("%s: request to flush inode %d volume '%s' type '%v' ignored",
			utils.GetFnName(), fileInodeNumber, vS.volumeName, fileInode.InodeType)
		return
	}

	if fileInode.dirty {
		err = flush(fileInode, andPurge)
		if nil != err {
			logger.ErrorWithError(err)
			return
		}
	}

	stats.IncrementOperations(&stats.FileFlushOps)

	return
}

func flush(fileInode *inMemoryInodeStruct, andPurge bool) (err error) {
	volume := fileInode.volume
	inodeNumber := fileInode.InodeNumber
	err = volume.flushInode(fileInode)
	if nil != err {
		logger.ErrorfWithError(err, "flushInode(fileInode) failed")
	}

	if andPurge {
		volume.Lock()
		delete(volume.inodeCache, inodeNumber)
		volume.Unlock()
	}

	return
}

func (vS *volumeStruct) Coalesce(containingDirInodeNumber InodeNumber, combinationName string, elements []CoalesceElement) (combinationInodeNumber InodeNumber, modificationTime time.Time, numWrites uint64, err error) {
	// We steal the log segments from each element by getting a read plan for the whole element, then calling
	// recordWrite to point the combined inode at them.
	//
	// While this does result in log segments being shared by two inodes (an illegal state), we undo the damage later by
	// deleting the elements' inodes but not their log segments. Also, we carefully manage transactions so this whole
	// thing is atomic.
	elementInodes := make([]*inMemoryInodeStruct, len(elements))
	elementDirInodes := make([]*inMemoryInodeStruct, len(elements))

	alreadyFlushing := make(map[InodeNumber]bool)

	// Validate that no element inodes are duplicated
	seenInode := make(map[InodeNumber]bool)
	for _, element := range elements {
		if seenInode[element.ElementInodeNumber] {
			err = blunder.NewError(blunder.InvalidArgError, "Inode %v passed to Coalesce() twice", element.ElementInodeNumber)
			return
		}
		seenInode[element.ElementInodeNumber] = true
	}

	for i := 0; i < len(elements); i++ {
		// Validation: the provided files are files, the dirs are dirs, and the files are singly-linked.
		elementInodes[i], err = vS.fetchInodeType(elements[i].ElementInodeNumber, FileType)
		if err != nil {
			return
		}

		if elementInodes[i].LinkCount > 1 {
			err = blunder.NewError(blunder.TooManyLinksError, "File %v has more than one link", elements[i].ElementName)
			return
		}

		elementDirInodes[i], err = vS.fetchInodeType(elements[i].ContainingDirectoryInodeNumber, DirType)
		if err != nil {
			return
		}
	}

	containingDirInode, err := vS.fetchInodeType(containingDirInodeNumber, DirType)
	if err != nil {
		return
	}

	combinationInode, err := vS.createFileInode(PosixModePerm, 0, 0)
	if err != nil {
		return
	}

	sumOfElementSizes := uint64(0)
	for _, inodeStruct := range elementInodes {
		offset := uint64(0)
		length := inodeStruct.Size
		// NB: we rely on the fact that GetReadPlan causes a flush of any pending writes to disk. This lets us steal log
		// segments without concerning ourselves with stealing data from the write-back cache as well.
		readPlanSteps, err1 := vS.GetReadPlan(inodeStruct.InodeNumber, &offset, &length)
		if err1 != nil {
			err = err1
			return
		}

		fileOffset := uint64(0)
		for _, step := range readPlanSteps {
			// For normal steps, steal the log segment. For sparse steps (zero-fill in sparse files), just increment the
			// file size.
			if step.LogSegmentNumber != 0 {
				err = recordWrite(combinationInode, sumOfElementSizes+fileOffset, step.Length, step.LogSegmentNumber, step.Offset)
				combinationInode.NumWrites++
				if err != nil {
					return
				}
			}
			fileOffset += step.Length
		}
		sumOfElementSizes += fileOffset
	}

	combinationInode.Size = sumOfElementSizes

	// Now combinationInode is completely set up; what remains is to link it into its directory, unlink all the element
	// inodes, and destroy the element inodes while leaving their log segments (just the data ones, not the payloads).
	//
	// We'll do all this and then flush all the inodes at once so that this function is atomic.
	toFlush := []*inMemoryInodeStruct{combinationInode}

	// Unlink elements from their directories
	for i := 0; i < len(elements); i++ {
		dirInode := elementDirInodes[i]
		fileInode := elementInodes[i]
		err = removeDirEntryInMemory(dirInode, fileInode, elements[i].ElementName)
		if err != nil {
			return
		}

		// we may have multiple files from a given directory, but a given file appears only once
		if !alreadyFlushing[dirInode.InodeNumber] {
			toFlush = append(toFlush, dirInode)
			alreadyFlushing[dirInode.InodeNumber] = true
		}
	}

	// Unlink the old dirent, if any, so we can overwrite it atomically
	obstacle, ok, err := containingDirInode.payload.(sortedmap.BPlusTree).GetByKey(combinationName)
	if err != nil {
		panic(err)
	}
	if ok {
		obstacleInodeNumber := obstacle.(InodeNumber)
		obstacleInode, ok, err1 := vS.fetchInode(obstacleInodeNumber)
		if !ok {
			err = errors.New("dir has inode, but we can't fetch it?")
			return
		}
		if err1 != nil {
			err = err1
			return
		}
		// can't remove a non-empty dir
		if obstacleInode.InodeType == DirType && obstacleInode.LinkCount > 2 {
			err = blunder.NewError(blunder.NotEmptyError, "Destination path refers to a non-empty directory")
			return
		}

		err = removeDirEntryInMemory(containingDirInode, obstacleInode, combinationName)
		if err != nil {
			return
		}
	}

	// Link the new entry in
	err = addDirEntryInMemory(containingDirInode, combinationInode, combinationName)
	if err != nil {
		return
	}

	if !alreadyFlushing[containingDirInode.InodeNumber] {
		toFlush = append(toFlush, containingDirInode)
		alreadyFlushing[containingDirInode.InodeNumber] = true
	}

	// This is a bit of a hack. flushInodes() doesn't let us destroy inodes. If we crash after flushInodes() but before
	// destroying all the elements' inodes, there will be unreferenced inodes left lying around. Should fsck discover
	// such inodes, it will delete them *and* their log segments, thus corrupting the combined file we have just built.
	//
	// To avoid that, we set the size of each element to 0, clear its LogSegmentMap, and flush it. This way, should fsck
	// get its hands on the inode, it will not destroy the underlying log segments, leaving the combined file intact.
	for _, elementInode := range elementInodes {
		elementInode.Size = uint64(0)
		elementInode.LogSegmentMap = make(map[uint64]uint64)
		toFlush = append(toFlush, elementInode)
	}

	err = vS.flushInodes(toFlush)
	if err != nil {
		return
	}
	// At this point, everything was successful from the caller's point of view. Now all that's left is to destroy our
	// unreferenced inodes, logging but not returning any errors, and move on.
	for _, elementInode := range elementInodes {
		destroyErr := vS.Destroy(elementInode.InodeNumber)
		if destroyErr != nil {
			logger.ErrorWithError(destroyErr)
		}
	}

	updateTime := time.Now()
	combinationInode.AttrChangeTime = updateTime
	combinationInode.ModificationTime = updateTime

	combinationInodeNumber = combinationInode.InodeNumber
	numWrites = combinationInode.NumWrites
	modificationTime = updateTime
	return
}

func (vS *volumeStruct) getLogSegmentContainer(logSegmentNumber uint64) (containerName string, err error) {
	containerNameAsByteSlice, err := vS.headhunterVolumeHandle.GetLogSegmentRec(logSegmentNumber)
	if nil != err {
		return
	}
	containerName = utils.ByteSliceToString(containerNameAsByteSlice)
	return
}

func (vS *volumeStruct) setLogSegmentContainer(logSegmentNumber uint64, containerName string) (err error) {
	containerNameAsByteSlice := utils.StringToByteSlice(containerName)
	err = vS.headhunterVolumeHandle.PutLogSegmentRec(logSegmentNumber, containerNameAsByteSlice)
	return
}

func (vS *volumeStruct) getObjectLocationFromLogSegmentNumber(logSegmentNumber uint64) (containerName string, objectName string, objectPath string, err error) {
	containerName, err = vS.getLogSegmentContainer(logSegmentNumber)
	if nil != err {
		return
	}
	objectName = fmt.Sprintf("%016X", logSegmentNumber)
	objectPath = vS.getObjectPathFromContainerNameAndLogSegmentNumber(containerName, logSegmentNumber)
	return
}

func (vS *volumeStruct) getObjectPathFromLogSegmentNumber(logSegmentNumber uint64) (objectPath string, err error) {
	containerName, err := vS.getLogSegmentContainer(logSegmentNumber)
	if nil != err {
		return
	}
	objectPath = vS.getObjectPathFromContainerNameAndLogSegmentNumber(containerName, logSegmentNumber)
	return
}

func (vS *volumeStruct) getObjectPathFromContainerNameAndLogSegmentNumber(containerName string, logSegmentNumber uint64) (objectPath string) {
	objectPath = fmt.Sprintf("/v1/%s/%s/%016X", vS.accountName, containerName, logSegmentNumber)
	return
}

func (vS *volumeStruct) deleteLogSegmentAsync(logSegmentNumber uint64, checkpointDoneWaitGroup *sync.WaitGroup) (err error) {
	containerName, err := vS.getLogSegmentContainer(logSegmentNumber)
	if nil != err {
		return
	}
	objectName := fmt.Sprintf("%016X", logSegmentNumber)
	err = vS.headhunterVolumeHandle.DeleteLogSegmentRec(logSegmentNumber)
	if nil != err {
		return
	}
	swiftclient.ObjectDeleteAsync(vS.accountName, containerName, objectName, checkpointDoneWaitGroup, nil)
	return
}
