package inode

import (
	"fmt"
	"strconv"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
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

// REVIEW TODO: Should one of these (and its siblings, CreateDir & CreateSymlink) be doing flush here?

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

	extents :=
		sortedmap.NewBPlusTree(
			vS.maxExtentsPerFileNode,
			sortedmap.CompareUint64,
			&fileInodeCallbacks{treeNodeLoadable{inode: fileInode}},
			globals.fileExtentMapCache)

	fileInode.payload = extents

	ok, err := vS.inodeCacheInsert(fileInode)
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("inodeCacheInsert(fileInode) failed")
		return
	}

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
		snapShotID    uint64
	)

	fileInode, err = vS.fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	_, snapShotID, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(fileInodeNumber))

	readPlan, readPlanBytes, err = vS.getReadPlanHelper(snapShotID, fileInode, &offset, &length)
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
		snapShotID    uint64
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

	_, snapShotID, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(fileInodeNumber))

	readPlan, readPlanBytes, err = vS.getReadPlanHelper(snapShotID, fileInode, offset, length)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	stats.IncrementOperationsBucketedEntriesAndBucketedBytes(stats.FileReadplan, uint64(len(readPlan)), readPlanBytes)
	return
}

func (vS *volumeStruct) getReadPlanHelper(snapShotID uint64, fileInode *inMemoryInodeStruct, requestedOffset *uint64, requestedLength *uint64) (readPlan []ReadPlanStep, readPlanBytes uint64, err error) {
	var (
		offset uint64
	)

	readPlan = make([]ReadPlanStep, 0)

	if requestedOffset == nil && requestedLength == nil {
		err = fmt.Errorf("requestedOffset and requestedLength cannot both be nil")
		return
	} else if requestedOffset == nil {
		// Suffix request, e.g. "bytes=-10", the last 10 bytes of the file
		if fileInode.Size > *requestedLength {
			offset = fileInode.Size - *requestedLength
			readPlanBytes = *requestedLength
		} else {
			// A suffix request for more bytes than the file has must get the whole file.
			offset = 0
			readPlanBytes = fileInode.Size
		}
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
				LogSegmentNumber: vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(snapShotID, curExtent.LogSegmentNumber),
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
				LogSegmentNumber: vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(snapShotID, curExtent.LogSegmentNumber),
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

func (vS *volumeStruct) FetchExtentMapChunk(fileInodeNumber InodeNumber, fileOffset uint64, maxEntriesFromFileOffset int64, maxEntriesBeforeFileOffset int64) (extentMapChunk *ExtentMapChunkStruct, err error) {
	var (
		containerName               string
		encodedLogSegmentNumber     uint64
		extentMap                   sortedmap.BPlusTree
		extentMapLen                int
		extentMapIndex              int
		extentMapIndexAtOffset      int
		extentMapIndexAtOffsetFound bool
		extentMapIndexEnd           int
		extentMapIndexStart         int
		fileExtent                  *fileExtentStruct
		fileExtentAsValue           sortedmap.Value
		fileInode                   *inMemoryInodeStruct
		objectName                  string
		snapShotID                  uint64
	)

	// Validate args

	if maxEntriesFromFileOffset < 1 {
		err = fmt.Errorf("inode.FetchExtentMap() requires maxEntriesFromOffset (%d) >= 1", maxEntriesFromFileOffset)
		return
	}
	if maxEntriesBeforeFileOffset < 0 {
		err = fmt.Errorf("inode.FetchExtentMap() requires maxEntriesBeforeOffset (%d) >= 0", maxEntriesBeforeFileOffset)
		return
	}

	fileInode, err = vS.fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		return
	}

	// Ensure in-flight LogSegments are flushed

	if fileInode.dirty {
		err = flush(fileInode, false)
		if nil != err {
			logger.ErrorWithError(err)
			return
		}
	}

	// Locate extent that either contains fileOffset,
	//   or if no extent does, that we select the one just after fileOffset

	extentMap = fileInode.payload.(sortedmap.BPlusTree)

	extentMapLen, err = extentMap.Len()
	if nil != err {
		panic(err)
	}

	if 0 == extentMapLen {
		// In the absence of any extents, just describe entire fileInode as zero-filled

		extentMapChunk = &ExtentMapChunkStruct{
			FileOffsetRangeStart: 0,
			FileOffsetRangeEnd:   fileInode.Size,
			FileSize:             fileInode.Size,
			ExtentMapEntry:       make([]ExtentMapEntryStruct, 0),
		}
		return
	}

	extentMapIndexAtOffset, extentMapIndexAtOffsetFound, err = extentMap.BisectLeft(fileOffset)
	if nil != err {
		panic(err)
	}

	if !extentMapIndexAtOffsetFound {
		if extentMapIndexAtOffset >= 0 {
			_, fileExtentAsValue, _, err = extentMap.GetByIndex(extentMapIndexAtOffset)
			if nil != err {
				panic(err)
			}

			fileExtent = fileExtentAsValue.(*fileExtentStruct)

			if (fileExtent.FileOffset + fileExtent.Length) <= fileOffset {
				extentMapIndexAtOffset++
			}
		} else {
			extentMapIndexAtOffset = 0
		}
	}

	// Compute extent indices surrounding fileOffset that are also requested

	if int64(extentMapIndexAtOffset) > maxEntriesBeforeFileOffset {
		extentMapIndexStart = extentMapIndexAtOffset - int(maxEntriesBeforeFileOffset)
	} else {
		extentMapIndexStart = 0
	}

	if int64(extentMapLen-extentMapIndexAtOffset) <= maxEntriesFromFileOffset {
		extentMapIndexEnd = extentMapLen - 1
	} else {
		extentMapIndexEnd = extentMapIndexAtOffset + int(maxEntriesFromFileOffset) - 1
	}

	// Populate extentMapChunk with selected extents

	_, snapShotID, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(fileInodeNumber))

	extentMapChunk = &ExtentMapChunkStruct{
		FileSize:       fileInode.Size,
		ExtentMapEntry: make([]ExtentMapEntryStruct, 0, extentMapIndexEnd-extentMapIndexStart+1),
	}

	// Fill in FileOffsetRangeStart to include zero-filled (non-)extent just before first returned extent

	if extentMapIndexStart > 0 {
		_, fileExtentAsValue, _, err = extentMap.GetByIndex(extentMapIndexStart - 1)
		if nil != err {
			panic(err)
		}

		fileExtent = fileExtentAsValue.(*fileExtentStruct)

		extentMapChunk.FileOffsetRangeStart = fileExtent.FileOffset + fileExtent.Length
	} else {
		extentMapChunk.FileOffsetRangeStart = 0
	}

	for extentMapIndex = extentMapIndexStart; extentMapIndex <= extentMapIndexEnd; extentMapIndex++ {
		_, fileExtentAsValue, _, err = extentMap.GetByIndex(extentMapIndex)
		if nil != err {
			panic(err)
		}

		fileExtent = fileExtentAsValue.(*fileExtentStruct)

		encodedLogSegmentNumber = vS.headhunterVolumeHandle.SnapShotIDAndNonceEncode(snapShotID, fileExtent.LogSegmentNumber)

		containerName, objectName, _, err = vS.getObjectLocationFromLogSegmentNumber(encodedLogSegmentNumber)
		if nil != err {
			panic(err)
		}

		extentMapChunk.ExtentMapEntry = append(extentMapChunk.ExtentMapEntry, ExtentMapEntryStruct{
			FileOffset:       fileExtent.FileOffset,
			LogSegmentOffset: fileExtent.LogSegmentOffset,
			Length:           fileExtent.Length,
			ContainerName:    containerName,
			ObjectName:       objectName,
		})
	}

	// Fill in FileOffsetRangeEnd to included zero-filled (non-)extent just after last returned extent

	if (extentMapIndexEnd + 1) == extentMapLen {
		extentMapChunk.FileOffsetRangeEnd = fileInode.Size
	} else {
		_, fileExtentAsValue, _, err = extentMap.GetByIndex(extentMapIndexEnd + 1)
		if nil != err {
			panic(err)
		}

		fileExtent = fileExtentAsValue.(*fileExtentStruct)

		extentMapChunk.FileOffsetRangeEnd = fileExtent.FileOffset
	}

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

	if (fileOffset + length) > fileInode.Size {
		fileInode.Size = fileOffset + length
	}

	incrementLogSegmentMapFileData(fileInode, logSegmentNumber, length)

	return nil
}

func (vS *volumeStruct) Write(fileInodeNumber InodeNumber, offset uint64, buf []byte, profiler *utils.Profiler) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(fileInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("Write() on non-LiveView fileInodeNumber not allowed")
		return
	}

	fileInode, err := vS.fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	// writes of length 0 succeed but do not change mtime
	if len(buf) == 0 {
		return
	}

	fileInode.dirty = true

	logSegmentNumber, logSegmentOffset, err := vS.doSendChunk(fileInode, buf)
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	length := uint64(len(buf))
	startingSize := fileInode.Size

	err = recordWrite(fileInode, offset, length, logSegmentNumber, logSegmentOffset)
	if nil != err {
		logger.ErrorWithError(err)
		return
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

func (vS *volumeStruct) Wrote(fileInodeNumber InodeNumber, objectPath string, fileOffset []uint64, objectOffset []uint64, length []uint64, patchOnly bool) (err error) {
	if (len(fileOffset) != len(objectOffset)) || (len(objectOffset) != len(length)) {
		err = fmt.Errorf("Wrote() called with unequal # of fileOffset's (%d), objectOffset's (%d), and length's (%d)", len(fileOffset), len(objectOffset), len(length))
		return
	}

	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(fileInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("Wrote() on non-LiveView fileInodeNumber not allowed")
		return
	}

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

	if !patchOnly {
		err = setSizeInMemory(fileInode, 0)
		if err != nil {
			logger.ErrorWithError(err)
			return
		}
	}

	bytesWritten := uint64(0)

	for i, thisLength := range length {
		if 0 < thisLength {
			err = recordWrite(fileInode, fileOffset[i], thisLength, logSegmentNumber, objectOffset[i])
			if err != nil {
				logger.ErrorWithError(err)
				return
			}

			fileInode.NumWrites++
			bytesWritten += thisLength
		}
	}

	if !patchOnly {
		// For this case only, make it appear we did precisely one write

		fileInode.NumWrites = 1
	}

	err = fileInode.volume.flushInode(fileInode)
	if err != nil {
		logger.ErrorWithError(err)
		return
	}

	stats.IncrementOperationsAndBucketedBytes(stats.FileWrote, bytesWritten)

	return
}

func (vS *volumeStruct) SetSize(fileInodeNumber InodeNumber, size uint64) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(fileInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetSize() on non-LiveView fileInodeNumber not allowed")
		return
	}

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
	vS := fileInode.volume
	err = vS.flushInode(fileInode)
	if nil != err {
		logger.ErrorfWithError(err, "flushInode(fileInode) failed")
	}

	if andPurge {
		var ok bool
		ok, err = vS.inodeCacheDrop(fileInode)
		if nil != err {
			return
		}
		if !ok {
			err = fmt.Errorf("inodeCacheDrop(fileInode) failed")
			return
		}
	}

	return
}

func (vS *volumeStruct) resetFileInodeInMemory(fileInode *inMemoryInodeStruct) (err error) {
	var (
		fileInodeExtentMap sortedmap.BPlusTree
		ok                 bool
	)

	fileInode.dirty = true

	fileInodeExtentMap = fileInode.payload.(sortedmap.BPlusTree)

	ok = true
	for ok {
		ok, err = fileInodeExtentMap.DeleteByIndex(0)
		if nil != err {
			err = fmt.Errorf("resetFileInodeInMemory() on Inode# 0x%016X failed: %v", fileInode.InodeNumber, err)
			return
		}
	}

	fileInode.LogSegmentMap = make(map[uint64]uint64)
	fileInode.Size = 0
	fileInode.NumWrites = 0

	err = nil
	return
}

func (vS *volumeStruct) Coalesce(destInodeNumber InodeNumber, elements []*CoalesceElement) (coalesceTime time.Time, numWrites uint64, fileSize uint64, err error) {
	var (
		alreadyInInodeMap                  bool
		destInode                          *inMemoryInodeStruct
		destInodeExtentMap                 sortedmap.BPlusTree
		destInodeOffsetBeforeElementAppend uint64
		dirEntryInodeNumber                InodeNumber
		element                            *CoalesceElement
		elementInode                       *inMemoryInodeStruct
		elementInodeExtent                 *fileExtentStruct
		elementInodeExtentAsValue          sortedmap.Value
		elementInodeExtentMap              sortedmap.BPlusTree
		elementInodeExtentMapIndex         int
		elementInodeExtentMapLen           int
		inodeList                          []*inMemoryInodeStruct
		inodeMap                           map[InodeNumber]*inMemoryInodeStruct
		localErr                           error
		logSegmentReferencedBytes          uint64
		ok                                 bool
		snapShotIDType                     headhunter.SnapShotIDType
	)

	// Validate all referenced {Dir|File}Inodes

	inodeMap = make(map[InodeNumber]*inMemoryInodeStruct)
	inodeList = make([]*inMemoryInodeStruct, 0, 1+len(elements))

	snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(destInodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = blunder.NewError(blunder.PermDeniedError, "Coalesce into non-LiveView destInodeNumber 0x%016X not allowed", destInodeNumber)
		return
	}

	destInode, ok, err = vS.fetchInode(destInodeNumber)
	if nil != err {
		err = blunder.NewError(blunder.BadFileError, "Coalesce() couldn't fetch destInodeNumber 0x%016X: %v", destInodeNumber, err)
		return
	}
	if !ok {
		err = blunder.NewError(blunder.NotFoundError, "Coalesce() couldn't find destInodeNumber 0x%16X", destInodeNumber)
		return
	}
	if destInode.InodeType != FileType {
		err = blunder.NewError(blunder.PermDeniedError, "Coalesce() called for destInodeNumber 0x%016X that is not a FileInode", destInodeNumber)
		return
	}

	inodeMap[destInodeNumber] = destInode
	inodeList = append(inodeList, destInode)

	for _, element = range elements {
		snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(element.ElementInodeNumber))
		if headhunter.SnapShotIDTypeLive != snapShotIDType {
			err = blunder.NewError(blunder.PermDeniedError, "Coalesce() from non-LiveView element.ElementInodeNumber (0x%016X) not allowed", element.ElementInodeNumber)
			return
		}

		_, alreadyInInodeMap = inodeMap[element.ElementInodeNumber]
		if alreadyInInodeMap {
			err = blunder.NewError(blunder.InvalidArgError, "Coalesce() called with duplicate Element Inode 0x%016X", element.ElementInodeNumber)
			return
		}

		elementInode, ok, err = vS.fetchInode(element.ElementInodeNumber)
		if nil != err {
			err = blunder.NewError(blunder.BadFileError, "Coalesce() couldn't fetch ElementInodeNumber 0x%016X: %v", element.ElementInodeNumber, err)
			return
		}

		inodeMap[element.ElementInodeNumber] = elementInode
		inodeList = append(inodeList, elementInode)

		if !ok {
			err = blunder.NewError(blunder.NotFoundError, "Coalesce() couldn't find ElementInodeNumber 0x%16X", element.ElementInodeNumber)
			return
		}
		if elementInode.InodeType != FileType {
			err = blunder.NewError(blunder.PermDeniedError, "Coalesce() called for ElementInodeNumber 0x%016X that is not a FileInode", element.ElementInodeNumber)
			return
		}
		if elementInode.LinkCount != 1 {
			err = blunder.NewError(blunder.TooManyLinksError, "Coalesce() called for ElementInodeNumber 0x%016X with LinkCount not == 1 (%v)", element.ElementInodeNumber, elementInode.LinkCount)
			return
		}

		dirEntryInodeNumber, err = vS.lookup(element.ContainingDirectoryInodeNumber, element.ElementName)
		if nil != err {
			err = blunder.NewError(blunder.InvalidArgError, "Coalesce() called for ElementName %s not found in ContainingDir 0x%016X: %v", element.ElementName, element.ContainingDirectoryInodeNumber, err)
			return
		}
		if dirEntryInodeNumber != element.ElementInodeNumber {
			err = blunder.NewError(blunder.InvalidArgError, "Coalesce() called for ElementName %s in ContainingDir 0x%016X had mismatched InodeNumber", element.ElementName, element.ContainingDirectoryInodeNumber)
			return
		}
	}

	// Ensure all referenced FileInodes are pre-flushed

	err = vS.flushInodes(inodeList)
	if nil != err {
		err = blunder.NewError(blunder.InvalidArgError, "Coalesce() unable to flush inodeList: %v", err)
		return
	}

	// Now truncate destInode & "append" each Element's extents to destInode (creating duplicate references to LogSegments for now)

	destInodeExtentMap = destInode.payload.(sortedmap.BPlusTree)

	destInode.dirty = true

	err = setSizeInMemory(destInode, 0)
	if nil != err {
		err = blunder.NewError(blunder.InvalidArgError, "Coalesce() unable to truncate destInodeNumber 0x%016X: %v", destInodeNumber, err)
		return
	}

	destInodeOffsetBeforeElementAppend = 0
	destInode.NumWrites = 0

	for _, element = range elements {
		elementInode = inodeMap[element.ElementInodeNumber]
		destInode.NumWrites += elementInode.NumWrites
		elementInodeExtentMap = elementInode.payload.(sortedmap.BPlusTree)
		elementInodeExtentMapLen, err = elementInodeExtentMap.Len()
		for elementInodeExtentMapIndex = 0; elementInodeExtentMapIndex < elementInodeExtentMapLen; elementInodeExtentMapIndex++ {
			_, elementInodeExtentAsValue, ok, err = elementInodeExtentMap.GetByIndex(elementInodeExtentMapIndex)
			if nil != err {
				localErr = vS.resetFileInodeInMemory(destInode)
				if nil != localErr {
					logger.Fatalf("Coalesce() doing resetFileInodeInMemory(destInode) failed: %v", localErr)
				}
				localErr = vS.flushInode(destInode)
				if nil != localErr {
					logger.Errorf("Coalesce() doing flushInode(destInode) failed: %v", localErr)
				}
				err = blunder.NewError(blunder.InvalidArgError, "Coalesce() unable to fetch fileExtentStruct from ExtentMap: %v", err)
				return
			}
			elementInodeExtent = elementInodeExtentAsValue.(*fileExtentStruct)
			elementInodeExtent.FileOffset += destInodeOffsetBeforeElementAppend
			_, err = destInodeExtentMap.Put(elementInodeExtent.FileOffset, elementInodeExtent)
			if nil != err {
				localErr = vS.resetFileInodeInMemory(destInode)
				if nil != localErr {
					logger.Fatalf("Coalesce() doing resetFileInodeInMemory(destInode) failed: %v", localErr)
				}
				localErr = vS.flushInode(destInode)
				if nil != localErr {
					logger.Errorf("Coalesce() doing flushInode(destInode) failed: %v", localErr)
				}
				err = blunder.NewError(blunder.InvalidArgError, "Coalesce() unable to append elementInodeExtent to destInodeExtentMap: %v", err)
				return
			}
			logSegmentReferencedBytes, ok = destInode.LogSegmentMap[elementInodeExtent.LogSegmentNumber]
			if ok {
				destInode.LogSegmentMap[elementInodeExtent.LogSegmentNumber] = elementInodeExtent.Length + logSegmentReferencedBytes
			} else {
				destInode.LogSegmentMap[elementInodeExtent.LogSegmentNumber] = elementInodeExtent.Length
			}
		}
		destInodeOffsetBeforeElementAppend += elementInode.Size
		err = setSizeInMemory(destInode, destInodeOffsetBeforeElementAppend)
		if nil != err {
			localErr = vS.resetFileInodeInMemory(destInode)
			if nil != localErr {
				logger.Fatalf("Coalesce() doing resetFileInodeInMemory(destInode) failed: %v", localErr)
			}
			localErr = vS.flushInode(destInode)
			if nil != localErr {
				logger.Errorf("Coalesce() doing flushInode(destInode) failed: %v", localErr)
			}
			err = blunder.NewError(blunder.InvalidArgError, "Coalesce() unable to setSize() destInodeNumber 0x%016X: %v", destInodeNumber, err)
			return
		}
	}

	// Now, destInode is fully assembled... update its metadata & assemble remaining results

	coalesceTime = time.Now()

	destInode.CreationTime = coalesceTime
	destInode.AttrChangeTime = coalesceTime
	destInode.ModificationTime = coalesceTime

	numWrites = destInode.NumWrites

	fileSize = destInode.Size

	// Now, destInode is fully assembled... need to remove all elements references to currently shared LogSegments

	for _, element = range elements {
		localErr = vS.resetFileInodeInMemory(inodeMap[element.ElementInodeNumber])
		if nil != err {
			logger.Fatalf("Coalesce() doing resetFileInodeInMemory(inodeMap[element.ElementInodeNumber]) failed: %v", localErr)
		}
	}

	// Time to flush all affected FileInodes

	err = vS.flushInodes(inodeList)
	if nil != err {
		err = fmt.Errorf("Coalesce() doing flushInodes(inodeList) failed: %v", err)
		return
	}

	// Now we can Unlink and Destroy each element

	for _, element = range elements {
		err = vS.Unlink(element.ContainingDirectoryInodeNumber, element.ElementName, false)
		if nil != err {
			err = fmt.Errorf("Coalesce() doing Unlink(element.ContainingDirectoryInodeNumber, element.ElementName, false) failed: %v", err)
			return
		}
		err = vS.Destroy(element.ElementInodeNumber)
		if nil != err {
			err = fmt.Errorf("Coalesce() doing Destroy(element.ElementInodeNumber) failed: %v", err)
			return
		}
	}

	// All done

	err = nil
	return
}

func (vS *volumeStruct) setLogSegmentContainer(logSegmentNumber uint64, containerName string) (err error) {
	containerNameAsByteSlice := utils.StringToByteSlice(containerName)
	err = vS.headhunterVolumeHandle.PutLogSegmentRec(logSegmentNumber, containerNameAsByteSlice)
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

func (vS *volumeStruct) getObjectLocationFromLogSegmentNumber(logSegmentNumber uint64) (containerName string, objectName string, objectPath string, err error) {
	var (
		nonce uint64
	)

	containerName, err = vS.getLogSegmentContainer(logSegmentNumber)
	if nil != err {
		return
	}

	_, _, nonce = vS.headhunterVolumeHandle.SnapShotU64Decode(logSegmentNumber)

	objectName = fmt.Sprintf("%016X", nonce)
	objectPath = fmt.Sprintf("/v1/%s/%s/%016X", vS.accountName, containerName, nonce)
	return
}
