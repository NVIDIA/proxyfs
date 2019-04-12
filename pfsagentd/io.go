package main

import (
	"container/list"
	"fmt"
	"net/http"
	"strconv"

	"bazil.org/fuse"

	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/utils"
	"github.com/swiftstack/sortedmap"
)

func handleReadRequestFileInodeCase(request *fuse.ReadRequest) {
	var (
		curObjectOffset                           uint64
		err                                       error
		fileInode                                 *fileInodeStruct
		grantedLock                               *fileInodeLockRequestStruct
		logSegmentCacheElement                    *logSegmentCacheElementStruct
		logSegmentCacheElementBufEndingPosition   uint64
		logSegmentCacheElementBufRemainingLen     uint64
		logSegmentCacheElementBufSelectedLen      uint64
		logSegmentCacheElementBufStartingPosition uint64
		readPlan                                  []*multiObjectExtentStruct
		readPlanSpan                              uint64
		readPlanStep                              *multiObjectExtentStruct
		readPlanStepRemainingLength               uint64
		response                                  *fuse.ReadResponse
	)

	fileInode = referenceFileInode(inode.InodeNumber(request.Header.Node))
	defer fileInode.dereference()

	grantedLock = fileInode.getSharedLock()
	defer grantedLock.release()

	err = fileInode.populateExtentMap(uint64(request.Offset), uint64(request.Size))
	if nil != err {
		request.RespondError(fuse.EIO)
		return
	}

	readPlan, readPlanSpan = fileInode.getReadPlan(uint64(request.Offset), uint64(request.Size))

	if (nil == readPlan) || (0 == readPlanSpan) {
		response = &fuse.ReadResponse{
			Data: make([]byte, 0),
		}
	} else {
		response = &fuse.ReadResponse{
			Data: make([]byte, 0, readPlanSpan),
		}

		// TODO: Need to handle readPlanStep's that are either *multiObjectExtentStruct or *singleObjectExtentStruct

		for _, readPlanStep = range readPlan {
			if "" == readPlanStep.objectName {
				// Zero-fill for readPlanStep.length

				response.Data = append(response.Data, make([]byte, readPlanStep.length)...)
			} else {
				// Fetch LogSegment data... possibly crossing LogSegmentCacheLine boundaries

				curObjectOffset = readPlanStep.objectOffset
				readPlanStepRemainingLength = readPlanStep.length

				for readPlanStepRemainingLength > 0 {
					logSegmentCacheElement = fetchLogSegmentCacheLine(readPlanStep.containerName, readPlanStep.objectName, curObjectOffset)

					if logSegmentCacheElementStateGetFailed == logSegmentCacheElement.state {
						request.RespondError(fuse.EIO)
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

					response.Data = append(response.Data, logSegmentCacheElement.buf[logSegmentCacheElementBufStartingPosition:logSegmentCacheElementBufEndingPosition]...)

					curObjectOffset += logSegmentCacheElementBufSelectedLen
					readPlanStepRemainingLength -= logSegmentCacheElementBufSelectedLen
				}
			}
		}
	}

	request.Respond(response)
}

func handleWriteRequest(request *fuse.WriteRequest) {
	var (
		fileInode   *fileInodeStruct
		grantedLock *fileInodeLockRequestStruct
	)

	fileInode = referenceFileInode(inode.InodeNumber(request.Header.Node))
	defer fileInode.dereference()

	grantedLock = fileInode.getExclusiveLock()
	defer grantedLock.release()

	logInfof("TODO: handleWriteRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

// populateExtentMap ensures that the range [fileOffset:fileOffset+length) is covered by
// fileInode.extentMap whether or not it is needed. Extents won't be needed if there is
// some fileInode.chunkedPutListElement that logically supercedes it.
//
func (fileInode *fileInodeStruct) populateExtentMap(fileOffset uint64, length uint64) (err error) {
	var (
		curExtent        *multiObjectExtentStruct
		curExtentAsValue sortedmap.Value
		curExtentIndex   int
		curFileOffset    uint64
		ok               bool
	)

	fileInode.Lock()
	defer fileInode.Unlock()

	if nil == fileInode.extentMap {
		// Create an empty ExtentMap... and perform initial population
		// This counts as a reference, too

		fileInode.reference()

		fileInode.extentMap = sortedmap.NewLLRBTree(sortedmap.CompareUint64, fileInode)

		err = fileInode.populateExtentMapHelper(fileOffset)
		if nil != err {
			fileInode.extentMap = nil
			fileInode.dereference()
			return
		}
	}

	// Handle cases where [fileOffset:fileOffset+length) references beyond FileSize

	if fileOffset >= fileInode.extentMapFileSize {
		// The entire [fileOffset:fileOffset+length) lies beyond FileSize... so just return

		err = nil
		return
	}
	if (fileOffset + length) > fileInode.extentMapFileSize {
		// Truncate length since ExtentMap doesn't contain any extent beyond FileSize

		length = fileInode.extentMapFileSize - fileOffset
	}

Restart:

	curFileOffset = fileOffset

	curExtentIndex, _, err = fileInode.extentMap.BisectLeft(curFileOffset)
	if nil != err {
		logFatalf("populateExtentMap() couldn't fetch extent [Case 1]: %v", err)
	}

	// Note it is possible for curExtentIndex == -1 if no extents are at or preceed fileOffset

	for curFileOffset < (fileOffset + length) {
		_, curExtentAsValue, ok, err = fileInode.extentMap.GetByIndex(curExtentIndex)
		if nil != err {
			logFatalf("populateExtentMap() couldn't fetch extent [Case 2]: %v", err)
		}

		if !ok {
			// No extent at curExtentIndex - so populate from here and restart

			err = fileInode.populateExtentMapHelper(curFileOffset)
			if nil != err {
				fileInode.extentMap = nil
				fileInode.dereference()
				return
			}
			goto Restart
		}

		curExtent, ok = curExtentAsValue.(*multiObjectExtentStruct)
		if !ok {
			logFatalf("populateExtentMap() couldn't fetch extent [Case 3]: %v", err)
		}

		if curFileOffset < curExtent.fileOffset {
			// Next extent starts after curFileOffset - so populate the hole and restart

			err = fileInode.populateExtentMapHelper(curFileOffset)
			if nil != err {
				fileInode.extentMap = nil
				fileInode.dereference()
				return
			}
			goto Restart
		}

		if curFileOffset >= (curExtent.fileOffset + curExtent.length) {
			// Handle case where BisectLeft pointed at an extent before fileOffset
			// and this extent ends before fileOffset - so populate from there and restart

			err = fileInode.populateExtentMapHelper(curExtent.fileOffset + curExtent.length)
			if nil != err {
				fileInode.extentMap = nil
				fileInode.dereference()
				return
			}
			goto Restart
		}

		// Advance to next extent to check for contiguity

		curFileOffset = curExtent.fileOffset + curExtent.length
		curExtentIndex++
	}

	return
}

// populateExtentMapHelper fetches an ExtentMapChunk anchored by fileOffset and inserts
// it into fileInode.extentMap using updateExtentMap().
//
func (fileInode *fileInodeStruct) populateExtentMapHelper(fileOffset uint64) (err error) {
	var (
		curExtent                  *multiObjectExtentStruct
		curExtentAsValue           sortedmap.Value
		curExtentMapChunkIndex     int
		curFileOffset              uint64
		extentMapEntry             *inode.ExtentMapEntryStruct
		extentMapLength            int
		fetchExtentMapChunkReply   *jrpcfs.FetchExtentMapChunkReply
		fetchExtentMapChunkRequest *jrpcfs.FetchExtentMapChunkRequest
		ok                         bool
	)

	fetchExtentMapChunkRequest = &jrpcfs.FetchExtentMapChunkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(fileInode.InodeNumber), // TODO: Check if SnapShot's work with this
		},
		FileOffset:                 fileOffset,
		MaxEntriesFromFileOffset:   int64(globals.config.FetchExtentsFromFileOffset),
		MaxEntriesBeforeFileOffset: int64(globals.config.FetchExtentsBeforeFileOffset),
	}

	fetchExtentMapChunkReply = &jrpcfs.FetchExtentMapChunkReply{}

	err = doJRPCRequest("Server.RpcFetchExtentMapChunk", fetchExtentMapChunkRequest, fetchExtentMapChunkReply)
	if nil != err {
		return
	}

	curFileOffset = fetchExtentMapChunkReply.FileOffsetRangeStart

	for curExtentMapChunkIndex = range fetchExtentMapChunkReply.ExtentMapEntry {
		extentMapEntry = &fetchExtentMapChunkReply.ExtentMapEntry[curExtentMapChunkIndex]

		if curFileOffset < extentMapEntry.FileOffset {
			// We need to insert a preceeding zero-fill extent first

			curExtent = &multiObjectExtentStruct{
				fileOffset:    curFileOffset,
				containerName: "",
				objectName:    "",
				objectOffset:  0,
				length:        extentMapEntry.FileOffset - curFileOffset,
			}

			_, err = fileInode.extentMap.Put(curExtent.fileOffset, curExtent)
			if nil != err {
				logFatalf("populateExtentMap() couldn't insert zero-fill extent [Case 1]: %v", err)
			}
		}

		// Insert the actual extent

		curExtent = &multiObjectExtentStruct{
			fileOffset:    extentMapEntry.FileOffset,
			containerName: extentMapEntry.ContainerName,
			objectName:    extentMapEntry.ObjectName,
			objectOffset:  extentMapEntry.LogSegmentOffset,
			length:        extentMapEntry.Length,
		}

		_, err = fileInode.extentMap.Put(curExtent.fileOffset, curExtent)
		if nil != err {
			logFatalf("populateExtentMap() couldn't insert extent: %v", err)
		}

		curFileOffset = extentMapEntry.FileOffset + extentMapEntry.Length
	}

	if curFileOffset < fetchExtentMapChunkReply.FileOffsetRangeEnd {
		// We need to insert a trailing  zero-fill extent

		curExtent = &multiObjectExtentStruct{
			fileOffset:    curFileOffset,
			containerName: "",
			objectName:    "",
			objectOffset:  0,
			length:        fetchExtentMapChunkReply.FileOffsetRangeEnd - curFileOffset,
		}

		_, err = fileInode.extentMap.Put(curExtent.fileOffset, curExtent)
		if nil != err {
			logFatalf("populateExtentMap() couldn't insert zero-fill extent [Case 2]: %v", err)
		}
	}

	// Finally, we need to set FileSize and trim, as necessary, excess extents

	fileInode.extentMapFileSize = fetchExtentMapChunkReply.FileSize

	extentMapLength, err = fileInode.extentMap.Len()
	if nil != err {
		logFatalf("populateExtentMap() couldn't get number of extents: %v", err)
	}

	for {
		if 0 == extentMapLength {
			return
		}

		_, curExtentAsValue, ok, err = fileInode.extentMap.GetByIndex(extentMapLength - 1)
		if nil != err {
			logFatalf("populateExtentMap() couldn't get last extent [Case 1]: %v", err)
		}
		if !ok {
			logFatalf("populateExtentMap() couldn't get last extent [Case 2]")
		}

		curExtent, ok = curExtentAsValue.(*multiObjectExtentStruct)
		if !ok {
			logFatalf("populateExtentMap() couldn't get last extent [Case 3]")
		}

		if (curExtent.fileOffset + curExtent.length) <= fileInode.extentMapFileSize {
			// Last extent does not extend beyond FileSize... so we are done

			return
		}

		if curExtent.fileOffset < fileInode.extentMapFileSize {
			// Last extent crossed FileSize boundary... truncate it and we are done

			curExtent.length = fileInode.extentMapFileSize - curExtent.fileOffset

			return
		}

		// Last extent completely beyond FileSize... just delete it and loop

		fileInode.extentMap.DeleteByIndex(extentMapLength - 1)

		extentMapLength--
	}
}

// updateExtentMap is called (while fileInode is locked) to update the Extent Map and,
// as necessary, the File Size with the supplied multiObjectExtent. This func is used
// both by populateExtentMapHelper() and TODO: fill in name of the eventual "merge" func upon Chunked PUT completion.
//
func (fileInode *fileInodeStruct) updateExtentMap(newExtent *multiObjectExtentStruct) {
	var (
		curExtent           *multiObjectExtentStruct
		curExtentAsValue    sortedmap.Value
		curExtentIndex      int
		curExtentLostLength uint64
		err                 error
		found               bool
		ok                  bool
		prevExtent          *multiObjectExtentStruct
		prevExtentAsValue   sortedmap.Value
		prevExtentIndex     int
		prevExtentNewLength uint64
		splitExtent         *multiObjectExtentStruct
	)

	// Locate prevExtent (if any)

	prevExtentIndex, found, err = fileInode.extentMap.BisectLeft(newExtent.fileOffset)
	if nil != err {
		logFatalf("updateExtentMap() couldn't find prevExtent [Case 1]: %v", err)
	}

	if found {
		// Make prevExtentIndex truly point to the previous (and non-overlapping) extent (if any)

		prevExtentIndex--
	} else {
		if prevExtentIndex < 0 {
			// No prevExtent exists... so it cannot need to be split
		} else {
			// A prevExtent exists... but does it need to be split?

			_, prevExtentAsValue, ok, err = fileInode.extentMap.GetByIndex(prevExtentIndex)
			if nil != err {
				logFatalf("updateExtentMap() couldn't find prevExtent [Case 2]: %v", err)
			}
			if !ok {
				logFatalf("updateExtentMap() couldn't find prevExtent [Case 3]")
			}
			prevExtent, ok = prevExtentAsValue.(*multiObjectExtentStruct)
			if !ok {
				logFatalf("updateExtentMap() couldn't find prevExtent [Case 4]")
			}

			if (prevExtent.fileOffset + prevExtent.length) > newExtent.fileOffset {
				// Existing prevExtent does overlap... so we need to split it now

				prevExtentNewLength = newExtent.fileOffset - prevExtent.fileOffset

				splitExtent = &multiObjectExtentStruct{
					fileOffset:    newExtent.fileOffset,
					containerName: prevExtent.containerName,
					objectName:    prevExtent.objectName,
					objectOffset:  prevExtent.objectOffset + prevExtentNewLength,
					length:        prevExtent.length - prevExtentNewLength,
				}

				prevExtent.length = prevExtentNewLength

				ok, err = fileInode.extentMap.Put(splitExtent.fileOffset, splitExtent)
				if nil != err {
					logFatalf("updateExtentMap() couldn't split prevExtent [Case 1]: %v", err)
				}
				if !ok {
					logFatalf("updateExtentMap() couldn't split prevExtent [Case 2]")
				}
			} else {
				// Existing prevExtent does not overlap
			}
		}
	}

	// Now loop thru extents after prevExtent replaced (partially or fully) by newExtent

	curExtentIndex = prevExtentIndex + 1

	for {
		_, curExtentAsValue, ok, err = fileInode.extentMap.GetByIndex(curExtentIndex)
		if nil != err {
			logFatalf("updateExtentMap() couldn't find curExtent [Case 1]: %v", err)
		}

		if !ok {
			// Simple case where we walked off the end of the ExtentMap

			break
		}

		curExtent, ok = curExtentAsValue.(*multiObjectExtentStruct)
		if !ok {
			logFatalf("updateExtentMap() couldn't find curExtent [Case 2]: %v", err)
		}

		if (newExtent.fileOffset + newExtent.length) <= curExtent.fileOffset {
			// Simple case where we walked to an Extent that start after newExtent

			break
		}

		// At this point, we know we are either going to split curExtent
		// or replace it... so we must remove it from the ExtentMap regardless
		// (since splitting curExtent would change it's fileOffset Key)

		ok, err = fileInode.extentMap.DeleteByIndex(curExtentIndex)
		if nil != err {
			logFatalf("updateExtentMap() couldn't delete curExtent [Case 1]: %v", err)
		}
		if !ok {
			logFatalf("updateExtentMap() couldn't delete curExtent [Case 2]: %v", err)
		}

		if (curExtent.fileOffset + curExtent.length) > (newExtent.fileOffset + newExtent.length) {
			// We need to split curExtent because newExtent only partially replaces it

			curExtentLostLength = (newExtent.fileOffset + newExtent.length) - curExtent.fileOffset

			splitExtent = &multiObjectExtentStruct{
				fileOffset:    curExtent.fileOffset + curExtentLostLength,
				containerName: curExtent.containerName,
				objectName:    curExtent.objectName,
				objectOffset:  curExtent.objectOffset + curExtentLostLength,
				length:        curExtent.length - curExtentLostLength,
			}

			ok, err = fileInode.extentMap.Put(splitExtent.fileOffset, splitExtent)
			if nil != err {
				logFatalf("updateExtentMap() couldn't split curExtent [Case 1]: %v", err)
			}
			if !ok {
				logFatalf("updateExtentMap() couldn't split curExtent [Case 2]")
			}

			// We also know that we are done scanning

			break
		}
	}

	// We can finally insert newExtent without fear of colliding with existing extents

	ok, err = fileInode.extentMap.Put(newExtent.fileOffset, newExtent)
	if nil != err {
		logFatalf("updateExtentMap() couldn't insert newExtent [Case 1]: %v", err)
	}
	if !ok {
		logFatalf("updateExtentMap() couldn't insert newExtent [Case 2]")
	}

	if (newExtent.fileOffset + newExtent.length) > fileInode.extentMapFileSize {
		fileInode.extentMapFileSize = newExtent.fileOffset + newExtent.length
	}
}

func (fileInode *fileInodeStruct) getReadPlan(fileOffset uint64, length uint64) (readPlan []*multiObjectExtentStruct, readPlanSpan uint64) {
	var (
		curExtent        *multiObjectExtentStruct
		curExtentAsValue sortedmap.Value
		curExtentIndex   int
		curFileOffset    uint64
		err              error
		ok               bool
		readPlanStep     *multiObjectExtentStruct
		remainingLength  uint64
	)

	fileInode.Lock()
	defer fileInode.Unlock()

	// TODO: When writes are supported, must pre-consult fileInode.chunkedPutList from Back() to Front()

	curExtentIndex, _, err = fileInode.extentMap.BisectLeft(fileOffset)
	if nil != err {
		logFatalf("getReadPlan() couldn't find curExtent: %v", err)
	}

	curFileOffset = fileOffset
	remainingLength = length

	readPlan = make([]*multiObjectExtentStruct, 0, 1)

	for remainingLength > 0 {
		_, curExtentAsValue, ok, err = fileInode.extentMap.GetByIndex(curExtentIndex)
		if nil != err {
			logFatalf("getReadPlan() couldn't find curExtent [Case 1]: %v", err)
		}

		if !ok {
			// Crossed EOF - just return what we have in readPlan

			readPlanSpan = length - remainingLength
			return
		}

		curExtent, ok = curExtentAsValue.(*multiObjectExtentStruct)
		if !ok {
			logFatalf("getReadPlan() couldn't find curExtent [Case 2]: %v", err)
		}

		readPlanStep = &multiObjectExtentStruct{
			fileOffset:    curFileOffset,
			containerName: curExtent.containerName,
			objectName:    curExtent.objectName, // May be == 0
			objectOffset:  curExtent.objectOffset + (curFileOffset - curExtent.fileOffset),
			length:        curExtent.length - (curFileOffset - curExtent.fileOffset),
		}

		if remainingLength < readPlanStep.length {
			// This is the last readPlanStep and needs to be truncated

			readPlanStep.length = remainingLength
		}

		if 0 == readPlanStep.length {
			// Reached EOF - just return what we have in readPlan

			readPlanSpan = length - remainingLength
			return
		}

		readPlan = append(readPlan, readPlanStep)

		curFileOffset += readPlanStep.length
		remainingLength -= readPlanStep.length

		curExtentIndex++
	}

	// If we got this far, readPlanSpan is what caller asked for

	readPlanSpan = length
	return
}

func fetchLogSegmentCacheLine(containerName string, objectName string, offset uint64) (logSegmentCacheElement *logSegmentCacheElementStruct) {
	var (
		err                                     error
		getRequest                              *http.Request
		logSegmentCacheElementKey               logSegmentCacheElementKeyStruct
		logSegmentCacheElementToEvict           *logSegmentCacheElementStruct
		logSegmentCacheElementToEvictKey        logSegmentCacheElementKeyStruct
		logSegmentCacheElementToEvictLRUElement *list.Element
		logSegmentEnd                           uint64
		logSegmentStart                         uint64
		ok                                      bool
		url                                     string
	)

	// Compute Key for LogSegment Cache lookup

	logSegmentCacheElementKey.logSegmentNumber, err = strconv.ParseUint(objectName, 16, 64)
	if nil != err {
		logWarnf("fetchLogSegmentCacheLine() passed un-parseable objectName: \"%s\" (err: %v)", objectName, err)
		logSegmentCacheElement = nil
		return
	}

	logSegmentCacheElementKey.cacheLineTag = offset / globals.config.ReadCacheLineSize

	// Perform lookup

	globals.Lock()

	logSegmentCacheElement, ok = globals.logSegmentCacheMap[logSegmentCacheElementKey]

	if ok {
		// Found it... so move it to MRU end of LRU

		globals.logSegmentCacheLRU.MoveToBack(logSegmentCacheElement.cacheLRUElement)

		globals.Unlock()

		// It may not be here yet, so wait for it to be

		logSegmentCacheElement.Wait()

		// GET may have failed... LogSegment disappeared

		if logSegmentCacheElementStateGetFailed == logSegmentCacheElement.state {
			logInfof("LogSegment %s/%s at Offset 0x%016X not available", containerName, objectName, logSegmentCacheElementKey.cacheLineTag*globals.config.ReadCacheLineSize)
			logSegmentCacheElement = nil
		}

		// In any case, we can now return

		return
	}

	// Make room for new LogSegment Cache Line if necessary

	for uint64(globals.logSegmentCacheLRU.Len()) >= globals.config.ReadCacheLineCount {
		logSegmentCacheElementToEvictLRUElement = globals.logSegmentCacheLRU.Front()
		logSegmentCacheElementToEvict = logSegmentCacheElementToEvictLRUElement.Value.(*logSegmentCacheElementStruct)
		logSegmentCacheElementToEvictKey.logSegmentNumber, err = strconv.ParseUint(logSegmentCacheElementToEvict.objectName, 16, 64)
		if nil != err {
			logFatalf("fetchLogSegmentCacheLine() evicting hit un-parseable objectName: \"%s\" (err: %v)", logSegmentCacheElementToEvict.objectName, err)
		}
		delete(globals.logSegmentCacheMap, logSegmentCacheElementToEvictKey)
		globals.logSegmentCacheLRU.Remove(logSegmentCacheElementToEvictLRUElement)
	}

	// Create new LogSegment Cache Line...as yet unfilled

	logSegmentCacheElement = &logSegmentCacheElementStruct{
		state:          logSegmentCacheElementStateGetIssued,
		containerName:  containerName,
		objectName:     objectName,
		startingOffset: logSegmentCacheElementKey.cacheLineTag * globals.config.ReadCacheLineSize,
	}

	logSegmentCacheElement.Add(1)

	// Make it findable while we fill it (to avoid multiple copies)

	globals.logSegmentCacheMap[logSegmentCacheElementKey] = logSegmentCacheElement
	logSegmentCacheElement.cacheLRUElement = globals.logSegmentCacheLRU.PushBack(logSegmentCacheElement)

	globals.Unlock()

	// Issue GET for it

	url = globals.swiftAccountURL + "/" + containerName + "/" + objectName

	logSegmentStart = logSegmentCacheElementKey.cacheLineTag * globals.config.ReadCacheLineSize
	logSegmentEnd = logSegmentStart + globals.config.ReadCacheLineSize - 1

	getRequest, err = http.NewRequest(http.MethodGet, url, nil)
	if nil != err {
		logFatalf("unable to create GET http.Request (,%s,): %v", url)
	}

	getRequest.Header.Add("X-Bypass-Proxyfs", "true")
	getRequest.Header.Add("Range", fmt.Sprintf("Bytes=%d-%d", logSegmentStart, logSegmentEnd))

	_, logSegmentCacheElement.buf, ok, _ = doHTTPRequest(getRequest, http.StatusOK, http.StatusPartialContent)

	globals.Lock()

	if ok {
		logSegmentCacheElement.state = logSegmentCacheElementStateGetSuccessful
	} else {
		logSegmentCacheElement.state = logSegmentCacheElementStateGetFailed

		// Remove it from the LogSegment Cache as well

		delete(globals.logSegmentCacheMap, logSegmentCacheElementKey)
		globals.logSegmentCacheLRU.Remove(logSegmentCacheElement.cacheLRUElement)
	}

	globals.Unlock()

	// Signal any (other) waiters GET completed (either successfully or not) before returning

	logSegmentCacheElement.Done()

	return
}

// DumpKey formats the Key (multiObjectExtentStruct.fileOffset) for fileInodeStruct.ExtentMap
func (fileInode *fileInodeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		keyAsU64 uint64
		ok       bool
	)

	keyAsU64, ok = key.(uint64)
	if ok {
		keyAsString = fmt.Sprintf("0x%016X", keyAsU64)
	} else {
		err = fmt.Errorf("Failure of *fileInodeStruct.DumpKey(%v)", key)
	}

	return
}

// DumpKey formats the Value (multiObjectExtentStruct) for fileInodeStruct.ExtentMap
func (fileInode *fileInodeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		ok                             bool
		valueAsMultiObjextExtentStruct *multiObjectExtentStruct
	)

	valueAsMultiObjextExtentStruct, ok = value.(*multiObjectExtentStruct)
	if ok {
		valueAsString = fmt.Sprintf(
			"{fileOffset:0x%016X,containerName:%s,objectName:%s,objectOffset:0x%016X,length:0x%016X}",
			valueAsMultiObjextExtentStruct.fileOffset,
			valueAsMultiObjextExtentStruct.containerName,
			valueAsMultiObjextExtentStruct.objectName,
			valueAsMultiObjextExtentStruct.objectOffset,
			valueAsMultiObjextExtentStruct.length)
	} else {
		err = fmt.Errorf("Failure of *fileInodeStruct.DumpValue(%v)", value)
	}

	return
}

// DumpKey formats the Key (singleObjectExtentStruct.fileOffset) for chunkedPutContextStruct.ExtentMap
func (chunkedPutContext *chunkedPutContextStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		keyAsU64 uint64
		ok       bool
	)

	keyAsU64, ok = key.(uint64)
	if ok {
		keyAsString = fmt.Sprintf("0x%016X", keyAsU64)
	} else {
		err = fmt.Errorf("Failure of *chunkedPutContextStruct.DumpKey(%v)", key)
	}

	return
}

// DumpKey formats the Value (singleObjectExtentStruct) for chunkedPutContextStruct.ExtentMap
func (chunkedPutContext *chunkedPutContextStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		ok                              bool
		valueAsSingleObjectExtentStruct *singleObjectExtentStruct
	)

	valueAsSingleObjectExtentStruct, ok = value.(*singleObjectExtentStruct)
	if ok {
		valueAsString = fmt.Sprintf(
			"{fileOffset:0x%016X,objectOffset:0x%016X,length:0x%016X}",
			valueAsSingleObjectExtentStruct.fileOffset,
			valueAsSingleObjectExtentStruct.objectOffset,
			valueAsSingleObjectExtentStruct.length)
	} else {
		err = fmt.Errorf("Failure of *chunkedPutContextStruct.DumpValue(%v)", value)
	}

	return
}
