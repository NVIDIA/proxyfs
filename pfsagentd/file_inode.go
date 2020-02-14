package main

import (
	"container/list"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
)

// doFlushIfNecessary (the non-receiver form) is currently necessary due to the lack
// of Lease Management whereby an implicitly deleted fileInode (due to a DoUnlink()
// or DoRename/DoRename2() call or equivalent somewhere) would revoke such Lease
// causing any in-flight LogSegment PUTs to be flushed first. In the meantime, this
// func will do the flush if necessary based on what *this* PFSAgent instance is
// doing.
//
func doFlushIfNecessary(dirInodeNumber inode.InodeNumber, name []byte) {
	var (
		err           error
		fileInode     *fileInodeStruct
		lookupReply   *jrpcfs.InodeReply
		lookupRequest *jrpcfs.LookupRequest
	)

	lookupRequest = &jrpcfs.LookupRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(dirInodeNumber),
		},
		Basename: string(name[:]),
	}

	lookupReply = &jrpcfs.InodeReply{}

	err = globals.retryRPCClient.Send("RpcLookup", lookupRequest, lookupReply)
	if nil != err {
		// Assume the fileInode simply did not exist, so just return
		return
	}

	fileInode = referenceFileInode(inode.InodeNumber(lookupReply.InodeNumber))

	fileInode.doFlushIfNecessary()

	fileInode.dereference()
}

func (fileInode *fileInodeStruct) doFlushIfNecessary() {
	var (
		chunkedPutContext        *chunkedPutContextStruct
		chunkedPutContextElement *list.Element
		flushWG                  sync.WaitGroup
		grantedLock              *fileInodeLockRequestStruct
	)

	grantedLock = fileInode.getExclusiveLock()

	if 0 == fileInode.chunkedPutList.Len() {
		// No Chunked PUTs in flight... so we can just exit
		grantedLock.release()
		return
	}

	// At least one Chunked PUT is in flight... so we know we'll have to block later

	flushWG.Add(1)
	_ = fileInode.chunkedPutFlushWaiterList.PushBack(&flushWG)

	// Now see if we need to initiate the flush

	if fileInode.flushInProgress {
		// We do not need to send a flush
	} else {
		// No explicit flush is in progress... so make it appear so

		fileInode.flushInProgress = true

		// See if the last Chunked PUT is already flushing anyway

		chunkedPutContextElement = fileInode.chunkedPutList.Back()
		chunkedPutContext = chunkedPutContextElement.Value.(*chunkedPutContextStruct)

		if chunkedPutContextStateOpen == chunkedPutContext.state {
			// We need to trigger a Flush
			chunkedPutContext.state = chunkedPutContextStateClosing
			close(chunkedPutContext.sendChan)
		}
	}

	grantedLock.release()

	// Finally, wait for the flush to complete

	flushWG.Wait()
}

func pruneFileInodeDirtyListIfNecessary() {
	var (
		fileInode        *fileInodeStruct
		fileInodeElement *list.Element
	)

	for {
		globals.Lock()
		if uint64(globals.fileInodeDirtyList.Len()) < globals.config.DirtyFileLimit {
			// There is room for fileInode about to be added to the list
			globals.Unlock()
			return
		}
		fileInodeElement = globals.fileInodeDirtyList.Front()
		globals.Unlock()
		fileInode = fileInodeElement.Value.(*fileInodeStruct)
		fileInode.doFlushIfNecessary()
	}
}

func emptyFileInodeDirtyList() {
	var (
		fileInode        *fileInodeStruct
		fileInodeElement *list.Element
	)

	for {
		globals.Lock()
		if 0 == globals.fileInodeDirtyList.Len() {
			// The list is now empty
			globals.Unlock()
			return
		}
		fileInodeElement = globals.fileInodeDirtyList.Front()
		globals.Unlock()
		fileInode = fileInodeElement.Value.(*fileInodeStruct)
		fileInode.doFlushIfNecessary()
	}
}

func (chunkedPutContext *chunkedPutContextStruct) sendDaemon() {
	var (
		expirationDelay           time.Duration
		expirationTime            time.Time
		fileInode                 *fileInodeStruct
		flushWaiterListElement    *list.Element
		grantedLock               *fileInodeLockRequestStruct
		nextChunkedPutContext     *chunkedPutContextStruct
		nextChunkedPutListElement *list.Element
		sendChanOpenOrNonEmpty    bool
	)

	fileInode = chunkedPutContext.fileInode

	// Kick off Chunked PUT

	chunkedPutContext.Add(1)
	go chunkedPutContext.performChunkedPut()

	// Start MaxFlushTime timer

	expirationTime = time.Now().Add(globals.config.MaxFlushTime)

	// Loop awaiting chunks (that could be explicit flushes) or expirationTime

	for {
		expirationDelay = expirationTime.Sub(time.Now())

		select {
		case <-time.After(expirationDelay):
			// MaxFlushTime-triggered flush requested

			grantedLock = fileInode.getExclusiveLock()
			chunkedPutContext.state = chunkedPutContextStateClosing
			grantedLock.release()

			goto PerformFlush
		case _, sendChanOpenOrNonEmpty = <-chunkedPutContext.sendChan:
			// Send non-flushing chunk to *chunkedPutContextStruct.Read()

			chunkedPutContext.wakeChan <- false

			if !sendChanOpenOrNonEmpty {
				goto PerformFlush
			}
		}
	}

PerformFlush:

	// Send flushing chunk to *chunkedPutContextStruct.Read() & wait for it to finish

	chunkedPutContext.wakeChan <- true
	chunkedPutContext.Wait()

	// Chunked PUT is complete

	grantedLock = fileInode.getExclusiveLock()

	chunkedPutContext.state = chunkedPutContextStateClosed

	// But first, make sure sendChan is drained

	for {
		select {
		case _, sendChanOpenOrNonEmpty = <-chunkedPutContext.sendChan:
			if sendChanOpenOrNonEmpty {
				continue
			} else {
				goto EscapeSendChanDrain
			}
		default:
			goto EscapeSendChanDrain
		}
	}

EscapeSendChanDrain:

	// Can we tell ProxyFS about it and dispose of it?

	if nil == chunkedPutContext.chunkedPutListElement.Prev() {
		// We can record this chunkedPutContext as having completed

		nextChunkedPutListElement = chunkedPutContext.chunkedPutListElement.Next()

		chunkedPutContext.complete()

		// Check to see subsequent chunkedPutContext's are also closed and able to be completed

		for nil != nextChunkedPutListElement {
			nextChunkedPutContext = nextChunkedPutListElement.Value.(*chunkedPutContextStruct)

			if chunkedPutContextStateClosed != nextChunkedPutContext.state {
				// Ran into an un-closed chunkedPutContext...so simply exit the loop
				break
			}

			// We can similarly reccord this chunkedPutContext as having completed

			nextChunkedPutListElement = nextChunkedPutContext.chunkedPutListElement.Next()

			nextChunkedPutContext.complete()
		}
	}

	if 0 == fileInode.chunkedPutList.Len() {
		// Indicate flush is complete

		fileInode.flushInProgress = false

		for fileInode.chunkedPutFlushWaiterList.Len() > 0 {
			flushWaiterListElement = fileInode.chunkedPutFlushWaiterList.Front()
			_ = fileInode.chunkedPutFlushWaiterList.Remove(flushWaiterListElement)
			flushWaiterListElement.Value.(*sync.WaitGroup).Done()
		}

		// Remove globals.fileInode from globals.fileInodeDirtyList

		globals.Lock()
		globals.fileInodeDirtyList.Remove(fileInode.dirtyListElement)
		fileInode.dirtyListElement = nil
		globals.Unlock()
	}

	grantedLock.release()
}

func (chunkedPutContext *chunkedPutContextStruct) performChunkedPut() {
	var (
		chunkedPutRequest            *http.Request
		containerAndObjectNames      string
		containerAndObjectNamesSplit []string
		err                          error
		ok                           bool
		physPathSplit                []string
		provisionObjectReply         *jrpcfs.ProvisionObjectReply
		provisionObjectRequest       *jrpcfs.ProvisionObjectRequest
		statusCode                   int
	)

	provisionObjectRequest = &jrpcfs.ProvisionObjectRequest{
		MountID: globals.mountID,
	}

	provisionObjectReply = &jrpcfs.ProvisionObjectReply{}

	err = globals.retryRPCClient.Send("RpcProvisionObject", provisionObjectRequest, provisionObjectReply)
	if nil != err {
		logFatalf("*chunkedPutContextStruct.performChunkedPut() call to Server.RpcProvisionObject failed: %v", err)
	}

	physPathSplit = strings.SplitAfterN(provisionObjectReply.PhysPath, "/", 4)
	containerAndObjectNames = physPathSplit[len(physPathSplit)-1]
	containerAndObjectNamesSplit = strings.Split(containerAndObjectNames, "/")
	chunkedPutContext.containerName = containerAndObjectNamesSplit[0]
	chunkedPutContext.objectName = containerAndObjectNamesSplit[1]

	chunkedPutRequest, err = http.NewRequest(http.MethodPut, globals.swiftAccountBypassURL+"/"+chunkedPutContext.containerName+"/"+chunkedPutContext.objectName, chunkedPutContext)
	if nil != err {
		logFatalf("*chunkedPutContextStruct.performChunkedPut() call to http.NewRequest() failed: %v", err)
	}

	chunkedPutRequest.Header.Add("Transfer-Encoding", "chunked")

	chunkedPutContext.pos = 0

	_, _, ok, statusCode = doHTTPRequest(chunkedPutRequest, http.StatusOK, http.StatusCreated)
	if !ok {
		logFatalf("*chunkedPutContextStruct.performChunkedPut() failed with unexpected statusCode: %v", statusCode)
	}

	chunkedPutContext.Done()
}

func (chunkedPutContext *chunkedPutContextStruct) complete() {
	var (
		curExtentAsMultiObjectExtent  *multiObjectExtentStruct
		curExtentAsSingleObjectExtent *singleObjectExtentStruct
		curExtentAsValue              sortedmap.Value
		curExtentIndex                int
		err                           error
		extentMapLen                  int
		fileInode                     *fileInodeStruct
		ok                            bool
		wroteReply                    *jrpcfs.WroteReply
		wroteRequest                  *jrpcfs.WroteRequest
	)

	fileInode = chunkedPutContext.fileInode

	extentMapLen, err = chunkedPutContext.extentMap.Len()
	if nil != err {
		logFatalf("*chunkedPutContextStruct.complete() failed chunkedPutContext.extentMap.Len(): %v", err)
	}

	// Now that LogSegment Chunked PUT has completed, update FileInode in ProxyFS and our fileInode.extentMap

	wroteRequest = &jrpcfs.WroteRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(fileInode.InodeNumber),
		},
		ObjectPath:   "/v1/" + globals.config.SwiftAccountName + "/" + chunkedPutContext.containerName + "/" + chunkedPutContext.objectName,
		FileOffset:   make([]uint64, extentMapLen),
		ObjectOffset: make([]uint64, extentMapLen),
		Length:       make([]uint64, extentMapLen),
	}

	for curExtentIndex = 0; curExtentIndex < extentMapLen; curExtentIndex++ {
		_, curExtentAsValue, ok, err = chunkedPutContext.extentMap.GetByIndex(curExtentIndex)
		if nil != err {
			logFatalf("*chunkedPutContextStruct.complete() failed chunkedPutContext.extentMap.GetByIndex(): %v", err)
		}
		if !ok {
			logFatalf("*chunkedPutContextStruct.complete() chunkedPutContext.extentMap.GetByIndex() returned !ok")
		}
		curExtentAsSingleObjectExtent = curExtentAsValue.(*singleObjectExtentStruct)

		wroteRequest.FileOffset[curExtentIndex] = curExtentAsSingleObjectExtent.fileOffset
		wroteRequest.ObjectOffset[curExtentIndex] = curExtentAsSingleObjectExtent.objectOffset
		wroteRequest.Length[curExtentIndex] = curExtentAsSingleObjectExtent.length

		curExtentAsMultiObjectExtent = &multiObjectExtentStruct{
			fileOffset:    curExtentAsSingleObjectExtent.fileOffset,
			containerName: chunkedPutContext.containerName,
			objectName:    chunkedPutContext.objectName,
			objectOffset:  curExtentAsSingleObjectExtent.objectOffset,
			length:        curExtentAsSingleObjectExtent.length,
		}

		fileInode.updateExtentMap(curExtentAsMultiObjectExtent)
	}

	wroteReply = &jrpcfs.WroteReply{}

	err = globals.retryRPCClient.Send("RpcWrote", wroteRequest, wroteReply)
	if nil != err {
		logFatalf("*chunkedPutContextStruct.complete() failed Server.RpcWrote: %v", err)
	}

	// Remove this chunkedPutContext from fileInode.chunkedPutList and mark as Done()

	_ = fileInode.chunkedPutList.Remove(chunkedPutContext.chunkedPutListElement)

	fileInode.dereference()

	chunkedPutContext.fileInode.Done()
}

func (chunkedPutContext *chunkedPutContextStruct) Read(p []byte) (n int, err error) {
	var (
		grantedLock *fileInodeLockRequestStruct
	)

	chunkedPutContext.inRead = true
	defer func() {
		chunkedPutContext.inRead = false
	}()

	grantedLock = chunkedPutContext.fileInode.getExclusiveLock()

	n = len(chunkedPutContext.buf) - chunkedPutContext.pos

	if n < 0 {
		logFatalf("*chunkedPutContextStruct.Read() called with pos past beyond len(chunkedPutContext.buf)")
	}

	if n > 0 {
		// Return any unsent data in buf that will fit in p immediately

		if n > len(p) {
			n = len(p)
		}

		copy(p, chunkedPutContext.buf[chunkedPutContext.pos:chunkedPutContext.pos+n])

		chunkedPutContext.pos += n

		grantedLock.release()

		err = nil
		return
	}

	grantedLock.release()

	if chunkedPutContext.flushRequested {
		// Return io.EOF to indicate all chunks have been sent and to close this Chunked PUT

		err = io.EOF
		return
	}

	// At this point:
	//   There was no data in buf to send
	//   We need to wait for sendDaemon() to wake us up
	//   Then simply return (n == 0; err == nil) to cause us to be re-entered

	chunkedPutContext.flushRequested = <-chunkedPutContext.wakeChan

	err = nil
	return
}

func (chunkedPutContext *chunkedPutContextStruct) Close() (err error) {
	// Make sure Read() gets a chance to cleanly exit

	if chunkedPutContext.inRead {
		chunkedPutContext.wakeChan <- false

		for chunkedPutContext.inRead {
			time.Sleep(chunkedPutContextExitReadPollingRate)
		}
	}

	// To ensure retry resends all the data, reset pos

	chunkedPutContext.pos = 0

	err = nil
	return
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
			InodeNumber: int64(fileInode.InodeNumber),
		},
		FileOffset:                 fileOffset,
		MaxEntriesFromFileOffset:   int64(globals.config.FetchExtentsFromFileOffset),
		MaxEntriesBeforeFileOffset: int64(globals.config.FetchExtentsBeforeFileOffset),
	}

	fetchExtentMapChunkReply = &jrpcfs.FetchExtentMapChunkReply{}

	err = globals.retryRPCClient.Send("RpcFetchExtentMapChunk", fetchExtentMapChunkRequest, fetchExtentMapChunkReply)
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

			fileInode.updateExtentMap(curExtent)
		}

		// Insert the actual extent

		curExtent = &multiObjectExtentStruct{
			fileOffset:    extentMapEntry.FileOffset,
			containerName: extentMapEntry.ContainerName,
			objectName:    extentMapEntry.ObjectName,
			objectOffset:  extentMapEntry.LogSegmentOffset,
			length:        extentMapEntry.Length,
		}

		fileInode.updateExtentMap(curExtent)

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
			// Handle case where we have no extents left at all

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

// updateExtentMap is called to update the ExtentMap and, as necessary, the FileSize with
// the supplied multiObjectExtent. This func is used during fetching of ExtentMap chunks
// by *fileInodeStruct.populateExtentMapHelper() and at completion of a Chunked PUT
// by *chunkedPutContextStruct.complete().
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

	if nil == fileInode.extentMap {
		// Create an empty ExtentMap... This counts as a reference, too

		fileInode.reference()

		fileInode.extentMap = sortedmap.NewLLRBTree(sortedmap.CompareUint64, fileInode)
	}

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

// getReadPlan returns a slice of extents and their span (to aid in the make([]byte,) call needed
// by the caller to provision the slice into which they will copy the extents). Each extent will
// be one of three types:
//
//   singleObjectExtentWithLinkStruct - a reference to a portion of a LogSegment being written by a chunkedPutContextStruct
//   multiObjectExtentStruct          - a reference to a portion of a LogSegment described by a fileInodeStruct.extentMap
//   multiObjectExtentStruct          - a description of a zero-filled extent (.objectName == "")
//
func (fileInode *fileInodeStruct) getReadPlan(fileOffset uint64, length uint64) (readPlan []interface{}, readPlanSpan uint64) {
	var (
		chunkedPutContext          *chunkedPutContextStruct
		chunkedPutContextAsElement *list.Element
		curExtentAsValue           sortedmap.Value
		curExtentIndex             int
		curFileOffset              uint64
		curMultiObjectExtent       *multiObjectExtentStruct
		err                        error
		multiObjectReadPlanStep    *multiObjectExtentStruct
		ok                         bool
		remainingLength            uint64
	)

	// First assemble readPlan based upon fileInode.extentMap

	readPlan = make([]interface{}, 0, 1)

	curFileOffset = fileOffset
	remainingLength = length

	curExtentIndex, _, err = fileInode.extentMap.BisectLeft(fileOffset)
	if nil != err {
		logFatalf("getReadPlan() couldn't find curExtent: %v", err)
	}

	for remainingLength > 0 {
		_, curExtentAsValue, ok, err = fileInode.extentMap.GetByIndex(curExtentIndex)
		if nil != err {
			logFatalf("getReadPlan() couldn't find curExtent [Case 1]: %v", err)
		}

		if !ok {
			// Crossed EOF - stop here

			break
		}

		curMultiObjectExtent, ok = curExtentAsValue.(*multiObjectExtentStruct)
		if !ok {
			logFatalf("getReadPlan() couldn't find curExtent [Case 2]: %v", err)
		}

		if (curMultiObjectExtent.fileOffset + curMultiObjectExtent.length) <= curFileOffset {
			// curExtent ends at or before curFileOffset - stop here

			break
		}

		multiObjectReadPlanStep = &multiObjectExtentStruct{
			fileOffset:    curFileOffset,
			containerName: curMultiObjectExtent.containerName,
			objectName:    curMultiObjectExtent.objectName, // May be == ""
			objectOffset:  curMultiObjectExtent.objectOffset + (curFileOffset - curMultiObjectExtent.fileOffset),
			length:        curMultiObjectExtent.length - (curFileOffset - curMultiObjectExtent.fileOffset),
		}

		if remainingLength < multiObjectReadPlanStep.length {
			// This is the last readPlanStep and needs to be truncated

			multiObjectReadPlanStep.length = remainingLength
		}

		if 0 == multiObjectReadPlanStep.length {
			// Reached EOF - stop here

			break
		}

		readPlan = append(readPlan, multiObjectReadPlanStep)

		curFileOffset += multiObjectReadPlanStep.length
		remainingLength -= multiObjectReadPlanStep.length

		curExtentIndex++
	}

	// Compute tentative readPlanSpan

	if 0 == len(readPlan) {
		readPlanSpan = 0
	} else {
		multiObjectReadPlanStep = readPlan[len(readPlan)-1].(*multiObjectExtentStruct)
		readPlanSpan = (multiObjectReadPlanStep.fileOffset + multiObjectReadPlanStep.length) - fileOffset
	}

	// But we must apply, in order, any changes due to chunkedPutContextStruct's

	chunkedPutContextAsElement = fileInode.chunkedPutList.Front()
	for nil != chunkedPutContextAsElement {
		chunkedPutContext = chunkedPutContextAsElement.Value.(*chunkedPutContextStruct)
		readPlan, readPlanSpan = chunkedPutContext.getReadPlanHelper(fileOffset, length, readPlan)
		chunkedPutContextAsElement = chunkedPutContextAsElement.Next()
	}

	// And we are done...

	return
}

func (chunkedPutContext *chunkedPutContextStruct) getReadPlanHelper(fileOffset uint64, length uint64, inReadPlan []interface{}) (outReadPlan []interface{}, outReadPlanSpan uint64) {
	var (
		curFileOffset                       uint64
		err                                 error
		found                               bool
		inReadPlanStepAsInterface           interface{}
		inReadPlanStepAsMultiObjectExtent   *multiObjectExtentStruct
		inReadPlanStepAsSingleObjectExtent  *singleObjectExtentWithLinkStruct
		inReadPlanStepFileOffset            uint64
		inReadPlanStepLength                uint64
		ok                                  bool
		outReadPlanStepAsInterface          interface{}
		outReadPlanStepAsMultiObjectExtent  *multiObjectExtentStruct
		outReadPlanStepAsSingleObjectExtent *singleObjectExtentWithLinkStruct
		overlapExtent                       *singleObjectExtentStruct
		overlapExtentAsValue                sortedmap.Value
		overlapExtentWithLink               *singleObjectExtentWithLinkStruct
		overlapExtentIndex                  int
		postExtent                          *singleObjectExtentStruct
		postExtentAsValue                   sortedmap.Value
		postExtentIndex                     int
		postOverlapLength                   uint64
		preExtent                           *singleObjectExtentStruct
		preExtentAsValue                    sortedmap.Value
		preExtentIndex                      int
		preOverlapLength                    uint64
		remainingLength                     uint64
		wasMultiObjectReadPlanStep          bool
	)

	outReadPlan = make([]interface{}, 0, len(inReadPlan))

	for _, inReadPlanStepAsInterface = range inReadPlan {
		// Compute overlap with chunkedPutContext.extentMap

		inReadPlanStepAsMultiObjectExtent, wasMultiObjectReadPlanStep = inReadPlanStepAsInterface.(*multiObjectExtentStruct)
		if wasMultiObjectReadPlanStep {
			inReadPlanStepAsSingleObjectExtent = nil
			inReadPlanStepFileOffset = inReadPlanStepAsMultiObjectExtent.fileOffset
			inReadPlanStepLength = inReadPlanStepAsMultiObjectExtent.length
		} else {
			inReadPlanStepAsMultiObjectExtent = nil
			inReadPlanStepAsSingleObjectExtent = inReadPlanStepAsInterface.(*singleObjectExtentWithLinkStruct)
			inReadPlanStepFileOffset = inReadPlanStepAsSingleObjectExtent.fileOffset
			inReadPlanStepLength = inReadPlanStepAsSingleObjectExtent.length
		}

		preExtentIndex, found, err = chunkedPutContext.extentMap.BisectLeft(inReadPlanStepFileOffset)
		if nil != err {
			logFatalf("getReadPlanHelper() couldn't find preExtentIndex: %v", err)
		}
		if found {
			// Back up preExtentIndex... we know previous extent (if any) doesn't overlap
			preExtentIndex--
		} else {
			// But preExtentIndex might point to extent overlapping
			if 0 <= preExtentIndex {
				_, preExtentAsValue, _, err = chunkedPutContext.extentMap.GetByIndex(preExtentIndex)
				if nil != err {
					logFatalf("getReadPlanHelper() couldn't fetch preExtent: %v", err)
				}
				preExtent = preExtentAsValue.(*singleObjectExtentStruct)
				if (preExtent.fileOffset + preExtent.length) > inReadPlanStepFileOffset {
					preExtentIndex--
				}
			}
		}
		postExtentIndex, _, err = chunkedPutContext.extentMap.BisectRight(inReadPlanStepFileOffset + inReadPlanStepLength)
		if nil != err {
			logFatalf("getReadPlanHelper() couldn't find postExtentIndex [Case 1]: %v", err)
		}

		if 1 == (postExtentIndex - preExtentIndex) {
			// No overlap... replicate inReadPlanStep as is

			outReadPlan = append(outReadPlan, inReadPlanStepAsInterface)

			continue
		}

		// Apply overlapping extents from chunkedPutContext.extentMap with inReadPlanStep

		curFileOffset = inReadPlanStepFileOffset
		remainingLength = inReadPlanStepLength

		for overlapExtentIndex = preExtentIndex + 1; overlapExtentIndex < postExtentIndex; overlapExtentIndex++ {
			_, overlapExtentAsValue, _, err = chunkedPutContext.extentMap.GetByIndex(overlapExtentIndex)
			if nil != err {
				logFatalf("getReadPlanHelper() couldn't find overlapExtentIndex: %v", err)
			}
			overlapExtent = overlapExtentAsValue.(*singleObjectExtentStruct)

			if overlapExtent.fileOffset < curFileOffset {
				preOverlapLength = curFileOffset - overlapExtent.fileOffset
			} else {
				preOverlapLength = 0
			}
			if (overlapExtent.fileOffset + overlapExtent.length) > (curFileOffset + remainingLength) {
				postOverlapLength = (overlapExtent.fileOffset + overlapExtent.length) - (curFileOffset + remainingLength)
			} else {
				postOverlapLength = 0
			}

			overlapExtentWithLink = &singleObjectExtentWithLinkStruct{
				fileOffset:        overlapExtent.fileOffset + preOverlapLength,
				objectOffset:      overlapExtent.objectOffset + preOverlapLength,
				length:            overlapExtent.length - (preOverlapLength + postOverlapLength),
				chunkedPutContext: chunkedPutContext,
			}

			if curFileOffset < overlapExtentWithLink.fileOffset {
				// Append non-overlapped portion of inReadPlanStep preceeding overlapExtentWithLink

				if wasMultiObjectReadPlanStep {
					outReadPlanStepAsMultiObjectExtent = &multiObjectExtentStruct{
						fileOffset:    curFileOffset,
						containerName: inReadPlanStepAsMultiObjectExtent.containerName,
						objectName:    inReadPlanStepAsMultiObjectExtent.objectName,
						objectOffset:  inReadPlanStepAsMultiObjectExtent.objectOffset + (curFileOffset - inReadPlanStepAsMultiObjectExtent.fileOffset),
						length:        overlapExtentWithLink.fileOffset - curFileOffset,
					}

					outReadPlan = append(outReadPlan, outReadPlanStepAsMultiObjectExtent)

					curFileOffset += outReadPlanStepAsMultiObjectExtent.length
					remainingLength -= outReadPlanStepAsMultiObjectExtent.length
				} else {
					outReadPlanStepAsSingleObjectExtent = &singleObjectExtentWithLinkStruct{
						fileOffset:        curFileOffset,
						objectOffset:      inReadPlanStepAsSingleObjectExtent.objectOffset + (curFileOffset - inReadPlanStepAsSingleObjectExtent.fileOffset),
						length:            overlapExtentWithLink.fileOffset - curFileOffset,
						chunkedPutContext: inReadPlanStepAsSingleObjectExtent.chunkedPutContext,
					}

					outReadPlan = append(outReadPlan, outReadPlanStepAsSingleObjectExtent)

					curFileOffset += outReadPlanStepAsSingleObjectExtent.length
					remainingLength -= outReadPlanStepAsSingleObjectExtent.length
				}
			}

			// Append overlapExtentWithLink

			outReadPlan = append(outReadPlan, overlapExtentWithLink)

			curFileOffset += overlapExtentWithLink.length
			remainingLength -= overlapExtentWithLink.length
		}

		if 0 < remainingLength {
			// Append non-overlapped trailing portion of inReadPlanStep in outReadPlan

			if wasMultiObjectReadPlanStep {
				outReadPlanStepAsMultiObjectExtent = &multiObjectExtentStruct{
					fileOffset:    curFileOffset,
					containerName: inReadPlanStepAsMultiObjectExtent.containerName,
					objectName:    inReadPlanStepAsMultiObjectExtent.objectName,
					objectOffset:  inReadPlanStepAsMultiObjectExtent.objectOffset + (curFileOffset - inReadPlanStepAsMultiObjectExtent.fileOffset),
					length:        remainingLength,
				}

				outReadPlan = append(outReadPlan, outReadPlanStepAsMultiObjectExtent)
			} else {
				outReadPlanStepAsSingleObjectExtent = &singleObjectExtentWithLinkStruct{
					fileOffset:        curFileOffset,
					objectOffset:      inReadPlanStepAsSingleObjectExtent.objectOffset + (curFileOffset - inReadPlanStepAsSingleObjectExtent.fileOffset),
					length:            remainingLength,
					chunkedPutContext: inReadPlanStepAsSingleObjectExtent.chunkedPutContext,
				}

				outReadPlan = append(outReadPlan, outReadPlanStepAsSingleObjectExtent)
			}
		}
	}

	// Compute tentative outReadPlanSpan

	if 0 == len(outReadPlan) {
		outReadPlanSpan = 0
	} else {
		outReadPlanStepAsInterface = outReadPlan[len(outReadPlan)-1]
		outReadPlanStepAsMultiObjectExtent, wasMultiObjectReadPlanStep = outReadPlanStepAsInterface.(*multiObjectExtentStruct)
		if wasMultiObjectReadPlanStep {
			outReadPlanSpan = (outReadPlanStepAsMultiObjectExtent.fileOffset + outReadPlanStepAsMultiObjectExtent.length) - fileOffset
		} else {
			outReadPlanStepAsSingleObjectExtent = outReadPlanStepAsInterface.(*singleObjectExtentWithLinkStruct)
			outReadPlanSpan = (outReadPlanStepAsSingleObjectExtent.fileOffset + outReadPlanStepAsSingleObjectExtent.length) - fileOffset
		}
	}

	if outReadPlanSpan == length {
		return
	}

	// inReadPlan was limited by incoming fileSize... can we extend it?

	curFileOffset = fileOffset + outReadPlanSpan

	postExtentIndex, found, err = chunkedPutContext.extentMap.BisectLeft(curFileOffset)
	if nil != err {
		logFatalf("getReadPlanHelper() couldn't find postExtentIndex [Case 2]: %v", err)
	}
	if found {
		// We know this extent, if it exists, does not overlap

		_, postExtentAsValue, ok, err = chunkedPutContext.extentMap.GetByIndex(postExtentIndex)
		if nil != err {
			logFatalf("getReadPlanHelper() couldn't find postExtent [Case 1]: %v", err)
		}
		if !ok {
			return
		}

		postExtent = postExtentAsValue.(*singleObjectExtentStruct)
	} else {
		// So this extent, if it exists, must overlap... and possibly extend beyond

		_, postExtentAsValue, ok, err = chunkedPutContext.extentMap.GetByIndex(postExtentIndex)
		if nil != err {
			logFatalf("getReadPlanHelper() couldn't find postExtent [Case 2]: %v", err)
		}

		if ok {
			overlapExtent = postExtentAsValue.(*singleObjectExtentStruct)

			if (overlapExtent.fileOffset + overlapExtent.length) > curFileOffset {
				// Create a postExtent equivalent to the non-overlapping tail of overlapExtent

				postExtent = &singleObjectExtentStruct{
					fileOffset:   curFileOffset,
					objectOffset: overlapExtent.objectOffset + (curFileOffset - overlapExtent.fileOffset),
					length:       overlapExtent.length - (curFileOffset - overlapExtent.fileOffset),
				}
			} else {
				// Create a zero-length postExtent instead

				postExtent = &singleObjectExtentStruct{
					fileOffset:   curFileOffset,
					objectOffset: 0,
					length:       0,
				}
			}
		} else {
			// Create a zero-length postExtent instead

			postExtent = &singleObjectExtentStruct{
				fileOffset:   curFileOffset,
				objectOffset: 0,
				length:       0,
			}
		}
	}

	// Now enter a loop until either outReadPlanSpan reaches length or we reach chunkedPutContext.fileSize
	// Each loop iteration, postExtent either starts at or after curFileSize (requiring zero-fill)
	// Note that the last chunkedPutContext.extentMap extent "ends" at chunkedPutContext.fileSize

	for {
		if 0 < postExtent.length {
			if postExtent.fileOffset > curFileOffset {
				// We must "zero-fill" to MIN(postExtent.fileOffset, chunkedPutContext.fileSize)

				if postExtent.fileOffset >= (fileOffset + length) {
					// postExtent starts beyond fileOffset+length, so just append zero-fill step & return
					outReadPlanStepAsSingleObjectExtent = &singleObjectExtentWithLinkStruct{
						fileOffset:        curFileOffset,
						objectOffset:      0,
						length:            length - outReadPlanSpan,
						chunkedPutContext: nil,
					}

					outReadPlan = append(outReadPlan, outReadPlanStepAsSingleObjectExtent)
					outReadPlanSpan = length

					return
				}

				// postExtent starts after curFileOffset but before fileOffset+length, so insert zero-fill step first

				outReadPlanStepAsSingleObjectExtent = &singleObjectExtentWithLinkStruct{
					fileOffset:        curFileOffset,
					objectOffset:      0,
					length:            postExtent.fileOffset - curFileOffset,
					chunkedPutContext: nil,
				}

				outReadPlan = append(outReadPlan, outReadPlanStepAsSingleObjectExtent)

				curFileOffset += outReadPlanStepAsSingleObjectExtent.length
				outReadPlanSpan += outReadPlanStepAsSingleObjectExtent.length
			}

			// Now append a step for some or all of postExtent

			if (postExtent.fileOffset + postExtent.length) >= (fileOffset + length) {
				// postExtent will take us to (and beyond) fileOffset+length, so insert proper portion & return

				outReadPlanStepAsSingleObjectExtent = &singleObjectExtentWithLinkStruct{
					fileOffset:        postExtent.fileOffset,
					objectOffset:      postExtent.objectOffset,
					length:            (fileOffset + length) - postExtent.fileOffset,
					chunkedPutContext: chunkedPutContext,
				}

				outReadPlan = append(outReadPlan, outReadPlanStepAsSingleObjectExtent)
				outReadPlanSpan = length

				return
			}

			// The entire postExtent will "fit"... and not exhaust fileOffset+length

			outReadPlanStepAsSingleObjectExtent = &singleObjectExtentWithLinkStruct{
				fileOffset:        postExtent.fileOffset,
				objectOffset:      postExtent.objectOffset,
				length:            postExtent.length,
				chunkedPutContext: chunkedPutContext,
			}

			outReadPlan = append(outReadPlan, outReadPlanStepAsSingleObjectExtent)

			curFileOffset += outReadPlanStepAsSingleObjectExtent.length
			outReadPlanSpan += outReadPlanStepAsSingleObjectExtent.length
		}

		// Index to next postExtent

		postExtentIndex++

		_, postExtentAsValue, ok, err = chunkedPutContext.extentMap.GetByIndex(postExtentIndex)
		if nil != err {
			logFatalf("getReadPlanHelper() couldn't find postExtent [Case 3]: %v", err)
		}
		if !ok {
			return
		}

		postExtent = postExtentAsValue.(*singleObjectExtentStruct)
	}
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
		// Found it... so indicate cache hit & move it to MRU end of LRU

		_ = atomic.AddUint64(&globals.metrics.ReadCacheHits, 1)

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

	// Cache miss

	_ = atomic.AddUint64(&globals.metrics.ReadCacheMisses, 1)

	// Make room for new LogSegment Cache Line if necessary

	for uint64(globals.logSegmentCacheLRU.Len()) >= globals.config.ReadCacheLineCount {
		logSegmentCacheElementToEvictLRUElement = globals.logSegmentCacheLRU.Front()
		logSegmentCacheElementToEvict = logSegmentCacheElementToEvictLRUElement.Value.(*logSegmentCacheElementStruct)
		logSegmentCacheElementToEvictKey.logSegmentNumber, err = strconv.ParseUint(logSegmentCacheElementToEvict.objectName, 16, 64)
		logSegmentCacheElementToEvictKey.cacheLineTag = logSegmentCacheElementToEvict.startingOffset / globals.config.ReadCacheLineSize
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

	url = globals.swiftAccountBypassURL + "/" + containerName + "/" + objectName

	logSegmentStart = logSegmentCacheElementKey.cacheLineTag * globals.config.ReadCacheLineSize
	logSegmentEnd = logSegmentStart + globals.config.ReadCacheLineSize - 1

	getRequest, err = http.NewRequest(http.MethodGet, url, nil)
	if nil != err {
		logFatalf("unable to create GET http.Request (,%s,): %v", url)
	}

	getRequest.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", logSegmentStart, logSegmentEnd))

	_, logSegmentCacheElement.buf, ok, _ = doHTTPRequest(getRequest, http.StatusOK, http.StatusPartialContent)

	globals.Lock()

	if ok {
		logSegmentCacheElement.state = logSegmentCacheElementStateGetSuccessful
	} else {
		logSegmentCacheElement.state = logSegmentCacheElementStateGetFailed

		// Remove it from the LogSegment Cache as well

		delete(globals.logSegmentCacheMap, logSegmentCacheElementKey)
		globals.logSegmentCacheLRU.Remove(logSegmentCacheElement.cacheLRUElement)

		// While the above accounting has correctly reported the cache fetch failure,
		// we cannot continue from this point and must abruptly fail

		logFatalf("unable to complete GET http.Request (,%s,)", url)
	}

	globals.Unlock()

	// Signal any (other) waiters GET completed (either successfully or not) before returning

	logSegmentCacheElement.Done()

	return
}

func (chunkedPutContext *chunkedPutContextStruct) mergeSingleObjectExtent(newExtent *singleObjectExtentStruct) {
	var (
		curExtent        *singleObjectExtentStruct
		curExtentAsValue sortedmap.Value
		curExtentIndex   int
		err              error
		extentMapLen     int
		found            bool
		ok               bool
		postLength       uint64
		preLength        uint64
		splitExtent      *singleObjectExtentStruct
	)

	// See if we can simply extend last element

	extentMapLen, err = chunkedPutContext.extentMap.Len()
	if nil != err {
		logFatalf("mergeSingleObjectExtent() failed to Len(): %v", err)
	}

	if 0 < extentMapLen {
		_, curExtentAsValue, _, err = chunkedPutContext.extentMap.GetByIndex(extentMapLen - 1)
		if nil != err {
			logFatalf("mergeSingleObjectExtent() failed to GetByIndex() [Case 1]: %v", err)
		}
		curExtent = curExtentAsValue.(*singleObjectExtentStruct)

		if (curExtent.fileOffset + curExtent.length) == newExtent.fileOffset {
			if (curExtent.objectOffset + curExtent.length) == newExtent.objectOffset {
				// Simply extend curExtent (coalescing newExtent into it)

				curExtent.length += newExtent.length

				return
			}
		}
	}

	// See if newExtent collides with a first curExtent

	curExtentIndex, found, err = chunkedPutContext.extentMap.BisectLeft(newExtent.fileOffset)
	if nil != err {
		logFatalf("mergeSingleObjectExtent() failed to BisectLeft(): %v", err)
	}

	if found {
		// curExtent exists and starts precisely at newExtent.fileOffset... fully overlapped by newExtent?

		_, curExtentAsValue, _, err = chunkedPutContext.extentMap.GetByIndex(curExtentIndex)
		if nil != err {
			logFatalf("mergeSingleObjectExtent() failed to GetByIndex() [Case 2]: %v", err)
		}
		curExtent = curExtentAsValue.(*singleObjectExtentStruct)

		if (curExtent.fileOffset + curExtent.length) <= (newExtent.fileOffset + newExtent.length) {
			// curExtent fully overlapped by newExtent... just drop it

			_, err = chunkedPutContext.extentMap.DeleteByIndex(curExtentIndex)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to DeleteByIndex() [Case 1]: %v", err)
			}

			// curExtentIndex left pointing to subsequent extent (if any) for loop below
		} else {
			// curExtent is overlapped "on the left" by newExtent... so tuncate and move curExtent

			postLength = (curExtent.fileOffset + curExtent.length) - (newExtent.fileOffset + newExtent.length)

			_, err = chunkedPutContext.extentMap.DeleteByIndex(curExtentIndex)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to DeleteByIndex() curExtent [Case 2]: %v", err)
			}

			splitExtent = &singleObjectExtentStruct{
				fileOffset:   newExtent.fileOffset + newExtent.length,
				objectOffset: curExtent.objectOffset + preLength + newExtent.length,
				length:       postLength,
			}

			_, err = chunkedPutContext.extentMap.Put(splitExtent.fileOffset, splitExtent)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to Put() splitExtent [Case 1]: %v", err)
			}

			// From here, we know we can just insert newExtent and we are done

			_, err = chunkedPutContext.extentMap.Put(newExtent.fileOffset, newExtent)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to Put() newExtent [Case 1]: %v", err)
			}

			return
		}
	} else { // !found
		if 0 > curExtentIndex {
			// curExtent does not exist (so cannot overlap)... so set curExtentIndex to point to first extent (if any) for loop below

			curExtentIndex = 0
		} else { // 0 <= curExtentIndex
			// curExtent exists and starts strictly before newExtent.fileOffset... any overlap with newExtent?

			_, curExtentAsValue, _, err = chunkedPutContext.extentMap.GetByIndex(curExtentIndex)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to GetByIndex() [Case 3]: %v", err)
			}
			curExtent = curExtentAsValue.(*singleObjectExtentStruct)

			if (curExtent.fileOffset + curExtent.length) > newExtent.fileOffset {
				// curExtent definitely collides with newExtent... can we just truncate or do we need to split

				preLength = newExtent.fileOffset - curExtent.fileOffset

				if (curExtent.fileOffset + curExtent.length) <= (newExtent.fileOffset + newExtent.length) {
					// curExtent ends at or before newExtent ends... so simply truncate curExtent

					curExtent.length = preLength

					// Set curExtentIndex to point to following extent (if any) for loop below

					curExtentIndex++
				} else {
					// curExtent is overlapped "in the middle" by newExtent... so split curExtent "around" newExtent

					postLength = (curExtent.fileOffset + curExtent.length) - (newExtent.fileOffset + newExtent.length)

					curExtent.length = preLength

					splitExtent = &singleObjectExtentStruct{
						fileOffset:   newExtent.fileOffset + newExtent.length,
						objectOffset: curExtent.objectOffset + preLength + newExtent.length,
						length:       postLength,
					}

					_, err = chunkedPutContext.extentMap.Put(splitExtent.fileOffset, splitExtent)
					if nil != err {
						logFatalf("mergeSingleObjectExtent() failed to Put() splitExtent [Case 2]: %v", err)
					}

					// From here, we know we can just insert newExtent and we are done

					_, err = chunkedPutContext.extentMap.Put(newExtent.fileOffset, newExtent)
					if nil != err {
						logFatalf("mergeSingleObjectExtent() failed to Put() newExtent [Case 2]: %v", err)
					}

					return
				}
			} else {
				// curExtent does not overlap newExtent... set curExtentIndex to point to following extent (in any) for loop below

				curExtentIndex++
			}
		}
	}

	// At this point, the special case of the first extent starting at or before newExtent has been
	// cleared from overlapping with newExtent... so now we have to loop from curExtentIndex looking
	// for additional extents to either delete entirely or truncate "on the left" in order to ensure
	// that no other extents overlap with newExtent

	for {
		_, curExtentAsValue, ok, err = chunkedPutContext.extentMap.GetByIndex(curExtentIndex)
		if nil != err {
			logFatalf("mergeSingleObjectExtent() failed to GetByIndex() [Case 4]: %v", err)
		}

		if !ok {
			// No more extents, so we know we are done removing overlapped extents

			break
		}

		curExtent = curExtentAsValue.(*singleObjectExtentStruct)

		if curExtent.fileOffset >= (newExtent.fileOffset + newExtent.length) {
			// This and all subsequent extents are "beyond" newExtent so cannot overlap

			break
		}

		if (curExtent.fileOffset + curExtent.length) <= (newExtent.fileOffset + newExtent.length) {
			// curExtent completely overlapped by newExtent... so simply delete it

			_, err = chunkedPutContext.extentMap.DeleteByIndex(curExtentIndex)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to DeleteByIndex() [Case 3]: %v", err)
			}

			// curExtentIndex left pointing to subsequent extent (if any) for next loop iteration
		} else {
			// curExtent is overlapped "on the left" by newExtent... so truncate and move curExtent

			postLength = (curExtent.fileOffset + curExtent.length) - (newExtent.fileOffset + newExtent.length)

			_, err = chunkedPutContext.extentMap.DeleteByIndex(curExtentIndex)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to DeleteByIndex() curExtent [Case 4]: %v", err)
			}

			splitExtent = &singleObjectExtentStruct{
				fileOffset:   newExtent.fileOffset + newExtent.length,
				objectOffset: curExtent.objectOffset + (curExtent.length - postLength),
				length:       postLength,
			}

			_, err = chunkedPutContext.extentMap.Put(splitExtent.fileOffset, splitExtent)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to Put() splitExtent [Case 3]: %v", err)
			}

			// From here, we know we can just insert newExtent and we are done

			_, err = chunkedPutContext.extentMap.Put(newExtent.fileOffset, newExtent)
			if nil != err {
				logFatalf("mergeSingleObjectExtent() failed to Put() newExtent [Case 3]: %v", err)
			}

			return
		}
	}

	// Having ensured there are no overlapping extents, it is safe to insert newExtent

	_, err = chunkedPutContext.extentMap.Put(newExtent.fileOffset, newExtent)
	if nil != err {
		logFatalf("mergeSingleObjectExtent() failed to Put() newExtent [Case 4]: %v", err)
	}
}

func pruneExtentMap(extentMap sortedmap.LLRBTree, newSize uint64) {
	var (
		err                                error
		extentAsMultiObjectExtent          *multiObjectExtentStruct
		extentAsSingleObjectExtent         *singleObjectExtentStruct
		extentAsSingleObjectExtentWithLink *singleObjectExtentWithLinkStruct
		extentAsValue                      sortedmap.Value
		index                              int
		ok                                 bool
	)

	// Nothing to do if extentMap hasn't been populated yet

	if nil == extentMap {
		return
	}

	// First, destroy any extents starting at or beyond newSize

	index, _, err = extentMap.BisectRight(newSize)
	if nil != err {
		logFatalf("extentMap.BisectLeft() failed: %v", err)
	}

	ok = true
	for ok {
		ok, err = extentMap.DeleteByIndex(index)
		if nil != err {
			logFatalf("extentMap.DeleteByIndex(index+1) failed: %v", err)
		}
	}

	// Next, back up and look at (new) tailing extent

	index--

	if 0 > index {
		// No extents remain in extentmap, so just return empty extentMap

		return
	}

	// See if trailing extent map entries need to be truncated

	_, extentAsValue, _, err = extentMap.GetByIndex(index)
	if nil != err {
		logFatalf("extentMap.GetByIndex(index) failed: %v", err)
	}

	extentAsMultiObjectExtent, ok = extentAsValue.(*multiObjectExtentStruct)
	if ok {
		if (extentAsMultiObjectExtent.fileOffset + extentAsMultiObjectExtent.length) > newSize {
			extentAsMultiObjectExtent.length = newSize - extentAsMultiObjectExtent.fileOffset
		}
	} else {
		extentAsSingleObjectExtent, ok = extentAsValue.(*singleObjectExtentStruct)
		if ok {
			if (extentAsSingleObjectExtent.fileOffset + extentAsSingleObjectExtent.length) > newSize {
				extentAsSingleObjectExtent.length = newSize - extentAsSingleObjectExtent.fileOffset
			}
		} else {
			extentAsSingleObjectExtentWithLink, ok = extentAsValue.(*singleObjectExtentWithLinkStruct)
			if ok {
				if (extentAsSingleObjectExtentWithLink.fileOffset + extentAsSingleObjectExtentWithLink.length) > newSize {
					extentAsSingleObjectExtentWithLink.length = newSize - extentAsSingleObjectExtentWithLink.fileOffset
				}
			} else {
				logFatalf("extentAsValue.(*{multi|single|single}ObjectExtent{||WithLink}Struct) returned !ok")
			}
		}
	}

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

func dumpExtentMap(extentMap sortedmap.LLRBTree) {
	var (
		err                                error
		extentAsMultiObjectExtent          *multiObjectExtentStruct
		extentAsSingleObjectExtent         *singleObjectExtentStruct
		extentAsSingleObjectExtentWithLink *singleObjectExtentWithLinkStruct
		extentAsValue                      sortedmap.Value
		extentIndex                        int
		extentMapLen                       int
		ok                                 bool
	)

	extentMapLen, err = extentMap.Len()
	if nil != err {
		logFatalf("dumpExtentMap() doing extentMap.Len() failed: %v", err)
	}

	for extentIndex = 0; extentIndex < extentMapLen; extentIndex++ {
		_, extentAsValue, ok, err = extentMap.GetByIndex(extentIndex)
		if nil != err {
			logFatalf("dumpExtentMap() doing extentMap.GetByIndex() failed: %v", err)
		}
		if !ok {
			logFatalf("dumpExtentMap() doing extentMap.GetByIndex() returned !ok")
		}

		extentAsMultiObjectExtent, ok = extentAsValue.(*multiObjectExtentStruct)
		if ok {
			logInfof("             MultiObjectExtent: %+v", extentAsMultiObjectExtent)
		} else {
			extentAsSingleObjectExtent, ok = extentAsValue.(*singleObjectExtentStruct)
			if ok {
				logInfof("            SingleObjectExtent: %+v", extentAsSingleObjectExtent)
			} else {
				extentAsSingleObjectExtentWithLink, ok = extentAsValue.(*singleObjectExtentWithLinkStruct)
				if ok {
					logInfof("    SingleObjectExtentWithLink: %+v [containerName:%s objectName:%s]", extentAsSingleObjectExtentWithLink, extentAsSingleObjectExtentWithLink.chunkedPutContext.containerName, extentAsSingleObjectExtentWithLink.chunkedPutContext.objectName)
				} else {
					logFatalf("dumpExtentMap() doing extentAsValue.(*{multi|single|single}ObjectExtent{||WithLink}Struct) returned !ok")
				}
			}
		}
	}
}

func (fileInode *fileInodeStruct) dumpExtentMaps() {
	var (
		chunkedPutContext        *chunkedPutContextStruct
		chunkedPutContextElement *list.Element
		chunkedPutContextIndex   uint64
		ok                       bool
	)

	logInfof("FileInode @%p ExtentMap:", fileInode)

	dumpExtentMap(fileInode.extentMap)

	chunkedPutContextIndex = 0
	chunkedPutContextElement = fileInode.chunkedPutList.Front()

	for nil != chunkedPutContextElement {
		chunkedPutContext, ok = chunkedPutContextElement.Value.(*chunkedPutContextStruct)
		if !ok {
			logFatalf("dumpExtentMaps() doing chunkedPutContextElement.Value.(*chunkedPutContextStruct) returned !ok")
		}

		logInfof("  ChunkedPutContext #%v @%p ExtentMap [containerName:%s objectName:%s]:", chunkedPutContextIndex, chunkedPutContext, chunkedPutContext.containerName, chunkedPutContext.objectName)

		dumpExtentMap(chunkedPutContext.extentMap)

		chunkedPutContextIndex++
		chunkedPutContextElement = chunkedPutContextElement.Next()
	}
}
