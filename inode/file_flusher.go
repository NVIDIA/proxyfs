package inode

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

func openLogSegmentLRUInsertWhileLocked(inFlightLogSegment *inFlightLogSegmentStruct) {
	// Place inode at the MRU end of openLogSegmentLRU

	ptr, ok := globals.openLogSegmentCache[inFlightLogSegment.fileInode.InodeNumber]
	if ok {
		panic(fmt.Sprintf("log segment for inode %d is already present in map at %p",
			inFlightLogSegment.fileInode.InodeNumber, ptr))
	}
	globals.openLogSegmentCache[inFlightLogSegment.fileInode.InodeNumber] = inFlightLogSegment

	if 0 == globals.openLogSegmentLRUItems {
		globals.openLogSegmentLRUHead = inFlightLogSegment
		globals.openLogSegmentLRUTail = inFlightLogSegment
		globals.openLogSegmentLRUItems = 1
	} else {
		inFlightLogSegment.openLogSegmentLRUPrev = globals.openLogSegmentLRUTail
		inFlightLogSegment.openLogSegmentLRUPrev.openLogSegmentLRUNext = inFlightLogSegment

		globals.openLogSegmentLRUTail = inFlightLogSegment
		globals.openLogSegmentLRUItems++
	}
}

func openLogSegmentLRUInsert(inFlightLogSegment *inFlightLogSegmentStruct) {
	globals.Lock()
	openLogSegmentLRUInsertWhileLocked(inFlightLogSegment)
	globals.Unlock()
}

func openLogSegmentLRURemoveWhileLocked(inFlightLogSegment *inFlightLogSegmentStruct) {
	_, ok := globals.openLogSegmentCache[inFlightLogSegment.fileInode.InodeNumber]
	if !ok {
		panic(fmt.Sprintf("log segment for inode %d not present in map",
			inFlightLogSegment.fileInode.InodeNumber))
	}
	delete(globals.openLogSegmentCache, inFlightLogSegment.fileInode.InodeNumber)

	if inFlightLogSegment == globals.openLogSegmentLRUHead {
		if inFlightLogSegment == globals.openLogSegmentLRUTail {
			globals.openLogSegmentLRUHead = nil
			globals.openLogSegmentLRUTail = nil
			globals.openLogSegmentLRUItems = 0
		} else {
			globals.openLogSegmentLRUHead = inFlightLogSegment.openLogSegmentLRUNext
			globals.openLogSegmentLRUHead.openLogSegmentLRUPrev = nil
			globals.openLogSegmentLRUItems--

			inFlightLogSegment.openLogSegmentLRUNext = nil
		}
	} else {
		if inFlightLogSegment == globals.openLogSegmentLRUTail {
			globals.openLogSegmentLRUTail = inFlightLogSegment.openLogSegmentLRUPrev
			globals.openLogSegmentLRUTail.openLogSegmentLRUNext = nil
			globals.openLogSegmentLRUItems--

			inFlightLogSegment.openLogSegmentLRUPrev = nil
		} else {
			inFlightLogSegment.openLogSegmentLRUPrev.openLogSegmentLRUNext = inFlightLogSegment.openLogSegmentLRUNext
			inFlightLogSegment.openLogSegmentLRUNext.openLogSegmentLRUPrev = inFlightLogSegment.openLogSegmentLRUPrev
			globals.openLogSegmentLRUItems--

			inFlightLogSegment.openLogSegmentLRUNext = nil
			inFlightLogSegment.openLogSegmentLRUPrev = nil
		}
	}
}

func openLogSegmentLRURemove(inFlightLogSegment *inFlightLogSegmentStruct) {
	globals.Lock()
	openLogSegmentLRURemoveWhileLocked(inFlightLogSegment)
	globals.Unlock()
}

// See if there's an open log segment for this inode number.  If there is remove
// it from the map and return it.  The caller must either Close() it or put it
// back in the cache.
//
func openLogSegmentLRUGet(inodeNumber InodeNumber) (inFlightLogSegment *inFlightLogSegmentStruct) {
	var ok bool

	globals.Lock()
	inFlightLogSegment, ok = globals.openLogSegmentCache[inodeNumber]
	if ok {
		openLogSegmentLRURemoveWhileLocked(inFlightLogSegment)
	}
	globals.Unlock()

	return
}

// Remove the oldest open log segment from the LRU, if any, and return it.
//
func openLogSegmentLRUGetOldest() (inFlightLogSegment *inFlightLogSegmentStruct) {

	globals.Lock()
	defer globals.Unlock()

	inFlightLogSegment = globals.openLogSegmentLRUHead
	if inFlightLogSegment == nil {
		return
	}

	openLogSegmentLRURemoveWhileLocked(inFlightLogSegment)
	return
}

func (flowControl *flowControlStruct) capReadCacheWhileLocked() {
	for uint64(len(flowControl.readCache)) > flowControl.readCacheLineCount {
		delete(flowControl.readCache, flowControl.readCacheLRU.readCacheKey)
		flowControl.readCacheLRU = flowControl.readCacheLRU.prev
		flowControl.readCacheLRU.next = nil
	}
}

func (flowControl *flowControlStruct) insertReadCacheElementWhileLocked(readCacheElement *readCacheElementStruct) {
	flowControl.readCache[readCacheElement.readCacheKey] = readCacheElement
	if nil == flowControl.readCacheMRU {
		flowControl.readCacheMRU = readCacheElement
		flowControl.readCacheLRU = readCacheElement
	} else {
		readCacheElement.next = flowControl.readCacheMRU
		readCacheElement.next.prev = readCacheElement
		flowControl.readCacheMRU = readCacheElement
	}
	flowControl.capReadCacheWhileLocked()
}

func (flowControl *flowControlStruct) touchReadCacheElementWhileLocked(readCacheElement *readCacheElementStruct) {
	if flowControl.readCacheMRU != readCacheElement {
		if readCacheElement == flowControl.readCacheLRU {
			flowControl.readCacheLRU = readCacheElement.prev
			flowControl.readCacheLRU.next = nil
		} else {
			readCacheElement.prev.next = readCacheElement.next
			readCacheElement.next.prev = readCacheElement.prev
		}
		readCacheElement.next = flowControl.readCacheMRU
		readCacheElement.prev = nil
		flowControl.readCacheMRU.prev = readCacheElement
		flowControl.readCacheMRU = readCacheElement
	}
}

func (vS *volumeStruct) doReadPlan(fileInode *inMemoryInodeStruct, readPlan []ReadPlanStep, readPlanBytes uint64) (buf []byte, err error) {
	var (
		cacheLine            []byte
		cacheLineHitLength   uint64
		cacheLineHitOffset   uint64
		cacheLineStartOffset uint64
		chunkOffset          uint64
		flowControl          *flowControlStruct
		inFlightHit          bool
		inFlightHitBuf       []byte
		inFlightLogSegment   *inFlightLogSegmentStruct
		readCacheElement     *readCacheElementStruct
		readCacheHit         bool
		readCacheKey         readCacheKeyStruct
		readCacheLineSize    uint64
		remainingLength      uint64
		step                 ReadPlanStep
		stepIndex            int
	)

	flowControl = vS.flowControl
	readCacheLineSize = flowControl.readCacheLineSize
	readCacheKey.volumeName = vS.volumeName

	if 1 == len(readPlan) {
		// Possibly a trivial case (allowing for a potential zero-copy return)... three exist:
		//   Case 1: The lone step calls for a zero-filled []byte
		//   Case 2: The lone step is satisfied by reading from an inFlightLogSegment
		//   Case 3: The lone step is satisfied by landing completely within a single Read Cache Line

		step = readPlan[0]

		if 0 == step.LogSegmentNumber {
			// Case 1: The lone step calls for a zero-filled []byte
			buf = make([]byte, step.Length)
			stats.IncrementOperationsAndBucketedBytes(stats.FileRead, step.Length)
			err = nil
			return
		}

		fileInode.Lock()

		inFlightLogSegment, inFlightHit = fileInode.inFlightLogSegmentMap[step.LogSegmentNumber]
		if inFlightHit {
			// Case 2: The lone step is satisfied by reading from an inFlightLogSegment
			buf, err = inFlightLogSegment.Read(step.Offset, step.Length)
			if nil != err {
				fileInode.Unlock()
				logger.ErrorfWithError(err, "Reading back inFlightLogSegment failed - optimal case")
				err = blunder.AddError(err, blunder.SegReadError)
				return
			}
			fileInode.Unlock()
			stats.IncrementOperations(&stats.FileWritebackHitOps)
			stats.IncrementOperationsAndBucketedBytes(stats.FileRead, step.Length)
			return
		}

		stats.IncrementOperations(&stats.FileWritebackMissOps)

		fileInode.Unlock()

		cacheLineHitOffset = step.Offset % readCacheLineSize

		if (cacheLineHitOffset + step.Length) <= readCacheLineSize {
			// Case 3: The lone step is satisfied by landing completely within a single Read Cache Line
			readCacheKey.logSegmentNumber = step.LogSegmentNumber
			readCacheKey.cacheLineTag = step.Offset / readCacheLineSize

			flowControl.Lock()

			readCacheElement, readCacheHit = flowControl.readCache[readCacheKey]

			if readCacheHit {
				flowControl.touchReadCacheElementWhileLocked(readCacheElement)
				cacheLine = readCacheElement.cacheLine
				flowControl.Unlock()
				stats.IncrementOperations(&stats.FileReadcacheHitOps)
			} else {
				flowControl.Unlock()
				stats.IncrementOperations(&stats.FileReadcacheMissOps)
				// Make readCacheHit true (at MRU, likely kicking out LRU)
				cacheLineStartOffset = readCacheKey.cacheLineTag * readCacheLineSize
				cacheLine, err = swiftclient.ObjectGet(step.AccountName, step.ContainerName, step.ObjectName, cacheLineStartOffset, readCacheLineSize)
				if nil != err {
					logger.ErrorfWithError(err, "Reading from LogSegment object failed - optimal case")
					err = blunder.AddError(err, blunder.SegReadError)
					return
				}
				readCacheElement = &readCacheElementStruct{
					readCacheKey: readCacheKey,
					next:         nil,
					prev:         nil,
					cacheLine:    cacheLine,
				}
				flowControl.Lock()
				flowControl.insertReadCacheElementWhileLocked(readCacheElement)
				flowControl.Unlock()
			}

			if (cacheLineHitOffset + step.Length) > uint64(len(cacheLine)) {
				err = fmt.Errorf("Invalid range for LogSegment object - optimal case")
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.SegReadError)
				return
			}

			buf = cacheLine[cacheLineHitOffset:(cacheLineHitOffset + step.Length)]

			stats.IncrementOperationsAndBucketedBytes(stats.FileRead, step.Length)

			err = nil
			return
		}
	}

	// If we reach here, normal readPlan processing will be performed... no zero-copy opportunity

	buf = make([]byte, 0, readPlanBytes)

	for stepIndex, step = range readPlan {
		if 0 == step.LogSegmentNumber {
			// The step calls for a zero-filled []byte
			buf = append(buf, make([]byte, step.Length)...)
		} else {
			fileInode.Lock()
			inFlightLogSegment, inFlightHit = fileInode.inFlightLogSegmentMap[step.LogSegmentNumber]
			if inFlightHit {
				// The step is satisfied by reading from an inFlightLogSegment
				inFlightHitBuf, err = inFlightLogSegment.Read(step.Offset, step.Length)
				if nil != err {
					fileInode.Unlock()
					logger.ErrorfWithError(err, "Reading back inFlightLogSegment failed - general case")
					err = blunder.AddError(err, blunder.SegReadError)
					return
				}
				fileInode.Unlock()
				buf = append(buf, inFlightHitBuf...)
				stats.IncrementOperations(&stats.FileWritebackHitOps)
			} else {
				fileInode.Unlock()
				if (0 == stepIndex) && (1 == len(readPlan)) {
					// No need to increment stats.FileWritebackMissOps since it was incremented above
				} else {
					stats.IncrementOperations(&stats.FileWritebackMissOps)
				}
			}
			if !inFlightHit {
				// The step is satisfied by hitting or missing the Read Cache
				readCacheKey.logSegmentNumber = step.LogSegmentNumber
				chunkOffset = step.Offset
				remainingLength = step.Length
				for 0 < remainingLength {
					readCacheKey.cacheLineTag = chunkOffset / readCacheLineSize
					cacheLineHitOffset = chunkOffset % readCacheLineSize
					if (cacheLineHitOffset + remainingLength) > readCacheLineSize {
						// When we've got a cache hit, the read extends beyond the cache line
						cacheLineHitLength = readCacheLineSize - cacheLineHitOffset
					} else {
						// When we've got a cache hit, all the data is inside the cache line
						cacheLineHitLength = remainingLength
					}
					flowControl.Lock()
					readCacheElement, readCacheHit = flowControl.readCache[readCacheKey]
					if readCacheHit {
						flowControl.touchReadCacheElementWhileLocked(readCacheElement)
						cacheLine = readCacheElement.cacheLine
						flowControl.Unlock()
						stats.IncrementOperations(&stats.FileReadcacheHitOps)
					} else {
						flowControl.Unlock()
						stats.IncrementOperations(&stats.FileReadcacheMissOps)
						// Make readCacheHit true (at MRU, likely kicking out LRU)
						cacheLineStartOffset = readCacheKey.cacheLineTag * readCacheLineSize
						cacheLine, err = swiftclient.ObjectGet(step.AccountName, step.ContainerName, step.ObjectName, cacheLineStartOffset, readCacheLineSize)
						if nil != err {
							logger.ErrorfWithError(err, "Reading from LogSegment object failed - general case")
							err = blunder.AddError(err, blunder.SegReadError)
							return
						}
						readCacheElement = &readCacheElementStruct{
							readCacheKey: readCacheKey,
							next:         nil,
							prev:         nil,
							cacheLine:    cacheLine,
						}
						flowControl.Lock()
						flowControl.insertReadCacheElementWhileLocked(readCacheElement)
						flowControl.Unlock()
					}
					if (cacheLineHitOffset + cacheLineHitLength) > uint64(len(cacheLine)) {
						err = fmt.Errorf("Invalid range for LogSegment object - general case")
						logger.ErrorWithError(err)
						err = blunder.AddError(err, blunder.SegReadError)
						return
					}
					buf = append(buf, cacheLine[cacheLineHitOffset:(cacheLineHitOffset+cacheLineHitLength)]...)
					chunkOffset += cacheLineHitLength
					remainingLength -= cacheLineHitLength
				}
			}
		}
	}

	stats.IncrementOperationsAndBucketedBytes(stats.FileRead, uint64(len(buf)))

	err = nil
	return
}

func (vS *volumeStruct) doSendChunk(fileInode *inMemoryInodeStruct, buf []byte) (logSegmentNumber uint64, logSegmentOffset uint64, err error) {
	var (
		openLogSegmentContainerName string
		openLogSegmentObjectNumber  uint64
		openLogSegment              *inFlightLogSegmentStruct
	)

	fileInode.Lock()
	defer fileInode.Unlock()

	openLogSegment = openLogSegmentLRUGet(fileInode.InodeNumber)
	if openLogSegment == nil {
		openLogSegmentContainerName, openLogSegmentObjectNumber, err = fileInode.volume.provisionObject()
		if nil != err {
			logger.ErrorfWithError(err, "Provisioning LogSegment failed")
			return
		}

		err = fileInode.volume.setLogSegmentContainer(openLogSegmentObjectNumber, openLogSegmentContainerName)
		if nil != err {
			logger.ErrorfWithError(err, "Recording LogSegment ContainerName failed")
			return
		}

		openLogSegment = &inFlightLogSegmentStruct{
			logSegmentNumber: openLogSegmentObjectNumber,
			fileInode:        fileInode,
			accountName:      fileInode.volume.accountName,
			containerName:    openLogSegmentContainerName,
			objectName:       utils.Uint64ToHexStr(openLogSegmentObjectNumber),
		}

		openLogSegment.ChunkedPutContext, err = swiftclient.ObjectFetchChunkedPutContext(
			openLogSegment.accountName, openLogSegment.containerName, openLogSegment.objectName)
		if nil != err {
			logger.ErrorfWithError(err, "Starting Chunked PUT to LogSegment failed")
			return
		}

		fileInode.inFlightLogSegmentMap[openLogSegment.logSegmentNumber] = openLogSegment

		// the log segment will need to be waited on eventually
		fileInode.Add(1)
	}

	logSegmentNumber = openLogSegment.logSegmentNumber
	logSegmentOffset, err = openLogSegment.BytesPut()
	if nil != err {
		err = blunder.AddError(err, blunder.InodeFlushError)
		logger.ErrorfWithError(err, "Failed to get current LogSegmentOffset")
		openLogSegment.err = err

		// don't need to wait for flush of this log segment
		fileInode.Done()
		return
	}

	err = openLogSegment.ChunkedPutContext.SendChunk(buf)
	if nil != err {
		err = blunder.AddError(err, blunder.InodeFlushError)
		logger.ErrorfWithError(err, "Sending Chunked PUT chunk to LogSegment failed")
		openLogSegment.err = err

		fileInode.Done()
		return
	}

	// if the log segment is full then flush it now.  Otherwise put it back
	// in the cache for use by the next doSendChunk
	if (logSegmentOffset + uint64(len(buf))) >= fileInode.volume.flowControl.maxFlushSize {
		go vS.inFlightLogSegmentFlush(openLogSegment)
	} else {
		openLogSegmentLRUInsert(openLogSegment)
	}

	err = nil
	return
}

func (vS *volumeStruct) doFileInodeDataFlush(fileInode *inMemoryInodeStruct) (err error) {

	// if there's an open log segment then start its flush
	openLogSegment := openLogSegmentLRUGet(fileInode.InodeNumber)
	if openLogSegment != nil {
		go vS.inFlightLogSegmentFlush(openLogSegment)
		openLogSegment = nil
	}

	// wait for any and all log segment flushes to complete
	fileInode.Wait()

	// clean out finished segments and check for errors
	fileInode.Lock()

	for logSegmentNumber, inFlightLogSegment := range fileInode.inFlightLogSegmentMap {

		if inFlightLogSegment.err != nil && err == nil {

			// REVIEW TODO: What should we about this error, other than report it to caller?
			//              File system could go "read only" if that's sufficient...
			//              This will discard the write-back data ...
			err = inFlightLogSegment.err
			logger.ErrorfWithError(err, "doFileInodeDataFlush(): "+
				"Error encountered while flushing inode %d inFlightLogSegment %d",
				fileInode.InodeNumber, logSegmentNumber)
		}
		delete(fileInode.inFlightLogSegmentMap, logSegmentNumber)
	}
	fileInode.Unlock()

	return
}

// Close an open log segment and signal the associated inode when its finished
//
func (vS *volumeStruct) inFlightLogSegmentFlush(inFlightLogSegment *inFlightLogSegmentStruct) {

	err := inFlightLogSegment.Close()
	if err != nil {
		err = blunder.AddError(err, blunder.InodeFlushError)
		logger.ErrorfWithError(err, "inFlightLogSegmentFlush for inode %d failed",
			inFlightLogSegment.fileInode.InodeNumber)

		inFlightLogSegment.err = err
	}

	// And we are done (its OK to touch the WaitGroup with the inode lock)
	inFlightLogSegment.fileInode.Done()
}

func chunkedPutConnectionPoolStarvationCallback() {

	inFlightLogSegment := openLogSegmentLRUGetOldest()
	if inFlightLogSegment == nil {
		logger.Infof("chunkedPutConnectionPoolStarvationCallback(): no log segments to flush")
		return
	}

	volume := inFlightLogSegment.fileInode.volume
	volume.inFlightLogSegmentFlush(inFlightLogSegment)
}
