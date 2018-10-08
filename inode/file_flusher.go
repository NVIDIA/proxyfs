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

func openLogSegmentLRUTouchWhileLocked(inFlightLogSegment *inFlightLogSegmentStruct) {
	// Move inode to the MRU end of openLogSegmentLRU

	if inFlightLogSegment != globals.openLogSegmentLRUTail {
		if inFlightLogSegment == globals.openLogSegmentLRUHead {
			globals.openLogSegmentLRUHead = inFlightLogSegment.openLogSegmentLRUNext
			globals.openLogSegmentLRUHead.openLogSegmentLRUPrev = nil

			inFlightLogSegment.openLogSegmentLRUPrev = globals.openLogSegmentLRUTail
			inFlightLogSegment.openLogSegmentLRUNext = nil

			globals.openLogSegmentLRUTail.openLogSegmentLRUNext = inFlightLogSegment
			globals.openLogSegmentLRUTail = inFlightLogSegment
		} else {
			inFlightLogSegment.openLogSegmentLRUPrev.openLogSegmentLRUNext = inFlightLogSegment.openLogSegmentLRUNext
			inFlightLogSegment.openLogSegmentLRUNext.openLogSegmentLRUPrev = inFlightLogSegment.openLogSegmentLRUPrev

			inFlightLogSegment.openLogSegmentLRUNext = nil
			inFlightLogSegment.openLogSegmentLRUPrev = globals.openLogSegmentLRUTail

			globals.openLogSegmentLRUTail.openLogSegmentLRUNext = inFlightLogSegment
			globals.openLogSegmentLRUTail = inFlightLogSegment
		}
	}
}

func openLogSegmentLRUTouch(inFlightLogSegment *inFlightLogSegmentStruct) {
	globals.Lock()
	openLogSegmentLRUTouchWhileLocked(inFlightLogSegment)
	globals.Unlock()
}

func openLogSegmentLRURemoveWhileLocked(inFlightLogSegment *inFlightLogSegmentStruct) {
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

func (volumeGroup *volumeGroupStruct) capReadCacheWhileLocked() {
	for uint64(len(volumeGroup.readCache)) > volumeGroup.readCacheLineCount {
		delete(volumeGroup.readCache, volumeGroup.readCacheLRU.readCacheKey)
		volumeGroup.readCacheLRU = volumeGroup.readCacheLRU.prev
		volumeGroup.readCacheLRU.next = nil
	}
}

func (volumeGroup *volumeGroupStruct) insertReadCacheElementWhileLocked(readCacheElement *readCacheElementStruct) {
	volumeGroup.readCache[readCacheElement.readCacheKey] = readCacheElement
	if nil == volumeGroup.readCacheMRU {
		volumeGroup.readCacheMRU = readCacheElement
		volumeGroup.readCacheLRU = readCacheElement
	} else {
		readCacheElement.next = volumeGroup.readCacheMRU
		readCacheElement.next.prev = readCacheElement
		volumeGroup.readCacheMRU = readCacheElement
	}
	volumeGroup.capReadCacheWhileLocked()
}

func (volumeGroup *volumeGroupStruct) touchReadCacheElementWhileLocked(readCacheElement *readCacheElementStruct) {
	if volumeGroup.readCacheMRU != readCacheElement {
		if readCacheElement == volumeGroup.readCacheLRU {
			volumeGroup.readCacheLRU = readCacheElement.prev
			volumeGroup.readCacheLRU.next = nil
		} else {
			readCacheElement.prev.next = readCacheElement.next
			readCacheElement.next.prev = readCacheElement.prev
		}
		readCacheElement.next = volumeGroup.readCacheMRU
		readCacheElement.prev = nil
		volumeGroup.readCacheMRU.prev = readCacheElement
		volumeGroup.readCacheMRU = readCacheElement
	}
}

func (vS *volumeStruct) doReadPlan(fileInode *inMemoryInodeStruct, readPlan []ReadPlanStep, readPlanBytes uint64) (buf []byte, err error) {
	var (
		cacheLine            []byte
		cacheLineHitLength   uint64
		cacheLineHitOffset   uint64
		cacheLineStartOffset uint64
		chunkOffset          uint64
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
		volumeGroup          *volumeGroupStruct
	)

	volumeGroup = vS.volumeGroup
	readCacheLineSize = volumeGroup.readCacheLineSize
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
			openLogSegmentLRUTouch(inFlightLogSegment)
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

			volumeGroup.Lock()

			readCacheElement, readCacheHit = volumeGroup.readCache[readCacheKey]

			if readCacheHit {
				volumeGroup.touchReadCacheElementWhileLocked(readCacheElement)
				cacheLine = readCacheElement.cacheLine
				volumeGroup.Unlock()
				stats.IncrementOperations(&stats.FileReadcacheHitOps)
			} else {
				volumeGroup.Unlock()
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
				volumeGroup.Lock()
				volumeGroup.insertReadCacheElementWhileLocked(readCacheElement)
				volumeGroup.Unlock()
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
				openLogSegmentLRUTouch(inFlightLogSegment)
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
					volumeGroup.Lock()
					readCacheElement, readCacheHit = volumeGroup.readCache[readCacheKey]
					if readCacheHit {
						volumeGroup.touchReadCacheElementWhileLocked(readCacheElement)
						cacheLine = readCacheElement.cacheLine
						volumeGroup.Unlock()
						stats.IncrementOperations(&stats.FileReadcacheHitOps)
					} else {
						volumeGroup.Unlock()
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
						volumeGroup.Lock()
						volumeGroup.insertReadCacheElementWhileLocked(readCacheElement)
						volumeGroup.Unlock()
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
		inFlightLogSegment          *inFlightLogSegmentStruct
		openLogSegmentContainerName string
		openLogSegmentObjectNumber  uint64
	)

	fileInode.Lock()

	if nil == fileInode.openLogSegment {
		// Drop fileInode Lock while preparing an inFlightLogSegment. This is to avoid a deadlock where
		// starvation for ChunkedPutContext's might need to grab this fileInode's Lock to check a previous
		// openLogSegment associated with this fileInode (and, hence, when we looked was then on the
		// openLogSegmentLRU).

		fileInode.Unlock()

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

		inFlightLogSegment = &inFlightLogSegmentStruct{
			logSegmentNumber: openLogSegmentObjectNumber,
			fileInode:        fileInode,
			accountName:      fileInode.volume.accountName,
			containerName:    openLogSegmentContainerName,
			objectName:       utils.Uint64ToHexStr(openLogSegmentObjectNumber),
		}

		inFlightLogSegment.ChunkedPutContext, err = swiftclient.ObjectFetchChunkedPutContext(inFlightLogSegment.accountName, inFlightLogSegment.containerName, inFlightLogSegment.objectName, "")
		if nil != err {
			logger.ErrorfWithError(err, "Starting Chunked PUT to LogSegment failed")
			return
		}

		// Now reestablish the fileInode Lock before continuing

		fileInode.Lock()

		fileInode.inFlightLogSegmentMap[inFlightLogSegment.logSegmentNumber] = inFlightLogSegment

		fileInode.openLogSegment = inFlightLogSegment
		openLogSegmentLRUInsert(inFlightLogSegment)
	} else {
		inFlightLogSegment = fileInode.openLogSegment
		openLogSegmentLRUTouch(inFlightLogSegment)
	}

	logSegmentNumber = inFlightLogSegment.logSegmentNumber

	logSegmentOffset, err = inFlightLogSegment.BytesPut()
	if nil != err {
		fileInode.Unlock()
		logger.ErrorfWithError(err, "Failed to get current LogSegmentOffset")
		return
	}

	err = inFlightLogSegment.ChunkedPutContext.SendChunk(buf)
	if nil != err {
		fileInode.Unlock()
		logger.ErrorfWithError(err, "Sending Chunked PUT chunk to LogSegment failed")
		return
	}

	if (logSegmentOffset + uint64(len(buf))) >= fileInode.volume.maxFlushSize {
		fileInode.Add(1)
		go vS.inFlightLogSegmentFlusher(inFlightLogSegment, true)
		// No need to wait for it to complete now... that's only in doFileInodeDataFlush()
	}

	fileInode.Unlock()

	err = nil
	return
}

func (vS *volumeStruct) doFileInodeDataFlush(fileInode *inMemoryInodeStruct) (err error) {
	var (
		inFlightLogSegment *inFlightLogSegmentStruct
	)

	fileInode.Lock()

	if nil != fileInode.openLogSegment {
		inFlightLogSegment = fileInode.openLogSegment
		fileInode.Add(1)
		go vS.inFlightLogSegmentFlusher(inFlightLogSegment, true)
	}

	fileInode.Unlock()

	// Wait for all invocations of inFlightLogSegmentFlusher() for this fileInode have completed

	fileInode.Wait()

	// REVIEW TODO: Does anybody ever empty the errors map? Should they? Would this mask prior errors?
	//              File system could go "read only" if that's sufficient...
	//              Problem with write-back data... must discard it...

	if 0 == len(fileInode.inFlightLogSegmentErrors) {
		err = nil
	} else {
		err = fmt.Errorf("Errors encountered while flushing inFlightLogSegments")
	}

	return
}

func (vS *volumeStruct) inFlightLogSegmentFlusher(inFlightLogSegment *inFlightLogSegmentStruct, doDone bool) {
	var (
		err       error
		fileInode *inMemoryInodeStruct
	)

	// Handle the race between a DLM-serialized Flush triggering this versus the starvatation condition
	// doing so... Either one will perform the appropriate steps to enable the Flush() to complete.

	fileInode = inFlightLogSegment.fileInode

	fileInode.Lock()

	if inFlightLogSegment != fileInode.openLogSegment {
		// Either a Close() is already in progress or has already completed

		fileInode.Unlock()
		if doDone {
			fileInode.Done()
		}
		return
	}

	// This, and inFlightLogSegment still being in fileInode.inFlightLogSegmentMap,
	// means "a Close() is already in progress"

	fileInode.openLogSegment = nil

	// Terminate Chunked PUT while not holding fileInode.Lock

	fileInode.Unlock()
	err = inFlightLogSegment.Close()
	fileInode.Lock()

	// Finish up... recording error (if any) in the process

	if nil != err {
		err = blunder.AddError(err, blunder.InodeFlushError)
		fileInode.inFlightLogSegmentErrors[inFlightLogSegment.logSegmentNumber] = err
	}

	delete(inFlightLogSegment.fileInode.inFlightLogSegmentMap, inFlightLogSegment.logSegmentNumber)

	openLogSegmentLRURemove(inFlightLogSegment)

	fileInode.Unlock()

	if doDone {
		fileInode.Done()
	}
}

func chunkedPutConnectionPoolStarvationCallback() {
	var (
		fileInode          *inMemoryInodeStruct
		inFlightLogSegment *inFlightLogSegmentStruct
		volume             *volumeStruct
	)

	globals.Lock()

	if 0 == globals.openLogSegmentLRUItems {
		globals.Unlock()
		return
	}

	inFlightLogSegment = globals.openLogSegmentLRUHead

	fileInode = inFlightLogSegment.fileInode
	volume = fileInode.volume

	globals.Unlock()

	// Call inFlightLogSegmentFlusher() synchronously because we only want to return when it completes
	// and we don't want to call fileInode.Wait() as this would wait until all invocations of
	// inFlightLogSegmentFlusher() for the fileInode have completed.

	volume.inFlightLogSegmentFlusher(inFlightLogSegment, false)
}
