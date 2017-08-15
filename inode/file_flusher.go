package inode

import (
	"fmt"
	"time"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

func capReadCache(flowControl *flowControlStruct) {
	flowControl.Lock()

	for uint64(len(flowControl.readCache)) > flowControl.readCacheLineCount {
		flowControl.readCacheLRU = flowControl.readCacheLRU.prev
		flowControl.readCacheLRU.next = nil
	}

	flowControl.Unlock()
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
				if flowControl.readCacheMRU != readCacheElement {
					// Move readCacheElement to MRU position in readCache
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
				// If the readCache size is at or greater than the limit, delete something.
				if uint64(len(flowControl.readCache)) >= flowControl.readCacheLineCount {
					// Purge LRU element
					delete(flowControl.readCache, flowControl.readCacheLRU.readCacheKey)
					if 1 == flowControl.readCacheLineCount {
						// Unreasonable... but still possible
						flowControl.readCacheMRU = nil
						flowControl.readCacheLRU = nil
					} else {
						flowControl.readCacheLRU = flowControl.readCacheLRU.prev
						flowControl.readCacheLRU.next = nil
					}
				}
				flowControl.readCache[readCacheElement.readCacheKey] = readCacheElement
				if nil == flowControl.readCacheMRU {
					flowControl.readCacheMRU = readCacheElement
					flowControl.readCacheLRU = readCacheElement
				} else {
					readCacheElement.next = flowControl.readCacheMRU
					readCacheElement.next.prev = readCacheElement
					flowControl.readCacheMRU = readCacheElement
				}
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
						if flowControl.readCacheMRU != readCacheElement {
							// Move readCacheElement to MRU position in readCache
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
						// If the readCache size is at or greater than the limit, delete something.
						if uint64(len(flowControl.readCache)) >= flowControl.readCacheLineCount {
							// Purge LRU element
							delete(flowControl.readCache, flowControl.readCacheLRU.readCacheKey)
							if 1 == flowControl.readCacheLineCount {
								// Unreasonable... but still possible
								flowControl.readCacheMRU = nil
								flowControl.readCacheLRU = nil
							} else {
								flowControl.readCacheLRU = flowControl.readCacheLRU.prev
								flowControl.readCacheLRU.next = nil
							}
						}
						flowControl.readCache[readCacheElement.readCacheKey] = readCacheElement
						if nil == flowControl.readCacheMRU {
							flowControl.readCacheMRU = readCacheElement
							flowControl.readCacheLRU = readCacheElement
						} else {
							readCacheElement.next = flowControl.readCacheMRU
							readCacheElement.next.prev = readCacheElement
							flowControl.readCacheMRU = readCacheElement
						}
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
	)

	fileInode.Lock()
	defer fileInode.Unlock()

	if nil == fileInode.openLogSegment {
		if 0 == len(fileInode.inFlightLogSegmentMap) {
			vS.Lock()
			vS.inFlightFileInodeDataMap[fileInode.InodeNumber] = fileInode
			vS.Unlock()
		}

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

		fileInode.openLogSegment = &inFlightLogSegmentStruct{
			logSegmentNumber: openLogSegmentObjectNumber,
			fileInode:        fileInode,
			flushChannel:     make(chan bool, 1), // Buffer explicit flush request sent while implicitly flushing
			accountName:      fileInode.volume.accountName,
			containerName:    openLogSegmentContainerName,
			objectName:       utils.Uint64ToHexStr(openLogSegmentObjectNumber),
		}

		fileInode.inFlightLogSegmentMap[fileInode.openLogSegment.logSegmentNumber] = fileInode.openLogSegment

		fileInode.openLogSegment.ChunkedPutContext, err = swiftclient.ObjectFetchChunkedPutContext(fileInode.openLogSegment.accountName, fileInode.openLogSegment.containerName, fileInode.openLogSegment.objectName)
		if nil != err {
			logger.ErrorfWithError(err, "Starting Chunked PUT to LogSegment failed")
			return
		}

		fileInode.Add(1)
		go inFlightLogSegmentFlusher(fileInode.openLogSegment)
	}

	logSegmentNumber = fileInode.openLogSegment.logSegmentNumber

	logSegmentOffset, err = fileInode.openLogSegment.BytesPut()
	if nil != err {
		logger.ErrorfWithError(err, "Failed to get current LogSegmentOffset")
		return
	}

	err = fileInode.openLogSegment.ChunkedPutContext.SendChunk(buf)
	if nil != err {
		logger.ErrorfWithError(err, "Sending Chunked PUT chunk to LogSegment failed")
		return
	}

	if (logSegmentOffset + uint64(len(buf))) >= fileInode.volume.flowControl.maxFlushSize {
		fileInode.openLogSegment.flushChannel <- true
		fileInode.openLogSegment = nil
	}

	err = nil
	return
}

func (vS *volumeStruct) doFileInodeDataFlush(fileInode *inMemoryInodeStruct) (err error) {
	fileInode.Lock()

	if nil != fileInode.openLogSegment {
		fileInode.openLogSegment.flushChannel <- true
		fileInode.openLogSegment = nil
	}

	fileInode.Unlock()

	fileInode.Wait()

	if 0 == len(fileInode.inFlightLogSegmentErrors) {
		err = nil
	} else {
		err = fmt.Errorf("Errors encountered while flushing inFlightLogSegments")
	}

	return
}

func inFlightLogSegmentFlusher(inFlightLogSegment *inFlightLogSegmentStruct) {
	var (
		err error
	)

	// Wait for either an explicit or implicit (maxFlushTime) indication to perform the flush
	select {
	case _ = <-inFlightLogSegment.flushChannel:
		// The sender of the (bool) signaled a trigger to FLUSH
	case <-time.After(inFlightLogSegment.fileInode.volume.flowControl.maxFlushTime):
		// We reached maxFlushTime... so flush it now
	}

	inFlightLogSegment.fileInode.Lock()

	// Terminate Chunked PUT
	err = inFlightLogSegment.Close()
	if nil != err {
		err = blunder.AddError(err, blunder.InodeFlushError)
		inFlightLogSegment.fileInode.inFlightLogSegmentErrors[inFlightLogSegment.logSegmentNumber] = err
		inFlightLogSegment.fileInode.Unlock()
		inFlightLogSegment.fileInode.Done()
		return
	}

	// Remove us from inFlightLogSegments.logSegmentsMap and let Go's Garbage Collector collect us as soon as we return/exit
	delete(inFlightLogSegment.fileInode.inFlightLogSegmentMap, inFlightLogSegment.logSegmentNumber)
	if 0 == len(inFlightLogSegment.fileInode.inFlightLogSegmentMap) {
		inFlightLogSegment.fileInode.volume.Lock()
		delete(inFlightLogSegment.fileInode.volume.inFlightFileInodeDataMap, inFlightLogSegment.fileInode.InodeNumber)
		inFlightLogSegment.fileInode.volume.Unlock()
	}

	if inFlightLogSegment.fileInode.openLogSegment == inFlightLogSegment {
		inFlightLogSegment.fileInode.openLogSegment = nil
	}

	inFlightLogSegment.fileInode.Unlock()

	// And we are done
	inFlightLogSegment.fileInode.Done()
	return
}
