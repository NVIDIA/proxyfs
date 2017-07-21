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

func (vS *volumeStruct) doReadPlan(fileInode *inMemoryInodeStruct, readPlan []ReadPlanStep, readPlanBytes uint64) (buf []byte, err error) {
	var (
		cacheLine            []byte
		cacheLineHitLength   uint64
		cacheLineHitOffset   uint64
		cacheLineStartOffset uint64
		chunkOffset          uint64
		fileInodeFlusher     *fileInodeFlusherStruct
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

		fileInodeFlusher = fileInode.fileInodeFlusher

		if nil != fileInodeFlusher {
			inFlightLogSegment, inFlightHit = fileInodeFlusher.logSegmentsMap[step.LogSegmentNumber]
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
		}

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
				vS.flowControl.Lock()
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
			fileInodeFlusher = fileInode.fileInodeFlusher
			if nil == fileInodeFlusher {
				fileInode.Unlock()
				inFlightHit = false
			} else {
				inFlightLogSegment, inFlightHit = fileInodeFlusher.logSegmentsMap[step.LogSegmentNumber]
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
						vS.flowControl.Unlock()
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
								flowControl.readCacheLRU = vS.flowControl.readCacheLRU.prev
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
		fileInodeFlusher            *fileInodeFlusherStruct
		openLogSegment              *inFlightLogSegmentStruct
		openLogSegmentContainerName string
		openLogSegmentObjectNumber  uint64
		volume                      = fileInode.volume
	)

	fileInode.Lock()

	if nil == fileInode.fileInodeFlusher {
		fileInodeFlusher = &fileInodeFlusherStruct{
			fileInode:      fileInode,
			flushChannel:   make(chan bool, 1), // Buffer explicit flush request sent while implicitly flushing
			logSegmentsMap: make(map[uint64]*inFlightLogSegmentStruct),
			openLogSegment: nil,
			errors:         []error{},
		}

		fileInode.fileInodeFlusher = fileInodeFlusher

		fileInode.Add(1)
		go fileInodeFlusherDaemon(fileInodeFlusher)
	} else {
		fileInodeFlusher = fileInode.fileInodeFlusher
	}

	if nil == fileInodeFlusher.openLogSegment {
		openLogSegmentContainerName, openLogSegmentObjectNumber, err = volume.provisionObject()
		if nil != err {
			fileInode.Unlock()
			logger.ErrorfWithError(err, "Provisioning LogSegment failed")
			return
		}

		err = volume.setLogSegmentContainer(openLogSegmentObjectNumber, openLogSegmentContainerName)
		if nil != err {
			fileInode.Unlock()
			logger.ErrorfWithError(err, "Recording LogSegment ContainerName failed")
			return
		}

		openLogSegment = &inFlightLogSegmentStruct{
			logSegmentNumber: openLogSegmentObjectNumber,
			fileInodeFlusher: fileInodeFlusher,
			flushChannel:     make(chan bool),
			accountName:      volume.accountName,
			containerName:    openLogSegmentContainerName,
			objectName:       utils.Uint64ToHexStr(openLogSegmentObjectNumber),
		}

		openLogSegment.ChunkedPutContext, err = swiftclient.ObjectFetchChunkedPutContext(openLogSegment.accountName, openLogSegment.containerName, openLogSegment.objectName)
		if nil != err {
			fileInode.Unlock()
			logger.ErrorfWithError(err, "Starting Chunked PUT to LogSegment failed")
			return
		}

		fileInodeFlusher.logSegmentsMap[openLogSegment.logSegmentNumber] = openLogSegment
		fileInodeFlusher.openLogSegment = openLogSegment

		vS.Lock()
		vS.inFlightFileInodeDataMap[fileInode.InodeNumber] = fileInode
		vS.Unlock()
	} else {
		openLogSegment = fileInodeFlusher.openLogSegment
	}

	logSegmentNumber = openLogSegment.logSegmentNumber

	logSegmentOffset, err = openLogSegment.BytesPut()
	if nil != err {
		fileInode.Unlock()
		logger.ErrorfWithError(err, "Failed to get current LogSegmentOffset")
		return
	}

	err = openLogSegment.ChunkedPutContext.SendChunk(buf)
	if nil != err {
		fileInode.Unlock()
		logger.ErrorfWithError(err, "Sending Chunked PUT chunk to LogSegment failed")
		return
	}

	if (logSegmentOffset + uint64(len(buf))) >= volume.flowControl.maxFlushSize {
		fileInodeFlusher.openLogSegment = nil
		fileInodeFlusher.Add(1)
		go inFlightLogSegmentFlusher(openLogSegment)
	}

	fileInode.Unlock()

	err = nil
	return
}

func (vS *volumeStruct) doFileInodeDataFlush(fileInode *inMemoryInodeStruct) (err error) {
	var (
		fileInodeFlusher *fileInodeFlusherStruct
	)

	fileInode.Lock()

	fileInodeFlusher = fileInode.fileInodeFlusher

	if nil == fileInodeFlusher {
		fileInode.Unlock()
		err = nil
		return
	}

	fileInodeFlusher.flushChannel <- true

	fileInode.Unlock()

	fileInode.Wait()

	if 0 == len(fileInodeFlusher.errors) {
		err = nil
	} else {
		err = fmt.Errorf("Errors encountered while flushing inFlightLogSegments")
	}

	fileInode.fileInodeFlusher = nil

	vS.Lock()
	delete(vS.inFlightFileInodeDataMap, fileInode.InodeNumber)
	vS.Unlock()

	return
}

func fileInodeFlusherDaemon(fileInodeFlusher *fileInodeFlusherStruct) {
	var (
		fileInode      = fileInodeFlusher.fileInode
		openLogSegment *inFlightLogSegmentStruct
		volume         = fileInode.volume
	)

	// Wait for either an explicit or implicit (maxFlushTime) indication to perform the flush
	select {
	case _ = <-fileInodeFlusher.flushChannel:
		// The sender of the (bool) signaled a trigger to FLUSH
	case <-time.After(volume.flowControl.maxFlushTime):
		// We reached maxFlushTime... so flush it now
	}

	// Ensure we are the only one accessing fileInodeFlusher
	fileInode.Lock()

	// If there is an openLogSegment, close it off
	openLogSegment = fileInodeFlusher.openLogSegment
	if nil != openLogSegment {
		fileInodeFlusher.openLogSegment = nil
		fileInodeFlusher.Add(1)
		go inFlightLogSegmentFlusher(openLogSegment)
	}

	// Allow inFlightLogSegmentFlusher's to run
	fileInode.Unlock()

	// Now await any outstanding inFlightLogSegment's
	fileInodeFlusher.Wait()

	// Finally, inform anybody waiting that we are done
	fileInode.Done()

	return
}

func inFlightLogSegmentFlusher(inFlightLogSegment *inFlightLogSegmentStruct) {
	var (
		err              error
		fileInodeFlusher = inFlightLogSegment.fileInodeFlusher
	)

	// Terminate Chunked PUT
	err = inFlightLogSegment.Close()
	if nil != err {
		err = blunder.AddError(err, blunder.InodeFlushError)
		fileInodeFlusher.fileInode.Lock()
		fileInodeFlusher.errors = append(fileInodeFlusher.errors, err)
		fileInodeFlusher.fileInode.Unlock()
		fileInodeFlusher.Done()
		return
	}

	// Remove us from inFlightLogSegments.logSegmentsMap and let Go's Garbage Collector collect us as soon as we return/exit
	fileInodeFlusher.fileInode.Lock()
	delete(fileInodeFlusher.logSegmentsMap, inFlightLogSegment.logSegmentNumber)
	fileInodeFlusher.fileInode.Unlock()

	// And we are done
	fileInodeFlusher.Done()
	return
}
