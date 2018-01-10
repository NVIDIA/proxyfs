package inode

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/refcntpool"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

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

func (vS *volumeStruct) doReadPlan(fileInode *inMemoryInodeStruct, readPlan []ReadPlanStep, readPlanBytes uint64) (bufList *refcntpool.RefCntBufList, err error) {
	var (
		bufList2             *refcntpool.RefCntBufList
		cacheLine            *refcntpool.RefCntBufList
		cacheLineHitLength   uint64
		cacheLineHitOffset   uint64
		cacheLineStartOffset uint64
		chunkOffset          uint64
		flowControl          *flowControlStruct
		inFlightHit          bool
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

	// if we exit with an error insure that bufList is released
	bufList = globals.refCntBufListPool.GetRefCntBufList()
	defer func() {
		if err != nil {
			bufList.Release()
			bufList = nil
		}
	}()

	if 1 == len(readPlan) {
		// Possibly a trivial case (allowing for a potential zero-copy return)... three exist:
		//   Case 1: The lone step calls for a zero-filled []byte
		//   Case 2: The lone step is satisfied by reading from an inFlightLogSegment
		//   Case 3: The lone step is satisfied by landing completely within a single Read Cache Line

		step = readPlan[0]

		if 0 == step.LogSegmentNumber {
			// Case 1: The lone step calls for a zero-filled []byte
			//
			// add references to the zero filled buffer until we
			// have enough zeros
			var (
				copyByteCnt int
				zeroBufLen  = len(globals.refCntBufOfZeros.Buf)
			)
			for copyByteCnt = 0; uint64(copyByteCnt) < step.Length; copyByteCnt += zeroBufLen {
				bufList.AppendRefCntBuf(globals.refCntBufOfZeros)
			}
			if uint64(copyByteCnt) > step.Length {
				copyByteCnt -= zeroBufLen
				idx := len(bufList.Bufs) - 1
				bufList.Bufs[idx] = bufList.Bufs[idx][0 : step.Length-uint64(copyByteCnt)]
			}

			stats.IncrementOperationsAndBucketedBytes(stats.FileRead, step.Length)
			err = nil
			return
		}

		fileInode.Lock()

		inFlightLogSegment, inFlightHit = fileInode.inFlightLogSegmentMap[step.LogSegmentNumber]
		if inFlightHit {
			// Case 2: The lone step is satisfied by reading from an inFlightLogSegment
			bufList2, err = inFlightLogSegment.Read(step.Offset, step.Length)
			if nil != err {
				fileInode.Unlock()
				logger.ErrorfWithError(err, "Reading back inFlightLogSegment failed - optimal case")
				err = blunder.AddError(err, blunder.SegReadError)
				return
			}
			fileInode.Unlock()
			bufList.Release()
			bufList = bufList2
			bufList2 = nil

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
				cacheLine.Hold()
				flowControl.Unlock()
				stats.IncrementOperations(&stats.FileReadcacheHitOps)
			} else {
				flowControl.Unlock()
				stats.IncrementOperations(&stats.FileReadcacheMissOps)
				// Make readCacheHit true (at MRU, likely kicking out LRU)
				cacheLineStartOffset = readCacheKey.cacheLineTag * readCacheLineSize
				cacheLine, err = swiftclient.ObjectGet(
					step.AccountName, step.ContainerName, step.ObjectName,
					cacheLineStartOffset, readCacheLineSize)
				if nil != err {
					logger.ErrorfWithError(err, "Reading from LogSegment object failed - optimal case")
					err = blunder.AddError(err, blunder.SegReadError)
					return
				}

				// the ReadCache needs its own hold
				cacheLine.Hold()
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

			if (cacheLineHitOffset + step.Length) > uint64(cacheLine.Length()) {
				cacheLine.Release()
				err = fmt.Errorf("Invalid range for LogSegment object - optimal case")
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.SegReadError)
				return
			}

			bufList.AppendRefCntBufList(cacheLine,
				int(cacheLineHitOffset), int(step.Length))
			cacheLine.Release()

			stats.IncrementOperationsAndBucketedBytes(stats.FileRead, step.Length)

			err = nil
			return
		}
	}

	// If we reach here, normal readPlan processing will be performed...

	for stepIndex, step = range readPlan {
		if 0 == step.LogSegmentNumber {

			// this step calls for a zero-filled []byte;
			// add references to the zero filled buffer until we
			// have enough zeros
			var (
				offset     int
				zeroBufLen = len(globals.refCntBufOfZeros.Buf)
			)
			for offset = 0; uint64(offset) < step.Length; offset += zeroBufLen {
				bufList.AppendRefCntBuf(globals.refCntBufOfZeros)
			}
			if uint64(offset) > step.Length {
				offset -= zeroBufLen
				idx := len(bufList.Bufs) - 1
				bufList.Bufs[idx] = bufList.Bufs[idx][0 : step.Length-uint64(offset)]
			}
		} else {
			fileInode.Lock()
			inFlightLogSegment, inFlightHit = fileInode.inFlightLogSegmentMap[step.LogSegmentNumber]
			if inFlightHit {
				// The step is satisfied by reading from an inFlightLogSegment
				bufList2, err = inFlightLogSegment.Read(step.Offset, step.Length)
				if nil != err {
					fileInode.Unlock()
					logger.ErrorfWithError(err, "Reading back inFlightLogSegment failed - general case")
					err = blunder.AddError(err, blunder.SegReadError)
					return
				}
				fileInode.Unlock()

				bufList.AppendRefCntBufList(bufList2, 0, int(step.Length))
				bufList2.Release()
				bufList2 = nil

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
						cacheLine.Hold()
						flowControl.Unlock()
						stats.IncrementOperations(&stats.FileReadcacheHitOps)
					} else {
						flowControl.Unlock()
						stats.IncrementOperations(&stats.FileReadcacheMissOps)

						// Make readCacheHit true (at MRU, likely kicking out LRU)
						cacheLineStartOffset = readCacheKey.cacheLineTag * readCacheLineSize
						cacheLine, err = swiftclient.ObjectGet(
							step.AccountName, step.ContainerName, step.ObjectName,
							cacheLineStartOffset, readCacheLineSize)
						if nil != err {
							logger.ErrorfWithError(err, "Reading from LogSegment object failed - general case")
							err = blunder.AddError(err, blunder.SegReadError)
							return
						}

						// the ReadCache needs its own hold
						cacheLine.Hold()
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
					if (cacheLineHitOffset + cacheLineHitLength) > uint64(cacheLine.Length()) {
						cacheLine.Release()
						err = fmt.Errorf("Invalid range for LogSegment object - general case")
						logger.ErrorWithError(err)
						err = blunder.AddError(err, blunder.SegReadError)
						return
					}
					bufList.AppendRefCntBufList(cacheLine,
						int(cacheLineHitOffset), int(cacheLineHitLength))
					cacheLine.Release()

					chunkOffset += cacheLineHitLength
					remainingLength -= cacheLineHitLength
				}
			}
		}
	}

	stats.IncrementOperationsAndBucketedBytes(stats.FileRead, uint64(bufList.Length()))
	err = nil
	return
}

func (vS *volumeStruct) doReadPlanReturnSlice(fileInode *inMemoryInodeStruct, readPlan []ReadPlanStep, readPlanBytes uint64) (buf []byte, err error) {
	var (
		bufList *refcntpool.RefCntBufList
		byteCnt int
	)

	bufList, err = vS.doReadPlan(fileInode, readPlan, readPlanBytes)
	if err != nil {
		return
	}

	byteCnt = bufList.Length()
	buf = make([]byte, byteCnt, byteCnt)
	bufList.CopyOut(buf, 0)
	bufList.Release()

	return
}

func (vS *volumeStruct) doSendChunk(fileInode *inMemoryInodeStruct, bufList *refcntpool.RefCntBufList) (logSegmentNumber uint64, logSegmentOffset uint64, err error) {
	var (
		openLogSegmentContainerName string
		openLogSegmentObjectNumber  uint64
	)

	fileInode.Lock()
	defer fileInode.Unlock()

	if nil == fileInode.openLogSegment {
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
	}

	logSegmentNumber = fileInode.openLogSegment.logSegmentNumber

	logSegmentOffset, err = fileInode.openLogSegment.BytesPut()
	if nil != err {
		logger.ErrorfWithError(err, "Failed to get current LogSegmentOffset")
		return
	}

	err = fileInode.openLogSegment.ChunkedPutContext.SendChunk(bufList)
	if nil != err {
		logger.ErrorfWithError(err, "Sending Chunked PUT chunk to LogSegment failed")
		return
	}

	if (logSegmentOffset + uint64(bufList.Length())) >= fileInode.volume.flowControl.maxFlushSize {
		fileInode.Add(1)
		go inFlightLogSegmentFlusher(fileInode.openLogSegment)
		fileInode.openLogSegment = nil
	}

	err = nil
	return
}

func (vS *volumeStruct) doFileInodeDataFlush(fileInode *inMemoryInodeStruct) (err error) {
	fileInode.Lock()

	if nil != fileInode.openLogSegment {
		fileInode.Add(1)
		go inFlightLogSegmentFlusher(fileInode.openLogSegment)
		fileInode.openLogSegment = nil
	}

	fileInode.Unlock()

	fileInode.Wait()

	// REVIEW TODO: Does anybody every empty the errors map? Should they? Would this mask prior errors?
	//              File system could go "read only" if that's sufficient...
	//              Problem with write-back data... must discard it...n

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

	// Terminate Chunked PUT
	err = inFlightLogSegment.Close()
	if nil != err {
		err = blunder.AddError(err, blunder.InodeFlushError)
		inFlightLogSegment.fileInode.Lock()
		inFlightLogSegment.fileInode.inFlightLogSegmentErrors[inFlightLogSegment.logSegmentNumber] = err
		delete(inFlightLogSegment.fileInode.inFlightLogSegmentMap, inFlightLogSegment.logSegmentNumber)
		inFlightLogSegment.fileInode.Unlock()
		inFlightLogSegment.fileInode.Done()
		return
	}

	// Remove us from inFlightLogSegments.logSegmentsMap and let Go's Garbage Collector collect us as soon as we return/exit
	inFlightLogSegment.fileInode.Lock()
	delete(inFlightLogSegment.fileInode.inFlightLogSegmentMap, inFlightLogSegment.logSegmentNumber)
	inFlightLogSegment.fileInode.Unlock()

	// And we are done
	inFlightLogSegment.fileInode.Done()

	return
}
