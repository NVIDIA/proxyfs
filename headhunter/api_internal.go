package headhunter

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

func (volume *volumeStruct) getCheckpoint() (err error) {
	var (
		checkpointContainerHeaders map[string][]string
		checkpointHeader           checkpointHeaderV2Struct
		checkpointHeaderValue      string
		checkpointHeaderValueSlice []string
		checkpointHeaderValues     []string
		checkpointObjectTrailerBuf []byte
		checkpointVersion          uint64
		ok                         bool
	)

	checkpointContainerHeaders, err = swiftclient.ContainerHead(volume.accountName, volume.checkpointContainerName)
	if nil == err {
		checkpointHeaderValues, ok = checkpointContainerHeaders[checkpointHeaderName]
		if !ok {
			err = fmt.Errorf("Missing %v/%v header %v", volume.accountName, volume.checkpointContainerName, checkpointHeaderName)
			return
		}
		if 1 != len(checkpointHeaderValues) {
			err = fmt.Errorf("Expected one single value for %v/%v header %v", volume.accountName, volume.checkpointContainerName, checkpointHeaderName)
			return
		}

		checkpointHeaderValue = checkpointHeaderValues[0]
	} else {
		if 404 == blunder.HTTPCode(err) {
			// Checkpoint Container not found... so try to create it with some initial values...

			checkpointHeader.checkpointObjectTrailerV2StructObjectNumber = 0
			checkpointHeader.checkpointObjectTrailerV2StructObjectLength = 0

			checkpointHeader.reservedToNonce = firstNonceToProvide - 1

			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
				checkpointHeaderVersion2,
				checkpointHeader.checkpointObjectTrailerV2StructObjectNumber,
				checkpointHeader.checkpointObjectTrailerV2StructObjectLength,
				checkpointHeader.reservedToNonce,
			)

			checkpointHeaderValues = []string{checkpointHeaderValue}

			checkpointContainerHeaders = make(map[string][]string)

			checkpointContainerHeaders[checkpointHeaderName] = checkpointHeaderValues

			err = swiftclient.ContainerPut(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
			if nil != err {
				return
			}
		} else {
			// If Checkpoint Container HEAD failed for some other reason, we must exit before doing any damage
			return
		}
	}

	checkpointHeaderValueSlice = strings.Split(checkpointHeaderValue, " ")

	if 1 > len(checkpointHeaderValueSlice) {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
		return
	}

	checkpointVersion, err = strconv.ParseUint(checkpointHeaderValueSlice[0], 16, 64)
	if nil != err {
		return
	}

	if checkpointHeaderVersion1 == checkpointVersion {
		// Read in checkpointHeaderV1Struct converting it to a checkpointHeaderV2Struct + checkpointObjectTrailerV2Struct

		if 11 != len(checkpointHeaderValueSlice) {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (wrong number of fields)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader = &checkpointHeaderV2Struct{
			checkpointObjectTrailerV2StructObjectNumber: 0,
			checkpointObjectTrailerV2StructObjectLength: 0,
		}

		volume.checkpointObjectTrailer = &checkpointObjectTrailerV2Struct{}

		volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[1], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectNumber)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[2], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectOffset)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[3], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectLength)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[4], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectNumber)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[5], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectOffset)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[6], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectLength)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[7], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectNumber)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[8], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectOffset)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[9], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectLength)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader.reservedToNonce, err = strconv.ParseUint(checkpointHeaderValueSlice[10], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad nextNonce)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		// TODO: load B+Trees
		//       compute volume.{inodeRec|logSegmentRec|bPlusTreeObject}BPlusTreeLayout LayoutReports
		//       fill in volume.checkpointObjectTrailer.{inodeRec|logSegmentRec|bPlusTreeObject}BPlusTreeLayoutNumElements
	} else if checkpointHeaderVersion2 == checkpointVersion {
		// Read in checkpointHeaderV2Struct

		if 4 != len(checkpointHeaderValueSlice) {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (wrong number of fields)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader = &checkpointHeaderV2Struct{}

		volume.checkpointHeader.checkpointObjectTrailerV2StructObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[1], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad objectNumber)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader.checkpointObjectTrailerV2StructObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[2], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad objectLength)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader.reservedToNonce, err = strconv.ParseUint(checkpointHeaderValueSlice[3], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad nextNonce)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue)
			return
		}

		// TODO: Read in checkpointObjectTrailerV2Struct
		checkpointObjectTrailerBuf, _, err = swiftclient.ObjectTail(volume.accountName, volume.checkpointContainerName, utils.Uint64ToHexStr(volume.checkpointHeader.checkpointObjectTrailerV2StructObjectNumber), volume.checkpointHeader.checkpointObjectTrailerV2StructObjectLength)
		if nil != err {
			return
		}

		volume.checkpointObjectTrailer = &checkpointObjectTrailerV2Struct{}

		_, _ = cstruct.Unpack(checkpointObjectTrailerBuf, volume.checkpointObjectTrailer, LittleEndian)

		// TODO: Deserialize volume.{inodeRec|logSegmentRec|bPlusTreeObject}BPlusTreeLayout LayoutReports
	} else {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (version: %v not supported)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue, checkpointVersion)
		return
	}

	err = nil
	return
}

//(bPT.volume.accountName, bPT.volume.checkpointContainerName, utils.Uint64ToHexStr(bPT.volume.checkpointObjectNumber))
/*
// ObjectTail invokes HTTP GET on the named Swift Object with a byte range selecting the specified length of trailing bytes.
func ObjectTail(accountName string, containerName string, objectName string, length uint64) (buf []byte, objectLength int64, err error) {
	return objectTailWithRetry(accountName, containerName, objectName, length)
}
*/

/*
func (checkpointHeader *checkpointHeaderV2Struct) put() (err error) {
	var (
		checkpointHeaderValue      string
		checkpointHeaderValues     []string
		checkpointContainerHeaders map[string][]string
	)

	checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
		checkpointHeaderVersion2,
		checkpointHeader.checkpointObjectTrailerV2StructObjectNumber,
		checkpointHeader.checkpointObjectTrailerV2StructObjectLength,
		checkpointHeader.reservedToNonce,
	)

	checkpointHeaderValues = []string{checkpointHeaderValue}

	checkpointContainerHeaders = make(map[string][]string)

	checkpointContainerHeaders[checkpointHeaderName] = checkpointHeaderValues

	err = swiftclient.ContainerPost(checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointContainerHeaders)

	return
}
*/
func (volume *volumeStruct) putCheckpoint() (err error) {
	return // TODO
}

// checkpointDaemon periodically and upon request persists a checkpoint/snapshot.
//
// Each checkpoint, the "live" B+Tree's implementing the state of headhunter are
// Clone()'d. In the background, these Clone()'d copies are serialized to a Swift
// Object and a description of the checkpoint taken is recorded in a Header of
// the Swift Container holding the checkpoint Swift Objects.
//
// When checkpointDaemon() takes a Clone() of each of the three B+Tree's, it
// specifies the andUnTouch==true option which will mark each node of the "live"
// B+Tree as CLEAN. This is so that a subsequent Clone() will only capture the
// delta from the previous Clone(). Unfortunately, should the checkpoint fail
// later in the process (e.g. when recording it's location), those changes could
// be lost in certain cases. To avoid that, we must ensure every node of each
// B+Tree is DIRTY such that a subsequent Clone() will fully replicate the B+Tree
// at that point and not miss any changes that failed to be recorded previously.
func (volume *volumeStruct) checkpointDaemon() {
	var (
		checkpointHeader                              *checkpointHeaderV1Struct
		checkpointRequest                             *checkpointRequestStruct
		checkpointDoneWaitGroup                       *sync.WaitGroup
		checkpointGateWaitGroup                       *sync.WaitGroup
		flushObjectLength                             uint64
		flushObjectNumber                             uint64
		flushObjectOffset                             uint64
		potentialCheckpointCompactorObjectNumberLimit uint64
	)

	checkpointRequest = nil // Left non-nil if request needs to be retried due to Nonce exhaustion

	for {
		if nil == checkpointRequest {
			select {
			case checkpointRequest = <-volume.checkpointRequestChan:
				// Explicitly requested checkpoint... use it below
			case <-time.After(volume.checkpointInterval):
				// Time to automatically do a checkpoint... so dummy up a checkpointRequest
				checkpointRequest = &checkpointRequestStruct{exitOnCompletion: false}
				checkpointRequest.waitGroup.Add(1) // ...even though we won't be waiting on it...
			}
		}

		volume.Lock()

		if volume.nextNonce == volume.reservedToNonce {
			// Need to first replenish Nonce pool

			volume.reservedToNonce += uint64(volume.nonceValuesToReserve)

			checkpointHeader = &checkpointHeaderV1Struct{}
			*checkpointHeader = *volume.lastSuccessfulCheckpointHeaderPut
			checkpointHeader.reservedToNonce = volume.reservedToNonce

			checkpointRequest.err = checkpointHeader.put()

			volume.Unlock()

			if nil != checkpointRequest.err {
				checkpointRequest.waitGroup.Done()
				if checkpointRequest.exitOnCompletion {
					return
				}
				checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
				continue
			}

			// Checkpoint done - now retry this checkpointRequest

			continue
		}

		// We are guaranteed to have a Nonce available if we reach here

		volume.checkpointObjectNumber = volume.nextNonce // Simply discarded if checkpointChunkedPutContext not used
		volume.checkpointObjectOffset = 0

		volume.nextNonce++

		potentialCheckpointCompactorObjectNumberLimit = volume.checkpointObjectNumber // Used only if compacting

		// Save checkpointDoneWaitGroup before taking SnapShots

		checkpointGateWaitGroup = volume.checkpointGateWaitGroup
		volume.checkpointGateWaitGroup = nil

		checkpointDoneWaitGroup = volume.checkpointDoneWaitGroup
		volume.checkpointDoneWaitGroup = nil

		// Trigger "full" clone if requested or it's been too long

		volume.checkpointIntervalsSinceCompaction++

		if volume.checkpointIntervalsSinceCompaction >= volume.checkpointIntervalsPerCompaction {
			volume.needFullClone = true
		}

		if volume.needFullClone {
			checkpointRequest.err = volume.inodeRec.bPlusTree.Touch()
			if nil != checkpointRequest.err {
				volume.Unlock()
				checkpointRequest.waitGroup.Done()
				logger.ErrorfWithError(checkpointRequest.err, "volume.inodeRec.bPlusTree.Touch() on volume \"%s\" failed", volume.volumeName)
				if checkpointRequest.exitOnCompletion {
					return
				}
				checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
				continue
			}

			checkpointRequest.err = volume.logSegmentRec.bPlusTree.Touch()
			if nil != checkpointRequest.err {
				volume.Unlock()
				checkpointRequest.waitGroup.Done()
				logger.ErrorfWithError(checkpointRequest.err, "volume.logSegmentRec.bPlusTree.Touch() on volume \"%s\" failed", volume.volumeName)
				if checkpointRequest.exitOnCompletion {
					return
				}
				checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
				continue
			}

			checkpointRequest.err = volume.bPlusTreeObject.bPlusTree.Touch()
			if nil != checkpointRequest.err {
				volume.Unlock()
				checkpointRequest.waitGroup.Done()
				logger.ErrorfWithError(checkpointRequest.err, "volume.bPlusTreeObject.bPlusTree.Touch() on volume \"%s\" failed", volume.volumeName)
				if checkpointRequest.exitOnCompletion {
					return
				}
				checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
				continue
			}
		}

		flushObjectNumber, flushObjectOffset, flushObjectLength, _, checkpointRequest.err = volume.inodeRec.bPlusTree.Flush(false)
		if nil != checkpointRequest.err {
			volume.Unlock()
			if nil != volume.checkpointChunkedPutContext {
				volume.checkpointObjectOffset = 0
				_ = volume.checkpointChunkedPutContext.Close()
				volume.checkpointChunkedPutContext = nil
			}
			volume.checkpointObjectNumber = 0
			volume.needFullClone = true // For next checkpoint...
			checkpointRequest.waitGroup.Done()
			logger.ErrorfWithError(checkpointRequest.err, "volume.inodeRec.bPlusTree.Flush() on volume \"%s\" failed", volume.volumeName)
			if checkpointRequest.exitOnCompletion {
				return
			}
			checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
			continue
		}
		if flushObjectNumber > volume.inodeRec.objectNumber {
			// Update checkpoint'd inodeRec location
			volume.inodeRec.objectNumber = flushObjectNumber
			volume.inodeRec.objectOffset = flushObjectOffset
			volume.inodeRec.objectLength = flushObjectLength
		}
		flushObjectNumber, flushObjectOffset, flushObjectLength, _, checkpointRequest.err = volume.logSegmentRec.bPlusTree.Flush(false)
		if nil != checkpointRequest.err {
			volume.Unlock()
			if nil != volume.checkpointChunkedPutContext {
				volume.checkpointObjectOffset = 0
				_ = volume.checkpointChunkedPutContext.Close()
				volume.checkpointChunkedPutContext = nil
			}
			volume.checkpointObjectNumber = 0
			volume.needFullClone = true // For next checkpoint...
			checkpointRequest.waitGroup.Done()
			logger.ErrorfWithError(checkpointRequest.err, "volume.logSegmentRec.bPlusTree.Flush() on volume \"%s\" failed", volume.volumeName)
			if checkpointRequest.exitOnCompletion {
				return
			}
			checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
			continue
		}
		if flushObjectNumber > volume.logSegmentRec.objectNumber {
			// Update checkpoint'd logSegmentRec location
			volume.logSegmentRec.objectNumber = flushObjectNumber
			volume.logSegmentRec.objectOffset = flushObjectOffset
			volume.logSegmentRec.objectLength = flushObjectLength
		}
		flushObjectNumber, flushObjectOffset, flushObjectLength, _, checkpointRequest.err = volume.bPlusTreeObject.bPlusTree.Flush(false)
		if nil != checkpointRequest.err {
			volume.Unlock()
			if nil != volume.checkpointChunkedPutContext {
				volume.checkpointObjectOffset = 0
				_ = volume.checkpointChunkedPutContext.Close()
				volume.checkpointChunkedPutContext = nil
			}
			volume.checkpointObjectNumber = 0
			volume.needFullClone = true // For next checkpoint...
			checkpointRequest.waitGroup.Done()
			logger.ErrorfWithError(checkpointRequest.err, "volume.bPlusTreeObject.bPlusTree.Flush() on volume \"%s\" failed", volume.volumeName)
			if checkpointRequest.exitOnCompletion {
				return
			}
			checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
			continue
		}
		if flushObjectNumber > volume.bPlusTreeObject.objectNumber {
			// Update checkpoint'd bPlusTreeObject location
			volume.bPlusTreeObject.objectNumber = flushObjectNumber
			volume.bPlusTreeObject.objectOffset = flushObjectOffset
			volume.bPlusTreeObject.objectLength = flushObjectLength
		}

		volume.Unlock()

		if nil == volume.checkpointChunkedPutContext {
			volume.checkpointObjectNumber = 0 // Throw away provisioned ObjectNumber... didn't need it
		} else {
			// Some checkpoint data was written... so close off Object Chunked PUT
			checkpointRequest.err = volume.checkpointChunkedPutContext.Close()
			if nil != checkpointRequest.err {
				volume.checkpointObjectNumber = 0
				volume.checkpointObjectOffset = 0
				volume.checkpointChunkedPutContext = nil
				volume.needFullClone = true // For next checkpoint...
				checkpointRequest.waitGroup.Done()
				logger.ErrorfWithError(checkpointRequest.err, "volume.checkpointChunkedPutContext.Close() on volume \"%s\" failed", volume.volumeName)
				if checkpointRequest.exitOnCompletion {
					return
				}
				checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
				continue
			}
			volume.checkpointObjectNumber = 0
			volume.checkpointObjectOffset = 0
			volume.checkpointChunkedPutContext = nil
		}

		if nil != checkpointGateWaitGroup {
			checkpointGateWaitGroup.Wait() // Wait for clients to say it's ok
		}

		checkpointHeader = &checkpointHeaderV1Struct{
			volume: volume,
			inodeRecBPlusTreeObjectNumber:        volume.inodeRec.objectNumber,
			inodeRecBPlusTreeObjectOffset:        volume.inodeRec.objectOffset,
			inodeRecBPlusTreeObjectLength:        volume.inodeRec.objectLength,
			logSegmentRecBPlusTreeObjectNumber:   volume.logSegmentRec.objectNumber,
			logSegmentRecBPlusTreeObjectOffset:   volume.logSegmentRec.objectOffset,
			logSegmentRecBPlusTreeObjectLength:   volume.logSegmentRec.objectLength,
			bPlusTreeObjectBPlusTreeObjectNumber: volume.bPlusTreeObject.objectNumber,
			bPlusTreeObjectBPlusTreeObjectOffset: volume.bPlusTreeObject.objectOffset,
			bPlusTreeObjectBPlusTreeObjectLength: volume.bPlusTreeObject.objectLength,
			reservedToNonce:                      volume.reservedToNonce,
		}

		checkpointRequest.err = checkpointHeader.put()
		if nil == checkpointRequest.err {
			volume.lastSuccessfulCheckpointHeaderPut = checkpointHeader
		} else {
			volume.needFullClone = true // For next checkpoint...
			checkpointRequest.waitGroup.Done()
			logger.ErrorfWithError(checkpointRequest.err, "checkpointHeader.put() on volume \"%s\" failed", volume.volumeName)
			if checkpointRequest.exitOnCompletion {
				return
			}
			checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
			continue
		}

		if volume.needFullClone {
			// Full Clone successfully recorded... so:
			//
			//   Clear this setting
			//   Reset interval counter
			//   Kick off background deletion of now unreferenced checkpoint objects

			volume.needFullClone = false
			volume.checkpointIntervalsSinceCompaction = 0

			if 0 == volume.checkpointCompactorObjectNumberLimit {
				// Must kick off background checkpoint compactor (after indicating limit)
				volume.checkpointCompactorObjectNumberLimit = potentialCheckpointCompactorObjectNumberLimit
				go volume.checkpointCompactor()
			} else {
				// Just bump up limit
				volume.checkpointCompactorObjectNumberLimit = potentialCheckpointCompactorObjectNumberLimit
			}
		}

		checkpointRequest.waitGroup.Done() // Awake the checkpoint requestor
		if nil != checkpointDoneWaitGroup {
			// Awake any others who were waiting the next checkpoint
			checkpointDoneWaitGroup.Done()
		}

		if checkpointRequest.exitOnCompletion {
			return
		}

		checkpointRequest = nil // Indicate we are done with this checkpointRequest before looping
	}
}

func (volume *volumeStruct) checkpointCompactor() {
	var (
		err                                             error
		lastWorkingCheckpointCompactorObjectNumberLimit uint64
		objectList                                      []string
		objectName                                      string
		objectNumber                                    uint64
		thisWorkingCheckpointCompactorObjectNumberLimit uint64
	)

	lastWorkingCheckpointCompactorObjectNumberLimit = 0

	for {
		volume.Lock()
		thisWorkingCheckpointCompactorObjectNumberLimit = volume.checkpointCompactorObjectNumberLimit
		if lastWorkingCheckpointCompactorObjectNumberLimit == thisWorkingCheckpointCompactorObjectNumberLimit {
			volume.checkpointCompactorObjectNumberLimit = 0
			volume.Unlock()
			return
		}
		volume.Unlock()

		_, objectList, err = swiftclient.ContainerGet(volume.accountName, volume.checkpointContainerName)
		if nil != err {
			volume.Lock()
			volume.checkpointCompactorObjectNumberLimit = 0
			volume.Unlock()
			logger.ErrorfWithError(err, "ContainerGet(%s, %s) on Checkpoint Container failed", volume.accountName, volume.checkpointContainerName)
			return
		}

		for _, objectName = range objectList {
			objectNumber, err = strconv.ParseUint(objectName, 16, 64)
			if nil != err {
				volume.Lock()
				volume.checkpointCompactorObjectNumberLimit = 0
				volume.Unlock()
				logger.ErrorfWithError(err, "strconv.ParseUint(\"%s\") on Checkpoint Container %s/%s failed", objectName, volume.accountName, volume.checkpointContainerName)
				return
			}

			if objectNumber < thisWorkingCheckpointCompactorObjectNumberLimit {
				err = swiftclient.ObjectDeleteSync(volume.accountName, volume.checkpointContainerName, objectName)
				if nil != err {
					volume.Lock()
					volume.checkpointCompactorObjectNumberLimit = 0
					volume.Unlock()
					logger.ErrorfWithError(err, "swiftclient.ObjectDeleteSync(%s/%s/%s) on Checkpoint Container objectName failed", volume.accountName, volume.checkpointContainerName, objectName)
					return
				}
			}
		}

		lastWorkingCheckpointCompactorObjectNumberLimit = thisWorkingCheckpointCompactorObjectNumberLimit
	}
}

func (bPT *bPlusTreeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("headhunter.bPlusTreeStruct.DumpKey() could not parse key as a uint64")
		return
	}

	keyAsString = fmt.Sprintf("0x%016X", keyAsUint64)

	err = nil
	return
}

func (bPT *bPlusTreeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsByteSlice, ok := value.([]byte)
	if !ok {
		err = fmt.Errorf("headhunter.bPlusTreeStruct.DumpValue() count not parse value as []byte")
		return
	}

	if 0 == len(valueAsByteSlice) {
		valueAsString = ""
		err = nil
		return
	}

	valueAsHexDigitByteSlice := make([]byte, 2+(3*(len(valueAsByteSlice)-1)))

	for i, u8 := range valueAsByteSlice {
		if i == 0 {
			valueAsHexDigitByteSlice[0] = utils.ByteToHexDigit(u8 >> 4)
			valueAsHexDigitByteSlice[1] = utils.ByteToHexDigit(u8 & 0x0F)
		} else {
			valueAsHexDigitByteSlice[(3*i)-1] = ' '
			valueAsHexDigitByteSlice[(3*i)+0] = utils.ByteToHexDigit(u8 >> 4)
			valueAsHexDigitByteSlice[(3*i)+1] = utils.ByteToHexDigit(u8 & 0x0F)
		}
	}

	valueAsString = string(valueAsHexDigitByteSlice[:])

	err = nil
	return
}

func (bPT *bPlusTreeStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	nodeByteSlice, err = swiftclient.ObjectGet(bPT.volume.accountName, bPT.volume.checkpointContainerName, utils.Uint64ToHexStr(objectNumber), objectOffset, objectLength)
	return
}

func (bPT *bPlusTreeStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	if 0 == bPT.volume.checkpointObjectNumber {
		err = fmt.Errorf("*bPlusTreeStruct.PutNode() called without an assigned checkpointObjectNumber")
		return
	}

	if nil == bPT.volume.checkpointChunkedPutContext {
		bPT.volume.checkpointObjectOffset = 0
		bPT.volume.checkpointChunkedPutContext, err = swiftclient.ObjectFetchChunkedPutContext(bPT.volume.accountName, bPT.volume.checkpointContainerName, utils.Uint64ToHexStr(bPT.volume.checkpointObjectNumber))
		if nil != err {
			bPT.volume.checkpointObjectNumber = 0
			bPT.volume.checkpointObjectOffset = 0
			bPT.volume.checkpointChunkedPutContext = nil
			return
		}
	}

	err = bPT.volume.checkpointChunkedPutContext.SendChunk(nodeByteSlice)
	if nil != err {
		bPT.volume.checkpointObjectNumber = 0
		bPT.volume.checkpointObjectOffset = 0
		bPT.volume.checkpointChunkedPutContext = nil
		return
	}

	objectNumber = bPT.volume.checkpointObjectNumber
	objectOffset = bPT.volume.checkpointObjectOffset

	bPT.volume.checkpointObjectOffset += uint64(len(nodeByteSlice))

	if bPT.volume.checkpointObjectOffset > bPT.volume.maxFlushSize {
		// Object is at maximum size... so must flush it and start another one...

		err = bPT.volume.checkpointChunkedPutContext.Close()
		if nil != err {
			return
		}

		bPT.volume.checkpointObjectNumber = 0 // We'll hopefully be updating this shortly
		bPT.volume.checkpointObjectOffset = 0
		bPT.volume.checkpointChunkedPutContext = nil

		// Now provision a new Nonce if possible

		if bPT.volume.nextNonce == bPT.volume.reservedToNonce {
			// Oops - got unlucky... we'll have to abort this checkpoint
			err = fmt.Errorf("checkpoint overflowed object and there is no available Nonce... must abort this checkpoint")
			return
		}

		// Good - there is room for our next object
		bPT.volume.checkpointObjectNumber = bPT.volume.nextNonce
		bPT.volume.nextNonce++
	}

	return
}

func (bPT *bPlusTreeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = nil // Garbage collection handled by doing a "full" snapshot and deleting old LogSegments in checkpointDaemon()
	return
}

func (bPT *bPlusTreeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("*bPlusTreeStruct.PackKey(key == %v) failed to convert key to uint64", key)
		return
	}
	packedKey = utils.Uint64ToByteSlice(keyAsUint64)
	err = nil
	return
}

func (bPT *bPlusTreeStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("*bPlusTreeStruct.UnpackKey(payloadData) failed - len(payloadData) must be atleast 8 (was %v)", len(payloadData))
		return
	}
	keyAsUint64, ok := utils.ByteSliceToUint64(payloadData[:8])
	if !ok {
		err = fmt.Errorf("*bPlusTreeStruct.UnpackKey(payloadData) failed in call to utils.ByteSliceToUint64()")
		return
	}
	key = keyAsUint64
	bytesConsumed = 8
	err = nil
	return
}

func (bPT *bPlusTreeStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	valueAsByteSlice, ok := value.([]byte)
	if !ok {
		err = fmt.Errorf("*bPlusTreeStruct.PackValue() failed - value isn't a []byte")
		return
	}
	packedValue = utils.Uint64ToByteSlice(uint64(len(valueAsByteSlice)))
	packedValue = append(packedValue, valueAsByteSlice...)
	err = nil
	return
}

func (bPT *bPlusTreeStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("*bPlusTreeStruct.UnpackValue(payloadData) failed - len(payloadData) must be atleast 8 (was %v)", len(payloadData))
		return
	}
	valueSize, ok := utils.ByteSliceToUint64(payloadData[:8])
	if !ok {
		err = fmt.Errorf("*bPlusTreeStruct.UnpackValue(payloadData) failed in call to utils.ByteSliceToUint64()")
		return
	}
	valueAsByteSlice := make([]byte, valueSize)
	if 0 < valueSize {
		copy(valueAsByteSlice, payloadData[8:(8+valueSize)])
	}
	value = valueAsByteSlice
	bytesConsumed = 8 + valueSize
	err = nil
	return
}

func (volume *volumeStruct) FetchNextCheckPointDoneWaitGroup() (wg *sync.WaitGroup) {
	volume.Lock()
	if nil == volume.checkpointDoneWaitGroup {
		volume.checkpointDoneWaitGroup = &sync.WaitGroup{}
		volume.checkpointDoneWaitGroup.Add(1)
	}
	wg = volume.checkpointDoneWaitGroup
	volume.Unlock()
	return
}

func (volume *volumeStruct) FetchNextCheckPointGateWaitGroup() (wg *sync.WaitGroup) {
	volume.Lock()
	if nil == volume.checkpointGateWaitGroup {
		volume.checkpointGateWaitGroup = &sync.WaitGroup{}
		volume.checkpointGateWaitGroup.Add(1)
	}
	wg = volume.checkpointGateWaitGroup
	volume.Unlock()
	return
}

func (volume *volumeStruct) FetchNonce() (nonce uint64, err error) {
	volume.Lock()

	for volume.nextNonce == volume.reservedToNonce {
		// Do a checkpoint to provision for Nonce's
		volume.Unlock()
		checkpointRequest := &checkpointRequestStruct{exitOnCompletion: false}
		checkpointRequest.waitGroup.Add(1)
		volume.checkpointRequestChan <- checkpointRequest
		checkpointRequest.waitGroup.Wait()
		if nil != checkpointRequest.err {
			err = checkpointRequest.err
			return
		}
		volume.Lock()
	}

	nonce = volume.nextNonce
	volume.nextNonce++

	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) GetInodeRec(inodeNumber uint64) (value []byte, err error) {
	volume.Lock()
	valueAsValue, ok, err := volume.inodeRec.bPlusTree.GetByKey(inodeNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		err = fmt.Errorf("inodeNumber 0x%016X not found in volume \"%v\" inodeRec.bPlusTree", inodeNumber, volume.volumeName)
		return
	}
	valueFromTree := valueAsValue.([]byte)
	value = make([]byte, len(valueFromTree))
	copy(value, valueFromTree)
	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) PutInodeRec(inodeNumber uint64, value []byte) (err error) {
	valueToTree := make([]byte, len(value))
	copy(valueToTree, value)

	volume.Lock()
	ok, err := volume.inodeRec.bPlusTree.PatchByKey(inodeNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.inodeRec.bPlusTree.Put(inodeNumber, valueToTree)
		if nil != err {
			volume.Unlock()
			return
		}
	}
	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) PutInodeRecs(inodeNumbers []uint64, values [][]byte) (err error) {
	if len(inodeNumbers) != len(values) {
		err = fmt.Errorf("InodeNumber and Values array don't match")
		return
	}

	valuesToTree := make([][]byte, len(inodeNumbers))

	for i := range inodeNumbers {
		valuesToTree[i] = make([]byte, len(values[i]))
		copy(valuesToTree[i], values[i])
	}

	volume.Lock()
	for i, inodeNumber := range inodeNumbers {
		ok, nonShadowingErr := volume.inodeRec.bPlusTree.PatchByKey(inodeNumber, valuesToTree[i])
		if nil != nonShadowingErr {
			volume.Unlock()
			err = nonShadowingErr
			return
		}
		if !ok {
			_, err = volume.inodeRec.bPlusTree.Put(inodeNumber, valuesToTree[i])
			if nil != err {
				volume.Unlock()
				return
			}
		}
	}
	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) DeleteInodeRec(inodeNumber uint64) (err error) {
	volume.Lock()
	_, err = volume.inodeRec.bPlusTree.DeleteByKey(inodeNumber)
	volume.Unlock()

	return
}

func (volume *volumeStruct) GetLogSegmentRec(logSegmentNumber uint64) (value []byte, err error) {
	volume.Lock()
	valueAsValue, ok, err := volume.logSegmentRec.bPlusTree.GetByKey(logSegmentNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		err = fmt.Errorf("logSegmentNumber 0x%016X not found in volume \"%v\" logSegmentRec.bPlusTree", logSegmentNumber, volume.volumeName)
		return
	}
	valueFromTree := valueAsValue.([]byte)
	value = make([]byte, len(valueFromTree))
	copy(value, valueFromTree)
	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) PutLogSegmentRec(logSegmentNumber uint64, value []byte) (err error) {
	valueToTree := make([]byte, len(value))
	copy(valueToTree, value)

	volume.Lock()
	ok, err := volume.logSegmentRec.bPlusTree.PatchByKey(logSegmentNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.logSegmentRec.bPlusTree.Put(logSegmentNumber, valueToTree)
		if nil != err {
			volume.Unlock()
			return
		}
	}
	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) DeleteLogSegmentRec(logSegmentNumber uint64) (err error) {
	volume.Lock()
	_, err = volume.logSegmentRec.bPlusTree.DeleteByKey(logSegmentNumber)
	volume.Unlock()

	return
}

func (volume *volumeStruct) GetBPlusTreeObject(objectNumber uint64) (value []byte, err error) {
	volume.Lock()
	valueAsValue, ok, err := volume.bPlusTreeObject.bPlusTree.GetByKey(objectNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		err = fmt.Errorf("objectNumber 0x%016X not found in volume \"%v\" bPlusTreeObject.bPlusTree", objectNumber, volume.volumeName)
		return
	}
	valueFromTree := valueAsValue.([]byte)
	value = make([]byte, len(valueFromTree))
	copy(value, valueFromTree)
	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) PutBPlusTreeObject(objectNumber uint64, value []byte) (err error) {
	valueToTree := make([]byte, len(value))
	copy(valueToTree, value)

	volume.Lock()
	ok, err := volume.bPlusTreeObject.bPlusTree.PatchByKey(objectNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.bPlusTreeObject.bPlusTree.Put(objectNumber, valueToTree)
		if nil != err {
			volume.Unlock()
			return
		}
	}
	volume.Unlock()

	err = nil
	return
}

func (volume *volumeStruct) DeleteBPlusTreeObject(objectNumber uint64) (err error) {
	volume.Lock()
	_, err = volume.bPlusTreeObject.bPlusTree.DeleteByKey(objectNumber)
	volume.Unlock()

	return
}

func (volume *volumeStruct) DoCheckpoint() (err error) {
	var (
		checkpointRequest checkpointRequestStruct
	)

	checkpointRequest.exitOnCompletion = false

	checkpointRequest.waitGroup.Add(1)
	volume.checkpointRequestChan <- &checkpointRequest
	checkpointRequest.waitGroup.Wait()

	err = checkpointRequest.err

	return
}
