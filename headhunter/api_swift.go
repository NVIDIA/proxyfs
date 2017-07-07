package headhunter

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/swiftstack/conf"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

const (
	checkpointHeaderName = "X-Container-Meta-Checkpoint"
)

const (
	checkpointHeaderVersion1 uint64 = iota + 1
	// uint64 in %016X indicating checkpointHeaderVersion1
	// ' '
	// uint64 in %016X indicating objectNumber containing inodeRec.bPlusTree        Root Node
	// ' '
	// uint64 in %016X indicating offset of               inodeRec.bPlusTree        Root Node
	// ' '
	// uint64 in %016X indicating length of               inodeRec.bPlusTree        Root Node
	// ' '
	// uint64 in %016X indicating objectNumber containing logSegmentRec.bPlusTree   Root Node
	// ' '
	// uint64 in %016X indicating offset of               logSegmentRec.bPlusTree   Root Node
	// ' '
	// uint64 in %016X indicating length of               logSegmentRec.bPlusTree   Root Node
	// ' '
	// uint64 in %016X indicating objectNumber containing bPlusTreeObject.bPlusTree Root Node
	// ' '
	// uint64 in %016X indicating offset of               bPlusTreeObject.bPlusTree Root Node
	// ' '
	// uint64 in %016X indicating length of               bPlusTreeObject.bPlusTree Root Node
	// ' '
	// uint64 in %016X indicating reservedToNonce
)

type checkpointHeaderV1Struct struct {
	volume                               *swiftVolumeStruct
	inodeRecBPlusTreeObjectNumber        uint64
	inodeRecBPlusTreeObjectOffset        uint64
	inodeRecBPlusTreeObjectLength        uint64
	logSegmentRecBPlusTreeObjectNumber   uint64
	logSegmentRecBPlusTreeObjectOffset   uint64
	logSegmentRecBPlusTreeObjectLength   uint64
	bPlusTreeObjectBPlusTreeObjectNumber uint64
	bPlusTreeObjectBPlusTreeObjectOffset uint64
	bPlusTreeObjectBPlusTreeObjectLength uint64
	reservedToNonce                      uint64
}

const (
	inodeRecStructType uint32 = iota
	logSegmentRecStructType
	bPlusTreeObjectStructType
)

type bPlusTreeStruct struct {
	volume       *swiftVolumeStruct
	structType   uint32              // Either inodeRecStructType, logSegmentRecStructType, or bPlusTreeObjectStructType
	bPlusTree    sortedmap.BPlusTree // In memory representation
	objectNumber uint64              // If != 0, objectNumber-named Object in accountName.checkpointContainerName
	objectOffset uint64              // if objectNumber != 0, offset into the Object where Root Inode starts
	objectLength uint64              // If objectNumber != 0, length of Root Inode
}

type checkpointRequestStruct struct {
	waitGroup        sync.WaitGroup
	err              error
	exitOnCompletion bool
}

type swiftVolumeStruct struct {
	sync.Mutex
	volumeName                           string
	accountName                          string
	maxFlushSize                         uint64
	maxInodesPerMetadataNode             uint64
	maxLogSegmentsPerMetadataNode        uint64
	maxDirFileNodesPerMetadataNode       uint64
	checkpointContainerName              string
	checkpointInterval                   time.Duration
	checkpointIntervalsPerCompaction     uint64
	checkpointIntervalsSinceCompaction   uint64
	checkpointCompactorObjectNumberLimit uint64 //                  If == 0, compactor isn't running
	//                                                              If != 0, compactor should delete up to, but not including, this object number
	checkpointObjectNumber            uint64 //                     If == 0, no checkpointObject ChunkedPut is active
	checkpointObjectOffset            uint64 //                     If ChunkedPut is active, next available offset
	checkpointChunkedPutContext       swiftclient.ChunkedPutContext
	checkpointGateWaitGroup           *sync.WaitGroup
	checkpointDoneWaitGroup           *sync.WaitGroup
	needFullClone                     bool
	inodeRec                          *bPlusTreeStruct
	logSegmentRec                     *bPlusTreeStruct
	bPlusTreeObject                   *bPlusTreeStruct
	nonceValuesToReserve              uint16
	reservedToNonce                   uint64
	nextNonce                         uint64
	checkpointRequestChan             chan *checkpointRequestStruct
	lastSuccessfulCheckpointHeaderPut *checkpointHeaderV1Struct
}

type swiftGlobalsStruct struct {
	volumeMap map[string]*swiftVolumeStruct // key == ramVolumeStruct.volumeName
}

var swiftGlobals swiftGlobalsStruct

func (swiftGlobals *swiftGlobalsStruct) up(confMap conf.ConfMap) (err error) {
	var (
		checkpointHeader *checkpointHeaderV1Struct
		flowControlName  string
		volume           *swiftVolumeStruct
		volumeName       string
	)

	// Init volume database(s)

	swiftGlobals.volumeMap = make(map[string]*swiftVolumeStruct)

	for _, volumeName = range globals.volumeList {
		volume = &swiftVolumeStruct{volumeName: volumeName, checkpointIntervalsSinceCompaction: 0, checkpointCompactorObjectNumberLimit: 0, checkpointObjectNumber: 0, checkpointGateWaitGroup: nil, checkpointDoneWaitGroup: nil, needFullClone: false}

		volume.inodeRec = &bPlusTreeStruct{volume: volume, structType: inodeRecStructType}
		volume.logSegmentRec = &bPlusTreeStruct{volume: volume, structType: logSegmentRecStructType}
		volume.bPlusTreeObject = &bPlusTreeStruct{volume: volume, structType: bPlusTreeObjectStructType}

		volume.accountName, err = confMap.FetchOptionValueString(volumeName, "AccountName")
		if nil != err {
			return
		}

		flowControlName, err = confMap.FetchOptionValueString(volumeName, "FlowControl")
		if nil != err {
			return
		}

		volume.maxFlushSize, err = confMap.FetchOptionValueUint64(flowControlName, "MaxFlushSize")
		if nil != err {
			return
		}

		volume.nonceValuesToReserve, err = confMap.FetchOptionValueUint16(volumeName, "NonceValuesToReserve")
		if nil != err {
			return
		}

		volume.maxInodesPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeName, "MaxInodesPerMetadataNode")
		if nil != err {
			// TODO: eventually, just return
			volume.maxInodesPerMetadataNode = 32
		}

		volume.maxLogSegmentsPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeName, "MaxLogSegmentsPerMetadataNode")
		if nil != err {
			// TODO: eventually, just return
			volume.maxLogSegmentsPerMetadataNode = 64
		}

		volume.maxDirFileNodesPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeName, "MaxDirFileNodesPerMetadataNode")
		if nil != err {
			// TODO: eventually, just return
			volume.maxDirFileNodesPerMetadataNode = 16
		}

		volume.checkpointContainerName, err = confMap.FetchOptionValueString(volumeName, "CheckpointContainerName")
		if nil != err {
			return
		}

		volume.checkpointInterval, err = confMap.FetchOptionValueDuration(volumeName, "CheckpointInterval")
		if nil != err {
			return
		}

		volume.checkpointIntervalsPerCompaction, err = confMap.FetchOptionValueUint64(volumeName, "CheckpointIntervalsPerCompaction")
		if nil != err {
			return
		}

		checkpointHeader = &checkpointHeaderV1Struct{volume: volume}

		err = checkpointHeader.get()
		if nil != err {
			return
		}

		volume.lastSuccessfulCheckpointHeaderPut = checkpointHeader

		volume.inodeRec.objectNumber = checkpointHeader.inodeRecBPlusTreeObjectNumber
		volume.inodeRec.objectOffset = checkpointHeader.inodeRecBPlusTreeObjectOffset
		volume.inodeRec.objectLength = checkpointHeader.inodeRecBPlusTreeObjectLength

		volume.logSegmentRec.objectNumber = checkpointHeader.logSegmentRecBPlusTreeObjectNumber
		volume.logSegmentRec.objectOffset = checkpointHeader.logSegmentRecBPlusTreeObjectOffset
		volume.logSegmentRec.objectLength = checkpointHeader.logSegmentRecBPlusTreeObjectLength

		volume.bPlusTreeObject.objectNumber = checkpointHeader.bPlusTreeObjectBPlusTreeObjectNumber
		volume.bPlusTreeObject.objectOffset = checkpointHeader.bPlusTreeObjectBPlusTreeObjectOffset
		volume.bPlusTreeObject.objectLength = checkpointHeader.bPlusTreeObjectBPlusTreeObjectLength

		volume.reservedToNonce = checkpointHeader.reservedToNonce

		volume.nextNonce = volume.reservedToNonce // This will trigger a Nonce pool population on next FetchNonce() call

		if 0 == volume.inodeRec.objectNumber {
			volume.inodeRec.bPlusTree = sortedmap.NewBPlusTree(volume.maxInodesPerMetadataNode, sortedmap.CompareUint64, volume.inodeRec)
		} else {
			volume.inodeRec.bPlusTree, err = sortedmap.OldBPlusTree(volume.inodeRec.objectNumber, volume.inodeRec.objectOffset, volume.inodeRec.objectLength, sortedmap.CompareUint64, volume.inodeRec)
			if nil != err {
				return
			}
		}
		if 0 == volume.logSegmentRec.objectNumber {
			volume.logSegmentRec.bPlusTree = sortedmap.NewBPlusTree(volume.maxLogSegmentsPerMetadataNode, sortedmap.CompareUint64, volume.logSegmentRec)
		} else {
			volume.logSegmentRec.bPlusTree, err = sortedmap.OldBPlusTree(volume.logSegmentRec.objectNumber, volume.logSegmentRec.objectOffset, volume.logSegmentRec.objectLength, sortedmap.CompareUint64, volume.logSegmentRec)
			if nil != err {
				return
			}
		}
		if 0 == volume.bPlusTreeObject.objectNumber {
			volume.bPlusTreeObject.bPlusTree = sortedmap.NewBPlusTree(volume.maxDirFileNodesPerMetadataNode, sortedmap.CompareUint64, volume.bPlusTreeObject)
		} else {
			volume.bPlusTreeObject.bPlusTree, err = sortedmap.OldBPlusTree(volume.bPlusTreeObject.objectNumber, volume.bPlusTreeObject.objectOffset, volume.bPlusTreeObject.objectLength, sortedmap.CompareUint64, volume.bPlusTreeObject)
			if nil != err {
				return
			}
		}

		volume.checkpointRequestChan = make(chan *checkpointRequestStruct, 1)

		swiftGlobals.volumeMap[volumeName] = volume

		go volume.checkpointDaemon()
	}

	return
}

func (swiftGlobals *swiftGlobalsStruct) down() (err error) {
	var (
		checkpointRequest checkpointRequestStruct
		volume            *swiftVolumeStruct
	)

	// Do final checkpoint on every Volume

	checkpointRequest.exitOnCompletion = true

	for _, volume = range swiftGlobals.volumeMap {
		checkpointRequest.waitGroup.Add(1)
		volume.checkpointRequestChan <- &checkpointRequest
		checkpointRequest.waitGroup.Wait()
		if nil != checkpointRequest.err {
			err = checkpointRequest.err
			return
		}
	}

	// All done - successfully exit

	err = nil
	return
}

func (checkpointHeader *checkpointHeaderV1Struct) get() (err error) {
	var (
		checkpointContainerHeaders map[string][]string
		checkpointHeaderValueSlice []string
		checkpointHeaderValues     []string
		checkpointVersion          uint64
		ok                         bool
	)

	checkpointContainerHeaders, err = swiftclient.ContainerHead(checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName)
	if nil != err {
		if 404 == blunder.HTTPCode(err) {
			// Checkpoint Container not found... so try to create it...
			checkpointContainerHeaders = make(map[string][]string)
			err = swiftclient.ContainerPut(checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointContainerHeaders)
			if nil != err {
				return
			}
		} else {
			// If Checkpoint Container HEAD failed for some other reason, we must exit before doing any damage
			return
		}
	}

	checkpointHeaderValues, ok = checkpointContainerHeaders[checkpointHeaderName]

	if ok {
		if 1 != len(checkpointHeaderValues) {
			err = fmt.Errorf("Expected one single value for %v/%v header %v", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName)
			return
		}

		checkpointHeaderValueSlice = strings.Split(checkpointHeaderValues[0], " ")

		if 1 > len(checkpointHeaderValueSlice) {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointVersion, err = strconv.ParseUint(checkpointHeaderValueSlice[0], 16, 64)
		if nil != err {
			return
		}

		if checkpointHeaderVersion1 != checkpointVersion {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (version: %v not supported)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0], checkpointVersion)
			return
		}

		if 11 != len(checkpointHeaderValueSlice) {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (wrong number of fields)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.inodeRecBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[1], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectNumber)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.inodeRecBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[2], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectOffset)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.inodeRecBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[3], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectLength)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.logSegmentRecBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[4], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectNumber)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.logSegmentRecBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[5], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectOffset)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.logSegmentRecBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[6], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectLength)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.bPlusTreeObjectBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[7], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectNumber)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.bPlusTreeObjectBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[8], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectOffset)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.bPlusTreeObjectBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[9], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectLength)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}

		checkpointHeader.reservedToNonce, err = strconv.ParseUint(checkpointHeaderValueSlice[10], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad nextNonce)", checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValues[0])
			return
		}
	} else { // !ok: Missing checkpointContainerHeaders[checkpointHeaderName], so return defaults
		checkpointHeader.inodeRecBPlusTreeObjectNumber = 0
		checkpointHeader.inodeRecBPlusTreeObjectOffset = 0
		checkpointHeader.inodeRecBPlusTreeObjectLength = 0

		checkpointHeader.logSegmentRecBPlusTreeObjectNumber = 0
		checkpointHeader.logSegmentRecBPlusTreeObjectOffset = 0
		checkpointHeader.logSegmentRecBPlusTreeObjectLength = 0

		checkpointHeader.bPlusTreeObjectBPlusTreeObjectNumber = 0
		checkpointHeader.bPlusTreeObjectBPlusTreeObjectOffset = 0
		checkpointHeader.bPlusTreeObjectBPlusTreeObjectLength = 0

		checkpointHeader.reservedToNonce = firstNonceToProvide - 1
	}

	err = nil
	return
}

func (checkpointHeader *checkpointHeaderV1Struct) put() (err error) {
	var (
		checkpointHeaderValue      string
		checkpointHeaderValues     []string
		checkpointContainerHeaders map[string][]string
	)

	checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X %016X %016X %016X %016X %016X %016X %016X",
		checkpointHeaderVersion1,
		checkpointHeader.inodeRecBPlusTreeObjectNumber,
		checkpointHeader.inodeRecBPlusTreeObjectOffset,
		checkpointHeader.inodeRecBPlusTreeObjectLength,
		checkpointHeader.logSegmentRecBPlusTreeObjectNumber,
		checkpointHeader.logSegmentRecBPlusTreeObjectOffset,
		checkpointHeader.logSegmentRecBPlusTreeObjectLength,
		checkpointHeader.bPlusTreeObjectBPlusTreeObjectNumber,
		checkpointHeader.bPlusTreeObjectBPlusTreeObjectOffset,
		checkpointHeader.bPlusTreeObjectBPlusTreeObjectLength,
		checkpointHeader.reservedToNonce,
	)

	checkpointHeaderValues = []string{checkpointHeaderValue}

	checkpointContainerHeaders = make(map[string][]string)

	checkpointContainerHeaders[checkpointHeaderName] = checkpointHeaderValues

	err = swiftclient.ContainerPost(checkpointHeader.volume.accountName, checkpointHeader.volume.checkpointContainerName, checkpointContainerHeaders)

	return
}

func (swiftGlobals *swiftGlobalsStruct) fetchVolumeHandle(volumeName string) (volumeHandle VolumeHandle, err error) {
	volume, ok := swiftGlobals.volumeMap[volumeName]
	if !ok {
		err = fmt.Errorf("FetchVolumeHandle(\"%v\") unable to find volume", volumeName)
		return
	}

	volumeHandle = volume
	err = nil

	return
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
func (volume *swiftVolumeStruct) checkpointDaemon() {
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

func (volume *swiftVolumeStruct) checkpointCompactor() {
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

func (volume *swiftVolumeStruct) FetchNextCheckPointDoneWaitGroup() (wg *sync.WaitGroup) {
	volume.Lock()
	if nil == volume.checkpointDoneWaitGroup {
		volume.checkpointDoneWaitGroup = &sync.WaitGroup{}
		volume.checkpointDoneWaitGroup.Add(1)
	}
	wg = volume.checkpointDoneWaitGroup
	volume.Unlock()
	return
}

func (volume *swiftVolumeStruct) FetchNextCheckPointGateWaitGroup() (wg *sync.WaitGroup) {
	volume.Lock()
	if nil == volume.checkpointGateWaitGroup {
		volume.checkpointGateWaitGroup = &sync.WaitGroup{}
		volume.checkpointGateWaitGroup.Add(1)
	}
	wg = volume.checkpointGateWaitGroup
	volume.Unlock()
	return
}

func (volume *swiftVolumeStruct) FetchNonce() (nonce uint64, err error) {
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

func (volume *swiftVolumeStruct) GetInodeRec(inodeNumber uint64) (value []byte, err error) {
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

func (volume *swiftVolumeStruct) PutInodeRec(inodeNumber uint64, value []byte) (err error) {
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

func (volume *swiftVolumeStruct) PutInodeRecs(inodeNumbers []uint64, values [][]byte) (err error) {
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

func (volume *swiftVolumeStruct) DeleteInodeRec(inodeNumber uint64) (err error) {
	volume.Lock()
	_, err = volume.inodeRec.bPlusTree.DeleteByKey(inodeNumber)
	volume.Unlock()

	return
}

func (volume *swiftVolumeStruct) GetLogSegmentRec(logSegmentNumber uint64) (value []byte, err error) {
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

func (volume *swiftVolumeStruct) PutLogSegmentRec(logSegmentNumber uint64, value []byte) (err error) {
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

func (volume *swiftVolumeStruct) DeleteLogSegmentRec(logSegmentNumber uint64) (err error) {
	volume.Lock()
	_, err = volume.logSegmentRec.bPlusTree.DeleteByKey(logSegmentNumber)
	volume.Unlock()

	return
}

func (volume *swiftVolumeStruct) GetBPlusTreeObject(objectNumber uint64) (value []byte, err error) {
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

func (volume *swiftVolumeStruct) PutBPlusTreeObject(objectNumber uint64, value []byte) (err error) {
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

func (volume *swiftVolumeStruct) DeleteBPlusTreeObject(objectNumber uint64) (err error) {
	volume.Lock()
	_, err = volume.bPlusTreeObject.bPlusTree.DeleteByKey(objectNumber)
	volume.Unlock()

	return
}

func (volume *swiftVolumeStruct) DoCheckpoint() (err error) {
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
