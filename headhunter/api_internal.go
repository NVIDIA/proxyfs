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
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

func (volume *volumeStruct) getCheckpoint() (err error) {
	var (
		bPTW                                *bPlusTreeWrapper
		bytesConsumed                       uint64
		checkpointContainerHeaders          map[string][]string
		checkpointHeader                    checkpointHeaderV2Struct
		checkpointHeaderValue               string
		checkpointHeaderValueSlice          []string
		checkpointHeaderValues              []string
		checkpointObjectTrailerBuf          []byte
		checkpointVersion                   uint64
		elementOfBPlusTreeLayout            elementOfBPlusTreeLayoutStruct
		expectedCheckpointObjectTrailerSize uint64
		layoutReportIndex                   uint64
		ok                                  bool
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

		volume.checkpointHeaderVersion = checkpointHeaderVersion1

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

		// Compute volume.{inodeRec|logSegmentRec|bPlusTreeObject}BPlusTreeLayout LayoutReports

		if 0 == volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectNumber {
			volume.inodeRecBPlusTreeLayout = make(sortedmap.LayoutReport)
			volume.checkpointObjectTrailer.inodeRecBPlusTreeLayoutNumElements = 0
		} else {
			bPTW = &bPlusTreeWrapper{
				volume:      volume,
				wrapperType: inodeRecBPlusTreeWrapperType,
			}
			bPTW.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					bPTW)
			if nil != err {
				return
			}
			volume.inodeRecBPlusTreeLayout, err = bPTW.bPlusTree.FetchLayoutReport()
			if nil != err {
				return
			}
			volume.checkpointObjectTrailer.inodeRecBPlusTreeLayoutNumElements = uint64(len(volume.inodeRecBPlusTreeLayout))
		}

		if 0 == volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectNumber {
			volume.logSegmentRecBPlusTreeLayout = make(sortedmap.LayoutReport)
			volume.checkpointObjectTrailer.logSegmentRecBPlusTreeLayoutNumElements = 0
		} else {
			bPTW = &bPlusTreeWrapper{
				volume:      volume,
				wrapperType: logSegmentRecBPlusTreeWrapperType,
			}
			bPTW.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					bPTW)
			if nil != err {
				return
			}
			volume.logSegmentRecBPlusTreeLayout, err = bPTW.bPlusTree.FetchLayoutReport()
			if nil != err {
				return
			}
			volume.checkpointObjectTrailer.logSegmentRecBPlusTreeLayoutNumElements = uint64(len(volume.logSegmentRecBPlusTreeLayout))
		}

		if 0 == volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectNumber {
			volume.bPlusTreeObjectBPlusTreeLayout = make(sortedmap.LayoutReport)
			volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeLayoutNumElements = 0
		} else {
			bPTW = &bPlusTreeWrapper{
				volume:      volume,
				wrapperType: bPlusTreeObjectBPlusTreeWrapperType,
			}
			bPTW.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					bPTW)
			if nil != err {
				return
			}
			volume.bPlusTreeObjectBPlusTreeLayout, err = bPTW.bPlusTree.FetchLayoutReport()
			if nil != err {
				return
			}
			volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeLayoutNumElements = uint64(len(volume.bPlusTreeObjectBPlusTreeLayout))
		}
	} else if checkpointHeaderVersion2 == checkpointVersion {
		// Read in checkpointHeaderV2Struct

		volume.checkpointHeaderVersion = checkpointHeaderVersion2

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

		// Read in checkpointObjectTrailerV2Struct
		checkpointObjectTrailerBuf, err =
			swiftclient.ObjectTail(
				volume.accountName,
				volume.checkpointContainerName,
				utils.Uint64ToHexStr(volume.checkpointHeader.checkpointObjectTrailerV2StructObjectNumber),
				volume.checkpointHeader.checkpointObjectTrailerV2StructObjectLength)
		if nil != err {
			return
		}

		volume.checkpointObjectTrailer = &checkpointObjectTrailerV2Struct{}

		bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, volume.checkpointObjectTrailer, LittleEndian)
		if nil != err {
			return
		}

		// Deserialize volume.{inodeRec|logSegmentRec|bPlusTreeObject}BPlusTreeLayout LayoutReports

		expectedCheckpointObjectTrailerSize = volume.checkpointObjectTrailer.inodeRecBPlusTreeLayoutNumElements
		expectedCheckpointObjectTrailerSize += volume.checkpointObjectTrailer.logSegmentRecBPlusTreeLayoutNumElements
		expectedCheckpointObjectTrailerSize += volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeLayoutNumElements
		expectedCheckpointObjectTrailerSize *= globals.elementOfBPlusTreeLayoutStructSize
		expectedCheckpointObjectTrailerSize += bytesConsumed

		if uint64(len(checkpointObjectTrailerBuf)) != expectedCheckpointObjectTrailerSize {
			err = fmt.Errorf("volume.checkpointObjectTrailer for volume %v does not match required size", volume.volumeName)
			return
		}

		volume.inodeRecBPlusTreeLayout = make(sortedmap.LayoutReport)

		for layoutReportIndex = 0; layoutReportIndex < volume.checkpointObjectTrailer.inodeRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
			checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
			bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
			if nil != err {
				return
			}

			volume.inodeRecBPlusTreeLayout[elementOfBPlusTreeLayout.objectNumber] = elementOfBPlusTreeLayout.objectBytes
		}

		volume.logSegmentRecBPlusTreeLayout = make(sortedmap.LayoutReport)

		for layoutReportIndex = 0; layoutReportIndex < volume.checkpointObjectTrailer.logSegmentRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
			checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
			bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
			if nil != err {
				return
			}

			volume.logSegmentRecBPlusTreeLayout[elementOfBPlusTreeLayout.objectNumber] = elementOfBPlusTreeLayout.objectBytes
		}

		volume.bPlusTreeObjectBPlusTreeLayout = make(sortedmap.LayoutReport)

		for layoutReportIndex = 0; layoutReportIndex < volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeLayoutNumElements; layoutReportIndex++ {
			checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
			bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
			if nil != err {
				return
			}

			volume.bPlusTreeObjectBPlusTreeLayout[elementOfBPlusTreeLayout.objectNumber] = elementOfBPlusTreeLayout.objectBytes
		}
	} else {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (version: %v not supported)", volume.accountName, volume.checkpointContainerName, checkpointHeaderName, checkpointHeaderValue, checkpointVersion)
		return
	}

	err = nil
	return
}

func (volume *volumeStruct) putCheckpoint(withFlush bool) (err error) {
	var (
		checkpointContainerHeaders             map[string][]string
		checkpointHeaderValue                  string
		checkpointHeaderValues                 []string
		checkpointObjectTrailerBeginningOffset uint64
		checkpointObjectTrailerEndingOffset    uint64
		checkpointTrailerBuf                   []byte
		elementOfBPlusTreeLayout               elementOfBPlusTreeLayoutStruct
		elementOfBPlusTreeLayoutBuf            []byte
		layoutReportIndex                      uint64
		treeLayoutBuf                          []byte
		treeLayoutBufSize                      uint64
	)

	if withFlush {
		volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectNumber,
			volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectOffset,
			volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectLength,
			_, err = volume.inodeRec.bPlusTree.Flush(false)
		if nil != err {
			return
		}
		volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectNumber,
			volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectOffset,
			volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectLength,
			_, err = volume.logSegmentRec.bPlusTree.Flush(false)
		if nil != err {
			return
		}
		volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectNumber,
			volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectOffset,
			volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectLength,
			_, err = volume.inodeRec.bPlusTree.Flush(false)
		if nil != err {
			return
		}
	}

	volume.checkpointObjectTrailer.inodeRecBPlusTreeLayoutNumElements = uint64(len(volume.inodeRecBPlusTreeLayout))
	volume.checkpointObjectTrailer.logSegmentRecBPlusTreeLayoutNumElements = uint64(len(volume.logSegmentRecBPlusTreeLayout))
	volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeLayoutNumElements = uint64(len(volume.bPlusTreeObjectBPlusTreeLayout))

	checkpointTrailerBuf, err = cstruct.Pack(volume.checkpointObjectTrailer, LittleEndian)
	if nil != err {
		return
	}

	treeLayoutBufSize = volume.checkpointObjectTrailer.inodeRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += volume.checkpointObjectTrailer.logSegmentRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeLayoutNumElements
	treeLayoutBufSize *= globals.elementOfBPlusTreeLayoutStructSize

	treeLayoutBuf = make([]byte, 0, treeLayoutBufSize)

	for elementOfBPlusTreeLayout.objectNumber, elementOfBPlusTreeLayout.objectBytes = range volume.inodeRecBPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.objectNumber, elementOfBPlusTreeLayout.objectBytes = range volume.logSegmentRecBPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.objectNumber, elementOfBPlusTreeLayout.objectBytes = range volume.bPlusTreeObjectBPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	err = volume.openCheckpointChunkedPutContextIfNecessary()
	if nil != err {
		return
	}

	checkpointObjectTrailerBeginningOffset, err = volume.bytesPutToCheckpointChunkedPutContext()
	if nil != err {
		return
	}

	err = volume.sendChunkToCheckpointChunkedPutContext(checkpointTrailerBuf)
	if nil != err {
		return
	}

	err = volume.sendChunkToCheckpointChunkedPutContext(treeLayoutBuf)
	if nil != err {
		return
	}

	checkpointObjectTrailerEndingOffset, err = volume.bytesPutToCheckpointChunkedPutContext()
	if nil != err {
		return
	}

	err = volume.closeCheckpointChunkedPutContext()
	if nil != err {
		return
	}

	volume.checkpointHeader.checkpointObjectTrailerV2StructObjectLength = checkpointObjectTrailerEndingOffset - checkpointObjectTrailerBeginningOffset

	checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
		checkpointHeaderVersion2,
		volume.checkpointHeader.checkpointObjectTrailerV2StructObjectNumber,
		volume.checkpointHeader.checkpointObjectTrailerV2StructObjectLength,
		volume.checkpointHeader.reservedToNonce,
	)

	checkpointHeaderValues = []string{checkpointHeaderValue}

	checkpointContainerHeaders = make(map[string][]string)

	checkpointContainerHeaders[checkpointHeaderName] = checkpointHeaderValues

	err = swiftclient.ContainerPost(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)

	return // err set as appropriate
}

func (volume *volumeStruct) openCheckpointChunkedPutContextIfNecessary() (err error) {
	if nil == volume.checkpointChunkedPutContext {
		volume.checkpointHeader.checkpointObjectTrailerV2StructObjectNumber, err = volume.fetchNonceWhileLocked()
		if nil != err {
			return
		}
		volume.checkpointChunkedPutContext, err =
			swiftclient.ObjectFetchChunkedPutContext(volume.accountName,
				volume.checkpointContainerName,
				utils.Uint64ToHexStr(volume.checkpointHeader.checkpointObjectTrailerV2StructObjectNumber))
		if nil != err {
			return
		}
	}
	err = nil
	return
}

func (volume *volumeStruct) bytesPutToCheckpointChunkedPutContext() (bytesPut uint64, err error) {
	if nil == volume.checkpointChunkedPutContext {
		err = fmt.Errorf("bytesPutToCheckpointChunkedPutContext() called while volume.checkpointChunkedPutContext == nil")
	} else {
		bytesPut, err = volume.checkpointChunkedPutContext.BytesPut()
	}
	return // err set as appropriate regardless of path
}

func (volume *volumeStruct) sendChunkToCheckpointChunkedPutContext(buf []byte) (err error) {
	if nil == volume.checkpointChunkedPutContext {
		err = fmt.Errorf("sendChunkToCheckpointChunkedPutContext() called while volume.checkpointChunkedPutContext == nil")
	} else {
		err = volume.checkpointChunkedPutContext.SendChunk(buf)
	}
	return // err set as appropriate regardless of path
}

func (volume *volumeStruct) closeCheckpointChunkedPutContextIfNecessary() (err error) {
	var (
		bytesPut uint64
	)

	if nil == volume.checkpointChunkedPutContext {
		err = nil
	} else {
		bytesPut, err = volume.checkpointChunkedPutContext.BytesPut()
		if nil == err {
			if bytesPut >= volume.maxFlushSize {
				err = volume.checkpointChunkedPutContext.Close()
			}
		}
	}
	return // err set as appropriate regardless of path
}

func (volume *volumeStruct) closeCheckpointChunkedPutContext() (err error) {
	if nil == volume.checkpointChunkedPutContext {
		err = fmt.Errorf("closeCheckpointChunkedPutContext() called while volume.checkpointChunkedPutContext == nil")
	} else {
		err = volume.checkpointChunkedPutContext.Close()
		if nil == err {
			volume.checkpointChunkedPutContext = nil
		}
	}
	return // err set as appropriate regardless of path
}

// checkpointDaemon periodically and upon request persists a checkpoint/snapshot.
func (volume *volumeStruct) checkpointDaemon() {
	var (
		checkpointRequest *checkpointRequestStruct
		exitOnCompletion  bool
	)

	for {
		select {
		case checkpointRequest = <-volume.checkpointRequestChan:
			// Explicitly requested checkpoint... use it below
		case <-time.After(volume.checkpointInterval):
			// Time to automatically do a checkpoint... so dummy up a checkpointRequest
			checkpointRequest = &checkpointRequestStruct{exitOnCompletion: false}
			checkpointRequest.waitGroup.Add(1) // ...even though we won't be waiting on it...
		}

		volume.Lock()

		checkpointRequest.err = volume.putCheckpoint(true)

		checkpointRequest.waitGroup.Done() // Awake the checkpoint requestor
		if nil != volume.checkpointDoneWaitGroup {
			// Awake any others who were waiting on this checkpoint
			volume.checkpointDoneWaitGroup.Done()
		}

		exitOnCompletion = checkpointRequest.exitOnCompletion // In case requestor re-uses checkpointRequest

		volume.Unlock()

		if exitOnCompletion {
			return
		}
	}
}

func (bPTW *bPlusTreeWrapper) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("headhunter.bPlusTreeWrapper.DumpKey() could not parse key as a uint64")
		return
	}

	keyAsString = fmt.Sprintf("0x%016X", keyAsUint64)

	err = nil
	return
}

func (bPTW *bPlusTreeWrapper) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsByteSlice, ok := value.([]byte)
	if !ok {
		err = fmt.Errorf("headhunter.bPlusTreeWrapper.DumpValue() count not parse value as []byte")
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

func (bPTW *bPlusTreeWrapper) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	nodeByteSlice, err = swiftclient.ObjectGet(bPTW.volume.accountName, bPTW.volume.checkpointContainerName, utils.Uint64ToHexStr(objectNumber), objectOffset, objectLength)
	return
}

func (bPTW *bPlusTreeWrapper) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	var (
		bytesUsed uint64
		ok        bool
	)

	err = bPTW.volume.openCheckpointChunkedPutContextIfNecessary()
	if nil != err {
		return
	}

	objectNumber = bPTW.volume.checkpointHeader.checkpointObjectTrailerV2StructObjectNumber

	objectOffset, err = bPTW.volume.bytesPutToCheckpointChunkedPutContext()
	if nil != err {
		return
	}

	err = bPTW.volume.sendChunkToCheckpointChunkedPutContext(nodeByteSlice)
	if nil != err {
		return
	}

	switch bPTW.wrapperType {
	case inodeRecBPlusTreeWrapperType:
		bytesUsed, ok = bPTW.volume.inodeRecBPlusTreeLayout[objectNumber]
		if ok {
			bPTW.volume.inodeRecBPlusTreeLayout[objectNumber] = bytesUsed + uint64(len(nodeByteSlice))
		} else {
			bPTW.volume.inodeRecBPlusTreeLayout[objectNumber] = uint64(len(nodeByteSlice))
		}
	case logSegmentRecBPlusTreeWrapperType:
		bytesUsed, ok = bPTW.volume.logSegmentRecBPlusTreeLayout[objectNumber]
		if ok {
			bPTW.volume.logSegmentRecBPlusTreeLayout[objectNumber] = bytesUsed + uint64(len(nodeByteSlice))
		} else {
			bPTW.volume.logSegmentRecBPlusTreeLayout[objectNumber] = uint64(len(nodeByteSlice))
		}
	case bPlusTreeObjectBPlusTreeWrapperType:
		bytesUsed, ok = bPTW.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber]
		if ok {
			bPTW.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber] = bytesUsed + uint64(len(nodeByteSlice))
		} else {
			bPTW.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber] = uint64(len(nodeByteSlice))
		}
	default:
		err = fmt.Errorf("Logic error: bPTW.PutNode() called for invalid wrapperType: %v", bPTW.wrapperType)
		panic(err)
	}

	err = bPTW.volume.closeCheckpointChunkedPutContextIfNecessary()

	return // err set as appropriate
}

func (bPTW *bPlusTreeWrapper) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	var (
		bytesUsed uint64
		ok        bool
	)

	switch bPTW.wrapperType {
	case inodeRecBPlusTreeWrapperType:
		bytesUsed, ok = bPTW.volume.inodeRecBPlusTreeLayout[objectNumber]
		if ok {
			if bytesUsed < objectLength {
				err = fmt.Errorf("Logic error: [inodeRecBPlusTreeWrapperType] bPTW.DiscardNode() called to dereference too many bytes in objectNumber 0x%016X", objectNumber)
			} else {
				err = nil
				if bytesUsed == 0 {
					delete(bPTW.volume.inodeRecBPlusTreeLayout, objectNumber)
					swiftclient.ObjectDeleteAsync(
						bPTW.volume.accountName,
						bPTW.volume.checkpointContainerName,
						utils.Uint64ToHexStr(objectNumber),
						bPTW.volume.fetchNextCheckPointDoneWaitGroupWhileLocked(),
						nil)
				} else {
					bPTW.volume.inodeRecBPlusTreeLayout[objectNumber] = bytesUsed - objectLength
				}
			}
		} else {
			err = fmt.Errorf("Logic error: [inodeRecBPlusTreeWrapperType] bPTW.DiscardNode() called referencing invalid objectNumber: 0x%016X", objectNumber)
		}
	case logSegmentRecBPlusTreeWrapperType:
		bytesUsed, ok = bPTW.volume.logSegmentRecBPlusTreeLayout[objectNumber]
		if ok {
			if bytesUsed < objectLength {
				err = fmt.Errorf("Logic error: [logSegmentRecBPlusTreeWrapperType] bPTW.DiscardNode() called to dereference too many bytes in objectNumber 0x%016X", objectNumber)
			} else {
				err = nil
				if bytesUsed == 0 {
					delete(bPTW.volume.logSegmentRecBPlusTreeLayout, objectNumber)
					swiftclient.ObjectDeleteAsync(
						bPTW.volume.accountName,
						bPTW.volume.checkpointContainerName,
						utils.Uint64ToHexStr(objectNumber),
						bPTW.volume.fetchNextCheckPointDoneWaitGroupWhileLocked(),
						nil)
				} else {
					bPTW.volume.logSegmentRecBPlusTreeLayout[objectNumber] = bytesUsed - objectLength
				}
			}
		} else {
			err = fmt.Errorf("Logic error: [logSegmentRecBPlusTreeWrapperType] bPTW.DiscardNode() called referencing invalid objectNumber: 0x%016X", objectNumber)
		}
	case bPlusTreeObjectBPlusTreeWrapperType:
		bytesUsed, ok = bPTW.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber]
		if ok {
			if bytesUsed < objectLength {
				err = fmt.Errorf("Logic error: [bPlusTreeObjectBPlusTreeWrapperType] bPTW.DiscardNode() called to dereference too many bytes in objectNumber 0x%016X", objectNumber)
			} else {
				err = nil
				if bytesUsed == 0 {
					delete(bPTW.volume.bPlusTreeObjectBPlusTreeLayout, objectNumber)
					swiftclient.ObjectDeleteAsync(
						bPTW.volume.accountName,
						bPTW.volume.checkpointContainerName,
						utils.Uint64ToHexStr(objectNumber),
						bPTW.volume.fetchNextCheckPointDoneWaitGroupWhileLocked(),
						nil)
				} else {
					bPTW.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber] = bytesUsed - objectLength
				}
			}
		} else {
			err = fmt.Errorf("Logic error: [bPlusTreeObjectBPlusTreeWrapperType] bPTW.DiscardNode() called referencing invalid objectNumber: 0x%016X", objectNumber)
		}
	default:
		err = fmt.Errorf("Logic error: bPTW.DiscardNode() called for invalid wrapperType: %v", bPTW.wrapperType)
	}

	return // err set as appropriate regardless of path
}

func (bPTW *bPlusTreeWrapper) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.PackKey(key == %v) failed to convert key to uint64", key)
		return
	}
	packedKey = utils.Uint64ToByteSlice(keyAsUint64)
	err = nil
	return
}

func (bPTW *bPlusTreeWrapper) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("*bPlusTreeWrapper.UnpackKey(payloadData) failed - len(payloadData) must be atleast 8 (was %v)", len(payloadData))
		return
	}
	keyAsUint64, ok := utils.ByteSliceToUint64(payloadData[:8])
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.UnpackKey(payloadData) failed in call to utils.ByteSliceToUint64()")
		return
	}
	key = keyAsUint64
	bytesConsumed = 8
	err = nil
	return
}

func (bPTW *bPlusTreeWrapper) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	valueAsByteSlice, ok := value.([]byte)
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.PackValue() failed - value isn't a []byte")
		return
	}
	packedValue = utils.Uint64ToByteSlice(uint64(len(valueAsByteSlice)))
	packedValue = append(packedValue, valueAsByteSlice...)
	err = nil
	return
}

func (bPTW *bPlusTreeWrapper) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("*bPlusTreeWrapper.UnpackValue(payloadData) failed - len(payloadData) must be atleast 8 (was %v)", len(payloadData))
		return
	}
	valueSize, ok := utils.ByteSliceToUint64(payloadData[:8])
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.UnpackValue(payloadData) failed in call to utils.ByteSliceToUint64()")
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

func (volume *volumeStruct) fetchNextCheckPointDoneWaitGroupWhileLocked() (wg *sync.WaitGroup) {
	if nil == volume.checkpointDoneWaitGroup {
		volume.checkpointDoneWaitGroup = &sync.WaitGroup{}
		volume.checkpointDoneWaitGroup.Add(1)
	}
	wg = volume.checkpointDoneWaitGroup
	return
}

func (volume *volumeStruct) FetchNextCheckPointDoneWaitGroup() (wg *sync.WaitGroup) {
	volume.Lock()
	wg = volume.fetchNextCheckPointDoneWaitGroupWhileLocked()
	volume.Unlock()
	return
}

func (volume *volumeStruct) fetchNonceWhileLocked() (nonce uint64, err error) {
	var (
		checkpointContainerHeaders map[string][]string
		checkpointHeaderValue      string
		checkpointHeaderValues     []string
		newReservedToNonce         uint64
	)

	if volume.nextNonce == volume.checkpointHeader.reservedToNonce {
		newReservedToNonce = volume.checkpointHeader.reservedToNonce + uint64(volume.nonceValuesToReserve)

		if checkpointHeaderVersion1 == volume.checkpointHeaderVersion {
			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X %016X %016X %016X %016X %016X %016X %016X",
				checkpointHeaderVersion1,
				volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectNumber,
				volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectOffset,
				volume.checkpointObjectTrailer.inodeRecBPlusTreeObjectLength,
				volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectNumber,
				volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectOffset,
				volume.checkpointObjectTrailer.logSegmentRecBPlusTreeObjectLength,
				volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectNumber,
				volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectOffset,
				volume.checkpointObjectTrailer.bPlusTreeObjectBPlusTreeObjectLength,
				newReservedToNonce,
			)
		} else { // checkpointHeaderVersion2 == volume.checkpointHeaderVersion1
			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
				checkpointHeaderVersion2,
				volume.checkpointHeader.checkpointObjectTrailerV2StructObjectNumber,
				volume.checkpointHeader.checkpointObjectTrailerV2StructObjectLength,
				newReservedToNonce,
			)
		}

		checkpointHeaderValues = []string{checkpointHeaderValue}

		checkpointContainerHeaders = make(map[string][]string)

		checkpointContainerHeaders[checkpointHeaderName] = checkpointHeaderValues

		err = swiftclient.ContainerPost(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
		if nil != err {
			return
		}

		volume.checkpointHeader.reservedToNonce = newReservedToNonce
	}

	nonce = volume.nextNonce
	volume.nextNonce++

	err = nil
	return
}

func (volume *volumeStruct) FetchNonce() (nonce uint64, err error) {
	volume.Lock()
	nonce, err = volume.fetchNonceWhileLocked()
	volume.Unlock()
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
