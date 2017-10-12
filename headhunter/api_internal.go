package headhunter

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"
)

// TODO: allowFormat should change to doFormat when controller/runway pre-formats
func (volume *volumeStruct) getCheckpoint(allowFormat bool) (err error) {
	var (
		accountHeaderValues                 []string
		accountHeaders                      map[string][]string
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

	volume.inodeRecWrapper = &bPlusTreeWrapperStruct{volume: volume, wrapperType: inodeRecBPlusTreeWrapperType}
	volume.logSegmentRecWrapper = &bPlusTreeWrapperStruct{volume: volume, wrapperType: logSegmentRecBPlusTreeWrapperType}
	volume.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{volume: volume, wrapperType: bPlusTreeObjectBPlusTreeWrapperType}

	checkpointContainerHeaders, err = swiftclient.ContainerHead(volume.accountName, volume.checkpointContainerName)
	if nil == err {
		checkpointHeaderValues, ok = checkpointContainerHeaders[CheckpointHeaderName]
		if !ok {
			err = fmt.Errorf("Missing %v/%v header %v", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName)
			return
		}
		if 1 != len(checkpointHeaderValues) {
			err = fmt.Errorf("Expected one single value for %v/%v header %v", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName)
			return
		}

		checkpointHeaderValue = checkpointHeaderValues[0]

		// TODO: when mkproxyfs is fully externalized, the following should be removed...
		accountHeaderValues = []string{AccountHeaderValue}

		accountHeaders = make(map[string][]string)

		accountHeaders[AccountHeaderName] = accountHeaderValues

		err = swiftclient.AccountPost(volume.accountName, accountHeaders)
		if nil != err {
			return
		}
	} else {
		if 404 == blunder.HTTPCode(err) {
			// Checkpoint Container not found... so try to create it with some initial values...

			checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber = 0
			checkpointHeader.CheckpointObjectTrailerV2StructObjectLength = 0

			checkpointHeader.ReservedToNonce = firstNonceToProvide - 1

			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
				checkpointHeaderVersion2,
				checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber,
				checkpointHeader.CheckpointObjectTrailerV2StructObjectLength,
				checkpointHeader.ReservedToNonce,
			)

			checkpointHeaderValues = []string{checkpointHeaderValue}

			checkpointContainerHeaders = make(map[string][]string)

			checkpointContainerHeaders[CheckpointHeaderName] = checkpointHeaderValues

			err = swiftclient.ContainerPut(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
			if nil != err {
				return
			}

			// Mark Account as bi-modal...
			// Note: pfs_middleware will actually see this header named AccountHeaderNameTranslated

			accountHeaderValues = []string{AccountHeaderValue}

			accountHeaders = make(map[string][]string)

			accountHeaders[AccountHeaderName] = accountHeaderValues

			err = swiftclient.AccountPost(volume.accountName, accountHeaders)
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
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
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
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (wrong number of fields)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader = &checkpointHeaderV2Struct{
			CheckpointObjectTrailerV2StructObjectNumber: 0,
			CheckpointObjectTrailerV2StructObjectLength: 0,
		}

		volume.checkpointObjectTrailer = &checkpointObjectTrailerV2Struct{}

		volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[1], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectNumber)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[2], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectOffset)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[3], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad inodeRec Root Node objectLength)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[4], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectNumber)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[5], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectOffset)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[6], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad logSegmentRec Root Node objectLength)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[7], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectNumber)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset, err = strconv.ParseUint(checkpointHeaderValueSlice[8], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectOffset)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[9], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad bPlusTreeObject Root Node objectLength)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader.ReservedToNonce, err = strconv.ParseUint(checkpointHeaderValueSlice[10], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad nextNonce)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		// Load/compute volume.{inodeRec|logSegmentRec|bPlusTreeObject} B+Trees/LayoutReports

		if 0 == volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber {
			volume.inodeRecWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxInodesPerMetadataNode,
					sortedmap.CompareUint64,
					volume.inodeRecWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			volume.inodeRecBPlusTreeLayout = make(sortedmap.LayoutReport)
			volume.checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements = 0
		} else {
			volume.inodeRecWrapper.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					volume.inodeRecWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			if nil != err {
				return
			}
			volume.inodeRecBPlusTreeLayout, err = volume.inodeRecWrapper.bPlusTree.FetchLayoutReport()
			if nil != err {
				return
			}
			volume.checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements = uint64(len(volume.inodeRecBPlusTreeLayout))
		}

		if 0 == volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber {
			volume.logSegmentRecWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxLogSegmentsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.logSegmentRecWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			volume.logSegmentRecBPlusTreeLayout = make(sortedmap.LayoutReport)
			volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements = 0
		} else {
			volume.logSegmentRecWrapper.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					volume.logSegmentRecWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			if nil != err {
				return
			}
			volume.logSegmentRecBPlusTreeLayout, err = volume.logSegmentRecWrapper.bPlusTree.FetchLayoutReport()
			if nil != err {
				return
			}
			volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements = uint64(len(volume.logSegmentRecBPlusTreeLayout))
		}

		if 0 == volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber {
			volume.bPlusTreeObjectWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxDirFileNodesPerMetadataNode,
					sortedmap.CompareUint64,
					volume.bPlusTreeObjectWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			volume.bPlusTreeObjectBPlusTreeLayout = make(sortedmap.LayoutReport)
			volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements = 0
		} else {
			volume.bPlusTreeObjectWrapper.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					volume.bPlusTreeObjectWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			if nil != err {
				return
			}
			volume.bPlusTreeObjectBPlusTreeLayout, err = volume.bPlusTreeObjectWrapper.bPlusTree.FetchLayoutReport()
			if nil != err {
				return
			}
			volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements = uint64(len(volume.bPlusTreeObjectBPlusTreeLayout))
		}
	} else if checkpointHeaderVersion2 == checkpointVersion {
		// Read in checkpointHeaderV2Struct

		volume.checkpointHeaderVersion = checkpointHeaderVersion2

		if 4 != len(checkpointHeaderValueSlice) {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (wrong number of fields)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader = &checkpointHeaderV2Struct{}

		volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[1], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad objectNumber)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[2], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad objectLength)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.checkpointHeader.ReservedToNonce, err = strconv.ParseUint(checkpointHeaderValueSlice[3], 16, 64)
		if nil != err {
			err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad nextNonce)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
			return
		}

		volume.inodeRecBPlusTreeLayout = make(sortedmap.LayoutReport)
		volume.logSegmentRecBPlusTreeLayout = make(sortedmap.LayoutReport)
		volume.bPlusTreeObjectBPlusTreeLayout = make(sortedmap.LayoutReport)

		if 0 == volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber {
			volume.checkpointObjectTrailer = &checkpointObjectTrailerV2Struct{
				InodeRecBPlusTreeObjectNumber:             0,
				InodeRecBPlusTreeObjectOffset:             0,
				InodeRecBPlusTreeObjectLength:             0,
				InodeRecBPlusTreeLayoutNumElements:        0,
				LogSegmentRecBPlusTreeObjectNumber:        0,
				LogSegmentRecBPlusTreeObjectOffset:        0,
				LogSegmentRecBPlusTreeObjectLength:        0,
				LogSegmentRecBPlusTreeLayoutNumElements:   0,
				BPlusTreeObjectBPlusTreeObjectNumber:      0,
				BPlusTreeObjectBPlusTreeObjectOffset:      0,
				BPlusTreeObjectBPlusTreeObjectLength:      0,
				BPlusTreeObjectBPlusTreeLayoutNumElements: 0,
			}
		} else {
			// Read in checkpointObjectTrailerV2Struct
			checkpointObjectTrailerBuf, err =
				swiftclient.ObjectTail(
					volume.accountName,
					volume.checkpointContainerName,
					utils.Uint64ToHexStr(volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber),
					volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectLength)
			if nil != err {
				return
			}

			volume.checkpointObjectTrailer = &checkpointObjectTrailerV2Struct{}

			bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, volume.checkpointObjectTrailer, LittleEndian)
			if nil != err {
				return
			}

			// Deserialize volume.{inodeRec|logSegmentRec|bPlusTreeObject}BPlusTreeLayout LayoutReports

			expectedCheckpointObjectTrailerSize = volume.checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize += volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize += volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize *= globals.elementOfBPlusTreeLayoutStructSize
			expectedCheckpointObjectTrailerSize += bytesConsumed

			if uint64(len(checkpointObjectTrailerBuf)) != expectedCheckpointObjectTrailerSize {
				err = fmt.Errorf("volume.checkpointObjectTrailer for volume %v does not match required size", volume.volumeName)
				return
			}

			for layoutReportIndex = 0; layoutReportIndex < volume.checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}

				volume.inodeRecBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}

				volume.logSegmentRecBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements; layoutReportIndex++ {
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}

				volume.bPlusTreeObjectBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}
		}

		// Load volume.{inodeRec|logSegmentRec|bPlusTreeObject} B+Trees

		if 0 == volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber {
			volume.inodeRecWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxInodesPerMetadataNode,
					sortedmap.CompareUint64,
					volume.inodeRecWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
		} else {
			volume.inodeRecWrapper.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					volume.inodeRecWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			if nil != err {
				return
			}
		}

		if 0 == volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber {
			volume.logSegmentRecWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxLogSegmentsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.logSegmentRecWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
		} else {
			volume.logSegmentRecWrapper.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					volume.logSegmentRecWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			if nil != err {
				return
			}
		}

		if 0 == volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber {
			volume.bPlusTreeObjectWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxDirFileNodesPerMetadataNode,
					sortedmap.CompareUint64,
					volume.bPlusTreeObjectWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
		} else {
			volume.bPlusTreeObjectWrapper.bPlusTree, err =
				sortedmap.OldBPlusTree(
					volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber,
					volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset,
					volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength,
					sortedmap.CompareUint64,
					volume.bPlusTreeObjectWrapper,
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
			if nil != err {
				return
			}
		}
	} else {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (version: %v not supported)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue, checkpointVersion)
		return
	}

	volume.nextNonce = volume.checkpointHeader.ReservedToNonce

	err = nil
	return
}

func (volume *volumeStruct) putCheckpoint() (err error) {
	var (
		bytesUsedCumulative                    uint64
		bytesUsedThisBPlusTree                 uint64
		checkpointContainerHeaders             map[string][]string
		checkpointHeaderValue                  string
		checkpointHeaderValues                 []string
		checkpointObjectTrailerBeginningOffset uint64
		checkpointObjectTrailerEndingOffset    uint64
		checkpointTrailerBuf                   []byte
		combinedBPlusTreeLayout                sortedmap.LayoutReport
		elementOfBPlusTreeLayout               elementOfBPlusTreeLayoutStruct
		elementOfBPlusTreeLayoutBuf            []byte
		objectNumber                           uint64
		ok                                     bool
		treeLayoutBuf                          []byte
		treeLayoutBufSize                      uint64
	)

	volume.checkpointFlushedData = false

	volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber,
		volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectOffset,
		volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectLength,
		err = volume.inodeRecWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber,
		volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset,
		volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength,
		err = volume.logSegmentRecWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber,
		volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset,
		volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength,
		err = volume.bPlusTreeObjectWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}

	if !volume.checkpointFlushedData {
		return // since nothing was flushed, we can simply return
	}

	err = volume.inodeRecWrapper.bPlusTree.Prune()
	if nil != err {
		return
	}
	err = volume.logSegmentRecWrapper.bPlusTree.Prune()
	if nil != err {
		return
	}
	err = volume.bPlusTreeObjectWrapper.bPlusTree.Prune()
	if nil != err {
		return
	}

	volume.checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements = uint64(len(volume.inodeRecBPlusTreeLayout))
	volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements = uint64(len(volume.logSegmentRecBPlusTreeLayout))
	volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements = uint64(len(volume.bPlusTreeObjectBPlusTreeLayout))

	checkpointTrailerBuf, err = cstruct.Pack(volume.checkpointObjectTrailer, LittleEndian)
	if nil != err {
		return
	}

	treeLayoutBufSize = volume.checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements
	treeLayoutBufSize *= globals.elementOfBPlusTreeLayoutStructSize

	treeLayoutBuf = make([]byte, 0, treeLayoutBufSize)

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.inodeRecBPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.logSegmentRecBPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.bPlusTreeObjectBPlusTreeLayout {
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

	volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectLength = checkpointObjectTrailerEndingOffset - checkpointObjectTrailerBeginningOffset

	checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
		checkpointHeaderVersion2,
		volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber,
		volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectLength,
		volume.checkpointHeader.ReservedToNonce,
	)

	checkpointHeaderValues = []string{checkpointHeaderValue}

	checkpointContainerHeaders = make(map[string][]string)

	checkpointContainerHeaders[CheckpointHeaderName] = checkpointHeaderValues

	err = swiftclient.ContainerPost(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
	if nil != err {
		return
	}

	volume.checkpointHeaderVersion = checkpointHeaderVersion2

	combinedBPlusTreeLayout = make(sortedmap.LayoutReport)

	for objectNumber, bytesUsedThisBPlusTree = range volume.inodeRecBPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.logSegmentRecBPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.bPlusTreeObjectBPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}

	for objectNumber, bytesUsedCumulative = range combinedBPlusTreeLayout {
		if 0 == bytesUsedCumulative {
			delete(volume.inodeRecBPlusTreeLayout, objectNumber)
			delete(volume.logSegmentRecBPlusTreeLayout, objectNumber)
			delete(volume.bPlusTreeObjectBPlusTreeLayout, objectNumber)

			swiftclient.ObjectDeleteAsync(
				volume.accountName,
				volume.checkpointContainerName,
				utils.Uint64ToHexStr(objectNumber),
				volume.fetchNextCheckPointDoneWaitGroupWhileLocked(),
				nil)
		}
	}

	return // err set as appropriate
}

func (volume *volumeStruct) openCheckpointChunkedPutContextIfNecessary() (err error) {
	if nil == volume.checkpointChunkedPutContext {
		volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber, err = volume.fetchNonceWhileLocked()
		if nil != err {
			return
		}
		volume.checkpointChunkedPutContext, err =
			swiftclient.ObjectFetchChunkedPutContext(volume.accountName,
				volume.checkpointContainerName,
				utils.Uint64ToHexStr(volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber))
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
				volume.checkpointChunkedPutContext = nil
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
		volume.checkpointChunkedPutContext = nil
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

		checkpointRequest.err = volume.putCheckpoint()

		exitOnCompletion = checkpointRequest.exitOnCompletion // In case requestor re-uses checkpointRequest

		checkpointRequest.waitGroup.Done() // Awake the checkpoint requestor
		if nil != volume.checkpointDoneWaitGroup {
			// Awake any others who were waiting on this checkpoint
			volume.checkpointDoneWaitGroup.Done()
			volume.checkpointDoneWaitGroup = nil
		}

		volume.Unlock()

		if exitOnCompletion {
			return
		}
	}
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("headhunter.bPlusTreeWrapper.DumpKey() could not parse key as a uint64")
		return
	}

	keyAsString = fmt.Sprintf("0x%016X", keyAsUint64)

	err = nil
	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
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

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	nodeByteSlice, err =
		swiftclient.ObjectGet(
			bPlusTreeWrapper.volume.accountName,
			bPlusTreeWrapper.volume.checkpointContainerName,
			utils.Uint64ToHexStr(objectNumber),
			objectOffset,
			objectLength)
	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	var (
		bytesUsed uint64
		ok        bool
	)

	err = bPlusTreeWrapper.volume.openCheckpointChunkedPutContextIfNecessary()
	if nil != err {
		return
	}

	objectNumber = bPlusTreeWrapper.volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber

	objectOffset, err = bPlusTreeWrapper.volume.bytesPutToCheckpointChunkedPutContext()
	if nil != err {
		return
	}

	err = bPlusTreeWrapper.volume.sendChunkToCheckpointChunkedPutContext(nodeByteSlice)
	if nil != err {
		return
	}

	bPlusTreeWrapper.volume.checkpointFlushedData = true

	switch bPlusTreeWrapper.wrapperType {
	case inodeRecBPlusTreeWrapperType:
		bytesUsed, ok = bPlusTreeWrapper.volume.inodeRecBPlusTreeLayout[objectNumber]
		if ok {
			bPlusTreeWrapper.volume.inodeRecBPlusTreeLayout[objectNumber] = bytesUsed + uint64(len(nodeByteSlice))
		} else {
			bPlusTreeWrapper.volume.inodeRecBPlusTreeLayout[objectNumber] = uint64(len(nodeByteSlice))
		}
	case logSegmentRecBPlusTreeWrapperType:
		bytesUsed, ok = bPlusTreeWrapper.volume.logSegmentRecBPlusTreeLayout[objectNumber]
		if ok {
			bPlusTreeWrapper.volume.logSegmentRecBPlusTreeLayout[objectNumber] = bytesUsed + uint64(len(nodeByteSlice))
		} else {
			bPlusTreeWrapper.volume.logSegmentRecBPlusTreeLayout[objectNumber] = uint64(len(nodeByteSlice))
		}
	case bPlusTreeObjectBPlusTreeWrapperType:
		bytesUsed, ok = bPlusTreeWrapper.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber]
		if ok {
			bPlusTreeWrapper.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber] = bytesUsed + uint64(len(nodeByteSlice))
		} else {
			bPlusTreeWrapper.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber] = uint64(len(nodeByteSlice))
		}
	default:
		err = fmt.Errorf("Logic error: bPlusTreeWrapper.PutNode() called for invalid wrapperType: %v", bPlusTreeWrapper.wrapperType)
		panic(err)
	}

	err = bPlusTreeWrapper.volume.closeCheckpointChunkedPutContextIfNecessary()

	return // err set as appropriate
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	var (
		bytesUsed uint64
		ok        bool
	)

	switch bPlusTreeWrapper.wrapperType {
	case inodeRecBPlusTreeWrapperType:
		bytesUsed, ok = bPlusTreeWrapper.volume.inodeRecBPlusTreeLayout[objectNumber]
		if ok {
			if bytesUsed < objectLength {
				err = fmt.Errorf("Logic error: [inodeRecBPlusTreeWrapperType] bPlusTreeWrapper.DiscardNode() called to dereference too many bytes in objectNumber 0x%016X", objectNumber)
			} else {
				err = nil
				bPlusTreeWrapper.volume.inodeRecBPlusTreeLayout[objectNumber] = bytesUsed - objectLength
			}
		} else {
			err = fmt.Errorf("Logic error: [inodeRecBPlusTreeWrapperType] bPlusTreeWrapper.DiscardNode() called referencing invalid objectNumber: 0x%016X", objectNumber)
		}
	case logSegmentRecBPlusTreeWrapperType:
		bytesUsed, ok = bPlusTreeWrapper.volume.logSegmentRecBPlusTreeLayout[objectNumber]
		if ok {
			if bytesUsed < objectLength {
				err = fmt.Errorf("Logic error: [logSegmentRecBPlusTreeWrapperType] bPlusTreeWrapper.DiscardNode() called to dereference too many bytes in objectNumber 0x%016X", objectNumber)
			} else {
				err = nil
				bPlusTreeWrapper.volume.logSegmentRecBPlusTreeLayout[objectNumber] = bytesUsed - objectLength
			}
		} else {
			err = fmt.Errorf("Logic error: [logSegmentRecBPlusTreeWrapperType] bPlusTreeWrapper.DiscardNode() called referencing invalid objectNumber: 0x%016X", objectNumber)
		}
	case bPlusTreeObjectBPlusTreeWrapperType:
		bytesUsed, ok = bPlusTreeWrapper.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber]
		if ok {
			if bytesUsed < objectLength {
				err = fmt.Errorf("Logic error: [bPlusTreeObjectBPlusTreeWrapperType] bPlusTreeWrapper.DiscardNode() called to dereference too many bytes in objectNumber 0x%016X", objectNumber)
			} else {
				err = nil
				bPlusTreeWrapper.volume.bPlusTreeObjectBPlusTreeLayout[objectNumber] = bytesUsed - objectLength
			}
		} else {
			err = fmt.Errorf("Logic error: [bPlusTreeObjectBPlusTreeWrapperType] bPlusTreeWrapper.DiscardNode() called referencing invalid objectNumber: 0x%016X", objectNumber)
		}
	default:
		err = fmt.Errorf("Logic error: bPlusTreeWrapper.DiscardNode() called for invalid wrapperType: %v", bPlusTreeWrapper.wrapperType)
	}

	return // err set as appropriate regardless of path
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("*bPlusTreeWrapper.PackKey(key == %v) failed to convert key to uint64", key)
		return
	}
	packedKey = utils.Uint64ToByteSlice(keyAsUint64)
	err = nil
	return
}

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
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

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
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

func (bPlusTreeWrapper *bPlusTreeWrapperStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
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

	if volume.nextNonce == volume.checkpointHeader.ReservedToNonce {
		newReservedToNonce = volume.checkpointHeader.ReservedToNonce + uint64(volume.nonceValuesToReserve)

		if checkpointHeaderVersion1 == volume.checkpointHeaderVersion {
			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X %016X %016X %016X %016X %016X %016X %016X",
				checkpointHeaderVersion1,
				volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber,
				volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectOffset,
				volume.checkpointObjectTrailer.InodeRecBPlusTreeObjectLength,
				volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber,
				volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset,
				volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength,
				volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber,
				volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset,
				volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength,
				newReservedToNonce,
			)
		} else { // checkpointHeaderVersion2 == volume.checkpointHeaderVersion
			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
				checkpointHeaderVersion2,
				volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber,
				volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectLength,
				newReservedToNonce,
			)
		}

		checkpointHeaderValues = []string{checkpointHeaderValue}

		checkpointContainerHeaders = make(map[string][]string)

		checkpointContainerHeaders[CheckpointHeaderName] = checkpointHeaderValues

		err = swiftclient.ContainerPost(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
		if nil != err {
			return
		}

		volume.checkpointHeader.ReservedToNonce = newReservedToNonce
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

func (volume *volumeStruct) GetInodeRec(inodeNumber uint64) (value []byte, ok bool, err error) {
	volume.Lock()
	valueAsValue, ok, err := volume.inodeRecWrapper.bPlusTree.GetByKey(inodeNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
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

	logger.Tracef("headhunger.PutInodeRec(): volume '%s' inode %d", volume.volumeName, inodeNumber)

	valueToTree := make([]byte, len(value))
	copy(valueToTree, value)

	volume.Lock()
	ok, err := volume.inodeRecWrapper.bPlusTree.PatchByKey(inodeNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.inodeRecWrapper.bPlusTree.Put(inodeNumber, valueToTree)
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

	logger.Tracef("headhunter.PutInodeRecs(): volume '%s' inodes %v", volume.volumeName, inodeNumbers)

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
		ok, nonShadowingErr := volume.inodeRecWrapper.bPlusTree.PatchByKey(inodeNumber, valuesToTree[i])
		if nil != nonShadowingErr {
			volume.Unlock()
			err = nonShadowingErr
			return
		}
		if !ok {
			_, err = volume.inodeRecWrapper.bPlusTree.Put(inodeNumber, valuesToTree[i])
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
	_, err = volume.inodeRecWrapper.bPlusTree.DeleteByKey(inodeNumber)
	volume.Unlock()

	return
}

func (volume *volumeStruct) GetLogSegmentRec(logSegmentNumber uint64) (value []byte, err error) {
	volume.Lock()
	valueAsValue, ok, err := volume.logSegmentRecWrapper.bPlusTree.GetByKey(logSegmentNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		err = fmt.Errorf("logSegmentNumber 0x%016X not found in volume \"%v\" logSegmentRecWrapper.bPlusTree", logSegmentNumber, volume.volumeName)
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
	ok, err := volume.logSegmentRecWrapper.bPlusTree.PatchByKey(logSegmentNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.logSegmentRecWrapper.bPlusTree.Put(logSegmentNumber, valueToTree)
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
	_, err = volume.logSegmentRecWrapper.bPlusTree.DeleteByKey(logSegmentNumber)
	volume.Unlock()

	return
}

func (volume *volumeStruct) GetBPlusTreeObject(objectNumber uint64) (value []byte, err error) {
	volume.Lock()
	valueAsValue, ok, err := volume.bPlusTreeObjectWrapper.bPlusTree.GetByKey(objectNumber)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		volume.Unlock()
		err = fmt.Errorf("objectNumber 0x%016X not found in volume \"%v\" bPlusTreeObjectWrapper.bPlusTree", objectNumber, volume.volumeName)
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
	ok, err := volume.bPlusTreeObjectWrapper.bPlusTree.PatchByKey(objectNumber, valueToTree)
	if nil != err {
		volume.Unlock()
		return
	}
	if !ok {
		_, err = volume.bPlusTreeObjectWrapper.bPlusTree.Put(objectNumber, valueToTree)
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
	_, err = volume.bPlusTreeObjectWrapper.bPlusTree.DeleteByKey(objectNumber)
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
