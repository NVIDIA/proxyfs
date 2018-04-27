package headhunter

import (
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/platform"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

var (
	LittleEndian = cstruct.LittleEndian // All data cstructs to be serialized in LittleEndian form
)

type uint64Struct struct {
	u64 uint64
}

const (
	checkpointHeaderVersion2 uint64 = iota + 2
	checkpointHeaderVersion3
	// uint64 in %016X indicating checkpointHeaderVersion2 or checkpointHeaderVersion3
	// ' '
	// uint64 in %016X indicating objectNumber containing checkpoint record at tail of object
	// ' '
	// uint64 in %016X indicating length of               checkpoint record at tail of object
	// ' '
	// uint64 in %016X indicating reservedToNonce
)

type checkpointHeaderV2Struct struct {
	CheckpointObjectTrailerV2StructObjectNumber uint64 // checkpointObjectTrailerV2Struct found at "tail" of object
	CheckpointObjectTrailerV2StructObjectLength uint64 // this length includes the three B+Tree "layouts" appended
	ReservedToNonce                             uint64 // highest nonce value reserved
}

type checkpointHeaderV3Struct struct {
	CheckpointObjectTrailerV3StructObjectNumber uint64 // checkpointObjectTrailerV3Struct found at "tail" of object
	CheckpointObjectTrailerV3StructObjectLength uint64 // this length includes appended non-fixed sized arrays
	ReservedToNonce                             uint64 // highest nonce value reserved
}

type checkpointObjectTrailerV2Struct struct {
	InodeRecBPlusTreeObjectNumber             uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of inodeRec        B+Tree
	InodeRecBPlusTreeObjectOffset             uint64 // ...and offset into the Object where root starts
	InodeRecBPlusTreeObjectLength             uint64 // ...and length if that root node
	InodeRecBPlusTreeLayoutNumElements        uint64 // elements immediately follow checkpointObjectTrailerV2Struct
	LogSegmentRecBPlusTreeObjectNumber        uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of logSegment      B+Tree
	LogSegmentRecBPlusTreeObjectOffset        uint64 // ...and offset into the Object where root starts
	LogSegmentRecBPlusTreeObjectLength        uint64 // ...and length if that root node
	LogSegmentRecBPlusTreeLayoutNumElements   uint64 // elements immediately follow inodeRecBPlusTreeLayout
	BPlusTreeObjectBPlusTreeObjectNumber      uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of bPlusTreeObject B+Tree
	BPlusTreeObjectBPlusTreeObjectOffset      uint64 // ...and offset into the Object where root starts
	BPlusTreeObjectBPlusTreeObjectLength      uint64 // ...and length if that root node
	BPlusTreeObjectBPlusTreeLayoutNumElements uint64 // elements immediately follow logSegmentRecBPlusTreeLayout
	// inodeRecBPlusTreeLayout        serialized as [inodeRecBPlusTreeLayoutNumElements       ]elementOfBPlusTreeLayoutStruct
	// logSegmentBPlusTreeLayout      serialized as [logSegmentRecBPlusTreeLayoutNumElements  ]elementOfBPlusTreeLayoutStruct
	// bPlusTreeObjectBPlusTreeLayout serialized as [bPlusTreeObjectBPlusTreeLayoutNumElements]elementOfBPlusTreeLayoutStruct
}

type checkpointObjectTrailerV3Struct struct {
	InodeRecBPlusTreeObjectNumber             uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of inodeRec        B+Tree
	InodeRecBPlusTreeObjectOffset             uint64 // ...and offset into the Object where root starts
	InodeRecBPlusTreeObjectLength             uint64 // ...and length if that root node
	InodeRecBPlusTreeLayoutNumElements        uint64 // elements immediately follow checkpointObjectTrailerV2Struct
	LogSegmentRecBPlusTreeObjectNumber        uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of logSegment      B+Tree
	LogSegmentRecBPlusTreeObjectOffset        uint64 // ...and offset into the Object where root starts
	LogSegmentRecBPlusTreeObjectLength        uint64 // ...and length if that root node
	LogSegmentRecBPlusTreeLayoutNumElements   uint64 // elements immediately follow inodeRecBPlusTreeLayout
	BPlusTreeObjectBPlusTreeObjectNumber      uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of bPlusTreeObject B+Tree
	BPlusTreeObjectBPlusTreeObjectOffset      uint64 // ...and offset into the Object where root starts
	BPlusTreeObjectBPlusTreeObjectLength      uint64 // ...and length if that root node
	BPlusTreeObjectBPlusTreeLayoutNumElements uint64 // elements immediately follow logSegmentRecBPlusTreeLayout
	SnapShotIDNumBits                         uint64 // number of bits reserved to hold SnapShotIDs
	SnapShotListNumElements                   uint64 // elements immediately follow bPlusTreeObjectBPlusTreeLayout
	SnapShotListTotalSize                     uint64 // size of entire SnapShotList
	// inodeRecBPlusTreeLayout        serialized as [inodeRecBPlusTreeLayoutNumElements       ]elementOfBPlusTreeLayoutStruct
	// logSegmentBPlusTreeLayout      serialized as [logSegmentRecBPlusTreeLayoutNumElements  ]elementOfBPlusTreeLayoutStruct
	// bPlusTreeObjectBPlusTreeLayout serialized as [bPlusTreeObjectBPlusTreeLayoutNumElements]elementOfBPlusTreeLayoutStruct
	// snapShotList                   serialized as [snapShotListNumElements                  ]elementOfSnapShotListStruct
}

type elementOfBPlusTreeLayoutStruct struct {
	ObjectNumber uint64
	ObjectBytes  uint64
}

type elementOfSnapShotListStruct struct { // Note: for illustrative purposes... not marshalled with cstruct
	nonce uint64 //        supplies strict time-ordering of SnapShots regardless of timebase resets
	id    uint64 //        in the range [1:2^SnapShotIDNumBits-2]
	//                       ID == 0                     reserved for the "live" view
	//                       ID == 2^SnapShotIDNumBits-1 reserved for the .snapshot subdir of a dir
	//                                                             or to indicate a SnapShot being deleted
	timeStamp time.Time // serialized/deserialized as a uint64 length followed by a that sized []byte
	//                       func (t  time.Time) time.MarshalBinary()              ([]byte, error)
	//                       func (t *time.Time) time.UnmarshalBinary(data []byte) (error)
	name string //         serialized/deserialized as a uint64 length followed by a that sized []byte
	//                       func utils.ByteSliceToString(byteSlice []byte)        (str string)
	//                       func utils.StringToByteSlice(str string)              (byteSlice []byte)
	createdObjectsBPlusTreeObjectNumber      uint64
	createdObjectsBPlusTreeObjectOffset      uint64
	createdObjectsBPlusTreeObjectLength      uint64
	createdObjectsBPlusTreeLayoutNumElements uint64
	// createdObjectsBPlusTreeLayout serialized as [createdObjectsBPlusTreeLayoutNumElements]elementOfBPlusTreeLayoutStruct
	deletedObjectsBPlusTreeObjectNumber      uint64
	deletedObjectsBPlusTreeObjectOffset      uint64
	deletedObjectsBPlusTreeObjectLength      uint64
	deletedObjectsBPlusTreeLayoutNumElements uint64
	// deletedObjectsBPlusTreeLayout serialized as [deletedObjectsBPlusTreeLayoutNumElements]elementOfBPlusTreeLayoutStruct
}

type checkpointRequestStruct struct {
	waitGroup        sync.WaitGroup
	err              error
	exitOnCompletion bool
}

const (
	replayLogWriteBufferAlignment   uintptr = 4096
	replayLogWriteBufferDefaultSize uint64  = 100 * uint64(replayLogWriteBufferAlignment)
)

const (
	transactionPutInodeRec uint64 = iota
	transactionPutInodeRecs
	transactionDeleteInodeRec
	transactionPutLogSegmentRec
	transactionDeleteLogSegmentRec
	transactionPutBPlusTreeObject
	transactionDeleteBPlusTreeObject
)

type replayLogTransactionFixedPartStruct struct { //          transactions begin on a replayLogWriteBufferAlignment boundary
	CRC64                                           uint64 // checksum of everything after this field
	BytesFollowing                                  uint64 // bytes following in this transaction
	LastCheckpointObjectTrailerV2StructObjectNumber uint64 // last checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber
	TransactionType                                 uint64 // transactionType from above const() block
}

func constructReplayLogWriteBuffer(minBufferSize uint64) (alignedBuf []byte) {
	var (
		alignedBufAddr   uintptr
		alignedBufOffset uintptr
		alignedBufSize   uintptr
		allocSize        uintptr
		unalignedBuf     []byte
		unalignedBufAddr uintptr
	)

	alignedBufSize = (uintptr(minBufferSize) + replayLogWriteBufferAlignment - 1) & ^(replayLogWriteBufferAlignment - 1)
	allocSize = alignedBufSize + replayLogWriteBufferAlignment - 1
	unalignedBuf = make([]byte, allocSize)
	unalignedBufAddr = uintptr(unsafe.Pointer(&unalignedBuf[0]))
	alignedBufAddr = (unalignedBufAddr + replayLogWriteBufferAlignment - 1) & ^(replayLogWriteBufferAlignment - 1)
	alignedBufOffset = uintptr(alignedBufAddr) - unalignedBufAddr
	alignedBuf = unalignedBuf[alignedBufOffset : alignedBufOffset+uintptr(alignedBufSize)]

	return
}

func (volume *volumeStruct) minimizeReplayLogWriteBuffer(bytesNeeded uint64) (minimizedBuf []byte) {
	var (
		truncatedDefaultReplayLogWriteBufferSize uintptr
	)

	truncatedDefaultReplayLogWriteBufferSize = (uintptr(bytesNeeded) + replayLogWriteBufferAlignment - 1) & ^(replayLogWriteBufferAlignment - 1)

	minimizedBuf = volume.defaultReplayLogWriteBuffer[:truncatedDefaultReplayLogWriteBufferSize]

	return
}

func (volume *volumeStruct) recordTransaction(transactionType uint64, keys interface{}, values interface{}) {
	var (
		bytesNeeded                  uint64
		err                          error
		i                            int
		multipleKeys                 []uint64
		multipleValues               [][]byte
		packedUint64                 []byte
		replayLogWriteBuffer         []byte
		replayLogWriteBufferPosition uint64
		singleKey                    uint64
		singleValue                  []byte
	)

	// TODO: Eventually embed this stuff in the case statement below
	switch transactionType {
	case transactionPutInodeRec:
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionPutInodeRec, volume.volumeName, keys.(uint64))
	case transactionPutInodeRecs:
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionPutInodeRecs, volume.volumeName, keys.([]uint64))
	case transactionDeleteInodeRec:
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionDeleteInodeRec, volume.volumeName, keys.(uint64))
	case transactionPutLogSegmentRec:
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionPutLogSegmentRec, volume.volumeName, keys.(uint64), string(values.([]byte)[:]))
	case transactionDeleteLogSegmentRec:
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionDeleteLogSegmentRec, volume.volumeName, keys.(uint64))
	case transactionPutBPlusTreeObject:
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionPutBPlusTreeObject, volume.volumeName, keys.(uint64))
	case transactionDeleteBPlusTreeObject:
		evtlog.Record(evtlog.FormatHeadhunterRecordTransactionDeleteBPlusTreeObject, volume.volumeName, keys.(uint64))
	default:
		logger.Fatalf("headhunter.recordTransaction(transactionType==%v,,) invalid", transactionType)
	}

	// TODO: Eventually just remove this (once replayLogFile is mandatory)
	if "" == volume.replayLogFileName {
		// Replay Log is disabled... simply return
		return
	}

	switch transactionType {
	case transactionPutInodeRec:
		singleKey = keys.(uint64)
		singleValue = values.([]byte)
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber
				globals.uint64Size + //               transactionType == transactionPutInodeRec
				globals.uint64Size + //               inodeNumber
				globals.uint64Size + //               len(value)
				uint64(len(singleValue)) //           value
	case transactionPutInodeRecs:
		multipleKeys = keys.([]uint64)
		multipleValues = values.([][]byte)
		if len(multipleKeys) != len(multipleValues) {
			logger.Fatalf("headhunter.recordTransaction(transactionType==transactionPutInodeRecs,,) passed len(keys) != len(values)")
		}
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber
				globals.uint64Size + //               transactionType == transactionPutInodeRecs
				globals.uint64Size //                 len(inodeNumbers) == len(values)
		for i = 0; i < len(multipleKeys); i++ {
			bytesNeeded +=
				globals.uint64Size + //               inodeNumbers[i]
					globals.uint64Size + //           len(values[i])
					uint64(len(multipleValues[i])) // values[i]
		}
	case transactionDeleteInodeRec:
		singleKey = keys.(uint64)
		if nil != values {
			logger.Fatalf("headhunter.recordTransaction(transactionType==transactionDeleteInodeRec,,) passed non-nil values")
		}
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber
				globals.uint64Size + //               transactionType == transactionDeleteInodeRec
				globals.uint64Size //                 inodeNumber
	case transactionPutLogSegmentRec:
		singleKey = keys.(uint64)
		singleValue = values.([]byte)
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber
				globals.uint64Size + //               transactionType == transactionPutLogSegmentRec
				globals.uint64Size + //               logSegmentNumber
				globals.uint64Size + //               len(value)
				uint64(len(singleValue)) //           value
	case transactionDeleteLogSegmentRec:
		singleKey = keys.(uint64)
		if nil != values {
			logger.Fatalf("headhunter.recordTransaction(transactionType==transactionDeleteLogSegmentRec,,) passed non-nil values")
		}
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber
				globals.uint64Size + //               transactionType == transactionDeleteLogSegmentRec
				globals.uint64Size //                 logSegmentNumber
	case transactionPutBPlusTreeObject:
		singleKey = keys.(uint64)
		singleValue = values.([]byte)
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber
				globals.uint64Size + //               transactionType == transactionPutBPlusTreeObject
				globals.uint64Size + //               objectNumber
				globals.uint64Size + //               len(value)
				uint64(len(singleValue)) //           value
	case transactionDeleteBPlusTreeObject:
		singleKey = keys.(uint64)
		if nil != values {
			logger.Fatalf("headhunter.recordTransaction(transactionType==transactionDeleteBPlusTreeObject,,) passed non-nil values")
		}
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber
				globals.uint64Size + //               transactionType == transactionDeleteBPlusTreeObject
				globals.uint64Size //                 objectNumber
	default:
		logger.Fatalf("headhunter.recordTransaction(transactionType==%v,,) invalid", transactionType)
	}

	if bytesNeeded <= replayLogWriteBufferDefaultSize {
		replayLogWriteBuffer = volume.minimizeReplayLogWriteBuffer(bytesNeeded)
	} else {
		replayLogWriteBuffer = constructReplayLogWriteBuffer(bytesNeeded)
	}

	// For now, leave room for ECMA CRC-64

	replayLogWriteBufferPosition = globals.uint64Size

	// Fill in bytes following in this transaction

	packedUint64, err = cstruct.Pack(bytesNeeded-globals.uint64Size-globals.uint64Size, LittleEndian)
	if nil != err {
		logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
	}
	_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
	replayLogWriteBufferPosition += globals.uint64Size

	// Fill in last checkpoint's checkpointHeaderV2Struct.CheckpointObjectTrailerV2StructObjectNumber

	packedUint64, err = cstruct.Pack(volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber, LittleEndian)
	if nil != err {
		logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
	}
	_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
	replayLogWriteBufferPosition += globals.uint64Size

	// Fill in transactionType

	packedUint64, err = cstruct.Pack(transactionType, LittleEndian)
	if nil != err {
		logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
	}
	_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
	replayLogWriteBufferPosition += globals.uint64Size

	// Fill in remaining transactionType-specific bytes

	switch transactionType {
	case transactionPutInodeRec:
		// Fill in inodeNumber

		packedUint64, err = cstruct.Pack(singleKey, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size

		// Fill in len(value) and value

		packedUint64, err = cstruct.Pack(uint64(len(singleValue)), LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size

		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], singleValue)
		replayLogWriteBufferPosition += uint64(len(singleValue))
	case transactionPutInodeRecs:
		// Fill in number of following inodeNumber:value pairs

		packedUint64, err = cstruct.Pack(uint64(len(multipleKeys)), LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size

		// Fill in each inodeNumber:value pair

		for i = 0; i < len(multipleKeys); i++ {
			// Fill in inodeNumber

			packedUint64, err = cstruct.Pack(multipleKeys[i], LittleEndian)
			if nil != err {
				logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
			}
			_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
			replayLogWriteBufferPosition += globals.uint64Size

			// Fill in len(value) and value

			packedUint64, err = cstruct.Pack(uint64(len(multipleValues[i])), LittleEndian)
			if nil != err {
				logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
			}
			_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
			replayLogWriteBufferPosition += globals.uint64Size

			_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], multipleValues[i])
			replayLogWriteBufferPosition += uint64(len(multipleValues[i]))
		}
	case transactionDeleteInodeRec:
		// Fill in inodeNumber

		packedUint64, err = cstruct.Pack(singleKey, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size
	case transactionPutLogSegmentRec:
		// Fill in logSegmentNumber

		packedUint64, err = cstruct.Pack(singleKey, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size

		// Fill in len(value) and value

		packedUint64, err = cstruct.Pack(uint64(len(singleValue)), LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size

		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], singleValue)
		replayLogWriteBufferPosition += uint64(len(singleValue))
	case transactionDeleteLogSegmentRec:
		// Fill in logSegmentNumber

		packedUint64, err = cstruct.Pack(singleKey, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size
	case transactionPutBPlusTreeObject:
		// Fill in objectNumber

		packedUint64, err = cstruct.Pack(singleKey, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size

		// Fill in len(value) and value

		packedUint64, err = cstruct.Pack(uint64(len(singleValue)), LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size

		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], singleValue)
		replayLogWriteBufferPosition += uint64(len(singleValue))
	case transactionDeleteBPlusTreeObject:
		// Fill in objectNumber

		packedUint64, err = cstruct.Pack(singleKey, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
		}
		_ = copy(replayLogWriteBuffer[replayLogWriteBufferPosition:], packedUint64)
		replayLogWriteBufferPosition += globals.uint64Size
	default:
		logger.Fatalf("headhunter.recordTransaction(transactionType==%v,,) invalid", transactionType)
	}

	// Compute and fill in ECMA CRC-64

	packedUint64, err = cstruct.Pack(crc64.Checksum(replayLogWriteBuffer[globals.uint64Size:bytesNeeded], globals.crc64ECMATable), LittleEndian)
	if nil != err {
		logger.Fatalf("cstruct.Pack() unexpectedly returned error: %v", err)
	}
	_ = copy(replayLogWriteBuffer, packedUint64)

	// Finally, write out replayLogWriteBuffer

	if nil == volume.replayLogFile {
		// Replay Log not currently open
		//
		// Either upVolume()'s call to getCheckpoint() found that a clean downVolume() was possible
		// or a successful putCheckpoint() has removed the Replay Log. In either case, a fresh
		// Replay Log will now be created.

		volume.replayLogFile, err = platform.OpenFileSync(volume.replayLogFileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if nil != err {
			logger.FatalfWithError(err, "platform.OpenFileSync(%v,os.O_CREATE|os.O_EXCL|os.O_WRONLY,) failed", volume.replayLogFileName)
		}
	} else {
		// Replay Log is currently open
		//
		// If this is the first call to recordTransaction() since upVolume() called getCheckpoint(),
		// volume.replayLogFile will be positioned for writing just after the last transaction replayed
		// following the loading of the checkpoint. If this is not the first call to recordTransaction()
		// since the last putCheckpoint(), volume.replayLogFile will be positioned for writing just
		// after the prior transaction.
	}

	_, err = volume.replayLogFile.Write(replayLogWriteBuffer)
	if nil != err {
		logger.Fatalf("os.Write() unexpectedly returned error: %v", err)
	}

	return
}

func (volume *volumeStruct) fetchCheckpointLayoutReport() (layoutReport sortedmap.LayoutReport, err error) {
	var (
		checkpointContainerHeaders map[string][]string
		checkpointHeaderValue      string
		checkpointHeaderValueSlice []string
		checkpointHeaderValues     []string
		checkpointVersion          uint64
		objectLength               uint64
		objectNumber               uint64
		ok                         bool
	)

	checkpointContainerHeaders, err = swiftclient.ContainerHead(volume.accountName, volume.checkpointContainerName)
	if nil != err {
		return
	}

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

	checkpointHeaderValueSlice = strings.Split(checkpointHeaderValue, " ")

	if 1 > len(checkpointHeaderValueSlice) {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
		return
	}

	checkpointVersion, err = strconv.ParseUint(checkpointHeaderValueSlice[0], 16, 64)
	if nil != err {
		return
	}
	if checkpointHeaderVersion2 != checkpointVersion {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (version: %v not supported)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue, checkpointVersion)
		return
	}

	objectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[1], 16, 64)
	if nil != err {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad objectNumber)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
		return
	}

	objectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[2], 16, 64)
	if nil != err {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad objectLength)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
		return
	}

	// Return layoutReport manufactured from the checkpointHeaderValue

	layoutReport = make(sortedmap.LayoutReport)
	layoutReport[objectNumber] = objectLength

	return
}

func (volumeView *volumeViewStruct) loadCheckpointObjectTrailerV2(checkpointObjectTrailerV2StructObjectNumber uint64, checkpointObjectTrailerV2StructObjectLength uint64) (checkpointObjectTrailer *checkpointObjectTrailerV2Struct, err error) {
	var (
		bytesConsumed                       uint64
		checkpointObjectTrailerBuf          []byte
		elementOfBPlusTreeLayout            elementOfBPlusTreeLayoutStruct
		expectedCheckpointObjectTrailerSize uint64
		layoutReportIndex                   uint64
	)

	// Read in checkpointObjectTrailerV2Struct

	checkpointObjectTrailerBuf, err =
		swiftclient.ObjectTail(
			volumeView.volume.accountName,
			volumeView.volume.checkpointContainerName,
			utils.Uint64ToHexStr(checkpointObjectTrailerV2StructObjectNumber),
			checkpointObjectTrailerV2StructObjectLength)
	if nil != err {
		return
	}

	checkpointObjectTrailer = &checkpointObjectTrailerV2Struct{}

	bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, checkpointObjectTrailer, LittleEndian)
	if nil != err {
		return
	}

	// Load volumeView.{inodeRec|logSegmentRec|bPlusTreeObject}Wrapper B+Trees

	volumeView.inodeRecWrapper = &bPlusTreeWrapperStruct{
		volumeView:              volumeView,
		trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
	}

	if 0 == checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber {
		volumeView.inodeRecWrapper.bPlusTree =
			sortedmap.NewBPlusTree(
				volumeView.volume.maxInodesPerMetadataNode,
				sortedmap.CompareUint64,
				volumeView.inodeRecWrapper,
				globals.inodeRecCache)
	} else {
		volumeView.inodeRecWrapper.bPlusTree, err =
			sortedmap.OldBPlusTree(
				checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber,
				checkpointObjectTrailer.InodeRecBPlusTreeObjectOffset,
				checkpointObjectTrailer.InodeRecBPlusTreeObjectLength,
				sortedmap.CompareUint64,
				volumeView.inodeRecWrapper,
				globals.inodeRecCache)
		if nil != err {
			return
		}
	}

	volumeView.logSegmentRecWrapper = &bPlusTreeWrapperStruct{
		volumeView:              volumeView,
		trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
	}

	if 0 == checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber {
		volumeView.logSegmentRecWrapper.bPlusTree =
			sortedmap.NewBPlusTree(
				volumeView.volume.maxLogSegmentsPerMetadataNode,
				sortedmap.CompareUint64,
				volumeView.logSegmentRecWrapper,
				globals.logSegmentRecCache)
	} else {
		volumeView.logSegmentRecWrapper.bPlusTree, err =
			sortedmap.OldBPlusTree(
				checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber,
				checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset,
				checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength,
				sortedmap.CompareUint64,
				volumeView.logSegmentRecWrapper,
				globals.logSegmentRecCache)
		if nil != err {
			return
		}
	}

	volumeView.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{
		volumeView:              volumeView,
		trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
	}

	if 0 == checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber {
		volumeView.bPlusTreeObjectWrapper.bPlusTree =
			sortedmap.NewBPlusTree(
				volumeView.volume.maxDirFileNodesPerMetadataNode,
				sortedmap.CompareUint64,
				volumeView.bPlusTreeObjectWrapper,
				globals.bPlusTreeObjectCache)
	} else {
		volumeView.bPlusTreeObjectWrapper.bPlusTree, err =
			sortedmap.OldBPlusTree(
				checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber,
				checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset,
				checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength,
				sortedmap.CompareUint64,
				volumeView.bPlusTreeObjectWrapper,
				globals.bPlusTreeObjectCache)
		if nil != err {
			return
		}
	}

	// Fake load volumeView.{createdObjects|deletedObjects}Wrapper B+Trees

	volumeView.createdObjectsWrapper = &bPlusTreeWrapperStruct{
		volumeView:              volumeView,
		trackingBPlusTreeLayout: make(sortedmap.LayoutReport), // Nothing to deserialize into this
	}

	volumeView.createdObjectsWrapper.bPlusTree =
		sortedmap.NewBPlusTree(
			volumeView.volume.maxCreatedDeletedObjectsPerMetadataNode,
			sortedmap.CompareUint64,
			volumeView.volume.liveView.createdObjectsWrapper,
			globals.createdDeletedObjectsCache)

	volumeView.deletedObjectsWrapper = &bPlusTreeWrapperStruct{
		volumeView:              volumeView,
		trackingBPlusTreeLayout: make(sortedmap.LayoutReport), // Nothing to deserialize into this
	}

	volumeView.deletedObjectsWrapper.bPlusTree =
		sortedmap.NewBPlusTree(
			volumeView.volume.maxCreatedDeletedObjectsPerMetadataNode,
			sortedmap.CompareUint64,
			volumeView.volume.liveView.createdObjectsWrapper,
			globals.createdDeletedObjectsCache)

	// Deserialize volumeView.{inodeRec|logSegmentRec|bPlusTreeObject}Wrapper LayoutReports

	expectedCheckpointObjectTrailerSize = checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements
	expectedCheckpointObjectTrailerSize += checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements
	expectedCheckpointObjectTrailerSize += checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements
	expectedCheckpointObjectTrailerSize *= globals.elementOfBPlusTreeLayoutStructSize
	expectedCheckpointObjectTrailerSize += bytesConsumed

	if uint64(len(checkpointObjectTrailerBuf)) != expectedCheckpointObjectTrailerSize {
		err = fmt.Errorf("checkpointObjectTrailer for volume %v does not match required size", volumeView.volume.volumeName)
		return
	}

	for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
		checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
		bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}

		volumeView.inodeRecWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
	}

	for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
		checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
		bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}

		volumeView.logSegmentRecWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
	}

	for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements; layoutReportIndex++ {
		checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
		bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}

		volumeView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
	}

	err = nil
	return
}

func (volume *volumeStruct) getCheckpoint(autoFormat bool) (err error) {
	var (
		accountHeaderValues           []string
		accountHeaders                map[string][]string
		bytesNeeded                   uint64
		checkpointContainerHeaders    map[string][]string
		checkpointHeader              checkpointHeaderV2Struct
		checkpointHeaderValue         string
		checkpointHeaderValueSlice    []string
		checkpointHeaderValues        []string
		checkpointVersion             uint64
		computedCRC64                 uint64
		defaultReplayLogReadBuffer    []byte
		inodeIndex                    uint64
		inodeNumber                   uint64
		logSegmentNumber              uint64
		numInodes                     uint64
		objectNumber                  uint64
		ok                            bool
		replayLogReadBuffer           []byte
		replayLogReadBufferPosition   uint64
		replayLogPosition             int64
		replayLogSize                 int64
		replayLogTransactionFixedPart replayLogTransactionFixedPartStruct
		storagePolicyHeaderValues     []string
		value                         []byte
		valueLen                      uint64
	)

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
	} else {
		if (autoFormat) && (404 == blunder.HTTPCode(err)) {
			// Checkpoint Container not found... so try to create it with some initial values...

			checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber = 0
			checkpointHeader.CheckpointObjectTrailerV2StructObjectLength = 0

			checkpointHeader.ReservedToNonce = firstNonceToProvide // First FetchNonce() will trigger a reserve step

			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
				checkpointHeaderVersion2,
				checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber,
				checkpointHeader.CheckpointObjectTrailerV2StructObjectLength,
				checkpointHeader.ReservedToNonce,
			)

			checkpointHeaderValues = []string{checkpointHeaderValue}

			storagePolicyHeaderValues = []string{volume.checkpointContainerStoragePolicy}

			checkpointContainerHeaders = make(map[string][]string)

			checkpointContainerHeaders[CheckpointHeaderName] = checkpointHeaderValues
			checkpointContainerHeaders[StoragePolicyHeaderName] = storagePolicyHeaderValues

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

	if checkpointHeaderVersion2 == checkpointVersion {
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

		volume.liveView = &volumeViewStruct{volume: volume}

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

			volume.liveView.inodeRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			volume.liveView.inodeRecWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxInodesPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.inodeRecWrapper,
					globals.inodeRecCache)

			volume.liveView.logSegmentRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			volume.liveView.logSegmentRecWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxLogSegmentsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.logSegmentRecWrapper,
					globals.logSegmentRecCache)

			volume.liveView.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			volume.liveView.bPlusTreeObjectWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxDirFileNodesPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.bPlusTreeObjectWrapper,
					globals.bPlusTreeObjectCache)

			volume.liveView.createdObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			volume.liveView.createdObjectsWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxCreatedDeletedObjectsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.createdObjectsWrapper,
					globals.createdDeletedObjectsCache)

			volume.liveView.deletedObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			volume.liveView.deletedObjectsWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxCreatedDeletedObjectsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.createdObjectsWrapper,
					globals.createdDeletedObjectsCache)
		} else {
			// Load/process checkpointObjectTrailerV2Struct

			volume.checkpointObjectTrailer, err = volume.liveView.loadCheckpointObjectTrailerV2(volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber, volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectLength)
			if nil != err {
				return
			}
		}

		// Fake load of viewTreeBy{Nonce|ID|Time|Name}

		volume.viewTreeByNonce = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
		volume.viewTreeByID = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
		volume.viewTreeByTime = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
		volume.viewTreeByName = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
	} else if checkpointHeaderVersion3 == checkpointVersion {
		// TODO
	} else {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (version: %v not supported)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue, checkpointVersion)
		return
	}

	volume.nextNonce = volume.checkpointHeader.ReservedToNonce

	// Check for the need to process a Replay Log

	if "" == volume.replayLogFileName {
		// Replay Log is disabled... simply return now
		err = nil
		return
	}

	volume.replayLogFile, err = platform.OpenFileSync(volume.replayLogFileName, os.O_RDWR, 0600)
	if nil != err {
		if os.IsNotExist(err) {
			// No Replay Log found... simply return now
			err = nil
			return
		} else {
			logger.FatalfWithError(err, "platform.OpenFileSync(%v,os.O_RDWR,) failed", volume.replayLogFileName)
		}
	}

	// Compute current end of Replay Log and round it down to replayLogWriteBufferAlignment multiple if necessary

	replayLogSize, err = volume.replayLogFile.Seek(0, 2)
	if nil != err {
		return
	}
	replayLogSize = int64(uintptr(replayLogSize) & ^(replayLogWriteBufferAlignment - 1))

	// Seek back to start of Replay Log

	_, err = volume.replayLogFile.Seek(0, 0)
	replayLogPosition = 0

	defaultReplayLogReadBuffer = constructReplayLogWriteBuffer(globals.replayLogTransactionFixedPartStructSize)

	for replayLogPosition < replayLogSize {
		// Read next Transaction Header from Replay Log

		_, err = io.ReadFull(volume.replayLogFile, defaultReplayLogReadBuffer)
		if nil != err {
			return
		}

		_, err = cstruct.Unpack(defaultReplayLogReadBuffer, &replayLogTransactionFixedPart, LittleEndian)
		if nil != err {
			// Logic error - we should never fail cstruct.Unpack() call

			logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
		}

		// Ensure entire Transaction is in replayLogReadBuffer and we are positioned correctly

		bytesNeeded = globals.uint64Size + globals.uint64Size + replayLogTransactionFixedPart.BytesFollowing

		if bytesNeeded <= uint64(len(defaultReplayLogReadBuffer)) {
			// We've already read the entire Transaction

			replayLogReadBuffer = defaultReplayLogReadBuffer
		} else {
			// Back up and read entire Transaction into fresh replayLogReadBuffer

			_, err = volume.replayLogFile.Seek(replayLogPosition, 0)

			replayLogReadBuffer = constructReplayLogWriteBuffer(bytesNeeded)

			_, err = io.ReadFull(volume.replayLogFile, replayLogReadBuffer)
			if nil != err {
				return
			}
		}

		// Validate ECMA CRC-64 of Transaction

		computedCRC64 = crc64.Checksum(replayLogReadBuffer[globals.uint64Size:bytesNeeded], globals.crc64ECMATable)
		if computedCRC64 != replayLogTransactionFixedPart.CRC64 {
			// Corruption in replayLogTransactionFixedPart - so exit as if Replay Log ended here

			logger.Infof("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)

			_, err = volume.replayLogFile.Seek(replayLogPosition, 0)
			if nil != err {
				return
			}
			err = volume.replayLogFile.Truncate(replayLogPosition)
			return
		}

		// Replay Transaction

		replayLogReadBufferPosition = globals.replayLogTransactionFixedPartStructSize

		switch replayLogTransactionFixedPart.TransactionType {
		case transactionPutInodeRec:
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &inodeNumber, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}
			replayLogReadBufferPosition += globals.uint64Size
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &valueLen, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}
			replayLogReadBufferPosition += globals.uint64Size
			value = make([]byte, valueLen)
			copy(value, replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+valueLen])

			ok, err = volume.liveView.inodeRecWrapper.bPlusTree.PatchByKey(inodeNumber, value)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.inodeRecWrapper.bPlusTree.PatchByKey() failure: %v", volume.volumeName, err)
			}
			if !ok {
				_, err = volume.liveView.inodeRecWrapper.bPlusTree.Put(inodeNumber, value)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.inodeRecWrapper.bPlusTree.Put() failure: %v", volume.volumeName, err)
				}
			}
		case transactionPutInodeRecs:
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &numInodes, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}
			replayLogReadBufferPosition += globals.uint64Size
			for inodeIndex = 0; inodeIndex < numInodes; inodeIndex++ {
				_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &inodeNumber, LittleEndian)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
				}
				replayLogReadBufferPosition += globals.uint64Size
				_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &valueLen, LittleEndian)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
				}
				replayLogReadBufferPosition += globals.uint64Size
				value = make([]byte, valueLen)
				copy(value, replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+valueLen])
				replayLogReadBufferPosition += valueLen

				ok, err = volume.liveView.inodeRecWrapper.bPlusTree.PatchByKey(inodeNumber, value)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.inodeRecWrapper.bPlusTree.PatchByKey() failure: %v", volume.volumeName, err)
				}
				if !ok {
					_, err = volume.liveView.inodeRecWrapper.bPlusTree.Put(inodeNumber, value)
					if nil != err {
						logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.inodeRecWrapper.bPlusTree.Put() failure: %v", volume.volumeName, err)
					}
				}
			}
		case transactionDeleteInodeRec:
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &inodeNumber, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}

			_, err = volume.liveView.inodeRecWrapper.bPlusTree.DeleteByKey(inodeNumber)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.inodeRecWrapper.bPlusTree.DeleteByKey() failure: %v", volume.volumeName, err)
			}
		case transactionPutLogSegmentRec:
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &logSegmentNumber, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}
			replayLogReadBufferPosition += globals.uint64Size
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &valueLen, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}
			replayLogReadBufferPosition += globals.uint64Size
			value = make([]byte, valueLen)
			copy(value, replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+valueLen])

			ok, err = volume.liveView.logSegmentRecWrapper.bPlusTree.PatchByKey(logSegmentNumber, value)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.logSegmentRecWrapper.bPlusTree.PatchByKey() failure: %v", volume.volumeName, err)
			}
			if !ok {
				_, err = volume.liveView.logSegmentRecWrapper.bPlusTree.Put(logSegmentNumber, value)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.logSegmentRecWrapper.bPlusTree.Put() failure: %v", volume.volumeName, err)
				}
			}
		case transactionDeleteLogSegmentRec:
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &logSegmentNumber, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}

			_, err = volume.liveView.logSegmentRecWrapper.bPlusTree.DeleteByKey(logSegmentNumber)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.logSegmentRecWrapper.bPlusTree.DeleteByKey() failure: %v", volume.volumeName, err)
			}
		case transactionPutBPlusTreeObject:
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &objectNumber, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}
			replayLogReadBufferPosition += globals.uint64Size
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &valueLen, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}
			replayLogReadBufferPosition += globals.uint64Size
			value = make([]byte, valueLen)
			copy(value, replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+valueLen])

			ok, err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.PatchByKey(objectNumber, value)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.bPlusTreeObjectWrapper.bPlusTree.PatchByKey() failure: %v", volume.volumeName, err)
			}
			if !ok {
				_, err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.Put(objectNumber, value)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.bPlusTreeObjectWrapper.bPlusTree.Put() failure: %v", volume.volumeName, err)
				}
			}
		case transactionDeleteBPlusTreeObject:
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &objectNumber, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}

			_, err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.DeleteByKey(objectNumber)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.bPlusTreeObjectWrapper.bPlusTree.DeleteByKey() failure: %v", volume.volumeName, err)
			}
		default:
			// Corruption in replayLogTransactionFixedPart - so exit as if Replay Log ended here

			logger.Infof("Reply Log for Volume %s hit unexpected replayLogTransactionFixedPart.TransactionType == %v", volume.volumeName, replayLogTransactionFixedPart.TransactionType)

			_, err = volume.replayLogFile.Seek(replayLogPosition, 0)
			if nil != err {
				return
			}
			err = volume.replayLogFile.Truncate(replayLogPosition)
			return
		}

		// Finally, make replayLogPosition match where we actually are in volume.replayLogFile

		replayLogPosition += int64(len(replayLogReadBuffer))
	}

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
		err = volume.liveView.inodeRecWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber,
		volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset,
		volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength,
		err = volume.liveView.logSegmentRecWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber,
		volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset,
		volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength,
		err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}

	if !volume.checkpointFlushedData {
		return // since nothing was flushed, we can simply return
	}

	err = volume.liveView.inodeRecWrapper.bPlusTree.Prune()
	if nil != err {
		return
	}
	err = volume.liveView.logSegmentRecWrapper.bPlusTree.Prune()
	if nil != err {
		return
	}
	err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.Prune()
	if nil != err {
		return
	}

	volume.checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements = uint64(len(volume.liveView.inodeRecWrapper.trackingBPlusTreeLayout))
	volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements = uint64(len(volume.liveView.logSegmentRecWrapper.trackingBPlusTreeLayout))
	volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements = uint64(len(volume.liveView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout))

	checkpointTrailerBuf, err = cstruct.Pack(volume.checkpointObjectTrailer, LittleEndian)
	if nil != err {
		return
	}

	treeLayoutBufSize = volume.checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += volume.checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += volume.checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements
	treeLayoutBufSize *= globals.elementOfBPlusTreeLayoutStructSize

	treeLayoutBuf = make([]byte, 0, treeLayoutBufSize)

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.liveView.inodeRecWrapper.trackingBPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.liveView.logSegmentRecWrapper.trackingBPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			return
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.liveView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout {
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

	volume.checkpointHeader.CheckpointObjectTrailerV2StructObjectNumber = volume.checkpointChunkedPutContextObjectNumber
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

	if nil != volume.replayLogFile {
		err = volume.replayLogFile.Close()
		if nil != err {
			return
		}
		volume.replayLogFile = nil
	}

	if "" != volume.replayLogFileName {
		err = os.Remove(volume.replayLogFileName)
		if nil != err {
			if !os.IsNotExist(err) {
				return
			}
		}
	}

	combinedBPlusTreeLayout = make(sortedmap.LayoutReport)

	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.inodeRecWrapper.trackingBPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.logSegmentRecWrapper.trackingBPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}

	for objectNumber, bytesUsedCumulative = range combinedBPlusTreeLayout {
		if 0 == bytesUsedCumulative {
			delete(volume.liveView.inodeRecWrapper.trackingBPlusTreeLayout, objectNumber)
			delete(volume.liveView.logSegmentRecWrapper.trackingBPlusTreeLayout, objectNumber)
			delete(volume.liveView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout, objectNumber)
			volume.delayedObjectDeleteSSTODOList = append(volume.delayedObjectDeleteSSTODOList, delayedObjectDeleteSSTODOStruct{containerName: volume.checkpointContainerName, objectNumber: objectNumber})
		}
	}

	if 0 < len(volume.delayedObjectDeleteSSTODOList) {
		volume.backgroundObjectDeleteWG.Add(1)
		go volume.performDelayedObjectDeletes(volume.delayedObjectDeleteSSTODOList)
		volume.delayedObjectDeleteSSTODOList = make([]delayedObjectDeleteSSTODOStruct, 0)
	}

	err = nil
	return
}

func (volume *volumeStruct) performDelayedObjectDeletes(delayedObjectDeleteSSTODOList []delayedObjectDeleteSSTODOStruct) {
	for _, delayedObjectDeleteSSTODO := range delayedObjectDeleteSSTODOList {
		err := swiftclient.ObjectDelete(
			volume.accountName,
			delayedObjectDeleteSSTODO.containerName,
			utils.Uint64ToHexStr(delayedObjectDeleteSSTODO.objectNumber),
			swiftclient.SkipRetry)
		if nil != err {
			logger.Errorf("DELETE %v/%v/%016X failed with err: %v", volume.accountName, delayedObjectDeleteSSTODO.containerName, delayedObjectDeleteSSTODO.objectNumber, err)
		}
	}
	volume.backgroundObjectDeleteWG.Done()
}

func (volume *volumeStruct) openCheckpointChunkedPutContextIfNecessary() (err error) {
	if nil == volume.checkpointChunkedPutContext {
		volume.checkpointChunkedPutContextObjectNumber, err = volume.fetchNonceWhileLocked()
		if nil != err {
			return
		}
		volume.checkpointChunkedPutContext, err =
			swiftclient.ObjectFetchChunkedPutContext(volume.accountName,
				volume.checkpointContainerName,
				utils.Uint64ToHexStr(volume.checkpointChunkedPutContextObjectNumber))
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

		evtlog.Record(evtlog.FormatHeadhunterCheckpointStart, volume.volumeName)

		checkpointRequest.err = volume.putCheckpoint()

		if nil == checkpointRequest.err {
			evtlog.Record(evtlog.FormatHeadhunterCheckpointEndSuccess, volume.volumeName)
		} else {
			// As part of conducting the checkpoint - and depending upon where the early non-nil
			// error was reported - it is highly likely that e.g. pages of the B+Trees have been
			// marked clean even though either their dirty data has not been successfully posted
			// to Swift and/or the Checkpoint Header that points to it has not been successfully
			// recorded in Swift. In either case, a subsequent checkpoint may, indeed, appear to
			// succeed and quite probably miss some of the references nodes of the B+Trees not
			// having made it to Swift... and, yet, wrongly presume all is (now) well.

			// It should also be noted that other activity (e.g. garbage collection of usually
			// now unreferenced data) awaiting completion of this checkpoint should not have
			// been allowed to proceed.

			// For now, we will instead promptly fail right here thus preventing that subsequent
			// checkpoint from masking the data loss. While there are alternatives (e.g. going
			// back and marking every node of the B+Trees as being dirty - or at least those that
			// were marked clean), such an approach will not be pursued at this time.

			evtlog.Record(evtlog.FormatHeadhunterCheckpointEndFailure, volume.volumeName, checkpointRequest.err.Error())
			logger.FatalfWithError(checkpointRequest.err, "Shutting down to prevent subsequent checkpoints from corrupting Swift")
		}

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
