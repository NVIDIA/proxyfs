package headhunter

import (
	"container/list"
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
	checkpointVersion2 uint64 = iota + 2
	checkpointVersion3
	// uint64 in %016X indicating checkpointVersion2 or checkpointVersion3
	// ' '
	// uint64 in %016X indicating objectNumber containing checkpoint record at tail of object
	// ' '
	// uint64 in %016X indicating length of               checkpoint record at tail of object
	// ' '
	// uint64 in %016X indicating reservedToNonce
)

type checkpointHeaderStruct struct {
	checkpointVersion                         uint64 // either checkpointVersion2 or checkpointVersion3
	checkpointObjectTrailerStructObjectNumber uint64 // checkpointObjectTrailerV?Struct found at "tail" of object
	checkpointObjectTrailerStructObjectLength uint64 // this length includes appended non-fixed sized arrays
	reservedToNonce                           uint64 // highest nonce value reserved
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
	InodeRecBPlusTreeLayoutNumElements        uint64 // elements immediately follow checkpointObjectTrailerV3Struct
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
	inodeRecBPlusTreeObjectNumber             uint64
	inodeRecBPlusTreeObjectOffset             uint64
	inodeRecBPlusTreeObjectLength             uint64
	logSegmentRecBPlusTreeObjectNumber        uint64
	logSegmentRecBPlusTreeObjectOffset        uint64
	logSegmentRecBPlusTreeObjectLength        uint64
	bPlusTreeObjectBPlusTreeObjectNumber      uint64
	bPlusTreeObjectBPlusTreeObjectOffset      uint64
	bPlusTreeObjectBPlusTreeLayoutNumElements uint64
	createdObjectsBPlusTreeObjectNumber       uint64
	createdObjectsBPlusTreeObjectOffset       uint64
	createdObjectsBPlusTreeObjectLength       uint64
	createdObjectsBPlusTreeLayoutNumElements  uint64
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
	CRC64                                         uint64 // checksum of everything after this field
	BytesFollowing                                uint64 // bytes following in this transaction
	LastCheckpointObjectTrailerStructObjectNumber uint64 // last checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
	TransactionType                               uint64 // transactionType from above const() block
}

type delayedObjectDeleteStruct struct {
	containerName string
	objectNumber  uint64
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
				globals.uint64Size + //               last checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
				globals.uint64Size + //               transactionType == transactionDeleteInodeRec
				globals.uint64Size //                 inodeNumber
	case transactionPutLogSegmentRec:
		singleKey = keys.(uint64)
		singleValue = values.([]byte)
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
				globals.uint64Size + //               transactionType == transactionDeleteLogSegmentRec
				globals.uint64Size //                 logSegmentNumber
	case transactionPutBPlusTreeObject:
		singleKey = keys.(uint64)
		singleValue = values.([]byte)
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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

	// Fill in last checkpoint's checkpointHeaderStruct.checkpointObjectTrailerStructObjectNumber

	packedUint64, err = cstruct.Pack(volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber, LittleEndian)
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

	if 4 != len(checkpointHeaderValueSlice) {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (wrong number of fields)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
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

func (volume *volumeStruct) getCheckpoint(autoFormat bool) (err error) {
	var (
		accountHeaderValues                                    []string
		accountHeaders                                         map[string][]string
		bytesConsumed                                          uint64
		bytesNeeded                                            uint64
		checkpointContainerHeaders                             map[string][]string
		checkpointHeader                                       checkpointHeaderStruct
		checkpointHeaderValue                                  string
		checkpointHeaderValueSlice                             []string
		checkpointHeaderValues                                 []string
		checkpointObjectTrailerBuf                             []byte
		checkpointObjectTrailerV2                              *checkpointObjectTrailerV2Struct
		checkpointObjectTrailerV3                              *checkpointObjectTrailerV3Struct
		computedCRC64                                          uint64
		defaultReplayLogReadBuffer                             []byte
		elementOfBPlusTreeLayout                               elementOfBPlusTreeLayoutStruct
		expectedCheckpointObjectTrailerSize                    uint64
		inodeIndex                                             uint64
		inodeNumber                                            uint64
		layoutReportIndex                                      uint64
		logSegmentNumber                                       uint64
		numInodes                                              uint64
		objectNumber                                           uint64
		ok                                                     bool
		replayLogReadBuffer                                    []byte
		replayLogReadBufferPosition                            uint64
		replayLogPosition                                      int64
		replayLogSize                                          int64
		replayLogTransactionFixedPart                          replayLogTransactionFixedPartStruct
		snapShotCreatedObjectsBPlusTreeLayoutNumElementsStruct uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectLengthStruct      uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectNumberStruct      uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectOffsetStruct      uint64Struct
		snapShotDeletedObjectsBPlusTreeLayoutNumElementsStruct uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectLengthStruct      uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectNumberStruct      uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectOffsetStruct      uint64Struct
		snapShotID                                             uint64
		snapShotIDStruct                                       uint64Struct
		snapShotIndex                                          uint64
		snapShotNameBuf                                        []byte
		snapShotNameBufLenStruct                               uint64Struct
		snapShotNonceStruct                                    uint64Struct
		snapShotTimeStampBuf                                   []byte
		snapShotTimeStampBufLenStruct                          uint64Struct
		storagePolicyHeaderValues                              []string
		value                                                  []byte
		valueLen                                               uint64
		volumeView                                             *volumeViewStruct
		volumeViewAsValue                                      sortedmap.Value
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

			checkpointHeader.checkpointVersion = checkpointVersion3

			checkpointHeader.checkpointObjectTrailerStructObjectNumber = 0
			checkpointHeader.checkpointObjectTrailerStructObjectLength = 0

			checkpointHeader.reservedToNonce = firstNonceToProvide // First FetchNonce() will trigger a reserve step

			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
				checkpointHeader.checkpointVersion,
				checkpointHeader.checkpointObjectTrailerStructObjectNumber,
				checkpointHeader.checkpointObjectTrailerStructObjectLength,
				checkpointHeader.reservedToNonce,
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

	if 4 != len(checkpointHeaderValueSlice) {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (wrong number of fields)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
		return
	}

	volume.checkpointHeader = &checkpointHeaderStruct{}

	volume.checkpointHeader.checkpointVersion, err = strconv.ParseUint(checkpointHeaderValueSlice[0], 16, 64)
	if nil != err {
		return
	}

	volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSlice[1], 16, 64)
	if nil != err {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad objectNumber)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
		return
	}

	volume.checkpointHeader.checkpointObjectTrailerStructObjectLength, err = strconv.ParseUint(checkpointHeaderValueSlice[2], 16, 64)
	if nil != err {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad objectLength)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
		return
	}

	volume.checkpointHeader.reservedToNonce, err = strconv.ParseUint(checkpointHeaderValueSlice[3], 16, 64)
	if nil != err {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (bad nextNonce)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue)
		return
	}

	volume.liveView = &volumeViewStruct{volume: volume}

	if checkpointVersion2 == volume.checkpointHeader.checkpointVersion {
		if 0 == volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber {
			// Initialize based on zero-filled checkpointObjectTrailerV2Struct

			checkpointObjectTrailerV3 = &checkpointObjectTrailerV3Struct{
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
				SnapShotIDNumBits:                         uint64(volume.snapShotIDNumBits),
				SnapShotListNumElements:                   0,
				SnapShotListTotalSize:                     0,
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
					volume.liveView.deletedObjectsWrapper,
					globals.createdDeletedObjectsCache)
		} else {
			// Read in checkpointObjectTrailerV2Struct

			checkpointObjectTrailerBuf, err =
				swiftclient.ObjectTail(
					volume.accountName,
					volume.checkpointContainerName,
					utils.Uint64ToHexStr(volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber),
					volume.checkpointHeader.checkpointObjectTrailerStructObjectLength)
			if nil != err {
				return
			}

			checkpointObjectTrailerV2 = &checkpointObjectTrailerV2Struct{}

			bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, checkpointObjectTrailerV2, LittleEndian)
			if nil != err {
				return
			}

			// Convert checkpointObjectTrailerV2Struct to a checkpointObjectTrailerV3Struct

			checkpointObjectTrailerV3 = &checkpointObjectTrailerV3Struct{
				InodeRecBPlusTreeObjectNumber:             checkpointObjectTrailerV2.InodeRecBPlusTreeObjectNumber,
				InodeRecBPlusTreeObjectOffset:             checkpointObjectTrailerV2.InodeRecBPlusTreeObjectOffset,
				InodeRecBPlusTreeObjectLength:             checkpointObjectTrailerV2.InodeRecBPlusTreeObjectLength,
				InodeRecBPlusTreeLayoutNumElements:        checkpointObjectTrailerV2.InodeRecBPlusTreeLayoutNumElements,
				LogSegmentRecBPlusTreeObjectNumber:        checkpointObjectTrailerV2.LogSegmentRecBPlusTreeObjectNumber,
				LogSegmentRecBPlusTreeObjectOffset:        checkpointObjectTrailerV2.LogSegmentRecBPlusTreeObjectOffset,
				LogSegmentRecBPlusTreeObjectLength:        checkpointObjectTrailerV2.LogSegmentRecBPlusTreeObjectLength,
				LogSegmentRecBPlusTreeLayoutNumElements:   checkpointObjectTrailerV2.LogSegmentRecBPlusTreeLayoutNumElements,
				BPlusTreeObjectBPlusTreeObjectNumber:      checkpointObjectTrailerV2.BPlusTreeObjectBPlusTreeObjectNumber,
				BPlusTreeObjectBPlusTreeObjectOffset:      checkpointObjectTrailerV2.BPlusTreeObjectBPlusTreeObjectOffset,
				BPlusTreeObjectBPlusTreeObjectLength:      checkpointObjectTrailerV2.BPlusTreeObjectBPlusTreeObjectLength,
				BPlusTreeObjectBPlusTreeLayoutNumElements: checkpointObjectTrailerV2.BPlusTreeObjectBPlusTreeLayoutNumElements,
				SnapShotIDNumBits:                         uint64(volume.snapShotIDNumBits),
				SnapShotListNumElements:                   0,
				SnapShotListTotalSize:                     0,
			}

			// Load liveView.{inodeRec|logSegmentRec|bPlusTreeObject}Wrapper B+Trees

			volume.liveView.inodeRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			if 0 == checkpointObjectTrailerV3.InodeRecBPlusTreeObjectNumber {
				volume.liveView.inodeRecWrapper.bPlusTree =
					sortedmap.NewBPlusTree(
						volume.maxInodesPerMetadataNode,
						sortedmap.CompareUint64,
						volume.liveView.inodeRecWrapper,
						globals.inodeRecCache)
			} else {
				volume.liveView.inodeRecWrapper.bPlusTree, err =
					sortedmap.OldBPlusTree(
						checkpointObjectTrailerV3.InodeRecBPlusTreeObjectNumber,
						checkpointObjectTrailerV3.InodeRecBPlusTreeObjectOffset,
						checkpointObjectTrailerV3.InodeRecBPlusTreeObjectLength,
						sortedmap.CompareUint64,
						volume.liveView.inodeRecWrapper,
						globals.inodeRecCache)
				if nil != err {
					return
				}
			}

			volume.liveView.logSegmentRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			if 0 == checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectNumber {
				volume.liveView.logSegmentRecWrapper.bPlusTree =
					sortedmap.NewBPlusTree(
						volume.maxLogSegmentsPerMetadataNode,
						sortedmap.CompareUint64,
						volume.liveView.logSegmentRecWrapper,
						globals.logSegmentRecCache)
			} else {
				volume.liveView.logSegmentRecWrapper.bPlusTree, err =
					sortedmap.OldBPlusTree(
						checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectNumber,
						checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectOffset,
						checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectLength,
						sortedmap.CompareUint64,
						volume.liveView.logSegmentRecWrapper,
						globals.logSegmentRecCache)
				if nil != err {
					return
				}
			}

			volume.liveView.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			if 0 == checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectNumber {
				volume.liveView.bPlusTreeObjectWrapper.bPlusTree =
					sortedmap.NewBPlusTree(
						volume.maxDirFileNodesPerMetadataNode,
						sortedmap.CompareUint64,
						volume.liveView.bPlusTreeObjectWrapper,
						globals.bPlusTreeObjectCache)
			} else {
				volume.liveView.bPlusTreeObjectWrapper.bPlusTree, err =
					sortedmap.OldBPlusTree(
						checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectNumber,
						checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectOffset,
						checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectLength,
						sortedmap.CompareUint64,
						volume.liveView.bPlusTreeObjectWrapper,
						globals.bPlusTreeObjectCache)
				if nil != err {
					return
				}
			}

			// Fake load liveView.{createdObjects|deletedObjects}Wrapper B+Trees

			volume.liveView.createdObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport), // Nothing to deserialize into this
			}

			volume.liveView.createdObjectsWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxCreatedDeletedObjectsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.createdObjectsWrapper,
					globals.createdDeletedObjectsCache)

			volume.liveView.deletedObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport), // Nothing to deserialize into this
			}

			volume.liveView.deletedObjectsWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxCreatedDeletedObjectsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.deletedObjectsWrapper,
					globals.createdDeletedObjectsCache)

			// Deserialize liveView.{inodeRec|logSegmentRec|bPlusTreeObject}Wrapper LayoutReports

			expectedCheckpointObjectTrailerSize = checkpointObjectTrailerV3.InodeRecBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize += checkpointObjectTrailerV3.LogSegmentRecBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize += checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize *= globals.elementOfBPlusTreeLayoutStructSize
			expectedCheckpointObjectTrailerSize += bytesConsumed

			if uint64(len(checkpointObjectTrailerBuf)) != expectedCheckpointObjectTrailerSize {
				err = fmt.Errorf("checkpointObjectTrailer for volume %v does not match required size", volume.volumeName)
				return
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.InodeRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}

				volume.liveView.inodeRecWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.LogSegmentRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}

				volume.liveView.logSegmentRecWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeLayoutNumElements; layoutReportIndex++ {
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}

				volume.liveView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}
		}

		// Compute SnapShotID shotcuts

		volume.snapShotIDShift = uint64(64) - uint64(volume.snapShotIDNumBits)
		volume.dotSnapShotDirSnapShotID = (uint64(1) << uint64(volume.snapShotIDNumBits)) - uint64(1)

		// Fake load of viewTreeBy{Nonce|ID|Time|Name}

		volume.viewTreeByNonce = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
		volume.viewTreeByID = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
		volume.viewTreeByTime = sortedmap.NewLLRBTree(sortedmap.CompareTime, nil)
		volume.viewTreeByName = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

		volume.priorView = nil

		// Fake derivation of available SnapShotIDs

		volume.availableSnapShotIDList = list.New()

		for snapShotID = uint64(1); snapShotID < volume.dotSnapShotDirSnapShotID; snapShotID++ {
			volume.availableSnapShotIDList.PushBack(snapShotID)
		}
	} else if checkpointVersion3 == volume.checkpointHeader.checkpointVersion {
		if 0 == volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber {
			// Initialize based on zero-filled checkpointObjectTrailerV3Struct

			checkpointObjectTrailerV3 = &checkpointObjectTrailerV3Struct{
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
				SnapShotIDNumBits:                         uint64(volume.snapShotIDNumBits),
				SnapShotListNumElements:                   0,
				SnapShotListTotalSize:                     0,
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
					volume.liveView.deletedObjectsWrapper,
					globals.createdDeletedObjectsCache)

			// Compute SnapShotID shortcuts

			volume.snapShotIDShift = uint64(64) - uint64(volume.snapShotIDNumBits)
			volume.dotSnapShotDirSnapShotID = (uint64(1) << uint64(volume.snapShotIDNumBits)) - uint64(1)

			// Initialize viewTreeBy{Nonce|ID|Time|Name}

			volume.viewTreeByNonce = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
			volume.viewTreeByID = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
			volume.viewTreeByTime = sortedmap.NewLLRBTree(sortedmap.CompareTime, nil)
			volume.viewTreeByName = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

			volume.priorView = nil

			// Initialize list of available SnapShotIDs

			volume.availableSnapShotIDList = list.New()

			for snapShotID = uint64(1); snapShotID < volume.dotSnapShotDirSnapShotID; snapShotID++ {
				volume.availableSnapShotIDList.PushBack(snapShotID)
			}
		} else {
			// Read in checkpointObjectTrailerV3Struct

			checkpointObjectTrailerBuf, err =
				swiftclient.ObjectTail(
					volume.accountName,
					volume.checkpointContainerName,
					utils.Uint64ToHexStr(volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber),
					volume.checkpointHeader.checkpointObjectTrailerStructObjectLength)
			if nil != err {
				return
			}

			checkpointObjectTrailerV3 = &checkpointObjectTrailerV3Struct{}

			bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, checkpointObjectTrailerV3, LittleEndian)
			if nil != err {
				return
			}
			checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

			// Load liveView.{inodeRec|logSegmentRec|bPlusTreeObject}Wrapper B+Trees

			volume.liveView.inodeRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			if 0 == checkpointObjectTrailerV3.InodeRecBPlusTreeObjectNumber {
				volume.liveView.inodeRecWrapper.bPlusTree =
					sortedmap.NewBPlusTree(
						volume.maxInodesPerMetadataNode,
						sortedmap.CompareUint64,
						volume.liveView.inodeRecWrapper,
						globals.inodeRecCache)
			} else {
				volume.liveView.inodeRecWrapper.bPlusTree, err =
					sortedmap.OldBPlusTree(
						checkpointObjectTrailerV3.InodeRecBPlusTreeObjectNumber,
						checkpointObjectTrailerV3.InodeRecBPlusTreeObjectOffset,
						checkpointObjectTrailerV3.InodeRecBPlusTreeObjectLength,
						sortedmap.CompareUint64,
						volume.liveView.inodeRecWrapper,
						globals.inodeRecCache)
				if nil != err {
					return
				}
			}

			volume.liveView.logSegmentRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			if 0 == checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectNumber {
				volume.liveView.logSegmentRecWrapper.bPlusTree =
					sortedmap.NewBPlusTree(
						volume.maxLogSegmentsPerMetadataNode,
						sortedmap.CompareUint64,
						volume.liveView.logSegmentRecWrapper,
						globals.logSegmentRecCache)
			} else {
				volume.liveView.logSegmentRecWrapper.bPlusTree, err =
					sortedmap.OldBPlusTree(
						checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectNumber,
						checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectOffset,
						checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectLength,
						sortedmap.CompareUint64,
						volume.liveView.logSegmentRecWrapper,
						globals.logSegmentRecCache)
				if nil != err {
					return
				}
			}

			volume.liveView.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
			}

			if 0 == checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectNumber {
				volume.liveView.bPlusTreeObjectWrapper.bPlusTree =
					sortedmap.NewBPlusTree(
						volume.maxDirFileNodesPerMetadataNode,
						sortedmap.CompareUint64,
						volume.liveView.bPlusTreeObjectWrapper,
						globals.bPlusTreeObjectCache)
			} else {
				volume.liveView.bPlusTreeObjectWrapper.bPlusTree, err =
					sortedmap.OldBPlusTree(
						checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectNumber,
						checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectOffset,
						checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectLength,
						sortedmap.CompareUint64,
						volume.liveView.bPlusTreeObjectWrapper,
						globals.bPlusTreeObjectCache)
				if nil != err {
					return
				}
			}

			// Initialize liveView.{createdObjects|deletedObjects}Wrapper B+Trees

			volume.liveView.createdObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport), // Nothing to deserialize into this
			}

			volume.liveView.createdObjectsWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxCreatedDeletedObjectsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.createdObjectsWrapper,
					globals.createdDeletedObjectsCache)

			volume.liveView.deletedObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:              volume.liveView,
				trackingBPlusTreeLayout: make(sortedmap.LayoutReport), // Nothing to deserialize into this
			}

			volume.liveView.deletedObjectsWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxCreatedDeletedObjectsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.deletedObjectsWrapper,
					globals.createdDeletedObjectsCache)

			// Validate size of checkpointObjectTrailerBuf

			expectedCheckpointObjectTrailerSize = checkpointObjectTrailerV3.InodeRecBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize += checkpointObjectTrailerV3.LogSegmentRecBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize += checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize *= globals.elementOfBPlusTreeLayoutStructSize
			expectedCheckpointObjectTrailerSize += checkpointObjectTrailerV3.SnapShotListTotalSize

			if uint64(len(checkpointObjectTrailerBuf)) != expectedCheckpointObjectTrailerSize {
				err = fmt.Errorf("checkpointObjectTrailer for volume %v does not match required size", volume.volumeName)
				return
			}

			// Deserialize liveView.{inodeRec|logSegmentRec|bPlusTreeObject}Wrapper LayoutReports

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.InodeRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volume.liveView.inodeRecWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.LogSegmentRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volume.liveView.logSegmentRecWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeLayoutNumElements; layoutReportIndex++ {
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volume.liveView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			// Compute SnapShotID shortcuts

			volume.snapShotIDShift = uint64(64) - uint64(volume.snapShotIDNumBits)
			volume.dotSnapShotDirSnapShotID = (uint64(1) << uint64(volume.snapShotIDNumBits)) - uint64(1)

			// Load SnapShotList

			volume.viewTreeByNonce = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
			volume.viewTreeByID = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
			volume.viewTreeByTime = sortedmap.NewLLRBTree(sortedmap.CompareTime, nil)
			volume.viewTreeByName = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

			// Load of viewTreeBy{Nonce|ID|Time|Name}

			for snapShotIndex = 0; snapShotIndex < checkpointObjectTrailerV3.SnapShotListNumElements; snapShotIndex++ {
				volumeView = &volumeViewStruct{volume: volume}

				// elementOfSnapShotListStruct.nonce

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the nonce", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotNonceStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				volumeView.nonce = snapShotNonceStruct.u64
				_, ok, err = volume.viewTreeByNonce.GetByKey(volumeView.nonce)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByNonce.GetByKey(%v) for SnapShotList element %v failed: %v", volume.volumeName, volumeView.nonce, snapShotIndex, err)
				}
				if ok {
					err = fmt.Errorf("Volume %v's viewTreeByNonce already contained nonce %v for SnapShotList element %v ", volume.volumeName, volumeView.nonce, snapShotIndex)
					return
				}

				// elementOfSnapShotListStruct.id

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the id", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotIDStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				volumeView.snapShotID = snapShotIDStruct.u64
				if volumeView.snapShotID >= volume.dotSnapShotDirSnapShotID {
					err = fmt.Errorf("Invalid volumeView.snapShotID (%v) for configured volume.snapShotIDNumBits (%v)", volumeView.snapShotID, volume.snapShotIDNumBits)
					return
				}
				_, ok, err = volume.viewTreeByID.GetByKey(volumeView.snapShotID)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByID.GetByKey(%v) for SnapShotList element %v failed: %v", volume.volumeName, volumeView.snapShotID, snapShotIndex, err)
				}
				if ok {
					err = fmt.Errorf("Volume %v's viewTreeByID already contained snapShotID %v for SnapShotList element %v ", volume.volumeName, volumeView.snapShotID, snapShotIndex)
					return
				}

				// elementOfSnapShotListStruct.timeStamp

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the timeStamp len", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotTimeStampBufLenStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				if uint64(len(checkpointObjectTrailerBuf)) < snapShotTimeStampBufLenStruct.u64 {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the timeStamp", volume.volumeName, snapShotIndex)
					return
				}
				snapShotTimeStampBuf = checkpointObjectTrailerBuf[:snapShotTimeStampBufLenStruct.u64]
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[snapShotTimeStampBufLenStruct.u64:]
				err = volumeView.snapShotTime.UnmarshalBinary(snapShotTimeStampBuf)
				if nil != err {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v's' timeStamp (err: %v)", volume.volumeName, snapShotIndex, err)
					return
				}
				_, ok, err = volume.viewTreeByTime.GetByKey(volumeView.snapShotTime)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByTime.GetByKey(%v) for SnapShotList element %v failed: %v", volume.volumeName, volumeView.snapShotTime, snapShotIndex, err)
				}
				if ok {
					err = fmt.Errorf("Volume %v's viewTreeByTime already contained snapShotTime %v for SnapShotList element %v ", volume.volumeName, volumeView.snapShotTime, snapShotIndex)
					return
				}

				// elementOfSnapShotListStruct.name

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the name len", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotNameBufLenStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				if uint64(len(checkpointObjectTrailerBuf)) < snapShotNameBufLenStruct.u64 {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the name", volume.volumeName, snapShotIndex)
					return
				}
				snapShotNameBuf = checkpointObjectTrailerBuf[:snapShotNameBufLenStruct.u64]
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[snapShotNameBufLenStruct.u64:]
				volumeView.snapShotName = utils.ByteSliceToString(snapShotNameBuf)
				_, ok, err = volume.viewTreeByName.GetByKey(volumeView.snapShotName)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByName.GetByKey(%v) for SnapShotList element %v failed: %v", volume.volumeName, volumeView.snapShotName, snapShotIndex, err)
				}
				if ok {
					err = fmt.Errorf("Volume %v's viewTreeByName already contained snapShotName %v for SnapShotList element %v ", volume.volumeName, volumeView.snapShotName, snapShotIndex)
					return
				}

				// elementOfSnapShotListStruct.createdObjectsBPlusTreeObjectNumber

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the createdObjectsBPlusTreeObjectNumber", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotCreatedObjectsBPlusTreeObjectNumberStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// elementOfSnapShotListStruct.createdObjectsBPlusTreeObjectOffset

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the createdObjectsBPlusTreeObjectOffset", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotCreatedObjectsBPlusTreeObjectOffsetStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// elementOfSnapShotListStruct.createdObjectsBPlusTreeObjectLength

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the createdObjectsBPlusTreeObjectLength", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotCreatedObjectsBPlusTreeObjectLengthStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volumeView.createdObjectsWrapper = &bPlusTreeWrapperStruct{
					volumeView:              volumeView,
					trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
				}

				if 0 == snapShotCreatedObjectsBPlusTreeObjectNumberStruct.u64 {
					volumeView.createdObjectsWrapper.bPlusTree =
						sortedmap.NewBPlusTree(
							volume.maxCreatedDeletedObjectsPerMetadataNode,
							sortedmap.CompareUint64,
							volumeView.createdObjectsWrapper,
							globals.createdDeletedObjectsCache)
				} else {
					volumeView.createdObjectsWrapper.bPlusTree, err =
						sortedmap.OldBPlusTree(
							snapShotCreatedObjectsBPlusTreeObjectNumberStruct.u64,
							snapShotCreatedObjectsBPlusTreeObjectOffsetStruct.u64,
							snapShotCreatedObjectsBPlusTreeObjectLengthStruct.u64,
							sortedmap.CompareUint64,
							volumeView.createdObjectsWrapper,
							globals.createdDeletedObjectsCache)
					if nil != err {
						return
					}
				}

				// elementOfSnapShotListStruct.createdObjectsBPlusTreeLayoutNumElements

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the createdObjectsBPlusTreeLayoutNumElements", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotCreatedObjectsBPlusTreeLayoutNumElementsStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				if uint64(len(checkpointObjectTrailerBuf)) < (snapShotCreatedObjectsBPlusTreeLayoutNumElementsStruct.u64 * globals.elementOfBPlusTreeLayoutStructSize) {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the createdObjectsBPlusTreeLayout", volume.volumeName, snapShotIndex)
					return
				}
				for layoutReportIndex = 0; layoutReportIndex < snapShotCreatedObjectsBPlusTreeLayoutNumElementsStruct.u64; layoutReportIndex++ {
					bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
					if nil != err {
						return
					}
					checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

					volumeView.createdObjectsWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
				}

				// elementOfSnapShotListStruct.deletedObjectsBPlusTreeObjectNumber

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the deletedObjectsBPlusTreeObjectNumber", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotDeletedObjectsBPlusTreeObjectNumberStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// elementOfSnapShotListStruct.deletedObjectsBPlusTreeObjectOffset

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the deletedObjectsBPlusTreeObjectOffset", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotDeletedObjectsBPlusTreeObjectOffsetStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// elementOfSnapShotListStruct.deletedObjectsBPlusTreeObjectLength

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the deletedObjectsBPlusTreeObjectLength", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotDeletedObjectsBPlusTreeObjectLengthStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volumeView.deletedObjectsWrapper = &bPlusTreeWrapperStruct{
					volumeView:              volumeView,
					trackingBPlusTreeLayout: make(sortedmap.LayoutReport),
				}

				if 0 == snapShotDeletedObjectsBPlusTreeObjectNumberStruct.u64 {
					volumeView.deletedObjectsWrapper.bPlusTree =
						sortedmap.NewBPlusTree(
							volume.maxCreatedDeletedObjectsPerMetadataNode,
							sortedmap.CompareUint64,
							volumeView.deletedObjectsWrapper,
							globals.createdDeletedObjectsCache)
				} else {
					volumeView.createdObjectsWrapper.bPlusTree, err =
						sortedmap.OldBPlusTree(
							snapShotDeletedObjectsBPlusTreeObjectNumberStruct.u64,
							snapShotDeletedObjectsBPlusTreeObjectOffsetStruct.u64,
							snapShotDeletedObjectsBPlusTreeObjectLengthStruct.u64,
							sortedmap.CompareUint64,
							volumeView.deletedObjectsWrapper,
							globals.createdDeletedObjectsCache)
					if nil != err {
						return
					}
				}

				// elementOfSnapShotListStruct.deletedObjectsBPlusTreeLayoutNumElements

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the deletedObjectsBPlusTreeLayoutNumElements", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotDeletedObjectsBPlusTreeLayoutNumElementsStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				if uint64(len(checkpointObjectTrailerBuf)) < (snapShotDeletedObjectsBPlusTreeLayoutNumElementsStruct.u64 * globals.elementOfBPlusTreeLayoutStructSize) {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the deletedObjectsBPlusTreeLayout", volume.volumeName, snapShotIndex)
					return
				}
				for layoutReportIndex = 0; layoutReportIndex < snapShotDeletedObjectsBPlusTreeLayoutNumElementsStruct.u64; layoutReportIndex++ {
					bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
					if nil != err {
						return
					}
					checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

					volumeView.deletedObjectsWrapper.trackingBPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
				}

				// Insert volumeView into viewTreeBy{Nonce|ID|Time|Name}

				_, err = volume.viewTreeByNonce.Put(volumeView.nonce, volumeView)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByNonce.Put() for SnapShotList element %v failed: %v", volume.volumeName, snapShotIndex, err)
				}
				_, err = volume.viewTreeByID.Put(volumeView.snapShotID, volumeView)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByID.Put() for SnapShotList element %v failed: %v", volume.volumeName, snapShotIndex, err)
				}
				_, err = volume.viewTreeByTime.Put(volumeView.snapShotTime, volumeView)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByTime.Put() for SnapShotList element %v failed: %v", volume.volumeName, snapShotIndex, err)
				}
				_, err = volume.viewTreeByName.Put(volumeView.snapShotName, volumeView)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByName.Put() for SnapShotList element %v failed: %v", volume.volumeName, snapShotIndex, err)
				}
			}

			if 0 == checkpointObjectTrailerV3.SnapShotListNumElements {
				volume.priorView = nil
			} else {
				_, volumeViewAsValue, ok, err = volume.viewTreeByNonce.GetByIndex(int(checkpointObjectTrailerV3.SnapShotListNumElements) - 1)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByID.GetByIndex() failed: %v", volume.volumeName, err)
				}
				if !ok {
					logger.Fatalf("Logic error - volume %v's viewTreeByID.GetByIndex() returned !ok", volume.volumeName)
				}
				volume.priorView, ok = volumeViewAsValue.(*volumeViewStruct)
				if !ok {
					logger.Fatalf("Logic error - volume %v's volumeViewAsValue.(*volumeViewStruct) returned !ok", volume.volumeName)
				}
			}

			// Validate checkpointObjectTrailerBuf was entirely consumed

			if 0 != len(checkpointObjectTrailerBuf) {
				err = fmt.Errorf("Extra %v bytes found in volume %v's checkpointObjectTrailer", len(checkpointObjectTrailerBuf), volume.volumeName)
				return
			}

			// Derive available SnapShotIDs

			volume.availableSnapShotIDList = list.New()

			for snapShotID = uint64(1); snapShotID < volume.dotSnapShotDirSnapShotID; snapShotID++ {
				volume.availableSnapShotIDList.PushBack(snapShotID)
				_, ok, err = volume.viewTreeByID.GetByKey(snapShotID)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByID.GetByKey() failed: %v", volume.volumeName, err)
				}
				if !ok {
					volume.availableSnapShotIDList.PushBack(snapShotID)
				}
			}
		}
	} else {
		err = fmt.Errorf("Cannot parse %v/%v header %v: %v (version: %v not supported)", volume.accountName, volume.checkpointContainerName, CheckpointHeaderName, checkpointHeaderValue, volume.checkpointHeader.checkpointVersion)
		return
	}

	volume.maxNonce = (1 << (64 - volume.snapShotIDNumBits)) - 1
	volume.nextNonce = volume.checkpointHeader.reservedToNonce

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
		}
		logger.FatalfWithError(err, "platform.OpenFileSync(%v,os.O_RDWR,) failed", volume.replayLogFileName)
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
		checkpointObjectTrailer                *checkpointObjectTrailerV3Struct
		checkpointObjectTrailerBeginningOffset uint64
		checkpointObjectTrailerEndingOffset    uint64
		checkpointTrailerBuf                   []byte
		combinedBPlusTreeLayout                sortedmap.LayoutReport
		containerNameAsByteSlice               []byte
		containerNameAsValue                   sortedmap.Value
		delayedObjectDeleteList                []delayedObjectDeleteStruct
		elementOfBPlusTreeLayout               elementOfBPlusTreeLayoutStruct
		elementOfBPlusTreeLayoutBuf            []byte
		logSegmentObjectsToDelete              int
		objectNumber                           uint64
		objectNumberAsKey                      sortedmap.Key
		ok                                     bool
		treeLayoutBuf                          []byte
		treeLayoutBufSize                      uint64
	)

	checkpointObjectTrailer = &checkpointObjectTrailerV3Struct{}

	checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber,
		checkpointObjectTrailer.InodeRecBPlusTreeObjectOffset,
		checkpointObjectTrailer.InodeRecBPlusTreeObjectLength,
		err = volume.liveView.inodeRecWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber,
		checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset,
		checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength,
		err = volume.liveView.logSegmentRecWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber,
		checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset,
		checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength,
		err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
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
	err = volume.liveView.createdObjectsWrapper.bPlusTree.Prune()
	if nil != err {
		return
	}
	err = volume.liveView.deletedObjectsWrapper.bPlusTree.Prune()
	if nil != err {
		return
	}

	checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements = uint64(len(volume.liveView.inodeRecWrapper.trackingBPlusTreeLayout))
	checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements = uint64(len(volume.liveView.logSegmentRecWrapper.trackingBPlusTreeLayout))
	checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements = uint64(len(volume.liveView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout))

	checkpointTrailerBuf, err = cstruct.Pack(checkpointObjectTrailer, LittleEndian)
	if nil != err {
		return
	}

	treeLayoutBufSize = checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements
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

	volume.checkpointHeader.checkpointVersion = checkpointVersion3

	volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber = volume.checkpointChunkedPutContextObjectNumber
	volume.checkpointHeader.checkpointObjectTrailerStructObjectLength = checkpointObjectTrailerEndingOffset - checkpointObjectTrailerBeginningOffset

	checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
		volume.checkpointHeader.checkpointVersion,
		volume.checkpointHeader.checkpointObjectTrailerStructObjectNumber,
		volume.checkpointHeader.checkpointObjectTrailerStructObjectLength,
		volume.checkpointHeader.reservedToNonce,
	)

	checkpointHeaderValues = []string{checkpointHeaderValue}

	checkpointContainerHeaders = make(map[string][]string)

	checkpointContainerHeaders[CheckpointHeaderName] = checkpointHeaderValues

	err = swiftclient.ContainerPost(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
	if nil != err {
		return
	}

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
	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.createdObjectsWrapper.trackingBPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.deletedObjectsWrapper.trackingBPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}

	logSegmentObjectsToDelete, err = volume.liveView.deletedObjectsWrapper.bPlusTree.Len()
	if nil != err {
		logger.Fatalf("volume.liveView.deletedObjectsWrapper.bPlusTree.Len() failed: %v", err)
	}

	delayedObjectDeleteList = make([]delayedObjectDeleteStruct, 0, len(combinedBPlusTreeLayout)+logSegmentObjectsToDelete)

	for objectNumber, bytesUsedCumulative = range combinedBPlusTreeLayout {
		if 0 == bytesUsedCumulative {
			delete(volume.liveView.inodeRecWrapper.trackingBPlusTreeLayout, objectNumber)
			delete(volume.liveView.logSegmentRecWrapper.trackingBPlusTreeLayout, objectNumber)
			delete(volume.liveView.bPlusTreeObjectWrapper.trackingBPlusTreeLayout, objectNumber)
			delete(volume.liveView.createdObjectsWrapper.trackingBPlusTreeLayout, objectNumber)
			delete(volume.liveView.deletedObjectsWrapper.trackingBPlusTreeLayout, objectNumber)
			delayedObjectDeleteList = append(delayedObjectDeleteList, delayedObjectDeleteStruct{containerName: volume.checkpointContainerName, objectNumber: objectNumber})
		}
	}

	for ; logSegmentObjectsToDelete > 0; logSegmentObjectsToDelete-- {
		objectNumberAsKey, containerNameAsValue, ok, err = volume.liveView.deletedObjectsWrapper.bPlusTree.GetByIndex(0)
		if nil != err {
			logger.Fatalf("volume.liveView.deletedObjectsWrapper.bPlusTree.GetByIndex(0) failed: %v", err)
		}
		if !ok {
			logger.Fatalf("volume.liveView.deletedObjectsWrapper.bPlusTree.GetByIndex(0) returned !ok")
		}

		objectNumber, ok = objectNumberAsKey.(uint64)
		if !ok {
			logger.Fatalf("objectNumberAsKey.(uint64) returned !ok")
		}

		containerNameAsByteSlice, ok = containerNameAsValue.([]byte)
		if !ok {
			logger.Fatalf("containerNameAsValue.([]byte) returned !ok")
		}

		delayedObjectDeleteList = append(delayedObjectDeleteList, delayedObjectDeleteStruct{containerName: string(containerNameAsByteSlice[:]), objectNumber: objectNumber})

		ok, err = volume.liveView.deletedObjectsWrapper.bPlusTree.DeleteByIndex(0)
		if nil != err {
			logger.Fatalf("volume.liveView.deletedObjectsWrapper.bPlusTree.DeleteByIndex(0) failed: %v", err)
		}
		if !ok {
			logger.Fatalf("volume.liveView.deletedObjectsWrapper.bPlusTree.DeleteByIndex(0) returned !ok")
		}
	}

	if 0 < len(delayedObjectDeleteList) {
		volume.backgroundObjectDeleteWG.Add(1)
		go volume.performDelayedObjectDeletes(delayedObjectDeleteList)
	}

	err = nil
	return
}

func (volume *volumeStruct) performDelayedObjectDeletes(delayedObjectDeleteList []delayedObjectDeleteStruct) {
	for _, delayedObjectDelete := range delayedObjectDeleteList {
		err := swiftclient.ObjectDelete(
			volume.accountName,
			delayedObjectDelete.containerName,
			utils.Uint64ToHexStr(delayedObjectDelete.objectNumber),
			swiftclient.SkipRetry)
		if nil != err {
			logger.Errorf("DELETE %v/%v/%016X failed with err: %v", volume.accountName, delayedObjectDelete.containerName, delayedObjectDelete.objectNumber, err)
		}
	}
	volume.backgroundObjectDeleteWG.Done()
}

func (volume *volumeStruct) openCheckpointChunkedPutContextIfNecessary() (err error) {
	var (
		ok bool
	)

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
		if nil != volume.priorView {
			ok, err = volume.priorView.createdObjectsWrapper.bPlusTree.Put(volume.checkpointChunkedPutContextObjectNumber, []byte(volume.checkpointContainerName))
			if nil != err {
				return
			}
			if !ok {
				err = fmt.Errorf("volume.priorView.createdObjectsWrapper.bPlusTree.Put() returned !ok")
				return
			}
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
