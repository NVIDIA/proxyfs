package headhunter

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	etcd "go.etcd.io/etcd/clientv3"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/platform"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

var (
	LittleEndian = cstruct.LittleEndian // All data cstruct's to be serialized in LittleEndian form
)

type uint64Struct struct {
	U64 uint64
}

const (
	CheckpointVersion3 uint64 = iota + 3
	// uint64 in %016X indicating checkpointVersion2 or CheckpointVersion3
	// ' '
	// uint64 in %016X indicating objectNumber containing checkpoint record at tail of object
	// ' '
	// uint64 in %016X indicating length of               checkpoint record at tail of object
	// ' '
	// uint64 in %016X indicating reservedToNonce
)

type CheckpointHeaderStruct struct {
	CheckpointVersion                         uint64 // either checkpointVersion2 or CheckpointVersion3
	CheckpointObjectTrailerStructObjectNumber uint64 // checkpointObjectTrailerV?Struct found at "tail" of object
	CheckpointObjectTrailerStructObjectLength uint64 // this length includes appended non-fixed sized arrays
	ReservedToNonce                           uint64 // highest nonce value reserved
}

type CheckpointObjectTrailerV3Struct struct {
	InodeRecBPlusTreeObjectNumber             uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of inodeRec        B+Tree
	InodeRecBPlusTreeObjectOffset             uint64 // ...and offset into the Object where root starts
	InodeRecBPlusTreeObjectLength             uint64 // ...and length if that root node
	InodeRecBPlusTreeLayoutNumElements        uint64 // elements immediately follow CheckpointObjectTrailerV3Struct
	LogSegmentRecBPlusTreeObjectNumber        uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of logSegment      B+Tree
	LogSegmentRecBPlusTreeObjectOffset        uint64 // ...and offset into the Object where root starts
	LogSegmentRecBPlusTreeObjectLength        uint64 // ...and length if that root node
	LogSegmentRecBPlusTreeLayoutNumElements   uint64 // elements immediately follow inodeRecBPlusTreeLayout
	BPlusTreeObjectBPlusTreeObjectNumber      uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of bPlusTreeObject B+Tree
	BPlusTreeObjectBPlusTreeObjectOffset      uint64 // ...and offset into the Object where root starts
	BPlusTreeObjectBPlusTreeObjectLength      uint64 // ...and length if that root node
	BPlusTreeObjectBPlusTreeLayoutNumElements uint64 // elements immediately follow logSegmentRecBPlusTreeLayout
	CreatedObjectsBPlusTreeLayoutNumElements  uint64 // elements immediately follow bPlusTreeObjectBPlusTreeLayout
	DeletedObjectsBPlusTreeLayoutNumElements  uint64 // elements immediately follow createdObjectsBPlusTreeLayout
	SnapShotIDNumBits                         uint64 // number of bits reserved to hold SnapShotIDs
	SnapShotListNumElements                   uint64 // elements immediately follow deletedObjectsBPlusTreeLayout
	SnapShotListTotalSize                     uint64 // size of entire SnapShotList
	// InodeRecBPlusTreeLayout        serialized as [InodeRecBPlusTreeLayoutNumElements       ]ElementOfBPlusTreeLayoutStruct
	// LogSegmentBPlusTreeLayout      serialized as [LogSegmentRecBPlusTreeLayoutNumElements  ]ElementOfBPlusTreeLayoutStruct
	// BPlusTreeObjectBPlusTreeLayout serialized as [BPlusTreeObjectBPlusTreeLayoutNumElements]ElementOfBPlusTreeLayoutStruct
	// CreatedObjectsBPlusTreeLayout  serialized as [CreatedObjectsBPlusTreeLayoutNumElements ]ElementOfBPlusTreeLayoutStruct
	// DeletedObjectsBPlusTreeLayout  serialized as [DeletedObjectsBPlusTreeLayoutNumElements ]ElementOfBPlusTreeLayoutStruct
	// SnapShotList                   serialized as [SnapShotListNumElements                  ]ElementOfSnapShotListStruct
}

type ElementOfBPlusTreeLayoutStruct struct {
	ObjectNumber uint64
	ObjectBytes  uint64
}

type ElementOfSnapShotListStruct struct { // Note: for illustrative purposes... not marshalled with cstruct
	Nonce uint64 //        supplies strict time-ordering of SnapShots regardless of timebase resets
	ID    uint64 //        in the range [1:2^SnapShotIDNumBits-2]
	//                       ID == 0                     reserved for the "live" view
	//                       ID == 2^SnapShotIDNumBits-1 reserved for the .snapshot subdir of a dir
	TimeStamp time.Time // serialized/deserialized as a uint64 length followed by a that sized []byte
	//                       func (t  time.Time) time.MarshalBinary()              ([]byte, error)
	//                       func (t *time.Time) time.UnmarshalBinary(data []byte) (error)
	Name string //         serialized/deserialized as a uint64 length followed by a that sized []byte
	//                       func utils.ByteSliceToString(byteSlice []byte)        (str string)
	//                       func utils.StringToByteSlice(str string)              (byteSlice []byte)
	InodeRecBPlusTreeObjectNumber        uint64
	InodeRecBPlusTreeObjectOffset        uint64
	InodeRecBPlusTreeObjectLength        uint64
	LogSegmentRecBPlusTreeObjectNumber   uint64
	LogSegmentRecBPlusTreeObjectOffset   uint64
	LogSegmentRecBPlusTreeObjectLength   uint64
	BPlusTreeObjectBPlusTreeObjectNumber uint64
	BPlusTreeObjectBPlusTreeObjectOffset uint64
	BPlusTreeObjectBPlusTreeObjectLength uint64
	CreatedObjectsBPlusTreeObjectNumber  uint64
	CreatedObjectsBPlusTreeObjectOffset  uint64
	CreatedObjectsBPlusTreeObjectLength  uint64
	DeletedObjectsBPlusTreeObjectNumber  uint64
	DeletedObjectsBPlusTreeObjectOffset  uint64
	DeletedObjectsBPlusTreeObjectLength  uint64
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

type replayLogTransactionFixedPartStruct struct { // transactions begin on a replayLogWriteBufferAlignment boundary
	CRC64                                         uint64 // checksum of everything after this field
	BytesFollowing                                uint64 // bytes following in this transaction
	LastCheckpointObjectTrailerStructObjectNumber uint64 // last CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
				globals.uint64Size + //               transactionType == transactionDeleteInodeRec
				globals.uint64Size //                 inodeNumber
	case transactionPutLogSegmentRec:
		singleKey = keys.(uint64)
		singleValue = values.([]byte)
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
				globals.uint64Size + //               transactionType == transactionDeleteLogSegmentRec
				globals.uint64Size //                 logSegmentNumber
	case transactionPutBPlusTreeObject:
		singleKey = keys.(uint64)
		singleValue = values.([]byte)
		bytesNeeded = //                              transactions begin on a replayLogWriteBufferAlignment boundary
			globals.uint64Size + //                   checksum of everything after this field
				globals.uint64Size + //               bytes following in this transaction
				globals.uint64Size + //               last CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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
				globals.uint64Size + //               last CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber
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

	// Fill in last checkpoint's CheckpointHeaderStruct.checkpointObjectTrailerStructObjectNumber

	packedUint64, err = cstruct.Pack(volume.checkpointHeader.CheckpointObjectTrailerStructObjectNumber, LittleEndian)
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

// getEtcdCheckpointHeader gets the JSON-encoded checkpointHeader from etcd returning also its revision.
//
func (volume *volumeStruct) getEtcdCheckpointHeader() (checkpointHeader *CheckpointHeaderStruct, checkpointHeaderEtcdRevision int64, err error) {
	var (
		cancel      context.CancelFunc
		ctx         context.Context
		getResponse *etcd.GetResponse
	)

	ctx, cancel = context.WithTimeout(context.Background(), globals.etcdOpTimeout)
	getResponse, err = globals.etcdKV.Get(ctx, volume.checkpointEtcdKeyName)
	cancel()
	if nil != err {
		err = fmt.Errorf("Error contacting etcd: %v", err)
		return
	}

	if 1 != getResponse.Count {
		err = fmt.Errorf("Could not find %s in etcd", volume.checkpointEtcdKeyName)
		return
	}

	checkpointHeader = &CheckpointHeaderStruct{}

	err = json.Unmarshal(getResponse.Kvs[0].Value, checkpointHeader)
	if nil != err {
		err = fmt.Errorf("Error unmarshalling %s's Value (%s): %v", volume.checkpointEtcdKeyName, string(getResponse.Kvs[0].Value[:]), err)
		return
	}

	checkpointHeaderEtcdRevision = getResponse.Kvs[0].ModRevision

	return
}

// putEtcdCheckpointHeader puts the JSON-encoding of the supplied checkpointHeader in etcd.
// If a non-zero oldCheckpointHeaderEtcdRevision, it must match the current revision of the checkpointHeader.
//
func (volume *volumeStruct) putEtcdCheckpointHeader(checkpointHeader *CheckpointHeaderStruct, oldCheckpointHeaderEtcdRevision int64) (newCheckpointHeaderEtcdRevision int64, err error) {
	var (
		cancel              context.CancelFunc
		checkpointHeaderBuf []byte
		ctx                 context.Context
		putResponse         *etcd.PutResponse
		txnResponse         *etcd.TxnResponse
	)

	checkpointHeaderBuf, err = json.MarshalIndent(checkpointHeader, "", "  ")
	if nil != err {
		err = fmt.Errorf("Error marshalling checkpointHeader (%#v): %v", checkpointHeader, err)
		return
	}

	if 0 == oldCheckpointHeaderEtcdRevision {
		ctx, cancel = context.WithTimeout(context.Background(), globals.etcdOpTimeout)
		putResponse, err = globals.etcdKV.Put(ctx, volume.checkpointEtcdKeyName, string(checkpointHeaderBuf[:]))
		cancel()
		if nil != err {
			err = fmt.Errorf("Error contacting etcd: %v", err)
			return
		}

		newCheckpointHeaderEtcdRevision = putResponse.Header.Revision
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), globals.etcdOpTimeout)
		txnResponse, err = globals.etcdKV.Txn(ctx).If(etcd.Compare(etcd.ModRevision(volume.checkpointEtcdKeyName), "=", oldCheckpointHeaderEtcdRevision)).Then(etcd.OpPut(volume.checkpointEtcdKeyName, string(checkpointHeaderBuf[:]))).Commit()
		cancel()
		if nil != err {
			err = fmt.Errorf("Error contacting etcd: %v", err)
			return
		}

		if !txnResponse.Succeeded {
			err = fmt.Errorf("Transaction to update %s failed", volume.checkpointEtcdKeyName)
			return
		}

		newCheckpointHeaderEtcdRevision = txnResponse.Responses[0].GetResponsePut().Header.Revision
	}

	return
}

// fetchCheckpointContainerHeader reads the Checkpoint Container's Checkpoint Header a total of
// FSGlobals.CheckpointHeaderConsensusAttempts times simultaneously. Once all attempts are complete,
// it loops through the results in an attempt to achieve consensus such that a majority of the
// results agree on the most recently fetched Checkpoint Header.
//
func (volume *volumeStruct) fetchCheckpointContainerHeader() (checkpointHeader *CheckpointHeaderStruct, agreements uint16, err error) {
	var (
		bestCheckpointHeader             *CheckpointHeaderStruct
		checkpointHeaders                []*CheckpointHeaderStruct
		checkpointHeaderConsensusAttempt uint16
		checkpointHeaderConsensusQuorum  uint16
		thisCheckpointHeader             *CheckpointHeaderStruct
		wg                               sync.WaitGroup
	)

	checkpointHeaders = make([]*CheckpointHeaderStruct, globals.checkpointHeaderConsensusAttempts)

	for checkpointHeaderConsensusAttempt = 0; checkpointHeaderConsensusAttempt < globals.checkpointHeaderConsensusAttempts; checkpointHeaderConsensusAttempt++ {
		wg.Add(1)
		go volume.fetchCheckpointContainerHeaderAsynchronously(&checkpointHeaders[checkpointHeaderConsensusAttempt], &wg)
	}

	wg.Wait()

	checkpointHeaderConsensusQuorum = (globals.checkpointHeaderConsensusAttempts + 1) / 2

	bestCheckpointHeader = nil
	agreements = 0

	for checkpointHeaderConsensusAttempt = 0; checkpointHeaderConsensusAttempt < globals.checkpointHeaderConsensusAttempts; checkpointHeaderConsensusAttempt++ {
		thisCheckpointHeader = checkpointHeaders[checkpointHeaderConsensusAttempt]
		if nil == thisCheckpointHeader {
			continue
		}

		if nil == bestCheckpointHeader {
			bestCheckpointHeader = thisCheckpointHeader
			agreements = 1
		} else {
			if (thisCheckpointHeader.CheckpointObjectTrailerStructObjectNumber == bestCheckpointHeader.CheckpointObjectTrailerStructObjectNumber) &&
				(thisCheckpointHeader.ReservedToNonce == bestCheckpointHeader.ReservedToNonce) {
				agreements++
			} else {
				if (thisCheckpointHeader.CheckpointObjectTrailerStructObjectNumber > bestCheckpointHeader.CheckpointObjectTrailerStructObjectNumber) ||
					(thisCheckpointHeader.ReservedToNonce > bestCheckpointHeader.ReservedToNonce) {
					logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s found to be newer than prior best", volume.volumeName, CheckpointHeaderName)
					bestCheckpointHeader = thisCheckpointHeader
					agreements = 1
				} else {
					logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s found to be older than prior best", volume.volumeName, CheckpointHeaderName)
				}
			}
		}
	}

	if agreements >= checkpointHeaderConsensusQuorum {
		logger.Infof("%s.fetchCheckpointContainerHeader() found consensus CheckpointHeader %016X %016X %016X %016X (%d out of %d)",
			volume.volumeName,
			bestCheckpointHeader.CheckpointVersion,
			bestCheckpointHeader.CheckpointObjectTrailerStructObjectNumber,
			bestCheckpointHeader.CheckpointObjectTrailerStructObjectLength,
			bestCheckpointHeader.ReservedToNonce,
			agreements,
			globals.checkpointHeaderConsensusAttempts)
		checkpointHeader = bestCheckpointHeader
		err = nil
	} else {
		err = fmt.Errorf("%s.fetchCheckpointContainerHeader() could not achieve consensus (%d out of %d)",
			volume.volumeName,
			agreements,
			globals.checkpointHeaderConsensusAttempts)
		logger.Errorf("%v", err)
	}

	return
}

// fetchCheckpointContainerHeaderAsynchronously is invoked as a goroutine (indicating completion by
// making a wg.Done() call) to fetch a CheckpointHeaderStruct for the Volume's Swift Account Checkpoint
// Container Header. If the Checkpoint Container is not accessible, is missing a Checkpoint Header, or
// contains an ill-formed Checkpoint Header, nil is written to where checkpointHeaderPtr indicates.
// If a well-formed Checkpoint Header is found, a CheckpointHeaderStruct is created and populated with
// the decoded values and a pointer to it is written to where checkpointHeaderPtr indicates.
//
func (volume *volumeStruct) fetchCheckpointContainerHeaderAsynchronously(checkpointHeaderPtr **CheckpointHeaderStruct, wg *sync.WaitGroup) {
	var (
		checkpointContainerHeaders map[string][]string
		checkpointHeaderValue      string
		checkpointHeaderValueSplit []string
		checkpointHeaderValues     []string
		err                        error
		ok                         bool
		thisCheckpointHeader       *CheckpointHeaderStruct
	)

	checkpointContainerHeaders, err = swiftclient.ContainerHead(volume.accountName, volume.checkpointContainerName)
	if nil != err {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead failed: %v", volume.volumeName, err)
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}

	checkpointHeaderValues, ok = checkpointContainerHeaders[CheckpointHeaderName]
	if !ok {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead missing Header %s", volume.volumeName, CheckpointHeaderName)
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}
	if 1 != len(checkpointHeaderValues) {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s should have only a single value (had %d)", volume.volumeName, CheckpointHeaderName, len(checkpointHeaderValues))
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}

	checkpointHeaderValue = checkpointHeaderValues[0]
	checkpointHeaderValueSplit = strings.Split(checkpointHeaderValue, " ")
	if 4 != len(checkpointHeaderValueSplit) {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s should have four fields (had %d)", volume.volumeName, CheckpointHeaderName, len(checkpointHeaderValueSplit))
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}

	thisCheckpointHeader = &CheckpointHeaderStruct{}

	thisCheckpointHeader.CheckpointVersion, err = strconv.ParseUint(checkpointHeaderValueSplit[0], 16, 64)
	if nil != err {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s contained unparseable CheckpointVersion (%s)", volume.volumeName, CheckpointHeaderName, checkpointHeaderValueSplit[0])
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}
	if CheckpointVersion3 != thisCheckpointHeader.CheckpointVersion {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s contained unsupported CheckpointVersion (%s)", volume.volumeName, CheckpointHeaderName, checkpointHeaderValueSplit[0])
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}

	thisCheckpointHeader.CheckpointObjectTrailerStructObjectNumber, err = strconv.ParseUint(checkpointHeaderValueSplit[1], 16, 64)
	if nil != err {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s contained unparseable CheckpointObjectTrailerStructObjectNumber (%s)", volume.volumeName, CheckpointHeaderName, checkpointHeaderValueSplit[1])
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}
	thisCheckpointHeader.CheckpointObjectTrailerStructObjectLength, err = strconv.ParseUint(checkpointHeaderValueSplit[2], 16, 64)
	if nil != err {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s contained unparseable CheckpointObjectTrailerStructObjectLength (%s)", volume.volumeName, CheckpointHeaderName, checkpointHeaderValueSplit[2])
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}
	thisCheckpointHeader.ReservedToNonce, err = strconv.ParseUint(checkpointHeaderValueSplit[3], 16, 64)
	if nil != err {
		logger.Warnf("%s.fetchCheckpointContainerHeader() ContainerHead Header %s contained unparseable ReservedToNonce (%s)", volume.volumeName, CheckpointHeaderName, checkpointHeaderValueSplit[3])
		*checkpointHeaderPtr = nil
		wg.Done()
		return
	}

	*checkpointHeaderPtr = thisCheckpointHeader
	wg.Done()
}

func (volume *volumeStruct) fetchCheckpointLayoutReport() (layoutReport sortedmap.LayoutReport, err error) {
	var (
		checkpointHeader *CheckpointHeaderStruct
	)

	if globals.etcdEnabled {
		checkpointHeader, _, err = volume.getEtcdCheckpointHeader()
		if nil == err {
			// Return layoutReport from checkpointHeader found in etcd

			layoutReport = make(sortedmap.LayoutReport)
			layoutReport[checkpointHeader.CheckpointObjectTrailerStructObjectNumber] = checkpointHeader.CheckpointObjectTrailerStructObjectLength

			return
		}

		// Fall-through to use the checkpointHeader from Swift
	}

	checkpointHeader, _, err = volume.fetchCheckpointContainerHeader()
	if nil != err {
		return
	}

	// Return layoutReport manufactured from the checkpointHeaderValue found in the Checkpoint Container

	layoutReport = make(sortedmap.LayoutReport)
	layoutReport[checkpointHeader.CheckpointObjectTrailerStructObjectNumber] = checkpointHeader.CheckpointObjectTrailerStructObjectLength

	return
}

func (volume *volumeStruct) getCheckpoint(autoFormat bool) (err error) {
	var (
		accountHeaderValues                                []string
		accountHeaders                                     map[string][]string
		bPlusTreeObjectWrapperBPlusTreeTracker             *bPlusTreeTrackerStruct
		bytesConsumed                                      uint64
		bytesNeeded                                        uint64
		checkpointContainerHeadAgreements                  uint16
		checkpointContainerHeaders                         map[string][]string
		checkpointHeader                                   CheckpointHeaderStruct
		checkpointHeaderValue                              string
		checkpointHeaderValues                             []string
		checkpointObjectTrailerBuf                         []byte
		checkpointObjectTrailerV3                          *CheckpointObjectTrailerV3Struct
		computedCRC64                                      uint64
		containerNameAsValue                               sortedmap.Value
		createdObjectsWrapperBPlusTreeTracker              *bPlusTreeTrackerStruct
		defaultReplayLogReadBuffer                         []byte
		deletedObjectsWrapperBPlusTreeTracker              *bPlusTreeTrackerStruct
		elementOfBPlusTreeLayout                           ElementOfBPlusTreeLayoutStruct
		expectedCheckpointObjectTrailerSize                uint64
		inodeIndex                                         uint64
		inodeNumber                                        uint64
		inodeRecWrapperBPlusTreeTracker                    *bPlusTreeTrackerStruct
		layoutReportIndex                                  uint64
		logSegmentNumber                                   uint64
		logSegmentRecWrapperBPlusTreeTracker               *bPlusTreeTrackerStruct
		numInodes                                          uint64
		objectNumber                                       uint64
		ok                                                 bool
		replayLogReadBuffer                                []byte
		replayLogReadBufferPosition                        uint64
		replayLogPosition                                  int64
		replayLogSize                                      int64
		replayLogTransactionFixedPart                      replayLogTransactionFixedPartStruct
		snapShotBPlusTreeObjectBPlusTreeObjectLengthStruct uint64Struct
		snapShotBPlusTreeObjectBPlusTreeObjectNumberStruct uint64Struct
		snapShotBPlusTreeObjectBPlusTreeObjectOffsetStruct uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectLengthStruct  uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectNumberStruct  uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectOffsetStruct  uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectLengthStruct  uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectNumberStruct  uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectOffsetStruct  uint64Struct
		snapShotID                                         uint64
		snapShotIDStruct                                   uint64Struct
		snapShotInodeRecBPlusTreeObjectLengthStruct        uint64Struct
		snapShotInodeRecBPlusTreeObjectNumberStruct        uint64Struct
		snapShotInodeRecBPlusTreeObjectOffsetStruct        uint64Struct
		snapShotIndex                                      uint64
		snapShotLogSegmentRecBPlusTreeObjectLengthStruct   uint64Struct
		snapShotLogSegmentRecBPlusTreeObjectNumberStruct   uint64Struct
		snapShotLogSegmentRecBPlusTreeObjectOffsetStruct   uint64Struct
		snapShotNameBuf                                    []byte
		snapShotNameBufLenStruct                           uint64Struct
		snapShotNonceStruct                                uint64Struct
		snapShotTimeStampBuf                               []byte
		snapShotTimeStampBufLenStruct                      uint64Struct
		storagePolicyHeaderValues                          []string
		value                                              []byte
		valueLen                                           uint64
		volumeView                                         *volumeViewStruct
		volumeViewAsValue                                  sortedmap.Value
	)

	if globals.etcdEnabled {
		volume.checkpointHeader, volume.checkpointHeaderEtcdRevision, err = volume.getEtcdCheckpointHeader()
		if nil != err {
			logger.Infof("No checkpointHeader found in etcd for volume %s: %v", volume.volumeName, err)

			volume.checkpointHeader, checkpointContainerHeadAgreements, err = volume.fetchCheckpointContainerHeader()
			if nil != err {
				if 0 != checkpointContainerHeadAgreements {
					err = fmt.Errorf("Found no consensus but non-empty checkpointHeader in Swift for volume %s", volume.volumeName)
					return
				}
				if autoFormat {
					logger.Infof("No checkpointHeader found in Swift for volume %s: %v", volume.volumeName, err)
				} else {
					err = fmt.Errorf("No checkpointHeader found in Swift for volume %s: %v", volume.volumeName, err)
					return
				}

				volume.checkpointHeader = &CheckpointHeaderStruct{
					CheckpointVersion:                         CheckpointVersion3,
					CheckpointObjectTrailerStructObjectNumber: 0,
					CheckpointObjectTrailerStructObjectLength: 0,
					ReservedToNonce:                           firstNonceToProvide, // First FetchNonce() will trigger a reserve step
				}

				volume.checkpointHeaderEtcdRevision, err = volume.putEtcdCheckpointHeader(volume.checkpointHeader, 0)
				if nil != err {
					err = fmt.Errorf("Unable to put checkpointHeader in etcd: %v", err)
					return
				}

				checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
					checkpointHeader.CheckpointVersion,
					checkpointHeader.CheckpointObjectTrailerStructObjectNumber,
					checkpointHeader.CheckpointObjectTrailerStructObjectLength,
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
			}
		}
	} else {
		volume.checkpointHeader, checkpointContainerHeadAgreements, err = volume.fetchCheckpointContainerHeader()
		if nil != err {
			if 0 != checkpointContainerHeadAgreements {
				err = fmt.Errorf("Found no consensus but non-empty checkpointHeader in Swift for volume %s", volume.volumeName)
				return
			}
			if autoFormat {
				logger.Infof("No checkpointHeader found in Swift for volume %s: %v", volume.volumeName, err)
			} else {
				err = fmt.Errorf("No checkpointHeader found in Swift for volume %s: %v", volume.volumeName, err)
				return
			}

			volume.checkpointHeader = &CheckpointHeaderStruct{
				CheckpointVersion:                         CheckpointVersion3,
				CheckpointObjectTrailerStructObjectNumber: 0,
				CheckpointObjectTrailerStructObjectLength: 0,
				ReservedToNonce:                           firstNonceToProvide, // First FetchNonce() will trigger a reserve step
			}

			checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
				checkpointHeader.CheckpointVersion,
				checkpointHeader.CheckpointObjectTrailerStructObjectNumber,
				checkpointHeader.CheckpointObjectTrailerStructObjectLength,
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
		}
	}

	volume.liveView = &volumeViewStruct{volume: volume}

	if CheckpointVersion3 == volume.checkpointHeader.CheckpointVersion {
		if 0 == volume.checkpointHeader.CheckpointObjectTrailerStructObjectNumber {
			// Initialize based on zero-filled CheckpointObjectTrailerV3Struct

			checkpointObjectTrailerV3 = &CheckpointObjectTrailerV3Struct{
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
				CreatedObjectsBPlusTreeLayoutNumElements:  0,
				DeletedObjectsBPlusTreeLayoutNumElements:  0,
				SnapShotIDNumBits:                         uint64(volume.snapShotIDNumBits),
				SnapShotListNumElements:                   0,
				SnapShotListTotalSize:                     0,
			}

			inodeRecWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.inodeRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: inodeRecWrapperBPlusTreeTracker,
			}

			volume.liveView.inodeRecWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxInodesPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.inodeRecWrapper,
					globals.inodeRecCache)

			logSegmentRecWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.logSegmentRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: logSegmentRecWrapperBPlusTreeTracker,
			}

			volume.liveView.logSegmentRecWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxLogSegmentsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.logSegmentRecWrapper,
					globals.logSegmentRecCache)

			bPlusTreeObjectWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: bPlusTreeObjectWrapperBPlusTreeTracker,
			}

			volume.liveView.bPlusTreeObjectWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxDirFileNodesPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.bPlusTreeObjectWrapper,
					globals.bPlusTreeObjectCache)

			createdObjectsWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.createdObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: createdObjectsWrapperBPlusTreeTracker,
			}

			volume.liveView.createdObjectsWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxCreatedDeletedObjectsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.createdObjectsWrapper,
					globals.createdDeletedObjectsCache)

			deletedObjectsWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.deletedObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: deletedObjectsWrapperBPlusTreeTracker,
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
			volume.snapShotU64NonceMask = (uint64(1) << volume.snapShotIDShift) - uint64(1)

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
			// Read in CheckpointObjectTrailerV3Struct

			checkpointObjectTrailerBuf, err =
				swiftclient.ObjectTail(
					volume.accountName,
					volume.checkpointContainerName,
					utils.Uint64ToHexStr(volume.checkpointHeader.CheckpointObjectTrailerStructObjectNumber),
					volume.checkpointHeader.CheckpointObjectTrailerStructObjectLength)
			if nil != err {
				return
			}

			checkpointObjectTrailerV3 = &CheckpointObjectTrailerV3Struct{}

			bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, checkpointObjectTrailerV3, LittleEndian)
			if nil != err {
				return
			}
			checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

			// Load liveView.{inodeRec|logSegmentRec|bPlusTreeObject}Wrapper B+Trees

			inodeRecWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.inodeRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: inodeRecWrapperBPlusTreeTracker,
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

			logSegmentRecWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.logSegmentRecWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: logSegmentRecWrapperBPlusTreeTracker,
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

			bPlusTreeObjectWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: bPlusTreeObjectWrapperBPlusTreeTracker,
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

			createdObjectsWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.createdObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: createdObjectsWrapperBPlusTreeTracker,
			}

			volume.liveView.createdObjectsWrapper.bPlusTree =
				sortedmap.NewBPlusTree(
					volume.maxCreatedDeletedObjectsPerMetadataNode,
					sortedmap.CompareUint64,
					volume.liveView.createdObjectsWrapper,
					globals.createdDeletedObjectsCache)

			deletedObjectsWrapperBPlusTreeTracker = &bPlusTreeTrackerStruct{bPlusTreeLayout: make(sortedmap.LayoutReport)}

			volume.liveView.deletedObjectsWrapper = &bPlusTreeWrapperStruct{
				volumeView:       volume.liveView,
				bPlusTreeTracker: deletedObjectsWrapperBPlusTreeTracker,
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
			expectedCheckpointObjectTrailerSize += checkpointObjectTrailerV3.CreatedObjectsBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize += checkpointObjectTrailerV3.DeletedObjectsBPlusTreeLayoutNumElements
			expectedCheckpointObjectTrailerSize *= globals.ElementOfBPlusTreeLayoutStructSize
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

				volume.liveView.inodeRecWrapper.bPlusTreeTracker.bPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.LogSegmentRecBPlusTreeLayoutNumElements; layoutReportIndex++ {
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volume.liveView.logSegmentRecWrapper.bPlusTreeTracker.bPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeLayoutNumElements; layoutReportIndex++ {
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volume.liveView.bPlusTreeObjectWrapper.bPlusTreeTracker.bPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.CreatedObjectsBPlusTreeLayoutNumElements; layoutReportIndex++ {
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volume.liveView.createdObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			for layoutReportIndex = 0; layoutReportIndex < checkpointObjectTrailerV3.DeletedObjectsBPlusTreeLayoutNumElements; layoutReportIndex++ {
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &elementOfBPlusTreeLayout, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volume.liveView.deletedObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectBytes
			}

			// Compute SnapShotID shortcuts

			volume.snapShotIDShift = uint64(64) - uint64(volume.snapShotIDNumBits)
			volume.dotSnapShotDirSnapShotID = (uint64(1) << uint64(volume.snapShotIDNumBits)) - uint64(1)
			volume.snapShotU64NonceMask = (uint64(1) << volume.snapShotIDShift) - uint64(1)

			// Load SnapShotList

			volume.viewTreeByNonce = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
			volume.viewTreeByID = sortedmap.NewLLRBTree(sortedmap.CompareUint64, nil)
			volume.viewTreeByTime = sortedmap.NewLLRBTree(sortedmap.CompareTime, nil)
			volume.viewTreeByName = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

			// Load of viewTreeBy{Nonce|ID|Time|Name}

			for snapShotIndex = 0; snapShotIndex < checkpointObjectTrailerV3.SnapShotListNumElements; snapShotIndex++ {
				volumeView = &volumeViewStruct{volume: volume}

				// ElementOfSnapShotListStruct.Nonce

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the nonce", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotNonceStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				volumeView.nonce = snapShotNonceStruct.U64
				_, ok, err = volume.viewTreeByNonce.GetByKey(volumeView.nonce)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByNonce.GetByKey(%v) for SnapShotList element %v failed: %v", volume.volumeName, volumeView.nonce, snapShotIndex, err)
				}
				if ok {
					err = fmt.Errorf("Volume %v's viewTreeByNonce already contained nonce %v for SnapShotList element %v ", volume.volumeName, volumeView.nonce, snapShotIndex)
					return
				}

				// ElementOfSnapShotListStruct.ID

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the id", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotIDStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				volumeView.snapShotID = snapShotIDStruct.U64
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

				// ElementOfSnapShotListStruct.TimeStamp

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the timeStamp len", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotTimeStampBufLenStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				if uint64(len(checkpointObjectTrailerBuf)) < snapShotTimeStampBufLenStruct.U64 {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the timeStamp", volume.volumeName, snapShotIndex)
					return
				}
				snapShotTimeStampBuf = checkpointObjectTrailerBuf[:snapShotTimeStampBufLenStruct.U64]
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[snapShotTimeStampBufLenStruct.U64:]
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

				// ElementOfSnapShotListStruct.Name

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the name len", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotNameBufLenStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]
				if uint64(len(checkpointObjectTrailerBuf)) < snapShotNameBufLenStruct.U64 {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the name", volume.volumeName, snapShotIndex)
					return
				}
				snapShotNameBuf = checkpointObjectTrailerBuf[:snapShotNameBufLenStruct.U64]
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[snapShotNameBufLenStruct.U64:]
				volumeView.snapShotName = utils.ByteSliceToString(snapShotNameBuf)
				_, ok, err = volume.viewTreeByName.GetByKey(volumeView.snapShotName)
				if nil != err {
					logger.Fatalf("Logic error - volume %v's viewTreeByName.GetByKey(%v) for SnapShotList element %v failed: %v", volume.volumeName, volumeView.snapShotName, snapShotIndex, err)
				}
				if ok {
					err = fmt.Errorf("Volume %v's viewTreeByName already contained snapShotName %v for SnapShotList element %v ", volume.volumeName, volumeView.snapShotName, snapShotIndex)
					return
				}

				// ElementOfSnapShotListStruct.InodeRecBPlusTreeObjectNumber

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the inodeRecBPlusTreeObjectNumber", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotInodeRecBPlusTreeObjectNumberStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.InodeRecBPlusTreeObjectOffset

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the inodeRecBPlusTreeObjectOffset", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotInodeRecBPlusTreeObjectOffsetStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.InodeRecBPlusTreeObjectLength

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the inodeRecBPlusTreeObjectLength", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotInodeRecBPlusTreeObjectLengthStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volumeView.inodeRecWrapper = &bPlusTreeWrapperStruct{
					volumeView:       volumeView,
					bPlusTreeTracker: nil,
				}

				if 0 == snapShotInodeRecBPlusTreeObjectNumberStruct.U64 {
					volumeView.inodeRecWrapper.bPlusTree =
						sortedmap.NewBPlusTree(
							volume.maxInodesPerMetadataNode,
							sortedmap.CompareUint64,
							volumeView.inodeRecWrapper,
							globals.inodeRecCache)
				} else {
					volumeView.inodeRecWrapper.bPlusTree, err =
						sortedmap.OldBPlusTree(
							snapShotInodeRecBPlusTreeObjectNumberStruct.U64,
							snapShotInodeRecBPlusTreeObjectOffsetStruct.U64,
							snapShotInodeRecBPlusTreeObjectLengthStruct.U64,
							sortedmap.CompareUint64,
							volumeView.inodeRecWrapper,
							globals.inodeRecCache)
					if nil != err {
						return
					}
				}

				// ElementOfSnapShotListStruct.LogSegmentRecBPlusTreeObjectNumber

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the logSegmentRecBPlusTreeObjectNumber", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotLogSegmentRecBPlusTreeObjectNumberStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.LogSegmentRecBPlusTreeObjectOffset

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the logSegmentRecBPlusTreeObjectOffset", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotLogSegmentRecBPlusTreeObjectOffsetStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.LogSegmentRecBPlusTreeObjectLength

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the logSegmentRecBPlusTreeObjectLength", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotLogSegmentRecBPlusTreeObjectLengthStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volumeView.logSegmentRecWrapper = &bPlusTreeWrapperStruct{
					volumeView:       volumeView,
					bPlusTreeTracker: nil,
				}

				if 0 == snapShotLogSegmentRecBPlusTreeObjectNumberStruct.U64 {
					volumeView.logSegmentRecWrapper.bPlusTree =
						sortedmap.NewBPlusTree(
							volume.maxLogSegmentsPerMetadataNode,
							sortedmap.CompareUint64,
							volumeView.logSegmentRecWrapper,
							globals.logSegmentRecCache)
				} else {
					volumeView.logSegmentRecWrapper.bPlusTree, err =
						sortedmap.OldBPlusTree(
							snapShotLogSegmentRecBPlusTreeObjectNumberStruct.U64,
							snapShotLogSegmentRecBPlusTreeObjectOffsetStruct.U64,
							snapShotLogSegmentRecBPlusTreeObjectLengthStruct.U64,
							sortedmap.CompareUint64,
							volumeView.logSegmentRecWrapper,
							globals.logSegmentRecCache)
					if nil != err {
						return
					}
				}

				// ElementOfSnapShotListStruct.BPlusTreeObjectBPlusTreeObjectNumber

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the bPlusTreeObjectBPlusTreeObjectNumber", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotBPlusTreeObjectBPlusTreeObjectNumberStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.BPlusTreeObjectBPlusTreeObjectOffset

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the bPlusTreeObjectBPlusTreeObjectOffset", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotBPlusTreeObjectBPlusTreeObjectOffsetStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.BPlusTreeObjectBPlusTreeObjectLength

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the bPlusTreeObjectBPlusTreeObjectLength", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotBPlusTreeObjectBPlusTreeObjectLengthStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				volumeView.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{
					volumeView:       volumeView,
					bPlusTreeTracker: nil,
				}

				if 0 == snapShotBPlusTreeObjectBPlusTreeObjectNumberStruct.U64 {
					volumeView.bPlusTreeObjectWrapper.bPlusTree =
						sortedmap.NewBPlusTree(
							volume.maxDirFileNodesPerMetadataNode,
							sortedmap.CompareUint64,
							volumeView.bPlusTreeObjectWrapper,
							globals.logSegmentRecCache)
				} else {
					volumeView.bPlusTreeObjectWrapper.bPlusTree, err =
						sortedmap.OldBPlusTree(
							snapShotBPlusTreeObjectBPlusTreeObjectNumberStruct.U64,
							snapShotBPlusTreeObjectBPlusTreeObjectOffsetStruct.U64,
							snapShotBPlusTreeObjectBPlusTreeObjectLengthStruct.U64,
							sortedmap.CompareUint64,
							volumeView.bPlusTreeObjectWrapper,
							globals.logSegmentRecCache)
					if nil != err {
						return
					}
				}

				// ElementOfSnapShotListStruct.CreatedObjectsBPlusTreeObjectNumber

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the createdObjectsBPlusTreeObjectNumber", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotCreatedObjectsBPlusTreeObjectNumberStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.CreatedObjectsBPlusTreeObjectOffset

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the createdObjectsBPlusTreeObjectOffset", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotCreatedObjectsBPlusTreeObjectOffsetStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.CreatedObjectsBPlusTreeObjectLength

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
					volumeView:       volumeView,
					bPlusTreeTracker: volumeView.volume.liveView.createdObjectsWrapper.bPlusTreeTracker,
				}

				if 0 == snapShotCreatedObjectsBPlusTreeObjectNumberStruct.U64 {
					volumeView.createdObjectsWrapper.bPlusTree =
						sortedmap.NewBPlusTree(
							volume.maxCreatedDeletedObjectsPerMetadataNode,
							sortedmap.CompareUint64,
							volumeView.createdObjectsWrapper,
							globals.createdDeletedObjectsCache)
				} else {
					volumeView.createdObjectsWrapper.bPlusTree, err =
						sortedmap.OldBPlusTree(
							snapShotCreatedObjectsBPlusTreeObjectNumberStruct.U64,
							snapShotCreatedObjectsBPlusTreeObjectOffsetStruct.U64,
							snapShotCreatedObjectsBPlusTreeObjectLengthStruct.U64,
							sortedmap.CompareUint64,
							volumeView.createdObjectsWrapper,
							globals.createdDeletedObjectsCache)
					if nil != err {
						return
					}
				}

				// ElementOfSnapShotListStruct.DeletedObjectsBPlusTreeObjectNumber

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the deletedObjectsBPlusTreeObjectNumber", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotDeletedObjectsBPlusTreeObjectNumberStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.DeletedObjectsBPlusTreeObjectOffset

				if uint64(len(checkpointObjectTrailerBuf)) < globals.uint64Size {
					err = fmt.Errorf("Cannot parse volume %v's checkpointObjectTrailer's SnapShotList element %v...no room for the deletedObjectsBPlusTreeObjectOffset", volume.volumeName, snapShotIndex)
					return
				}
				bytesConsumed, err = cstruct.Unpack(checkpointObjectTrailerBuf, &snapShotDeletedObjectsBPlusTreeObjectOffsetStruct, LittleEndian)
				if nil != err {
					return
				}
				checkpointObjectTrailerBuf = checkpointObjectTrailerBuf[bytesConsumed:]

				// ElementOfSnapShotListStruct.DeletedObjectsBPlusTreeObjectLength

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
					volumeView:       volumeView,
					bPlusTreeTracker: volumeView.volume.liveView.deletedObjectsWrapper.bPlusTreeTracker,
				}

				if 0 == snapShotDeletedObjectsBPlusTreeObjectNumberStruct.U64 {
					volumeView.deletedObjectsWrapper.bPlusTree =
						sortedmap.NewBPlusTree(
							volume.maxCreatedDeletedObjectsPerMetadataNode,
							sortedmap.CompareUint64,
							volumeView.deletedObjectsWrapper,
							globals.createdDeletedObjectsCache)
				} else {
					volumeView.deletedObjectsWrapper.bPlusTree, err =
						sortedmap.OldBPlusTree(
							snapShotDeletedObjectsBPlusTreeObjectNumberStruct.U64,
							snapShotDeletedObjectsBPlusTreeObjectOffsetStruct.U64,
							snapShotDeletedObjectsBPlusTreeObjectLengthStruct.U64,
							sortedmap.CompareUint64,
							volumeView.deletedObjectsWrapper,
							globals.createdDeletedObjectsCache)
					if nil != err {
						return
					}
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
		// Note that, currently, fetchCheckpointContainerHeader() would have error'd preventing us to reach here
		err = fmt.Errorf("CheckpointHeader.CheckpointVersion (%v) for volume %s not supported", volume.checkpointHeader.CheckpointVersion, volume.volumeName)
		return
	}

	volume.maxNonce = (1 << (64 - volume.snapShotIDNumBits)) - 1
	volume.nextNonce = volume.checkpointHeader.ReservedToNonce

	// Indicate that we've had (at least) one checkpointTriggeringEvent

	volume.checkpointTriggeringEvents++

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

		volume.checkpointTriggeringEvents++

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
			if nil != volume.priorView {
				_, err = volume.priorView.createdObjectsWrapper.bPlusTree.Put(logSegmentNumber, value)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected volume.priorView.createdObjectsWrapper.bPlusTree.Put() failure: %v", volume.volumeName, err)
				}
			}
		case transactionDeleteLogSegmentRec:
			_, err = cstruct.Unpack(replayLogReadBuffer[replayLogReadBufferPosition:replayLogReadBufferPosition+globals.uint64Size], &logSegmentNumber, LittleEndian)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected cstruct.Unpack() failure: %v", volume.volumeName, err)
			}
			containerNameAsValue, ok, err = volume.liveView.logSegmentRecWrapper.bPlusTree.GetByKey(logSegmentNumber)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.logSegmentRecWrapper.bPlusTree.GetByKey() failure: %v", volume.volumeName, err)
			}
			if !ok {
				logger.Fatalf("Replay Log for Volume %s hit unexpected missing logSegmentNumber (0x%016X) in LogSegmentRecB+Tree", volume.volumeName, logSegmentNumber)
			}
			_, err = volume.liveView.logSegmentRecWrapper.bPlusTree.DeleteByKey(logSegmentNumber)
			if nil != err {
				logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.logSegmentRecWrapper.bPlusTree.DeleteByKey() failure: %v", volume.volumeName, err)
			}
			if nil == volume.priorView {
				_, err = volume.liveView.deletedObjectsWrapper.bPlusTree.Put(logSegmentNumber, containerNameAsValue)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.deletedObjectsWrapper.bPlusTree.Put() failure: %v", volume.volumeName, err)
				}
			} else {
				ok, err = volume.priorView.createdObjectsWrapper.bPlusTree.DeleteByKey(logSegmentNumber)
				if nil != err {
					logger.Fatalf("Reply Log for Volume %s hit unexpected volume.priorView.createdObjectsWrapper.bPlusTree.DeleteByKey() failure: %v", volume.volumeName, err)
				}
				if ok {
					_, err = volume.liveView.deletedObjectsWrapper.bPlusTree.Put(logSegmentNumber, containerNameAsValue)
					if nil != err {
						logger.Fatalf("Reply Log for Volume %s hit unexpected volume.liveView.deletedObjectsWrapper.bPlusTree.Put() failure: %v", volume.volumeName, err)
					}
				} else {
					_, err = volume.priorView.deletedObjectsWrapper.bPlusTree.Put(logSegmentNumber, containerNameAsValue)
					if nil != err {
						logger.Fatalf("Reply Log for Volume %s hit unexpected volume.priorView.deletedObjectsWrapper.bPlusTree.Put() failure: %v", volume.volumeName, err)
					}
				}
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

// putCheckpoint does the dirty work of creating a ProxyFS checkpoint -- its complicated.
//
// putCheckpoint collects bucketized statistics of the total time taken by the
// checkpoint and the number of bytes written to Swift to store the checkpoint.
// In addition, the time required by, and number of bytes transferred by, each step
// along the way is recorded in individual PutCheckpoint* bucketstats.
//
// Note that startTime2 is recorded as the starting time of an interval several
// times in this routine, and each time it is used to measure time.Since(startTime2)
// it is reset to the current time.
//
func (volume *volumeStruct) putCheckpoint() (err error) {

	// measure entire time and bytes for putCheckpoint
	startTime := time.Now()
	var chunkedPutBytes uint64
	defer func() {
		globals.PutCheckpointUsec.Add(uint64(time.Since(startTime) / time.Microsecond))
		if err != nil {
			globals.PutCheckpointErrors.Add(1)
			return
		}
		globals.PutCheckpointBytes.Add(chunkedPutBytes)
	}()

	var (
		bytesUsedCumulative                                uint64
		bytesUsedThisBPlusTree                             uint64
		checkpointContainerHeaders                         map[string][]string
		checkpointHeaderValue                              string
		checkpointHeaderValues                             []string
		checkpointObjectTrailer                            *CheckpointObjectTrailerV3Struct
		checkpointObjectTrailerBeginningOffset             uint64
		checkpointObjectTrailerEndingOffset                uint64
		checkpointTrailerBuf                               []byte
		combinedBPlusTreeLayout                            sortedmap.LayoutReport
		containerNameAsByteSlice                           []byte
		containerNameAsValue                               sortedmap.Value
		delayedObjectDeleteList                            []delayedObjectDeleteStruct
		elementOfBPlusTreeLayout                           ElementOfBPlusTreeLayoutStruct
		elementOfBPlusTreeLayoutBuf                        []byte
		elementOfSnapShotListBuf                           []byte
		logSegmentObjectsToDelete                          int
		objectNumber                                       uint64
		objectNumberAsKey                                  sortedmap.Key
		ok                                                 bool
		postponedCreatedObjectNumber                       uint64
		postponedCreatedObjectsFound                       bool
		snapShotBPlusTreeObjectBPlusTreeObjectLengthBuf    []byte
		snapShotBPlusTreeObjectBPlusTreeObjectLengthStruct uint64Struct
		snapShotBPlusTreeObjectBPlusTreeObjectNumberBuf    []byte
		snapShotBPlusTreeObjectBPlusTreeObjectNumberStruct uint64Struct
		snapShotBPlusTreeObjectBPlusTreeObjectOffsetBuf    []byte
		snapShotBPlusTreeObjectBPlusTreeObjectOffsetStruct uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectLengthBuf     []byte
		snapShotCreatedObjectsBPlusTreeObjectLengthStruct  uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectNumberBuf     []byte
		snapShotCreatedObjectsBPlusTreeObjectNumberStruct  uint64Struct
		snapShotCreatedObjectsBPlusTreeObjectOffsetBuf     []byte
		snapShotCreatedObjectsBPlusTreeObjectOffsetStruct  uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectLengthBuf     []byte
		snapShotDeletedObjectsBPlusTreeObjectLengthStruct  uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectNumberBuf     []byte
		snapShotDeletedObjectsBPlusTreeObjectNumberStruct  uint64Struct
		snapShotDeletedObjectsBPlusTreeObjectOffsetBuf     []byte
		snapShotDeletedObjectsBPlusTreeObjectOffsetStruct  uint64Struct
		snapShotIDBuf                                      []byte
		snapShotIDStruct                                   uint64Struct
		snapShotInodeRecBPlusTreeObjectLengthBuf           []byte
		snapShotInodeRecBPlusTreeObjectLengthStruct        uint64Struct
		snapShotInodeRecBPlusTreeObjectNumberBuf           []byte
		snapShotInodeRecBPlusTreeObjectNumberStruct        uint64Struct
		snapShotInodeRecBPlusTreeObjectOffsetBuf           []byte
		snapShotInodeRecBPlusTreeObjectOffsetStruct        uint64Struct
		snapShotListBuf                                    []byte
		snapShotLogSegmentRecBPlusTreeObjectLengthBuf      []byte
		snapShotLogSegmentRecBPlusTreeObjectLengthStruct   uint64Struct
		snapShotLogSegmentRecBPlusTreeObjectNumberBuf      []byte
		snapShotLogSegmentRecBPlusTreeObjectNumberStruct   uint64Struct
		snapShotLogSegmentRecBPlusTreeObjectOffsetBuf      []byte
		snapShotLogSegmentRecBPlusTreeObjectOffsetStruct   uint64Struct
		snapShotNameBuf                                    []byte
		snapShotNameBufLenBuf                              []byte
		snapShotNameBufLenStruct                           uint64Struct
		snapShotNonceBuf                                   []byte
		snapShotNonceStruct                                uint64Struct
		snapShotTimeStampBuf                               []byte
		snapShotTimeStampBufLenBuf                         []byte
		snapShotTimeStampBufLenStruct                      uint64Struct
		treeLayoutBuf                                      []byte
		treeLayoutBufSize                                  uint64
		volumeView                                         *volumeViewStruct
		volumeViewAsValue                                  sortedmap.Value
		volumeViewCount                                    int
		volumeViewIndex                                    int
	)

	if 0 == volume.checkpointTriggeringEvents {
		stats.IncrementOperations(&stats.SkippedCheckpoints)
		err = nil
		return
	}

	stats.IncrementOperations(&stats.AttemptedCheckpoints)

	checkpointObjectTrailer = &CheckpointObjectTrailerV3Struct{}

	startTime2 := time.Now()
	volume.liveView.inodeRecWrapper.ClearCounters()

	checkpointObjectTrailer.InodeRecBPlusTreeObjectNumber,
		checkpointObjectTrailer.InodeRecBPlusTreeObjectOffset,
		checkpointObjectTrailer.InodeRecBPlusTreeObjectLength,
		err = volume.liveView.inodeRecWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	globals.PutCheckpointInodeRecUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))
	globals.PutCheckpointInodeRecNodes.Add(volume.liveView.inodeRecWrapper.totalPutNodes)
	globals.PutCheckpointInodeRecBytes.Add(volume.liveView.inodeRecWrapper.totalPutBytes)
	chunkedPutBytes += volume.liveView.inodeRecWrapper.totalPutBytes

	startTime2 = time.Now()
	volume.liveView.logSegmentRecWrapper.ClearCounters()

	checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectNumber,
		checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectOffset,
		checkpointObjectTrailer.LogSegmentRecBPlusTreeObjectLength,
		err = volume.liveView.logSegmentRecWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	globals.PutCheckpointLogSegmentUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))
	globals.PutCheckpointLogSegmentNodes.Add(volume.liveView.logSegmentRecWrapper.totalPutNodes)
	globals.PutCheckpointLogSegmentBytes.Add(volume.liveView.logSegmentRecWrapper.totalPutBytes)
	chunkedPutBytes += volume.liveView.logSegmentRecWrapper.totalPutBytes

	startTime2 = time.Now()
	volume.liveView.bPlusTreeObjectWrapper.ClearCounters()

	checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectNumber,
		checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectOffset,
		checkpointObjectTrailer.BPlusTreeObjectBPlusTreeObjectLength,
		err = volume.liveView.bPlusTreeObjectWrapper.bPlusTree.Flush(false)
	if nil != err {
		return
	}
	globals.PutCheckpointbPlusTreeObjectUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))
	globals.PutCheckpointbPlusTreeObjectNodes.Add(volume.liveView.bPlusTreeObjectWrapper.totalPutNodes)
	globals.PutCheckpointbPlusTreeObjectBytes.Add(volume.liveView.bPlusTreeObjectWrapper.totalPutBytes)
	chunkedPutBytes += volume.liveView.bPlusTreeObjectWrapper.totalPutBytes

	startTime2 = time.Now()
	volumeViewCount, err = volume.viewTreeByNonce.Len()
	if nil != err {
		logger.Fatalf("volume.viewTreeByNonce.Len() failed: %v", err)
	}

	for volumeViewIndex = 0; volumeViewIndex < volumeViewCount; volumeViewIndex++ {
		_, volumeViewAsValue, ok, err = volume.viewTreeByNonce.GetByIndex(volumeViewIndex)
		if nil != err {
			logger.Fatalf("volume.viewTreeByNonce.GetByIndex(%v) failed: %v", volumeViewIndex, err)
		}
		if !ok {
			logger.Fatalf("volume.viewTreeByNonce.GetByIndex(%v) returned !ok", volumeViewIndex)
		}

		volumeView, ok = volumeViewAsValue.(*volumeViewStruct)
		if !ok {
			logger.Fatalf("volume.viewTreeByNonce.GetByIndex(%v) returned something other than a *volumeViewStruct", volumeViewIndex)
		}

		if volumeView == volume.priorView {
			// We must avoid a deadlock that would occur if, during Flush() of volume.priorView's
			// createdObjectsWrapper.bPlusTree, we needed to do a Put() into it due to the creation
			// of a new Checkpoint Object. The following sequence postpones those Put() calls
			// until after the Flush() completes, performs them, then retries the Flush() call.

			volume.postponePriorViewCreatedObjectsPuts = true
			postponedCreatedObjectsFound = true

			for postponedCreatedObjectsFound {
				_, _, _, err = volumeView.createdObjectsWrapper.bPlusTree.Flush(false)
				if nil != err {
					volume.postponePriorViewCreatedObjectsPuts = false
					volume.postponedPriorViewCreatedObjectsPuts = make(map[uint64]struct{})
					return
				}

				postponedCreatedObjectsFound = 0 < len(volume.postponedPriorViewCreatedObjectsPuts)

				if postponedCreatedObjectsFound {
					for postponedCreatedObjectNumber = range volume.postponedPriorViewCreatedObjectsPuts {
						ok, err = volume.priorView.createdObjectsWrapper.bPlusTree.Put(postponedCreatedObjectNumber, []byte(volume.checkpointContainerName))
						if nil != err {
							volume.postponePriorViewCreatedObjectsPuts = false
							volume.postponedPriorViewCreatedObjectsPuts = make(map[uint64]struct{})
							return
						}
						if !ok {
							volume.postponePriorViewCreatedObjectsPuts = false
							volume.postponedPriorViewCreatedObjectsPuts = make(map[uint64]struct{})
							err = fmt.Errorf("volume.priorView.createdObjectsWrapper.bPlusTree.Put() returned !ok")
							return
						}
					}

					volume.postponedPriorViewCreatedObjectsPuts = make(map[uint64]struct{})
				}
			}

			volume.postponePriorViewCreatedObjectsPuts = false
		} else {
			_, _, _, err = volumeView.createdObjectsWrapper.bPlusTree.Flush(false)
			if nil != err {
				return
			}
		}

		err = volumeView.createdObjectsWrapper.bPlusTree.Prune()
		if nil != err {
			return
		}

		_, _, _, err = volumeView.deletedObjectsWrapper.bPlusTree.Flush(false)
		if nil != err {
			return
		}

		err = volumeView.deletedObjectsWrapper.bPlusTree.Prune()
		if nil != err {
			return
		}
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
	globals.PutCheckpointSnapshotFlushUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))

	startTime2 = time.Now()
	checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements = uint64(len(volume.liveView.inodeRecWrapper.bPlusTreeTracker.bPlusTreeLayout))
	checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements = uint64(len(volume.liveView.logSegmentRecWrapper.bPlusTreeTracker.bPlusTreeLayout))
	checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements = uint64(len(volume.liveView.bPlusTreeObjectWrapper.bPlusTreeTracker.bPlusTreeLayout))
	checkpointObjectTrailer.CreatedObjectsBPlusTreeLayoutNumElements = uint64(len(volume.liveView.createdObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout))
	checkpointObjectTrailer.DeletedObjectsBPlusTreeLayoutNumElements = uint64(len(volume.liveView.deletedObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout))

	treeLayoutBufSize = checkpointObjectTrailer.InodeRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += checkpointObjectTrailer.LogSegmentRecBPlusTreeLayoutNumElements
	treeLayoutBufSize += checkpointObjectTrailer.BPlusTreeObjectBPlusTreeLayoutNumElements
	treeLayoutBufSize += checkpointObjectTrailer.CreatedObjectsBPlusTreeLayoutNumElements
	treeLayoutBufSize += checkpointObjectTrailer.DeletedObjectsBPlusTreeLayoutNumElements
	treeLayoutBufSize *= globals.ElementOfBPlusTreeLayoutStructSize

	treeLayoutBuf = make([]byte, 0, treeLayoutBufSize)

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.liveView.inodeRecWrapper.bPlusTreeTracker.bPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian) for volume %v inodeRec failed: %v", volume.volumeName, err)
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.liveView.logSegmentRecWrapper.bPlusTreeTracker.bPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian) for volume %v logSegmentRec failed: %v", volume.volumeName, err)
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.liveView.bPlusTreeObjectWrapper.bPlusTreeTracker.bPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian) for volume %v bPlusTreeObject failed: %v", volume.volumeName, err)
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.liveView.createdObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian) for volume %v createdObjects failed: %v", volume.volumeName, err)
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}

	for elementOfBPlusTreeLayout.ObjectNumber, elementOfBPlusTreeLayout.ObjectBytes = range volume.liveView.deletedObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout {
		elementOfBPlusTreeLayoutBuf, err = cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(&elementOfBPlusTreeLayout, LittleEndian) for volume %v deletedObjects failed: %v", volume.volumeName, err)
		}
		treeLayoutBuf = append(treeLayoutBuf, elementOfBPlusTreeLayoutBuf...)
	}
	globals.PutCheckpointTreeLayoutUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))

	startTime2 = time.Now()
	checkpointObjectTrailer.SnapShotIDNumBits = uint64(volume.snapShotIDNumBits)

	checkpointObjectTrailer.SnapShotListNumElements = uint64(volumeViewCount)

	snapShotListBuf = make([]byte, 0)

	for volumeViewIndex = 0; volumeViewIndex < volumeViewCount; volumeViewIndex++ {
		_, volumeViewAsValue, ok, err = volume.viewTreeByNonce.GetByIndex(volumeViewIndex)
		if nil != err {
			logger.Fatalf("volume.viewTreeByNonce.GetByIndex(%v) failed: %v", volumeViewIndex, err)
		}
		if !ok {
			logger.Fatalf("volume.viewTreeByNonce.GetByIndex(%v) returned !ok", volumeViewIndex)
		}

		volumeView, ok = volumeViewAsValue.(*volumeViewStruct)
		if !ok {
			logger.Fatalf("volume.viewTreeByNonce.GetByIndex(%v) returned something other than a *volumeViewStruct", volumeViewIndex)
		}

		snapShotNonceStruct.U64 = volumeView.nonce
		snapShotNonceBuf, err = cstruct.Pack(snapShotNonceStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotNonceStruct, LittleEndian) failed: %v", err)
		}

		snapShotIDStruct.U64 = volumeView.snapShotID
		snapShotIDBuf, err = cstruct.Pack(snapShotIDStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotIDStruct, LittleEndian) failed: %v", err)
		}

		snapShotTimeStampBuf, err = volumeView.snapShotTime.MarshalBinary()
		if nil != err {
			logger.Fatalf("volumeView.snapShotTime.MarshalBinary()for volumeViewIndex %v failed: %v", volumeViewIndex, err)
		}
		snapShotTimeStampBufLenStruct.U64 = uint64(len(snapShotTimeStampBuf))
		snapShotTimeStampBufLenBuf, err = cstruct.Pack(snapShotTimeStampBufLenStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotTimeStampBufLenStruct, LittleEndian) failed: %v", err)
		}

		snapShotNameBuf = utils.StringToByteSlice(volumeView.snapShotName)
		snapShotNameBufLenStruct.U64 = uint64(len(snapShotNameBuf))
		snapShotNameBufLenBuf, err = cstruct.Pack(snapShotNameBufLenStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotNameBufLenStruct, LittleEndian) failed: %v", err)
		}

		snapShotInodeRecBPlusTreeObjectNumberStruct.U64,
			snapShotInodeRecBPlusTreeObjectOffsetStruct.U64,
			snapShotInodeRecBPlusTreeObjectLengthStruct.U64 = volumeView.inodeRecWrapper.bPlusTree.FetchLocation()
		snapShotInodeRecBPlusTreeObjectNumberBuf, err = cstruct.Pack(snapShotInodeRecBPlusTreeObjectNumberStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotInodeRecBPlusTreeObjectNumberStruct, LittleEndian) failed: %v", err)
		}
		snapShotInodeRecBPlusTreeObjectOffsetBuf, err = cstruct.Pack(snapShotInodeRecBPlusTreeObjectOffsetStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotInodeRecBPlusTreeObjectOffsetStruct, LittleEndian) failed: %v", err)
		}
		snapShotInodeRecBPlusTreeObjectLengthBuf, err = cstruct.Pack(snapShotInodeRecBPlusTreeObjectLengthStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotInodeRecBPlusTreeObjectLengthStruct, LittleEndian) failed: %v", err)
		}

		snapShotLogSegmentRecBPlusTreeObjectNumberStruct.U64,
			snapShotLogSegmentRecBPlusTreeObjectOffsetStruct.U64,
			snapShotLogSegmentRecBPlusTreeObjectLengthStruct.U64 = volumeView.logSegmentRecWrapper.bPlusTree.FetchLocation()
		snapShotLogSegmentRecBPlusTreeObjectNumberBuf, err = cstruct.Pack(snapShotLogSegmentRecBPlusTreeObjectNumberStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotLogSegmentRecBPlusTreeObjectNumberStruct, LittleEndian) failed: %v", err)
		}
		snapShotLogSegmentRecBPlusTreeObjectOffsetBuf, err = cstruct.Pack(snapShotLogSegmentRecBPlusTreeObjectOffsetStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotLogSegmentRecBPlusTreeObjectOffsetStruct, LittleEndian) failed: %v", err)
		}
		snapShotLogSegmentRecBPlusTreeObjectLengthBuf, err = cstruct.Pack(snapShotLogSegmentRecBPlusTreeObjectLengthStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotLogSegmentRecBPlusTreeObjectLengthStruct, LittleEndian) failed: %v", err)
		}

		snapShotBPlusTreeObjectBPlusTreeObjectNumberStruct.U64,
			snapShotBPlusTreeObjectBPlusTreeObjectOffsetStruct.U64,
			snapShotBPlusTreeObjectBPlusTreeObjectLengthStruct.U64 = volumeView.bPlusTreeObjectWrapper.bPlusTree.FetchLocation()
		snapShotBPlusTreeObjectBPlusTreeObjectNumberBuf, err = cstruct.Pack(snapShotBPlusTreeObjectBPlusTreeObjectNumberStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotBPlusTreeObjectBPlusTreeObjectNumberStruct, LittleEndian) failed: %v", err)
		}
		snapShotBPlusTreeObjectBPlusTreeObjectOffsetBuf, err = cstruct.Pack(snapShotBPlusTreeObjectBPlusTreeObjectOffsetStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotBPlusTreeObjectBPlusTreeObjectOffsetStruct, LittleEndian) failed: %v", err)
		}
		snapShotBPlusTreeObjectBPlusTreeObjectLengthBuf, err = cstruct.Pack(snapShotBPlusTreeObjectBPlusTreeObjectLengthStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotBPlusTreeObjectBPlusTreeObjectLengthStruct, LittleEndian) failed: %v", err)
		}

		snapShotCreatedObjectsBPlusTreeObjectNumberStruct.U64,
			snapShotCreatedObjectsBPlusTreeObjectOffsetStruct.U64,
			snapShotCreatedObjectsBPlusTreeObjectLengthStruct.U64 = volumeView.createdObjectsWrapper.bPlusTree.FetchLocation()
		snapShotCreatedObjectsBPlusTreeObjectNumberBuf, err = cstruct.Pack(snapShotCreatedObjectsBPlusTreeObjectNumberStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotCreatedObjectsBPlusTreeObjectNumberStruct, LittleEndian) failed: %v", err)
		}
		snapShotCreatedObjectsBPlusTreeObjectOffsetBuf, err = cstruct.Pack(snapShotCreatedObjectsBPlusTreeObjectOffsetStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotCreatedObjectsBPlusTreeObjectOffsetStruct, LittleEndian) failed: %v", err)
		}
		snapShotCreatedObjectsBPlusTreeObjectLengthBuf, err = cstruct.Pack(snapShotCreatedObjectsBPlusTreeObjectLengthStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotCreatedObjectsBPlusTreeObjectLengthStruct, LittleEndian) failed: %v", err)
		}

		snapShotDeletedObjectsBPlusTreeObjectNumberStruct.U64,
			snapShotDeletedObjectsBPlusTreeObjectOffsetStruct.U64,
			snapShotDeletedObjectsBPlusTreeObjectLengthStruct.U64 = volumeView.deletedObjectsWrapper.bPlusTree.FetchLocation()
		snapShotDeletedObjectsBPlusTreeObjectNumberBuf, err = cstruct.Pack(snapShotDeletedObjectsBPlusTreeObjectNumberStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotDeletedObjectsBPlusTreeObjectNumberStruct, LittleEndian) failed: %v", err)
		}
		snapShotDeletedObjectsBPlusTreeObjectOffsetBuf, err = cstruct.Pack(snapShotDeletedObjectsBPlusTreeObjectOffsetStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotDeletedObjectsBPlusTreeObjectOffsetStruct, LittleEndian) failed: %v", err)
		}
		snapShotDeletedObjectsBPlusTreeObjectLengthBuf, err = cstruct.Pack(snapShotDeletedObjectsBPlusTreeObjectLengthStruct, LittleEndian)
		if nil != err {
			logger.Fatalf("cstruct.Pack(snapShotDeletedObjectsBPlusTreeObjectLengthStruct, LittleEndian) failed: %v", err)
		}

		elementOfSnapShotListBuf = make([]byte, 0, 19*globals.uint64Size+snapShotTimeStampBufLenStruct.U64+snapShotNameBufLenStruct.U64)

		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotNonceBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotIDBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotTimeStampBufLenBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotTimeStampBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotNameBufLenBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotNameBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotInodeRecBPlusTreeObjectNumberBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotInodeRecBPlusTreeObjectOffsetBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotInodeRecBPlusTreeObjectLengthBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotLogSegmentRecBPlusTreeObjectNumberBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotLogSegmentRecBPlusTreeObjectOffsetBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotLogSegmentRecBPlusTreeObjectLengthBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotBPlusTreeObjectBPlusTreeObjectNumberBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotBPlusTreeObjectBPlusTreeObjectOffsetBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotBPlusTreeObjectBPlusTreeObjectLengthBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotCreatedObjectsBPlusTreeObjectNumberBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotCreatedObjectsBPlusTreeObjectOffsetBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotCreatedObjectsBPlusTreeObjectLengthBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotDeletedObjectsBPlusTreeObjectNumberBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotDeletedObjectsBPlusTreeObjectOffsetBuf...)
		elementOfSnapShotListBuf = append(elementOfSnapShotListBuf, snapShotDeletedObjectsBPlusTreeObjectLengthBuf...)

		snapShotListBuf = append(snapShotListBuf, elementOfSnapShotListBuf...)
	}
	checkpointObjectTrailer.SnapShotListTotalSize = uint64(len(snapShotListBuf))

	globals.PutCheckpointSnapshotListUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))

	startTime2 = time.Now()

	checkpointTrailerBuf, err = cstruct.Pack(checkpointObjectTrailer, LittleEndian)
	if nil != err {
		return
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
	globals.PutCheckpointCheckpointTrailerBytes.Add(uint64(len(checkpointTrailerBuf)))
	chunkedPutBytes += uint64(len(checkpointTrailerBuf))

	err = volume.sendChunkToCheckpointChunkedPutContext(treeLayoutBuf)
	if nil != err {
		return
	}
	globals.PutCheckpointTreeLayoutBytes.Add(uint64(len(treeLayoutBuf)))
	chunkedPutBytes += uint64(len(treeLayoutBuf))

	if 0 < len(snapShotListBuf) {
		err = volume.sendChunkToCheckpointChunkedPutContext(snapShotListBuf)
		if nil != err {
			return
		}
	}
	globals.PutCheckpointSnapshotListBytes.Add(uint64(len(snapShotListBuf)))
	chunkedPutBytes += uint64(len(snapShotListBuf))

	checkpointObjectTrailerEndingOffset, err = volume.bytesPutToCheckpointChunkedPutContext()
	if nil != err {
		return
	}

	err = volume.closeCheckpointChunkedPutContext()
	if nil != err {
		return
	}
	globals.PutCheckpointChunkedPutUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))

	startTime2 = time.Now()

	// Before updating checkpointHeader, start accounting for unreferencing of prior checkpointTrailer

	combinedBPlusTreeLayout = make(sortedmap.LayoutReport)

	if 0 != volume.checkpointHeader.CheckpointObjectTrailerStructObjectNumber {
		combinedBPlusTreeLayout[volume.checkpointHeader.CheckpointObjectTrailerStructObjectNumber] = 0
	}

	// Now update checkpointHeader atomically indicating checkpoint is complete

	volume.checkpointHeader.CheckpointVersion = CheckpointVersion3

	volume.checkpointHeader.CheckpointObjectTrailerStructObjectNumber = volume.checkpointChunkedPutContextObjectNumber
	volume.checkpointHeader.CheckpointObjectTrailerStructObjectLength = checkpointObjectTrailerEndingOffset - checkpointObjectTrailerBeginningOffset

	checkpointHeaderValue = fmt.Sprintf("%016X %016X %016X %016X",
		volume.checkpointHeader.CheckpointVersion,
		volume.checkpointHeader.CheckpointObjectTrailerStructObjectNumber,
		volume.checkpointHeader.CheckpointObjectTrailerStructObjectLength,
		volume.checkpointHeader.ReservedToNonce,
	)

	if globals.etcdEnabled {
		volume.checkpointHeaderEtcdRevision, err = volume.putEtcdCheckpointHeader(volume.checkpointHeader, volume.checkpointHeaderEtcdRevision)
		if nil != err {
			return
		}
	}

	checkpointHeaderValues = []string{checkpointHeaderValue}

	checkpointContainerHeaders = make(map[string][]string)

	checkpointContainerHeaders[CheckpointHeaderName] = checkpointHeaderValues

	err = swiftclient.ContainerPost(volume.accountName, volume.checkpointContainerName, checkpointContainerHeaders)
	if nil != err {
		return
	}
	if globals.logCheckpointHeaderPosts {
		logger.Infof("POST'd checkpointHeaderValue %s for volume %s from putCheckpoint()", checkpointHeaderValue, volume.volumeName)
	}
	globals.PutCheckpointPostAndEtcdUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))

	volume.checkpointTriggeringEvents = 0

	stats.IncrementOperations(&stats.CompletedCheckpoints)

	startTime2 = time.Now()
	// Remove replayLogFile if necessary

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

	// Now continue computing what checkpoint objects may be deleted

	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.inodeRecWrapper.bPlusTreeTracker.bPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.logSegmentRecWrapper.bPlusTreeTracker.bPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.bPlusTreeObjectWrapper.bPlusTreeTracker.bPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.createdObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout {
		bytesUsedCumulative, ok = combinedBPlusTreeLayout[objectNumber]
		if ok {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedCumulative + bytesUsedThisBPlusTree
		} else {
			combinedBPlusTreeLayout[objectNumber] = bytesUsedThisBPlusTree
		}
	}
	for objectNumber, bytesUsedThisBPlusTree = range volume.liveView.deletedObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout {
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
			delete(volume.liveView.inodeRecWrapper.bPlusTreeTracker.bPlusTreeLayout, objectNumber)
			delete(volume.liveView.logSegmentRecWrapper.bPlusTreeTracker.bPlusTreeLayout, objectNumber)
			delete(volume.liveView.bPlusTreeObjectWrapper.bPlusTreeTracker.bPlusTreeLayout, objectNumber)
			delete(volume.liveView.createdObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout, objectNumber)
			delete(volume.liveView.deletedObjectsWrapper.bPlusTreeTracker.bPlusTreeLayout, objectNumber)

			if nil == volume.priorView {
				delayedObjectDeleteList = append(delayedObjectDeleteList, delayedObjectDeleteStruct{containerName: volume.checkpointContainerName, objectNumber: objectNumber})
			} else {
				ok, err = volume.priorView.createdObjectsWrapper.bPlusTree.DeleteByKey(objectNumber)
				if nil != err {
					logger.Fatalf("volume.priorView.createdObjectsWrapper.bPlusTree.DeleteByKey(objectNumber==0x%016X) failed: %v", objectNumber, err)
				}
				if ok {
					delayedObjectDeleteList = append(delayedObjectDeleteList, delayedObjectDeleteStruct{containerName: volume.checkpointContainerName, objectNumber: objectNumber})
				} else {
					ok, err = volume.priorView.deletedObjectsWrapper.bPlusTree.Put(objectNumber, utils.StringToByteSlice(volume.checkpointContainerName))
					if nil != err {
						logger.Fatalf("volume.priorView.deletedObjectsWrapper.bPlusTree.Put(objectNumber==0x%016X,%s) failed: %v", objectNumber, volume.checkpointContainerName, err)
					}
					if !ok {
						logger.Fatalf("volume.priorView.deletedObjectsWrapper.bPlusTree.Put(objectNumber==0x%016X,%s) returned !ok", objectNumber, volume.checkpointContainerName)
					}
				}
			}
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
	globals.PutCheckpointObjectCleanupUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))

	err = nil
	return
}

func (volume *volumeStruct) performDelayedObjectDeletes(delayedObjectDeleteList []delayedObjectDeleteStruct) {
	var (
		delayedObjectDelete     delayedObjectDeleteStruct
		delayedObjectDeleteName string
		err                     error
	)

RetryCheckForObjectDeleteEnabled:
	globals.backgroundObjectDeleteActiveWG.Add(1)

	globals.backgroundObjectDeleteRWMutex.RLock()

	if !globals.backgroundObjectDeleteEnabled {
		// We must block...and retry
		globals.backgroundObjectDeleteActiveWG.Done()
		globals.backgroundObjectDeleteRWMutex.RUnlock()
		globals.backgroundObjectDeleteEnabledWG.Wait()
		goto RetryCheckForObjectDeleteEnabled
	}

	globals.backgroundObjectDeleteRWMutex.RUnlock()

	for _, delayedObjectDelete = range delayedObjectDeleteList {
		delayedObjectDeleteName = utils.Uint64ToHexStr(delayedObjectDelete.objectNumber)
		if globals.metadataRecycleBin && (delayedObjectDelete.containerName == volume.checkpointContainerName) {
			err = swiftclient.ObjectPost(
				volume.accountName,
				delayedObjectDelete.containerName,
				delayedObjectDeleteName,
				globals.metadataRecycleBinHeader) // UNDO
			if nil != err {
				logger.Errorf("POST %s/%s/%s failed with err: %v", volume.accountName, delayedObjectDelete.containerName, delayedObjectDeleteName, err)
			}
		} else {
			err = swiftclient.ObjectDelete(
				volume.accountName,
				delayedObjectDelete.containerName,
				delayedObjectDeleteName,
				swiftclient.SkipRetry)
			if nil != err {
				logger.Errorf("DELETE %s/%s/%s failed with err: %v", volume.accountName, delayedObjectDelete.containerName, delayedObjectDeleteName, err)
			}
		}
	}

	volume.backgroundObjectDeleteWG.Done()
	globals.backgroundObjectDeleteActiveWG.Done()
}

func (volume *volumeStruct) openCheckpointChunkedPutContextIfNecessary() (err error) {
	var (
		ok bool
	)

	if nil == volume.checkpointChunkedPutContext {
		volume.checkpointChunkedPutContextObjectNumber = volume.fetchNonceWhileLocked()
		volume.checkpointChunkedPutContext, err =
			swiftclient.ObjectFetchChunkedPutContext(volume.accountName,
				volume.checkpointContainerName,
				utils.Uint64ToHexStr(volume.checkpointChunkedPutContextObjectNumber),
				volume.volumeName)
		if nil != err {
			return
		}
		if nil != volume.priorView {
			if volume.postponePriorViewCreatedObjectsPuts {
				_, ok = volume.postponedPriorViewCreatedObjectsPuts[volume.checkpointChunkedPutContextObjectNumber]
				if ok {
					err = fmt.Errorf("volume.postponedPriorViewCreatedObjectsPuts[volume.checkpointChunkedPutContextObjectNumber] check returned ok")
					return
				}
				volume.postponedPriorViewCreatedObjectsPuts[volume.checkpointChunkedPutContextObjectNumber] = struct{}{}
			} else {
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
		bPlusTreeObjectCacheHitsDelta         uint64
		bPlusTreeObjectCacheMissesDelta       uint64
		bPlusTreeObjectCacheStats             *sortedmap.BPlusTreeCacheStats
		checkpointListener                    VolumeEventListener
		checkpointListeners                   []VolumeEventListener
		checkpointRequest                     *checkpointRequestStruct
		checkpointRequesters                  []*checkpointRequestStruct
		createdDeletedObjectsCacheHitsDelta   uint64
		createdDeletedObjectsCacheMissesDelta uint64
		createdDeletedObjectsCacheStats       *sortedmap.BPlusTreeCacheStats
		exitOnCompletion                      bool
		inodeRecCacheHitsDelta                uint64
		inodeRecCacheMissesDelta              uint64
		inodeRecCacheStats                    *sortedmap.BPlusTreeCacheStats
		logSegmentRecCacheHitsDelta           uint64
		logSegmentRecCacheMissesDelta         uint64
		logSegmentRecCacheStats               *sortedmap.BPlusTreeCacheStats
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

		// measure the time required to perform the checkpoint
		startTime := time.Now()

		// measure the time required to get the volume lock for the checkpoint
		startTime2 := startTime

		volume.Lock()
		globals.DaemonPerCheckpointLockWaitUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))

		// measure the time while the lock is held for the checkpoint
		startTime2 = time.Now()

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

		// Collect any outstanding requests for a checkpoint.
		//
		// The volume lock has been held since the checkpoint started
		// and the volume lock is required to add any metadata that can
		// be part of the checkpoint, so any metadata that the
		// requesters wanted flushed has been flushed.
		checkpointRequesters = make([]*checkpointRequestStruct, 0, 1)
		moreRequests := true
		for moreRequests {
			checkpointRequesters = append(checkpointRequesters, checkpointRequest)

			select {
			case checkpointRequest = <-volume.checkpointRequestChan:
			default:
				moreRequests = false
			}
		}

		checkpointListeners = make([]VolumeEventListener, 0, len(volume.eventListeners))
		for checkpointListener = range volume.eventListeners {
			checkpointListeners = append(checkpointListeners, checkpointListener)
		}

		volume.Unlock()
		globals.DaemonPerCheckpointLockedUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))

		// Measure time spent updating statistics (including lock wait
		// time), time spent waking waiters and calling back listeners
		// (note that one listener also updates statistics).
		startTime2 = time.Now()

		exitOnCompletion = false
		for _, checkpointRequest = range checkpointRequesters {
			if checkpointRequest.exitOnCompletion {
				exitOnCompletion = true
			}
			checkpointRequest.waitGroup.Done() // Awake the checkpoint requester
		}

		for _, checkpointListener = range checkpointListeners {
			checkpointListener.CheckpointCompleted()
		}

		// Update Global B+Tree Cache stats now

		inodeRecCacheStats = globals.inodeRecCache.Stats()
		logSegmentRecCacheStats = globals.logSegmentRecCache.Stats()
		bPlusTreeObjectCacheStats = globals.bPlusTreeObjectCache.Stats()
		createdDeletedObjectsCacheStats = globals.createdDeletedObjectsCache.Stats()

		inodeRecCacheHitsDelta = inodeRecCacheStats.CacheHits - globals.inodeRecCachePriorCacheHits
		inodeRecCacheMissesDelta = inodeRecCacheStats.CacheMisses - globals.inodeRecCachePriorCacheMisses

		logSegmentRecCacheHitsDelta = logSegmentRecCacheStats.CacheHits - globals.logSegmentRecCachePriorCacheHits
		logSegmentRecCacheMissesDelta = logSegmentRecCacheStats.CacheMisses - globals.logSegmentRecCachePriorCacheMisses

		bPlusTreeObjectCacheHitsDelta = bPlusTreeObjectCacheStats.CacheHits - globals.bPlusTreeObjectCachePriorCacheHits
		bPlusTreeObjectCacheMissesDelta = bPlusTreeObjectCacheStats.CacheMisses - globals.bPlusTreeObjectCachePriorCacheMisses

		createdDeletedObjectsCacheHitsDelta = createdDeletedObjectsCacheStats.CacheHits - globals.createdDeletedObjectsCachePriorCacheHits
		createdDeletedObjectsCacheMissesDelta = createdDeletedObjectsCacheStats.CacheMisses - globals.createdDeletedObjectsCachePriorCacheMisses

		globals.Lock()

		if 0 != inodeRecCacheHitsDelta {
			stats.IncrementOperationsBy(&stats.InodeRecCacheHits, inodeRecCacheHitsDelta)
			globals.inodeRecCachePriorCacheHits = inodeRecCacheStats.CacheHits
		}
		if 0 != inodeRecCacheMissesDelta {
			stats.IncrementOperationsBy(&stats.InodeRecCacheMisses, inodeRecCacheMissesDelta)
			globals.inodeRecCachePriorCacheMisses = inodeRecCacheStats.CacheMisses
		}

		if 0 != logSegmentRecCacheHitsDelta {
			stats.IncrementOperationsBy(&stats.LogSegmentRecCacheHits, logSegmentRecCacheHitsDelta)
			globals.logSegmentRecCachePriorCacheHits = logSegmentRecCacheStats.CacheHits
		}
		if 0 != logSegmentRecCacheMissesDelta {
			stats.IncrementOperationsBy(&stats.LogSegmentRecCacheMisses, logSegmentRecCacheMissesDelta)
			globals.logSegmentRecCachePriorCacheMisses = logSegmentRecCacheStats.CacheMisses
		}

		if 0 != bPlusTreeObjectCacheHitsDelta {
			stats.IncrementOperationsBy(&stats.BPlusTreeObjectCacheHits, bPlusTreeObjectCacheHitsDelta)
			globals.bPlusTreeObjectCachePriorCacheHits = bPlusTreeObjectCacheStats.CacheHits
		}
		if 0 != bPlusTreeObjectCacheMissesDelta {
			stats.IncrementOperationsBy(&stats.BPlusTreeObjectCacheMisses, bPlusTreeObjectCacheMissesDelta)
			globals.bPlusTreeObjectCachePriorCacheMisses = bPlusTreeObjectCacheStats.CacheMisses
		}

		if 0 != createdDeletedObjectsCacheHitsDelta {
			stats.IncrementOperationsBy(&stats.CreatedDeletedObjectsCacheHits, createdDeletedObjectsCacheHitsDelta)
			globals.createdDeletedObjectsCachePriorCacheHits = createdDeletedObjectsCacheStats.CacheHits
		}
		if 0 != createdDeletedObjectsCacheMissesDelta {
			stats.IncrementOperationsBy(&stats.CreatedDeletedObjectsCacheMisses, createdDeletedObjectsCacheMissesDelta)
			globals.createdDeletedObjectsCachePriorCacheMisses = createdDeletedObjectsCacheStats.CacheMisses
		}

		globals.Unlock()
		globals.DaemonPerCheckpointStatsUpdateUsec.Add(uint64(time.Since(startTime2) / time.Microsecond))
		globals.DaemonPerCheckpointUsec.Add(uint64(time.Since(startTime) / time.Microsecond))

		if exitOnCompletion {
			return
		}
	}
}
