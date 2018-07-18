package inode

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/ansel1/merry"
	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/halter"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

// Shorthand for inode internal API debug log id; global to the package
var int_inode_debug = logger.DbgInodeInternal

const (
	optimisticInodeFetchBytes = 2048
)

type CorruptionDetected bool
type Version uint64

const (
	V1                               Version = iota + 1 // use type/struct onDiskInodeV1Struct
	onDiskInodeV1PayloadObjectOffset uint64  = 0
)

type onDiskInodeV1Struct struct { // Preceded "on disk" by CorruptionDetected then Version both in cstruct.LittleEndian form
	InodeNumber
	InodeType
	LinkCount           uint64
	Size                uint64
	CreationTime        time.Time
	ModificationTime    time.Time
	AccessTime          time.Time
	AttrChangeTime      time.Time
	NumWrites           uint64
	Mode                InodeMode
	UserID              InodeUserID
	GroupID             InodeGroupID
	StreamMap           map[string][]byte
	PayloadObjectNumber uint64            // DirInode:     B+Tree Root with Key == dir_entry_name, Value = InodeNumber
	PayloadObjectLength uint64            // FileInode:    B+Tree Root with Key == fileOffset, Value = fileExtent
	SymlinkTarget       string            // SymlinkInode: target path of symbolic link
	LogSegmentMap       map[uint64]uint64 // FileInode:    Key == LogSegment#, Value = file user data byte count
}

type inFlightLogSegmentStruct struct { // Used as (by reference) Value for inMemoryInodeStruct.inFlightLogSegmentMap
	logSegmentNumber uint64 //            Used as (by value)     Key   for inMemoryInodeStruct.inFlightLogSegmentMap
	fileInode        *inMemoryInodeStruct
	accountName      string
	containerName    string
	objectName       string
	swiftclient.ChunkedPutContext
}

type inMemoryInodeStruct struct {
	sync.Mutex        //                                             Used to synchronize with background fileInodeFlusherDaemon
	sync.WaitGroup    //                                             FileInode Flush requests wait on this
	inodeCacheLRUNext *inMemoryInodeStruct
	inodeCacheLRUPrev *inMemoryInodeStruct
	dirty             bool
	volume            *volumeStruct
	snapShotID        uint64
	payload           interface{} //                                 DirInode:  B+Tree with Key == dir_entry_name, Value = InodeNumber
	//                                                               FileInode: B+Tree with Key == fileOffset, Value = *fileExtent
	openLogSegment           *inFlightLogSegmentStruct            // FileInode only... also in inFlightLogSegmentMap
	inFlightLogSegmentMap    map[uint64]*inFlightLogSegmentStruct // FileInode: key == logSegmentNumber
	inFlightLogSegmentErrors map[uint64]error                     // FileInode: key == logSegmentNumber; value == err (if non nil)
	onDiskInodeV1Struct                                           // Real on-disk inode information embedded here
}

func (vS *volumeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsInodeNumber, ok := key.(InodeNumber)
	if !ok {
		err = fmt.Errorf("inode.volumeStruct.DumpKey() could not parse key as a InodeNumber")
		return
	}

	keyAsString = fmt.Sprintf("0x%016X", keyAsInodeNumber)

	err = nil
	return
}

func (vS *volumeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsInMemoryInodeStructPtr, ok := value.(*inMemoryInodeStruct)
	if !ok {
		err = fmt.Errorf("inode.volumeStruct.DumpValue() could not parse value as a *inMemoryInodeStruct")
		return
	}

	valueAsString = fmt.Sprintf("%016p", valueAsInMemoryInodeStructPtr)

	err = nil
	return
}

func compareInodeNumber(key1 sortedmap.Key, key2 sortedmap.Key) (result int, err error) {
	key1InodeNumber, ok := key1.(InodeNumber)
	if !ok {
		err = fmt.Errorf("compareInodeNumber(non-InodeNumber,) not supported")
		return
	}
	key2InodeNumber, ok := key2.(InodeNumber)
	if !ok {
		err = fmt.Errorf("compareInodeNumber(InodeNumber, non-InodeNumber) not supported")
		return
	}

	if key1InodeNumber < key2InodeNumber {
		result = -1
	} else if key1InodeNumber == key2InodeNumber {
		result = 0
	} else { // key1InodeNumber > key2InodeNumber
		result = 1
	}

	err = nil

	return
}

func (vS *volumeStruct) fetchOnDiskInode(inodeNumber InodeNumber) (inMemoryInode *inMemoryInodeStruct, ok bool, err error) {
	var (
		bytesConsumedByCorruptionDetected uint64
		bytesConsumedByVersion            uint64
		corruptionDetected                CorruptionDetected
		inodeRec                          []byte
		onDiskInodeV1                     *onDiskInodeV1Struct
		snapShotID                        uint64
		snapShotIDType                    headhunter.SnapShotIDType
		version                           Version
	)

	snapShotIDType, snapShotID, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		logger.Fatalf("fetchOnDiskInode for headhunter.SnapShotIDTypeDotSnapShot not allowed")
	}

	inodeRec, ok, err = vS.headhunterVolumeHandle.GetInodeRec(uint64(inodeNumber))
	if nil != err {
		stackStr := string(debug.Stack())
		err = fmt.Errorf("%s: unable to get inodeRec for inode %d: %v stack: %s",
			utils.GetFnName(), inodeNumber, err, stackStr)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}
	if !ok {
		return
	}

	bytesConsumedByCorruptionDetected, err = cstruct.Unpack(inodeRec, &corruptionDetected, cstruct.LittleEndian)
	if nil != err {
		err = fmt.Errorf("%s: unable to parse inodeRec.CorruptionDetected for inode %d: %v", utils.GetFnName(), inodeNumber, err)
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}
	if corruptionDetected {
		err = fmt.Errorf("%s: inode %d has been marked corrupted", utils.GetFnName(), inodeNumber)
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}

	bytesConsumedByVersion, err = cstruct.Unpack(inodeRec[bytesConsumedByCorruptionDetected:], &version, cstruct.LittleEndian)
	if nil != err {
		err = fmt.Errorf("%s: unable to get inodeRec.Version for inode %d: %v", utils.GetFnName(), inodeNumber, err)
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}
	if V1 != version {
		err = fmt.Errorf("%s: inodeRec.Version for inode %d (%v) not supported", utils.GetFnName(), inodeNumber, version)
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}

	onDiskInodeV1 = &onDiskInodeV1Struct{StreamMap: make(map[string][]byte)}

	err = json.Unmarshal(inodeRec[bytesConsumedByCorruptionDetected+bytesConsumedByVersion:], onDiskInodeV1)
	if nil != err {
		err = fmt.Errorf("%s: inodeRec.<body> for inode %d json.Unmarshal() failed: %v", utils.GetFnName(), inodeNumber, err)
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}

	inMemoryInode = &inMemoryInodeStruct{
		inodeCacheLRUNext:        nil,
		inodeCacheLRUPrev:        nil,
		dirty:                    false,
		volume:                   vS,
		snapShotID:               snapShotID,
		openLogSegment:           nil,
		inFlightLogSegmentMap:    make(map[uint64]*inFlightLogSegmentStruct),
		inFlightLogSegmentErrors: make(map[uint64]error),
		onDiskInodeV1Struct:      *onDiskInodeV1,
	}

	inMemoryInode.onDiskInodeV1Struct.InodeNumber = inodeNumber

	switch inMemoryInode.InodeType {
	case DirType:
		if 0 == inMemoryInode.PayloadObjectNumber {
			inMemoryInode.payload =
				sortedmap.NewBPlusTree(
					vS.maxEntriesPerDirNode,
					sortedmap.CompareString,
					&dirInodeCallbacks{treeNodeLoadable{inode: inMemoryInode}},
					globals.dirEntryCache)
		} else {
			inMemoryInode.payload, err =
				sortedmap.OldBPlusTree(
					inMemoryInode.PayloadObjectNumber,
					onDiskInodeV1PayloadObjectOffset,
					inMemoryInode.PayloadObjectLength,
					sortedmap.CompareString,
					&dirInodeCallbacks{treeNodeLoadable{inode: inMemoryInode}},
					globals.dirEntryCache)
			if nil != err {
				err = fmt.Errorf("%s: sortedmap.OldBPlusTree(inodeRec.<body>.PayloadObjectNumber) for DirType inode %d failed: %v", utils.GetFnName(), inodeNumber, err)
				err = blunder.AddError(err, blunder.CorruptInodeError)
				return
			}
		}
	case FileType:
		if 0 == inMemoryInode.PayloadObjectNumber {
			inMemoryInode.payload =
				sortedmap.NewBPlusTree(
					vS.maxExtentsPerFileNode,
					sortedmap.CompareUint64,
					&fileInodeCallbacks{treeNodeLoadable{inode: inMemoryInode}},
					globals.fileExtentMapCache)
		} else {
			inMemoryInode.payload, err =
				sortedmap.OldBPlusTree(
					inMemoryInode.PayloadObjectNumber,
					onDiskInodeV1PayloadObjectOffset,
					inMemoryInode.PayloadObjectLength,
					sortedmap.CompareUint64,
					&fileInodeCallbacks{treeNodeLoadable{inode: inMemoryInode}},
					globals.fileExtentMapCache)
			if nil != err {
				err = fmt.Errorf("%s: sortedmap.OldBPlusTree(inodeRec.<body>.PayloadObjectNumber) for FileType inode %d failed: %v", utils.GetFnName(), inodeNumber, err)
				err = blunder.AddError(err, blunder.CorruptInodeError)
				return
			}
		}
	case SymlinkType:
		// Nothing special here
	default:
		err = fmt.Errorf("%s: inodeRec.InodeType for inode %d (%v) not supported", utils.GetFnName(), inodeNumber, inMemoryInode.InodeType)
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}

	err = nil
	return
}

func (vS *volumeStruct) inodeCacheFetchWhileLocked(inodeNumber InodeNumber) (inode *inMemoryInodeStruct, ok bool, err error) {
	var (
		inodeAsValue sortedmap.Value
	)

	inodeAsValue, ok, err = vS.inodeCache.GetByKey(inodeNumber)
	if nil != err {
		return
	}

	if ok {
		inode, ok = inodeAsValue.(*inMemoryInodeStruct)
		if ok {
			vS.inodeCacheTouchWhileLocked(inode)
			err = nil
		} else {
			ok = false
			err = fmt.Errorf("inodeCache[inodeNumber==0x%016X] contains a value not mappable to a *inMemoryInodeStruct", inodeNumber)
		}
	}

	return
}

func (vS *volumeStruct) inodeCacheFetch(inodeNumber InodeNumber) (inode *inMemoryInodeStruct, ok bool, err error) {
	vS.Lock()
	inode, ok, err = vS.inodeCacheFetchWhileLocked(inodeNumber)
	vS.Unlock()
	return
}

func (vS *volumeStruct) inodeCacheInsertWhileLocked(inode *inMemoryInodeStruct) (ok bool, err error) {
	ok, err = vS.inodeCache.Put(inode.InodeNumber, inode)
	if (nil != err) || !ok {
		return
	}

	// Place inode at the MRU end of inodeCacheLRU

	if 0 == vS.inodeCacheLRUItems {
		vS.inodeCacheLRUHead = inode
		vS.inodeCacheLRUTail = inode
		vS.inodeCacheLRUItems = 1
	} else {
		inode.inodeCacheLRUPrev = vS.inodeCacheLRUTail
		inode.inodeCacheLRUPrev.inodeCacheLRUNext = inode

		vS.inodeCacheLRUTail = inode
		vS.inodeCacheLRUItems++
	}

	return
}

func (vS *volumeStruct) inodeCacheInsert(inode *inMemoryInodeStruct) (ok bool, err error) {
	vS.Lock()
	ok, err = vS.inodeCacheInsertWhileLocked(inode)
	vS.Unlock()
	return
}

func (vS *volumeStruct) inodeCacheTouchWhileLocked(inode *inMemoryInodeStruct) {
	// Move inode to the MRU end of inodeCacheLRU

	if inode != vS.inodeCacheLRUTail {
		if inode == vS.inodeCacheLRUHead {
			vS.inodeCacheLRUHead = inode.inodeCacheLRUNext
			vS.inodeCacheLRUHead.inodeCacheLRUPrev = nil

			inode.inodeCacheLRUPrev = vS.inodeCacheLRUTail
			inode.inodeCacheLRUNext = nil

			vS.inodeCacheLRUTail.inodeCacheLRUNext = inode
			vS.inodeCacheLRUTail = inode
		} else {
			inode.inodeCacheLRUPrev.inodeCacheLRUNext = inode.inodeCacheLRUNext
			inode.inodeCacheLRUNext.inodeCacheLRUPrev = inode.inodeCacheLRUPrev

			inode.inodeCacheLRUNext = nil
			inode.inodeCacheLRUPrev = vS.inodeCacheLRUTail

			vS.inodeCacheLRUTail.inodeCacheLRUNext = inode
			vS.inodeCacheLRUTail = inode
		}
	}
}

func (vS *volumeStruct) inodeCacheTouch(inode *inMemoryInodeStruct) {
	vS.Lock()
	vS.inodeCacheTouchWhileLocked(inode)
	vS.Unlock()
}

// The inode cache discard thread calls this routine when the ticker goes off.
func (vS *volumeStruct) inodeCacheDiscard() {
	inodesToDrop := uint64(0)

	vS.Lock()

	if (vS.inodeCacheLRUItems * globals.inodeSize) > vS.inodeCacheLRUMaxBytes {
		// Check, at most, 1.25 * (minimum_number_to_drop)
		inodesToDrop = (vS.inodeCacheLRUItems * globals.inodeSize) - vS.inodeCacheLRUMaxBytes
		inodesToDrop = inodesToDrop / globals.inodeSize
		inodesToDrop += inodesToDrop / 4
		for inodesToDrop > 0 {
			inodesToDrop--

			ic := vS.inodeCacheLRUHead

			// Create a DLM lock object
			id := dlm.GenerateCallerID()
			inodeRWLock, _ := vS.InitInodeLock(ic.InodeNumber, id)
			err := inodeRWLock.TryWriteLock()

			// Inode is locked; skip it
			if err != nil {
				// Move inode to tail of LRU
				vS.inodeCacheTouchWhileLocked(ic)
				continue
			}

			if ic.dirty {
				// The inode is busy - drop the DLM lock and move to tail
				inodeRWLock.Unlock()
				vS.inodeCacheTouchWhileLocked(ic)
				continue
			}

			var ok bool

			ok, err = vS.inodeCacheDropWhileLocked(ic)
			if err != nil || !ok {
				pStr := fmt.Errorf("The inodes was not found in the inode cache - ok: %v err: %v", ok, err)
				panic(pStr)
			}

			// NOTE: Releasing the locks out of order here.
			//
			// We acquire the locks in this order:
			// 1. Volume lock
			// 2. DLM lock for inode
			//
			// We then release the volume lock before deleting the inode from the cache and
			// then releasing the DLM lock.
			inodeRWLock.Unlock()

			// NOTE: vS.inodeCacheDropWhileLocked() removed the inode from the LRU list so
			// the head is now different
		}
	}
	vS.Unlock()
}

func (vS *volumeStruct) inodeCacheDropWhileLocked(inode *inMemoryInodeStruct) (ok bool, err error) {
	ok, err = vS.inodeCache.DeleteByKey(inode.InodeNumber)
	if (nil != err) || !ok {
		return
	}

	if inode == vS.inodeCacheLRUHead {
		if inode == vS.inodeCacheLRUTail {
			vS.inodeCacheLRUHead = nil
			vS.inodeCacheLRUTail = nil
			vS.inodeCacheLRUItems = 0
		} else {
			vS.inodeCacheLRUHead = inode.inodeCacheLRUNext
			vS.inodeCacheLRUHead.inodeCacheLRUPrev = nil
			vS.inodeCacheLRUItems--

			inode.inodeCacheLRUNext = nil
		}
	} else {
		if inode == vS.inodeCacheLRUTail {
			vS.inodeCacheLRUTail = inode.inodeCacheLRUPrev
			vS.inodeCacheLRUTail.inodeCacheLRUNext = nil
			vS.inodeCacheLRUItems--

			inode.inodeCacheLRUPrev = nil
		} else {
			inode.inodeCacheLRUPrev.inodeCacheLRUNext = inode.inodeCacheLRUNext
			inode.inodeCacheLRUNext.inodeCacheLRUPrev = inode.inodeCacheLRUPrev
			vS.inodeCacheLRUItems--

			inode.inodeCacheLRUNext = nil
			inode.inodeCacheLRUPrev = nil
		}
	}

	return
}

func (vS *volumeStruct) inodeCacheDrop(inode *inMemoryInodeStruct) (ok bool, err error) {
	vS.Lock()
	ok, err = vS.inodeCacheDropWhileLocked(inode)
	vS.Unlock()
	return
}

func (vS *volumeStruct) fetchInode(inodeNumber InodeNumber) (inode *inMemoryInodeStruct, ok bool, err error) {
	inode, ok, err = vS.inodeCacheFetch(inodeNumber)
	if nil != err {
		return
	}

	if ok {
		return
	}

	inode, ok, err = vS.fetchOnDiskInode(inodeNumber)
	if nil != err {
		return
	}

	if ok {
		ok, err = vS.inodeCacheInsert(inode)
		if (nil == err) && !ok {
			err = fmt.Errorf("inodeCacheInsert(inode) failed")
		}
	}

	return
}

// Fetch inode with inode type checking
func (vS *volumeStruct) fetchInodeType(inodeNumber InodeNumber, expectedType InodeType) (inode *inMemoryInodeStruct, err error) {
	inode, ok, err := vS.fetchInode(inodeNumber)
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("%s: expected inode %d volume '%s' to be type %v, but it was unallocated",
			utils.GetFnName(), inode.InodeNumber, vS.volumeName, expectedType)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}
	if inode.InodeType == expectedType {
		// success
		return
	}

	err = fmt.Errorf("%s: expected inode %d volume '%s' to be type %v, got %v",
		utils.GetFnName(), inode.InodeNumber, vS.volumeName, expectedType, inode.InodeType)

	var errVal blunder.FsError
	switch expectedType {
	case DirType:
		errVal = blunder.NotDirError
	case FileType:
		errVal = blunder.NotFileError
	case SymlinkType:
		errVal = blunder.NotSymlinkError
	default:
		panic(fmt.Sprintf("unknown inode type=%v!", expectedType))
	}
	err = blunder.AddError(err, errVal)

	return
}

func (vS *volumeStruct) makeInMemoryInodeWithThisInodeNumber(inodeType InodeType, fileMode InodeMode, userID InodeUserID, groupID InodeGroupID, inodeNumber InodeNumber, volumeLocked bool) (inMemoryInode *inMemoryInodeStruct) {
	var (
		birthTime      time.Time
		nonce          uint64
		snapShotID     uint64
		snapShotIDType headhunter.SnapShotIDType
	)

	snapShotIDType, snapShotID, nonce = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		logger.Fatalf("makeInMemoryInodeWithThisInodeNumber for headhunter.SnapShotIDTypeDotSnapShot not allowed")
	}

	birthTime = time.Now()

	inMemoryInode = &inMemoryInodeStruct{
		inodeCacheLRUNext:        nil,
		inodeCacheLRUPrev:        nil,
		dirty:                    true,
		volume:                   vS,
		snapShotID:               snapShotID,
		openLogSegment:           nil,
		inFlightLogSegmentMap:    make(map[uint64]*inFlightLogSegmentStruct),
		inFlightLogSegmentErrors: make(map[uint64]error),
		onDiskInodeV1Struct: onDiskInodeV1Struct{
			InodeNumber:      InodeNumber(nonce),
			InodeType:        inodeType,
			CreationTime:     birthTime,
			ModificationTime: birthTime,
			AccessTime:       birthTime,
			AttrChangeTime:   birthTime,
			NumWrites:        0,
			Mode:             fileMode,
			UserID:           userID,
			GroupID:          groupID,
			StreamMap:        make(map[string][]byte),
			LogSegmentMap:    make(map[uint64]uint64),
		},
	}

	return
}

func (vS *volumeStruct) makeInMemoryInode(inodeType InodeType, fileMode InodeMode, userID InodeUserID, groupID InodeGroupID) (inMemoryInode *inMemoryInodeStruct, err error) {
	inodeNumberAsUint64, err := vS.headhunterVolumeHandle.FetchNonce()
	if nil != err {
		err = fmt.Errorf("headhunter.FetchNonce() returned error: %v", err)
		return
	}

	inMemoryInode = vS.makeInMemoryInodeWithThisInodeNumber(inodeType, fileMode, userID, groupID, InodeNumber(inodeNumberAsUint64), false)

	return
}

func (inMemoryInode *inMemoryInodeStruct) convertToOnDiskInodeV1() (onDiskInodeV1 *onDiskInodeV1Struct, err error) {
	onDiskInode := inMemoryInode.onDiskInodeV1Struct

	if (DirType == inMemoryInode.InodeType) || (FileType == inMemoryInode.InodeType) {
		content := inMemoryInode.payload.(sortedmap.BPlusTree)
		payloadObjectNumber, payloadObjectOffset, payloadObjectLength, flushErr := content.Flush(false)
		if nil != flushErr {
			panic(flushErr)
		}
		pruneErr := content.Prune()
		if nil != pruneErr {
			panic(pruneErr)
		}
		if onDiskInodeV1PayloadObjectOffset != payloadObjectOffset {
			flushErr = fmt.Errorf("Logic Error: content.Flush() should have returned payloadObjectOffset == %v", onDiskInodeV1PayloadObjectOffset)
			panic(flushErr)
		}
		onDiskInode.PayloadObjectNumber = payloadObjectNumber
		onDiskInode.PayloadObjectLength = payloadObjectLength
	}

	// maps are refernce types, so this needs to be copied manually

	onDiskInode.StreamMap = make(map[string][]byte)
	for key, value := range inMemoryInode.StreamMap {
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		onDiskInode.StreamMap[key] = valueCopy
	}

	onDiskInode.LogSegmentMap = make(map[uint64]uint64)
	for logSegmentNumber, logSegmentBytesUsed := range inMemoryInode.LogSegmentMap {
		onDiskInode.LogSegmentMap[logSegmentNumber] = logSegmentBytesUsed
	}

	return &onDiskInode, nil
}

func (vS *volumeStruct) flushInode(inode *inMemoryInodeStruct) (err error) {
	err = vS.flushInodes([]*inMemoryInodeStruct{inode})
	return
}

func (vS *volumeStruct) flushInodeNumber(inodeNumber InodeNumber) (err error) {
	err = vS.flushInodeNumbers([]InodeNumber{inodeNumber})
	return
}

// REVIEW: Need to clearly explain what "flush" means (i.e. "to HH", not "to disk")

func (vS *volumeStruct) flushInodes(inodes []*inMemoryInodeStruct) (err error) {
	var (
		dirtyInodeNumbers         []uint64
		dirtyInodeRecBytes        []byte
		dirtyInodeRecs            [][]byte
		emptyLogSegments          []uint64
		emptyLogSegmentsThisInode []uint64
		inode                     *inMemoryInodeStruct
		logSegmentNumber          uint64
		logSegmentValidBytes      uint64
		onDiskInodeV1             *onDiskInodeV1Struct
		onDiskInodeV1Buf          []byte
		payloadAsBPlusTree        sortedmap.BPlusTree
		payloadObjectLength       uint64
		payloadObjectNumber       uint64
		toFlushInodeNumbers       []uint64
	)

	halter.Trigger(halter.InodeFlushInodesEntry)
	defer halter.Trigger(halter.InodeFlushInodesExit)

	toFlushInodeNumbers = make([]uint64, 0, len(inodes))
	for _, inode = range inodes {
		toFlushInodeNumbers = append(toFlushInodeNumbers, uint64(inode.InodeNumber))
	}

	evtlog.Record(evtlog.FormatFlushInodesEntry, vS.volumeName, toFlushInodeNumbers)

	// Assemble slice of "dirty" inodes while flushing them
	dirtyInodeNumbers = make([]uint64, 0, len(inodes))
	dirtyInodeRecs = make([][]byte, 0, len(inodes))
	emptyLogSegments = make([]uint64, 0)

	for _, inode = range inodes {
		if FileType == inode.InodeType {
			err = vS.doFileInodeDataFlush(inode)
			if nil != err {
				evtlog.Record(evtlog.FormatFlushInodesErrorOnInode, vS.volumeName, uint64(inode.InodeNumber), err.Error())
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.InodeFlushError)
				return
			}
			emptyLogSegmentsThisInode = make([]uint64, 0)
			for logSegmentNumber, logSegmentValidBytes = range inode.LogSegmentMap {
				if 0 == logSegmentValidBytes {
					emptyLogSegmentsThisInode = append(emptyLogSegmentsThisInode, logSegmentNumber)
				}
			}
			for _, logSegmentNumber = range emptyLogSegmentsThisInode {
				delete(inode.LogSegmentMap, logSegmentNumber)
			}
			emptyLogSegments = append(emptyLogSegments, emptyLogSegmentsThisInode...)
		}
		if SymlinkType != inode.InodeType {
			// (FileType == inode.InodeType || (DirType == inode.InodeType)
			payloadAsBPlusTree = inode.payload.(sortedmap.BPlusTree)
			payloadObjectNumber, _, payloadObjectLength, err = payloadAsBPlusTree.Flush(false)
			if nil != err {
				evtlog.Record(evtlog.FormatFlushInodesErrorOnInode, vS.volumeName, uint64(inode.InodeNumber), err.Error())
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.InodeFlushError)
				return
			}
			if payloadObjectNumber > inode.PayloadObjectNumber {
				if !inode.dirty {
					err = fmt.Errorf("Logic error: inode.dirty should have been true")
					evtlog.Record(evtlog.FormatFlushInodesErrorOnInode, vS.volumeName, uint64(inode.InodeNumber), err.Error())
					logger.ErrorWithError(err)
					err = blunder.AddError(err, blunder.InodeFlushError)
					return
				}
				// REVIEW: What if cache pressure flushed before we got here?
				//         Is it possible that Number doesn't get updated?

				if inode.PayloadObjectNumber != 0 {
					logger.Tracef("flushInodes(): volume '%s' %v inode %d: updating Payload"+
						" from Object %016X to %016X bytes %d to %d",
						vS.volumeName, inode.InodeType, inode.InodeNumber,
						inode.PayloadObjectNumber, payloadObjectNumber,
						inode.PayloadObjectLength, payloadObjectLength)
				}
				inode.PayloadObjectNumber = payloadObjectNumber
				inode.PayloadObjectLength = payloadObjectLength

				evtlog.Record(evtlog.FormatFlushInodesDirOrFilePayloadObjectNumberUpdated, vS.volumeName, uint64(inode.InodeNumber), payloadObjectNumber)
			}
		}
		if inode.dirty {
			onDiskInodeV1, err = inode.convertToOnDiskInodeV1()
			if nil != err {
				evtlog.Record(evtlog.FormatFlushInodesErrorOnInode, vS.volumeName, uint64(inode.InodeNumber), err.Error())
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.InodeFlushError)
				return
			}
			onDiskInodeV1Buf, err = json.Marshal(onDiskInodeV1)
			if nil != err {
				evtlog.Record(evtlog.FormatFlushInodesErrorOnInode, vS.volumeName, uint64(inode.InodeNumber), err.Error())
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.InodeFlushError)
				return
			}
			dirtyInodeRecBytes = make([]byte, 0, len(globals.inodeRecDefaultPreambleBuf)+len(onDiskInodeV1Buf))
			dirtyInodeRecBytes = append(dirtyInodeRecBytes, globals.inodeRecDefaultPreambleBuf...)
			dirtyInodeRecBytes = append(dirtyInodeRecBytes, onDiskInodeV1Buf...)
			dirtyInodeNumbers = append(dirtyInodeNumbers, uint64(inode.InodeNumber))
			dirtyInodeRecs = append(dirtyInodeRecs, dirtyInodeRecBytes)
		}
	}

	// Go update HeadHunter (if necessary)
	if 0 < len(dirtyInodeNumbers) {
		err = vS.headhunterVolumeHandle.PutInodeRecs(dirtyInodeNumbers, dirtyInodeRecs)
		if nil != err {
			evtlog.Record(evtlog.FormatFlushInodesErrorOnHeadhunterPut, vS.volumeName, err.Error())
			logger.ErrorWithError(err)
			err = blunder.AddError(err, blunder.InodeFlushError)
			return
		}
		for _, inode = range inodes {
			inode.dirty = false
		}
	}

	// Now do phase one of garbage collection
	if 0 < len(emptyLogSegments) {
		for _, logSegmentNumber = range emptyLogSegments {
			err = vS.headhunterVolumeHandle.DeleteLogSegmentRec(logSegmentNumber)
			if nil != err {
				logger.WarnfWithError(err, "couldn't delete garbage log segment")
			}
		}
	}

	evtlog.Record(evtlog.FormatFlushInodesExit, vS.volumeName, toFlushInodeNumbers)

	err = nil
	return
}

func (vS *volumeStruct) flushInodeNumbers(inodeNumbers []InodeNumber) (err error) {
	var (
		inode       *inMemoryInodeStruct
		inodes      []*inMemoryInodeStruct
		inodeNumber InodeNumber
		ok          bool
	)

	// Fetch referenced inodes
	inodes = make([]*inMemoryInodeStruct, 0, len(inodeNumbers))
	for _, inodeNumber = range inodeNumbers {
		inode, ok, err = vS.fetchInode(inodeNumber)
		if nil != err {
			// the inode is locked so this should never happen (unless the inode
			// was evicted from the cache and it was corrupt when read from disk)
			// (err includes volume name and inode number)
			logger.ErrorfWithError(err, "%s: fetch of inode to flush failed", utils.GetFnName())
			err = blunder.AddError(err, blunder.InodeFlushError)
			return
		}
		if !ok {
			// this should never happen (see above)
			err = fmt.Errorf("%s: fetch of inode %d volume '%s' failed because it is unallocated",
				utils.GetFnName(), inodeNumber, vS.volumeName)
			logger.ErrorWithError(err)
			err = blunder.AddError(err, blunder.NotFoundError)
			return
		}

		inodes = append(inodes, inode)
	}

	err = vS.flushInodes(inodes)

	return
}

func accountNameToVolumeName(accountName string) (volumeName string, ok bool) {
	globals.Lock()

	volume, ok := globals.accountMap[accountName]

	if ok {
		volumeName = volume.volumeName
	}

	globals.Unlock()

	return
}

func volumeNameToActivePeerPrivateIPAddr(volumeName string) (activePeerPrivateIPAddr string, ok bool) {
	globals.Lock()

	volume, ok := globals.volumeMap[volumeName]

	if ok {
		activePeerPrivateIPAddr = volume.activePeerPrivateIPAddr
	}

	globals.Unlock()

	return
}

func fetchVolumeHandle(volumeName string) (volumeHandle VolumeHandle, err error) {
	globals.Lock()
	volume, ok := globals.volumeMap[volumeName]
	globals.Unlock()

	if !ok {
		err = fmt.Errorf("%s: volumeName \"%v\" not found", utils.GetFnName(), volumeName)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	volumeHandle = volume

	volume.Lock()         // REVIEW: Once Tracker https://www.pivotaltracker.com/story/show/133377567
	defer volume.Unlock() //         is resolved, these two lines should be removed

	if !volume.active {
		err = fmt.Errorf("%s: volumeName \"%v\" not active", utils.GetFnName(), volumeName)
		err = blunder.AddError(err, blunder.NotActiveError)
		return
	}

	_, ok, err = volume.headhunterVolumeHandle.GetInodeRec(uint64(RootDirInodeNumber))
	if nil != err {
		// disk corruption of the inode btree (or software error)
		err = fmt.Errorf("%s: unable to lookup root inode for volume '%s': %v",
			utils.GetFnName(), volume.volumeName, err)
		err = blunder.AddError(err, blunder.NotFoundError)
	}
	if !ok {
		// First access didn't find root dir... so create it
		_, err = volume.createRootOrSubDir(PosixModePerm, 0, 0, true)
		if nil != err {
			err = fmt.Errorf("%s: unable to create root inode for volume '%s': %v",
				utils.GetFnName(), volume.volumeName, err)
			err = blunder.AddError(err, blunder.NotFoundError)
		}
	}

	// If we get this far, return values are already set as desired

	err = nil

	return
}

func (vS *volumeStruct) provisionPhysicalContainer(physicalContainerLayout *physicalContainerLayoutStruct) (err error) {
	existingObjectCount := physicalContainerLayout.physicalContainerNameSliceLoopCount % (physicalContainerLayout.physicalContainerCountMax + 1)

	if 0 == existingObjectCount {
		// We need to provision a new PhysicalContainer in this PhysicalContainerLayout

		physicalContainerNameSuffix, nonShadowingErr := vS.headhunterVolumeHandle.FetchNonce()
		if nil != nonShadowingErr {
			return
		}

		newContainerName := fmt.Sprintf("%s%s", physicalContainerLayout.physicalContainerNamePrefix, utils.Uint64ToHexStr(physicalContainerNameSuffix))

		storagePolicyHeaderValues := []string{vS.defaultPhysicalContainerLayout.physicalContainerStoragePolicy}
		newContainerHeaders := make(map[string][]string)
		newContainerHeaders["X-Storage-Policy"] = storagePolicyHeaderValues

		err = swiftclient.ContainerPut(vS.accountName, newContainerName, newContainerHeaders)
		if nil != err {
			return
		}

		physicalContainerLayout.physicalContainerNameSlice[physicalContainerLayout.physicalContainerNameSliceNextIndex] = newContainerName
	}

	err = nil
	return
}

func (vS *volumeStruct) provisionObject() (containerName string, objectNumber uint64, err error) {
	objectNumber, err = vS.headhunterVolumeHandle.FetchNonce()
	if nil != err {
		return
	}

	vS.Lock()

	err = vS.provisionPhysicalContainer(vS.defaultPhysicalContainerLayout)
	if nil != err {
		vS.Unlock()
		return
	}

	containerName = vS.defaultPhysicalContainerLayout.physicalContainerNameSlice[vS.defaultPhysicalContainerLayout.physicalContainerNameSliceNextIndex]

	vS.defaultPhysicalContainerLayout.physicalContainerNameSliceNextIndex++

	if vS.defaultPhysicalContainerLayout.physicalContainerNameSliceNextIndex == vS.defaultPhysicalContainerLayout.physicalContainerCountMax {
		vS.defaultPhysicalContainerLayout.physicalContainerNameSliceNextIndex = 0
		vS.defaultPhysicalContainerLayout.physicalContainerNameSliceLoopCount++
	}

	vS.Unlock()

	err = nil
	return
}

func (vS *volumeStruct) Access(inodeNumber InodeNumber, userID InodeUserID, groupID InodeGroupID, otherGroupIDs []InodeGroupID, accessMode InodeMode, override AccessOverride) (accessReturn bool) {
	var (
		adjustedInodeNumber InodeNumber
		err                 error
		groupIDCheck        bool
		ok                  bool
		otherGroupID        InodeGroupID
		ourInode            *inMemoryInodeStruct
		ourInodeGroupID     InodeGroupID
		ourInodeMode        InodeMode
		ourInodeUserID      InodeUserID
		snapShotIDType      headhunter.SnapShotIDType
	)

	snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))

	switch snapShotIDType {
	case headhunter.SnapShotIDTypeLive:
		adjustedInodeNumber = inodeNumber
	case headhunter.SnapShotIDTypeSnapShot:
		adjustedInodeNumber = inodeNumber
	case headhunter.SnapShotIDTypeDotSnapShot:
		adjustedInodeNumber = RootDirInodeNumber
	default:
		logger.Fatalf("headhunter.SnapShotU64Decode(inodeNumber == 0x%016X) returned unknown snapShotIDType: %v", inodeNumber, snapShotIDType)
	}
	if (headhunter.SnapShotIDTypeLive != snapShotIDType) && (0 != (W_OK & accessMode)) {
		err = blunder.NewError(blunder.InvalidArgError, "Access() where accessMode includes W_OK of non-LiveView inodeNumber not allowed")
		return
	}

	ourInode, ok, err = vS.fetchInode(adjustedInodeNumber)
	if nil != err {
		// this indicates disk corruption or software bug
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())

		// if we can't fetch the inode we can't access it
		accessReturn = false
		return
	}
	if !ok {
		// disk corruption or client requested a free inode
		logger.Infof("%s: fetch of inode %d volume '%s' failed because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)

		// if the inode is free then we can't access it
		accessReturn = false
		return
	}

	ourInodeUserID = ourInode.UserID
	ourInodeGroupID = ourInode.GroupID

	if headhunter.SnapShotIDTypeLive == snapShotIDType {
		ourInodeMode = ourInode.Mode
	} else {
		ourInodeMode = ourInode.Mode // TODO: Make it read-only...
	}

	if F_OK == accessMode {
		// the inode exists so its F_OK
		accessReturn = true
		return
	}

	if P_OK == accessMode {
		accessReturn = (InodeRootUserID == userID) || (userID == ourInodeUserID)
		return
	}

	if accessMode != (accessMode & (R_OK | W_OK | X_OK)) {
		// Default to false if P_OK bit set along with any others)
		accessReturn = false
		return
	}

	// Only the LiveView is ever writeable... even by the root user
	if (accessMode&W_OK != 0) && (headhunter.SnapShotIDTypeLive != snapShotIDType) {
		accessReturn = false
		return
	}

	// The root user (if not squashed) can do anything except exec files
	// that are not executable by any user
	if userID == InodeRootUserID {
		if (accessMode&X_OK != 0) && (ourInodeMode&(X_OK<<6|X_OK<<3|X_OK) == 0) {
			accessReturn = false
		} else {
			accessReturn = true
		}
		return
	}

	// We check against permissions for the user, group, and other.  The
	// first match wins (not the first permission granted).  If the user is
	// the owner of the file then those permission bits determine what
	// happens.  In other words, if the permission bits deny read permission
	// to the owner of a file but allow read permission for group and other,
	// then everyone except the owner of the file can read it.
	//
	// On a local file system, the owner of a file is *not* allowed to write
	// to the file unless it was opened for writing and the permission bits
	// allowed it *or* the process created the file and opened it for
	// writing at the same time.  However, NFS does not have an open state
	// (there's no file descriptor that tracks permissions when the the file
	// was opened) so we check for write permission on every write.  This
	// breaks things like tar when it tries to unpack a file which has
	// permission 0444 (read only).  On a local file system that works, but
	// it doesn't work for NFS unless we bend the rules a bit for the owner
	// of the file and allow the owner to write to the file even if
	// appropriate permissions are lacking.  (This is only done for the user
	// that owns the file, not the group that owns the file. Note that the
	// owner can always change the permissions to allow writing so its not a
	// security risk, but the owning group cannot).
	//
	// Note that the NFS client will typically call Access() when an app
	// wants to open the file and fail an open request for writing that if
	// the permission bits do not allow it.
	//
	// Similar rules apply to Read() and Truncate() (for ftruncate(2)), but
	// not for execute permission.  Also, this only applies to regular files
	// but we'll rely on the caller for that.
	if userID == ourInodeUserID {
		if override == OwnerOverride && (accessMode&X_OK == 0) {
			accessReturn = true
		} else {
			accessReturn = (((ourInodeMode >> 6) & accessMode) == accessMode)
		}
		return
	}

	groupIDCheck = (groupID == ourInodeGroupID)
	if !groupIDCheck {
		for _, otherGroupID = range otherGroupIDs {
			if otherGroupID == ourInodeGroupID {
				groupIDCheck = true
				break
			}
		}
	}
	if groupIDCheck {
		accessReturn = ((((ourInodeMode >> 3) & 07) & accessMode) == accessMode)
		return
	}

	accessReturn = ((((ourInodeMode >> 0) & 07) & accessMode) == accessMode)
	return
}

func (vS *volumeStruct) ProvisionObject() (objectPath string, err error) {
	containerName, objectNumber, err := vS.provisionObject()
	if nil != err {
		return
	}

	objectPath = fmt.Sprintf("/v1/%s/%s/%016X", vS.accountName, containerName, objectNumber)

	err = nil
	return
}

func (vS *volumeStruct) Purge(inodeNumber InodeNumber) (err error) {
	var (
		inode *inMemoryInodeStruct
		ok    bool
	)

	inode, ok, err = vS.inodeCacheFetch(inodeNumber)
	if (nil != err) || !ok {
		return
	}

	if inode.dirty {
		err = fmt.Errorf("Inode dirty... cannot be purged")
		return
	}

	ok, err = vS.inodeCacheDrop(inode)
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("inodeCacheDrop(inode) failed")
	}

	return
}

func (vS *volumeStruct) Destroy(inodeNumber InodeNumber) (err error) {
	logger.Tracef("inode.Destroy(): volume '%s' inode %d", vS.volumeName, inodeNumber)

	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("Destroy() on non-LiveView inodeNumber not allowed")
		return
	}

	ourInode, ok, err := vS.fetchInode(inodeNumber)
	if nil != err {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		// (err includes volume name and inode number)
		logger.ErrorWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: cannot destroy inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		err = blunder.AddError(err, blunder.NotFoundError)
		logger.ErrorWithError(err)
		return
	}

	ok, err = vS.inodeCacheDrop(ourInode)
	if nil != err {
		logger.ErrorWithError(err, "%s: inodeCacheDrop() of inode failed: %v", utils.GetFnName(), err)
		return
	}
	if !ok {
		logger.ErrorWithError(err, "%s: inodeCacheDrop() of inode returned !ok", utils.GetFnName())
		return
	}

	if ourInode.InodeType == FileType {
		_ = vS.doFileInodeDataFlush(ourInode)
	}

	err = vS.headhunterVolumeHandle.DeleteInodeRec(uint64(inodeNumber))
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	if DirType == ourInode.InodeType {
		logger.Tracef("inode.Destroy(): volume '%s' inode %d: discarding dirmap payload Object %016X  len %d",
			vS.volumeName, inodeNumber, ourInode.PayloadObjectNumber, ourInode.PayloadObjectLength)

		dirMapping := ourInode.payload.(sortedmap.BPlusTree)

		err = dirMapping.Discard()
		if nil != err {
			logger.ErrorWithError(err)
			return
		}

		stats.IncrementOperations(&stats.DirDestroyOps)

	} else if FileType == ourInode.InodeType {
		logger.Tracef("inode.Destroy(): volume '%s' inode %d: discarding extmap payload Object %016X  len %d",
			vS.volumeName, inodeNumber, ourInode.PayloadObjectNumber, ourInode.PayloadObjectLength)

		extents := ourInode.payload.(sortedmap.BPlusTree)

		err = extents.Discard()
		if nil != err {
			logger.ErrorWithError(err)
			return
		}

		for logSegmentNumber := range ourInode.LogSegmentMap {
			deleteSegmentErr := vS.headhunterVolumeHandle.DeleteLogSegmentRec(logSegmentNumber)
			if nil != deleteSegmentErr {
				logger.WarnfWithError(deleteSegmentErr, "couldn't delete destroy'd log segment")
				return
			}
			stats.IncrementOperations(&stats.GcLogSegDeleteOps)
		}
		stats.IncrementOperations(&stats.GcLogSegOps)

		stats.IncrementOperations(&stats.FileDestroyOps)
	} else { // SymlinkType == ourInode.InodeType
		stats.IncrementOperations(&stats.SymlinkDestroyOps)
	}

	return
}

func (vS *volumeStruct) GetMetadata(inodeNumber InodeNumber) (metadata *MetadataStruct, err error) {
	var (
		inode          *inMemoryInodeStruct
		ok             bool
		pos            int
		snapShotIDType headhunter.SnapShotIDType
	)

	snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		// For /<SnapShotDirName>, start with metadata from /
		inode, ok, err = vS.fetchInode(RootDirInodeNumber)
	} else {
		inode, ok, err = vS.fetchInode(inodeNumber)
	}

	if nil != err {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		err = blunder.AddError(err, blunder.NotFoundError)
		logger.InfoWithError(err)
		return
	}

	metadata = &MetadataStruct{
		InodeType:            inode.InodeType,
		LinkCount:            inode.LinkCount,
		Size:                 inode.Size,
		CreationTime:         inode.CreationTime,
		ModificationTime:     inode.ModificationTime,
		AccessTime:           inode.AccessTime,
		AttrChangeTime:       inode.AttrChangeTime,
		NumWrites:            inode.NumWrites,
		InodeStreamNameSlice: make([]string, len(inode.StreamMap)),
		Mode:                 inode.Mode,
		UserID:               inode.UserID,
		GroupID:              inode.GroupID,
	}

	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		// For /<SnapShotDirName>, simply remove Write Access... and skip InodeStreamNameSlice
		metadata.Mode &= metadata.Mode & ^(W_OK<<6 | W_OK<<3 | W_OK<<0)
	} else {
		if headhunter.SnapShotIDTypeSnapShot == snapShotIDType {
			// For inodes in a SnapShot, simply remove Write Access
			metadata.Mode &= metadata.Mode & ^(W_OK<<6 | W_OK<<3 | W_OK<<0)
		}
		pos = 0
		for inodeStreamName := range inode.StreamMap {
			metadata.InodeStreamNameSlice[pos] = inodeStreamName
			pos++
		}
	}

	stats.IncrementOperations(&stats.InodeGetMetadataOps)
	return
}

func (vS *volumeStruct) GetType(inodeNumber InodeNumber) (inodeType InodeType, err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		inodeType = DirType
		err = nil
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if nil != err {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	inodeType = inode.InodeType

	stats.IncrementOperations(&stats.InodeGetTypeOps)
	return
}

func (vS *volumeStruct) GetLinkCount(inodeNumber InodeNumber) (linkCount uint64, err error) {
	var (
		adjustLinkCountForSnapShotSubDirInRootDirInode bool
		inode                                          *inMemoryInodeStruct
		ok                                             bool
		snapShotCount                                  uint64
		snapShotIDType                                 headhunter.SnapShotIDType
	)

	if RootDirInodeNumber == inodeNumber {
		// Account for .. in /<SnapShotDirName> if any SnapShot's exist
		snapShotCount = vS.headhunterVolumeHandle.SnapShotCount()
		adjustLinkCountForSnapShotSubDirInRootDirInode = (0 != snapShotCount)
	} else {
		snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
		if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
			// linkCount == 1 (/<SnapShotDirName>'s '.') + 1 (/'s reference to <SnapShotDirName>) + # SnapShot's (/..' in each SnapShot's /)
			snapShotCount = vS.headhunterVolumeHandle.SnapShotCount()
			linkCount = 1 + 1 + snapShotCount
			err = nil
			return
		}
		adjustLinkCountForSnapShotSubDirInRootDirInode = false
	}

	inode, ok, err = vS.fetchInode(inodeNumber)
	if nil != err {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	if adjustLinkCountForSnapShotSubDirInRootDirInode {
		linkCount = inode.LinkCount + 1
	} else {
		linkCount = inode.LinkCount
	}

	return
}

// SetLinkCount is used to adjust the LinkCount property to match current reference count during FSCK TreeWalk.
func (vS *volumeStruct) SetLinkCount(inodeNumber InodeNumber, linkCount uint64) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetLinkCount() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	inode.dirty = true
	inode.LinkCount = linkCount

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

func (vS *volumeStruct) SetCreationTime(inodeNumber InodeNumber, CreationTime time.Time) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetCreationTime() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	inode.dirty = true
	inode.AttrChangeTime = time.Now()
	inode.CreationTime = CreationTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}
	return
}

func (vS *volumeStruct) SetModificationTime(inodeNumber InodeNumber, ModificationTime time.Time) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetModificationTime() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	inode.dirty = true
	inode.AttrChangeTime = time.Now()
	inode.ModificationTime = ModificationTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

func (vS *volumeStruct) SetAccessTime(inodeNumber InodeNumber, accessTime time.Time) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetAccessTime() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	inode.dirty = true
	inode.AttrChangeTime = time.Now()
	inode.AccessTime = accessTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

// NOTE: Would have liked to use os.FileMode bitmask definitions here instead of creating our own,
//       but unfortunately the bitmasks used by os.ModeDir and os.ModeSymlink (0x80000000 and 0x8000000)
//       are not the same values as what is expected on the linux side (0x4000 and 0xa000).
const (
	PosixModeDir     InodeMode = 0x4000
	PosixModeFile    InodeMode = 0x8000
	PosixModeSymlink InodeMode = 0xa000
	PosixModePerm    InodeMode = 0777
)

func determineMode(filePerm InodeMode, inodeType InodeType) (fileMode InodeMode, err error) {
	// Caller should only be setting the file perm bits, but samba seems to send file type
	// bits as well. Since we need to work with whatever samba does, let's just silently
	// mask off the other bits.
	if filePerm&^PosixModePerm != 0 {
		logger.Tracef("inode.determineMode(): invalid file mode 0x%x (max 0x%x); removing file type bits.", uint32(filePerm), uint32(PosixModePerm))
	}

	// Build fileMode starting with the file permission bits
	fileMode = filePerm & PosixModePerm

	// Add the file type to the mode.
	switch inodeType {
	case DirType:
		fileMode |= PosixModeDir
	case FileType:
		fileMode |= PosixModeFile
		break
	case SymlinkType:
		fileMode |= PosixModeSymlink
	default:
		err = fmt.Errorf("%s: unrecognized inode type %v", utils.GetFnName(), inodeType)
		err = blunder.AddError(err, blunder.InvalidInodeTypeError)
		return
	}

	err = nil

	return
}

func (vS *volumeStruct) SetPermMode(inodeNumber InodeNumber, filePerm InodeMode) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetPermMode() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	// Create file mode out of file permissions plus inode type
	fileMode, err := determineMode(filePerm, inode.InodeType)
	if err != nil {
		return err
	}

	inode.dirty = true
	inode.Mode = fileMode

	updateTime := time.Now()
	inode.AttrChangeTime = updateTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

func (vS *volumeStruct) SetOwnerUserID(inodeNumber InodeNumber, userID InodeUserID) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetOwnerUserID() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	inode.dirty = true
	inode.UserID = userID

	updateTime := time.Now()
	inode.AttrChangeTime = updateTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

func (vS *volumeStruct) SetOwnerUserIDGroupID(inodeNumber InodeNumber, userID InodeUserID, groupID InodeGroupID) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetOwnerUserIDGroupID() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	inode.dirty = true
	inode.UserID = userID
	inode.GroupID = groupID

	updateTime := time.Now()
	inode.AttrChangeTime = updateTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

func (vS *volumeStruct) SetOwnerGroupID(inodeNumber InodeNumber, groupID InodeGroupID) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("SetOwnerGroupID() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.ErrorWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	inode.dirty = true
	inode.GroupID = groupID

	updateTime := time.Now()
	inode.AttrChangeTime = updateTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

func (vS *volumeStruct) GetStream(inodeNumber InodeNumber, inodeStreamName string) (buf []byte, err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		err = fmt.Errorf("No stream '%v'", inodeStreamName)
		return buf, blunder.AddError(err, blunder.StreamNotFound)
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return nil, err
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return nil, err
	}

	inodeStreamBuf, ok := inode.StreamMap[inodeStreamName]

	if !ok {
		err = fmt.Errorf("No stream '%v'", inodeStreamName)
		return buf, blunder.AddError(err, blunder.StreamNotFound)
	}

	buf = make([]byte, len(inodeStreamBuf))

	copy(buf, inodeStreamBuf)

	err = nil

	return
}

func (vS *volumeStruct) PutStream(inodeNumber InodeNumber, inodeStreamName string, buf []byte) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("PutStream() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return err
	}

	inodeStreamBuf := make([]byte, len(buf))

	copy(inodeStreamBuf, buf)

	inode.dirty = true
	inode.StreamMap[inodeStreamName] = inodeStreamBuf

	updateTime := time.Now()
	inode.AttrChangeTime = updateTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

func (vS *volumeStruct) DeleteStream(inodeNumber InodeNumber, inodeStreamName string) (err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = fmt.Errorf("DeleteStream() on non-LiveView inodeNumber not allowed")
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	inode.dirty = true
	delete(inode.StreamMap, inodeStreamName)

	updateTime := time.Now()
	inode.AttrChangeTime = updateTime

	err = vS.flushInode(inode)
	if err != nil {
		logger.ErrorWithError(err)
		return err
	}

	return
}

func (vS *volumeStruct) FetchLayoutReport(inodeNumber InodeNumber) (layoutReport sortedmap.LayoutReport, err error) {
	snapShotIDType, _, _ := vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		layoutReport = make(sortedmap.LayoutReport)
		err = nil
		return
	}

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return nil, err
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return nil, err
	}

	if SymlinkType == inode.InodeType {
		layoutReport = make(sortedmap.LayoutReport)
		err = nil
	} else {
		layoutReport, err = inode.payload.(sortedmap.BPlusTree).FetchLayoutReport()
	}

	return
}

func (vS *volumeStruct) FetchFragmentationReport(inodeNumber InodeNumber) (fragmentationReport FragmentationReport, err error) {
	err = fmt.Errorf("FetchFragmentationReport not yet implemented")
	return
}

func (vS *volumeStruct) Optimize(inodeNumber InodeNumber, maxDuration time.Duration) (err error) {
	err = fmt.Errorf("Optimize not yet implemented")
	return
}

func validateFileExtents(snapShotID uint64, ourInode *inMemoryInodeStruct) (err error) {
	var (
		zero = uint64(0)
	)

	readPlan, readPlanBytes, err := ourInode.volume.getReadPlanHelper(snapShotID, ourInode, &zero, nil)
	if err != nil {
		return err
	}

	// We read the whole file, so these should match
	if readPlanBytes != ourInode.Size {
		return blunder.NewError(blunder.CorruptInodeError, "inode %v had recorded size %v bytes, but full read plan was only %v bytes", ourInode.InodeNumber, ourInode.Size, readPlanBytes)
	}

	// Let's check that the read plan is consistent with what the inode's
	// internal log-segment map says about which segments should have how much data.
	//
	// Make a copy of the inode's LogSegmentMap map so we can decrement the
	// byte count for each segment as we walk the readPlan entries.
	remainingExpectedBytes := make(map[uint64]uint64)
	for segmentNumber, segmentBytesUsed := range ourInode.LogSegmentMap {
		remainingExpectedBytes[segmentNumber] += segmentBytesUsed
	}
	// Then we can compare with the actual read plan we got ...
	for _, readPlanStep := range readPlan {

		// holes in a sparse file aren't counted
		if readPlanStep.LogSegmentNumber == 0 {
			continue
		}
		pathSegments := strings.Split(readPlanStep.ObjectPath, "/")
		logSegmentRepresentation := pathSegments[len(pathSegments)-1]
		logSegmentNumber, hexConvErr := utils.HexStrToUint64(logSegmentRepresentation)
		if hexConvErr != nil {
			return blunder.NewError(blunder.CorruptInodeError,
				"conversion of read plan object name to log segment number failed; "+
					"readPlanStep: %v  logSegmentString: '%v'  err: %v",
				readPlanStep, logSegmentRepresentation, hexConvErr)
		}
		remainingExpectedBytes[logSegmentNumber] -= readPlanStep.Length
	}
	// ... and fail validation if any log segment didn't match. We'll put the
	// mismatches in a separate map that we'll attach to the error in case a
	// consumer or logger wants it.
	logSegmentByteCountMismatches := make(map[uint64]uint64)
	for logSegmentNumber, remainingExpectedByteCount := range remainingExpectedBytes {
		if remainingExpectedByteCount != 0 {
			logSegmentByteCountMismatches[logSegmentNumber] = remainingExpectedByteCount
		}
	}
	if len(logSegmentByteCountMismatches) != 0 {
		rootErr := fmt.Errorf("inconsistency detected between log segment map and read plan for inode %v", ourInode.InodeNumber)
		return merry.WithValue(blunder.AddError(rootErr, blunder.CorruptInodeError), "logSegmentByteCountMismatches", logSegmentByteCountMismatches)
	}

	// Having verified that our read plan is consistent with our internal log
	// segment map, we also want to check that it's consistent with the actual log
	// segment objects in Swift. First, we'll construct a map of object paths to
	// the largest offset we would need read up to in that object.
	objectPathToEndOffset := make(map[string]uint64)

	for _, planStep := range readPlan {

		// holes in a sparse file don't have objects
		if planStep.LogSegmentNumber == 0 {
			continue
		}
		stepEndOffset := planStep.Offset + planStep.Length
		endOffset, ok := objectPathToEndOffset[planStep.ObjectPath]
		if !ok || stepEndOffset > endOffset {
			objectPathToEndOffset[planStep.ObjectPath] = stepEndOffset
		}
	}

	// then, HEAD each object to make sure that it has enough bytes.
	for objectPath, endOffset := range objectPathToEndOffset {
		accountName, containerName, objectName, err := utils.PathToAcctContObj(objectPath)
		if err != nil {
			logger.ErrorWithError(err)
			return err
		}

		contentLength, err := swiftclient.ObjectContentLength(accountName, containerName, objectName)
		if err != nil {
			logger.ErrorWithError(err)
			return err
		}

		if contentLength < endOffset {
			// REVIEW: it might be helpful to continue and make a combined report of all
			//         insufficiently long log segments, rather than erroring out immediately
			err = fmt.Errorf("expected %q to have at least %v bytes, content length was %v", objectPath, endOffset, contentLength)
			logger.ErrorWithError(err)
			return err
		}

	}

	return nil
}

func (vS *volumeStruct) markCorrupted(inodeNumber InodeNumber) (err error) {
	var (
		inodeRec       []byte
		ok             bool
		snapShotIDType headhunter.SnapShotIDType
	)

	snapShotIDType, _, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeLive != snapShotIDType {
		err = blunder.NewError(blunder.InvalidArgError, "markCorrupted() of non-LiveView inodeNumber not allowed")
		return
	}

	inodeRec, ok, err = vS.headhunterVolumeHandle.GetInodeRec(uint64(inodeNumber))
	if nil == err && ok && (len(globals.corruptionDetectedTrueBuf) <= len(inodeRec)) {
		// Just overwrite CorruptionDetected field with true
		_ = copy(inodeRec, globals.corruptionDetectedTrueBuf)
	} else {
		// Use a simple CorruptionDetected == true inodeRec
		inodeRec = globals.corruptionDetectedTrueBuf
	}

	err = vS.headhunterVolumeHandle.PutInodeRec(uint64(inodeNumber), inodeRec)

	return
}

func (vS *volumeStruct) Validate(inodeNumber InodeNumber, deeply bool) (err error) {
	var (
		ok             bool
		ourInode       *inMemoryInodeStruct
		snapShotID     uint64
		snapShotIDType headhunter.SnapShotIDType
		tree           sortedmap.BPlusTree
	)

	snapShotIDType, snapShotID, _ = vS.headhunterVolumeHandle.SnapShotU64Decode(uint64(inodeNumber))
	if headhunter.SnapShotIDTypeDotSnapShot == snapShotIDType {
		err = nil // Since /<SnapShotDirName> is emulated, always return success
		return
	}

	// we don't want to use the in-memory cache for this; we'll need to fetch
	// the current real-world bits from disk.

	// If this is a file inode, we flush to ensure that the inode is not dirty
	// (and that DLM locking therefore ensures we have exclusive access to the
	// inode and don't need to serialize this operation, as there can be no pending
	// time-based flush to race with).

	err = vS.flushInodeNumber(inodeNumber)
	if nil != err {
		logger.ErrorfWithError(err, "couldn't flush inode %v", inodeNumber)
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}

	err = vS.Purge(inodeNumber)
	if nil != err {
		logger.ErrorfWithError(err, "couldn't purge inode %v", inodeNumber)
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}

	ourInode, ok, err = vS.fetchInode(inodeNumber)
	if nil != err {
		// this indicates diskj corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		err = blunder.AddError(err, blunder.CorruptInodeError)
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because it is unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	switch ourInode.InodeType {
	case DirType, FileType:
		tree, ok = ourInode.payload.(sortedmap.BPlusTree)
		if !ok {
			err = fmt.Errorf("type conversion of inode %v payload to sortedmap.BPlusTree failed", ourInode.InodeNumber)
			err = blunder.AddError(err, blunder.CorruptInodeError)
			_ = vS.markCorrupted(inodeNumber)
			return
		}
		err = tree.Validate()
		if nil != err {
			err = blunder.AddError(err, blunder.CorruptInodeError)
			_ = vS.markCorrupted(inodeNumber)
			return
		}
		if FileType == ourInode.InodeType {
			if deeply {
				err = validateFileExtents(snapShotID, ourInode)
				if nil != err {
					err = blunder.AddError(err, blunder.CorruptInodeError)
					_ = vS.markCorrupted(inodeNumber)
					return
				}
			}
		}
	case SymlinkType:
		// Nothing to be done here
	default:
		err = fmt.Errorf("unrecognized inode type")
		err = blunder.AddError(err, blunder.CorruptInodeError)
		_ = vS.markCorrupted(inodeNumber)
		return
	}

	err = nil
	return
}
