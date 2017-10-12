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
	sync.Mutex     //                                                Used to synchronize with background fileInodeFlusherDaemon
	sync.WaitGroup //                                                FileInode Flush requests wait on this
	dirty          bool
	volume         *volumeStruct
	payload        interface{} //                                    DirInode:  B+Tree with Key == dir_entry_name, Value = InodeNumber
	//                                                               FileInode: B+Tree with Key == fileOffset, Value = *fileExtent
	openLogSegment           *inFlightLogSegmentStruct            // FileInode only... also in inFlightLogSegmentMap
	inFlightLogSegmentMap    map[uint64]*inFlightLogSegmentStruct // FileInode: key == logSegmentNumber
	inFlightLogSegmentErrors map[uint64]error                     // FileInode: key == logSegmentNumber; value == err (if non nil)
	onDiskInodeV1Struct                                           // Real on-disk inode information embedded here
}

func (vS *volumeStruct) fetchOnDiskInode(inodeNumber InodeNumber) (inMemoryInode *inMemoryInodeStruct, ok bool, err error) {
	var (
		bytesConsumedByCorruptionDetected uint64
		bytesConsumedByVersion            uint64
		corruptionDetected                CorruptionDetected
		inodeRec                          []byte
		onDiskInodeV1                     *onDiskInodeV1Struct
		version                           Version
	)

	logger.Tracef("inode.fetchOnDiskInode(): volume '%s' inode %d", vS.volumeName, inodeNumber)

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
		dirty:                    true,
		volume:                   vS,
		openLogSegment:           nil,
		inFlightLogSegmentMap:    make(map[uint64]*inFlightLogSegmentStruct),
		inFlightLogSegmentErrors: make(map[uint64]error),
		onDiskInodeV1Struct:      *onDiskInodeV1,
	}

	switch inMemoryInode.InodeType {
	case DirType:
		if 0 == inMemoryInode.PayloadObjectNumber {
			inMemoryInode.payload =
				sortedmap.NewBPlusTree(
					vS.maxEntriesPerDirNode,
					sortedmap.CompareString,
					&dirInodeCallbacks{treeNodeLoadable{inode: inMemoryInode}},
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
		} else {
			inMemoryInode.payload, err =
				sortedmap.OldBPlusTree(
					inMemoryInode.PayloadObjectNumber,
					onDiskInodeV1PayloadObjectOffset,
					inMemoryInode.PayloadObjectLength,
					sortedmap.CompareString,
					&dirInodeCallbacks{treeNodeLoadable{inode: inMemoryInode}},
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
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
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
		} else {
			inMemoryInode.payload, err =
				sortedmap.OldBPlusTree(
					inMemoryInode.PayloadObjectNumber,
					onDiskInodeV1PayloadObjectOffset,
					inMemoryInode.PayloadObjectLength,
					sortedmap.CompareUint64,
					&fileInodeCallbacks{treeNodeLoadable{inode: inMemoryInode}},
					nil) // TODO: supply appropriate sortedmap.BPlusTreeCache here
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

func (vS *volumeStruct) fetchInode(inodeNumber InodeNumber) (inode *inMemoryInodeStruct, ok bool, err error) {
	vS.Lock()
	inode, ok = vS.inodeCache[inodeNumber]
	vS.Unlock()

	if ok {
		err = nil
		return
	}

	inode, ok, err = vS.fetchOnDiskInode(inodeNumber)
	if err == nil && ok {
		vS.Lock()
		vS.inodeCache[inodeNumber] = inode
		vS.Unlock()
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
	birthTime := time.Now()

	inMemoryInode = &inMemoryInodeStruct{
		dirty:                    true,
		volume:                   vS,
		openLogSegment:           nil,
		inFlightLogSegmentMap:    make(map[uint64]*inFlightLogSegmentStruct),
		inFlightLogSegmentErrors: make(map[uint64]error),
		onDiskInodeV1Struct: onDiskInodeV1Struct{
			InodeNumber:      InodeNumber(inodeNumber),
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

func (vS *volumeStruct) flushInodes(inodes []*inMemoryInodeStruct) (err error) {
	var (
		checkpointDoneWaitGroup   *sync.WaitGroup
		dirtyInodeNumbers         []uint64
		dirtyInodeRecBytes        []byte
		dirtyInodeRecs            [][]byte
		emptyLogSegments          []uint64
		emptyLogSegmentsThisInode []uint64
		inode                     *inMemoryInodeStruct
		logSegmentNumber          uint64
		logSegmentValidBytes      uint64
		payloadAsBPlusTree        sortedmap.BPlusTree
		payloadObjectLength       uint64
		payloadObjectNumber       uint64
		onDiskInodeV1             *onDiskInodeV1Struct
		onDiskInodeV1Buf          []byte
	)

	// Assemble slice of "dirty" inodes while flushing them
	dirtyInodeNumbers = make([]uint64, 0, len(inodes))
	dirtyInodeRecs = make([][]byte, 0, len(inodes))
	emptyLogSegments = make([]uint64, 0)

	for _, inode = range inodes {
		if FileType == inode.InodeType {
			err = vS.doFileInodeDataFlush(inode)
			if nil != err {
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
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.InodeFlushError)
				return
			}
			if payloadObjectNumber > inode.PayloadObjectNumber {
				if !inode.dirty {
					err = fmt.Errorf("Logic error: inode.dirty should have been true")
					logger.ErrorWithError(err)
					err = blunder.AddError(err, blunder.InodeFlushError)
					return
				}
				inode.PayloadObjectNumber = payloadObjectNumber
				inode.PayloadObjectLength = payloadObjectLength
			}
		}
		if inode.dirty {
			onDiskInodeV1, err = inode.convertToOnDiskInodeV1()
			if nil != err {
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.InodeFlushError)
				return
			}
			onDiskInodeV1Buf, err = json.Marshal(onDiskInodeV1)
			if nil != err {
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
			logger.ErrorWithError(err)
			err = blunder.AddError(err, blunder.InodeFlushError)
			return
		}
		for _, inode = range inodes {
			inode.dirty = false
		}
		checkpointDoneWaitGroup = vS.headhunterVolumeHandle.FetchNextCheckPointDoneWaitGroup()
	} else {
		checkpointDoneWaitGroup = nil
	}

	// Now do phase one of garbage collection
	if 0 < len(emptyLogSegments) {
		for _, logSegmentNumber = range emptyLogSegments {
			err = vS.deleteLogSegmentAsync(logSegmentNumber, checkpointDoneWaitGroup)
			if nil != err {
				logger.WarnfWithError(err, "couldn't delete garbage log segment")
			}
		}
	}

	err = nil
	return
}

func (vS *volumeStruct) flushInodeNumbers(inodeNumbers []InodeNumber) (err error) {
	var (
		inode *inMemoryInodeStruct
		ok    bool
	)
	// Fetch referenced inodes
	inodes := make([]*inMemoryInodeStruct, 0, len(inodeNumbers))
	for _, inodeNumber := range inodeNumbers {
		vS.Lock()
		_, ok = vS.inodeCache[inodeNumber]
		vS.Unlock()
		if ok {
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
				err = fmt.Errorf("%s: fetch of inode %d volume '%s' failed because its unallocated",
					utils.GetFnName(), inodeNumber, vS.volumeName)
				logger.ErrorWithError(err)
				err = blunder.AddError(err, blunder.NotFoundError)
				return
			}

			inodes = append(inodes, inode)
		}
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

	volume.Lock()         // TODO: Once Tracker https://www.pivotaltracker.com/story/show/133377567
	defer volume.Unlock() //       is resolved, these two lines should be removed

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

		err = swiftclient.ContainerPut(vS.accountName, newContainerName, make(map[string][]string))
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

func (vS *volumeStruct) Access(inodeNumber InodeNumber, userID InodeUserID, groupID InodeGroupID, otherGroupIDs []InodeGroupID, accessMode InodeMode) (accessReturn bool) {

	ourInode, ok, err := vS.fetchInode(inodeNumber)
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
		logger.Infof("%s: fetch of inode %d volume '%s' failed because its unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)

		// if the inode is free then we can't access it
		accessReturn = false
		return
	}

	if F_OK == accessMode {
		// the inode exists so its F_OK
		accessReturn = true
		return
	}

	if P_OK == accessMode {
		accessReturn = (InodeRootUserID == userID) || (userID == ourInode.UserID)
		return
	}

	if accessMode != (accessMode & (R_OK | W_OK | X_OK)) {
		// Default to false if P_OK bit set along with any others)
		accessReturn = false
		return
	}

	// On a local file system, the owner of a file can *not* write to the
	// file unless the permission bits say so.  However, NFS relaxes this to
	// allow the owner of a file to write to it because NFS does not have an
	// open state (there's no file descriptor that tracks if the file was
	// opened with write permission).  But I'm not sure that other operations
	// that require write permission, like truncate(2) work the same way.
	if (InodeRootUserID == userID) || (InodeRootGroupID == groupID) {
		accessReturn = true
		return
	}

	if (userID == ourInode.UserID) && (((ourInode.Mode >> 6) & accessMode) == accessMode) {
		accessReturn = true
		return
	}

	groupIDCheck := (groupID == ourInode.GroupID)

	if !groupIDCheck {
		for _, otherGroupID := range otherGroupIDs {
			if InodeRootGroupID == otherGroupID {
				accessReturn = true
				return
			}
			if otherGroupID == ourInode.GroupID {
				groupIDCheck = true
				break
			}
		}
	}

	if groupIDCheck && ((((ourInode.Mode >> 3) & 07) & accessMode) == accessMode) {
		accessReturn = true
		return
	}

	accessReturn = ((((ourInode.Mode >> 0) & 07) & accessMode) == accessMode)

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
	vS.Lock()
	defer vS.Unlock()

	inMemoryInode, ok := vS.inodeCache[inodeNumber]

	if !ok {
		err = nil
		return
	}

	if inMemoryInode.dirty {
		err = fmt.Errorf("Inode dirty... cannot be purged")
		return
	}

	delete(vS.inodeCache, inodeNumber)

	err = nil

	return
}

func (vS *volumeStruct) Destroy(inodeNumber InodeNumber) (err error) {

	logger.Tracef("inode.Destroy(): volume '%s' inode %d", vS.volumeName, inodeNumber)

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
		err = fmt.Errorf("%s: cannot destroy inode %d volume '%s' because its unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		err = blunder.AddError(err, blunder.NotFoundError)
		logger.ErrorWithError(err)
		return
	}

	vS.Lock()
	delete(vS.inodeCache, inodeNumber)
	vS.Unlock()

	if ourInode.InodeType == FileType {
		_ = vS.doFileInodeDataFlush(ourInode)
	}

	err = vS.headhunterVolumeHandle.DeleteInodeRec(uint64(inodeNumber))
	if nil != err {
		logger.ErrorWithError(err)
		return
	}

	if DirType == ourInode.InodeType {
		dirMapping := ourInode.payload.(sortedmap.BPlusTree)

		err = dirMapping.Discard()
		if nil != err {
			logger.ErrorWithError(err)
			return
		}

		stats.IncrementOperations(&stats.DirDestroyOps)
	} else if FileType == ourInode.InodeType {
		extents := ourInode.payload.(sortedmap.BPlusTree)

		err = extents.Discard()
		if nil != err {
			logger.ErrorWithError(err)
			return
		}

		checkpointDoneWaitGroup := vS.headhunterVolumeHandle.FetchNextCheckPointDoneWaitGroup()

		for logSegmentNumber := range ourInode.LogSegmentMap {
			deleteSegmentErr := vS.deleteLogSegmentAsync(logSegmentNumber, checkpointDoneWaitGroup)
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

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return nil, err
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		err = blunder.AddError(err, blunder.NotFoundError)
		logger.InfoWithError(err)
		return nil, err
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

	pos := 0
	for inodeStreamName := range inode.StreamMap {
		metadata.InodeStreamNameSlice[pos] = inodeStreamName
		pos++
	}

	stats.IncrementOperations(&stats.InodeGetMetadataOps)
	return metadata, err
}

func (vS *volumeStruct) GetType(inodeNumber InodeNumber) (inodeType InodeType, err error) {

	inode, ok, err := vS.fetchInode(inodeNumber)
	if nil != err {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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

	inode, ok, err := vS.fetchInode(inodeNumber)
	if nil != err {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	linkCount = inode.LinkCount
	return
}

// SetLinkCount is used to adjust the LinkCount property to match current reference count during FSCK TreeWalk.
func (vS *volumeStruct) SetLinkCount(inodeNumber InodeNumber, linkCount uint64) (err error) {

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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
	// NOTE: Errors are logged by the caller

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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
	// NOTE: Errors are logged by the caller

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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
	// NOTE: Errors are logged by the caller

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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
	// NOTE: Errors are logged by the caller

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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
	// NOTE: Errors are logged by the caller

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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
	// NOTE: Errors are logged by the caller

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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
	// NOTE: Errors are logged by the caller

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// the inode is locked so this should never happen (unless the inode
		// was evicted from the cache and it was corrupt when read from disk)
		logger.ErrorfWithError(err, "%s: fetch of target inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// this should never happen (see above)
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return nil, err
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return err
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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

	inode, ok, err := vS.fetchInode(inodeNumber)
	if err != nil {
		// this indicates disk corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
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

func (vS *volumeStruct) GetFragmentationReport(inodeNumber InodeNumber) (fragmentationReport FragmentationReport, err error) {
	err = fmt.Errorf("GetFragmentationReport not yet implemented")
	return
}

func (vS *volumeStruct) Optimize(inodeNumber InodeNumber, maxDuration time.Duration) (err error) {
	err = fmt.Errorf("Optimize not yet implemented")
	return
}

func validateFileExtents(ourInode *inMemoryInodeStruct) (err error) {
	var (
		zero = uint64(0)
	)

	readPlan, readPlanBytes, err := ourInode.volume.getReadPlanHelper(ourInode, &zero, nil)
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
	// We'll make a map of how many bytes we expect to see in each segment (just
	// like the LogSegmentMap, but with the values just being the file-data count,
	// not the entire LogSegmentRecordStruct, which also tallies payload data).
	remainingExpectedBytes := make(map[uint64]uint64)
	for segmentNumber, segmentBytesUsed := range ourInode.LogSegmentMap {
		remainingExpectedBytes[segmentNumber] += segmentBytesUsed
	}
	// Then we can compare with the actual read plan we got ...
	for _, readPlanStep := range readPlan {
		pathSegments := strings.Split(readPlanStep.ObjectPath, "/")
		logSegmentRepresentation := pathSegments[len(pathSegments)-1]
		logSegmentNumber, hexConvErr := utils.HexStrToUint64(logSegmentRepresentation)
		if hexConvErr != nil {
			return blunder.NewError(blunder.CorruptInodeError, "conversion of read plan object name to log segment number failed?!")
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
			// XXX TODO CONSIDER: it might be helpful to continue and make a
			// combined report of all insufficiently long log segments, rather than
			// erroring out immediately
			err = fmt.Errorf("expected %q to have at least %v bytes, content length was %v", objectPath, endOffset, contentLength)
			logger.ErrorWithError(err)
			return err
		}

	}

	return nil
}

func (vS *volumeStruct) markCorrupted(inodeNumber InodeNumber) (err error) {
	var (
		inodeRec []byte
		ok       bool
	)

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

func (vS *volumeStruct) Validate(inodeNumber InodeNumber) (err error) {
	// we don't want to use the in-memory cache for this; we'll need to fetch
	// the current real-world bits from disk.

	// If this is a file inode, we flush to ensure that the inode is not dirty
	// (and that DLM locking therefore ensures we have exclusive access to the
	// inode and don't need to serialize this operation, as there can be no pending
	// time-based flush to race with).

	err = vS.flushInodeNumber(inodeNumber)
	if nil != err {
		logger.ErrorfWithError(err, "couldn't flush inode %v", inodeNumber)
		return
	}

	err = vS.Purge(inodeNumber)
	if nil != err {
		logger.ErrorfWithError(err, "couldn't purge inode %v", inodeNumber)
		return
	}

	ourInode, ok, err := vS.fetchInode(inodeNumber)
	if nil != err {
		// this indicates diskj corruption or software error
		// (err includes volume name and inode number)
		logger.ErrorfWithError(err, "%s: fetch of inode failed", utils.GetFnName())
		return
	}
	if !ok {
		// disk corruption or client request for unallocated inode
		err = fmt.Errorf("%s: failing request for inode %d volume '%s' because its unallocated",
			utils.GetFnName(), inodeNumber, vS.volumeName)
		logger.InfoWithError(err)
		err = blunder.AddError(err, blunder.NotFoundError)
		return
	}

	switch ourInode.InodeType {
	case DirType, FileType:
		tree, ok := ourInode.payload.(sortedmap.BPlusTree)
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
			err = validateFileExtents(ourInode)
			if nil != err {
				err = blunder.AddError(err, blunder.CorruptInodeError)
				_ = vS.markCorrupted(inodeNumber)
				return
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
