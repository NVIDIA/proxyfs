package fs

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

const (
	validateVolumeBPTreeMaxKeysPerNode = uint64(100)
	validateVolumeBPTreeEvictLowLimit  = uint64(90)
	validateVolumeBPTreeEvictHighLimit = uint64(100)

	validateVolumeInodeParallelism              = uint64(100)
	validateVolumeCalculateLinkCountParallelism = uint64(50)
	validateVolumeFixLinkCountParallelism       = uint64(50)

	validateVolumeContainerRescanDelay = time.Duration(1 * time.Second)

	lostAndFoundDirName = ".Lost+Found"
)

type validateVolumeStruct struct {
	sync.Mutex
	globalWaitGroup            sync.WaitGroup
	volumeName                 string
	active                     bool
	stopFlag                   bool
	err                        []string
	info                       []string
	volume                     *volumeStruct
	inodeVolumeHandle          inode.VolumeHandle
	headhunterVolumeHandle     headhunter.VolumeHandle
	parallelismChan            chan struct{}
	parallelismChanSize        uint64
	childrenWaitGroup          sync.WaitGroup
	bpTree                     sortedmap.BPlusTree
	bpTreeCache                sortedmap.BPlusTreeCache
	bpTreeFile                 *os.File
	bpTreeNextOffset           uint64
	lostAndFoundDirInodeNumber inode.InodeNumber
	accountName                string
	checkpointContainerName    string
}

func (vVS *validateVolumeStruct) Active() (active bool) {
	active = vVS.active
	return
}

func (vVS *validateVolumeStruct) Wait() {
	vVS.globalWaitGroup.Wait()
}

func (vVS *validateVolumeStruct) Cancel() {
	vVS.stopFlag = true
	vVS.Wait()
}

func (vVS *validateVolumeStruct) Error() (err []string) {
	var errString string

	vVS.Lock()

	err = make([]string, 0, len(vVS.err))
	for _, errString = range vVS.err {
		err = append(err, errString)
	}

	vVS.Unlock()

	return
}

func (vVS *validateVolumeStruct) Info() (info []string) {
	var infoString string

	vVS.Lock()

	info = make([]string, 0, len(vVS.info))
	for _, infoString = range vVS.info {
		info = append(info, infoString)
	}

	vVS.Unlock()

	return
}

func (vVS *validateVolumeStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString = fmt.Sprintf("0x%016X", key.(uint64))
	err = nil
	return
}

func (vVS *validateVolumeStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsString = fmt.Sprintf("0x%016X", value.(uint64))
	err = nil
	return
}

func (vVS *validateVolumeStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	vVS.Lock()
	defer vVS.Unlock()
	nodeByteSlice = make([]byte, 0, objectLength)
	_, err = vVS.bpTreeFile.Seek(io.SeekStart, int(objectOffset))
	if nil != err {
		return
	}
	_, err = io.ReadFull(vVS.bpTreeFile, nodeByteSlice)
	return
}

func (vVS *validateVolumeStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	vVS.Lock()
	defer vVS.Unlock()
	_, err = vVS.bpTreeFile.Seek(io.SeekEnd, int(0))
	if nil != err {
		return
	}
	_, err = vVS.bpTreeFile.WriteAt(nodeByteSlice, int64(vVS.bpTreeNextOffset))
	if nil != err {
		return
	}
	objectNumber = 0
	objectOffset = vVS.bpTreeNextOffset
	vVS.bpTreeNextOffset += uint64(len(nodeByteSlice))
	return
}

func (vVS *validateVolumeStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	// Short-lived vVS.bpTreeFile... no compaction/garbage-collection needed
	err = nil
	return
}

func (vVS *validateVolumeStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		u64 uint64
	)

	u64 = key.(uint64)

	packedKey = make([]byte, 8)

	binary.LittleEndian.PutUint64(packedKey, u64)

	err = nil
	return
}

func (vVS *validateVolumeStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("fs.validateVolumeStruct.UnpackKey() called with insufficiently sized payloadData (%v) - should be at least 8", len(payloadData))
		return
	}

	key = binary.LittleEndian.Uint64(payloadData[:8])
	bytesConsumed = 8

	err = nil
	return
}

func (vVS *validateVolumeStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		u64 uint64
	)

	u64 = value.(uint64)

	packedValue = make([]byte, 8)

	binary.LittleEndian.PutUint64(packedValue, u64)

	err = nil
	return
}

func (vVS *validateVolumeStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("fs.validateVolumeStruct.UnpackValue() called with insufficiently sized payloadData (%v) - should be at least 8", len(payloadData))
		return
	}

	value = binary.LittleEndian.Uint64(payloadData[:8])
	bytesConsumed = 8

	err = nil
	return
}

func (vVS *validateVolumeStruct) validateVolumeStartParallelism(parallelismChanSize uint64) {
	var (
		i uint64
	)

	vVS.parallelismChan = make(chan struct{}, parallelismChanSize)
	vVS.parallelismChanSize = parallelismChanSize

	for i = uint64(0); i < parallelismChanSize; i++ {
		vVS.validateVolumeReleaseParallelism()
	}
}

func (vVS *validateVolumeStruct) validateVolumeGrabParallelism() {
	_ = <-vVS.parallelismChan
}

func (vVS *validateVolumeStruct) validateVolumeReleaseParallelism() {
	vVS.parallelismChan <- struct{}{}
}

func (vVS *validateVolumeStruct) validateVolumeEndParallelism() {
	var (
		i uint64
	)

	for i = uint64(0); i < vVS.parallelismChanSize; i++ {
		vVS.validateVolumeGrabParallelism()
	}
}

func (vVS *validateVolumeStruct) validateVolumeLogErr(formatString string, args ...interface{}) {
	vVS.Lock()
	vVS.validateVolumeLogErrWhileLocked(formatString, args...)
	vVS.Unlock()
}

func (vVS *validateVolumeStruct) validateVolumeLogErrWhileLocked(formatString string, args ...interface{}) {
	var (
		suffix string
	)

	suffix = fmt.Sprintf(formatString, args...)

	logger.Errorf("FSCK[%v] %v", vVS.volumeName, suffix)

	vVS.err = append(vVS.err, fmt.Sprintf("%v %v", time.Now().Format(time.RFC3339), suffix))
}

func (vVS *validateVolumeStruct) validateVolumeLogInfo(formatString string, args ...interface{}) {
	vVS.Lock()
	vVS.validateVolumeLogInfoWhileLocked(formatString, args...)
	vVS.Unlock()
}

func (vVS *validateVolumeStruct) validateVolumeLogInfoWhileLocked(formatString string, args ...interface{}) {
	var (
		suffix string
	)

	suffix = fmt.Sprintf(formatString, args...)

	logger.Infof("FSCK[%v] %v", vVS.volumeName, suffix)

	vVS.info = append(vVS.info, fmt.Sprintf("%v %v", time.Now().Format(time.RFC3339), suffix))
}

func (vVS *validateVolumeStruct) validateVolumeInode(inodeNumber uint64) {
	var (
		err error
		ok  bool
	)

	defer vVS.childrenWaitGroup.Done()

	vVS.validateVolumeGrabParallelism()
	defer vVS.validateVolumeReleaseParallelism()

	if vVS.stopFlag {
		return
	}

	// TODO (now): Avoid full ReadPlan vs. LogSegments validation for FileInode's
	err = vVS.inodeVolumeHandle.Validate(inode.InodeNumber(inodeNumber))

	vVS.Lock()
	defer vVS.Unlock()

	if nil == err {
		ok, err = vVS.bpTree.Put(inodeNumber, uint64(0)) // Initial LinkCount == 0
		if nil != err {
			vVS.validateVolumeLogErrWhileLocked("Got bpTree.Put(0x%016X, 0) failure: %v", inodeNumber, err)
			return
		}
		if !ok {
			vVS.validateVolumeLogErrWhileLocked("Got bpTree.Put(0x%016X, 0) !ok", inodeNumber)
			return
		}
	} else {
		vVS.validateVolumeLogInfoWhileLocked("Got inode.Validate(0x%016X) failure: %v ... removing it", inodeNumber, err)

		err = vVS.headhunterVolumeHandle.DeleteInodeRec(inodeNumber)
		if nil != err {
			vVS.validateVolumeLogErrWhileLocked("Got headhunter.DeleteInodeRec(0x%016X) failure: %v", inodeNumber, err)
		}
	}
}

func (vVS *validateVolumeStruct) validateVolumeIncrementCalculatedLinkCountWhileLocked(inodeNumber uint64) (ok bool) {
	var (
		err       error
		linkCount uint64
		value     sortedmap.Value
	)

	value, ok, err = vVS.bpTree.GetByKey(inodeNumber)
	if nil != err {
		vVS.validateVolumeLogErrWhileLocked("Got vVS.bpTree.GetByKey(0x%016X) failure: %v", inodeNumber, err)
		ok = false
		return
	}
	if !ok {
		vVS.validateVolumeLogErrWhileLocked("Got vVS.bpTree.GetByKey(0x%016X) !ok", inodeNumber)
		ok = false
		return
	}
	linkCount = value.(uint64) + 1
	ok, err = vVS.bpTree.PatchByKey(inodeNumber, linkCount)
	if nil != err {
		vVS.validateVolumeLogErrWhileLocked("Got vVS.bpTree.PatchByKey(0x%016X,) failure: %v", inodeNumber, err)
		ok = false
		return
	}
	if !ok {
		vVS.validateVolumeLogErrWhileLocked("Got vVS.bpTree.PatchByKey(0x%016X,) !ok", inodeNumber)
		ok = false
		return
	}

	ok = true

	return
}

func (vVS *validateVolumeStruct) validateVolumeCalculateLinkCount(parentDirInodeNumber uint64, dirInodeNumber uint64) {
	var (
		dirEntrySlice        []inode.DirEntry
		dotDotWasSeen        bool
		dotWasSeen           bool
		err                  error
		inodeNumber          uint64
		inodeType            inode.InodeType
		moreEntries          bool
		ok                   bool
		prevReturnedAsString string
	)

	defer vVS.childrenWaitGroup.Done()

	vVS.validateVolumeGrabParallelism()
	defer vVS.validateVolumeReleaseParallelism()

	prevReturnedAsString = ""

	dotWasSeen = false
	dotDotWasSeen = false

forLabel:
	for {
		if vVS.stopFlag {
			return
		}

		dirEntrySlice, moreEntries, err = vVS.inodeVolumeHandle.ReadDir(inode.InodeNumber(dirInodeNumber), 1, 0, prevReturnedAsString)
		if nil != err {
			vVS.validateVolumeLogErr("Got inode.ReadDir(0x%016X,1,0,\"%v\") failure: %v", dirInodeNumber, prevReturnedAsString, err)
			return
		}

		if 0 == len(dirEntrySlice) {
			break forLabel
		} else if 1 < len(dirEntrySlice) {
			vVS.validateVolumeLogErr("Got too many DirEntry's from inode.ReadDir(0x%016X,1,0,\"%v\")", dirInodeNumber, prevReturnedAsString)
			return
		}

		// Increment LinkCount for dirEntrySlice[0]'s InodeNumber

		inodeNumber = uint64(dirEntrySlice[0].InodeNumber)
		prevReturnedAsString = dirEntrySlice[0].Basename

		vVS.Lock()

		if ("." == prevReturnedAsString) && (inodeNumber != dirInodeNumber) {
			err = vVS.inodeVolumeHandle.Unlink(inode.InodeNumber(dirInodeNumber), ".", true)
			if nil != err {
				vVS.validateVolumeLogErrWhileLocked("Got inode.Unlink(0x%016X,\".\",true) failure: %v", dirInodeNumber, err)
				vVS.Unlock()
				return
			}
			err = vVS.inodeVolumeHandle.Link(inode.InodeNumber(dirInodeNumber), ".", inode.InodeNumber(dirInodeNumber), true)
			if nil != err {
				vVS.validateVolumeLogErrWhileLocked("Got inode.Link(0x%016X,\".\",0x%016X,true) failure: %v", dirInodeNumber, dirInodeNumber, err)
				vVS.Unlock()
				return
			}

			vVS.validateVolumeLogInfoWhileLocked("\".\" dirEntry in dirInodeNumber 0x%016X was 0x%016X...fixed to point to dirInodeNumber", dirInodeNumber, inodeNumber)

			inodeNumber = dirInodeNumber
		} else if (".." == prevReturnedAsString) && (inodeNumber != parentDirInodeNumber) {
			err = vVS.inodeVolumeHandle.Unlink(inode.InodeNumber(dirInodeNumber), "..", true)
			if nil != err {
				vVS.validateVolumeLogErrWhileLocked("Got inode.Unlink(0x%016X,\"..\",true) failure: %v", dirInodeNumber, err)
				vVS.Unlock()
				return
			}
			err = vVS.inodeVolumeHandle.Link(inode.InodeNumber(dirInodeNumber), "..", inode.InodeNumber(parentDirInodeNumber), true)
			if nil != err {
				vVS.validateVolumeLogErrWhileLocked("Got inode.Link(0x%016X,\"..\",0x%016X,true) failure: %v", dirInodeNumber, parentDirInodeNumber, err)
				vVS.Unlock()
				return
			}

			vVS.validateVolumeLogInfoWhileLocked("\"..\" dirEntry in dirInodeNumber 0x%016X was 0x%016X...fixed to point to parentDirInodeNumber (0x%016X)", dirInodeNumber, inodeNumber, parentDirInodeNumber)

			inodeNumber = parentDirInodeNumber
		}

		ok = vVS.validateVolumeIncrementCalculatedLinkCountWhileLocked(inodeNumber)
		if !ok {
			vVS.Unlock()
			return
		}

		vVS.Unlock()

		// Recurse into sub-directories

		if "." == prevReturnedAsString {
			dotWasSeen = true
		} else if ".." == prevReturnedAsString {
			dotDotWasSeen = true
		} else {
			inodeType, err = vVS.inodeVolumeHandle.GetType(inode.InodeNumber(inodeNumber))
			if nil != err {
				vVS.validateVolumeLogErr("Got inode.GetType(0x%016X) failure: %v", inodeNumber, err)
				return
			}

			if inode.DirType == inodeType {
				vVS.childrenWaitGroup.Add(1)
				go vVS.validateVolumeCalculateLinkCount(dirInodeNumber, inodeNumber)
			}
		}

		if !moreEntries {
			break forLabel
		}
	}

	if !dotWasSeen {
		vVS.Lock()
		err = vVS.inodeVolumeHandle.Link(inode.InodeNumber(dirInodeNumber), ".", inode.InodeNumber(dirInodeNumber), true)
		if nil != err {
			vVS.validateVolumeLogErrWhileLocked("Got inode.Link(0x%016X,\".\",0x%016X,true) failure: %v", dirInodeNumber, dirInodeNumber, err)
			vVS.Unlock()
			return
		}
		ok = vVS.validateVolumeIncrementCalculatedLinkCountWhileLocked(dirInodeNumber)
		if !ok {
			vVS.Unlock()
			return
		}
		vVS.validateVolumeLogInfoWhileLocked("\"..\" dirEntry in dirInodeNumber 0x%016X was missing...fixed to point to dirInodeNumber", dirInodeNumber)
		vVS.Unlock()
		return
	}

	if !dotDotWasSeen {
		vVS.Lock()
		err = vVS.inodeVolumeHandle.Link(inode.InodeNumber(dirInodeNumber), "..", inode.InodeNumber(parentDirInodeNumber), true)
		if nil != err {
			vVS.validateVolumeLogErrWhileLocked("Got inode.Link(0x%016X,\"..\",0x%016X,true) failure: %v", dirInodeNumber, parentDirInodeNumber, err)
			vVS.Unlock()
			return
		}
		ok = vVS.validateVolumeIncrementCalculatedLinkCountWhileLocked(parentDirInodeNumber)
		if !ok {
			vVS.Unlock()
			return
		}
		vVS.validateVolumeLogInfoWhileLocked("\"..\" dirEntry in dirInodeNumber 0x%016X was missing...fixed to point to parentDirInodeNumber (0x%016X)", dirInodeNumber, parentDirInodeNumber)
		vVS.Unlock()
		return
	}
}

func (vVS *validateVolumeStruct) validateVolumeFixLinkCount(inodeNumber uint64, linkCountComputed uint64) {
	var (
		err              error
		linkCountInInode uint64
	)

	defer vVS.childrenWaitGroup.Done()

	vVS.validateVolumeGrabParallelism()
	defer vVS.validateVolumeReleaseParallelism()

	if vVS.stopFlag {
		return
	}

	linkCountInInode, err = vVS.inodeVolumeHandle.GetLinkCount(inode.InodeNumber(inodeNumber))
	if nil != err {
		vVS.validateVolumeLogErr("Got inode.GetLinkCount(0x%016X) failure: %v", inodeNumber, err)
		return
	}

	if linkCountComputed != linkCountInInode {
		err = vVS.inodeVolumeHandle.SetLinkCount(inode.InodeNumber(inodeNumber), linkCountComputed)
		if nil == err {
			vVS.validateVolumeLogInfo("Corrected LinkCount in Inode# 0x%016X from 0x%016X to 0x%016X", inodeNumber, linkCountInInode, linkCountComputed)
		} else {
			vVS.validateVolumeLogErr("Got inode.SetLinkCount(0x%016X,) failure: %v", inodeNumber, err)
			return
		}
	}
}

func (vVS *validateVolumeStruct) validateVolume() {
	var (
		checkpointContainerObjectList   []string
		checkpointContainerObjectName   string
		checkpointContainerObjectNumber uint64
		checkpointContainerScanning     bool
		err                             error
		headhunterLayoutReport          sortedmap.LayoutReport
		inodeCount                      int
		inodeIndex                      uint64
		inodeNumber                     uint64
		inodeType                       inode.InodeType
		key                             sortedmap.Key
		linkCountComputed               uint64
		moreEntries                     bool
		ok                              bool
		validObjectNameRE               = regexp.MustCompile("\\A[0-9a-fA-F]+\\z")
		value                           sortedmap.Value
	)

	vVS.validateVolumeLogInfo("FSCK job initiated")

	defer func(vVS *validateVolumeStruct) {
		if vVS.stopFlag {
			vVS.validateVolumeLogInfo("FSCK job stopped")
		} else if 0 == len(vVS.err) {
			vVS.validateVolumeLogInfo("FSCK job completed without error")
		} else if 1 == len(vVS.err) {
			vVS.validateVolumeLogInfo("FSCK job exited with one error")
		} else {
			vVS.validateVolumeLogInfo("FSCK job exited with errors")
		}
	}(vVS)

	defer func(vVS *validateVolumeStruct) {
		vVS.active = false
	}(vVS)

	defer vVS.globalWaitGroup.Done()

	// Find specified volume

	globals.Lock()

	vVS.volume, ok = globals.volumeMap[vVS.volumeName]
	if !ok {
		globals.Unlock()
		vVS.validateVolumeLogErr("Couldn't find fs.volumeStruct")
		return
	}

	globals.Unlock()

	vVS.inodeVolumeHandle, err = inode.FetchVolumeHandle(vVS.volumeName)
	if nil != err {
		vVS.validateVolumeLogErr("Couldn't find inode.VolumeHandle")
		return
	}

	vVS.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(vVS.volumeName)
	if nil != err {
		vVS.validateVolumeLogErr("Couldn't find headhunter.VolumeHandle")
		return
	}

	vVS.volume.validateVolumeRWMutex.Lock()
	defer vVS.volume.validateVolumeRWMutex.Unlock()

	// Flush all File Inodes currently in flight

	vVS.volume.untrackInFlightFileInodeDataAll()

	vVS.validateVolumeLogInfo("Completed flush of all inflight File Inode write traffic")

	// Do a checkpoint before actual FSCK work

	err = vVS.headhunterVolumeHandle.DoCheckpoint()
	if nil != err {
		vVS.validateVolumeLogErr("Got headhunter.DoCheckpoint failure: %v", err)
		return
	}

	vVS.validateVolumeLogInfo("Completed checkpoint prior to FSCK work")

	// Setup B+Tree to hold arbitrarily sized map[uint64]uint64 (i.e. beyond what will fit in memoory)

	vVS.bpTreeFile, err = ioutil.TempFile("", "ProxyFS_VaidateVolume_")
	if nil != err {
		vVS.validateVolumeLogErr("Got ioutil.TempFile() failure: %v", err)
		return
	}
	defer func(vVS *validateVolumeStruct) {
		var (
			bpTreeFileName string
		)

		bpTreeFileName = vVS.bpTreeFile.Name()
		_ = vVS.bpTreeFile.Close()
		_ = os.Remove(bpTreeFileName)
	}(vVS)

	vVS.bpTreeNextOffset = 0

	vVS.bpTreeCache = sortedmap.NewBPlusTreeCache(validateVolumeBPTreeEvictLowLimit, validateVolumeBPTreeEvictHighLimit)

	vVS.bpTree = sortedmap.NewBPlusTree(validateVolumeBPTreeMaxKeysPerNode, sortedmap.CompareUint64, vVS, vVS.bpTreeCache)

	// Validate all Inodes in InodeRec table in headhunter

	vVS.Lock() // Hold off vVS.validateVolumeInode() goroutines throughout loop

	vVS.validateVolumeStartParallelism(validateVolumeInodeParallelism)

	inodeIndex = 0

	for {
		if vVS.stopFlag {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.validateVolumeEndParallelism()
			return
		}

		inodeNumber, ok, err = vVS.headhunterVolumeHandle.IndexedInodeNumber(inodeIndex)
		if nil != err {
			vVS.validateVolumeLogErrWhileLocked("Got headhunter.IndexedInodeNumber(0x%016X) failure: %v", inodeIndex, err)
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.validateVolumeEndParallelism()
			return
		}
		if !ok {
			break
		}

		vVS.childrenWaitGroup.Add(1)
		go vVS.validateVolumeInode(inodeNumber) // Will be blocked until subsequent vVS.Unlock()

		inodeIndex++
	}

	vVS.Unlock()

	vVS.childrenWaitGroup.Wait()

	vVS.validateVolumeEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.validateVolumeLogInfo("Completed validation of all Inode's")

	// TreeWalk computing LinkCounts for all inodeNumbers

	_, ok, err = vVS.bpTree.GetByKey(uint64(inode.RootDirInodeNumber))
	if nil != err {
		vVS.validateVolumeLogErr("Got bpTree.GetByKey(RootDirInodeNumber) failure: %v", err)
		return
	}
	if !ok {
		vVS.validateVolumeLogErr("Got bpTree.GetByKey(RootDirInodeNumber) !ok")
		return
	}

	vVS.validateVolumeStartParallelism(validateVolumeCalculateLinkCountParallelism)

	vVS.childrenWaitGroup.Add(1)
	go vVS.validateVolumeCalculateLinkCount(uint64(inode.RootDirInodeNumber), uint64(inode.RootDirInodeNumber))

	vVS.childrenWaitGroup.Wait()

	vVS.validateVolumeEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.validateVolumeLogInfo("Completed treewalk before populating /%v/", lostAndFoundDirName)

	// Establish that lostAndFoundDirName exists

	vVS.lostAndFoundDirInodeNumber, err = vVS.inodeVolumeHandle.Lookup(inode.RootDirInodeNumber, lostAndFoundDirName)
	if nil == err {
		// Found it - make sure it is a directory

		inodeType, err = vVS.inodeVolumeHandle.GetType(vVS.lostAndFoundDirInodeNumber)
		if nil != err {
			vVS.validateVolumeLogErr("Got inode.GetType(vVS.lostAndFoundDirNumber==0x%016X) failure: %v", vVS.lostAndFoundDirInodeNumber, err)
			return
		}
		if inode.DirType != inodeType {
			vVS.validateVolumeLogErr("Got inode.GetType(vVS.lostAndFoundDirNumber==0x%016X) non-DirType", vVS.lostAndFoundDirInodeNumber)
			return
		}

		vVS.validateVolumeLogInfo("Found pre-existing /%v/", lostAndFoundDirName)
	} else {
		if blunder.Is(err, blunder.NotFoundError) {
			// Create it

			vVS.lostAndFoundDirInodeNumber, err = vVS.inodeVolumeHandle.CreateDir(inode.PosixModePerm, 0, 0)
			if nil != err {
				vVS.validateVolumeLogErr("Got inode.CreateDir() failure: %v", err)
				return
			}
			err = vVS.inodeVolumeHandle.Link(inode.RootDirInodeNumber, lostAndFoundDirName, vVS.lostAndFoundDirInodeNumber, false)
			if nil != err {
				vVS.validateVolumeLogErr("Got inode.Link(inode.RootDirInodeNumber, lostAndFoundDirName, vVS.lostAndFoundDirInodeNumber, false) failure: %v", err)
				err = vVS.inodeVolumeHandle.Destroy(vVS.lostAndFoundDirInodeNumber)
				if nil != err {
					vVS.validateVolumeLogErr("Got inode.Destroy(vVS.lostAndFoundDirInodeNumber) failure: %v", err)
				}
				return
			}

			ok, err = vVS.bpTree.Put(uint64(vVS.lostAndFoundDirInodeNumber), uint64(2)) // /<lostAndFoundDirName> as well as /<lostAndFoundDirName>/.
			if nil != err {
				vVS.validateVolumeLogErr("Got bpTree.Put(vVS.lostAndFoundDirInodeNumber, 1) failure: %v", err)
				vVS.Unlock()
				return
			}
			if !ok {
				vVS.validateVolumeLogErr("Got bpTree.Put(vVS.lostAndFoundDirInodeNumber, 1) !ok")
				vVS.Unlock()
				return
			}

			vVS.validateVolumeLogInfo("Created /%v/", lostAndFoundDirName)
		} else {
			vVS.validateVolumeLogErr("Got inode.Lookup(inode.RootDirInodeNumber, lostAndFoundDirName) failure: %v", err)
			return
		}
	}

	// TODO (someday): Scan B+Tree placing top-most orphan DirInodes as elements of vVS.lostAndFoundDirInodeNumber

	// Re-compute LinkCounts

	inodeCount, err = vVS.bpTree.Len()
	if nil != err {
		vVS.validateVolumeLogErr("Got inode.Len() failure: %v", err)
		return
	}

	for inodeIndex = uint64(0); inodeIndex < uint64(inodeCount); inodeIndex++ {
		if vVS.stopFlag {
			return
		}

		ok, err = vVS.bpTree.PatchByIndex(int(inodeIndex), uint64(0))
		if nil != err {
			vVS.validateVolumeLogErr("Got vVS.bpTree.PatchByIndex(0x%016X, 0) failure: %v", inodeIndex, err)
			return
		}
		if !ok {
			vVS.validateVolumeLogErr("Got vVS.bpTree.PatchByIndex(0x%016X, 0) !ok", inodeIndex)
			return
		}
	}

	vVS.validateVolumeStartParallelism(validateVolumeCalculateLinkCountParallelism)

	vVS.childrenWaitGroup.Add(1)
	go vVS.validateVolumeCalculateLinkCount(uint64(inode.RootDirInodeNumber), uint64(inode.RootDirInodeNumber))

	vVS.childrenWaitGroup.Wait()

	vVS.validateVolumeEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.validateVolumeLogInfo("Completed treewalk after populating /%v/", lostAndFoundDirName)

	// TODO (someday): Scan B+Tree placing orphaned non-DirInodes as elements of vVS.lostAndFoundDirInodeNumber

	// Update incorrect LinkCounts

	vVS.Lock() // Hold off vVS.validateVolumeFixLinkCount() goroutines throughout loop

	vVS.validateVolumeStartParallelism(validateVolumeFixLinkCountParallelism)

	for inodeIndex = uint64(0); inodeIndex < uint64(inodeCount); inodeIndex++ {
		if vVS.stopFlag {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.validateVolumeEndParallelism()
			return
		}

		key, value, ok, err = vVS.bpTree.GetByIndex(int(inodeIndex))
		if nil != err {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.validateVolumeEndParallelism()
			vVS.validateVolumeLogErr("Got vVS.bpTree.GetByIndex(0x%016X) failure: %v", inodeIndex, err)
			return
		}
		if !ok {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.validateVolumeEndParallelism()
			vVS.validateVolumeLogErr("Got vVS.bpTree.GetByIndex(0x%016X) !ok", inodeIndex)
			return
		}

		inodeNumber = key.(uint64)
		linkCountComputed = value.(uint64)

		vVS.childrenWaitGroup.Add(1)
		go vVS.validateVolumeFixLinkCount(inodeNumber, linkCountComputed) // Will be blocked until subsequent vVS.Unlock()
	}

	vVS.Unlock()

	vVS.childrenWaitGroup.Wait()

	vVS.validateVolumeEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.validateVolumeLogInfo("Completed LinkCount fix-up of all Inode's")

	// If vVS.lostAndFoundDirInodeNumber is empty, remove it

	_, moreEntries, err = vVS.inodeVolumeHandle.ReadDir(vVS.lostAndFoundDirInodeNumber, 2, 0)
	if nil != err {
		vVS.validateVolumeLogErr("Got ReadDir(vVS.lostAndFoundDirInodeNumber, 2, 0) failure: %v", err)
		return
	}

	if moreEntries {
		vVS.validateVolumeLogInfo("Preserving non-empty /%v/", lostAndFoundDirName)
	} else {
		err = vVS.inodeVolumeHandle.Unlink(inode.RootDirInodeNumber, lostAndFoundDirName, false)
		if nil != err {
			vVS.validateVolumeLogErr("Got inode.Unlink(inode.RootDirInodeNumber, lostAndFoundDirName, false) failure: %v", err)
			return
		}

		err = vVS.inodeVolumeHandle.Destroy(vVS.lostAndFoundDirInodeNumber)
		if nil != err {
			vVS.validateVolumeLogErr("Got inode.Destroy(vVS.lostAndFoundDirInodeNumber) failure: %v", err)
			return
		}

		vVS.validateVolumeLogInfo("Removed empty /%v/", lostAndFoundDirName)
	}

	// TODO (now): Walk all DirInodes & FileInodes tracking referenced headhunter B+Tree "Objects"
	// TODO (now): Delete unreferenced headhunter B+Tree "Objects"

	// TODO (someday): Walk all FileInodes tracking referenced LogSegments
	// TODO (someday): Remove non-Checkpoint Objects not in headhunter's LogSegment B+Tree
	// TODO (someday): Delete unreferenced LogSegments (both headhunter records & Objects)

	// Do a final checkpoint

	err = vVS.headhunterVolumeHandle.DoCheckpoint()
	if nil != err {
		vVS.validateVolumeLogErr("Got headhunter.DoCheckpoint failure: %v", err)
		return
	}

	vVS.validateVolumeLogInfo("Completed checkpoint after FSCK work")

	// Validate headhunter checkpoint container contents

	headhunterLayoutReport, err = vVS.headhunterVolumeHandle.FetchLayoutReport(headhunter.MergedBPlusTree)
	if nil != err {
		vVS.validateVolumeLogErr("Got headhunter.FetchLayoutReport() failure: %v", err)
		return
	}

	vVS.accountName, vVS.checkpointContainerName = vVS.headhunterVolumeHandle.FetchAccountAndCheckpointContainerNames()

	checkpointContainerScanning = true

	for checkpointContainerScanning {
		checkpointContainerScanning = false

		_, checkpointContainerObjectList, err = swiftclient.ContainerGet(vVS.accountName, vVS.checkpointContainerName)
		if nil != err {
			vVS.validateVolumeLogErr("Got swiftclient.ContainerGet(\"%v\",\"%v\") failure: %v", vVS.accountName, vVS.checkpointContainerName, err)
			return
		}

		for _, checkpointContainerObjectName = range checkpointContainerObjectList {
			checkpointContainerObjectNumber = uint64(0) // If remains 0 or results in returning to 0,
			//                                             checkpointContainerObjectName should be deleted

			if 16 == len(checkpointContainerObjectName) {
				if validObjectNameRE.MatchString(checkpointContainerObjectName) {
					checkpointContainerObjectNumber, err = strconv.ParseUint(checkpointContainerObjectName, 16, 64)
					if nil != err {
						vVS.validateVolumeLogErr("Got strconv.ParseUint(\"%v\",16,64) failure: %v", checkpointContainerObjectName)
						return
					}

					_, ok = headhunterLayoutReport[checkpointContainerObjectNumber]
					if !ok {
						checkpointContainerObjectNumber = uint64(0)
					}
				}
			}

			if uint64(0) == checkpointContainerObjectNumber {
				err = swiftclient.ObjectDeleteSync(vVS.accountName, vVS.checkpointContainerName, checkpointContainerObjectName)
				if nil != err {
					vVS.validateVolumeLogErr("Got swiftclient.ObjectDeleteSync(\"%v\",\"%v\",\"%v\") failure: %v", vVS.accountName, vVS.checkpointContainerName, checkpointContainerObjectName, err)
					return
				}

				checkpointContainerScanning = true // Continue looping until no new objects are found to delete
				vVS.validateVolumeLogInfo("Removed unreferenced checkpointContainerObject %v", checkpointContainerObjectName)
			}
		}

		if vVS.stopFlag {
			return
		}

		if checkpointContainerScanning {
			time.Sleep(validateVolumeContainerRescanDelay)
		}
	}
}
