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
	jobBPTreeMaxKeysPerNode = uint64(100)
	jobBPTreeEvictLowLimit  = uint64(90)
	jobBPTreeEvictHighLimit = uint64(100)

	validateVolumeInodeParallelism               = uint64(100)
	validateVolumeCalculateLinkCountParallelism  = uint64(50)
	validateVolumeFixLinkCountParallelism        = uint64(50)
	validateVolumeIncrementByteCountsParallelism = uint64(100)

	scrubVolumeInodeParallelism = uint64(100)

	lostAndFoundDirName = ".Lost+Found"
)

type jobStruct struct {
	sync.Mutex
	globalWaitGroup        sync.WaitGroup
	jobType                string
	volumeName             string
	active                 bool
	stopFlag               bool
	err                    []string
	info                   []string
	volume                 *volumeStruct
	bpTreeCache            sortedmap.BPlusTreeCache
	bpTreeFile             *os.File
	bpTreeFileNextOffset   uint64
	parallelismChan        chan struct{}
	parallelismChanSize    uint64
	inodeVolumeHandle      inode.VolumeHandle
	headhunterVolumeHandle headhunter.VolumeHandle
}

type validateVolumeStruct struct {
	jobStruct
	childrenWaitGroup          sync.WaitGroup
	inodeBPTree                sortedmap.BPlusTree // Maps Inode# to LinkCount
	objectBPTree               sortedmap.BPlusTree // Maps Object# to ByteCount
	lostAndFoundDirInodeNumber inode.InodeNumber
	accountName                string
	checkpointContainerName    string
}

type scrubVolumeStruct struct {
	jobStruct
}

func (jS *jobStruct) Active() (active bool) {
	active = jS.active
	return
}

func (jS *jobStruct) Wait() {
	jS.globalWaitGroup.Wait()
}

func (jS *jobStruct) Cancel() {
	jS.stopFlag = true
	jS.Wait()
}

func (jS *jobStruct) Error() (err []string) {
	var errString string

	jS.Lock()

	err = make([]string, 0, len(jS.err))
	for _, errString = range jS.err {
		err = append(err, errString)
	}

	jS.Unlock()

	return
}

func (jS *jobStruct) Info() (info []string) {
	var infoString string

	jS.Lock()

	info = make([]string, 0, len(jS.info))
	for _, infoString = range jS.info {
		info = append(info, infoString)
	}

	jS.Unlock()

	return
}

func (jS *jobStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString = fmt.Sprintf("0x%016X", key.(uint64))
	err = nil
	return
}

func (jS *jobStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsString = fmt.Sprintf("0x%016X", value.(uint64))
	err = nil
	return
}

func (jS *jobStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	jS.Lock()
	defer jS.Unlock()
	nodeByteSlice = make([]byte, 0, objectLength)
	_, err = jS.bpTreeFile.Seek(io.SeekStart, int(objectOffset))
	if nil != err {
		return
	}
	_, err = io.ReadFull(jS.bpTreeFile, nodeByteSlice)
	return
}

func (jS *jobStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	jS.Lock()
	defer jS.Unlock()
	_, err = jS.bpTreeFile.Seek(io.SeekEnd, int(0))
	if nil != err {
		return
	}
	_, err = jS.bpTreeFile.WriteAt(nodeByteSlice, int64(jS.bpTreeFileNextOffset))
	if nil != err {
		return
	}
	objectNumber = 0
	objectOffset = jS.bpTreeFileNextOffset
	jS.bpTreeFileNextOffset += uint64(len(nodeByteSlice))
	return
}

func (jS *jobStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	// Short-lived vVS.bpTreeFile... no compaction/garbage-collection needed
	err = nil
	return
}

func (jS *jobStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		u64 uint64
	)

	u64 = key.(uint64)

	packedKey = make([]byte, 8)

	binary.LittleEndian.PutUint64(packedKey, u64)

	err = nil
	return
}

func (jS *jobStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("fs.validateVolumeStruct.UnpackKey() called with insufficiently sized payloadData (%v) - should be at least 8", len(payloadData))
		return
	}

	key = binary.LittleEndian.Uint64(payloadData[:8])
	bytesConsumed = 8

	err = nil
	return
}

func (jS *jobStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		u64 uint64
	)

	u64 = value.(uint64)

	packedValue = make([]byte, 8)

	binary.LittleEndian.PutUint64(packedValue, u64)

	err = nil
	return
}

func (jS *jobStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	if 8 > len(payloadData) {
		err = fmt.Errorf("fs.validateVolumeStruct.UnpackValue() called with insufficiently sized payloadData (%v) - should be at least 8", len(payloadData))
		return
	}

	value = binary.LittleEndian.Uint64(payloadData[:8])
	bytesConsumed = 8

	err = nil
	return
}

func (jS *jobStruct) jobStartParallelism(parallelismChanSize uint64) {
	var (
		i uint64
	)

	jS.parallelismChan = make(chan struct{}, parallelismChanSize)
	jS.parallelismChanSize = parallelismChanSize

	for i = uint64(0); i < parallelismChanSize; i++ {
		jS.jobReleaseParallelism()
	}
}

func (jS *jobStruct) jobGrabParallelism() {
	_ = <-jS.parallelismChan
}

func (jS *jobStruct) jobReleaseParallelism() {
	jS.parallelismChan <- struct{}{}
}

func (jS *jobStruct) jobEndParallelism() {
	var (
		i uint64
	)

	for i = uint64(0); i < jS.parallelismChanSize; i++ {
		jS.jobGrabParallelism()
	}
}

func (jS *jobStruct) jobLogErr(formatString string, args ...interface{}) {
	jS.Lock()
	jS.jobLogErrWhileLocked(formatString, args...)
	jS.Unlock()
}

func (jS *jobStruct) jobLogErrWhileLocked(formatString string, args ...interface{}) {
	var (
		suffix string
	)

	suffix = fmt.Sprintf(formatString, args...)

	logger.Errorf("SCRUB[%v] %v", jS.volumeName, suffix)

	jS.err = append(jS.err, fmt.Sprintf("%v %v", time.Now().Format(time.RFC3339), suffix))
}

func (jS *jobStruct) jobLogInfo(formatString string, args ...interface{}) {
	jS.Lock()
	jS.jobLogInfoWhileLocked(formatString, args...)
	jS.Unlock()
}

func (jS *jobStruct) jobLogInfoWhileLocked(formatString string, args ...interface{}) {
	var (
		suffix string
	)

	suffix = fmt.Sprintf(formatString, args...)

	logger.Infof("SCRUB[%v] %v", jS.volumeName, suffix)

	jS.info = append(jS.info, fmt.Sprintf("%v %v", time.Now().Format(time.RFC3339), suffix))
}

func (vVS *validateVolumeStruct) validateVolumeInode(inodeNumber uint64) {
	var (
		err error
		ok  bool
	)

	defer vVS.childrenWaitGroup.Done()

	vVS.jobGrabParallelism()
	defer vVS.jobReleaseParallelism()

	if vVS.stopFlag {
		return
	}

	err = vVS.inodeVolumeHandle.Validate(inode.InodeNumber(inodeNumber), false)

	vVS.Lock()
	defer vVS.Unlock()

	if nil == err {
		ok, err = vVS.inodeBPTree.Put(inodeNumber, uint64(0)) // Initial LinkCount == 0
		if nil != err {
			vVS.jobLogErrWhileLocked("Got vVS.inodeBPTree.Put(0x%016X, 0) failure: %v", inodeNumber, err)
			return
		}
		if !ok {
			vVS.jobLogErrWhileLocked("Got vVS.inodeBPTree.Put(0x%016X, 0) !ok", inodeNumber)
			return
		}
	} else {
		vVS.jobLogInfoWhileLocked("Got inode.Validate(0x%016X) failure: %v ... removing it", inodeNumber, err)

		err = vVS.headhunterVolumeHandle.DeleteInodeRec(inodeNumber)
		if nil != err {
			vVS.jobLogErrWhileLocked("Got headhunter.DeleteInodeRec(0x%016X) failure: %v", inodeNumber, err)
		}
	}
}

func (vVS *validateVolumeStruct) validateVolumeIncrementCalculatedLinkCountWhileLocked(inodeNumber uint64) (ok bool) {
	var (
		err       error
		linkCount uint64
		value     sortedmap.Value
	)

	value, ok, err = vVS.inodeBPTree.GetByKey(inodeNumber)
	if nil != err {
		vVS.jobLogErrWhileLocked("Got vVS.inodeBPTree.GetByKey(0x%016X) failure: %v", inodeNumber, err)
		ok = false
		return
	}
	if !ok {
		vVS.jobLogErrWhileLocked("Got vVS.inodeBPTree.GetByKey(0x%016X) !ok", inodeNumber)
		ok = false
		return
	}
	linkCount = value.(uint64) + 1
	ok, err = vVS.inodeBPTree.PatchByKey(inodeNumber, linkCount)
	if nil != err {
		vVS.jobLogErrWhileLocked("Got vVS.inodeBPTree.PatchByKey(0x%016X,) failure: %v", inodeNumber, err)
		ok = false
		return
	}
	if !ok {
		vVS.jobLogErrWhileLocked("Got vVS.inodeBPTree.PatchByKey(0x%016X,) !ok", inodeNumber)
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

	vVS.jobGrabParallelism()
	defer vVS.jobReleaseParallelism()

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
			vVS.jobLogErr("Got inode.ReadDir(0x%016X,1,0,\"%v\") failure: %v", dirInodeNumber, prevReturnedAsString, err)
			return
		}

		if 0 == len(dirEntrySlice) {
			break forLabel
		} else if 1 < len(dirEntrySlice) {
			vVS.jobLogErr("Got too many DirEntry's from inode.ReadDir(0x%016X,1,0,\"%v\")", dirInodeNumber, prevReturnedAsString)
			return
		}

		// Increment LinkCount for dirEntrySlice[0]'s InodeNumber

		inodeNumber = uint64(dirEntrySlice[0].InodeNumber)
		prevReturnedAsString = dirEntrySlice[0].Basename

		vVS.Lock()

		if ("." == prevReturnedAsString) && (inodeNumber != dirInodeNumber) {
			err = vVS.inodeVolumeHandle.Unlink(inode.InodeNumber(dirInodeNumber), ".", true)
			if nil != err {
				vVS.jobLogErrWhileLocked("Got inode.Unlink(0x%016X,\".\",true) failure: %v", dirInodeNumber, err)
				vVS.Unlock()
				return
			}
			err = vVS.inodeVolumeHandle.Link(inode.InodeNumber(dirInodeNumber), ".", inode.InodeNumber(dirInodeNumber), true)
			if nil != err {
				vVS.jobLogErrWhileLocked("Got inode.Link(0x%016X,\".\",0x%016X,true) failure: %v", dirInodeNumber, dirInodeNumber, err)
				vVS.Unlock()
				return
			}

			vVS.jobLogInfoWhileLocked("\".\" dirEntry in dirInodeNumber 0x%016X was 0x%016X...fixed to point to dirInodeNumber", dirInodeNumber, inodeNumber)

			inodeNumber = dirInodeNumber
		} else if (".." == prevReturnedAsString) && (inodeNumber != parentDirInodeNumber) {
			err = vVS.inodeVolumeHandle.Unlink(inode.InodeNumber(dirInodeNumber), "..", true)
			if nil != err {
				vVS.jobLogErrWhileLocked("Got inode.Unlink(0x%016X,\"..\",true) failure: %v", dirInodeNumber, err)
				vVS.Unlock()
				return
			}
			err = vVS.inodeVolumeHandle.Link(inode.InodeNumber(dirInodeNumber), "..", inode.InodeNumber(parentDirInodeNumber), true)
			if nil != err {
				vVS.jobLogErrWhileLocked("Got inode.Link(0x%016X,\"..\",0x%016X,true) failure: %v", dirInodeNumber, parentDirInodeNumber, err)
				vVS.Unlock()
				return
			}

			vVS.jobLogInfoWhileLocked("\"..\" dirEntry in dirInodeNumber 0x%016X was 0x%016X...fixed to point to parentDirInodeNumber (0x%016X)", dirInodeNumber, inodeNumber, parentDirInodeNumber)

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
				vVS.jobLogErr("Got inode.GetType(0x%016X) failure: %v", inodeNumber, err)
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
			vVS.jobLogErrWhileLocked("Got inode.Link(0x%016X,\".\",0x%016X,true) failure: %v", dirInodeNumber, dirInodeNumber, err)
			vVS.Unlock()
			return
		}
		ok = vVS.validateVolumeIncrementCalculatedLinkCountWhileLocked(dirInodeNumber)
		if !ok {
			vVS.Unlock()
			return
		}
		vVS.jobLogInfoWhileLocked("\"..\" dirEntry in dirInodeNumber 0x%016X was missing...fixed to point to dirInodeNumber", dirInodeNumber)
		vVS.Unlock()
		return
	}

	if !dotDotWasSeen {
		vVS.Lock()
		err = vVS.inodeVolumeHandle.Link(inode.InodeNumber(dirInodeNumber), "..", inode.InodeNumber(parentDirInodeNumber), true)
		if nil != err {
			vVS.jobLogErrWhileLocked("Got inode.Link(0x%016X,\"..\",0x%016X,true) failure: %v", dirInodeNumber, parentDirInodeNumber, err)
			vVS.Unlock()
			return
		}
		ok = vVS.validateVolumeIncrementCalculatedLinkCountWhileLocked(parentDirInodeNumber)
		if !ok {
			vVS.Unlock()
			return
		}
		vVS.jobLogInfoWhileLocked("\"..\" dirEntry in dirInodeNumber 0x%016X was missing...fixed to point to parentDirInodeNumber (0x%016X)", dirInodeNumber, parentDirInodeNumber)
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

	vVS.jobGrabParallelism()
	defer vVS.jobReleaseParallelism()

	if vVS.stopFlag {
		return
	}

	linkCountInInode, err = vVS.inodeVolumeHandle.GetLinkCount(inode.InodeNumber(inodeNumber))
	if nil != err {
		vVS.jobLogErr("Got inode.GetLinkCount(0x%016X) failure: %v", inodeNumber, err)
		return
	}

	if linkCountComputed != linkCountInInode {
		err = vVS.inodeVolumeHandle.SetLinkCount(inode.InodeNumber(inodeNumber), linkCountComputed)
		if nil == err {
			vVS.jobLogInfo("Corrected LinkCount in Inode# 0x%016X from 0x%016X to 0x%016X", inodeNumber, linkCountInInode, linkCountComputed)
		} else {
			vVS.jobLogErr("Got inode.SetLinkCount(0x%016X,) failure: %v", inodeNumber, err)
			return
		}
	}
}

func (vVS *validateVolumeStruct) validateVolumeIncrementByteCounts(inodeNumber uint64) {
	var (
		err          error
		layoutReport sortedmap.LayoutReport
		objectBytes  uint64
		objectNumber uint64
		ok           bool
		value        sortedmap.Value
	)

	defer vVS.childrenWaitGroup.Done()

	vVS.jobGrabParallelism()
	defer vVS.jobReleaseParallelism()

	if vVS.stopFlag {
		return
	}

	layoutReport, err = vVS.inodeVolumeHandle.FetchLayoutReport(inode.InodeNumber(inodeNumber))
	if nil != err {
		vVS.jobLogErr("Got vVS.inodeVolumeHandle.FetchLayoutReport(0x%016X) failure: %v", inodeNumber, err)
		return
	}

	vVS.Lock()
	defer vVS.Unlock()

	for objectNumber, objectBytes = range layoutReport {
		value, ok, err = vVS.objectBPTree.GetByKey(objectNumber)
		if nil != err {
			vVS.jobLogErrWhileLocked("Got vVS.objectBPTree.GetByKey() failure: %v", err)
			return
		}

		if ok {
			objectBytes += value.(uint64)

			ok, err = vVS.objectBPTree.PatchByKey(objectNumber, objectBytes)
			if nil != err {
				vVS.jobLogErrWhileLocked("Got vVS.objectBPTree.PatchByKey() failure: %v", err)
				return
			}
			if !ok {
				vVS.jobLogErrWhileLocked("Got vVS.objectBPTree.PatchByKey(0) !ok")
				return
			}
		} else {
			ok, err = vVS.objectBPTree.Put(objectNumber, objectBytes)
			if nil != err {
				vVS.jobLogErrWhileLocked("Got vVS.objectBPTree.Put() failure: %v", err)
				return
			}
			if !ok {
				vVS.jobLogErrWhileLocked("Got vVS.objectBPTree.Put() !ok")
				return
			}
		}
	}
}

func (vVS *validateVolumeStruct) validateVolume() {
	var (
		asyncDeleteWaitGroup            sync.WaitGroup
		checkpointContainerObjectList   []string
		checkpointContainerObjectName   string
		checkpointContainerObjectNumber uint64
		err                             error
		headhunterLayoutReport          sortedmap.LayoutReport
		inodeCount                      int
		inodeIndex                      uint64
		inodeNumber                     uint64
		inodeType                       inode.InodeType
		key                             sortedmap.Key
		linkCountComputed               uint64
		moreEntries                     bool
		objectIndex                     uint64
		objectNumber                    uint64
		ok                              bool
		validObjectNameRE               = regexp.MustCompile("\\A[0-9a-fA-F]+\\z")
		value                           sortedmap.Value
	)

	vVS.jobLogInfo("FSCK job initiated")

	defer func(vVS *validateVolumeStruct) {
		if vVS.stopFlag {
			vVS.jobLogInfo("FSCK job stopped")
		} else if 0 == len(vVS.err) {
			vVS.jobLogInfo("FSCK job completed without error")
		} else if 1 == len(vVS.err) {
			vVS.jobLogInfo("FSCK job exited with one error")
		} else {
			vVS.jobLogInfo("FSCK job exited with errors")
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
		vVS.jobLogErr("Couldn't find fs.volumeStruct")
		return
	}

	globals.Unlock()

	vVS.inodeVolumeHandle, err = inode.FetchVolumeHandle(vVS.volumeName)
	if nil != err {
		vVS.jobLogErr("Couldn't find inode.VolumeHandle")
		return
	}

	vVS.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(vVS.volumeName)
	if nil != err {
		vVS.jobLogErr("Couldn't find headhunter.VolumeHandle")
		return
	}

	vVS.volume.validateVolumeRWMutex.Lock()
	defer vVS.volume.validateVolumeRWMutex.Unlock()

	// Flush all File Inodes currently in flight

	vVS.volume.untrackInFlightFileInodeDataAll()

	vVS.jobLogInfo("Completed flush of all inflight File Inode write traffic")

	// Do a checkpoint before actual FSCK work

	err = vVS.headhunterVolumeHandle.DoCheckpoint()
	if nil != err {
		vVS.jobLogErr("Got headhunter.DoCheckpoint failure: %v", err)
		return
	}

	vVS.jobLogInfo("Completed checkpoint prior to FSCK work")

	// Setup B+Tree to hold arbitrarily sized map[uint64]uint64 (i.e. beyond what will fit in memoory)

	vVS.bpTreeFile, err = ioutil.TempFile("", "ProxyFS_VaidateVolume_")
	if nil != err {
		vVS.jobLogErr("Got ioutil.TempFile() failure: %v", err)
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

	vVS.bpTreeFileNextOffset = 0

	vVS.bpTreeCache = sortedmap.NewBPlusTreeCache(jobBPTreeEvictLowLimit, jobBPTreeEvictHighLimit)

	// Validate all Inodes in InodeRec table in headhunter

	vVS.inodeBPTree = sortedmap.NewBPlusTree(jobBPTreeMaxKeysPerNode, sortedmap.CompareUint64, vVS, vVS.bpTreeCache)
	defer func(vVS *validateVolumeStruct) {
		var err error

		err = vVS.inodeBPTree.Discard()
		if nil != err {
			vVS.jobLogErr("Got vVS.inodeBPTree.Discard() failure: %v", err)
		}
	}(vVS)

	vVS.Lock() // Hold off vVS.validateVolumeInode() goroutines throughout loop

	vVS.jobStartParallelism(validateVolumeInodeParallelism)

	inodeIndex = 0

	for {
		if vVS.stopFlag {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.jobEndParallelism()
			return
		}

		inodeNumber, ok, err = vVS.headhunterVolumeHandle.IndexedInodeNumber(inodeIndex)
		if nil != err {
			vVS.jobLogErrWhileLocked("Got headhunter.IndexedInodeNumber(0x%016X) failure: %v", inodeIndex, err)
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.jobEndParallelism()
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

	vVS.jobEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.jobLogInfo("Completed validation of all Inode's")

	// TreeWalk computing LinkCounts for all inodeNumbers

	_, ok, err = vVS.inodeBPTree.GetByKey(uint64(inode.RootDirInodeNumber))
	if nil != err {
		vVS.jobLogErr("Got vVS.inodeBPTree.GetByKey(RootDirInodeNumber) failure: %v", err)
		return
	}
	if !ok {
		vVS.jobLogErr("Got vVS.inodeBPTree.GetByKey(RootDirInodeNumber) !ok")
		return
	}

	vVS.jobStartParallelism(validateVolumeCalculateLinkCountParallelism)

	vVS.childrenWaitGroup.Add(1)
	go vVS.validateVolumeCalculateLinkCount(uint64(inode.RootDirInodeNumber), uint64(inode.RootDirInodeNumber))

	vVS.childrenWaitGroup.Wait()

	vVS.jobEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.jobLogInfo("Completed treewalk before populating /%v/", lostAndFoundDirName)

	// Establish that lostAndFoundDirName exists

	vVS.lostAndFoundDirInodeNumber, err = vVS.inodeVolumeHandle.Lookup(inode.RootDirInodeNumber, lostAndFoundDirName)
	if nil == err {
		// Found it - make sure it is a directory

		inodeType, err = vVS.inodeVolumeHandle.GetType(vVS.lostAndFoundDirInodeNumber)
		if nil != err {
			vVS.jobLogErr("Got inode.GetType(vVS.lostAndFoundDirNumber==0x%016X) failure: %v", vVS.lostAndFoundDirInodeNumber, err)
			return
		}
		if inode.DirType != inodeType {
			vVS.jobLogErr("Got inode.GetType(vVS.lostAndFoundDirNumber==0x%016X) non-DirType", vVS.lostAndFoundDirInodeNumber)
			return
		}

		vVS.jobLogInfo("Found pre-existing /%v/", lostAndFoundDirName)
	} else {
		if blunder.Is(err, blunder.NotFoundError) {
			// Create it

			vVS.lostAndFoundDirInodeNumber, err = vVS.inodeVolumeHandle.CreateDir(inode.PosixModePerm, 0, 0)
			if nil != err {
				vVS.jobLogErr("Got inode.CreateDir() failure: %v", err)
				return
			}
			err = vVS.inodeVolumeHandle.Link(inode.RootDirInodeNumber, lostAndFoundDirName, vVS.lostAndFoundDirInodeNumber, false)
			if nil != err {
				vVS.jobLogErr("Got inode.Link(inode.RootDirInodeNumber, lostAndFoundDirName, vVS.lostAndFoundDirInodeNumber, false) failure: %v", err)
				err = vVS.inodeVolumeHandle.Destroy(vVS.lostAndFoundDirInodeNumber)
				if nil != err {
					vVS.jobLogErr("Got inode.Destroy(vVS.lostAndFoundDirInodeNumber) failure: %v", err)
				}
				return
			}

			ok, err = vVS.inodeBPTree.Put(uint64(vVS.lostAndFoundDirInodeNumber), uint64(2)) // /<lostAndFoundDirName> as well as /<lostAndFoundDirName>/.
			if nil != err {
				vVS.jobLogErr("Got vVS.inodeBPTree.Put(vVS.lostAndFoundDirInodeNumber, 1) failure: %v", err)
				vVS.Unlock()
				return
			}
			if !ok {
				vVS.jobLogErr("Got vVS.inodeBPTree.Put(vVS.lostAndFoundDirInodeNumber, 1) !ok")
				vVS.Unlock()
				return
			}

			vVS.jobLogInfo("Created /%v/", lostAndFoundDirName)
		} else {
			vVS.jobLogErr("Got inode.Lookup(inode.RootDirInodeNumber, lostAndFoundDirName) failure: %v", err)
			return
		}
	}

	// TODO: Scan B+Tree placing top-most orphan DirInodes as elements of vVS.lostAndFoundDirInodeNumber

	// Re-compute LinkCounts

	inodeCount, err = vVS.inodeBPTree.Len()
	if nil != err {
		vVS.jobLogErr("Got inode.Len() failure: %v", err)
		return
	}

	for inodeIndex = uint64(0); inodeIndex < uint64(inodeCount); inodeIndex++ {
		if vVS.stopFlag {
			return
		}

		ok, err = vVS.inodeBPTree.PatchByIndex(int(inodeIndex), uint64(0))
		if nil != err {
			vVS.jobLogErr("Got vVS.inodeBPTree.PatchByIndex(0x%016X, 0) failure: %v", inodeIndex, err)
			return
		}
		if !ok {
			vVS.jobLogErr("Got vVS.inodeBPTree.PatchByIndex(0x%016X, 0) !ok", inodeIndex)
			return
		}
	}

	vVS.jobStartParallelism(validateVolumeCalculateLinkCountParallelism)

	vVS.childrenWaitGroup.Add(1)
	go vVS.validateVolumeCalculateLinkCount(uint64(inode.RootDirInodeNumber), uint64(inode.RootDirInodeNumber))

	vVS.childrenWaitGroup.Wait()

	vVS.jobEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.jobLogInfo("Completed treewalk after populating /%v/", lostAndFoundDirName)

	// TODO: Scan B+Tree placing orphaned non-DirInodes as elements of vVS.lostAndFoundDirInodeNumber

	// Update incorrect LinkCounts

	vVS.Lock() // Hold off vVS.validateVolumeFixLinkCount() goroutines throughout loop

	vVS.jobStartParallelism(validateVolumeFixLinkCountParallelism)

	for inodeIndex = uint64(0); inodeIndex < uint64(inodeCount); inodeIndex++ {
		if vVS.stopFlag {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.jobEndParallelism()
			return
		}

		key, value, ok, err = vVS.inodeBPTree.GetByIndex(int(inodeIndex))
		if nil != err {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.jobEndParallelism()
			vVS.jobLogErr("Got vVS.inodeBPTree.GetByIndex(0x%016X) failure: %v", inodeIndex, err)
			return
		}
		if !ok {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.jobEndParallelism()
			vVS.jobLogErr("Got vVS.inodeBPTree.GetByIndex(0x%016X) !ok", inodeIndex)
			return
		}

		inodeNumber = key.(uint64)
		linkCountComputed = value.(uint64)

		vVS.childrenWaitGroup.Add(1)
		go vVS.validateVolumeFixLinkCount(inodeNumber, linkCountComputed) // Will be blocked until subsequent vVS.Unlock()
	}

	vVS.Unlock()

	vVS.childrenWaitGroup.Wait()

	vVS.jobEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.jobLogInfo("Completed LinkCount fix-up of all Inode's")

	// If vVS.lostAndFoundDirInodeNumber is empty, remove it

	_, moreEntries, err = vVS.inodeVolumeHandle.ReadDir(vVS.lostAndFoundDirInodeNumber, 2, 0)
	if nil != err {
		vVS.jobLogErr("Got ReadDir(vVS.lostAndFoundDirInodeNumber, 2, 0) failure: %v", err)
		return
	}

	if moreEntries {
		vVS.jobLogInfo("Preserving non-empty /%v/", lostAndFoundDirName)
	} else {
		err = vVS.inodeVolumeHandle.Unlink(inode.RootDirInodeNumber, lostAndFoundDirName, false)
		if nil != err {
			vVS.jobLogErr("Got inode.Unlink(inode.RootDirInodeNumber, lostAndFoundDirName, false) failure: %v", err)
			return
		}

		err = vVS.inodeVolumeHandle.Destroy(vVS.lostAndFoundDirInodeNumber)
		if nil != err {
			vVS.jobLogErr("Got inode.Destroy(vVS.lostAndFoundDirInodeNumber) failure: %v", err)
			return
		}

		vVS.jobLogInfo("Removed empty /%v/", lostAndFoundDirName)
	}

	// Clean out unreferenced headhunter B+Tree "Objects")

	vVS.objectBPTree = sortedmap.NewBPlusTree(jobBPTreeMaxKeysPerNode, sortedmap.CompareUint64, vVS, vVS.bpTreeCache)
	defer func(vVS *validateVolumeStruct) {
		var err error

		err = vVS.objectBPTree.Discard()
		if nil != err {
			vVS.jobLogErr("Got vVS.objectBPTree.Discard() failure: %v", err)
		}
	}(vVS)

	vVS.Lock() // Hold off vVS.validateVolumeIncrementByteCounts() goroutines throughout loop

	vVS.jobStartParallelism(validateVolumeIncrementByteCountsParallelism)

	inodeIndex = 0

	for {
		if vVS.stopFlag {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.jobEndParallelism()
			return
		}

		inodeNumber, ok, err = vVS.headhunterVolumeHandle.IndexedInodeNumber(inodeIndex)
		if nil != err {
			vVS.jobLogErrWhileLocked("Got headhunter.IndexedInodeNumber(0x%016X) failure: %v", inodeIndex, err)
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.jobEndParallelism()
			return
		}
		if !ok {
			break
		}

		vVS.childrenWaitGroup.Add(1)
		go vVS.validateVolumeIncrementByteCounts(inodeNumber) // Will be blocked until subsequent vVS.Unlock()

		inodeIndex++
	}

	vVS.Unlock()

	vVS.childrenWaitGroup.Wait()

	vVS.jobEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	objectIndex = 0

	for {
		if vVS.stopFlag {
			return
		}

		objectNumber, ok, err = vVS.headhunterVolumeHandle.IndexedBPlusTreeObjectNumber(objectIndex)
		if nil != err {
			vVS.jobLogErr("Got headhunter.IndexedBPlusTreeObjectNumber(0x%016X) failure: %v", objectIndex, err)
			return
		}

		if !ok {
			break
		}

		_, ok, err = vVS.objectBPTree.GetByKey(objectNumber)
		if nil != err {
			vVS.jobLogErr("Got vVS.objectBPTree.GetByKey(0x%016X) failure: %v", objectNumber, err)
			return
		}

		if ok {
			objectIndex++
		} else {
			err = vVS.headhunterVolumeHandle.DeleteBPlusTreeObject(objectNumber)
			if nil == err {
				vVS.jobLogInfo("Removed unreferenced headhunter B+Tree \"Object\" 0x%016X", objectNumber)
			} else {
				vVS.jobLogErr("Got headhunter.DeleteBPlusTreeObject(0x%016X) failure: %v", objectNumber, err)
				return
			}
		}
	}

	vVS.jobLogInfo("Completed clean out unreferenced headhunter B+Tree \"Objects\"")

	// TODO: Walk all FileInodes tracking referenced LogSegments
	// TODO: Remove non-Checkpoint Objects not in headhunter's LogSegment B+Tree
	// TODO: Delete unreferenced LogSegments (both headhunter records & Objects)

	// Do a final checkpoint

	err = vVS.headhunterVolumeHandle.DoCheckpoint()
	if nil != err {
		vVS.jobLogErr("Got headhunter.DoCheckpoint failure: %v", err)
		return
	}

	vVS.jobLogInfo("Completed checkpoint after FSCK work")

	// Validate headhunter checkpoint container contents

	headhunterLayoutReport, err = vVS.headhunterVolumeHandle.FetchLayoutReport(headhunter.MergedBPlusTree)
	if nil != err {
		vVS.jobLogErr("Got headhunter.FetchLayoutReport() failure: %v", err)
		return
	}

	vVS.accountName, vVS.checkpointContainerName = vVS.headhunterVolumeHandle.FetchAccountAndCheckpointContainerNames()

	_, checkpointContainerObjectList, err = swiftclient.ContainerGet(vVS.accountName, vVS.checkpointContainerName)
	if nil != err {
		vVS.jobLogErr("Got swiftclient.ContainerGet(\"%v\",\"%v\") failure: %v", vVS.accountName, vVS.checkpointContainerName, err)
		return
	}

	for _, checkpointContainerObjectName = range checkpointContainerObjectList {
		checkpointContainerObjectNumber = uint64(0) // If remains 0 or results in returning to 0,
		//                                             checkpointContainerObjectName should be deleted

		if 16 == len(checkpointContainerObjectName) {
			if validObjectNameRE.MatchString(checkpointContainerObjectName) {
				checkpointContainerObjectNumber, err = strconv.ParseUint(checkpointContainerObjectName, 16, 64)
				if nil != err {
					vVS.jobLogErr("Got strconv.ParseUint(\"%v\",16,64) failure: %v", checkpointContainerObjectName)
					return // Note: Already scheduled async Object DELETEs will continue in the background
				}

				_, ok = headhunterLayoutReport[checkpointContainerObjectNumber]
				if !ok {
					checkpointContainerObjectNumber = uint64(0)
				}
			}
		}

		if uint64(0) == checkpointContainerObjectNumber {
			vVS.jobLogInfo("Removing unreferenced checkpointContainerObject %v", checkpointContainerObjectName)
			asyncDeleteWaitGroup.Add(1)
			swiftclient.ObjectDeleteAsync(vVS.accountName, vVS.checkpointContainerName, checkpointContainerObjectName, swiftclient.SkipRetry, nil, &asyncDeleteWaitGroup)
		}

		if vVS.stopFlag {
			return // Note: Already scheduled async Object DELETEs will continue in the background
		}
	}

	asyncDeleteWaitGroup.Wait()
}

func (sVS *scrubVolumeStruct) Active() (active bool) {
	active = sVS.active
	return
}

func (sVS *scrubVolumeStruct) Wait() {
	sVS.globalWaitGroup.Wait()
}

func (sVS *scrubVolumeStruct) Cancel() {
	sVS.stopFlag = true
	sVS.Wait()
}

func (sVS *scrubVolumeStruct) Error() (err []string) {
	var errString string

	sVS.Lock()

	err = make([]string, 0, len(sVS.err))
	for _, errString = range sVS.err {
		err = append(err, errString)
	}

	sVS.Unlock()

	return
}

func (sVS *scrubVolumeStruct) Info() (info []string) {
	var infoString string

	sVS.Lock()

	info = make([]string, 0, len(sVS.info))
	for _, infoString = range sVS.info {
		info = append(info, infoString)
	}

	sVS.Unlock()

	return
}

/*
func (sVS *scrubVolumeStruct) scrubVolumeInode(inodeNumber uint64) {
	var (
		err error
		ok  bool
	)

	defer vVS.childrenWaitGroup.Done()

	vVS.jobGrabParallelism()
	defer vVS.jobReleaseParallelism()

	if vVS.stopFlag {
		return
	}

	err = vVS.inodeVolumeHandle.Validate(inode.InodeNumber(inodeNumber), true)

	vVS.Lock()
	defer vVS.Unlock()

	if nil == err {
		ok, err = vVS.inodeBPTree.Put(inodeNumber, uint64(0)) // Initial LinkCount == 0
		if nil != err {
			vVS.jobLogErrWhileLocked("Got vVS.inodeBPTree.Put(0x%016X, 0) failure: %v", inodeNumber, err)
			return
		}
		if !ok {
			vVS.jobLogErrWhileLocked("Got vVS.inodeBPTree.Put(0x%016X, 0) !ok", inodeNumber)
			return
		}
	} else {
		vVS.jobLogInfoWhileLocked("Got inode.Validate(0x%016X) failure: %v ... removing it", inodeNumber, err)

		err = vVS.headhunterVolumeHandle.DeleteInodeRec(inodeNumber)
		if nil != err {
			vVS.jobLogErrWhileLocked("Got headhunter.DeleteInodeRec(0x%016X) failure: %v", inodeNumber, err)
		}
	}
}
*/

func (sVS *scrubVolumeStruct) scrubVolume() {
	var (
		err error
		ok  bool
	)

	sVS.jobLogInfo("SCRUB job initiated")

	defer func(sVS *scrubVolumeStruct) {
		if sVS.stopFlag {
			sVS.jobLogInfo("SCRUB job stopped")
		} else if 0 == len(sVS.err) {
			sVS.jobLogInfo("SCRUB job completed without error")
		} else if 1 == len(sVS.err) {
			sVS.jobLogInfo("SCRUB job exited with one error")
		} else {
			sVS.jobLogInfo("SCRUB job exited with errors")
		}
	}(sVS)

	defer func(sVS *scrubVolumeStruct) {
		sVS.active = false
	}(sVS)

	defer sVS.globalWaitGroup.Done()

	// Find specified volume

	globals.Lock()

	sVS.volume, ok = globals.volumeMap[sVS.volumeName]
	if !ok {
		globals.Unlock()
		sVS.jobLogErr("Couldn't find fs.volumeStruct")
		return
	}

	globals.Unlock()

	sVS.inodeVolumeHandle, err = inode.FetchVolumeHandle(sVS.volumeName)
	if nil != err {
		sVS.jobLogErr("Couldn't find inode.VolumeHandle")
		return
	}

	sVS.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(sVS.volumeName)
	if nil != err {
		sVS.jobLogErr("Couldn't find headhunter.VolumeHandle")
		return
	}

	// Setup B+Tree to hold arbitrarily sized map[uint64]uint64 (i.e. beyond what will fit in memoory)

	sVS.bpTreeFile, err = ioutil.TempFile("", "ProxyFS_ScrubVolume_")
	if nil != err {
		sVS.jobLogErr("Got ioutil.TempFile() failure: %v", err)
		return
	}
	defer func(sVS *scrubVolumeStruct) {
		var (
			bpTreeFileName string
		)

		bpTreeFileName = sVS.bpTreeFile.Name()
		_ = sVS.bpTreeFile.Close()
		_ = os.Remove(bpTreeFileName)
	}(sVS)

	sVS.bpTreeFileNextOffset = 0

	sVS.bpTreeCache = sortedmap.NewBPlusTreeCache(jobBPTreeEvictLowLimit, jobBPTreeEvictHighLimit)

	sVS.jobLogInfo("TODO: Finish scrubVolume() - need to fetch inode list while ex-Lock'd")

	sVS.volume.validateVolumeRWMutex.RLock()
	defer sVS.volume.validateVolumeRWMutex.RLock()

	/*
		// Validate all Inodes in InodeRec table in headhunter

		vVS.inodeBPTree = sortedmap.NewBPlusTree(jobBPTreeMaxKeysPerNode, sortedmap.CompareUint64, vVS, vVS.bpTreeCache)
		defer func(vVS *validateVolumeStruct) {
			var err error

			err = vVS.inodeBPTree.Discard()
			if nil != err {
				vVS.jobLogErr("Got vVS.inodeBPTree.Discard() failure: %v", err)
			}
		}(vVS)

		vVS.Lock() // Hold off vVS.validateVolumeInode() goroutines throughout loop

		vVS.jobStartParallelism(validateVolumeInodeParallelism)

		inodeIndex = 0

		for {
			if vVS.stopFlag {
				vVS.Unlock()
				vVS.childrenWaitGroup.Wait()
				vVS.jobEndParallelism()
				return
			}

			inodeNumber, ok, err = vVS.headhunterVolumeHandle.IndexedInodeNumber(inodeIndex)
			if nil != err {
				vVS.jobLogErrWhileLocked("Got headhunter.IndexedInodeNumber(0x%016X) failure: %v", inodeIndex, err)
				vVS.Unlock()
				vVS.childrenWaitGroup.Wait()
				vVS.jobEndParallelism()
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

		vVS.jobEndParallelism()

		if vVS.stopFlag || (0 < len(vVS.err)) {
			return
		}

		vVS.jobLogInfo("Completed validation of all Inode's")
	*/
}
