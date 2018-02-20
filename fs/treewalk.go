package fs

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
)

const (
	validateVolumeBPTreeMaxKeysPerNode = uint64(100)
	validateVolumeBPTreeEvictLowLimit  = uint64(90)
	validateVolumeBPTreeEvictHighLimit = uint64(100)

	validateVolumeInodeParallelism        = uint64(100)
	validateVolumeDirInodeParallelism     = uint64(50)
	validateVolumeLinkCountParallelism    = uint64(50)
	validateVolumeLinkCountFixParallelism = uint64(50)

	lostAndFoundDirName = "Lost+Found"
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

	err = vVS.inodeVolumeHandle.Validate(inode.InodeNumber(inodeNumber))

	vVS.Lock()
	defer vVS.Unlock()

	if nil == err {
		ok, err = vVS.bpTree.Put(inodeNumber, uint64(0)) // Initial LinkCount == 0
		if nil != err {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got bpTree.Put(0x%016X, 0) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
			return
		}
		if !ok {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got bpTree.Put(0x%016X, 0) !ok", time.Now().Format(time.RFC3339), inodeNumber))
			return
		}
	} else {
		vVS.info = append(vVS.info, fmt.Sprintf("%v Got inode.Validate(0x%016X) failure: %v ... removing it", time.Now().Format(time.RFC3339), inodeNumber, err))

		err = vVS.headhunterVolumeHandle.DeleteInodeRec(inodeNumber)
		if nil != err {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got headhunter.DeleteInodeRec(0x%016X) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
		}
	}
}

func (vVS *validateVolumeStruct) validateVolumeDirInode(parentDirInodeNumber uint64, dirInodeNumber uint64) {
	var (
		dirEntrySlice        []inode.DirEntry
		dotDotWasSeen        bool
		dotWasSeen           bool
		err                  error
		inodeNumber          uint64
		inodeType            inode.InodeType
		linkCount            uint64
		moreEntries          bool
		ok                   bool
		prevReturnedAsString string
		value                sortedmap.Value
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
			vVS.Lock()
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.ReadDir(0x%016X,1,0,\"%v\") failure: %v", time.Now().Format(time.RFC3339), dirInodeNumber, prevReturnedAsString, err))
			vVS.Unlock()
			return
		}

		switch len(dirEntrySlice) {
		case 0:
			break forLabel
		case 1:
			// Increment LinkCount for dirEntrySlice[0]'s InodeNumber

			inodeNumber = uint64(dirEntrySlice[0].InodeNumber)
			prevReturnedAsString = dirEntrySlice[0].Basename

			vVS.Lock()
			value, ok, err = vVS.bpTree.GetByKey(inodeNumber)
			if nil != err {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.GetByKey(0x%016X) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
				vVS.Unlock()
				return
			}
			if !ok {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.GetByKey(0x%016X) !ok", time.Now().Format(time.RFC3339), inodeNumber))
				vVS.Unlock()
				return
			}
			linkCount = value.(uint64) + 1
			ok, err = vVS.bpTree.PatchByKey(inodeNumber, linkCount)
			if nil != err {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.PatchByKey(0x%016X,) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
				vVS.Unlock()
				return
			}
			if !ok {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.PatchByKey(0x%016X,) !ok", time.Now().Format(time.RFC3339), inodeNumber))
				vVS.Unlock()
				return
			}
			vVS.Unlock()

			// Recurse into sub-directories

			switch prevReturnedAsString {
			case ".":
				dotWasSeen = true
				if inodeNumber != dirInodeNumber {
					vVS.Lock()
					vVS.err = append(vVS.err, fmt.Sprintf("%v Got \".\" DirEntry not matching dirInodeNumber 0x%016X from inode.ReadDir(0x%016X,1,0,\"%v\")", time.Now().Format(time.RFC3339), dirInodeNumber, dirInodeNumber, prevReturnedAsString))
					vVS.Unlock()
					return
				}
			case "..":
				dotDotWasSeen = true
				if inodeNumber != parentDirInodeNumber {
					vVS.Lock()
					vVS.err = append(vVS.err, fmt.Sprintf("%v Got \"..\" DirEntry not matching parentDirInodeNumber 0x%016X from inode.ReadDir(0x%016X,1,0,\"%v\")", time.Now().Format(time.RFC3339), parentDirInodeNumber, dirInodeNumber, prevReturnedAsString))
					vVS.Unlock()
					return
				}
			default:
				inodeType, err = vVS.inodeVolumeHandle.GetType(inode.InodeNumber(inodeNumber))
				if nil != err {
					vVS.Lock()
					vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.GetType(0x%016X) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
					vVS.Unlock()
					return
				}

				if inode.DirType == inodeType {
					vVS.childrenWaitGroup.Add(1)
					go vVS.validateVolumeDirInode(dirInodeNumber, inodeNumber)
				}
			}

			if !moreEntries {
				break forLabel
			}
		default:
			vVS.Lock()
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got too many DirEntry's from inode.ReadDir(0x%016X,1,0,\"%v\")", time.Now().Format(time.RFC3339), dirInodeNumber, prevReturnedAsString))
			vVS.Unlock()
			return
		}
	}

	if !dotWasSeen {
		vVS.Lock()
		vVS.err = append(vVS.err, fmt.Sprintf("%v Missing \".\" for dirInodeNumber 0x%016X", time.Now().Format(time.RFC3339), dirInodeNumber))
		vVS.Unlock()
		return
	}

	if !dotDotWasSeen {
		vVS.Lock()
		vVS.err = append(vVS.err, fmt.Sprintf("%v Missing \"..\" for dirInodeNumber 0x%016X", time.Now().Format(time.RFC3339), dirInodeNumber))
		vVS.Unlock()
		return
	}
}

func (vVS *validateVolumeStruct) validateVolumeLinkCount(dirInodeNumber uint64) {
	var (
		dirEntrySlice        []inode.DirEntry
		err                  error
		inodeNumber          uint64
		inodeType            inode.InodeType
		linkCount            uint64
		moreEntries          bool
		ok                   bool
		prevReturnedAsString string
		value                sortedmap.Value
	)

	defer vVS.childrenWaitGroup.Done()

	vVS.validateVolumeGrabParallelism()
	defer vVS.validateVolumeReleaseParallelism()

	prevReturnedAsString = ""

forLabel:
	for {
		if vVS.stopFlag {
			return
		}

		dirEntrySlice, moreEntries, err = vVS.inodeVolumeHandle.ReadDir(inode.InodeNumber(dirInodeNumber), 1, 0, prevReturnedAsString)
		if nil != err {
			vVS.Lock()
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.ReadDir(0x%016X,1,0,\"%v\") failure: %v", time.Now().Format(time.RFC3339), dirInodeNumber, prevReturnedAsString, err))
			vVS.Unlock()
			return
		}

		switch len(dirEntrySlice) {
		case 0:
			break forLabel
		case 1:
			// Increment LinkCount for dirEntrySlice[0]'s InodeNumber

			inodeNumber = uint64(dirEntrySlice[0].InodeNumber)
			prevReturnedAsString = dirEntrySlice[0].Basename

			vVS.Lock()
			value, ok, err = vVS.bpTree.GetByKey(inodeNumber)
			if nil != err {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.GetByKey(0x%016X) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
				vVS.Unlock()
				return
			}
			if !ok {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.GetByKey(0x%016X) !ok", time.Now().Format(time.RFC3339), inodeNumber))
				vVS.Unlock()
				return
			}

			linkCount = value.(uint64) + 1

			ok, err = vVS.bpTree.PatchByKey(inodeNumber, linkCount)
			if nil != err {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.PatchByKey(0x%016X,) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
				vVS.Unlock()
				return
			}
			if !ok {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.PatchByKey(0x%016X,) !ok", time.Now().Format(time.RFC3339), inodeNumber))
				vVS.Unlock()
				return
			}
			vVS.Unlock()

			// Recurse into sub-directories

			if ("." != prevReturnedAsString) && (".." != prevReturnedAsString) {
				inodeType, err = vVS.inodeVolumeHandle.GetType(inode.InodeNumber(inodeNumber))
				if nil != err {
					vVS.Lock()
					vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.GetType(0x%016X) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
					vVS.Unlock()
					return
				}

				if inode.DirType == inodeType {
					vVS.childrenWaitGroup.Add(1)
					go vVS.validateVolumeLinkCount(inodeNumber)
				}
			}

			if !moreEntries {
				break forLabel
			}
		default:
			vVS.Lock()
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got too many DirEntry's from inode.ReadDir(0x%016X,1,0,\"%v\")", time.Now().Format(time.RFC3339), dirInodeNumber, prevReturnedAsString))
			vVS.Unlock()
			return
		}
	}
}

func (vVS *validateVolumeStruct) validateVolumeLinkCountFix(inodeNumber uint64, linkCountComputed uint64) {
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
		vVS.Lock()
		vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.GetLinkCount(0x%016X) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
		vVS.Unlock()
		return
	}

	if linkCountComputed != linkCountInInode {
		err = vVS.inodeVolumeHandle.SetLinkCount(inode.InodeNumber(inodeNumber), linkCountComputed)
		if nil == err {
			vVS.Lock()
			vVS.info = append(vVS.info, fmt.Sprintf("%v Corrected LinkCount in Inode# 0x%016X from 0x%016X to 0x%016X", time.Now().Format(time.RFC3339), inodeNumber, linkCountInInode, linkCountComputed))
			vVS.Unlock()
		} else {
			vVS.Lock()
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.SetLinkCount(0x%016X,) failure: %v", time.Now().Format(time.RFC3339), inodeNumber, err))
			vVS.Unlock()
			return
		}
	}
}

func (vVS *validateVolumeStruct) validateVolume() {
	var (
		err               error
		key               sortedmap.Key
		inodeCount        int
		inodeIndex        uint64
		inodeNumber       uint64
		inodeType         inode.InodeType
		linkCountComputed uint64
		moreEntries       bool
		ok                bool
		value             sortedmap.Value
	)

	defer func(vVS *validateVolumeStruct) {
		vVS.active = false
	}(vVS)

	defer vVS.globalWaitGroup.Done()

	// Find specified volume

	globals.Lock()

	vVS.volume, ok = globals.volumeMap[vVS.volumeName]
	if !ok {
		globals.Unlock()
		vVS.err = append(vVS.err, fmt.Sprintf("%v Couldn't find fs.volumeStruct", time.Now().Format(time.RFC3339)))
		return
	}

	globals.Unlock()

	vVS.inodeVolumeHandle, err = inode.FetchVolumeHandle(vVS.volumeName)
	if nil != err {
		vVS.err = append(vVS.err, fmt.Sprintf("%v Couldn't find inode.VolumeHandle", time.Now().Format(time.RFC3339)))
		return
	}

	vVS.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(vVS.volumeName)
	if nil != err {
		vVS.err = append(vVS.err, fmt.Sprintf("%v Couldn't find headhunter.VolumeHandle", time.Now().Format(time.RFC3339)))
		return
	}

	vVS.volume.validateVolumeRWMutex.Lock()
	defer vVS.volume.validateVolumeRWMutex.Unlock()

	// Flush all File Inodes currently in flight

	vVS.volume.untrackInFlightFileInodeDataAll()

	vVS.info = append(vVS.info, fmt.Sprintf("%v Completed flush of all inflight File Inode write traffic", time.Now().Format(time.RFC3339)))

	// Do a checkpoint before actual FSCK work

	err = vVS.headhunterVolumeHandle.DoCheckpoint()
	if nil != err {
		vVS.err = append(vVS.err, fmt.Sprintf("%v Got headhunter.DoCheckpoint failure: %v", time.Now().Format(time.RFC3339), err))
		return
	}

	vVS.info = append(vVS.info, fmt.Sprintf("%v Completed checkpoint prior to FSCK work", time.Now().Format(time.RFC3339)))

	// Setup B+Tree to hold arbitrarily sized map[uint64]uint64 (i.e. beyond what will fit in memoory)

	vVS.bpTreeFile, err = ioutil.TempFile("", "ProxyFS_VaidateVolume_")
	if nil != err {
		vVS.err = append(vVS.err, fmt.Sprintf("%v Got ioutil.TempFile() failure: %v", time.Now().Format(time.RFC3339), err))
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
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got headhunter.IndexedInodeNumber(0x%016X) failure: %v", time.Now().Format(time.RFC3339), inodeIndex, err))
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

	vVS.info = append(vVS.info, fmt.Sprintf("%v Completed validation of all Inode's", time.Now().Format(time.RFC3339)))

	// TreeWalk computing LinkCounts for all inodeNumbers

	_, ok, err = vVS.bpTree.GetByKey(uint64(inode.RootDirInodeNumber))
	if nil != err {
		vVS.err = append(vVS.err, fmt.Sprintf("%v Got bpTree.GetByKey(RootDirInodeNumber) failure: %v", time.Now().Format(time.RFC3339), err))
		return
	}
	if !ok {
		vVS.err = append(vVS.err, fmt.Sprintf("%v Got bpTree.GetByKey(RootDirInodeNumber) !ok", time.Now().Format(time.RFC3339)))
		return
	}

	vVS.validateVolumeStartParallelism(validateVolumeDirInodeParallelism)

	vVS.childrenWaitGroup.Add(1)
	go vVS.validateVolumeDirInode(uint64(inode.RootDirInodeNumber), uint64(inode.RootDirInodeNumber))

	vVS.childrenWaitGroup.Wait()

	vVS.validateVolumeEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.info = append(vVS.info, fmt.Sprintf("%v Completed treewalk before populating /%v/", time.Now().Format(time.RFC3339), lostAndFoundDirName))

	// Establish that lostAndFoundDirName exists

	vVS.lostAndFoundDirInodeNumber, err = vVS.inodeVolumeHandle.Lookup(inode.RootDirInodeNumber, lostAndFoundDirName)
	if nil == err {
		// Found it - make sure it is a directory

		inodeType, err = vVS.inodeVolumeHandle.GetType(vVS.lostAndFoundDirInodeNumber)
		if nil != err {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.GetType(vVS.lostAndFoundDirNumber==0x%016X) failure: %v", time.Now().Format(time.RFC3339), vVS.lostAndFoundDirInodeNumber, err))
			return
		}
		if inode.DirType != inodeType {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.GetType(vVS.lostAndFoundDirNumber==0x%016X) non-DirType", time.Now().Format(time.RFC3339), vVS.lostAndFoundDirInodeNumber))
			return
		}

		vVS.info = append(vVS.info, fmt.Sprintf("%v Found pre-existing /%v/", time.Now().Format(time.RFC3339), lostAndFoundDirName))
	} else {
		if blunder.Is(err, blunder.NotFoundError) {
			// Create it

			vVS.lostAndFoundDirInodeNumber, err = vVS.inodeVolumeHandle.CreateDir(inode.PosixModePerm, 0, 0)
			if nil != err {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.CreateDir() failure: %v", time.Now().Format(time.RFC3339), err))
				return
			}
			err = vVS.inodeVolumeHandle.Link(inode.RootDirInodeNumber, lostAndFoundDirName, vVS.lostAndFoundDirInodeNumber)
			if nil != err {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.Link(inode.RootDirInodeNumber, lostAndFoundDirName, vVS.lostAndFoundDirInodeNumber) failure: %v", time.Now().Format(time.RFC3339), err))
				err = vVS.inodeVolumeHandle.Destroy(vVS.lostAndFoundDirInodeNumber)
				if nil != err {
					vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.Destroy(vVS.lostAndFoundDirInodeNumber) failure: %v", time.Now().Format(time.RFC3339), err))
				}
				return
			}

			ok, err = vVS.bpTree.Put(uint64(vVS.lostAndFoundDirInodeNumber), uint64(2)) // /<lostAndFoundDirName> as well as /<lostAndFoundDirName>/.
			if nil != err {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got bpTree.Put(vVS.lostAndFoundDirInodeNumber, 1) failure: %v", time.Now().Format(time.RFC3339), err))
				vVS.Unlock()
				return
			}
			if !ok {
				vVS.err = append(vVS.err, fmt.Sprintf("%v Got bpTree.Put(vVS.lostAndFoundDirInodeNumber, 1) !ok", time.Now().Format(time.RFC3339)))
				vVS.Unlock()
				return
			}

			vVS.info = append(vVS.info, fmt.Sprintf("%v Created /%v/", time.Now().Format(time.RFC3339), lostAndFoundDirName))
		} else {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.Lookup(inode.RootDirInodeNumber, lostAndFoundDirName) failure: %v", time.Now().Format(time.RFC3339), err))
			return
		}
	}

	// TODO: Scan B+Tree placing top-most orphan DirInodes as elements of vVS.lostAndFoundDirInodeNumber

	// Re-compute LinkCounts

	inodeCount, err = vVS.bpTree.Len()
	if nil != err {
		vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.Len() failure: %v", time.Now().Format(time.RFC3339), err))
		return
	}

	for inodeIndex = uint64(0); inodeIndex < uint64(inodeCount); inodeIndex++ {
		if vVS.stopFlag {
			return
		}

		ok, err = vVS.bpTree.PatchByIndex(int(inodeIndex), uint64(0))
		if nil != err {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.PatchByIndex(0x%016X, 0) failure: %v", time.Now().Format(time.RFC3339), inodeIndex, err))
			return
		}
		if !ok {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.PatchByIndex(0x%016X, 0) !ok", time.Now().Format(time.RFC3339), inodeIndex))
			return
		}
	}

	vVS.validateVolumeStartParallelism(validateVolumeLinkCountParallelism)

	vVS.childrenWaitGroup.Add(1)
	go vVS.validateVolumeLinkCount(uint64(inode.RootDirInodeNumber))

	vVS.childrenWaitGroup.Wait()

	vVS.validateVolumeEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.info = append(vVS.info, fmt.Sprintf("%v Completed treewalk after populating /%v/", time.Now().Format(time.RFC3339), lostAndFoundDirName))

	// TODO: Scan B+Tree placing orphaned non-DirInodes as elements of vVS.lostAndFoundDirInodeNumber

	// Update incorrect LinkCounts

	vVS.Lock() // Hold off vVS.validateVolumeLinkCountFix() goroutines throughout loop

	vVS.validateVolumeStartParallelism(validateVolumeLinkCountFixParallelism)

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
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.GetByIndex(0x%016X) failure: %v", time.Now().Format(time.RFC3339), inodeIndex, err))
			return
		}
		if !ok {
			vVS.Unlock()
			vVS.childrenWaitGroup.Wait()
			vVS.validateVolumeEndParallelism()
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got vVS.bpTree.GetByIndex(0x%016X) !ok", time.Now().Format(time.RFC3339), inodeIndex))
			return
		}

		inodeNumber = key.(uint64)
		linkCountComputed = value.(uint64)

		vVS.childrenWaitGroup.Add(1)
		go vVS.validateVolumeLinkCountFix(inodeNumber, linkCountComputed) // Will be blocked until subsequent vVS.Unlock()
	}

	vVS.Unlock()

	vVS.childrenWaitGroup.Wait()

	vVS.validateVolumeEndParallelism()

	if vVS.stopFlag || (0 < len(vVS.err)) {
		return
	}

	vVS.info = append(vVS.info, fmt.Sprintf("%v Completed LinkCount fix-up of all Inode's", time.Now().Format(time.RFC3339)))

	// If vVS.lostAndFoundDirInodeNumber is empty, remove it

	_, moreEntries, err = vVS.inodeVolumeHandle.ReadDir(vVS.lostAndFoundDirInodeNumber, 2, 0)
	if nil != err {
		vVS.err = append(vVS.err, fmt.Sprintf("%v Got ReadDir(vVS.lostAndFoundDirInodeNumber, 2, 0) failure: %v", time.Now().Format(time.RFC3339), err))
		return
	}

	if moreEntries {
		vVS.info = append(vVS.info, fmt.Sprintf("%v Preserving non-empty /%v/", time.Now().Format(time.RFC3339), lostAndFoundDirName))
	} else {
		err = vVS.inodeVolumeHandle.Unlink(inode.RootDirInodeNumber, lostAndFoundDirName)
		if nil != err {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.Unlink(inode.RootDirInodeNumber, lostAndFoundDirName) failure: %v", time.Now().Format(time.RFC3339), err))
			return
		}

		err = vVS.inodeVolumeHandle.Destroy(vVS.lostAndFoundDirInodeNumber)
		if nil != err {
			vVS.err = append(vVS.err, fmt.Sprintf("%v Got inode.Destroy(vVS.lostAndFoundDirInodeNumber) failure: %v", time.Now().Format(time.RFC3339), err))
			return
		}

		vVS.info = append(vVS.info, fmt.Sprintf("%v Removed empty /%v/", time.Now().Format(time.RFC3339), lostAndFoundDirName))
	}

	// TODO: Remove non-Checkpoint Objects not in headhunter's LogSegment B+Tree
	// TODO: Walk all FileInodes tracking referenced LogSegments
	// TODO: Delete unreferenced LogSegments (both headhunter records & Objects)
	// TODO: Walk all DirInodes & FileInodes tracking referenced headhunter B+Tree "Objects"
	// TODO: Delete unreferenced headhunter B+Tree "Objects"
	// TODO: Do a final checkpoint
	// TODO: Compute TreeLayout for all three headhunter B+Trees
	// TODO: Remove unreferenced Checkpoint Objects
}
