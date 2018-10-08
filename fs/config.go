package fs

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/transitions"
)

type inFlightFileInodeDataStruct struct {
	inode.InodeNumber                 // Indicates the InodeNumber of a fileInode with unflushed
	volStruct          *volumeStruct  // Synchronized via volStruct's sync.Mutex
	control            chan bool      // Signal with true to flush (and exit), false to simply exit
	wg                 sync.WaitGroup // Client can know when done
	globalsListElement *list.Element  // Back-pointer to wrapper used to insert into globals.inFlightFileInodeDataList
}

// inFlightFileInodeDataControlBuffering specifies the inFlightFileInodeDataStruct.control channel buffer size
// Note: There are potentially multiple initiators of this signal
const inFlightFileInodeDataControlBuffering = 100

type mountStruct struct {
	id        MountID
	options   MountOptions
	volStruct *volumeStruct
}

type volumeStruct struct {
	dataMutex                sync.Mutex
	volumeName               string
	doCheckpointPerFlush     bool
	maxFlushTime             time.Duration
	FLockMap                 map[inode.InodeNumber]*list.List
	inFlightFileInodeDataMap map[inode.InodeNumber]*inFlightFileInodeDataStruct
	mountList                []MountID
	jobRWMutex               sync.RWMutex
	inodeVolumeHandle        inode.VolumeHandle
	headhunterVolumeHandle   headhunter.VolumeHandle
}

type globalsStruct struct {
	sync.Mutex
	volumeMap                 map[string]*volumeStruct // key == volumeStruct.volumeName
	mountMap                  map[MountID]*mountStruct
	lastMountID               MountID
	inFlightFileInodeDataList *list.List

	AccessUsec         bucketstats.BucketLog2Round
	CreateUsec         bucketstats.BucketLog2Round
	FlushUsec          bucketstats.BucketLog2Round
	FlockGetUsec       bucketstats.BucketLog2Round
	FlockLockUsec      bucketstats.BucketLog2Round
	FlockUnlockUsec    bucketstats.BucketLog2Round
	GetstatUsec        bucketstats.BucketLog2Round
	GetTypeUsec        bucketstats.BucketLog2Round
	GetXAttrUsec       bucketstats.BucketLog2Round
	IsDirUsec          bucketstats.BucketLog2Round
	IsFileUsec         bucketstats.BucketLog2Round
	IsSymlinkUsec      bucketstats.BucketLog2Round
	LinkUsec           bucketstats.BucketLog2Round
	ListXAttrUsec      bucketstats.BucketLog2Round
	LookupUsec         bucketstats.BucketLog2Round
	LookupPathUsec     bucketstats.BucketLog2Round
	MkdirUsec          bucketstats.BucketLog2Round
	RemoveXAttrUsec    bucketstats.BucketLog2Round
	RenameUsec         bucketstats.BucketLog2Round
	ReadUsec           bucketstats.BucketLog2Round
	ReadBytes          bucketstats.BucketLog2Round
	ReaddirUsec        bucketstats.BucketLog2Round
	ReaddirEntries     bucketstats.BucketLog2Round
	ReaddirOneUsec     bucketstats.BucketLog2Round
	ReaddirOnePlusUsec bucketstats.BucketLog2Round
	ReaddirPlusUsec    bucketstats.BucketLog2Round
	ReaddirPlusBytes   bucketstats.BucketLog2Round
	ReadsymlinkUsec    bucketstats.BucketLog2Round
	ResizeUsec         bucketstats.BucketLog2Round
	RmdirUsec          bucketstats.BucketLog2Round
	SetstatUsec        bucketstats.BucketLog2Round
	SetXAttrUsec       bucketstats.BucketLog2Round
	StatVfsUsec        bucketstats.BucketLog2Round
	SymlinkUsec        bucketstats.BucketLog2Round
	UnlinkUsec         bucketstats.BucketLog2Round
	VolumeNameUsec     bucketstats.BucketLog2Round
	WriteUsec          bucketstats.BucketLog2Round
	WriteBytes         bucketstats.BucketLog2Round

	CreateErrors         bucketstats.Total
	FetchReadPlanErrors  bucketstats.Total
	FlushErrors          bucketstats.Total
	FlockOtherErrors     bucketstats.Total
	FlockGetErrors       bucketstats.Total
	FlockLockErrors      bucketstats.Total
	FlockUnlockErrors    bucketstats.Total
	GetstatErrors        bucketstats.Total
	GetTypeErrors        bucketstats.Total
	GetXAttrErrors       bucketstats.Total
	IsDirErrors          bucketstats.Total
	IsFileErrors         bucketstats.Total
	IsSymlinkErrors      bucketstats.Total
	LinkErrors           bucketstats.Total
	ListXAttrErrors      bucketstats.Total
	LookupErrors         bucketstats.Total
	LookupPathErrors     bucketstats.Total
	MkdirErrors          bucketstats.Total
	RemoveXAttrErrors    bucketstats.Total
	RenameErrors         bucketstats.Total
	ReadErrors           bucketstats.Total
	ReaddirErrors        bucketstats.Total
	ReaddirOneErrors     bucketstats.Total
	ReaddirOnePlusErrors bucketstats.Total
	ReaddirPlusErrors    bucketstats.Total
	ReadsymlinkErrors    bucketstats.Total
	ResizeErrors         bucketstats.Total
	RmdirErrors          bucketstats.Total
	SetstatErrors        bucketstats.Total
	SetXAttrErrors       bucketstats.Total
	StatVfsErrors        bucketstats.Total
	SymlinkErrors        bucketstats.Total
	UnlinkErrors         bucketstats.Total
	WriteErrors          bucketstats.Total

	FetchReadPlanUsec              bucketstats.BucketLog2Round
	CallInodeToProvisionObjectUsec bucketstats.BucketLog2Round
	MiddlewareCoalesceUsec         bucketstats.BucketLog2Round
	MiddlewareCoalesceBytes        bucketstats.BucketLog2Round
	MiddlewareDeleteUsec           bucketstats.BucketLog2Round
	MiddlewareGetAccountUsec       bucketstats.BucketLog2Round
	MiddlewareGetContainerUsec     bucketstats.BucketLog2Round
	MiddlewareGetObjectUsec        bucketstats.BucketLog2Round
	MiddlewareGetObjectBytes       bucketstats.BucketLog2Round
	MiddlewareHeadResponseUsec     bucketstats.BucketLog2Round
	MiddlewareMkdirUsec            bucketstats.BucketLog2Round
	MiddlewarePostUsec             bucketstats.BucketLog2Round
	MiddlewarePostBytes            bucketstats.BucketLog2Round
	MiddlewarePutCompleteUsec      bucketstats.BucketLog2Round
	MiddlewarePutCompleteBytes     bucketstats.BucketLog2Round
	MiddlewarePutContainerUsec     bucketstats.BucketLog2Round
	MiddlewarePutContainerBytes    bucketstats.BucketLog2Round

	CallInodeToProvisionObjectErrors bucketstats.Total
	MiddlewareCoalesceErrors         bucketstats.Total
	MiddlewareDeleteErrors           bucketstats.Total
	MiddlewareGetAccountErrors       bucketstats.Total
	MiddlewareGetContainerErrors     bucketstats.Total
	MiddlewareGetObjectErrors        bucketstats.Total
	MiddlewareHeadResponseErrors     bucketstats.Total
	MiddlewareMkdirErrors            bucketstats.Total
	MiddlewarePostErrors             bucketstats.Total
	MiddlewarePutCompleteErrors      bucketstats.Total
	MiddlewarePutContainerErrors     bucketstats.Total

	MountUsec                               bucketstats.BucketLog2Round
	MountErrors                             bucketstats.BucketLog2Round
	ValidateVolumeUsec                      bucketstats.BucketLog2Round
	ScrubVolumeUsec                         bucketstats.BucketLog2Round
	ValidateBaseNameUsec                    bucketstats.BucketLog2Round
	ValidateBaseNameErrors                  bucketstats.Total
	ValidateFullPathUsec                    bucketstats.BucketLog2Round
	ValidateFullPathErrors                  bucketstats.Total
	AccountNameToVolumeNameUsec             bucketstats.BucketLog2Round
	VolumeNameToActivePeerPrivateIPAddrUsec bucketstats.BucketLog2Round
}

var globals globalsStruct

func init() {
	transitions.Register("fs", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	bucketstats.Register("proxyfs.fs", "", &globals)

	globals.volumeMap = make(map[string]*volumeStruct)
	globals.mountMap = make(map[MountID]*mountStruct)
	globals.lastMountID = MountID(0)
	globals.inFlightFileInodeDataList = list.New()

	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

func (dummy *globalsStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		replayLogFileName string
		volume            *volumeStruct
		volumeSectionName string
	)

	volume = &volumeStruct{
		volumeName:               volumeName,
		FLockMap:                 make(map[inode.InodeNumber]*list.List),
		inFlightFileInodeDataMap: make(map[inode.InodeNumber]*inFlightFileInodeDataStruct),
		mountList:                make([]MountID, 0),
	}

	volumeSectionName = "Volume:" + volumeName

	replayLogFileName, err = confMap.FetchOptionValueString(volumeSectionName, "ReplayLogFileName")
	if nil == err {
		volume.doCheckpointPerFlush = ("" == replayLogFileName)
	} else {
		volume.doCheckpointPerFlush = true
	}

	logger.Infof("Checkpoint per Flush for volume %v is %v", volume.volumeName, volume.doCheckpointPerFlush)

	volume.maxFlushTime, err = confMap.FetchOptionValueDuration(volumeSectionName, "MaxFlushTime")
	if nil != err {
		return
	}

	volume.inodeVolumeHandle, err = inode.FetchVolumeHandle(volumeName)
	if nil != err {
		return
	}
	volume.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(volumeName)
	if nil != err {
		return
	}

	globals.volumeMap[volumeName] = volume

	return nil
}

func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		id     MountID
		ok     bool
		volume *volumeStruct
	)

	volume, ok = globals.volumeMap[volumeName]

	if !ok {
		err = nil
		return
	}

	for _, id = range volume.mountList {
		delete(globals.mountMap, id)
	}

	volume.untrackInFlightFileInodeDataAll()

	delete(globals.volumeMap, volumeName)

	err = nil
	return nil
}

func (dummy *globalsStruct) Signaled(confMap conf.ConfMap) (err error) {
	return nil
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	var (
		volume *volumeStruct
	)

	if 0 != len(globals.volumeMap) {
		err = fmt.Errorf("fs.Down() called with 0 != len(globals.volumeMap")
		return
	}
	if 0 != len(globals.mountMap) {
		err = fmt.Errorf("fs.Down() called with 0 != len(globals.mountMap")
		return
	}
	if 0 != globals.inFlightFileInodeDataList.Len() {
		err = fmt.Errorf("fs.Down() called with 0 != globals.inFlightFileInodeDataList.Len()")
		return
	}

	for _, volume = range globals.volumeMap {
		volume.untrackInFlightFileInodeDataAll()
	}

	if 0 < globals.inFlightFileInodeDataList.Len() {
		logger.Fatalf("fs.Down() has completed all un-mount's... but found non-empty globals.inFlightFileInodeDataList")
	}

	bucketstats.UnRegister("proxyfs.fs", "")

	err = nil
	return
}
