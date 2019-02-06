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
	"github.com/swiftstack/ProxyFS/trackedlock"
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
	dataMutex                trackedlock.Mutex
	renameLock               trackedlock.Mutex
	volumeName               string
	doCheckpointPerFlush     bool
	maxFlushTime             time.Duration
	reportedBlockSize        uint64
	reportedFragmentSize     uint64
	reportedNumBlocks        uint64 // Used for Total, Free, and Avail
	reportedNumInodes        uint64 // Used for Total, Free, and Avail
	FLockMap                 map[inode.InodeNumber]*list.List
	inFlightFileInodeDataMap map[inode.InodeNumber]*inFlightFileInodeDataStruct
	mountList                []MountID
	jobRWMutex               trackedlock.RWMutex
	inodeVolumeHandle        inode.VolumeHandle
	headhunterVolumeHandle   headhunter.VolumeHandle
}

type globalsStruct struct {
	trackedlock.Mutex
	whoAmI                    string
	volumeMap                 map[string]*volumeStruct
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
	ReaddirPlusEntries bucketstats.BucketLog2Round
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

func Up(confMap conf.ConfMap) (err error) {
	var (
		flowControlName        string
		flowControlSectionName string
		primaryPeerList        []string
		replayLogFileName      string
		volume                 *volumeStruct
		volumeList             []string
		volumeName             string
		volumeSectionName      string
	)

	// register statistics
	bucketstats.Register("proxyfs.fs", "", &globals)

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"Cluster\", \"WhoAmI\") failed: %v", err)
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	globals.volumeMap = make(map[string]*volumeStruct)

	for _, volumeName = range volumeList {
		volumeSectionName = "Volume:" + volumeName

		primaryPeerList, err = confMap.FetchOptionValueStringSlice(volumeSectionName, "PrimaryPeer")
		if nil != err {
			err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"%s\", \"PrimaryPeer\") failed: %v", volumeName, err)
			return
		}

		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if globals.whoAmI == primaryPeerList[0] {
				volume = &volumeStruct{
					volumeName:               volumeName,
					FLockMap:                 make(map[inode.InodeNumber]*list.List),
					inFlightFileInodeDataMap: make(map[inode.InodeNumber]*inFlightFileInodeDataStruct),
					mountList:                make([]MountID, 0),
				}

				replayLogFileName, err = confMap.FetchOptionValueString(volumeSectionName, "ReplayLogFileName")
				if nil == err {
					volume.doCheckpointPerFlush = ("" == replayLogFileName)
				} else {
					volume.doCheckpointPerFlush = true
				}
				logger.Infof("Checkpoint per Flush for volume %v is %v", volume.volumeName, volume.doCheckpointPerFlush)

				flowControlName, err = confMap.FetchOptionValueString(volumeSectionName, "FlowControl")
				if nil != err {
					return
				}
				flowControlSectionName = "FlowControl:" + flowControlName

				volume.maxFlushTime, err = confMap.FetchOptionValueDuration(flowControlSectionName, "MaxFlushTime")
				if nil != err {
					return
				}

				volume.reportedBlockSize, err = confMap.FetchOptionValueUint64(volumeSectionName, "ReportedBlockSize")
				if nil != err {
					volume.reportedBlockSize = DefaultReportedBlockSize // TODO: Eventually, just return
				}
				volume.reportedFragmentSize, err = confMap.FetchOptionValueUint64(volumeSectionName, "ReportedFragmentSize")
				if nil != err {
					volume.reportedFragmentSize = DefaultReportedFragmentSize // TODO: Eventually, just return
				}
				volume.reportedNumBlocks, err = confMap.FetchOptionValueUint64(volumeSectionName, "ReportedNumBlocks")
				if nil != err {
					volume.reportedNumBlocks = DefaultReportedNumBlocks // TODO: Eventually, just return
				}
				volume.reportedNumInodes, err = confMap.FetchOptionValueUint64(volumeSectionName, "ReportedNumInodes")
				if nil != err {
					volume.reportedNumInodes = DefaultReportedNumInodes // TODO: Eventually, just return
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
			}
		} else {
			err = fmt.Errorf("%v.PrimaryPeer cannot be multi-valued", volumeName)
			return
		}
	}

	globals.mountMap = make(map[MountID]*mountStruct)
	globals.lastMountID = MountID(0)
	globals.inFlightFileInodeDataList = list.New()

	return
}

func PauseAndContract(confMap conf.ConfMap) (err error) {
	var (
		id                MountID
		ok                bool
		primaryPeerList   []string
		removedVolumeList []string
		updatedVolumeMap  map[string]bool // key == volumeName; value ignored
		volume            *volumeStruct
		volumeList        []string
		volumeName        string
		whoAmI            string
	)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"Cluster\", \"WhoAmI\") failed: %v", err)
		return
	}
	if whoAmI != globals.whoAmI {
		err = fmt.Errorf("confMap change not allowed to alter [Cluster]WhoAmI")
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	updatedVolumeMap = make(map[string]bool)

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice("Volume:"+volumeName, "PrimaryPeer")
		if nil != err {
			err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"%s\", \"PrimaryPeer\") failed: %v", volumeName, err)
			return
		}

		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if globals.whoAmI == primaryPeerList[0] {
				updatedVolumeMap[volumeName] = true
			}
		} else {
			err = fmt.Errorf("%v.PrimaryPeer cannot be multi-valued", volumeName)
			return
		}
	}

	removedVolumeList = make([]string, 0, len(globals.volumeMap))

	for volumeName = range globals.volumeMap {
		_, ok = updatedVolumeMap[volumeName]
		if !ok {
			removedVolumeList = append(removedVolumeList, volumeName)
		}
	}

	for _, volumeName = range removedVolumeList {
		volume = globals.volumeMap[volumeName]
		for _, id = range volume.mountList {
			delete(globals.mountMap, id)
		}
		volume.untrackInFlightFileInodeDataAll()
		delete(globals.volumeMap, volumeName)
	}

	err = nil
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	var (
		flowControlName        string
		flowControlSectionName string
		ok                     bool
		primaryPeerList        []string
		replayLogFileName      string
		volume                 *volumeStruct
		volumeList             []string
		volumeName             string
		volumeSectionName      string
		whoAmI                 string
	)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"Cluster\", \"WhoAmI\") failed: %v", err)
		return
	}
	if whoAmI != globals.whoAmI {
		err = fmt.Errorf("confMap change not allowed to alter [Cluster]WhoAmI")
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	for _, volumeName = range volumeList {
		volumeSectionName = "Volume:" + volumeName

		primaryPeerList, err = confMap.FetchOptionValueStringSlice(volumeSectionName, "PrimaryPeer")
		if nil != err {
			err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"%s\", \"PrimaryPeer\") failed: %v", volumeName, err)
			return
		}

		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if globals.whoAmI == primaryPeerList[0] {
				volume, ok = globals.volumeMap[volumeName]
				if !ok {
					volume = &volumeStruct{
						volumeName:               volumeName,
						FLockMap:                 make(map[inode.InodeNumber]*list.List),
						inFlightFileInodeDataMap: make(map[inode.InodeNumber]*inFlightFileInodeDataStruct),
						mountList:                make([]MountID, 0),
					}

					replayLogFileName, err = confMap.FetchOptionValueString(volumeSectionName, "ReplayLogFileName")
					if nil == err {
						volume.doCheckpointPerFlush = ("" == replayLogFileName)
					} else {
						volume.doCheckpointPerFlush = true
					}
					logger.Infof("Checkpoint per Flush for volume %v is %v", volume.volumeName, volume.doCheckpointPerFlush)

					flowControlName, err = confMap.FetchOptionValueString(volumeSectionName, "FlowControl")
					if nil != err {
						return
					}
					flowControlSectionName = "FlowControl:" + flowControlName

					volume.maxFlushTime, err = confMap.FetchOptionValueDuration(flowControlSectionName, "MaxFlushTime")
					if nil != err {
						return
					}

					volume.reportedBlockSize, err = confMap.FetchOptionValueUint64(volumeSectionName, "ReportedBlockSize")
					if nil != err {
						volume.reportedBlockSize = DefaultReportedBlockSize // TODO: Eventually, just return
					}
					volume.reportedFragmentSize, err = confMap.FetchOptionValueUint64(volumeSectionName, "ReportedFragmentSize")
					if nil != err {
						volume.reportedFragmentSize = DefaultReportedFragmentSize // TODO: Eventually, just return
					}
					volume.reportedNumBlocks, err = confMap.FetchOptionValueUint64(volumeSectionName, "ReportedNumBlocks")
					if nil != err {
						volume.reportedNumBlocks = DefaultReportedNumBlocks // TODO: Eventually, just return
					}
					volume.reportedNumInodes, err = confMap.FetchOptionValueUint64(volumeSectionName, "ReportedNumInodes")
					if nil != err {
						volume.reportedNumInodes = DefaultReportedNumInodes // TODO: Eventually, just return
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
				}
			}
		} else {
			err = fmt.Errorf("%v.PrimaryPeer cannot be multi-valued", volumeName)
			return
		}
	}

	err = nil
	return
}

func Down() (err error) {
	var (
		volume *volumeStruct
	)

	for _, volume = range globals.volumeMap {
		volume.untrackInFlightFileInodeDataAll()
	}

	if 0 < globals.inFlightFileInodeDataList.Len() {
		logger.Fatalf("fs.Down() has completed all un-mount's... but found non-empty globals.inFlightFileInodeDataList")
	}

	// unregister statistics
	bucketstats.UnRegister("proxyfs.fs", "")

	err = nil
	return
}
