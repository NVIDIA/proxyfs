package fs

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
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
	id                     MountID
	options                MountOptions
	volStruct              *volumeStruct
	headhunterVolumeHandle headhunter.VolumeHandle
}

type volumeStruct struct {
	sync.Mutex
	volumeName               string
	doCheckpointPerFlush     bool
	maxFlushTime             time.Duration
	FLockMap                 map[inode.InodeNumber]*list.List
	inFlightFileInodeDataMap map[inode.InodeNumber]*inFlightFileInodeDataStruct
	mountList                []MountID
	inode.VolumeHandle
}

type globalsStruct struct {
	sync.Mutex
	whoAmI                    string
	volumeMap                 map[string]*volumeStruct
	mountMap                  map[MountID]*mountStruct
	lastMountID               MountID
	inFlightFileInodeDataList *list.List
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
		volumeSectionName = utils.VolumeNameConfSection(volumeName)

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
				flowControlSectionName = utils.FlowControlNameConfSection(flowControlName)

				volume.maxFlushTime, err = confMap.FetchOptionValueDuration(flowControlSectionName, "MaxFlushTime")
				if nil != err {
					return
				}

				volume.VolumeHandle, err = inode.FetchVolumeHandle(volumeName)
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

	swiftclient.SetStarvationCallbackFunc(chunkedPutConnectionPoolStarvationCallback)

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

	swiftclient.SetStarvationCallbackFunc(nil)

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
		primaryPeerList, err = confMap.FetchOptionValueStringSlice(utils.VolumeNameConfSection(volumeName), "PrimaryPeer")
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
		globals.Lock()
		delete(globals.volumeMap, volumeName)
		globals.Unlock()
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
		volumeSectionName = utils.VolumeNameConfSection(volumeName)

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
					flowControlSectionName = utils.FlowControlNameConfSection(flowControlName)

					volume.maxFlushTime, err = confMap.FetchOptionValueDuration(flowControlSectionName, "MaxFlushTime")
					if nil != err {
						return
					}

					volume.VolumeHandle, err = inode.FetchVolumeHandle(volumeName)
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

	swiftclient.SetStarvationCallbackFunc(chunkedPutConnectionPoolStarvationCallback)

	err = nil
	return
}

func Down() (err error) {
	var (
		volume *volumeStruct
	)

	swiftclient.SetStarvationCallbackFunc(nil)

	for _, volume = range globals.volumeMap {
		volume.untrackInFlightFileInodeDataAll()
	}

	if 0 < globals.inFlightFileInodeDataList.Len() {
		logger.Fatalf("fs.Down() has completed all un-mount's... but found non-empty globals.inFlightFileInodeDataList")
	}

	err = nil
	return
}
