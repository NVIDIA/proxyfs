package inode

import (
	"fmt"
	"sync"
	"time"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/platform"
	"github.com/swiftstack/ProxyFS/utils"
)

type physicalContainerLayoutStruct struct {
	physicalContainerLayoutName         string
	physicalContainerStoragePolicy      string
	physicalContainerNamePrefix         string   // == prefix for every PhysicalContainer in this PhysicalContainerLayout
	physicalContainerNameSlice          []string // == slice of current PhysicalContainers for this PhysicalContainerLayout
	physicalContainerCountMax           uint64   // [<LayoutSectionName>]ContainersPerPeer
	physicalObjectCountMax              uint64   // [<LayoutSectionName>]MaxObjectsPerContainer
	physicalContainerNameSliceNextIndex uint64   // == next index in physicalContainerNameSlice
	physicalContainerNameSliceLoopCount uint64   // == number of times looped through physicalContainerNameSlice
	//                                                 Note: need to re-provision indexed Container if 0 == (physicalContainerNameSliceLoopCount mod physicalContainerCountMax)
}

type readCacheKeyStruct struct {
	volumeName       string
	logSegmentNumber uint64
	cacheLineTag     uint64 // LogSegment offset / readCacheLineSize
}

type readCacheElementStruct struct {
	readCacheKey readCacheKeyStruct
	next         *readCacheElementStruct // nil if MRU element of flowControlStruct.readCache
	prev         *readCacheElementStruct // nil if LRU element of flowControlStruct.readCache
	cacheLine    []byte
}

type flowControlStruct struct {
	sync.Mutex
	flowControlName    string //     == [volume-section]FlowControl (<flow-control-section>)
	refCount           uint32
	maxFlushSize       uint64
	maxFlushTime       time.Duration
	readCacheLineSize  uint64
	readCacheWeight    uint64
	readCacheLineCount uint64
	readCache          map[readCacheKeyStruct]*readCacheElementStruct
	readCacheMRU       *readCacheElementStruct
	readCacheLRU       *readCacheElementStruct
}

type volumeStruct struct {
	sync.Mutex
	fsid                           uint64
	volumeName                     string
	accountName                    string
	active                         bool
	activePeerPrivateIPAddr        string
	maxEntriesPerDirNode           uint64
	maxExtentsPerFileNode          uint64
	physicalContainerLayoutSet     map[string]struct{}                       // key == physicalContainerLayoutStruct.physicalContainerLayoutName
	physicalContainerNamePrefixSet map[string]struct{}                       // key == physicalContainerLayoutStruct.physicalContainerNamePrefix
	physicalContainerLayoutMap     map[string]*physicalContainerLayoutStruct // key == physicalContainerLayoutStruct.physicalContainerLayoutName
	defaultPhysicalContainerLayout *physicalContainerLayoutStruct
	flowControl                    *flowControlStruct
	headhunterVolumeHandle         headhunter.VolumeHandle
	inodeCache                     map[InodeNumber]*inMemoryInodeStruct //      key == InodeNumber
}

type globalsStruct struct {
	sync.Mutex
	whoAmI                       string
	myPrivateIPAddr              string
	dirEntryCache                sortedmap.BPlusTreeCache
	fileExtentMapCache           sortedmap.BPlusTreeCache
	volumeMap                    map[string]*volumeStruct      // key == volumeStruct.volumeName
	accountMap                   map[string]*volumeStruct      // key == volumeStruct.accountName
	flowControlMap               map[string]*flowControlStruct // key == flowControlStruct.flowControlName
	fileExtentStructSize         uint64                        // pre-calculated size of cstruct-packed fileExtentStruct
	supportedOnDiskInodeVersions map[Version]struct{}          // key == on disk inode version
	corruptionDetectedTrueBuf    []byte                        // holds serialized CorruptionDetected == true
	corruptionDetectedFalseBuf   []byte                        // holds serialized CorruptionDetected == false
	versionV1Buf                 []byte                        // holds serialized Version            == V1
	inodeRecDefaultPreambleBuf   []byte                        // holds concatenated corruptionDetectedFalseBuf & versionV1Buf
}

var globals globalsStruct

func Up(confMap conf.ConfMap) (err error) {
	var (
		alreadyInAccountMap                            bool
		alreadyInFlowControlMap                        bool
		alreadyInGlobalsPhysicalContainerLayoutSet     bool
		alreadyInGlobalsPhysicalContainerNamePrefixSet bool
		alreadyInVolumeMap                             bool
		alreadyInVolumePhysicalContainerLayoutMap      bool
		corruptionDetectedFalse                        = CorruptionDetected(false)
		corruptionDetectedTrue                         = CorruptionDetected(true)
		defaultPhysicalContainerLayoutName             string
		dirEntryCacheEvictHighLimit                    uint64
		dirEntryCacheEvictLowLimit                     uint64
		fileExtentMapEvictHighLimit                    uint64
		fileExtentMapEvictLowLimit                     uint64
		flowControl                                    *flowControlStruct
		flowControlName                                string
		flowControlSectionName                         string
		flowControlWeightSum                           uint64
		ok                                             bool
		peerName                                       string
		peerNames                                      []string
		peerPrivateIPAddr                              string
		peerPrivateIPAddrMap                           map[string]string
		physicalContainerLayout                        *physicalContainerLayoutStruct
		physicalContainerLayoutName                    string
		physicalContainerLayoutNameSlice               []string
		physicalContainerLayoutSectionName             string
		prevVolume                                     *volumeStruct
		primaryPeerNameList                            []string
		readCacheMemSize                               uint64
		readCacheQuotaPercentage                       uint64
		readCacheTotalSize                             uint64
		versionV1                                      = Version(V1)
		volume                                         *volumeStruct
		volumeList                                     []string
		volumeName                                     string
		volumeSectionName                              string
	)

	peerPrivateIPAddrMap = make(map[string]string)

	peerNames, err = confMap.FetchOptionValueStringSlice("Cluster", "Peers")
	if nil != err {
		return
	}

	for _, peerName = range peerNames {
		peerPrivateIPAddr, err = confMap.FetchOptionValueString(utils.PeerNameConfSection(peerName), "PrivateIPAddr")
		if nil != err {
			return
		}

		peerPrivateIPAddrMap[peerName] = peerPrivateIPAddr
	}

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}
	globals.myPrivateIPAddr, ok = peerPrivateIPAddrMap[globals.whoAmI]
	if !ok {
		err = fmt.Errorf("Cluster.WhoAmI (\"%v\") not in Cluster.Peers list", globals.whoAmI)
		return
	}

	dirEntryCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "DirEntryCacheEvictLowLimit")
	if nil != err {
		// TODO: eventually, just return
		dirEntryCacheEvictLowLimit = 10000
	}
	dirEntryCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "DirEntryCacheEvictHighLimit")
	if nil != err {
		// TODO: eventually, just return
		dirEntryCacheEvictHighLimit = 10010
	}

	globals.dirEntryCache = sortedmap.NewBPlusTreeCache(dirEntryCacheEvictLowLimit, dirEntryCacheEvictHighLimit)

	fileExtentMapEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "FileExtentMapEvictLowLimit")
	if nil != err {
		// TODO: eventually, just return
		fileExtentMapEvictLowLimit = 10000
	}
	fileExtentMapEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "FileExtentMapEvictHighLimit")
	if nil != err {
		// TODO: eventually, just return
		fileExtentMapEvictHighLimit = 10010
	}

	globals.fileExtentMapCache = sortedmap.NewBPlusTreeCache(fileExtentMapEvictLowLimit, fileExtentMapEvictHighLimit)

	globals.volumeMap = make(map[string]*volumeStruct)
	globals.accountMap = make(map[string]*volumeStruct)
	globals.flowControlMap = make(map[string]*flowControlStruct)

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	for _, volumeName = range volumeList {
		volumeSectionName = utils.VolumeNameConfSection(volumeName)

		volume = &volumeStruct{
			volumeName:                     volumeName,
			physicalContainerLayoutSet:     make(map[string]struct{}),
			physicalContainerNamePrefixSet: make(map[string]struct{}),
			physicalContainerLayoutMap:     make(map[string]*physicalContainerLayoutStruct),
			inodeCache:                     make(map[InodeNumber]*inMemoryInodeStruct),
		}

		volume.fsid, err = confMap.FetchOptionValueUint64(volumeSectionName, "FSID")
		if nil != err {
			return
		}

		for _, prevVolume = range globals.volumeMap {
			if volume.fsid == prevVolume.fsid {
				err = fmt.Errorf("Volume \"%v\" duplicates FSID (%v) of volume \"%v\"", volume.volumeName, volume.fsid, prevVolume.volumeName)
				return
			}
		}

		volume.accountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
		if nil != err {
			return
		}

		_, alreadyInVolumeMap = globals.volumeMap[volume.volumeName]
		if alreadyInVolumeMap {
			err = fmt.Errorf("Volume \"%v\" only allowed once in [FSGlobals]VolumeList", volume.volumeName)
			return
		}

		_, alreadyInAccountMap = globals.accountMap[volume.accountName]
		if alreadyInAccountMap {
			err = fmt.Errorf("Account \"%v\" only allowed once in [FSGlobals]VolumeList", volume.accountName)
			return
		}

		primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(volumeSectionName, "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerNameList) {
			volume.active = false
			volume.activePeerPrivateIPAddr = ""
		} else if 1 == len(primaryPeerNameList) {
			volume.active = (globals.whoAmI == primaryPeerNameList[0])
			volume.activePeerPrivateIPAddr, ok = peerPrivateIPAddrMap[primaryPeerNameList[0]]
			if !ok {
				err = fmt.Errorf("Volume \"%v\" specifies unknown PrimaryPeer \"%v\"", volumeSectionName, primaryPeerNameList[0])
				return
			}
		} else {
			err = fmt.Errorf("%s.PrimaryPeer cannot have multiple values", volumeSectionName)
			return
		}

		if volume.active {
			volume.maxEntriesPerDirNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxEntriesPerDirNode")
			if nil != err {
				return
			}

			volume.maxExtentsPerFileNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxExtentsPerFileNode")
			if nil != err {
				return
			}

			// [Case 1] For now, physicalContainerLayoutNameSlice will simply contain only defaultPhysicalContainerLayoutName
			//
			// The expectation is that, at some point, multiple container layouts may be supported along with
			// a set of policies used to determine which one to apply. At such time, the following code will
			// ensure that the container layouts don't conflict (obviously not a problem when there is only one).

			defaultPhysicalContainerLayoutName, err = confMap.FetchOptionValueString(volumeSectionName, "DefaultPhysicalContainerLayout")
			if nil != err {
				return
			}

			physicalContainerLayoutNameSlice = []string{defaultPhysicalContainerLayoutName}

			for _, physicalContainerLayoutName = range physicalContainerLayoutNameSlice {
				_, alreadyInGlobalsPhysicalContainerLayoutSet = volume.physicalContainerLayoutSet[physicalContainerLayoutName]
				if alreadyInGlobalsPhysicalContainerLayoutSet {
					err = fmt.Errorf("PhysicalContainerLayout \"%v\" only allowed once", physicalContainerLayoutName)
					return
				}

				physicalContainerLayout = &physicalContainerLayoutStruct{}

				physicalContainerLayout.physicalContainerLayoutName = physicalContainerLayoutName

				physicalContainerLayoutSectionName = utils.PhysicalContainerLayoutNameConfSection(physicalContainerLayoutName)

				physicalContainerLayout.physicalContainerStoragePolicy, err = confMap.FetchOptionValueString(physicalContainerLayoutSectionName, "ContainerStoragePolicy")
				if nil != err {
					return
				}

				physicalContainerLayout.physicalContainerNamePrefix, err = confMap.FetchOptionValueString(physicalContainerLayoutSectionName, "ContainerNamePrefix")
				if nil != err {
					return
				}
				_, alreadyInGlobalsPhysicalContainerNamePrefixSet = volume.physicalContainerLayoutSet[physicalContainerLayout.physicalContainerNamePrefix]
				if alreadyInGlobalsPhysicalContainerNamePrefixSet {
					err = fmt.Errorf("ContainerNamePrefix \"%v\" only allowed once", physicalContainerLayout.physicalContainerNamePrefix)
					return
				}

				physicalContainerLayout.physicalContainerCountMax, err = confMap.FetchOptionValueUint64(physicalContainerLayoutSectionName, "ContainersPerPeer")
				if nil != err {
					return
				}

				physicalContainerLayout.physicalObjectCountMax, err = confMap.FetchOptionValueUint64(physicalContainerLayoutSectionName, "MaxObjectsPerContainer")
				if nil != err {
					return
				}

				physicalContainerLayout.physicalContainerNameSlice = make([]string, physicalContainerLayout.physicalContainerCountMax)

				physicalContainerLayout.physicalContainerNameSliceNextIndex = 0
				physicalContainerLayout.physicalContainerNameSliceLoopCount = 0

				volume.physicalContainerLayoutMap[physicalContainerLayoutName] = physicalContainerLayout

				volume.physicalContainerLayoutSet[physicalContainerLayoutName] = struct{}{}
				volume.physicalContainerNamePrefixSet[physicalContainerLayout.physicalContainerNamePrefix] = struct{}{}
			}

			volume.defaultPhysicalContainerLayout, alreadyInVolumePhysicalContainerLayoutMap = volume.physicalContainerLayoutMap[defaultPhysicalContainerLayoutName]
			if !alreadyInVolumePhysicalContainerLayoutMap {
				err = fmt.Errorf("DefaultPhysicalContainerLayout \"%v\" must be in [%v]PhysicalContaonerLayoutList", defaultPhysicalContainerLayoutName, volumeSectionName)
				return
			}

			flowControlName, err = confMap.FetchOptionValueString(volumeSectionName, "FlowControl")
			if nil != err {
				return
			}
			flowControlSectionName = utils.FlowControlNameConfSection(flowControlName)

			_, alreadyInFlowControlMap = globals.flowControlMap[flowControlName]

			if !alreadyInFlowControlMap {
				flowControl = &flowControlStruct{
					flowControlName: flowControlName,
					refCount:        0,
					readCache:       make(map[readCacheKeyStruct]*readCacheElementStruct),
					readCacheMRU:    nil,
					readCacheLRU:    nil,
				}

				flowControl.maxFlushSize, err = confMap.FetchOptionValueUint64(flowControlSectionName, "MaxFlushSize")
				if nil != err {
					return
				}

				flowControl.maxFlushTime, err = confMap.FetchOptionValueDuration(flowControlSectionName, "MaxFlushTime")
				if nil != err {
					return
				}

				flowControl.readCacheLineSize, err = confMap.FetchOptionValueUint64(flowControlSectionName, "ReadCacheLineSize")
				if nil != err {
					return
				}

				flowControl.readCacheWeight, err = confMap.FetchOptionValueUint64(flowControlSectionName, "ReadCacheWeight")
				if nil != err {
					return
				}

				globals.flowControlMap[flowControlName] = flowControl
			}

			volume.flowControl = globals.flowControlMap[flowControlName]
			volume.flowControl.refCount++

			volume.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(volume.volumeName)
			if nil != err {
				return
			}
		}

		globals.volumeMap[volume.volumeName] = volume
		globals.accountMap[volume.accountName] = volume
	}

	for _, flowControl = range globals.flowControlMap {
		flowControlWeightSum += flowControl.readCacheWeight
	}

	readCacheQuotaPercentage, err = confMap.FetchOptionValueFloatScaledToUint64(utils.PeerNameConfSection(globals.whoAmI), "ReadCacheQuotaFraction", 100)
	if nil != err {
		return
	}
	if 100 < readCacheQuotaPercentage {
		err = fmt.Errorf("%s.ReadCacheQuotaFraction must be no greater than 1", globals.whoAmI)
		return
	}

	readCacheMemSize = platform.MemSize() * readCacheQuotaPercentage / 100

	for _, flowControl = range globals.flowControlMap {
		readCacheTotalSize = readCacheMemSize * flowControl.readCacheWeight / flowControlWeightSum

		flowControl.readCacheLineCount = readCacheTotalSize / flowControl.readCacheLineSize
		if 0 == flowControl.readCacheLineCount {
			err = fmt.Errorf("[\"%v\"]ReadCacheWeight must result in at least one ReadCacheLineSize (%v) of memory", flowControl.flowControlName, flowControl.readCacheLineSize)
			return
		}
	}

	globals.fileExtentStructSize, _, err = cstruct.Examine(fileExtentStruct{})
	if nil != err {
		return
	}

	globals.supportedOnDiskInodeVersions = make(map[Version]struct{})

	globals.supportedOnDiskInodeVersions[V1] = struct{}{}

	globals.corruptionDetectedTrueBuf, err = cstruct.Pack(corruptionDetectedTrue, cstruct.LittleEndian)
	if nil != err {
		return
	}
	globals.corruptionDetectedFalseBuf, err = cstruct.Pack(corruptionDetectedFalse, cstruct.LittleEndian)
	if nil != err {
		return
	}
	globals.versionV1Buf, err = cstruct.Pack(versionV1, cstruct.LittleEndian)
	if nil != err {
		return
	}

	globals.inodeRecDefaultPreambleBuf = make([]byte, 0, len(globals.corruptionDetectedFalseBuf)+len(globals.versionV1Buf))
	globals.inodeRecDefaultPreambleBuf = append(globals.inodeRecDefaultPreambleBuf, globals.corruptionDetectedFalseBuf...)
	globals.inodeRecDefaultPreambleBuf = append(globals.inodeRecDefaultPreambleBuf, globals.versionV1Buf...)

	err = nil
	return
}

func PauseAndContract(confMap conf.ConfMap) (err error) {
	var (
		myPrivateIPAddr         string
		newVolumeSet            map[string]bool
		ok                      bool
		peerName                string
		peerNames               []string
		primaryPeerNameList     []string
		peerPrivateIPAddr       string
		peerPrivateIPAddrMap    map[string]string
		volume                  *volumeStruct
		volumeList              []string
		volumeName              string
		volumesDeletedSet       map[string]bool
		volumesNewlyInactiveSet map[string]bool
		whoAmI                  string
	)

	peerPrivateIPAddrMap = make(map[string]string)

	peerNames, err = confMap.FetchOptionValueStringSlice("Cluster", "Peers")
	if nil != err {
		return
	}

	for _, peerName = range peerNames {
		peerPrivateIPAddr, err = confMap.FetchOptionValueString(utils.PeerNameConfSection(peerName), "PrivateIPAddr")
		if nil != err {
			return
		}

		peerPrivateIPAddrMap[peerName] = peerPrivateIPAddr
	}

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}
	if whoAmI != globals.whoAmI {
		err = fmt.Errorf("confMap change not allowed to alter [Cluster]WhoAmI")
		return
	}
	myPrivateIPAddr, ok = peerPrivateIPAddrMap[globals.whoAmI]
	if !ok {
		err = fmt.Errorf("Cluster.WhoAmI (\"%v\") not in Cluster.Peers list", globals.whoAmI)
		return
	}
	if myPrivateIPAddr != globals.myPrivateIPAddr {
		err = fmt.Errorf("confMap change not allowed to alter [<Cluster.WhoAmI>]PrivateIPAddr")
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	newVolumeSet = make(map[string]bool)

	for _, volumeName = range volumeList {
		newVolumeSet[volumeName] = true
	}

	volumesDeletedSet = make(map[string]bool)
	volumesNewlyInactiveSet = make(map[string]bool)

	for volumeName, volume = range globals.volumeMap {
		_, ok = newVolumeSet[volumeName]
		if ok {
			primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(utils.VolumeNameConfSection(volumeName), "PrimaryPeer")
			if nil != err {
				return
			}
			if 0 == len(primaryPeerNameList) {
				if volume.active {
					volumesNewlyInactiveSet[volumeName] = true
				}
			} else if 1 == len(primaryPeerNameList) {
				if volume.active {
					if whoAmI != primaryPeerNameList[0] {
						volumesNewlyInactiveSet[volumeName] = true
					}
				}
			} else {
				err = fmt.Errorf("%s.PrimaryPeer cannot have multiple values", volumeName)
				return
			}
		} else {
			volumesDeletedSet[volumeName] = true
		}
	}

	for volumeName = range volumesDeletedSet {
		volume = globals.volumeMap[volumeName]
		volume.flowControl.refCount--
		if 0 == volume.flowControl.refCount {
			delete(globals.flowControlMap, volume.flowControl.flowControlName)
		}
		delete(globals.volumeMap, volumeName)
	}

	for volumeName = range volumesNewlyInactiveSet {
		volume = globals.volumeMap[volumeName]
		volume.active = false
		primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(utils.VolumeNameConfSection(volumeName), "PrimaryPeer")
		if nil != err {
			return
		}
		if 0 == len(primaryPeerNameList) {
			volume.activePeerPrivateIPAddr = ""
		} else if 1 == len(primaryPeerNameList) {
			volume.activePeerPrivateIPAddr, ok = peerPrivateIPAddrMap[primaryPeerNameList[0]]
			if !ok {
				err = fmt.Errorf("Volume \"%v\" specifies unknown PrimaryPeer \"%v\"", volumeName, primaryPeerNameList[0])
				return
			}
		} else {
			err = fmt.Errorf("%s.PrimaryPeer cannot have multiple values", volumeName)
			return
		}
		volume.physicalContainerLayoutSet = make(map[string]struct{})
		volume.physicalContainerNamePrefixSet = make(map[string]struct{})
		volume.physicalContainerLayoutMap = make(map[string]*physicalContainerLayoutStruct)
		volume.defaultPhysicalContainerLayout = nil
		volume.flowControl.refCount--
		if 0 == volume.flowControl.refCount {
			delete(globals.flowControlMap, volume.flowControl.flowControlName)
		}
		volume.flowControl = nil
		volume.inodeCache = make(map[InodeNumber]*inMemoryInodeStruct)
	}

	err = nil
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	var (
		accountName                                    string
		active                                         bool
		activePeerPrivateIPAddr                        string
		alreadyInAccountMap                            bool
		alreadyInFlowControlMap                        bool
		alreadyInGlobalsPhysicalContainerLayoutSet     bool
		alreadyInGlobalsPhysicalContainerNamePrefixSet bool
		alreadyInVolumePhysicalContainerLayoutMap      bool
		defaultPhysicalContainerLayoutName             string
		flowControlName                                string
		flowControlSectionName                         string
		flowControl                                    *flowControlStruct
		flowControlWeightSum                           uint64
		fsid                                           uint64
		newlyActiveVolumeSet                           map[string]*volumeStruct
		ok                                             bool
		peerName                                       string
		peerNames                                      []string
		peerPrivateIPAddr                              string
		peerPrivateIPAddrMap                           map[string]string
		physicalContainerLayout                        *physicalContainerLayoutStruct
		physicalContainerLayoutName                    string
		physicalContainerLayoutNameSlice               []string
		physicalContainerLayoutSectionName             string
		prevVolume                                     *volumeStruct
		primaryPeerNameList                            []string
		readCacheLineSize                              uint64
		readCacheMemSize                               uint64
		readCacheQuotaPercentage                       uint64
		readCacheTotalSize                             uint64
		volume                                         *volumeStruct
		volumeList                                     []string
		volumeName                                     string
		volumeSectionName                              string
	)

	peerPrivateIPAddrMap = make(map[string]string)

	peerNames, err = confMap.FetchOptionValueStringSlice("Cluster", "Peers")
	if nil != err {
		return
	}

	for _, peerName = range peerNames {
		peerPrivateIPAddr, err = confMap.FetchOptionValueString(utils.PeerNameConfSection(peerName), "PrivateIPAddr")
		if nil != err {
			return
		}

		peerPrivateIPAddrMap[peerName] = peerPrivateIPAddr
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	for _, volumeName = range volumeList {
		volumeSectionName = utils.VolumeNameConfSection(volumeName)

		fsid, err = confMap.FetchOptionValueUint64(volumeSectionName, "FSID")
		if nil != err {
			return
		}

		accountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
		if nil != err {
			return
		}

		primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(volumeSectionName, "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerNameList) {
			active = false
			activePeerPrivateIPAddr = ""
		} else if 1 == len(primaryPeerNameList) {
			active = (globals.whoAmI == primaryPeerNameList[0])
			activePeerPrivateIPAddr, ok = peerPrivateIPAddrMap[primaryPeerNameList[0]]
			if !ok {
				err = fmt.Errorf("Volume \"%v\" specifies unknown PrimaryPeer \"%v\"", volumeName, primaryPeerNameList[0])
				return
			}
		} else {
			err = fmt.Errorf("%s.PrimaryPeer cannot have multiple values", volumeName)
			return
		}

		newlyActiveVolumeSet = make(map[string]*volumeStruct)

		volume, ok = globals.volumeMap[volumeName]
		if ok { // previously known volumeName
			if fsid != volume.fsid {
				err = fmt.Errorf("Volume \"%v\" changed its FSID", volumeName)
				return
			}

			if accountName != volume.accountName {
				err = fmt.Errorf("Volume \"%v\" changed its AccountName", volumeName)
				return
			}

			volume.activePeerPrivateIPAddr = activePeerPrivateIPAddr

			if active {
				if volume.active { // also previously active
					flowControlName, err = confMap.FetchOptionValueString(volumeSectionName, "FlowControl")
					if nil != err {
						return
					}
					flowControlSectionName = utils.FlowControlNameConfSection(flowControlName)

					flowControl = volume.flowControl

					if flowControlName == flowControl.flowControlName {
						flowControl.maxFlushSize, err = confMap.FetchOptionValueUint64(flowControlSectionName, "MaxFlushSize")
						if nil != err {
							return
						}

						flowControl.maxFlushTime, err = confMap.FetchOptionValueDuration(flowControlSectionName, "MaxFlushTime")
						if nil != err {
							return
						}

						readCacheLineSize, err = confMap.FetchOptionValueUint64(flowControlSectionName, "ReadCacheLineSize")
						if nil != err {
							return
						}
						if readCacheLineSize != flowControl.readCacheLineSize {
							err = fmt.Errorf("FlowControl \"%v\" changed its ReadCacheLineSize", flowControl.flowControlName)
							return
						}

						flowControl.readCacheWeight, err = confMap.FetchOptionValueUint64(flowControlSectionName, "ReadCacheWeight")
						if nil != err {
							return
						}

					} else {
						err = fmt.Errorf("Volume \"%v\" changed its FlowControl name", volumeName)
						return
					}
				} else { // newly active
					volume.active = true
					newlyActiveVolumeSet[volumeName] = volume
				}
			}
		} else { // previously unknown volumeName
			for _, prevVolume = range globals.volumeMap {
				if fsid == prevVolume.fsid {
					err = fmt.Errorf("Volume \"%v\" duplicates FSID (%v) of volume \"%v\"", volumeName, fsid, prevVolume.volumeName)
					return
				}
			}

			_, alreadyInAccountMap = globals.accountMap[volumeName]
			if alreadyInAccountMap {
				err = fmt.Errorf("Account \"%v\" only allowed once in [FSGlobals]VolumeList", accountName)
				return
			}

			volume = &volumeStruct{
				fsid:                           fsid,
				volumeName:                     volumeName,
				accountName:                    accountName,
				active:                         active,
				activePeerPrivateIPAddr:        activePeerPrivateIPAddr,
				physicalContainerLayoutSet:     make(map[string]struct{}),
				physicalContainerNamePrefixSet: make(map[string]struct{}),
				physicalContainerLayoutMap:     make(map[string]*physicalContainerLayoutStruct),
				inodeCache:                     make(map[InodeNumber]*inMemoryInodeStruct),
			}

			globals.volumeMap[volume.volumeName] = volume
			globals.accountMap[volume.accountName] = volume

			if active {
				newlyActiveVolumeSet[volumeName] = volume
			}
		}

		for _, volume = range newlyActiveVolumeSet {
			// [Case 2] For now, physicalContainerLayoutNameSlice will simply contain only defaultPhysicalContainerLayoutName
			//
			// The expectation is that, at some point, multiple container layouts may be supported along with
			// a set of policies used to determine which one to apply. At such time, the following code will
			// ensure that the container layouts don't conflict (obviously not a problem when there is only one).

			volumeSectionName = utils.VolumeNameConfSection(volume.volumeName)

			volume.maxEntriesPerDirNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxEntriesPerDirNode")
			if nil != err {
				return
			}

			volume.maxExtentsPerFileNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxExtentsPerFileNode")
			if nil != err {
				return
			}

			defaultPhysicalContainerLayoutName, err = confMap.FetchOptionValueString(volumeSectionName, "DefaultPhysicalContainerLayout")
			if nil != err {
				return
			}

			physicalContainerLayoutNameSlice = []string{defaultPhysicalContainerLayoutName}

			for _, physicalContainerLayoutName = range physicalContainerLayoutNameSlice {
				_, alreadyInGlobalsPhysicalContainerLayoutSet = volume.physicalContainerLayoutSet[physicalContainerLayoutName]
				if alreadyInGlobalsPhysicalContainerLayoutSet {
					err = fmt.Errorf("PhysicalContainerLayout \"%v\" only allowed once", physicalContainerLayoutName)
					return
				}

				physicalContainerLayout = &physicalContainerLayoutStruct{}

				physicalContainerLayout.physicalContainerLayoutName = physicalContainerLayoutName

				physicalContainerLayoutSectionName = utils.PhysicalContainerLayoutNameConfSection(physicalContainerLayoutName)

				physicalContainerLayout.physicalContainerStoragePolicy, err = confMap.FetchOptionValueString(physicalContainerLayoutSectionName, "ContainerStoragePolicy")
				if nil != err {
					return
				}

				physicalContainerLayout.physicalContainerNamePrefix, err = confMap.FetchOptionValueString(physicalContainerLayoutSectionName, "ContainerNamePrefix")
				if nil != err {
					return
				}
				_, alreadyInGlobalsPhysicalContainerNamePrefixSet = volume.physicalContainerLayoutSet[physicalContainerLayout.physicalContainerNamePrefix]
				if alreadyInGlobalsPhysicalContainerNamePrefixSet {
					err = fmt.Errorf("ContainerNamePrefix \"%v\" only allowed once", physicalContainerLayout.physicalContainerNamePrefix)
					return
				}

				physicalContainerLayout.physicalContainerCountMax, err = confMap.FetchOptionValueUint64(physicalContainerLayoutSectionName, "ContainersPerPeer")
				if nil != err {
					return
				}

				physicalContainerLayout.physicalObjectCountMax, err = confMap.FetchOptionValueUint64(physicalContainerLayoutSectionName, "MaxObjectsPerContainer")
				if nil != err {
					return
				}

				physicalContainerLayout.physicalContainerNameSlice = make([]string, physicalContainerLayout.physicalContainerCountMax)

				physicalContainerLayout.physicalContainerNameSliceNextIndex = 0
				physicalContainerLayout.physicalContainerNameSliceLoopCount = 0

				volume.physicalContainerLayoutMap[physicalContainerLayoutName] = physicalContainerLayout

				volume.physicalContainerLayoutSet[physicalContainerLayoutName] = struct{}{}
				volume.physicalContainerNamePrefixSet[physicalContainerLayout.physicalContainerNamePrefix] = struct{}{}
			}

			volume.defaultPhysicalContainerLayout, alreadyInVolumePhysicalContainerLayoutMap = volume.physicalContainerLayoutMap[defaultPhysicalContainerLayoutName]
			if !alreadyInVolumePhysicalContainerLayoutMap {
				err = fmt.Errorf("DefaultPhysicalContainerLayout \"%v\" must be in [%v]PhysicalContaonerLayoutList", defaultPhysicalContainerLayoutName, volume.volumeName)
				return
			}

			flowControlName, err = confMap.FetchOptionValueString(volumeSectionName, "FlowControl")
			if nil != err {
				return
			}
			flowControlSectionName = utils.FlowControlNameConfSection(flowControlName)

			_, alreadyInFlowControlMap = globals.flowControlMap[flowControlName]

			if !alreadyInFlowControlMap {
				flowControl = &flowControlStruct{
					flowControlName: flowControlName,
					refCount:        0,
					readCache:       make(map[readCacheKeyStruct]*readCacheElementStruct),
					readCacheMRU:    nil,
					readCacheLRU:    nil,
				}

				flowControl.maxFlushSize, err = confMap.FetchOptionValueUint64(flowControlSectionName, "MaxFlushSize")
				if nil != err {
					return
				}

				flowControl.maxFlushTime, err = confMap.FetchOptionValueDuration(flowControlSectionName, "MaxFlushTime")
				if nil != err {
					return
				}

				flowControl.readCacheLineSize, err = confMap.FetchOptionValueUint64(flowControlSectionName, "ReadCacheLineSize")
				if nil != err {
					return
				}

				flowControl.readCacheWeight, err = confMap.FetchOptionValueUint64(flowControlSectionName, "ReadCacheWeight")
				if nil != err {
					return
				}

				globals.flowControlMap[flowControlName] = flowControl
			}

			volume.flowControl = globals.flowControlMap[flowControlName]
			volume.flowControl.refCount++

			volume.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(volume.volumeName)
			if nil != err {
				return
			}
		}
	}

	for _, flowControl = range globals.flowControlMap {
		flowControlWeightSum += flowControl.readCacheWeight
	}

	readCacheQuotaPercentage, err = confMap.FetchOptionValueFloatScaledToUint64(utils.PeerNameConfSection(globals.whoAmI), "ReadCacheQuotaFraction", 100)
	if nil != err {
		return
	}
	if 100 < readCacheQuotaPercentage {
		err = fmt.Errorf("%s.ReadCacheQuotaFraction must be no greater than 1", globals.whoAmI)
		return
	}

	readCacheMemSize = platform.MemSize() * readCacheQuotaPercentage / 100

	for _, flowControl = range globals.flowControlMap {
		readCacheTotalSize = readCacheMemSize * flowControl.readCacheWeight / flowControlWeightSum

		flowControl.readCacheLineCount = readCacheTotalSize / flowControl.readCacheLineSize
		if 0 == flowControl.readCacheLineCount {
			err = fmt.Errorf("[\"%v\"]ReadCacheWeight must result in at least one ReadCacheLineSize (%v) of memory", flowControl.flowControlName, flowControl.readCacheLineSize)
			return
		}

		capReadCache(flowControl)
	}

	err = nil
	return
}

func Down() (err error) {
	err = nil // Nothing to do
	return
}
