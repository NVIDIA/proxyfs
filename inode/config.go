package inode

import (
	"fmt"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/platform"
	"github.com/swiftstack/conf"
	"github.com/swiftstack/cstruct"
)

type physicalContainerLayoutStruct struct {
	physicalContainerLayoutName         string //   == [VolumeSectionName>]PhysicalContainerLayoutList element (<physical-container-layout-section>)
	physicalContainerStoragePolicyIndex uint32
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
	inFlightFileInodeDataMap       map[InodeNumber]*inMemoryInodeStruct //      key == InodeNumber
}

type globalsStruct struct {
	sync.Mutex
	whoAmI                       string
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
	peerPrivateIPAddrMap := make(map[string]string)

	peerNames, err := confMap.FetchOptionValueStringSlice("Cluster", "Peers")
	if nil != err {
		return
	}

	for _, peerName := range peerNames {
		peerPrivateIPAddr, nonShadowingErr := confMap.FetchOptionValueString(peerName, "PrivateIPAddr")
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		peerPrivateIPAddrMap[peerName] = peerPrivateIPAddr
	}

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}
	_, ok := peerPrivateIPAddrMap[globals.whoAmI]
	if !ok {
		err = fmt.Errorf("Cluster.WhoAmI (\"%v\") not in Cluster.Peers list", globals.whoAmI)
		return
	}

	readCacheQuotaPercentage, err := confMap.FetchOptionValueFloatScaledToUint64(globals.whoAmI, "ReadCacheQuotaFraction", 100)
	if nil != err {
		// TODO: eventually, just return
		readCacheQuotaPercentage = 20
	}
	if 100 < readCacheQuotaPercentage {
		err = fmt.Errorf("%s.ReadCacheQuotaFraction must be no greater than 1", globals.whoAmI)
		return
	}

	globals.volumeMap = make(map[string]*volumeStruct)
	globals.accountMap = make(map[string]*volumeStruct)
	globals.flowControlMap = make(map[string]*flowControlStruct)

	volumeSectionNameSlice, err := confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	for _, volumeSectionName := range volumeSectionNameSlice {
		volume := &volumeStruct{
			volumeName:                     volumeSectionName,
			physicalContainerLayoutSet:     make(map[string]struct{}),
			physicalContainerNamePrefixSet: make(map[string]struct{}),
			inodeCache:                     make(map[InodeNumber]*inMemoryInodeStruct),
			inFlightFileInodeDataMap:       make(map[InodeNumber]*inMemoryInodeStruct),
		}

		volume.fsid, err = confMap.FetchOptionValueUint64(volumeSectionName, "FSID")
		if nil != err {
			return
		}

		for _, prevVolume := range globals.volumeMap {
			if volume.fsid == prevVolume.fsid {
				err = fmt.Errorf("Volume \"%v\" duplicates FSID (%v) of volume \"%v\"", volume.volumeName, volume.fsid, prevVolume.volumeName)
				return
			}
		}

		volume.accountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
		if nil != err {
			return
		}

		_, alreadyInVolumeMap := globals.volumeMap[volume.volumeName]
		if alreadyInVolumeMap {
			err = fmt.Errorf("Volume \"%v\" only allowed once in [FSGlobals]VolumeList", volume.volumeName)
			return
		}

		_, alreadyInAccountMap := globals.volumeMap[volume.accountName]
		if alreadyInAccountMap {
			err = fmt.Errorf("Account \"%v\" only allowed once in [FSGlobals]VolumeList", volume.accountName)
			return
		}

		primaryPeerName, nonShadowingErr := confMap.FetchOptionValueString(volumeSectionName, "PrimaryPeer")
		if nil != nonShadowingErr {
			err = nonShadowingErr
			return
		}

		volume.active = (primaryPeerName == globals.whoAmI)
		volume.activePeerPrivateIPAddr, ok = peerPrivateIPAddrMap[primaryPeerName]
		if !ok {
			err = fmt.Errorf("Volume \"%v\" specifies unknown PrimaryPeer \"%v\"", volumeSectionName, primaryPeerName)
			return
		}

		volume.maxEntriesPerDirNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxEntriesPerDirNode")
		if nil != err {
			// TODO: eventually, just return
			volume.maxEntriesPerDirNode = 32
		}

		volume.maxExtentsPerFileNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxExtentsPerFileNode")
		if nil != err {
			// TODO: eventually, just return
			volume.maxExtentsPerFileNode = 32
		}

		volume.physicalContainerLayoutMap = make(map[string]*physicalContainerLayoutStruct)

		physicalContainerLayoutSectionNameSlice, fetchOptionErr := confMap.FetchOptionValueStringSlice(volumeSectionName, "PhysicalContainerLayoutList")
		if nil != fetchOptionErr {
			err = fetchOptionErr
			return
		}

		for _, physicalContainerLayoutSectionName := range physicalContainerLayoutSectionNameSlice {
			_, alreadyInGlobalsPhysicalContainerLayoutSet := volume.physicalContainerLayoutSet[physicalContainerLayoutSectionName]
			if alreadyInGlobalsPhysicalContainerLayoutSet {
				err = fmt.Errorf("PhysicalContainerLayout \"%v\" only allowed once", physicalContainerLayoutSectionName)
				return
			}

			physicalContainerLayout := &physicalContainerLayoutStruct{}

			physicalContainerLayout.physicalContainerLayoutName = physicalContainerLayoutSectionName

			physicalContainerLayout.physicalContainerStoragePolicyIndex, err = confMap.FetchOptionValueUint32(physicalContainerLayoutSectionName, "ContainerStoragePolicyIndex")
			if nil != err {
				return
			}

			physicalContainerLayout.physicalContainerNamePrefix, err = confMap.FetchOptionValueString(physicalContainerLayoutSectionName, "ContainerNamePrefix")
			if nil != err {
				return
			}
			_, alreadyInGlobalsPhysicalContainerNamePrefixSet := volume.physicalContainerLayoutSet[physicalContainerLayout.physicalContainerNamePrefix]
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

			volume.physicalContainerLayoutMap[physicalContainerLayoutSectionName] = physicalContainerLayout

			volume.physicalContainerLayoutSet[physicalContainerLayoutSectionName] = struct{}{}
			volume.physicalContainerNamePrefixSet[physicalContainerLayout.physicalContainerNamePrefix] = struct{}{}
		}

		defaultPhysicalContainerLayoutName, fetchOptionErr := confMap.FetchOptionValueString(volumeSectionName, "DefaultPhysicalContainerLayout")
		if nil != fetchOptionErr {
			err = fetchOptionErr
			return
		}

		var alreadyInVolumePhysicalContainerLayoutMap bool

		volume.defaultPhysicalContainerLayout, alreadyInVolumePhysicalContainerLayoutMap = volume.physicalContainerLayoutMap[defaultPhysicalContainerLayoutName]
		if !alreadyInVolumePhysicalContainerLayoutMap {
			err = fmt.Errorf("DefaultPhysicalContainerLayout \"%v\" must be in [%v]PhysicalContaonerLayoutList", defaultPhysicalContainerLayoutName, volumeSectionName)
			return
		}

		flowControlSectionName, fetchOptionErr := confMap.FetchOptionValueString(volumeSectionName, "FlowControl")
		if nil != fetchOptionErr {
			err = fetchOptionErr
			return
		}

		_, alreadyInFlowControlMap := globals.flowControlMap[flowControlSectionName]

		if !alreadyInFlowControlMap {
			flowControl := &flowControlStruct{
				readCache:    make(map[readCacheKeyStruct]*readCacheElementStruct),
				readCacheMRU: nil,
				readCacheLRU: nil,
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
				// TODO: eventually, just return
				flowControl.readCacheWeight, err = confMap.FetchOptionValueUint64(flowControlSectionName, "ReadCacheTotalSize")
				if nil != err {
					return
				}
			}

			globals.flowControlMap[flowControlSectionName] = flowControl
		}

		volume.flowControl = globals.flowControlMap[flowControlSectionName]

		if volume.active {
			volume.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(volume.volumeName)
			if nil != err {
				return
			}
		}

		globals.volumeMap[volume.volumeName] = volume
		globals.accountMap[volume.accountName] = volume
	}

	var flowControlWeightSum uint64

	for _, flowControl := range globals.flowControlMap {
		flowControlWeightSum += flowControl.readCacheWeight
	}

	readCacheMemSize := platform.MemSize() * readCacheQuotaPercentage / 100

	for _, flowControl := range globals.flowControlMap {
		readCacheTotalSize := readCacheMemSize * flowControl.readCacheWeight / flowControlWeightSum

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

	corruptionDetectedTrue := CorruptionDetected(true)
	globals.corruptionDetectedTrueBuf, err = cstruct.Pack(corruptionDetectedTrue, cstruct.LittleEndian)
	if nil != err {
		return
	}

	corruptionDetectedFalse := CorruptionDetected(false)
	globals.corruptionDetectedFalseBuf, err = cstruct.Pack(corruptionDetectedFalse, cstruct.LittleEndian)
	if nil != err {
		return
	}

	versionV1 := Version(V1)
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
	err = nil // TODO
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	err = nil // TODO
	return
}

func Down() (err error) {
	var (
		volume *volumeStruct
		wg     sync.WaitGroup
	)

	for _, volume = range globals.volumeMap {
		if volume.active {
			wg.Add(1) // Add one "item" to await per volume
			go volume.drainVolume(&wg)
		}
	}

	wg.Wait()

	err = nil
	return
}

func (vS *volumeStruct) drainVolume(wg *sync.WaitGroup) {
	vS.Lock() // Prevent vS.inFlightFileInodeDataMap from changing while we are iterating over it
	for _, fileInode := range vS.inFlightFileInodeDataMap {
		wg.Add(1) // Add one "item" to await per file
		go func(fileInode *inMemoryInodeStruct, wg *sync.WaitGroup) {
			err := vS.doFileInodeDataFlush(fileInode)
			if nil != err {
				logger.ErrorWithError(err, "Forced flush during Volume Offline failed")
			}
			wg.Done() // This file "item" is done
		}(fileInode, wg)
	}
	vS.Unlock()
	wg.Done() // This volume "item" is done
}
