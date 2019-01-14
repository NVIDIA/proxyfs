package inode

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/transitions"
)

type readCacheKeyStruct struct {
	volumeName       string
	logSegmentNumber uint64
	cacheLineTag     uint64 // LogSegment offset / readCacheLineSize
}

type readCacheElementStruct struct {
	readCacheKey readCacheKeyStruct
	next         *readCacheElementStruct // nil if MRU element of volumeGroupStruct.readCache
	prev         *readCacheElementStruct // nil if LRU element of volumeGroupStruct.readCache
	cacheLine    []byte
}

type volumeGroupStruct struct {
	sync.Mutex
	name                    string
	volumeMap               map[string]*volumeStruct // key == volumeStruct.volumeName
	numServed               uint64
	virtualIPAddr           string
	activePeerPrivateIPAddr string
	readCacheLineSize       uint64
	readCacheWeight         uint64
	readCacheLineCount      uint64
	readCache               map[readCacheKeyStruct]*readCacheElementStruct
	readCacheMRU            *readCacheElementStruct
	readCacheLRU            *readCacheElementStruct
}

type physicalContainerLayoutStruct struct {
	name                   string
	containerStoragePolicy string   //    [<PhysicalContainerLayout>]ContainerStoragePolicy
	containerNamePrefix    string   //    [<PhysicalContainerLayout>]ContainerNamePrefix
	containerNameSlice     []string //    == slice of current PhysicalContainers for this PhysicalContainerLayout
	//                                       Note: all prefixed by containerNamePrefix
	containersPerPeer           uint64 // [<PhysicalContainerLayout>]ContainersPerPeer
	maxObjectsPerContainer      uint64 // [<PhysicalContainerLayout>]MaxObjectsPerContainer
	containerNameSliceNextIndex uint64 // == next index in nameSlice
	containerNameSliceLoopCount uint64 // == number of times looped through nameSlice
	//                                       Note: if 0 == (containerNameSliceLoopCount mod maxObjectsPerContainer)
	//                                             then we need to re-provision containerNameSlice
}

type volumeStruct struct {
	sync.Mutex
	volumeGroup                    *volumeGroupStruct
	served                         bool
	fsid                           uint64
	volumeName                     string
	accountName                    string
	maxEntriesPerDirNode           uint64
	maxExtentsPerFileNode          uint64
	defaultPhysicalContainerLayout *physicalContainerLayoutStruct
	maxFlushSize                   uint64
	headhunterVolumeHandle         headhunter.VolumeHandle
	inodeCache                     sortedmap.LLRBTree //                        key == InodeNumber; value == *inMemoryInodeStruct
	inodeCacheLRUHead              *inMemoryInodeStruct
	inodeCacheLRUTail              *inMemoryInodeStruct
	inodeCacheLRUItems             uint64
	inodeCacheLRUMaxBytes          uint64
	inodeCacheLRUTicker            *time.Ticker
	inodeCacheLRUTickerInterval    time.Duration
	snapShotPolicy                 *snapShotPolicyStruct
}

type globalsStruct struct {
	sync.Mutex
	whoAmI                             string
	myPrivateIPAddr                    string
	dirEntryCache                      sortedmap.BPlusTreeCache
	dirEntryCachePriorCacheHits        uint64
	dirEntryCachePriorCacheMisses      uint64
	fileExtentMapCache                 sortedmap.BPlusTreeCache
	fileExtentMapCachePriorCacheHits   uint64
	fileExtentMapCachePriorCacheMisses uint64
	volumeGroupMap                     map[string]*volumeGroupStruct // key == volumeGroupStruct.name
	volumeMap                          map[string]*volumeStruct      // key == volumeStruct.volumeName
	accountMap                         map[string]*volumeStruct      // key == volumeStruct.accountName
	fileExtentStructSize               uint64                        // pre-calculated size of cstruct-packed fileExtentStruct
	supportedOnDiskInodeVersions       map[Version]struct{}          // key == on disk inode version
	corruptionDetectedTrueBuf          []byte                        // holds serialized CorruptionDetected == true
	corruptionDetectedFalseBuf         []byte                        // holds serialized CorruptionDetected == false
	versionV1Buf                       []byte                        // holds serialized Version            == V1
	inodeRecDefaultPreambleBuf         []byte                        // holds concatenated corruptionDetectedFalseBuf & versionV1Buf
	inodeSize                          uint64                        // size of in-memory inode struct
	openLogSegmentLRUHead              *inFlightLogSegmentStruct
	openLogSegmentLRUTail              *inFlightLogSegmentStruct
	openLogSegmentLRUItems             uint64
}

var globals globalsStruct

func init() {
	transitions.Register("inode", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	var (
		corruptionDetectedFalse     = CorruptionDetected(false)
		corruptionDetectedTrue      = CorruptionDetected(true)
		dirEntryCacheEvictHighLimit uint64
		dirEntryCacheEvictLowLimit  uint64
		fileExtentMapEvictHighLimit uint64
		fileExtentMapEvictLowLimit  uint64
		ok                          bool
		peerName                    string
		peerNames                   []string
		peerPrivateIPAddr           string
		peerPrivateIPAddrMap        map[string]string
		tempInode                   inMemoryInodeStruct
		versionV1                   = Version(V1)
	)

	peerPrivateIPAddrMap = make(map[string]string)

	peerNames, err = confMap.FetchOptionValueStringSlice("Cluster", "Peers")
	if nil != err {
		return
	}

	for _, peerName = range peerNames {
		peerPrivateIPAddr, err = confMap.FetchOptionValueString("Peer:"+peerName, "PrivateIPAddr")
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
		return
	}
	dirEntryCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "DirEntryCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.dirEntryCache = sortedmap.NewBPlusTreeCache(dirEntryCacheEvictLowLimit, dirEntryCacheEvictHighLimit)

	globals.dirEntryCachePriorCacheHits = 0
	globals.dirEntryCachePriorCacheMisses = 0

	fileExtentMapEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "FileExtentMapEvictLowLimit")
	if nil != err {
		return
	}
	fileExtentMapEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "FileExtentMapEvictHighLimit")
	if nil != err {
		return
	}

	globals.fileExtentMapCache = sortedmap.NewBPlusTreeCache(fileExtentMapEvictLowLimit, fileExtentMapEvictHighLimit)

	globals.fileExtentMapCachePriorCacheHits = 0
	globals.fileExtentMapCachePriorCacheMisses = 0

	globals.volumeGroupMap = make(map[string]*volumeGroupStruct)
	globals.volumeMap = make(map[string]*volumeStruct)
	globals.accountMap = make(map[string]*volumeStruct)

	globals.inodeSize = uint64(unsafe.Sizeof(tempInode))

	globals.openLogSegmentLRUHead = nil
	globals.openLogSegmentLRUTail = nil
	globals.openLogSegmentLRUItems = 0

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

	swiftclient.SetStarvationCallbackFunc(chunkedPutConnectionPoolStarvationCallback)

	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	var (
		ok                     bool
		volumeGroup            *volumeGroupStruct
		volumeGroupSectionName string
	)

	volumeGroup = &volumeGroupStruct{
		name:               volumeGroupName,
		volumeMap:          make(map[string]*volumeStruct),
		numServed:          0,
		readCacheLineCount: 0,
		readCache:          make(map[readCacheKeyStruct]*readCacheElementStruct),
		readCacheMRU:       nil,
		readCacheLRU:       nil,
	}

	volumeGroupSectionName = "VolumeGroup:" + volumeGroupName

	volumeGroup.virtualIPAddr, err = confMap.FetchOptionValueString(volumeGroupSectionName, "VirtualIPAddr")
	if nil != err {
		if nil == confMap.VerifyOptionValueIsEmpty(volumeGroupSectionName, "VirtualIPAddr") {
			volumeGroup.virtualIPAddr = ""
		} else {
			return
		}
	}

	if "" == activePeer {
		volumeGroup.activePeerPrivateIPAddr = ""
	} else {
		volumeGroup.activePeerPrivateIPAddr, err = confMap.FetchOptionValueString("Peer:"+activePeer, "PrivateIPAddr")
		if nil != err {
			return
		}
	}

	volumeGroup.readCacheLineSize, err = confMap.FetchOptionValueUint64(volumeGroupSectionName, "ReadCacheLineSize")
	if nil != err {
		return
	}

	volumeGroup.readCacheWeight, err = confMap.FetchOptionValueUint64(volumeGroupSectionName, "ReadCacheWeight")
	if nil != err {
		return
	}

	globals.Lock()

	_, ok = globals.volumeGroupMap[volumeGroupName]
	if ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeGroupCreated() called for preexisting VolumeGroup (%s)", volumeGroupName)
		return
	}

	globals.volumeGroupMap[volumeGroupName] = volumeGroup

	globals.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	var (
		ok          bool
		volumeGroup *volumeGroupStruct
	)

	globals.Lock()

	volumeGroup, ok = globals.volumeGroupMap[volumeGroupName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeGroupMoved() called for nonexistent VolumeGroup (%s)", volumeGroupName)
		return
	}

	volumeGroup.Lock()

	if "" == activePeer {
		volumeGroup.activePeerPrivateIPAddr = ""
	} else {
		volumeGroup.activePeerPrivateIPAddr, err = confMap.FetchOptionValueString("Peer:"+activePeer, "PrivateIPAddr")
		if nil != err {
			volumeGroup.Unlock()
			globals.Unlock()
			return
		}
	}

	// Note that VirtualIPAddr, ReadCacheLineSize, & ReadCacheWeight are not reloaded

	volumeGroup.Unlock()
	globals.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	var (
		ok          bool
		volumeGroup *volumeGroupStruct
	)

	globals.Lock()

	volumeGroup, ok = globals.volumeGroupMap[volumeGroupName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeGroupDestroyed() called for nonexistent VolumeGroup (%s)", volumeGroupName)
		return
	}

	volumeGroup.Lock()

	if 0 != len(volumeGroup.volumeMap) {
		volumeGroup.Unlock()
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeGroupDestroyed() called for non-empty VolumeGroup (%s)", volumeGroupName)
		return
	}

	delete(globals.volumeGroupMap, volumeGroupName)

	volumeGroup.Unlock()
	globals.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	var (
		ok                bool
		volumeGroup       *volumeGroupStruct
		volumeSectionName string
	)

	volume := &volumeStruct{volumeName: volumeName, served: false}

	volumeSectionName = "Volume:" + volumeName

	volume.fsid, err = confMap.FetchOptionValueUint64(volumeSectionName, "FSID")
	if nil != err {
		return
	}

	volume.accountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
	if nil != err {
		return
	}

	globals.Lock()

	_, ok = globals.volumeMap[volumeName]
	if ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeCreated() called for preexiting Volume (%s)", volumeName)
		return
	}

	_, ok = globals.accountMap[volume.accountName]
	if ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeCreated() called for preexiting Account (%s)", volume.accountName)
		return
	}

	volumeGroup, ok = globals.volumeGroupMap[volumeGroupName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeCreated() called for Volume (%s) to be added to nonexistent VolumeGroup (%s)", volumeName, volumeGroupName)
		return
	}

	volumeGroup.Lock()

	_, ok = volumeGroup.volumeMap[volumeName]
	if ok {
		volumeGroup.Unlock()
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeCreated() called for preexiting Volume (%s) to be added to VolumeGroup (%s)", volumeName, volumeGroupName)
		return
	}

	volume.volumeGroup = volumeGroup
	volumeGroup.volumeMap[volumeName] = volume
	globals.volumeMap[volumeName] = volume
	globals.accountMap[volume.accountName] = volume

	volumeGroup.Unlock()
	globals.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	var (
		newVolumeGroup *volumeGroupStruct
		ok             bool
		oldVolumeGroup *volumeGroupStruct
		volume         *volumeStruct
	)

	globals.Lock()

	volume, ok = globals.volumeMap[volumeName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeMoved() called for nonexistent Volume (%s)", volumeName)
		return
	}

	if volume.served {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeMoved() called for Volume (%s) being actively served", volumeName)
		return
	}

	newVolumeGroup, ok = globals.volumeGroupMap[volumeGroupName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeMoved() called for Volume (%s) to be moved to nonexistent VolumeGroup (%s)", volumeName, volumeGroupName)
		return
	}

	newVolumeGroup.Lock()

	_, ok = newVolumeGroup.volumeMap[volumeName]
	if !ok {
		newVolumeGroup.Unlock()
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeMoved() called for Volume (%s) to be moved to VolumeGroup (%s) already containing the Volume", volumeName, volumeGroupName)
		return
	}

	oldVolumeGroup = volume.volumeGroup

	oldVolumeGroup.Lock()

	delete(oldVolumeGroup.volumeMap, volumeName)
	newVolumeGroup.volumeMap[volumeName] = volume
	volume.volumeGroup = newVolumeGroup

	// Note that FSID & AccountName are not reloaded

	oldVolumeGroup.Unlock()
	newVolumeGroup.Unlock()
	globals.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		ok     bool
		volume *volumeStruct
	)

	globals.Lock()

	volume, ok = globals.volumeMap[volumeName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeDestroyed() called for nonexistent Volume (%s)", volumeName)
		return
	}

	if volume.served {
		globals.Unlock()
		err = fmt.Errorf("inode.VolumeDestroyed() called for Volume (%s) being actively served", volumeName)
		return
	}

	volume.volumeGroup.Lock()

	delete(volume.volumeGroup.volumeMap, volumeName)
	delete(globals.volumeMap, volumeName)
	delete(globals.accountMap, volume.accountName)

	volume.volumeGroup.Unlock()
	globals.Unlock()

	err = nil
	return
}

func (dummy *globalsStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		defaultPhysicalContainerLayout            *physicalContainerLayoutStruct
		defaultPhysicalContainerLayoutName        string
		defaultPhysicalContainerLayoutSectionName string
		ok                                        bool
		volume                                    *volumeStruct
		volumeSectionName                         string
	)

	volumeSectionName = "Volume:" + volumeName

	globals.Lock()

	volume, ok = globals.volumeMap[volumeName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("inode.ServeVolume() called for nonexistent Volume (%s)", volumeName)
		return
	}
	if volume.served {
		globals.Unlock()
		err = fmt.Errorf("inode.ServeVolume() called for Volume (%s) already being served", volumeName)
		return
	}

	volume.maxEntriesPerDirNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxEntriesPerDirNode")
	if nil != err {
		globals.Unlock()
		return
	}
	volume.maxExtentsPerFileNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxExtentsPerFileNode")
	if nil != err {
		globals.Unlock()
		return
	}
	defaultPhysicalContainerLayoutName, err = confMap.FetchOptionValueString(volumeSectionName, "DefaultPhysicalContainerLayout")
	if nil != err {
		globals.Unlock()
		return
	}

	defaultPhysicalContainerLayout = &physicalContainerLayoutStruct{
		name:                        defaultPhysicalContainerLayoutName,
		containerNameSliceNextIndex: 0,
		containerNameSliceLoopCount: 0,
	}

	defaultPhysicalContainerLayoutSectionName = "PhysicalContainerLayout:" + defaultPhysicalContainerLayoutName

	defaultPhysicalContainerLayout.containerStoragePolicy, err = confMap.FetchOptionValueString(defaultPhysicalContainerLayoutSectionName, "ContainerStoragePolicy")
	if nil != err {
		globals.Unlock()
		return
	}
	defaultPhysicalContainerLayout.containerNamePrefix, err = confMap.FetchOptionValueString(defaultPhysicalContainerLayoutSectionName, "ContainerNamePrefix")
	if nil != err {
		globals.Unlock()
		return
	}
	defaultPhysicalContainerLayout.containersPerPeer, err = confMap.FetchOptionValueUint64(defaultPhysicalContainerLayoutSectionName, "ContainersPerPeer")
	if nil != err {
		globals.Unlock()
		return
	}
	defaultPhysicalContainerLayout.maxObjectsPerContainer, err = confMap.FetchOptionValueUint64(defaultPhysicalContainerLayoutSectionName, "MaxObjectsPerContainer")
	if nil != err {
		globals.Unlock()
		return
	}

	defaultPhysicalContainerLayout.containerNameSlice = make([]string, defaultPhysicalContainerLayout.containersPerPeer)

	volume.defaultPhysicalContainerLayout = defaultPhysicalContainerLayout

	volume.maxFlushSize, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxFlushSize")
	if nil != err {
		globals.Unlock()
		return
	}

	err = volume.loadSnapShotPolicy(confMap)
	if nil != err {
		globals.Unlock()
		return
	}

	volume.headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(volume.volumeName)
	if nil != err {
		globals.Unlock()
		return
	}

	volume.headhunterVolumeHandle.RegisterForEvents(volume)

	volume.inodeCache = sortedmap.NewLLRBTree(compareInodeNumber, volume)
	volume.inodeCacheLRUHead = nil
	volume.inodeCacheLRUTail = nil
	volume.inodeCacheLRUItems = 0

	err = startInodeCacheDiscard(confMap, volume, volumeSectionName)
	if nil != err {
		globals.Unlock()
		return
	}

	volume.volumeGroup.Lock()

	volume.served = true
	volume.volumeGroup.numServed++

	volume.volumeGroup.Unlock()
	globals.Unlock()

	err = adoptVolumeGroupReadCacheParameters(confMap)

	return // err from call to adoptVolumeGroupReadCacheParameters() is fine to return here
}

func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		ok     bool
		volume *volumeStruct
	)

	globals.Lock()

	volume, ok = globals.volumeMap[volumeName]
	if !ok {
		globals.Unlock()
		err = fmt.Errorf("inode.UnserveVolume() called for nonexistent Volume (%s)", volumeName)
		return
	}
	if !volume.served {
		globals.Unlock()
		err = fmt.Errorf("inode.UnserveVolume() called for Volume (%s) not being served", volumeName)
		return
	}

	stopInodeCacheDiscard(volume)

	volume.inodeCache = nil
	volume.inodeCacheLRUHead = nil
	volume.inodeCacheLRUTail = nil
	volume.inodeCacheLRUItems = 0

	volume.headhunterVolumeHandle.UnregisterForEvents(volume)

	volume.volumeGroup.Lock()

	volume.served = false
	volume.volumeGroup.numServed--

	volume.volumeGroup.Unlock()
	globals.Unlock()

	err = adoptVolumeGroupReadCacheParameters(confMap)

	return // err from call to adoptVolumeGroupReadCacheParameters() is fine to return here
}

func (dummy *globalsStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	return nil
}
func (dummy *globalsStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	return nil
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	if 0 != len(globals.volumeGroupMap) {
		err = fmt.Errorf("inode.Down() called with 0 != len(globals.volumeGroupMap")
		return
	}
	if 0 != len(globals.volumeMap) {
		err = fmt.Errorf("inode.Down() called with 0 != len(globals.volumeMap")
		return
	}

	err = nil
	return
}
