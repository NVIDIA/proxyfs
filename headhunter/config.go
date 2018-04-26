package headhunter

import (
	"fmt"
	"hash/crc64"
	"os"
	"sync"
	"time"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

const (
	firstNonceToProvide = uint64(2) // Must skip useful values: 0 == unassigned and 1 == RootDirInodeNumber
)

const (
	AccountHeaderName           = "X-ProxyFS-BiModal"
	AccountHeaderNameTranslated = "X-Account-Sysmeta-Proxyfs-Bimodal"
	AccountHeaderValue          = "true"
	CheckpointHeaderName        = "X-Container-Meta-Checkpoint"
	StoragePolicyHeaderName     = "X-Storage-Policy"
)

type bPlusTreeWrapperStruct struct {
	volumeView              *volumeViewStruct
	bPlusTree               sortedmap.BPlusTree
	trackingBPlusTreeLayout sortedmap.LayoutReport
}

type volumeViewStruct struct {
	volume                *volumeStruct
	nonce                 uint64    //   supplies strict time-ordering of views regardless timebase resets
	snapShotID            uint64    //   only valid for active SnapShot views
	snapShotTimeStamp     time.Time //   only valid for active SnapShot views
	snapShotName          string    //   only valid for active SnapShot views
	holdOffDeletesWG      sync.WaitGroup
	trackingObjectCreates bool //        creates need not be tracked for the oldest SnapShot
	//                                     (or liveView if no active SnapShots precede it)
	inodeRecWrapper        *bPlusTreeWrapperStruct
	logSegmentRecWrapper   *bPlusTreeWrapperStruct
	bPlusTreeObjectWrapper *bPlusTreeWrapperStruct
	createdObjectsWrapper  *bPlusTreeWrapperStruct
	deletedObjectsWrapper  *bPlusTreeWrapperStruct
}

type delayedObjectDeleteSSTODOStruct struct {
	containerName string
	objectNumber  uint64
}

type volumeStruct struct {
	sync.Mutex
	volumeName                              string
	accountName                             string
	maxFlushSize                            uint64
	nonceValuesToReserve                    uint16
	maxInodesPerMetadataNode                uint64
	maxLogSegmentsPerMetadataNode           uint64
	maxDirFileNodesPerMetadataNode          uint64
	maxCreatedDeletedObjectsPerMetadataNode uint64
	checkpointContainerName                 string
	checkpointContainerStoragePolicy        string
	checkpointInterval                      time.Duration
	replayLogFileName                       string   //           if != "", use replay log to reduce RPO to zero
	replayLogFile                           *os.File //           opened on first Put or Delete after checkpoint
	//                                                            closed/deleted on successful checkpoint
	defaultReplayLogWriteBuffer             []byte //             used for O_DIRECT writes to replay log
	checkpointFlushedData                   bool
	checkpointChunkedPutContext             swiftclient.ChunkedPutContext
	checkpointChunkedPutContextObjectNumber uint64 //             ultimately copied to CheckpointObjectTrailerV2StructObjectNumber
	checkpointDoneWaitGroup                 *sync.WaitGroup
	nextNonce                               uint64
	checkpointRequestChan                   chan *checkpointRequestStruct
	checkpointHeaderVersion                 uint64
	checkpointHeader                        *checkpointHeaderV2Struct
	checkpointObjectTrailer                 *checkpointObjectTrailerV2Struct
	liveView                                *volumeViewStruct
	viewByNonce                             sortedmap.LLRBTree // non-live volumeViewStruct's all in this LLRBTree
	snapViewByID                            sortedmap.LLRBTree // non-live active volumeViewStruct's in this LLRBTree
	snapViewByTimeStamp                     sortedmap.LLRBTree // non-live active volumeViewStruct's in this LLRBTree
	snapViewByName                          sortedmap.LLRBTree // non-live active volumeViewStruct's in this LLRBTree
	snapViewBeingDeleted                    sortedmap.LLRBTree // volumeVewStruct's being deleted are in this LLRBTree (key:Nonce)
	delayedObjectDeleteSSTODOList           []delayedObjectDeleteSSTODOStruct
	backgroundObjectDeleteWG                sync.WaitGroup
}

type globalsStruct struct {
	crc64ECMATable                          *crc64.Table
	uint64Size                              uint64
	checkpointHeaderV2StructSize            uint64
	checkpointObjectTrailerStructSize       uint64
	elementOfBPlusTreeLayoutStructSize      uint64
	replayLogTransactionFixedPartStructSize uint64
	inodeRecCache                           sortedmap.BPlusTreeCache
	logSegmentRecCache                      sortedmap.BPlusTreeCache
	bPlusTreeObjectCache                    sortedmap.BPlusTreeCache
	createdDeletedObjectsCache              sortedmap.BPlusTreeCache
	volumeMap                               map[string]*volumeStruct // key == ramVolumeStruct.volumeName
}

var globals globalsStruct

// Up starts the headhunter package
func Up(confMap conf.ConfMap) (err error) {
	var (
		bPlusTreeObjectCacheEvictHighLimit       uint64
		bPlusTreeObjectCacheEvictLowLimit        uint64
		createdDeletedObjectsCacheEvictHighLimit uint64
		createdDeletedObjectsCacheEvictLowLimit  uint64
		dummyCheckpointHeaderV2Struct            checkpointHeaderV2Struct
		dummyCheckpointObjectTrailerV2Struct     checkpointObjectTrailerV2Struct
		dummyElementOfBPlusTreeLayoutStruct      elementOfBPlusTreeLayoutStruct
		dummyReplayLogTransactionFixedPartStruct replayLogTransactionFixedPartStruct
		dummyUint64                              uint64
		inodeRecCacheEvictHighLimit              uint64
		inodeRecCacheEvictLowLimit               uint64
		logSegmentRecCacheEvictHighLimit         uint64
		logSegmentRecCacheEvictLowLimit          uint64
		primaryPeerList                          []string
		volumeName                               string
		volumeList                               []string
		whoAmI                                   string
	)

	// Pre-compute crc64 ECMA Table & useful cstruct sizes

	globals.crc64ECMATable = crc64.MakeTable(crc64.ECMA)

	globals.uint64Size, _, err = cstruct.Examine(dummyUint64)
	if nil != err {
		return
	}

	globals.checkpointHeaderV2StructSize, _, err = cstruct.Examine(dummyCheckpointHeaderV2Struct)
	if nil != err {
		return
	}

	globals.checkpointObjectTrailerStructSize, _, err = cstruct.Examine(dummyCheckpointObjectTrailerV2Struct)
	if nil != err {
		return
	}

	globals.elementOfBPlusTreeLayoutStructSize, _, err = cstruct.Examine(dummyElementOfBPlusTreeLayoutStruct)
	if nil != err {
		return
	}

	globals.replayLogTransactionFixedPartStructSize, _, err = cstruct.Examine(dummyReplayLogTransactionFixedPartStruct)
	if nil != err {
		return
	}

	// Init volume database(s)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	inodeRecCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "InodeRecCacheEvictLowLimit")
	if nil != err {
		return
	}
	inodeRecCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "InodeRecCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.inodeRecCache = sortedmap.NewBPlusTreeCache(inodeRecCacheEvictLowLimit, inodeRecCacheEvictHighLimit)

	logSegmentRecCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "LogSegmentRecCacheEvictLowLimit")
	if nil != err {
		return
	}
	logSegmentRecCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "LogSegmentRecCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.logSegmentRecCache = sortedmap.NewBPlusTreeCache(logSegmentRecCacheEvictLowLimit, logSegmentRecCacheEvictHighLimit)

	bPlusTreeObjectCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "BPlusTreeObjectCacheEvictLowLimit")
	if nil != err {
		return
	}
	bPlusTreeObjectCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "BPlusTreeObjectCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.bPlusTreeObjectCache = sortedmap.NewBPlusTreeCache(bPlusTreeObjectCacheEvictLowLimit, bPlusTreeObjectCacheEvictHighLimit)

	createdDeletedObjectsCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "CreatedDeletedObjectsCacheEvictLowLimit")
	if nil != err {
		createdDeletedObjectsCacheEvictLowLimit = logSegmentRecCacheEvictLowLimit // TODO: Eventually just return
	}
	createdDeletedObjectsCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "CreatedDeletedObjectsCacheEvictHighLimit")
	if nil != err {
		createdDeletedObjectsCacheEvictHighLimit = logSegmentRecCacheEvictHighLimit // TODO: Eventually just return
	}

	globals.createdDeletedObjectsCache = sortedmap.NewBPlusTreeCache(createdDeletedObjectsCacheEvictLowLimit, createdDeletedObjectsCacheEvictHighLimit)

	globals.volumeMap = make(map[string]*volumeStruct)

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice(utils.VolumeNameConfSection(volumeName), "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if whoAmI == primaryPeerList[0] {
				err = upVolume(confMap, volumeName, false)
				if nil != err {
					return
				}
			}
		} else {
			err = fmt.Errorf("Volume \"%v\" cannot have multiple PrimaryPeer values", volumeName)
			return
		}
	}

	return
}

// PauseAndContract pauses the headhunter package and applies any removals from the supplied confMap
func PauseAndContract(confMap conf.ConfMap) (err error) {
	var (
		deletedVolumeNames map[string]bool
		primaryPeerList    []string
		volumeName         string
		volumeList         []string
		whoAmI             string
	)

	deletedVolumeNames = make(map[string]bool)

	for volumeName = range globals.volumeMap {
		deletedVolumeNames[volumeName] = true
	}

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice(utils.VolumeNameConfSection(volumeName), "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if whoAmI == primaryPeerList[0] {
				delete(deletedVolumeNames, volumeName)
			}
		} else {
			err = fmt.Errorf("Volume \"%v\" cannot have multiple PrimaryPeer values", volumeName)
			return
		}
	}

	for volumeName = range deletedVolumeNames {
		err = downVolume(volumeName)
		if nil != err {
			return
		}

		delete(globals.volumeMap, volumeName)
	}

	err = nil
	return
}

// ExpandAndResume applies any additions from the supplied confMap and resumes the headhunter package
func ExpandAndResume(confMap conf.ConfMap) (err error) {
	var (
		ok              bool
		primaryPeerList []string
		volumeName      string
		volumeList      []string
		whoAmI          string
	)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice(utils.VolumeNameConfSection(volumeName), "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerList) {
			continue
		} else if 1 == len(primaryPeerList) {
			if whoAmI == primaryPeerList[0] {
				_, ok = globals.volumeMap[volumeName]
				if !ok {
					err = upVolume(confMap, volumeName, false)
					if nil != err {
						return
					}
				}
			}
		} else {
			err = fmt.Errorf("Volume \"%v\" cannot have multiple PrimaryPeer values", volumeName)
			return
		}
	}

	err = nil
	return
}

// Down terminates the headhunter package
func Down() (err error) {
	var (
		volumeName string
	)

	for volumeName = range globals.volumeMap {
		err = downVolume(volumeName)
		if nil != err {
			return
		}
	}

	err = nil
	return
}

// Format runs an instance of the headhunter package for formatting a new volume
func Format(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		dummyCheckpointHeaderV2Struct            checkpointHeaderV2Struct
		dummyCheckpointObjectTrailerV2Struct     checkpointObjectTrailerV2Struct
		dummyElementOfBPlusTreeLayoutStruct      elementOfBPlusTreeLayoutStruct
		dummyReplayLogTransactionFixedPartStruct replayLogTransactionFixedPartStruct
		dummyUint64                              uint64
	)

	// Pre-compute crc64 ECMA Table & useful cstruct sizes

	globals.crc64ECMATable = crc64.MakeTable(crc64.ECMA)

	globals.uint64Size, _, err = cstruct.Examine(dummyUint64)
	if nil != err {
		return
	}

	globals.checkpointHeaderV2StructSize, _, err = cstruct.Examine(dummyCheckpointHeaderV2Struct)
	if nil != err {
		return
	}

	globals.checkpointObjectTrailerStructSize, _, err = cstruct.Examine(dummyCheckpointObjectTrailerV2Struct)
	if nil != err {
		return
	}

	globals.elementOfBPlusTreeLayoutStructSize, _, err = cstruct.Examine(dummyElementOfBPlusTreeLayoutStruct)
	if nil != err {
		return
	}

	globals.replayLogTransactionFixedPartStructSize, _, err = cstruct.Examine(dummyReplayLogTransactionFixedPartStruct)
	if nil != err {
		return
	}

	// Init volume database...triggering format

	globals.volumeMap = make(map[string]*volumeStruct)

	err = upVolume(confMap, volumeName, true)
	if nil != err {
		return
	}

	// Shutdown and exit

	err = downVolume(volumeName)

	return
}

func upVolume(confMap conf.ConfMap, volumeName string, autoFormat bool) (err error) {
	var (
		flowControlName        string
		flowControlSectionName string
		volume                 *volumeStruct
		volumeSectionName      string
	)

	volumeSectionName = utils.VolumeNameConfSection(volumeName)

	volume = &volumeStruct{
		volumeName:                    volumeName,
		checkpointChunkedPutContext:   nil,
		checkpointDoneWaitGroup:       nil,
		checkpointRequestChan:         make(chan *checkpointRequestStruct, 1),
		delayedObjectDeleteSSTODOList: make([]delayedObjectDeleteSSTODOStruct, 0),
	}

	volume.accountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
	if nil != err {
		return
	}

	flowControlName, err = confMap.FetchOptionValueString(volumeSectionName, "FlowControl")
	if nil != err {
		return
	}
	flowControlSectionName = utils.FlowControlNameConfSection(flowControlName)

	volume.maxFlushSize, err = confMap.FetchOptionValueUint64(flowControlSectionName, "MaxFlushSize")
	if nil != err {
		return
	}

	volume.nonceValuesToReserve, err = confMap.FetchOptionValueUint16(volumeSectionName, "NonceValuesToReserve")
	if nil != err {
		return
	}

	volume.maxInodesPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxInodesPerMetadataNode")
	if nil != err {
		return
	}

	volume.maxLogSegmentsPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxLogSegmentsPerMetadataNode")
	if nil != err {
		return
	}

	volume.maxDirFileNodesPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxDirFileNodesPerMetadataNode")
	if nil != err {
		return
	}

	volume.maxCreatedDeletedObjectsPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeSectionName, "MaxCreatedDeletedObjectsPerMetadataNode")
	if nil != err {
		volume.maxCreatedDeletedObjectsPerMetadataNode = volume.maxLogSegmentsPerMetadataNode // TODO: Eventually just return
	}

	volume.checkpointContainerName, err = confMap.FetchOptionValueString(volumeSectionName, "CheckpointContainerName")
	if nil != err {
		return
	}

	volume.checkpointContainerStoragePolicy, err = confMap.FetchOptionValueString(volumeSectionName, "CheckpointContainerStoragePolicy")
	if nil != err {
		return
	}

	volume.checkpointInterval, err = confMap.FetchOptionValueDuration(volumeSectionName, "CheckpointInterval")
	if nil != err {
		return
	}

	volume.replayLogFileName, err = confMap.FetchOptionValueString(volumeSectionName, "ReplayLogFileName")
	if nil == err {
		// Provision aligned buffer used to write to Replay Log
		volume.defaultReplayLogWriteBuffer = constructReplayLogWriteBuffer(replayLogWriteBufferDefaultSize)
	} else {
		// Disable Replay Log
		volume.replayLogFileName = ""
	}

	err = volume.getCheckpoint(autoFormat)
	if nil != err {
		return
	}

	globals.volumeMap[volumeName] = volume

	go volume.checkpointDaemon()

	err = nil
	return
}

func downVolume(volumeName string) (err error) {
	var (
		checkpointRequest checkpointRequestStruct
		ok                bool
		volume            *volumeStruct
	)

	volume, ok = globals.volumeMap[volumeName]
	if !ok {
		err = fmt.Errorf("\"%v\" not found in volumeMap", volumeName)
		return
	}

	checkpointRequest.exitOnCompletion = true
	checkpointRequest.waitGroup.Add(1)

	volume.checkpointRequestChan <- &checkpointRequest

	checkpointRequest.waitGroup.Wait()

	err = checkpointRequest.err

	volume.backgroundObjectDeleteWG.Wait()

	return
}
