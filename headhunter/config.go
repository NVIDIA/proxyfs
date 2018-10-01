package headhunter

import (
	"container/list"
	"fmt"
	"hash/crc64"
	"os"
	"sync"
	"time"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

const (
	firstNonceToProvide = uint64(2) // Must skip useful values: 0 == unassigned and 1 == RootDirInodeNumber
)

const (
	liveSnapShotID = uint64(0)
)

const (
	AccountHeaderName           = "X-ProxyFS-BiModal"
	AccountHeaderNameTranslated = "X-Account-Sysmeta-Proxyfs-Bimodal"
	AccountHeaderValue          = "true"
	CheckpointHeaderName        = "X-Container-Meta-Checkpoint"
	StoragePolicyHeaderName     = "X-Storage-Policy"
)

type bPlusTreeTrackerStruct struct {
	bPlusTreeLayout sortedmap.LayoutReport
}

type bPlusTreeWrapperStruct struct {
	volumeView       *volumeViewStruct
	bPlusTree        sortedmap.BPlusTree
	bPlusTreeTracker *bPlusTreeTrackerStruct // For inodeRecWrapper, logSegmentRecWrapper, & bPlusTreeObjectWrapper:
	//                                            only valid for liveView... nil otherwise
	//                                          For createdObjectsWrapper & deletedObjectsWrapper:
	//                                            all volumeView's share the corresponding one created for liveView
}

type volumeViewStruct struct {
	volume     *volumeStruct
	nonce      uint64 // supplies strict time-ordering of views regardless of timebase resets
	snapShotID uint64 // in the range [1:2^SnapShotIDNumBits-2]
	//                     ID == 0                     reserved for the "live" view
	//                     ID == 2^SnapShotIDNumBits-1 reserved for the .snapshot subdir of a dir
	snapShotTime           time.Time
	snapShotName           string
	inodeRecWrapper        *bPlusTreeWrapperStruct
	logSegmentRecWrapper   *bPlusTreeWrapperStruct
	bPlusTreeObjectWrapper *bPlusTreeWrapperStruct
	createdObjectsWrapper  *bPlusTreeWrapperStruct // if volumeView is     the liveView, should be empty
	//                                                if volumeView is not the liveView, tracks objects created between this and the next volumeView
	deletedObjectsWrapper *bPlusTreeWrapperStruct //  if volumeView is     the liveView, tracks objects to be deleted at next checkpoint
	//                                                if volumeView is not the liveView, tracks objects deleted between this and the next volumeView
	// upon object creation:
	//   if prior volumeView exists:
	//     add object to that volumeView's createdObjects
	//   else:
	//     do nothing
	//
	// upon object deletion:
	//   if prior volumeView exists:
	//     if object in prior volumeView createdObjects:
	//       remove object from prior volumeView createdObjects
	//       add object to liveView deletedObjects
	//     else:
	//       add object to prior volumeView deletedObjects
	//   else:
	//     add object to liveView deletedObjects
	//
	// upon next checkpoint:
	//   remove all objects from liveView deletedObjects
	//   schedule background deletion of those removed objects
	//
	// upon SnapShot creation:
	//   take a fresh checkpoint (will drain liveView deletedObjects)
	//   create a fresh volumeView
	//   take a fresh checkpoint (will record volumeView in this checkpoint)
	//
	// upon volumeView deletion:
	//   if deleted volumeView is oldest:
	//     discard volumeView createdObjects
	//     schedule background deletion of volumeView deletedObjects
	//     discard volumeView deletedObjects
	//   else:
	//     createdObjects merged into prior volumeView
	//     discard volumeView createdObjects
	//     for each object in deletedObjects:
	//       if object in prior volumeView createdObjects:
	//         remove object from prior volumeView createdObjects
	//         schedule background deletion of object
	//       else:
	//         add object to prior volumeView deletedObjects
	//     discard volumeView deletedObjects
	//   discard volumeView
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
	checkpointChunkedPutContext             swiftclient.ChunkedPutContext
	checkpointChunkedPutContextObjectNumber uint64 //             ultimately copied to CheckpointObjectTrailerStructObjectNumber
	checkpointDoneWaitGroup                 *sync.WaitGroup
	eventListeners                          map[VolumeEventListener]struct{}
	snapShotIDNumBits                       uint16
	snapShotIDShift                         uint64 //             e.g. inodeNumber >> snapShotIDNumBits == snapShotID
	dotSnapShotDirSnapShotID                uint64 //             .snapshot/ pseudo directory Inode's snapShotID
	snapShotU64NonceMask                    uint64 //             used to mask of snapShotID bits
	maxNonce                                uint64
	nextNonce                               uint64
	checkpointRequestChan                   chan *checkpointRequestStruct
	checkpointHeader                        *checkpointHeaderStruct
	liveView                                *volumeViewStruct
	priorView                               *volumeViewStruct
	postponePriorViewCreatedObjectsPuts     bool
	postponedPriorViewCreatedObjectsPuts    map[uint64]struct{}
	viewTreeByNonce                         sortedmap.LLRBTree // key == volumeViewStruct.Nonce; value == *volumeViewStruct
	viewTreeByID                            sortedmap.LLRBTree // key == volumeViewStruct.ID;    value == *volumeViewStruct
	viewTreeByTime                          sortedmap.LLRBTree // key == volumeViewStruct.Time;  value == *volumeViewStruct
	viewTreeByName                          sortedmap.LLRBTree // key == volumeViewStruct.Name;  value == *volumeViewStruct
	availableSnapShotIDList                 *list.List
	backgroundObjectDeleteWG                sync.WaitGroup
}

type globalsStruct struct {
	sync.Mutex
	crc64ECMATable                             *crc64.Table
	uint64Size                                 uint64
	elementOfBPlusTreeLayoutStructSize         uint64
	replayLogTransactionFixedPartStructSize    uint64
	inodeRecCache                              sortedmap.BPlusTreeCache
	inodeRecCachePriorCacheHits                uint64
	inodeRecCachePriorCacheMisses              uint64
	logSegmentRecCache                         sortedmap.BPlusTreeCache
	logSegmentRecCachePriorCacheHits           uint64
	logSegmentRecCachePriorCacheMisses         uint64
	bPlusTreeObjectCache                       sortedmap.BPlusTreeCache
	bPlusTreeObjectCachePriorCacheHits         uint64
	bPlusTreeObjectCachePriorCacheMisses       uint64
	createdDeletedObjectsCache                 sortedmap.BPlusTreeCache
	createdDeletedObjectsCachePriorCacheHits   uint64
	createdDeletedObjectsCachePriorCacheMisses uint64
	volumeMap                                  map[string]*volumeStruct // key == ramVolumeStruct.volumeName

	FetchNonceUsec                            bucketstats.BucketLog2Round
	GetInodeRecUsec                           bucketstats.BucketLog2Round
	GetInodeRecBytes                          bucketstats.BucketLog2Round
	PutInodeRecUsec                           bucketstats.BucketLog2Round
	PutInodeRecBytes                          bucketstats.BucketLog2Round
	PutInodeRecsUsec                          bucketstats.BucketLog2Round
	PutInodeRecsBytes                         bucketstats.BucketLog2Round
	DeleteInodeRecUsec                        bucketstats.BucketLog2Round
	IndexedInodeNumberUsec                    bucketstats.BucketLog2Round
	GetLogSegmentRecUsec                      bucketstats.BucketLog2Round
	PutLogSegmentRecUsec                      bucketstats.BucketLog2Round
	DeleteLogSegmentRecUsec                   bucketstats.BucketLog2Round
	IndexedLogSegmentNumberUsec               bucketstats.BucketLog2Round
	GetBPlusTreeObjectUsec                    bucketstats.BucketLog2Round
	GetBPlusTreeObjectBytes                   bucketstats.BucketLog2Round
	PutBPlusTreeObjectUsec                    bucketstats.BucketLog2Round
	PutBPlusTreeObjectBytes                   bucketstats.BucketLog2Round
	DeleteBPlusTreeObjectUsec                 bucketstats.BucketLog2Round
	IndexedBPlusTreeObjectNumberUsec          bucketstats.BucketLog2Round
	DoCheckpointUsec                          bucketstats.BucketLog2Round
	FetchLayoutReportUsec                     bucketstats.BucketLog2Round
	SnapShotCreateByInodeLayerUsec            bucketstats.BucketLog2Round
	SnapShotDeleteByInodeLayerUsec            bucketstats.BucketLog2Round
	SnapShotCountUsec                         bucketstats.BucketLog2Round
	SnapShotLookupByNameUsec                  bucketstats.BucketLog2Round
	SnapShotListByIDUsec                      bucketstats.BucketLog2Round
	SnapShotListByTimeUsec                    bucketstats.BucketLog2Round
	SnapShotListByNameUsec                    bucketstats.BucketLog2Round
	SnapShotU64DecodeUsec                     bucketstats.BucketLog2Round
	SnapShotIDAndNonceEncodeUsec              bucketstats.BucketLog2Round
	SnapShotTypeDotSnapShotAndNonceEncodeUsec bucketstats.BucketLog2Round

	FetchNonceErrors                   bucketstats.BucketLog2Round
	GetInodeRecErrors                  bucketstats.BucketLog2Round
	PutInodeRecErrors                  bucketstats.BucketLog2Round
	PutInodeRecsErrors                 bucketstats.BucketLog2Round
	DeleteInodeRecErrors               bucketstats.BucketLog2Round
	IndexedInodeNumberErrors           bucketstats.BucketLog2Round
	GetLogSegmentRecErrors             bucketstats.BucketLog2Round
	PutLogSegmentRecErrors             bucketstats.BucketLog2Round
	DeleteLogSegmentRecErrors          bucketstats.BucketLog2Round
	IndexedLogSegmentNumberErrors      bucketstats.BucketLog2Round
	GetBPlusTreeObjectErrors           bucketstats.BucketLog2Round
	PutBPlusTreeObjectErrors           bucketstats.BucketLog2Round
	DeleteBPlusTreeObjectErrors        bucketstats.BucketLog2Round
	IndexedBPlusTreeObjectNumberErrors bucketstats.BucketLog2Round
	DoCheckpointErrors                 bucketstats.BucketLog2Round
	FetchLayoutReportErrors            bucketstats.BucketLog2Round
	SnapShotCreateByInodeLayerErrors   bucketstats.BucketLog2Round
	SnapShotDeleteByInodeLayerErrors   bucketstats.BucketLog2Round
	SnapShotCountErrors                bucketstats.BucketLog2Round
	SnapShotLookupByNameErrors         bucketstats.BucketLog2Round
}

var globals globalsStruct

// Up starts the headhunter package
func Up(confMap conf.ConfMap) (err error) {
	var (
		primaryPeerList []string
		volumeName      string
		volumeList      []string
		whoAmI          string
	)

	bucketstats.Register("proxyfs.headhunter", "", &globals)

	err = commonInitialization(confMap)
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

	globals.volumeMap = make(map[string]*volumeStruct)

	for _, volumeName = range volumeList {
		primaryPeerList, err = confMap.FetchOptionValueStringSlice("Volume:"+volumeName, "PrimaryPeer")
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
		primaryPeerList, err = confMap.FetchOptionValueStringSlice("Volume:"+volumeName, "PrimaryPeer")
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
		primaryPeerList, err = confMap.FetchOptionValueStringSlice("Volume:"+volumeName, "PrimaryPeer")
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

	bucketstats.UnRegister("proxyfs.headhunter", "")

	err = nil
	return
}

// Format runs an instance of the headhunter package for formatting a new volume
func Format(confMap conf.ConfMap, volumeName string) (err error) {
	err = commonInitialization(confMap)
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

func commonInitialization(confMap conf.ConfMap) (err error) {
	var (
		bPlusTreeObjectCacheEvictHighLimit       uint64
		bPlusTreeObjectCacheEvictLowLimit        uint64
		createdDeletedObjectsCacheEvictHighLimit uint64
		createdDeletedObjectsCacheEvictLowLimit  uint64
		dummyElementOfBPlusTreeLayoutStruct      elementOfBPlusTreeLayoutStruct
		dummyReplayLogTransactionFixedPartStruct replayLogTransactionFixedPartStruct
		dummyUint64                              uint64
		inodeRecCacheEvictHighLimit              uint64
		inodeRecCacheEvictLowLimit               uint64
		logSegmentRecCacheEvictHighLimit         uint64
		logSegmentRecCacheEvictLowLimit          uint64
	)

	// Pre-compute crc64 ECMA Table & useful cstruct sizes

	globals.crc64ECMATable = crc64.MakeTable(crc64.ECMA)

	globals.uint64Size, _, err = cstruct.Examine(dummyUint64)
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

	// Initialize B+Tree caches

	inodeRecCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "InodeRecCacheEvictLowLimit")
	if nil != err {
		return
	}
	inodeRecCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "InodeRecCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.inodeRecCache = sortedmap.NewBPlusTreeCache(inodeRecCacheEvictLowLimit, inodeRecCacheEvictHighLimit)

	globals.inodeRecCachePriorCacheHits = 0
	globals.inodeRecCachePriorCacheMisses = 0

	logSegmentRecCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "LogSegmentRecCacheEvictLowLimit")
	if nil != err {
		return
	}
	logSegmentRecCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "LogSegmentRecCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.logSegmentRecCache = sortedmap.NewBPlusTreeCache(logSegmentRecCacheEvictLowLimit, logSegmentRecCacheEvictHighLimit)

	globals.logSegmentRecCachePriorCacheHits = 0
	globals.logSegmentRecCachePriorCacheMisses = 0

	bPlusTreeObjectCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "BPlusTreeObjectCacheEvictLowLimit")
	if nil != err {
		return
	}
	bPlusTreeObjectCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "BPlusTreeObjectCacheEvictHighLimit")
	if nil != err {
		return
	}

	globals.bPlusTreeObjectCache = sortedmap.NewBPlusTreeCache(bPlusTreeObjectCacheEvictLowLimit, bPlusTreeObjectCacheEvictHighLimit)

	globals.bPlusTreeObjectCachePriorCacheHits = 0
	globals.bPlusTreeObjectCachePriorCacheMisses = 0

	createdDeletedObjectsCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "CreatedDeletedObjectsCacheEvictLowLimit")
	if nil != err {
		createdDeletedObjectsCacheEvictLowLimit = logSegmentRecCacheEvictLowLimit // TODO: Eventually just return
	}
	createdDeletedObjectsCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "CreatedDeletedObjectsCacheEvictHighLimit")
	if nil != err {
		createdDeletedObjectsCacheEvictHighLimit = logSegmentRecCacheEvictHighLimit // TODO: Eventually just return
	}

	globals.createdDeletedObjectsCache = sortedmap.NewBPlusTreeCache(createdDeletedObjectsCacheEvictLowLimit, createdDeletedObjectsCacheEvictHighLimit)

	globals.createdDeletedObjectsCachePriorCacheHits = 0
	globals.createdDeletedObjectsCachePriorCacheMisses = 0

	err = nil
	return
}

func upVolume(confMap conf.ConfMap, volumeName string, autoFormat bool) (err error) {
	var (
		flowControlName        string
		flowControlSectionName string
		volume                 *volumeStruct
		volumeSectionName      string
	)

	volumeSectionName = "Volume:" + volumeName

	volume = &volumeStruct{
		volumeName:                           volumeName,
		checkpointChunkedPutContext:          nil,
		checkpointDoneWaitGroup:              nil,
		eventListeners:                       make(map[VolumeEventListener]struct{}),
		checkpointRequestChan:                make(chan *checkpointRequestStruct, 1),
		postponePriorViewCreatedObjectsPuts:  false,
		postponedPriorViewCreatedObjectsPuts: make(map[uint64]struct{}),
	}

	volume.accountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
	if nil != err {
		return
	}

	flowControlName, err = confMap.FetchOptionValueString(volumeSectionName, "FlowControl")
	if nil != err {
		return
	}
	flowControlSectionName = "FlowControl:" + flowControlName

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

	volume.snapShotIDNumBits, err = confMap.FetchOptionValueUint16(volumeSectionName, "SnapShotIDNumBits")
	if nil != err {
		volume.snapShotIDNumBits = 10 // TODO: Eventually just return
	}
	if 2 > volume.snapShotIDNumBits {
		err = fmt.Errorf("[%v]SnapShotIDNumBits must be at least 2", volumeSectionName)
		return
	}
	if 32 < volume.snapShotIDNumBits {
		err = fmt.Errorf("[%v]SnapShotIDNumBits must be no more than 32", volumeSectionName)
		return
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
