package headhunter

import (
	"fmt"
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
)

var (
	LittleEndian = cstruct.LittleEndian // All data cstructs to be serialized in LittleEndian form
)

const (
	checkpointHeaderVersion1 uint64 = iota + 1
	// uint64 in %016X indicating checkpointHeaderVersion1
	// ' '
	// uint64 in %016X indicating objectNumber containing inodeRecWrapper.bPlusTree        Root Node
	// ' '
	// uint64 in %016X indicating offset of               inodeRecWrapper.bPlusTree        Root Node
	// ' '
	// uint64 in %016X indicating length of               inodeRecWrapper.bPlusTree        Root Node
	// ' '
	// uint64 in %016X indicating objectNumber containing logSegmentRecWrapper.bPlusTree   Root Node
	// ' '
	// uint64 in %016X indicating offset of               logSegmentRecWrapper.bPlusTree   Root Node
	// ' '
	// uint64 in %016X indicating length of               logSegmentRecWrapper.bPlusTree   Root Node
	// ' '
	// uint64 in %016X indicating objectNumber containing bPlusTreeObjectWrapper.bPlusTree Root Node
	// ' '
	// uint64 in %016X indicating offset of               bPlusTreeObjectWrapper.bPlusTree Root Node
	// ' '
	// uint64 in %016X indicating length of               bPlusTreeObjectWrapper.bPlusTree Root Node
	// ' '
	// uint64 in %016X indicating reservedToNonce

	checkpointHeaderVersion2
	// uint64 in %016X indicating checkpointHeaderVersion2
	// ' '
	// uint64 in %016X indicating objectNumber containing checkpoint record at tail of object
	// ' '
	// uint64 in %016X indicating length of               checkpoint record at tail of object
	// ' '
	// uint64 in %016X indicating reservedToNonce
)

type checkpointHeaderV1Struct struct {
	InodeRecBPlusTreeObjectNumber        uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of inodeRec        B+Tree
	InodeRecBPlusTreeObjectOffset        uint64 // ...and offset into the Object where root starts
	InodeRecBPlusTreeObjectLength        uint64 // ...and length if that root node
	LogSegmentRecBPlusTreeObjectNumber   uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of logSegment      B+Tree
	LogSegmentRecBPlusTreeObjectOffset   uint64 // ...and offset into the Object where root starts
	LogSegmentRecBPlusTreeObjectLength   uint64 // ...and length if that root node
	BPlusTreeObjectBPlusTreeObjectNumber uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of bPlusTreeObject B+Tree
	BPlusTreeObjectBPlusTreeObjectOffset uint64 // ...and offset into the Object where root starts
	BPlusTreeObjectBPlusTreeObjectLength uint64 // ...and length if that root node
	ReservedToNonce                      uint64 // highest nonce value reserved
}

type checkpointHeaderV2Struct struct {
	CheckpointObjectTrailerV2StructObjectNumber uint64 // checkpointObjectTrailerV2Struct found at "tail" of object
	CheckpointObjectTrailerV2StructObjectLength uint64 // this length includes the three B+Tree "layouts" appended
	ReservedToNonce                             uint64 // highest nonce value reserved
}

type checkpointObjectTrailerV2Struct struct {
	InodeRecBPlusTreeObjectNumber             uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of inodeRec        B+Tree
	InodeRecBPlusTreeObjectOffset             uint64 // ...and offset into the Object where root starts
	InodeRecBPlusTreeObjectLength             uint64 // ...and length if that root node
	InodeRecBPlusTreeLayoutNumElements        uint64 // elements immediately follow checkpointObjectTrailerV2Struct
	LogSegmentRecBPlusTreeObjectNumber        uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of logSegment      B+Tree
	LogSegmentRecBPlusTreeObjectOffset        uint64 // ...and offset into the Object where root starts
	LogSegmentRecBPlusTreeObjectLength        uint64 // ...and length if that root node
	LogSegmentRecBPlusTreeLayoutNumElements   uint64 // elements immediately follow inodeRecBPlusTreeLayout
	BPlusTreeObjectBPlusTreeObjectNumber      uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of bPlusTreeObject B+Tree
	BPlusTreeObjectBPlusTreeObjectOffset      uint64 // ...and offset into the Object where root starts
	BPlusTreeObjectBPlusTreeObjectLength      uint64 // ...and length if that root node
	BPlusTreeObjectBPlusTreeLayoutNumElements uint64 // elements immediately follow logSegmentRecBPlusTreeLayout
	// inodeRecBPlusTreeLayout        serialized as [inodeRecBPlusTreeLayoutNumElements       ]elementOfBPlusTreeLayoutStruct
	// logSegmentBPlusTreeLayout      serialized as [logSegmentRecBPlusTreeLayoutNumElements  ]elementOfBPlusTreeLayoutStruct
	// bPlusTreeObjectBPlusTreeLayout serialized as [bPlusTreeObjectBPlusTreeLayoutNumElements]elementOfBPlusTreeLayoutStruct
}

type elementOfBPlusTreeLayoutStruct struct {
	ObjectNumber uint64
	ObjectBytes  uint64
}

const (
	inodeRecBPlusTreeWrapperType uint32 = iota
	logSegmentRecBPlusTreeWrapperType
	bPlusTreeObjectBPlusTreeWrapperType
)

type bPlusTreeWrapperStruct struct {
	volume      *volumeStruct
	wrapperType uint32 // Either inodeRecBPlusTreeWrapperType, logSegmentRecBPlusTreeWrapperType, or bPlusTreeObjectBPlusTreeWrapperType
	bPlusTree   sortedmap.BPlusTree
}

type checkpointRequestStruct struct {
	waitGroup        sync.WaitGroup
	err              error
	exitOnCompletion bool
}

type volumeStruct struct {
	sync.Mutex
	volumeName                     string
	accountName                    string
	maxFlushSize                   uint64
	nonceValuesToReserve           uint16
	maxInodesPerMetadataNode       uint64
	maxLogSegmentsPerMetadataNode  uint64
	maxDirFileNodesPerMetadataNode uint64
	checkpointContainerName        string
	checkpointInterval             time.Duration
	checkpointFlushedData          bool
	checkpointChunkedPutContext    swiftclient.ChunkedPutContext
	checkpointDoneWaitGroup        *sync.WaitGroup
	nextNonce                      uint64
	checkpointRequestChan          chan *checkpointRequestStruct
	checkpointHeaderVersion        uint64
	checkpointHeader               *checkpointHeaderV2Struct
	checkpointObjectTrailer        *checkpointObjectTrailerV2Struct
	inodeRecWrapper                *bPlusTreeWrapperStruct
	logSegmentRecWrapper           *bPlusTreeWrapperStruct
	bPlusTreeObjectWrapper         *bPlusTreeWrapperStruct
	inodeRecBPlusTreeLayout        sortedmap.LayoutReport
	logSegmentRecBPlusTreeLayout   sortedmap.LayoutReport
	bPlusTreeObjectBPlusTreeLayout sortedmap.LayoutReport
}

type globalsStruct struct {
	checkpointObjectTrailerStructSize  uint64
	elementOfBPlusTreeLayoutStructSize uint64
	inodeRecCache                      sortedmap.BPlusTreeCache
	logSegmentRecCache                 sortedmap.BPlusTreeCache
	bPlusTreeObjectCache               sortedmap.BPlusTreeCache
	volumeMap                          map[string]*volumeStruct // key == ramVolumeStruct.volumeName
}

var globals globalsStruct

// Up starts the headhunter package
func Up(confMap conf.ConfMap) (err error) {
	var (
		bPlusTreeObjectCacheEvictHighLimit   uint64
		bPlusTreeObjectCacheEvictLowLimit    uint64
		dummyCheckpointObjectTrailerV2Struct checkpointObjectTrailerV2Struct
		dummyElementOfBPlusTreeLayoutStruct  elementOfBPlusTreeLayoutStruct
		inodeRecCacheEvictHighLimit          uint64
		inodeRecCacheEvictLowLimit           uint64
		logSegmentRecCacheEvictHighLimit     uint64
		logSegmentRecCacheEvictLowLimit      uint64
		primaryPeerList                      []string
		trailingByteSlice                    bool
		volumeName                           string
		volumeList                           []string
		whoAmI                               string
	)

	// Pre-compute sizeof(checkpointObjectTrailerV2Struct) & sizeof(elementOfBPlusTreeLayoutStruct)

	globals.checkpointObjectTrailerStructSize, trailingByteSlice, err = cstruct.Examine(dummyCheckpointObjectTrailerV2Struct)
	if nil != err {
		return
	}
	if trailingByteSlice {
		err = fmt.Errorf("Logic error: cstruct.Examine(checkpointObjectTrailerV2Struct) returned trailingByteSlice == true")
		return
	}

	globals.elementOfBPlusTreeLayoutStructSize, trailingByteSlice, err = cstruct.Examine(dummyElementOfBPlusTreeLayoutStruct)
	if nil != err {
		return
	}
	if trailingByteSlice {
		err = fmt.Errorf("Logic error: cstruct.Examine(elementOfBPlusTreeLayoutStruct) returned trailingByteSlice == true")
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
		// TODO: eventually, just return
		inodeRecCacheEvictLowLimit = 10000
		err = nil
	}
	inodeRecCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "InodeRecCacheEvictHighLimit")
	if nil != err {
		// TODO: eventually, just return
		inodeRecCacheEvictHighLimit = 10010
		err = nil
	}

	globals.inodeRecCache = sortedmap.NewBPlusTreeCache(inodeRecCacheEvictLowLimit, inodeRecCacheEvictHighLimit)

	logSegmentRecCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "LogSegmentRecCacheEvictLowLimit")
	if nil != err {
		// TODO: eventually, just return
		logSegmentRecCacheEvictLowLimit = 10000
		err = nil
	}
	logSegmentRecCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "LogSegmentRecCacheEvictHighLimit")
	if nil != err {
		// TODO: eventually, just return
		logSegmentRecCacheEvictHighLimit = 10010
		err = nil
	}

	globals.logSegmentRecCache = sortedmap.NewBPlusTreeCache(logSegmentRecCacheEvictLowLimit, logSegmentRecCacheEvictHighLimit)

	bPlusTreeObjectCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "BPlusTreeObjectCacheEvictLowLimit")
	if nil != err {
		// TODO: eventually, just return
		bPlusTreeObjectCacheEvictLowLimit = 10000
		err = nil
	}
	bPlusTreeObjectCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSGlobals", "BPlusTreeObjectCacheEvictHighLimit")
	if nil != err {
		// TODO: eventually, just return
		bPlusTreeObjectCacheEvictHighLimit = 10010
		err = nil
	}

	globals.bPlusTreeObjectCache = sortedmap.NewBPlusTreeCache(bPlusTreeObjectCacheEvictLowLimit, bPlusTreeObjectCacheEvictHighLimit)

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
				err = upVolume(confMap, volumeName, true) // TODO: ultimately change this to false
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
		err = upVolume(confMap, volumeName, true) // TODO: ultimately change this to false
		if nil != err {
			return
		}
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
					err = upVolume(confMap, volumeName, true) // TODO: ultimately change this to false
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
		dummyCheckpointObjectTrailerV2Struct checkpointObjectTrailerV2Struct
		dummyElementOfBPlusTreeLayoutStruct  elementOfBPlusTreeLayoutStruct
		trailingByteSlice                    bool
	)

	// Pre-compute sizeof(checkpointObjectTrailerV2Struct) & sizeof(elementOfBPlusTreeLayoutStruct)

	globals.checkpointObjectTrailerStructSize, trailingByteSlice, err = cstruct.Examine(dummyCheckpointObjectTrailerV2Struct)
	if nil != err {
		return
	}
	if trailingByteSlice {
		err = fmt.Errorf("Logic error: cstruct.Examine(checkpointObjectTrailerV2Struct) returned trailingByteSlice == true")
		return
	}

	globals.elementOfBPlusTreeLayoutStructSize, trailingByteSlice, err = cstruct.Examine(dummyElementOfBPlusTreeLayoutStruct)
	if nil != err {
		return
	}
	if trailingByteSlice {
		err = fmt.Errorf("Logic error: cstruct.Examine(elementOfBPlusTreeLayoutStruct) returned trailingByteSlice == true")
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

// TODO: allowFormat should change to doFormat when controller/runway pre-formats
func upVolume(confMap conf.ConfMap, volumeName string, allowFormat bool) (err error) {
	var (
		flowControlName        string
		flowControlSectionName string
		volume                 *volumeStruct
		volumeSectionName      string
	)

	volumeSectionName = utils.VolumeNameConfSection(volumeName)

	volume = &volumeStruct{
		volumeName:                  volumeName,
		checkpointChunkedPutContext: nil,
		checkpointDoneWaitGroup:     nil,
		checkpointRequestChan:       make(chan *checkpointRequestStruct, 1),
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

	volume.checkpointContainerName, err = confMap.FetchOptionValueString(volumeSectionName, "CheckpointContainerName")
	if nil != err {
		return
	}

	volume.checkpointInterval, err = confMap.FetchOptionValueDuration(volumeSectionName, "CheckpointInterval")
	if nil != err {
		return
	}

	err = volume.getCheckpoint(allowFormat) // TODO: change to doFormat ultimately
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

	return
}
