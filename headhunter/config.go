package headhunter

import (
	"fmt"
	"sync"
	"time"

	"github.com/swiftstack/conf"
	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/swiftclient"
)

const (
	firstNonceToProvide = uint64(2) // Must skip useful values: 0 == unassigned and 1 == RootDirInodeNumber
)

const (
	checkpointHeaderName = "X-Container-Meta-Checkpoint"
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
	inodeRecBPlusTreeObjectNumber        uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of inodeRec        B+Tree
	inodeRecBPlusTreeObjectOffset        uint64 // ...and offset into the Object where root starts
	inodeRecBPlusTreeObjectLength        uint64 // ...and length if that root node
	logSegmentRecBPlusTreeObjectNumber   uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of logSegment      B+Tree
	logSegmentRecBPlusTreeObjectOffset   uint64 // ...and offset into the Object where root starts
	logSegmentRecBPlusTreeObjectLength   uint64 // ...and length if that root node
	bPlusTreeObjectBPlusTreeObjectNumber uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of bPlusTreeObject B+Tree
	bPlusTreeObjectBPlusTreeObjectOffset uint64 // ...and offset into the Object where root starts
	bPlusTreeObjectBPlusTreeObjectLength uint64 // ...and length if that root node
	reservedToNonce                      uint64 // highest nonce value reserved
}

type checkpointHeaderV2Struct struct {
	checkpointObjectTrailerV2StructObjectNumber uint64 // checkpointObjectTrailerV2Struct found at "tail" of object
	checkpointObjectTrailerV2StructObjectLength uint64 // this length includes the three B+Tree "layouts" appended
	reservedToNonce                             uint64 // highest nonce value reserved
}

type checkpointObjectTrailerV2Struct struct {
	inodeRecBPlusTreeObjectNumber             uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of inodeRec        B+Tree
	inodeRecBPlusTreeObjectOffset             uint64 // ...and offset into the Object where root starts
	inodeRecBPlusTreeObjectLength             uint64 // ...and length if that root node
	inodeRecBPlusTreeLayoutNumElements        uint64 // elements immediately follow checkpointObjectTrailerV2Struct
	logSegmentRecBPlusTreeObjectNumber        uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of logSegment      B+Tree
	logSegmentRecBPlusTreeObjectOffset        uint64 // ...and offset into the Object where root starts
	logSegmentRecBPlusTreeObjectLength        uint64 // ...and length if that root node
	logSegmentRecBPlusTreeLayoutNumElements   uint64 // elements immediately follow inodeRecBPlusTreeLayout
	bPlusTreeObjectBPlusTreeObjectNumber      uint64 // if != 0, objectNumber-named Object in <accountName>.<checkpointContainerName> where root of bPlusTreeObject B+Tree
	bPlusTreeObjectBPlusTreeObjectOffset      uint64 // ...and offset into the Object where root starts
	bPlusTreeObjectBPlusTreeObjectLength      uint64 // ...and length if that root node
	bPlusTreeObjectBPlusTreeLayoutNumElements uint64 // elements immediately follow logSegmentRecBPlusTreeLayout
	// inodeRecBPlusTreeLayout        serialized as [inodeRecBPlusTreeLayoutNumElements       ]elementOfBPlusTreeLayoutStruct
	// logSegmentBPlusTreeLayout      serialized as [logSegmentRecBPlusTreeLayoutNumElements  ]elementOfBPlusTreeLayoutStruct
	// bPlusTreeObjectBPlusTreeLayout serialized as [bPlusTreeObjectBPlusTreeLayoutNumElements]elementOfBPlusTreeLayoutStruct
}

type elementOfBPlusTreeLayoutStruct struct {
	objectNumber uint64
	objectBytes  uint64
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
	volumeMap                          map[string]*volumeStruct // key == ramVolumeStruct.volumeName
	checkpointObjectTrailerStructSize  uint64
	elementOfBPlusTreeLayoutStructSize uint64
}

var globals globalsStruct

// Up starts the headhunter package
func Up(confMap conf.ConfMap) (err error) {
	var (
		dummyCheckpointObjectTrailerV2Struct checkpointObjectTrailerV2Struct
		dummyElementOfBPlusTreeLayoutStruct  elementOfBPlusTreeLayoutStruct
		primaryPeer                          string
		trailingByteSlice                    bool
		volumeName                           string
		volumeNames                          []string
		whoAmI                               string
	)

	// Init volume database(s)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	volumeNames, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	globals.volumeMap = make(map[string]*volumeStruct)

	for _, volumeName = range volumeNames {
		primaryPeer, err = confMap.FetchOptionValueString(volumeName, "PrimaryPeer")
		if nil != err {
			return
		}

		if whoAmI == primaryPeer {
			err = addVolume(confMap, volumeName)
			if nil != err {
				return
			}
		}
	}

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

	return
}

// PauseAndContract pauses the headhunter package and applies any removals from the supplied confMap
func PauseAndContract(confMap conf.ConfMap) (err error) {
	var (
		deletedVolumeNames map[string]bool
		primaryPeer        string
		volumeName         string
		volumeNames        []string
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

	volumeNames, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	for _, volumeName = range volumeNames {
		primaryPeer, err = confMap.FetchOptionValueString(volumeName, "PrimaryPeer")
		if nil != err {
			return
		}

		if whoAmI == primaryPeer {
			delete(deletedVolumeNames, volumeName)
		}
	}

	for volumeName = range deletedVolumeNames {
		err = addVolume(confMap, volumeName)
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
		ok          bool
		primaryPeer string
		volumeName  string
		volumeNames []string
		whoAmI      string
	)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	volumeNames, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	for _, volumeName = range volumeNames {
		primaryPeer, err = confMap.FetchOptionValueString(volumeName, "PrimaryPeer")
		if nil != err {
			return
		}

		if whoAmI == primaryPeer {
			_, ok = globals.volumeMap[volumeName]
			if !ok {
				err = downVolume(volumeName)
				if nil != err {
					return
				}
			}
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

func addVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		flowControlName string
		volume          *volumeStruct
	)

	volume = &volumeStruct{
		volumeName:                  volumeName,
		checkpointChunkedPutContext: nil,
		checkpointDoneWaitGroup:     nil,
		checkpointRequestChan:       make(chan *checkpointRequestStruct, 1),
	}

	volume.inodeRecWrapper = &bPlusTreeWrapperStruct{volume: volume, wrapperType: inodeRecBPlusTreeWrapperType}
	volume.logSegmentRecWrapper = &bPlusTreeWrapperStruct{volume: volume, wrapperType: logSegmentRecBPlusTreeWrapperType}
	volume.bPlusTreeObjectWrapper = &bPlusTreeWrapperStruct{volume: volume, wrapperType: bPlusTreeObjectBPlusTreeWrapperType}

	volume.accountName, err = confMap.FetchOptionValueString(volumeName, "AccountName")
	if nil != err {
		return
	}

	flowControlName, err = confMap.FetchOptionValueString(volumeName, "FlowControl")
	if nil != err {
		return
	}

	volume.maxFlushSize, err = confMap.FetchOptionValueUint64(flowControlName, "MaxFlushSize")
	if nil != err {
		return
	}

	volume.nonceValuesToReserve, err = confMap.FetchOptionValueUint16(volumeName, "NonceValuesToReserve")
	if nil != err {
		return
	}

	volume.maxInodesPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeName, "MaxInodesPerMetadataNode")
	if nil != err {
		// TODO: eventually, just return
		volume.maxInodesPerMetadataNode = 32
	}

	volume.maxLogSegmentsPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeName, "MaxLogSegmentsPerMetadataNode")
	if nil != err {
		// TODO: eventually, just return
		volume.maxLogSegmentsPerMetadataNode = 64
	}

	volume.maxDirFileNodesPerMetadataNode, err = confMap.FetchOptionValueUint64(volumeName, "MaxDirFileNodesPerMetadataNode")
	if nil != err {
		// TODO: eventually, just return
		volume.maxDirFileNodesPerMetadataNode = 16
	}

	volume.checkpointContainerName, err = confMap.FetchOptionValueString(volumeName, "CheckpointContainerName")
	if nil != err {
		return
	}

	volume.checkpointInterval, err = confMap.FetchOptionValueDuration(volumeName, "CheckpointInterval")
	if nil != err {
		return
	}

	err = volume.getCheckpoint()
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
