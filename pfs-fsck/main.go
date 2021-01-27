// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"

	"github.com/swiftstack/cstruct"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/etcdclient"
	"github.com/swiftstack/ProxyFS/headhunter"
)

const (
	bPlusTreeCacheEvictHighLimitDefault = uint64(10010)
	bPlusTreeCacheEvictLowLimitDefault  = uint64(10000)
)

type globalsStruct struct {
	accountName                        string
	alsoDump                           bool
	bPlusTreeCache                     sortedmap.BPlusTreeCache
	bPlusTreeCacheEvictHighLimit       uint64 // [FSCK]BPlusTreeCacheEvictHighLimit
	bPlusTreeCacheEvictLowLimit        uint64 // [FSCK]BPlusTreeCacheEvictLowLimit
	bPlusTreeObjectBPlusTreeLayout     sortedmap.LayoutReport
	checkpointEtcdKeyName              string
	checkpointContainerName            string
	checkpointObjectTrailerV3          *headhunter.CheckpointObjectTrailerV3Struct
	checkpointObjectTrailerV3Buf       []byte
	checkpointObjectTrailerV3BufPos    uint64
	createdObjectsBPlusTreeLayout      sortedmap.LayoutReport
	deletedObjectsBPlusTreeLayout      sortedmap.LayoutReport
	elementOfBPlusTreeLayoutStructSize uint64
	etcdAutoSyncInterval               time.Duration
	etcdClient                         *etcd.Client
	etcdDialTimeout                    time.Duration
	etcdEnabled                        bool
	etcdEndpoints                      []string
	etcdKV                             etcd.KV
	etcdOpTimeout                      time.Duration
	initialCheckpointHeader            *headhunter.CheckpointHeaderStruct
	inodeRecBPlusTreeLayout            sortedmap.LayoutReport
	logSegmentBPlusTreeLayout          sortedmap.LayoutReport
	noAuthIPAddr                       string
	noAuthTCPPort                      uint16
	noAuthURL                          string
	recycleBinRefMap                   map[string]uint64 // Key: object path ; Value: # references
	snapShotList                       []*headhunter.ElementOfSnapShotListStruct
	uint64Size                         uint64
	volumeName                         string
}

var globals globalsStruct

func main() {
	var (
		bPlusTree                  sortedmap.BPlusTree
		elementOfSnapShotList      *headhunter.ElementOfSnapShotListStruct
		err                        error
		recycleBinObjectName       string
		recycleBinObjectReferences uint64
	)

	setup()

	globals.initialCheckpointHeader = fetchCheckpointHeader()

	globals.recycleBinRefMap = make(map[string]uint64)

	err = fetchCheckpointObjectTrailerV3()
	if nil != err {
		log.Fatal(err)
	}

	globals.inodeRecBPlusTreeLayout, err = fetchBPlusTreeLayout(globals.checkpointObjectTrailerV3.InodeRecBPlusTreeLayoutNumElements)
	if nil != err {
		log.Fatalf("fetching inodeRecBPlusTreeLayout failed: %v", err)
	}
	globals.logSegmentBPlusTreeLayout, err = fetchBPlusTreeLayout(globals.checkpointObjectTrailerV3.LogSegmentRecBPlusTreeLayoutNumElements)
	if nil != err {
		log.Fatalf("fetching logSegmentBPlusTreeLayout failed: %v", err)
	}
	globals.bPlusTreeObjectBPlusTreeLayout, err = fetchBPlusTreeLayout(globals.checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeLayoutNumElements)
	if nil != err {
		log.Fatalf("fetching bPlusTreeObjectBPlusTreeLayout failed: %v", err)
	}
	globals.createdObjectsBPlusTreeLayout, err = fetchBPlusTreeLayout(globals.checkpointObjectTrailerV3.CreatedObjectsBPlusTreeLayoutNumElements)
	if nil != err {
		log.Fatalf("fetching createdObjectsBPlusTreeLayout failed: %v", err)
	}
	globals.deletedObjectsBPlusTreeLayout, err = fetchBPlusTreeLayout(globals.checkpointObjectTrailerV3.DeletedObjectsBPlusTreeLayoutNumElements)
	if nil != err {
		log.Fatalf("fetching deletedObjectsBPlusTreeLayout failed: %v", err)
	}

	err = fetchSnapShotList(globals.checkpointObjectTrailerV3.SnapShotListNumElements)
	if nil != err {
		log.Fatalf("fetching snapShotList failed: %v", err)
	}

	if globals.checkpointObjectTrailerV3BufPos < globals.initialCheckpointHeader.CheckpointObjectTrailerStructObjectLength {
		log.Fatalf("unmarshalling checkpointObjectTrailerV3Buf not fully consumed")
	}

	globals.bPlusTreeCache = sortedmap.NewBPlusTreeCache(globals.bPlusTreeCacheEvictLowLimit, globals.bPlusTreeCacheEvictHighLimit)

	if uint64(0) != globals.checkpointObjectTrailerV3.InodeRecBPlusTreeObjectNumber {
		bPlusTree, err = sortedmap.OldBPlusTree(globals.checkpointObjectTrailerV3.InodeRecBPlusTreeObjectNumber,
			globals.checkpointObjectTrailerV3.InodeRecBPlusTreeObjectOffset,
			globals.checkpointObjectTrailerV3.InodeRecBPlusTreeObjectLength,
			sortedmap.CompareUint64,
			&globals,
			globals.bPlusTreeCache)
		if nil != err {
			log.Fatalf("loading of InodeRecBPlusTree failed: %v", err)
		}

		err = bPlusTree.Validate()
		if nil != err {
			log.Fatalf("validate of InodeRecBPlusTree failed: %v", err)
		}

		dumpKeysIfRequested(bPlusTree, "InodeRecBPlusTree")
	}

	if uint64(0) != globals.checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectNumber {
		bPlusTree, err = sortedmap.OldBPlusTree(globals.checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectNumber,
			globals.checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectOffset,
			globals.checkpointObjectTrailerV3.LogSegmentRecBPlusTreeObjectLength,
			sortedmap.CompareUint64,
			&globals,
			globals.bPlusTreeCache)
		if nil != err {
			log.Fatalf("loading of LogSegmentRecBPlusTree failed: %v", err)
		}

		err = bPlusTree.Validate()
		if nil != err {
			log.Fatalf("validate of LogSegmentRecBPlusTree failed: %v", err)
		}

		dumpKeysIfRequested(bPlusTree, "LogSegmentRecBPlusTree")
	}

	if uint64(0) != globals.checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectNumber {
		bPlusTree, err = sortedmap.OldBPlusTree(globals.checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectNumber,
			globals.checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectOffset,
			globals.checkpointObjectTrailerV3.BPlusTreeObjectBPlusTreeObjectLength,
			sortedmap.CompareUint64,
			&globals,
			globals.bPlusTreeCache)
		if nil != err {
			log.Fatalf("loading of BPlusTreeObjectBPlusTree failed: %v", err)
		}

		err = bPlusTree.Validate()
		if nil != err {
			log.Fatalf("validate of BPlusTreeObjectBPlusTree failed: %v", err)
		}

		dumpKeysIfRequested(bPlusTree, "BPlusTreeObjectBPlusTree")
	}

	for _, elementOfSnapShotList = range globals.snapShotList {
		if uint64(0) != elementOfSnapShotList.InodeRecBPlusTreeObjectNumber {
			bPlusTree, err = sortedmap.OldBPlusTree(elementOfSnapShotList.InodeRecBPlusTreeObjectNumber,
				elementOfSnapShotList.InodeRecBPlusTreeObjectOffset,
				elementOfSnapShotList.InodeRecBPlusTreeObjectLength,
				sortedmap.CompareUint64,
				&globals,
				globals.bPlusTreeCache)
			if nil != err {
				log.Fatalf("loading of SnapShot.ID==%d InodeRecBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			err = bPlusTree.Validate()
			if nil != err {
				log.Fatalf("validate of SnapShot.ID==%d InodeRecBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			dumpKeysIfRequested(bPlusTree, fmt.Sprintf("InodeRecBPlusTree for SnapShotID==%d", elementOfSnapShotList.ID))
		}

		if uint64(0) != elementOfSnapShotList.LogSegmentRecBPlusTreeObjectNumber {
			bPlusTree, err = sortedmap.OldBPlusTree(elementOfSnapShotList.LogSegmentRecBPlusTreeObjectNumber,
				elementOfSnapShotList.LogSegmentRecBPlusTreeObjectOffset,
				elementOfSnapShotList.LogSegmentRecBPlusTreeObjectLength,
				sortedmap.CompareUint64,
				&globals,
				globals.bPlusTreeCache)
			if nil != err {
				log.Fatalf("loading of SnapShot.ID==%d LogSegmentRecBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			err = bPlusTree.Validate()
			if nil != err {
				log.Fatalf("validate of SnapShot.ID==%d LogSegmentRecBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			dumpKeysIfRequested(bPlusTree, fmt.Sprintf("LogSegmentRecBPlusTree for SnapShotID==%d", elementOfSnapShotList.ID))
		}

		if uint64(0) != elementOfSnapShotList.BPlusTreeObjectBPlusTreeObjectNumber {
			bPlusTree, err = sortedmap.OldBPlusTree(elementOfSnapShotList.BPlusTreeObjectBPlusTreeObjectNumber,
				elementOfSnapShotList.BPlusTreeObjectBPlusTreeObjectOffset,
				elementOfSnapShotList.BPlusTreeObjectBPlusTreeObjectLength,
				sortedmap.CompareUint64,
				&globals,
				globals.bPlusTreeCache)
			if nil != err {
				log.Fatalf("loading of SnapShot.ID==%d BPlusTreeObjectBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			err = bPlusTree.Validate()
			if nil != err {
				log.Fatalf("validate of SnapShot.ID==%d BPlusTreeObjectBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			dumpKeysIfRequested(bPlusTree, fmt.Sprintf("BPlusTreeObjectBPlusTree for SnapShotID==%d", elementOfSnapShotList.ID))
		}

		if uint64(0) != elementOfSnapShotList.CreatedObjectsBPlusTreeObjectNumber {
			bPlusTree, err = sortedmap.OldBPlusTree(elementOfSnapShotList.CreatedObjectsBPlusTreeObjectNumber,
				elementOfSnapShotList.CreatedObjectsBPlusTreeObjectOffset,
				elementOfSnapShotList.CreatedObjectsBPlusTreeObjectLength,
				sortedmap.CompareUint64,
				&globals,
				globals.bPlusTreeCache)
			if nil != err {
				log.Fatalf("loading of SnapShot.ID==%d CreatedObjectsBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			err = bPlusTree.Validate()
			if nil != err {
				log.Fatalf("validate of SnapShot.ID==%d CreatedObjectsBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			dumpKeysIfRequested(bPlusTree, fmt.Sprintf("CreatedObjectsBPlusTree for SnapShotID==%d", elementOfSnapShotList.ID))
		}

		if uint64(0) != elementOfSnapShotList.DeletedObjectsBPlusTreeObjectNumber {
			bPlusTree, err = sortedmap.OldBPlusTree(elementOfSnapShotList.DeletedObjectsBPlusTreeObjectNumber,
				elementOfSnapShotList.DeletedObjectsBPlusTreeObjectOffset,
				elementOfSnapShotList.DeletedObjectsBPlusTreeObjectLength,
				sortedmap.CompareUint64,
				&globals,
				globals.bPlusTreeCache)
			if nil != err {
				log.Fatalf("loading of SnapShot.ID==%d DeletedObjectsBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			err = bPlusTree.Validate()
			if nil != err {
				log.Fatalf("validate of SnapShot.ID==%d DeletedObjectsBPlusTree failed: %v", elementOfSnapShotList.ID, err)
			}

			dumpKeysIfRequested(bPlusTree, fmt.Sprintf("DeletedObjectsBPlusTree for SnapShotID==%d", elementOfSnapShotList.ID))
		}
	}

	// TODO: Validate InodeRec, LogSegmentRec, BPlusTreeObject, CreatedObjects, & DeletedObjects B+Tree Layouts

	if 0 < len(globals.recycleBinRefMap) {
		for recycleBinObjectName, recycleBinObjectReferences = range globals.recycleBinRefMap {
			log.Printf("Recycled Object %s referenced %d times", recycleBinObjectName, recycleBinObjectReferences)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Printf("%v [-D] <volumeName> <confFile> [<confOverride>]*\n", os.Args[0])
}

func setup() {
	var (
		confFile                      string
		confMap                       conf.ConfMap
		confOverrides                 []string
		dummyElementOfBPlusTreeLayout headhunter.ElementOfBPlusTreeLayoutStruct
		dummyUint64                   uint64
		err                           error
		volumeSectionName             string
	)

	if 3 > len(os.Args) {
		usage()
		os.Exit(1)
	}

	if "-D" == os.Args[1] {
		globals.alsoDump = true

		if 4 > len(os.Args) {
			usage()
			os.Exit(1)
		}

		globals.volumeName = os.Args[2]
		confFile = os.Args[3]
		confOverrides = os.Args[4:]
	} else {
		globals.alsoDump = false

		globals.volumeName = os.Args[1]
		confFile = os.Args[2]
		confOverrides = os.Args[3:]
	}

	volumeSectionName = "Volume:" + globals.volumeName

	confMap, err = conf.MakeConfMapFromFile(confFile)
	if nil != err {
		log.Fatalf("confFile (\"%s\") not parseable: %v", confFile, err)
	}

	err = confMap.UpdateFromStrings(confOverrides)
	if nil != err {
		log.Fatalf("confOverrides (%v) not parseable: %v", confOverrides, err)
	}

	globals.noAuthIPAddr, err = confMap.FetchOptionValueString("SwiftClient", "NoAuthIPAddr")
	if nil != err {
		globals.noAuthIPAddr = "127.0.0.1" // TODO: Eventually just return
	}
	globals.noAuthTCPPort, err = confMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		log.Fatal(err)
	}

	globals.noAuthURL = "http://" + net.JoinHostPort(globals.noAuthIPAddr, fmt.Sprintf("%d", globals.noAuthTCPPort)) + "/"

	globals.accountName, err = confMap.FetchOptionValueString(volumeSectionName, "AccountName")
	if nil != err {
		log.Fatal(err)
	}
	globals.checkpointContainerName, err = confMap.FetchOptionValueString(volumeSectionName, "CheckpointContainerName")
	if nil != err {
		log.Fatal(err)
	}

	globals.etcdEnabled, err = confMap.FetchOptionValueBool("FSGlobals", "EtcdEnabled")
	if nil != err {
		globals.etcdEnabled = false // TODO: Current default... perhaps eventually just log.Fatal(err)
	}

	if globals.etcdEnabled {
		globals.etcdAutoSyncInterval, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdAutoSyncInterval")
		if nil != err {
			log.Fatal(err)
		}
		globals.etcdDialTimeout, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdDialTimeout")
		if nil != err {
			log.Fatal(err)
		}
		globals.etcdEndpoints, err = confMap.FetchOptionValueStringSlice("FSGlobals", "EtcdEndpoints")
		if nil != err {
			log.Fatal(err)
		}
		globals.etcdOpTimeout, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdOpTimeout")
		if nil != err {
			log.Fatal(err)
		}

		tlsInfo := transport.TLSInfo{
			CertFile:      etcdclient.GetCertFile(),
			KeyFile:       etcdclient.GetKeyFile(),
			TrustedCAFile: etcdclient.GetCA(),
		}

		globals.etcdClient, err = etcdclient.New(&tlsInfo, globals.etcdEndpoints,
			globals.etcdAutoSyncInterval, globals.etcdDialTimeout)
		if nil != err {
			log.Fatalf("unable to create etcdClient: %v\n", err)
		}

		globals.etcdKV = etcd.NewKV(globals.etcdClient)

		globals.checkpointEtcdKeyName, err = confMap.FetchOptionValueString(volumeSectionName, "CheckpointEtcdKeyName")
	}

	globals.bPlusTreeCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("FSCK", "BPlusTreeCacheEvictLowLimit")
	if nil != err {
		globals.bPlusTreeCacheEvictLowLimit = bPlusTreeCacheEvictLowLimitDefault
	}
	globals.bPlusTreeCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("FSCK", "BPlusTreeCacheEvictHighLimit")
	if nil != err {
		globals.bPlusTreeCacheEvictHighLimit = bPlusTreeCacheEvictHighLimitDefault
	}

	globals.elementOfBPlusTreeLayoutStructSize, _, err = cstruct.Examine(&dummyElementOfBPlusTreeLayout)
	if nil != err {
		log.Fatal(err)
	}
	globals.uint64Size, _, err = cstruct.Examine(&dummyUint64)
	if nil != err {
		log.Fatal(err)
	}
}

func fetchCheckpointHeader() (checkpointHeader *headhunter.CheckpointHeaderStruct) {
	var (
		cancel                               context.CancelFunc
		checkpointContainerHeaderMap         http.Header
		checkpointContainerHeaderString      string
		checkpointContainerHeaderStringSlice []string
		checkpointContainerHeaderStringSplit []string
		ctx                                  context.Context
		err                                  error
		etcdGetResponse                      *etcd.GetResponse
		ok                                   bool
	)

	checkpointHeader = &headhunter.CheckpointHeaderStruct{}

	if globals.etcdEnabled {
		ctx, cancel = context.WithTimeout(context.Background(), globals.etcdOpTimeout)
		etcdGetResponse, err = globals.etcdKV.Get(ctx, globals.checkpointEtcdKeyName)
		cancel()
		if nil != err {
			log.Fatalf("error contacting etcd: %v", err)
		}

		if 1 == etcdGetResponse.Count {
			err = json.Unmarshal(etcdGetResponse.Kvs[0].Value, checkpointHeader)
			if nil != err {
				log.Fatalf("error unmarshalling %s's Value (%s): %v", globals.checkpointEtcdKeyName, string(etcdGetResponse.Kvs[0].Value[:]), err)
			}

			if headhunter.CheckpointVersion3 != checkpointHeader.CheckpointVersion {
				log.Fatalf("unsupported CheckpointVersion (%v)...must be == CheckpointVersion3 (%v)", checkpointHeader.CheckpointVersion, headhunter.CheckpointVersion3)
			}

			return
		}
	}

	checkpointContainerHeaderMap, err = doHead(globals.accountName + "/" + globals.checkpointContainerName)
	if nil != err {
		log.Fatalf("error fetching checkpointContainerHeaderMap: %v", err)
	}

	checkpointContainerHeaderStringSlice, ok = checkpointContainerHeaderMap[headhunter.CheckpointHeaderName]
	if !ok {
		log.Fatalf("error fetching checkpointContainerHeaderStringSlice")
	}
	if 1 != len(checkpointContainerHeaderStringSlice) {
		log.Fatalf("checkpointContainerHeaderStringSlice must be single-valued")
	}

	checkpointContainerHeaderString = checkpointContainerHeaderStringSlice[0]

	checkpointContainerHeaderStringSplit = strings.Split(checkpointContainerHeaderString, " ")
	if 4 != len(checkpointContainerHeaderStringSplit) {
		log.Fatalf("checkpointContainerHeaderStringSplit must be four-valued")
	}

	checkpointHeader.CheckpointVersion, err = strconv.ParseUint(checkpointContainerHeaderStringSplit[0], 16, 64)
	if nil != err {
		log.Fatalf("error parsing CheckpointVersion: %v", err)
	}
	if headhunter.CheckpointVersion3 != checkpointHeader.CheckpointVersion {
		log.Fatalf("unsupported CheckpointVersion (%v)...must be == CheckpointVersion3 (%v)", checkpointHeader.CheckpointVersion, headhunter.CheckpointVersion3)
	}

	checkpointHeader.CheckpointObjectTrailerStructObjectNumber, err = strconv.ParseUint(checkpointContainerHeaderStringSplit[1], 16, 64)
	if nil != err {
		log.Fatalf("error parsing CheckpointObjectTrailerStructObjectNumber:%v", err)
	}

	checkpointHeader.CheckpointObjectTrailerStructObjectLength, err = strconv.ParseUint(checkpointContainerHeaderStringSplit[2], 16, 64)
	if nil != err {
		log.Fatalf("error parsing CheckpointObjectTrailerStructObjectLength:%v", err)
	}

	checkpointHeader.ReservedToNonce, err = strconv.ParseUint(checkpointContainerHeaderStringSplit[3], 16, 64)
	if nil != err {
		log.Fatalf("error parsing ReservedToNonce:%v", err)
	}

	return
}

func checkpointHeadersAreEqual(checkpointHeader1 *headhunter.CheckpointHeaderStruct, checkpointHeader2 *headhunter.CheckpointHeaderStruct) (areEqual bool) {
	areEqual = (checkpointHeader1.CheckpointVersion == checkpointHeader2.CheckpointVersion) &&
		(checkpointHeader1.CheckpointObjectTrailerStructObjectNumber == checkpointHeader2.CheckpointObjectTrailerStructObjectNumber) &&
		(checkpointHeader1.CheckpointObjectTrailerStructObjectLength == checkpointHeader2.CheckpointObjectTrailerStructObjectLength) &&
		(checkpointHeader1.ReservedToNonce == checkpointHeader2.ReservedToNonce)
	return // Note: Comparing CheckpointObjectTrailerStructObjectNumber's would actually have been sufficient
}

func fetchCheckpointObjectTrailerV3() (err error) {
	var (
		checkpointObjectHeaderMap http.Header
		checkpointObjectPath      string
		inRecycleBinRefMap        bool
		refs                      uint64
	)

	checkpointObjectPath = globals.accountName + "/" + globals.checkpointContainerName + "/" + fmt.Sprintf("%016X", globals.initialCheckpointHeader.CheckpointObjectTrailerStructObjectNumber)

	checkpointObjectHeaderMap, globals.checkpointObjectTrailerV3Buf, err = doGetTail(checkpointObjectPath, globals.initialCheckpointHeader.CheckpointObjectTrailerStructObjectLength)
	if nil != err {
		err = fmt.Errorf("error reading checkpointObjectTrailerV3Buf: %v", err)
		return
	}

	if metadataRecycleBinHeaderPresent(checkpointObjectHeaderMap) {
		refs, inRecycleBinRefMap = globals.recycleBinRefMap[checkpointObjectPath]
		if inRecycleBinRefMap {
			refs++
		} else {
			refs = 1
		}
		globals.recycleBinRefMap[checkpointObjectPath] = refs
	}

	globals.checkpointObjectTrailerV3 = &headhunter.CheckpointObjectTrailerV3Struct{}

	globals.checkpointObjectTrailerV3BufPos, err = cstruct.Unpack(globals.checkpointObjectTrailerV3Buf, globals.checkpointObjectTrailerV3, headhunter.LittleEndian)
	if nil != err {
		err = fmt.Errorf("unable to Unpack checkpointObjectTrailerV3Buf: %v", err)
		return
	}

	return
}

func fetchBPlusTreeLayout(numElements uint64) (treeLayout sortedmap.LayoutReport, err error) {
	var (
		alreadyInTreeLayout      bool
		bytesNeeded              uint64
		elementIndex             uint64
		elementOfBPlusTreeLayout headhunter.ElementOfBPlusTreeLayoutStruct
	)

	bytesNeeded = numElements * globals.elementOfBPlusTreeLayoutStructSize
	if bytesNeeded > (globals.initialCheckpointHeader.CheckpointObjectTrailerStructObjectLength - globals.checkpointObjectTrailerV3BufPos) {
		err = fmt.Errorf("insufficient bytes left in checkpointObjectTrailerV3Buf")
		return
	}

	treeLayout = make(sortedmap.LayoutReport)

	for elementIndex = 0; elementIndex < numElements; elementIndex++ {
		_, err = cstruct.Unpack(globals.checkpointObjectTrailerV3Buf[globals.checkpointObjectTrailerV3BufPos:], &elementOfBPlusTreeLayout, headhunter.LittleEndian)
		if nil != err {
			return
		}

		globals.checkpointObjectTrailerV3BufPos += globals.elementOfBPlusTreeLayoutStructSize

		_, alreadyInTreeLayout = treeLayout[elementOfBPlusTreeLayout.ObjectNumber]
		if alreadyInTreeLayout {
			err = fmt.Errorf("duplicate elementOfBPlusTreeLayout.ObjectNumber (0x%016X) encountered", elementOfBPlusTreeLayout.ObjectNumber)
			return
		}

		treeLayout[elementOfBPlusTreeLayout.ObjectNumber] = elementOfBPlusTreeLayout.ObjectNumber
	}

	return
}

func fetchSnapShotList(numElements uint64) (err error) {
	var (
		alreadySeenThisID       bool
		alreadySeenThisIDSet    map[uint64]struct{}
		alreadySeenThisName     bool
		alreadySeenThisNameSet  map[string]struct{}
		alreadySeenThisNonce    bool
		alreadySeenThisNonceSet map[uint64]struct{}
		elementIndex            uint64
		elementOfSnapShotList   *headhunter.ElementOfSnapShotListStruct
	)

	globals.snapShotList = make([]*headhunter.ElementOfSnapShotListStruct, numElements)

	alreadySeenThisNonceSet = make(map[uint64]struct{})
	alreadySeenThisIDSet = make(map[uint64]struct{})
	alreadySeenThisNameSet = make(map[string]struct{})

	for elementIndex = 0; elementIndex < numElements; elementIndex++ {
		elementOfSnapShotList = &headhunter.ElementOfSnapShotListStruct{}

		elementOfSnapShotList.Nonce, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.ID, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.TimeStamp, err = consumeCheckpointObjectTrailerV3BufTimeStamp()
		if nil != err {
			return
		}
		elementOfSnapShotList.Name, err = consumeCheckpointObjectTrailerV3BufString()
		if nil != err {
			return
		}
		elementOfSnapShotList.InodeRecBPlusTreeObjectNumber, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.InodeRecBPlusTreeObjectOffset, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.InodeRecBPlusTreeObjectLength, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.LogSegmentRecBPlusTreeObjectNumber, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.LogSegmentRecBPlusTreeObjectOffset, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.LogSegmentRecBPlusTreeObjectLength, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.BPlusTreeObjectBPlusTreeObjectNumber, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.BPlusTreeObjectBPlusTreeObjectOffset, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.BPlusTreeObjectBPlusTreeObjectLength, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.CreatedObjectsBPlusTreeObjectNumber, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.CreatedObjectsBPlusTreeObjectOffset, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.CreatedObjectsBPlusTreeObjectLength, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.DeletedObjectsBPlusTreeObjectNumber, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.DeletedObjectsBPlusTreeObjectOffset, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}
		elementOfSnapShotList.DeletedObjectsBPlusTreeObjectLength, err = consumeCheckpointObjectTrailerV3BufUint64()
		if nil != err {
			return
		}

		_, alreadySeenThisNonce = alreadySeenThisNonceSet[elementOfSnapShotList.Nonce]
		if alreadySeenThisNonce {
			err = fmt.Errorf("duplicate elementOfSnapShotList.Nonce found: 0x%16X", elementOfSnapShotList.Nonce)
			return
		}
		alreadySeenThisNonceSet[elementOfSnapShotList.Nonce] = struct{}{}

		_, alreadySeenThisID = alreadySeenThisIDSet[elementOfSnapShotList.ID]
		if alreadySeenThisID {
			err = fmt.Errorf("duplicate elementOfSnapShotList.ID found: 0x%16X", elementOfSnapShotList.ID)
			return
		}
		alreadySeenThisIDSet[elementOfSnapShotList.ID] = struct{}{}

		_, alreadySeenThisName = alreadySeenThisNameSet[elementOfSnapShotList.Name]
		if alreadySeenThisName {
			err = fmt.Errorf("duplicate elementOfSnapShotList.Name found: 0x%16X", elementOfSnapShotList.Name)
			return
		}
		alreadySeenThisNameSet[elementOfSnapShotList.Name] = struct{}{}

		globals.snapShotList[elementIndex] = elementOfSnapShotList
	}

	return
}

func consumeCheckpointObjectTrailerV3BufUint64() (u64 uint64, err error) {
	var (
		u64Copy uint64
	)

	if globals.uint64Size > (globals.initialCheckpointHeader.CheckpointObjectTrailerStructObjectLength - globals.checkpointObjectTrailerV3BufPos) {
		err = fmt.Errorf("insufficient bytes left in checkpointObjectTrailerV3Buf")
		return
	}

	_, err = cstruct.Unpack(globals.checkpointObjectTrailerV3Buf[globals.checkpointObjectTrailerV3BufPos:], &u64Copy, headhunter.LittleEndian)
	if nil != err {
		return
	}

	globals.checkpointObjectTrailerV3BufPos += globals.uint64Size

	u64 = u64Copy

	return
}

func consumeCheckpointObjectTrailerV3BufTimeStamp() (timeStamp time.Time, err error) {
	var (
		timeStampBuf    []byte
		timeStampBufLen uint64
	)

	timeStampBufLen, err = consumeCheckpointObjectTrailerV3BufUint64()
	if nil != err {
		return
	}

	if timeStampBufLen > (globals.initialCheckpointHeader.CheckpointObjectTrailerStructObjectLength - globals.checkpointObjectTrailerV3BufPos) {
		err = fmt.Errorf("insufficient bytes left in checkpointObjectTrailerV3Buf")
		return
	}

	timeStampBuf = globals.checkpointObjectTrailerV3Buf[globals.checkpointObjectTrailerV3BufPos : globals.checkpointObjectTrailerV3BufPos+timeStampBufLen]

	err = timeStamp.UnmarshalBinary(timeStampBuf)
	if nil != err {
		return
	}

	globals.checkpointObjectTrailerV3BufPos += timeStampBufLen

	return
}

func consumeCheckpointObjectTrailerV3BufString() (str string, err error) {
	var (
		strLen uint64
	)

	strLen, err = consumeCheckpointObjectTrailerV3BufUint64()
	if nil != err {
		return
	}

	if strLen > (globals.initialCheckpointHeader.CheckpointObjectTrailerStructObjectLength - globals.checkpointObjectTrailerV3BufPos) {
		err = fmt.Errorf("insufficient bytes left in checkpointObjectTrailerV3Buf")
		return
	}

	str = string(globals.checkpointObjectTrailerV3Buf[globals.checkpointObjectTrailerV3BufPos : globals.checkpointObjectTrailerV3BufPos+strLen])

	globals.checkpointObjectTrailerV3BufPos += strLen

	return
}

func dumpKeysIfRequested(bPlusTree sortedmap.BPlusTree, bPlusTreeName string) {
	var (
		err           error
		itemIndex     int
		keyAsKey      sortedmap.Key
		keyAsUint64   uint64
		numberOfItems int
		ok            bool
	)

	if globals.alsoDump {
		fmt.Printf("%s:\n", bPlusTreeName)

		numberOfItems, err = bPlusTree.Len()
		if nil != err {
			log.Fatalf("  bPlusTree.Len() failed: %v", err)
		}

		if 0 == numberOfItems {
			fmt.Printf("  <empty>\n")
			return
		}

		for itemIndex = 0; itemIndex < numberOfItems; itemIndex++ {
			keyAsKey, _, ok, err = bPlusTree.GetByIndex(itemIndex)
			if nil != err {
				log.Fatalf(" bPlusTree.GetByIndex(%d) failed: %v", itemIndex, err)
			}
			if !ok {
				log.Fatalf(" bPlusTree.GetByIndex(%d) returned !ok", itemIndex)
			}

			keyAsUint64, ok = keyAsKey.(uint64)
			if !ok {
				log.Fatalf(" bPlusTree.GetByIndex(%d) returned non-uint64 Key", itemIndex)
			}

			fmt.Printf("  0x%016X\n", keyAsUint64)
		}
	}
}

func (dummy *globalsStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (dummy *globalsStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (dummy *globalsStruct) GetNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (nodeByteSlice []byte, err error) {
	var (
		inRecycleBinRefMap bool
		objectHeaderMap    http.Header
		objectPath         string
		refs               uint64
	)

	objectPath = globals.accountName + "/" + globals.checkpointContainerName + "/" + fmt.Sprintf("%016X", objectNumber)

	objectHeaderMap, nodeByteSlice, err = doGetRange(objectPath, objectOffset, objectLength)
	if nil != err {
		err = fmt.Errorf("error reading %s (offset=0x%016X,length=0x%016X): %v", objectPath, objectOffset, objectLength, err)
		return
	}

	if metadataRecycleBinHeaderPresent(objectHeaderMap) {
		refs, inRecycleBinRefMap = globals.recycleBinRefMap[objectPath]
		if inRecycleBinRefMap {
			refs++
		} else {
			refs = 1
		}
		globals.recycleBinRefMap[objectPath] = refs
	}

	return
}

func (dummy *globalsStruct) PutNode(nodeByteSlice []byte) (objectNumber uint64, objectOffset uint64, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (dummy *globalsStruct) DiscardNode(objectNumber uint64, objectOffset uint64, objectLength uint64) (err error) {
	err = nil
	return
}

func (dummy *globalsStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (dummy *globalsStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		keyAsUint64 uint64
	)

	_, err = cstruct.Unpack(payloadData, &keyAsUint64, headhunter.LittleEndian)
	if nil != err {
		return
	}

	key = keyAsUint64
	bytesConsumed = globals.uint64Size

	return
}

func (dummy *globalsStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	err = fmt.Errorf("not implemented")
	return
}

func (dummy *globalsStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		valueLen uint64
	)

	_, err = cstruct.Unpack(payloadData, &valueLen, headhunter.LittleEndian)
	if nil != err {
		return
	}

	if (globals.uint64Size + valueLen) > uint64(len(payloadData)) {
		err = fmt.Errorf("payloadData insufficient to UnpackValue()")
		return
	}

	value = make([]byte, valueLen)
	_ = copy(value.([]byte), payloadData)

	bytesConsumed = globals.uint64Size + valueLen

	return
}

func doHead(path string) (headerMap http.Header, err error) {
	var (
		headResponse *http.Response
	)

	headResponse, err = http.Head(globals.noAuthURL + "v1/" + path)
	if nil != err {
		return
	}

	_, err = ioutil.ReadAll(headResponse.Body)
	if nil != err {
		return
	}
	err = headResponse.Body.Close()
	if nil != err {
		return
	}

	if (http.StatusOK != headResponse.StatusCode) && (http.StatusNoContent != headResponse.StatusCode) {
		err = fmt.Errorf("unexpected getResponse.Status: %s", headResponse.Status)
		return
	}

	headerMap = headResponse.Header

	return
}

func doGetRange(path string, offset uint64, length uint64) (headerMap http.Header, buf []byte, err error) {
	var (
		getRequest  *http.Request
		getResponse *http.Response
		httpClient  *http.Client
	)

	getRequest, err = http.NewRequest("GET", globals.noAuthURL+"v1/"+path, nil)
	if nil != err {
		return
	}

	getRequest.Header.Set("Range", "bytes="+strconv.FormatUint(offset, 10)+"-"+strconv.FormatUint((offset+length-1), 10))

	httpClient = &http.Client{}

	getResponse, err = httpClient.Do(getRequest)
	if nil != err {
		return
	}

	buf, err = ioutil.ReadAll(getResponse.Body)
	if nil != err {
		return
	}
	err = getResponse.Body.Close()
	if nil != err {
		return
	}

	if (http.StatusOK != getResponse.StatusCode) && (http.StatusNoContent != getResponse.StatusCode) && (http.StatusPartialContent != getResponse.StatusCode) {
		err = fmt.Errorf("unexpected getResponse.Status: %s", getResponse.Status)
		return
	}

	if uint64(len(buf)) != length {
		err = fmt.Errorf("unexpected getResponse.Body length")
		return
	}

	headerMap = getResponse.Header

	return
}

func doGetTail(path string, length uint64) (headerMap http.Header, buf []byte, err error) {
	var (
		getRequest  *http.Request
		getResponse *http.Response
		httpClient  *http.Client
	)

	getRequest, err = http.NewRequest("GET", globals.noAuthURL+"v1/"+path, nil)
	if nil != err {
		return
	}

	getRequest.Header.Set("Range", "bytes=-"+strconv.FormatUint(length, 10))

	httpClient = &http.Client{}

	getResponse, err = httpClient.Do(getRequest)
	if nil != err {
		return
	}

	buf, err = ioutil.ReadAll(getResponse.Body)
	if nil != err {
		return
	}
	err = getResponse.Body.Close()
	if nil != err {
		return
	}

	if (http.StatusOK != getResponse.StatusCode) && (http.StatusNoContent != getResponse.StatusCode) && (http.StatusPartialContent != getResponse.StatusCode) {
		err = fmt.Errorf("unexpected getResponse.Status: %s", getResponse.Status)
		return
	}

	if uint64(len(buf)) != length {
		err = fmt.Errorf("unexpected getResponse.Body length")
		return
	}

	headerMap = getResponse.Header

	return
}

func doGetList(path string) (headerMap http.Header, list []string, err error) {
	var (
		buf         []byte
		getResponse *http.Response
	)

	list = make([]string, 0)

	for {
		if 0 == len(list) {
			getResponse, err = http.Get(globals.noAuthURL + "v1/" + path)
		} else {
			getResponse, err = http.Get(globals.noAuthURL + "v1/" + path + "?marker=" + list[len(list)-1])
		}
		if nil != err {
			return
		}

		buf, err = ioutil.ReadAll(getResponse.Body)
		if nil != err {
			return
		}
		err = getResponse.Body.Close()
		if nil != err {
			return
		}

		if (http.StatusOK != getResponse.StatusCode) && (http.StatusNoContent != getResponse.StatusCode) && (http.StatusPartialContent != getResponse.StatusCode) {
			err = fmt.Errorf("unexpected getResponse.Status: %s", getResponse.Status)
			return
		}

		time.Sleep(time.Second)
		if 0 == len(buf) {
			break
		}

		list = append(list, strings.Split(string(buf[:]), "\n")...)
		list = list[:len(list)-1]
	}

	headerMap = getResponse.Header

	return
}

func metadataRecycleBinHeaderPresent(headerMap http.Header) (present bool) {
	_, present = headerMap[headhunter.MetadataRecycleBinHeaderName]
	return
}
