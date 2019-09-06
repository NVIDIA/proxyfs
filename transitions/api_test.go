package transitions

import (
	"fmt"
	"sort"
	"testing"

	"github.com/swiftstack/ProxyFS/conf"
)

type testCallbacksInterfaceStruct struct {
	name string
	t    *testing.T
}

var testConfStrings = []string{
	"Logging.LogFilePath=/dev/null",
	"Logging.LogToConsole=false",
	"FlowControl:FlowControlX.MaxFlushSize=10000000",
	"FlowControl:FlowControlX.MaxFlushTime=10s",
	"FlowControl:FlowControlX.FileDefragmentChunkSize=10000000",
	"FlowControl:FlowControlX.FileDefragmentChunkDelay=10ms",
	"FlowControl:FlowControlX.ReadCacheLineSize=1000000",
	"FlowControl:FlowControlX.ReadCacheWeight=100",
	"FlowControl:FlowControlY.MaxFlushSize=20000000",
	"FlowControl:FlowControlY.MaxFlushTime=20s",
	"FlowControl:FlowControlY.FileDefragmentChunkSize=20000000",
	"FlowControl:FlowControlY.FileDefragmentChunkDelay=20ms",
	"FlowControl:FlowControlY.ReadCacheLineSize=2000000",
	"FlowControl:FlowControlY.ReadCacheWeight=200",
	"Volume:VolumeA.PrimaryPeer=Peer0",
	"Volume:VolumeA.FlowControl=FlowControlX",
	"Volume:VolumeB.PrimaryPeer=Peer0", // Will migrate to Peer1
	"Volume:VolumeB.FlowControl=FlowControlY",
	"Volume:VolumeC.PrimaryPeer=Peer1", // Will migrate to Peer2...then destroyed
	"Volume:VolumeC.FlowControl=FlowControlX",
	"Volume:VolumeD.PrimaryPeer=Peer2", // Will migrate to Peer0...then destroyed
	"Volume:VolumeD.FlowControl=FlowControlY",
	//                                     VolumeE will be created on Peer0
	//                                     VolumeF will be created on Peer1
	//                                     VolumeG will be created on Peer2...then migrate to VolumeF's VolumeGroup
	"FSGlobals.VolumeList=VolumeA,VolumeB,VolumeC,VolumeD",
	"FSGlobals.CheckpointHeaderConsensusAttempts=5",
	"FSGlobals.MountRetryLimit=6",
	"FSGlobals.MountRetryDelay=1s",
	"FSGlobals.MountRetryExpBackoff=2",
	"FSGlobals.LogCheckpointHeaderPosts=true",
	"FSGlobals.TryLockBackoffMin=10ms",
	"FSGlobals.TryLockBackoffMax=50ms",
	"FSGlobals.TryLockSerializationThreshhold=5",
	"FSGlobals.SymlinkMax=32",
	"Cluster.Peers=Peer0,Peer1,Peer2,Peer3",
	"Cluster.WhoAmI=Peer0",
	"Transitions.AutoVolumeGroupPrefix=V_",
	"Transitions.AutoVolumeGroupSuffix=_G",
}

var testConfStringsToAddVolumeE = []string{
	"Volume:VolumeE.MaxFlushSize=30000000",
	"Volume:VolumeE.MaxFlushTime=30s",
	"Volume:VolumeE.FileDefragmentChunkSize=30000000",
	"Volume:VolumeE.FileDefragmentChunkDelay=30ms",
	"VolumeGroup:VG_One.PrimaryPeer=Peer0",
	"VolumeGroup:VG_One.ReadCacheLineSize=3000000",
	"VolumeGroup:VG_One.ReadCacheWeight=300",
	"VolumeGroup:VG_One.VirtualIPAddr=",
	"VolumeGroup:VG_One.VolumeList=VolumeE",
	"FSGlobals.VolumeGroupList=V_VolumeA_G,V_VolumeB_G,VG_One",
}

var testConfStringsToAddVolumeF = []string{
	"Volume:VolumeF.MaxFlushSize=40000000",
	"Volume:VolumeF.MaxFlushTime=40s",
	"Volume:VolumeF.FileDefragmentChunkSize=40000000",
	"Volume:VolumeF.FileDefragmentChunkDelay=40ms",
	"VolumeGroup:VG_Two.PrimaryPeer=Peer1",
	"VolumeGroup:VG_Two.ReadCacheLineSize=4000000",
	"VolumeGroup:VG_Two.ReadCacheWeight=400",
	"VolumeGroup:VG_Two.VirtualIPAddr=",
	"VolumeGroup:VG_Two.VolumeList=VolumeF",
	"FSGlobals.VolumeGroupList=V_VolumeA_G,V_VolumeB_G,VG_One,VG_Two",
}

var testConfStringsToAddVolumeG = []string{
	"Volume:VolumeG.MaxFlushSize=50000000",
	"Volume:VolumeG.MaxFlushTime=50s",
	"Volume:VolumeG.FileDefragmentChunkSize=50000000",
	"Volume:VolumeG.FileDefragmentChunkDelay=50ms",
	"VolumeGroup:VG_Three.PrimaryPeer=Peer2",
	"VolumeGroup:VG_Three.ReadCacheLineSize=5000000",
	"VolumeGroup:VG_Three.ReadCacheWeight=500",
	"VolumeGroup:VG_Three.VirtualIPAddr=",
	"VolumeGroup:VG_Three.VolumeList=VolumeG",
	"FSGlobals.VolumeGroupList=V_VolumeA_G,V_VolumeB_G,VG_One,VG_Two,VG_Three",
}

var testConfStringsToMoveVolumeG = []string{
	"VolumeGroup:VG_Two.VolumeList=VolumeF,VolumeG",
	"VolumeGroup:VG_Three.VolumeList=",
	"FSGlobals.VolumeGroupList=V_VolumeA_G,V_VolumeB_G,VG_One,VG_Two,VG_Three",
}

var testCallbackLog []string // Accumulates log messages output by transitions.Callbacks implementations

func testValidateUpgradedConfMap(t *testing.T, testConfMap conf.ConfMap) {
	var (
		fileDefragmentChunkDelay   conf.ConfMapOption
		fileDefragmentChunkDelayOK bool
		fileDefragmentChunkSize    conf.ConfMapOption
		fileDefragmentChunkSizeOK  bool
		fsGlobals                  conf.ConfMapSection
		fsGlobalsOK                bool
		maxFlushSize               conf.ConfMapOption
		maxFlushSizeOK             bool
		maxFlushTime               conf.ConfMapOption
		maxFlushTimeOK             bool
		primaryPeer                conf.ConfMapOption
		primaryPeerOK              bool
		readCacheLineSize          conf.ConfMapOption
		readCacheLineSizeOK        bool
		readCacheWeight            conf.ConfMapOption
		readCacheWeightOK          bool
		testVolume                 conf.ConfMapSection
		testVolumeGroup            conf.ConfMapSection
		testVolumeGroupOK          bool
		testVolumeOK               bool
		virtualIPAddr              conf.ConfMapOption
		virtualIPAddrOK            bool
		volumeGroupList            conf.ConfMapOption
		volumeGroupListOK          bool
		volumeList                 conf.ConfMapOption
		volumeListOK               bool
	)

	// Validate FSGlobals

	fsGlobals, fsGlobalsOK = testConfMap["FSGlobals"]
	if !fsGlobalsOK {
		t.Fatalf("testConfMap must contain an FSGlobals section")
	}

	volumeGroupList, volumeGroupListOK = fsGlobals["VolumeGroupList"]
	if !volumeGroupListOK || (4 != len(volumeGroupList)) || ("V_VolumeA_G" != volumeGroupList[0]) || ("V_VolumeB_G" != volumeGroupList[1]) || ("V_VolumeC_G" != volumeGroupList[2]) || ("V_VolumeD_G" != volumeGroupList[3]) {
		t.Fatalf("testConfMap missing FSGlobals.VolumeGroupList=V_VolumeA_G,V_VolumeB_G,V_VolumeC_G,V_VolumeD_G")
	}

	// Validate VolumeGroup:V_VolumeA_G

	testVolumeGroup, testVolumeGroupOK = testConfMap["VolumeGroup:V_VolumeA_G"]
	if !testVolumeGroupOK {
		t.Fatalf("testConfMap must contain a VolumeGroup:V_VolumeA_G section")
	}

	volumeList, volumeListOK = testVolumeGroup["VolumeList"]
	if !volumeListOK || (1 != len(volumeList)) || ("VolumeA" != volumeList[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeA.VolumeList=VolumeA")
	}
	virtualIPAddr, virtualIPAddrOK = testVolumeGroup["VirtualIPAddr"]
	if !virtualIPAddrOK || (0 != len(virtualIPAddr)) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeA.VirtualIPAddr=")
	}
	primaryPeer, primaryPeerOK = testVolumeGroup["PrimaryPeer"]
	if !primaryPeerOK || (1 != len(primaryPeer)) || ("Peer0" != primaryPeer[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeA.PrimaryPeer=Peer0")
	}
	readCacheLineSize, readCacheLineSizeOK = testVolumeGroup["ReadCacheLineSize"]
	if !readCacheLineSizeOK || (1 != len(readCacheLineSize)) || ("1000000" != readCacheLineSize[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeA.ReadCacheLineSize=1000000")
	}
	readCacheWeight, readCacheWeightOK = testVolumeGroup["ReadCacheWeight"]
	if !readCacheWeightOK || (1 != len(readCacheWeight)) || ("100" != readCacheWeight[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeA.ReadCacheWeight=100")
	}

	testVolume, testVolumeOK = testConfMap["Volume:VolumeA"]
	if !testVolumeOK {
		t.Fatalf("testConfMap must contain a Volume:VolumeA section")
	}

	maxFlushSize, maxFlushSizeOK = testVolume["MaxFlushSize"]
	if !maxFlushSizeOK || (1 != len(maxFlushSize)) || ("10000000" != maxFlushSize[0]) {
		t.Fatalf("testConfMap missing Volume:VolumeA.MaxFlushSize=10000000")
	}
	maxFlushTime, maxFlushTimeOK = testVolume["MaxFlushTime"]
	if !maxFlushTimeOK || (1 != len(maxFlushTime)) || ("10s" != maxFlushTime[0]) {
		t.Fatalf("testConfMap missing Volume:VolumeA.MaxFlushTime=10s")
	}

	fileDefragmentChunkSize, fileDefragmentChunkSizeOK = testVolume["FileDefragmentChunkSize"]
	if !fileDefragmentChunkSizeOK || (1 != len(fileDefragmentChunkSize)) || ("10000000" != fileDefragmentChunkSize[0]) {
		t.Fatalf("testConfigMap has invalid Volume:VolumeA.FileDefragmentChunkSize")
	}
	fileDefragmentChunkDelay, fileDefragmentChunkDelayOK = testVolume["FileDefragmentChunkDelay"]
	if !fileDefragmentChunkDelayOK || (1 != len(fileDefragmentChunkDelay)) || ("10ms" != fileDefragmentChunkDelay[0]) {
		t.Fatalf("testConfigMap has invalid Volume:VolumeA.FileDefragmentChunkDelay")
	}

	// Validate VolumeGroup:V_VolumeB_G

	testVolumeGroup, testVolumeGroupOK = testConfMap["VolumeGroup:V_VolumeB_G"]
	if !testVolumeGroupOK {
		t.Fatalf("testConfMap must contain a VolumeGroup:V_VolumeB_G section")
	}

	volumeList, volumeListOK = testVolumeGroup["VolumeList"]
	if !volumeListOK || (1 != len(volumeList)) || ("VolumeB" != volumeList[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeB.VolumeList=VolumeB")
	}
	virtualIPAddr, virtualIPAddrOK = testVolumeGroup["VirtualIPAddr"]
	if !virtualIPAddrOK || (0 != len(virtualIPAddr)) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeB.VirtualIPAddr=")
	}
	primaryPeer, primaryPeerOK = testVolumeGroup["PrimaryPeer"]
	if !primaryPeerOK || (1 != len(primaryPeer)) || ("Peer0" != primaryPeer[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeB.PrimaryPeer=Peer0")
	}
	readCacheLineSize, readCacheLineSizeOK = testVolumeGroup["ReadCacheLineSize"]
	if !readCacheLineSizeOK || (1 != len(readCacheLineSize)) || ("2000000" != readCacheLineSize[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeB.ReadCacheLineSize=2000000")
	}
	readCacheWeight, readCacheWeightOK = testVolumeGroup["ReadCacheWeight"]
	if !readCacheWeightOK || (1 != len(readCacheWeight)) || ("200" != readCacheWeight[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeB.ReadCacheWeight=200")
	}

	testVolume, testVolumeOK = testConfMap["Volume:VolumeB"]
	if !testVolumeOK {
		t.Fatalf("testConfMap must contain a Volume:VolumeB section")
	}

	maxFlushSize, maxFlushSizeOK = testVolume["MaxFlushSize"]
	if !maxFlushSizeOK || (1 != len(maxFlushSize)) || ("20000000" != maxFlushSize[0]) {
		t.Fatalf("testConfMap missing Volume:VolumeB.MaxFlushSize=20000000")
	}
	maxFlushTime, maxFlushTimeOK = testVolume["MaxFlushTime"]
	if !maxFlushTimeOK || (1 != len(maxFlushTime)) || ("20s" != maxFlushTime[0]) {
		t.Fatalf("testConfMap missing Volume:VolumeB.MaxFlushTime=20s")
	}

	fileDefragmentChunkSize, fileDefragmentChunkSizeOK = testVolume["FileDefragmentChunkSize"]
	if !fileDefragmentChunkSizeOK || (1 != len(fileDefragmentChunkSize)) || ("20000000" != fileDefragmentChunkSize[0]) {
		t.Fatalf("testConfigMap has invalid Volume:VolumeB.FileDefragmentChunkSize")
	}
	fileDefragmentChunkDelay, fileDefragmentChunkDelayOK = testVolume["FileDefragmentChunkDelay"]
	if !fileDefragmentChunkDelayOK || (1 != len(fileDefragmentChunkDelay)) || ("20ms" != fileDefragmentChunkDelay[0]) {
		t.Fatalf("testConfigMap has invalid Volume:VolumeB.FileDefragmentChunkDelay")
	}

	// Validate VolumeGroup:V_VolumeC_G

	testVolumeGroup, testVolumeGroupOK = testConfMap["VolumeGroup:V_VolumeC_G"]
	if !testVolumeGroupOK {
		t.Fatalf("testConfMap must contain a VolumeGroup:V_VolumeC_G section")
	}

	volumeList, volumeListOK = testVolumeGroup["VolumeList"]
	if !volumeListOK || (1 != len(volumeList)) || ("VolumeC" != volumeList[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeC.VolumeList=VolumeC")
	}
	virtualIPAddr, virtualIPAddrOK = testVolumeGroup["VirtualIPAddr"]
	if !virtualIPAddrOK || (0 != len(virtualIPAddr)) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeC.VirtualIPAddr=")
	}
	primaryPeer, primaryPeerOK = testVolumeGroup["PrimaryPeer"]
	if !primaryPeerOK || (1 != len(primaryPeer)) || ("Peer1" != primaryPeer[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeA.PrimaryPeer=Peer1")
	}
	readCacheLineSize, readCacheLineSizeOK = testVolumeGroup["ReadCacheLineSize"]
	if !readCacheLineSizeOK || (1 != len(readCacheLineSize)) || ("1000000" != readCacheLineSize[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeC.ReadCacheLineSize=1000000")
	}
	readCacheWeight, readCacheWeightOK = testVolumeGroup["ReadCacheWeight"]
	if !readCacheWeightOK || (1 != len(readCacheWeight)) || ("100" != readCacheWeight[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeC.ReadCacheWeight=100")
	}

	testVolume, testVolumeOK = testConfMap["Volume:VolumeC"]
	if !testVolumeOK {
		t.Fatalf("testConfMap must contain a Volume:VolumeC section")
	}

	maxFlushSize, maxFlushSizeOK = testVolume["MaxFlushSize"]
	if !maxFlushSizeOK || (1 != len(maxFlushSize)) || ("10000000" != maxFlushSize[0]) {
		t.Fatalf("testConfMap missing Volume:VolumeC.MaxFlushSize=10000000")
	}
	maxFlushTime, maxFlushTimeOK = testVolume["MaxFlushTime"]
	if !maxFlushTimeOK || (1 != len(maxFlushTime)) || ("10s" != maxFlushTime[0]) {
		t.Fatalf("testConfMap missing Volume:VolumeC.MaxFlushTime=10s")
	}

	fileDefragmentChunkSize, fileDefragmentChunkSizeOK = testVolume["FileDefragmentChunkSize"]
	if !fileDefragmentChunkSizeOK || (1 != len(fileDefragmentChunkSize)) || ("10000000" != fileDefragmentChunkSize[0]) {
		t.Fatalf("testConfigMap has invalid Volume:VolumeC.FileDefragmentChunkSize")
	}
	fileDefragmentChunkDelay, fileDefragmentChunkDelayOK = testVolume["FileDefragmentChunkDelay"]
	if !fileDefragmentChunkDelayOK || (1 != len(fileDefragmentChunkDelay)) || ("10ms" != fileDefragmentChunkDelay[0]) {
		t.Fatalf("testConfigMap has invalid Volume:VolumeC.FileDefragmentChunkDelay")
	}

	// Validate VolumeGroup:V_VolumeD_G

	testVolumeGroup, testVolumeGroupOK = testConfMap["VolumeGroup:V_VolumeD_G"]
	if !testVolumeGroupOK {
		t.Fatalf("testConfMap must contain a VolumeGroup:V_VolumeD_G section")
	}

	volumeList, volumeListOK = testVolumeGroup["VolumeList"]
	if !volumeListOK || (1 != len(volumeList)) || ("VolumeD" != volumeList[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeD.VolumeList=VolumeD")
	}
	virtualIPAddr, virtualIPAddrOK = testVolumeGroup["VirtualIPAddr"]
	if !virtualIPAddrOK || (0 != len(virtualIPAddr)) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeD.VirtualIPAddr=")
	}
	primaryPeer, primaryPeerOK = testVolumeGroup["PrimaryPeer"]
	if !primaryPeerOK || (1 != len(primaryPeer)) || ("Peer2" != primaryPeer[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeD.PrimaryPeer=Peer2")
	}
	readCacheLineSize, readCacheLineSizeOK = testVolumeGroup["ReadCacheLineSize"]
	if !readCacheLineSizeOK || (1 != len(readCacheLineSize)) || ("2000000" != readCacheLineSize[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeD.ReadCacheLineSize=2000000")
	}
	readCacheWeight, readCacheWeightOK = testVolumeGroup["ReadCacheWeight"]
	if !readCacheWeightOK || (1 != len(readCacheWeight)) || ("200" != readCacheWeight[0]) {
		t.Fatalf("testConfMap missing VolumeGroup:VolumeD.ReadCacheWeight=200")
	}

	testVolume, testVolumeOK = testConfMap["Volume:VolumeD"]
	if !testVolumeOK {
		t.Fatalf("testConfMap must contain a Volume:VolumeD section")
	}

	maxFlushSize, maxFlushSizeOK = testVolume["MaxFlushSize"]
	if !maxFlushSizeOK || (1 != len(maxFlushSize)) || ("20000000" != maxFlushSize[0]) {
		t.Fatalf("testConfMap missing Volume:VolumeD.MaxFlushSize=20000000")
	}
	maxFlushTime, maxFlushTimeOK = testVolume["MaxFlushTime"]
	if !maxFlushTimeOK || (1 != len(maxFlushTime)) || ("20s" != maxFlushTime[0]) {
		t.Fatalf("testConfMap missing Volume:VolumeD.MaxFlushTime=20s")
	}

	fileDefragmentChunkSize, fileDefragmentChunkSizeOK = testVolume["FileDefragmentChunkSize"]
	if !fileDefragmentChunkSizeOK || (1 != len(fileDefragmentChunkSize)) || ("20000000" != fileDefragmentChunkSize[0]) {
		t.Fatalf("testConfigMap has invalid Volume:VolumeD.FileDefragmentChunkSize")
	}
	fileDefragmentChunkDelay, fileDefragmentChunkDelayOK = testVolume["FileDefragmentChunkDelay"]
	if !fileDefragmentChunkDelayOK || (1 != len(fileDefragmentChunkDelay)) || ("20ms" != fileDefragmentChunkDelay[0]) {
		t.Fatalf("testConfigMap has invalid Volume:VolumeD.FileDefragmentChunkDelay")
	}
}

// testValidateCallbackLog compares the global testCallbackLog to the
// provided expectedCallbackLog. Note that testCallbackLog is a time
// ordered []string of log messages. It is expected that some have
// order dependency (e.g. one cannot destroy a volume unless it has
// been unserved if it was previously served by this peer). As such,
// the structure of expectedCallbackLog is a time ordered list of
// []string. Within each []string, order is not required...though each
// element must match precisely once. In the slice of []string's, order
// is strictly required.
//
func testValidateCallbackLog(t *testing.T, testcase string, expectedCallbackLog [][]string) {
	var (
		expectedCallbackLogSliceElement []string
		numLogMessages                  int
		testCallbackLogIndex            int
		testCallbackLogSubset           []string
		testCallbackLogSubsetElement    string
		testCallbackLogSubsetIndex      int
	)
	// First ensure we have the expected number of log messages

	numLogMessages = 0

	for _, expectedCallbackLogSliceElement = range expectedCallbackLog {
		numLogMessages += len(expectedCallbackLogSliceElement)
	}

	if len(testCallbackLog) != numLogMessages {
		t.Fatalf("In testcase \"%s\", unexpected testCallbackLog", testcase)
	}

	testCallbackLogIndex = 0

	for _, expectedCallbackLogSliceElement = range expectedCallbackLog {
		testCallbackLogSubset = testCallbackLog[testCallbackLogIndex : testCallbackLogIndex+len(expectedCallbackLogSliceElement)]

		sort.Strings(testCallbackLogSubset)
		sort.Strings(expectedCallbackLogSliceElement)

		for testCallbackLogSubsetIndex, testCallbackLogSubsetElement = range testCallbackLogSubset {
			if testCallbackLogSubsetElement != expectedCallbackLogSliceElement[testCallbackLogSubsetIndex] {
				t.Fatalf("In testcase \"%s\", unexpected testCallbackLog", testcase)
			}
		}

		testCallbackLogIndex += len(testCallbackLogSubset)
	}
}

func TestAPI(t *testing.T) {
	var (
		err                     error
		testCallbacksInterface1 *testCallbacksInterfaceStruct
		testCallbacksInterface2 *testCallbacksInterfaceStruct
		testConfMap             conf.ConfMap
	)

	testCallbacksInterface1 = &testCallbacksInterfaceStruct{name: "1", t: t}
	testCallbacksInterface2 = &testCallbacksInterfaceStruct{name: "2", t: t}

	Register(testCallbacksInterface1.name, testCallbacksInterface1)
	Register(testCallbacksInterface2.name, testCallbacksInterface2)

	testConfMap, err = conf.MakeConfMapFromStrings(testConfStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	t.Log("Verify old->new conf.ConfMap conversion")

	err = upgradeConfMapIfNeeded(testConfMap)
	if nil != err {
		t.Fatalf("upgradeConfMapIfNeeded(<old confMap contents) failed: %v", err)
	}

	testValidateUpgradedConfMap(t, testConfMap)

	err = upgradeConfMapIfNeeded(testConfMap)
	if nil != err {
		t.Fatalf("upgradeConfMapIfNeeded(<new confMap contents) failed: %v", err)
	}

	testValidateUpgradedConfMap(t, testConfMap)

	t.Log("Perform Up() sequence")

	testCallbackLog = make([]string, 0, 24)

	err = Up(testConfMap)
	if nil != err {
		t.Fatalf("Up() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Perform Up() sequence",
		[][]string{
			[]string{
				"testCallbacksInterface1.Up() called"},
			[]string{
				"testCallbacksInterface2.Up() called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupCreated(,V_VolumeA_G,Peer0,) called",
				"testCallbacksInterface1.VolumeGroupCreated(,V_VolumeB_G,Peer0,) called",
				"testCallbacksInterface1.VolumeGroupCreated(,V_VolumeC_G,Peer1,) called",
				"testCallbacksInterface1.VolumeGroupCreated(,V_VolumeD_G,Peer2,) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupCreated(,V_VolumeA_G,Peer0,) called",
				"testCallbacksInterface2.VolumeGroupCreated(,V_VolumeB_G,Peer0,) called",
				"testCallbacksInterface2.VolumeGroupCreated(,V_VolumeC_G,Peer1,) called",
				"testCallbacksInterface2.VolumeGroupCreated(,V_VolumeD_G,Peer2,) called"},
			[]string{
				"testCallbacksInterface1.VolumeCreated(,VolumeA,V_VolumeA_G) called",
				"testCallbacksInterface1.VolumeCreated(,VolumeB,V_VolumeB_G) called",
				"testCallbacksInterface1.VolumeCreated(,VolumeC,V_VolumeC_G) called",
				"testCallbacksInterface1.VolumeCreated(,VolumeD,V_VolumeD_G) called"},
			[]string{
				"testCallbacksInterface2.VolumeCreated(,VolumeA,V_VolumeA_G) called",
				"testCallbacksInterface2.VolumeCreated(,VolumeB,V_VolumeB_G) called",
				"testCallbacksInterface2.VolumeCreated(,VolumeC,V_VolumeC_G) called",
				"testCallbacksInterface2.VolumeCreated(,VolumeD,V_VolumeD_G) called"},
			[]string{
				"testCallbacksInterface1.ServeVolume(,VolumeA) called",
				"testCallbacksInterface1.ServeVolume(,VolumeB) called"},
			[]string{
				"testCallbacksInterface2.ServeVolume(,VolumeA) called",
				"testCallbacksInterface2.ServeVolume(,VolumeB) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"},
		})

	t.Log("Perform Signaled() sequence with no changes")

	testCallbackLog = make([]string, 0, 4)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Perform Signaled() sequence with no changes",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Move VolumeB from Peer0 to Peer1")

	testConfMap["VolumeGroup:V_VolumeB_G"]["PrimaryPeer"] = []string{"Peer1"}

	testCallbackLog = make([]string, 0, 8)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Move VolumeB from Peer0 to Peer1",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface2.UnserveVolume(,VolumeB) called"},
			[]string{
				"testCallbacksInterface1.UnserveVolume(,VolumeB) called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupMoved(,V_VolumeB_G,Peer1,) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupMoved(,V_VolumeB_G,Peer1,) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Move VolumeC from Peer1 to Peer2")

	testConfMap["VolumeGroup:V_VolumeC_G"]["PrimaryPeer"] = []string{"Peer2"}

	testCallbackLog = make([]string, 0, 6)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Move VolumeC from Peer1 to Peer2",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupMoved(,V_VolumeC_G,Peer2,) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupMoved(,V_VolumeC_G,Peer2,) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Destroy VolumeC")

	testConfMap["FSGlobals"]["VolumeGroupList"] = []string{"V_VolumeA_G", "V_VolumeB_G", "V_VolumeD_G"}

	delete(testConfMap, "VolumeGroup:V_VolumeC_G")
	delete(testConfMap, "Volume:VolumeC")

	testCallbackLog = make([]string, 0, 8)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Destroy VolumeC",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface2.VolumeDestroyed(,VolumeC) called"},
			[]string{
				"testCallbacksInterface1.VolumeDestroyed(,VolumeC) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupDestroyed(,V_VolumeC_G) called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupDestroyed(,V_VolumeC_G) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Move VolumeD from Peer2 to Peer0")

	testConfMap["VolumeGroup:V_VolumeD_G"]["PrimaryPeer"] = []string{"Peer0"}

	testCallbackLog = make([]string, 0, 8)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Move VolumeD from Peer2 to Peer0",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupMoved(,V_VolumeD_G,Peer0,) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupMoved(,V_VolumeD_G,Peer0,) called"},
			[]string{
				"testCallbacksInterface1.ServeVolume(,VolumeD) called"},
			[]string{
				"testCallbacksInterface2.ServeVolume(,VolumeD) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Destroy VolumeD")

	testConfMap["FSGlobals"]["VolumeGroupList"] = []string{"V_VolumeA_G", "V_VolumeB_G"}

	delete(testConfMap, "VolumeGroup:V_VolumeD_G")
	delete(testConfMap, "Volume:VolumeD")

	testCallbackLog = make([]string, 0, 10)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Destroy VolumeD",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface2.UnserveVolume(,VolumeD) called"},
			[]string{
				"testCallbacksInterface1.UnserveVolume(,VolumeD) called"},
			[]string{
				"testCallbacksInterface2.VolumeDestroyed(,VolumeD) called"},
			[]string{
				"testCallbacksInterface1.VolumeDestroyed(,VolumeD) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupDestroyed(,V_VolumeD_G) called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupDestroyed(,V_VolumeD_G) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Create VolumeE on Peer0")

	err = testConfMap.UpdateFromStrings(testConfStringsToAddVolumeE)
	if nil != err {
		t.Fatalf("testConfMap.UpdateFromStrings(testConfStringsToAddVolumeE) failed: %v", err)
	}

	testCallbackLog = make([]string, 0, 10)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Create VolumeE on Peer0",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupCreated(,VG_One,Peer0,) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupCreated(,VG_One,Peer0,) called"},
			[]string{
				"testCallbacksInterface1.VolumeCreated(,VolumeE,VG_One) called"},
			[]string{
				"testCallbacksInterface2.VolumeCreated(,VolumeE,VG_One) called"},
			[]string{
				"testCallbacksInterface1.ServeVolume(,VolumeE) called"},
			[]string{
				"testCallbacksInterface2.ServeVolume(,VolumeE) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Create VolumeF on Peer1")

	err = testConfMap.UpdateFromStrings(testConfStringsToAddVolumeF)
	if nil != err {
		t.Fatalf("testConfMap.UpdateFromStrings(testConfStringsToAddVolumeF) failed: %v", err)
	}

	testCallbackLog = make([]string, 0, 8)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Create VolumeF on Peer1",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupCreated(,VG_Two,Peer1,) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupCreated(,VG_Two,Peer1,) called"},
			[]string{
				"testCallbacksInterface1.VolumeCreated(,VolumeF,VG_Two) called"},
			[]string{
				"testCallbacksInterface2.VolumeCreated(,VolumeF,VG_Two) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Create VolumeG on Peer2")

	err = testConfMap.UpdateFromStrings(testConfStringsToAddVolumeG)
	if nil != err {
		t.Fatalf("testConfMap.UpdateFromStrings(testConfStringsToAddVolumeG) failed: %v", err)
	}

	testCallbackLog = make([]string, 0, 8)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Create VolumeG on Peer2",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupCreated(,VG_Three,Peer2,) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupCreated(,VG_Three,Peer2,) called"},
			[]string{
				"testCallbacksInterface1.VolumeCreated(,VolumeG,VG_Three) called"},
			[]string{
				"testCallbacksInterface2.VolumeCreated(,VolumeG,VG_Three) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Move VolumeG to VolumeF's VolumeGroup")

	err = testConfMap.UpdateFromStrings(testConfStringsToMoveVolumeG)
	if nil != err {
		t.Fatalf("testConfMap.UpdateFromStrings(testConfStringsToMoveVolumeG) failed: %v", err)
	}

	testCallbackLog = make([]string, 0, 6)

	err = Signaled(testConfMap)
	if nil != err {
		t.Fatalf("Signaled() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Move VolumeG to VolumeF's VolumeGroup",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.VolumeMoved(,VolumeG,VG_Two) called"},
			[]string{
				"testCallbacksInterface2.VolumeMoved(,VolumeG,VG_Two) called"},
			[]string{
				"testCallbacksInterface1.SignaledFinish() called"},
			[]string{
				"testCallbacksInterface2.SignaledFinish() called"}})

	t.Log("Perform Down() sequence")

	testCallbackLog = make([]string, 0, 28)

	err = Down(testConfMap)
	if nil != err {
		t.Fatalf("Down() failed: %v", err)
	}

	testValidateCallbackLog(t,
		"Perform Down() sequence",
		[][]string{
			[]string{
				"testCallbacksInterface2.SignaledStart() called"},
			[]string{
				"testCallbacksInterface1.SignaledStart() called"},
			[]string{
				"testCallbacksInterface2.UnserveVolume(,VolumeA) called",
				"testCallbacksInterface2.UnserveVolume(,VolumeE) called"},
			[]string{
				"testCallbacksInterface1.UnserveVolume(,VolumeA) called",
				"testCallbacksInterface1.UnserveVolume(,VolumeE) called"},
			[]string{
				"testCallbacksInterface2.VolumeDestroyed(,VolumeA) called",
				"testCallbacksInterface2.VolumeDestroyed(,VolumeB) called",
				"testCallbacksInterface2.VolumeDestroyed(,VolumeE) called",
				"testCallbacksInterface2.VolumeDestroyed(,VolumeF) called",
				"testCallbacksInterface2.VolumeDestroyed(,VolumeG) called"},
			[]string{
				"testCallbacksInterface1.VolumeDestroyed(,VolumeA) called",
				"testCallbacksInterface1.VolumeDestroyed(,VolumeB) called",
				"testCallbacksInterface1.VolumeDestroyed(,VolumeE) called",
				"testCallbacksInterface1.VolumeDestroyed(,VolumeF) called",
				"testCallbacksInterface1.VolumeDestroyed(,VolumeG) called"},
			[]string{
				"testCallbacksInterface2.VolumeGroupDestroyed(,V_VolumeA_G) called",
				"testCallbacksInterface2.VolumeGroupDestroyed(,V_VolumeB_G) called",
				"testCallbacksInterface2.VolumeGroupDestroyed(,VG_One) called",
				"testCallbacksInterface2.VolumeGroupDestroyed(,VG_Two) called",
				"testCallbacksInterface2.VolumeGroupDestroyed(,VG_Three) called"},
			[]string{
				"testCallbacksInterface1.VolumeGroupDestroyed(,V_VolumeA_G) called",
				"testCallbacksInterface1.VolumeGroupDestroyed(,V_VolumeB_G) called",
				"testCallbacksInterface1.VolumeGroupDestroyed(,VG_One) called",
				"testCallbacksInterface1.VolumeGroupDestroyed(,VG_Two) called",
				"testCallbacksInterface1.VolumeGroupDestroyed(,VG_Three) called"},
			[]string{
				"testCallbacksInterface2.Down() called"},
			[]string{
				"testCallbacksInterface1.Down() called"}})
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) Up(confMap conf.ConfMap) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.Up() called", testCallbacksInterface.name)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.VolumeGroupCreated(,%s,%s,%s) called", testCallbacksInterface.name, volumeGroupName, activePeer, virtualIPAddr)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.VolumeGroupMoved(,%s,%s,%s) called", testCallbacksInterface.name, volumeGroupName, activePeer, virtualIPAddr)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.VolumeGroupDestroyed(,%s) called", testCallbacksInterface.name, volumeGroupName)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.VolumeCreated(,%s,%s) called", testCallbacksInterface.name, volumeName, volumeGroupName)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.VolumeMoved(,%s,%s) called", testCallbacksInterface.name, volumeName, volumeGroupName)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.VolumeDestroyed(,%s) called", testCallbacksInterface.name, volumeName)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.ServeVolume(,%s) called", testCallbacksInterface.name, volumeName)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.UnserveVolume(,%s) called", testCallbacksInterface.name, volumeName)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.SignaledStart() called", testCallbacksInterface.name)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.SignaledFinish() called", testCallbacksInterface.name)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}

func (testCallbacksInterface *testCallbacksInterfaceStruct) Down(confMap conf.ConfMap) (err error) {
	logMessage := fmt.Sprintf("testCallbacksInterface%s.Down() called", testCallbacksInterface.name)
	testCallbacksInterface.t.Logf("  %s", logMessage)
	testCallbackLog = append(testCallbackLog, logMessage)
	return nil
}
