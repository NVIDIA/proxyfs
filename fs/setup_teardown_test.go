package fs

import (
	"io/ioutil"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/transitions"
)

var (
	testConfMap          conf.ConfMap
	testRamswiftDoneChan chan bool     // our test chan used during testTeardown() to know ramswift is, indeed, down
	testVolumeStruct     *volumeStruct // our global volumeStruct to be used in tests
)

func testSetup(t *testing.T, starvationMode bool) {
	var (
		err                    error
		ok                     bool
		signalHandlerIsArmedWG sync.WaitGroup
		testConfMapStrings     []string
		testConfUpdateStrings  []string
		testDir                string
		testVolumeHandle       VolumeHandle
	)

	testDir, err = ioutil.TempDir(os.TempDir(), "ProxyFS_test_fs_")
	if nil != err {
		t.Fatalf("ioutil.TempDir() failed: %v", err)
	}

	err = os.Chdir(testDir)
	if nil != err {
		t.Fatalf("os.Chdir() failed: %v", err)
	}

	err = os.Mkdir("TestVolume", os.ModePerm)

	testConfMapStrings = []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"Logging.LogFilePath=/dev/null",
		"Logging.LogToConsole=false",
		"SwiftClient.NoAuthIPAddr=127.0.0.1",
		"SwiftClient.NoAuthTCPPort=35262",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=3",
		"SwiftClient.RetryLimitObject=3",
		"SwiftClient.RetryDelay=10ms",
		"SwiftClient.RetryDelayObject=10ms",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainerStoragePolicy=silver",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainerNamePrefix=Replicated3Way_",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainersPerPeer=10",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.MaxObjectsPerContainer=1000000",
		"Peer:Peer0.PublicIPAddr=127.0.0.1",
		"Peer:Peer0.PrivateIPAddr=127.0.0.1",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",
		"Cluster.Peers=Peer0",
		"Cluster.WhoAmI=Peer0",
		"Volume:TestVolume.FSID=1",
		"Volume:TestVolume.PrimaryPeer=Peer0",
		"Volume:TestVolume.AccountName=AUTH_test",
		"Volume:TestVolume.AutoFormat=true",
		"Volume:TestVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:TestVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:TestVolume.CheckpointInterval=10s",
		"Volume:TestVolume.DefaultPhysicalContainerLayout=PhysicalContainerLayoutReplicated3Way",
		"Volume:TestVolume.MaxFlushSize=10485760",
		"Volume:TestVolume.MaxFlushTime=10s",
		"Volume:TestVolume.FileDefragmentChunkSize=10485760",
		"Volume:TestVolume.FileDefragmentChunkDelay=10ms",
		"Volume:TestVolume.NonceValuesToReserve=100",
		"Volume:TestVolume.MaxEntriesPerDirNode=32",
		"Volume:TestVolume.MaxExtentsPerFileNode=32",
		"Volume:TestVolume.MaxInodesPerMetadataNode=32",
		"Volume:TestVolume.MaxLogSegmentsPerMetadataNode=64",
		"Volume:TestVolume.MaxDirFileNodesPerMetadataNode=16",
		"Volume:TestVolume.MaxBytesInodeCache=100000",
		"Volume:TestVolume.InodeCacheEvictInterval=1s",
		"VolumeGroup:TestVolumeGroup.VolumeList=TestVolume",
		"VolumeGroup:TestVolumeGroup.VirtualIPAddr=",
		"VolumeGroup:TestVolumeGroup.PrimaryPeer=Peer0",
		"VolumeGroup:TestVolumeGroup.ReadCacheLineSize=1000000",
		"VolumeGroup:TestVolumeGroup.ReadCacheWeight=100",
		"FSGlobals.VolumeGroupList=TestVolumeGroup",
		"FSGlobals.CheckpointHeaderConsensusAttempts=5",
		"FSGlobals.MountRetryLimit=6",
		"FSGlobals.MountRetryDelay=1s",
		"FSGlobals.MountRetryExpBackoff=2",
		"FSGlobals.LogCheckpointHeaderPosts=true",
		"FSGlobals.TryLockBackoffMin=10ms",
		"FSGlobals.TryLockBackoffMax=50ms",
		"FSGlobals.TryLockSerializationThreshhold=5",
		"FSGlobals.SymlinkMax=32",
		"FSGlobals.InodeRecCacheEvictLowLimit=10000",
		"FSGlobals.InodeRecCacheEvictHighLimit=10010",
		"FSGlobals.LogSegmentRecCacheEvictLowLimit=10000",
		"FSGlobals.LogSegmentRecCacheEvictHighLimit=10010",
		"FSGlobals.BPlusTreeObjectCacheEvictLowLimit=10000",
		"FSGlobals.BPlusTreeObjectCacheEvictHighLimit=10010",
		"FSGlobals.DirEntryCacheEvictLowLimit=10000",
		"FSGlobals.DirEntryCacheEvictHighLimit=10010",
		"FSGlobals.FileExtentMapEvictLowLimit=10000",
		"FSGlobals.FileExtentMapEvictHighLimit=10010",
		"FSGlobals.EtcdEnabled=false",
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	if starvationMode {
		testConfUpdateStrings = []string{
			"SwiftClient.ChunkedConnectionPoolSize=1",
			"SwiftClient.NonChunkedConnectionPoolSize=1",
		}
	} else {
		testConfUpdateStrings = []string{
			"SwiftClient.ChunkedConnectionPoolSize=256",
			"SwiftClient.NonChunkedConnectionPoolSize=64",
		}
	}

	err = testConfMap.UpdateFromStrings(testConfUpdateStrings)
	if nil != err {
		t.Fatalf("testConfMap.UpdateFromStrings(testConfUpdateStrings) failed: %v", err)
	}

	signalHandlerIsArmedWG.Add(1)
	testRamswiftDoneChan = make(chan bool, 1)
	go ramswift.Daemon("/dev/null", testConfMapStrings, &signalHandlerIsArmedWG, testRamswiftDoneChan, unix.SIGTERM)

	signalHandlerIsArmedWG.Wait()

	err = transitions.Up(testConfMap)
	if nil != err {
		t.Fatalf("transitions.Up() failed: %v", err)
	}

	testVolumeHandle, err = FetchVolumeHandleByVolumeName("TestVolume")
	if nil != err {
		t.Fatalf("fs.FetchVolumeHandleByVolumeName() failed: %v", err)
	}
	testVolumeStruct, ok = testVolumeHandle.(*volumeStruct)
	if !ok {
		t.Fatalf("fs.Mount() returned !ok")
	}
}

func testTeardown(t *testing.T) {
	var (
		err     error
		testDir string
	)

	err = transitions.Down(testConfMap)
	if nil != err {
		t.Fatalf("transitions.Down() failed: %v", err)
	}

	_ = syscall.Kill(syscall.Getpid(), unix.SIGTERM)
	_ = <-testRamswiftDoneChan

	// Run GC to reclaim memory before we proceed to next test
	runtime.GC()

	testDir, err = os.Getwd()
	if nil != err {
		t.Fatalf("os.Getwd() failed: %v", err)
	}

	err = os.Chdir("..")
	if nil != err {
		t.Fatalf("os.Chdir() failed: %v", err)
	}

	err = os.RemoveAll(testDir)
	if nil != err {
		t.Fatalf("os.RemoveAll() failed: %v", err)
	}
}
