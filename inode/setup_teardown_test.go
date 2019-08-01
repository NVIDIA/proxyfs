package inode

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
	ramswiftDoneChan chan bool
	testConfMap      conf.ConfMap
)

func testSetup(t *testing.T, starvationMode bool) {
	var (
		err                           error
		rLimit                        syscall.Rlimit
		rLimitMinimum                 uint64
		signalHandlerIsArmedWG        sync.WaitGroup
		testChunkedConnectionPoolSize uint64
		testConfStrings               []string
		testConfUpdateStrings         []string
		testDir                       string
	)

	testDir, err = ioutil.TempDir(os.TempDir(), "ProxyFS_test_inode_")
	if nil != err {
		t.Fatalf("ioutil.TempDir() failed: %v", err)
	}

	err = os.Chdir(testDir)
	if nil != err {
		t.Fatalf("os.Chdir() failed: %v", err)
	}

	testConfStrings = []string{
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
		"SwiftClient.RetryExpBackoffObject=1.0",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainerStoragePolicy=silver",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainerNamePrefix=Replicated3Way_",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainersPerPeer=10",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.MaxObjectsPerContainer=1000000",
		"Peer:Peer0.PrivateIPAddr=127.0.0.1",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",
		"Cluster.Peers=Peer0",
		"Cluster.WhoAmI=Peer0",
		"Volume:TestVolume.FSID=1",
		"Volume:TestVolume.AccountName=AUTH_test",
		"Volume:TestVolume.AutoFormat=true",
		"Volume:TestVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:TestVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:TestVolume.CheckpointInterval=10s",
		"Volume:TestVolume.DefaultPhysicalContainerLayout=PhysicalContainerLayoutReplicated3Way",
		"Volume:TestVolume.MaxFlushSize=10000000",
		"Volume:TestVolume.MaxFlushTime=10s",
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
		"FSGlobals.MountRetryLimit=6",
		"FSGlobals.MountRetryDelay=1s",
		"FSGlobals.MountRetryExpBackoff=2",
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

	testConfMap, err = conf.MakeConfMapFromStrings(testConfStrings)
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

	testChunkedConnectionPoolSize, err = testConfMap.FetchOptionValueUint64("SwiftClient", "ChunkedConnectionPoolSize")
	if nil != err {
		t.Fatalf("testConfMap.FetchOptionValueUint64(\"SwiftClient\", \"ChunkedConnectionPoolSize\") failed: %v", err)
	}

	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if nil != err {
		t.Fatalf("syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)() failed: %v", err)
	}
	rLimitMinimum = 3 * testChunkedConnectionPoolSize
	if rLimitMinimum > rLimit.Cur {
		t.Errorf("RLIMIT too low... must be at least %v (was %v)", rLimitMinimum, rLimit.Cur)
		t.Errorf("On Mac:")
		t.Errorf("  <CHECK CURRENT KERNEL LIMITS>")
		t.Errorf("    sysctl -a | grep ^kern.maxfiles")
		t.Errorf("  <ADJUST KERNEL LIMITS IF NECESSARY>")
		t.Errorf("    sudo /usr/libexec/PlistBuddy /Library/LaunchAgents/com.kern.maxfiles.plist -c \"add Label string com.kern.maxfiles\" -c \"add ProgramArguments array\" -c \"add ProgramArguments: string sysctl\" -c \"add ProgramArguments: string -w\" -c \"add ProgramArguments: string kern.maxfiles=20480\" -c \"add RunAtLoad bool true\"")
		t.Errorf("    sudo /usr/libexec/PlistBuddy /Library/LaunchAgents/com.kern.maxfilesperproc.plist -c \"add Label string com.kern.maxfilesperproc\" -c \"add ProgramArguments array\" -c \"add ProgramArguments: string sysctl\" -c \"add ProgramArguments: string -w\" -c \"add ProgramArguments: string kern.perprocmaxfiles=10240\" -c \"add RunAtLoad bool true\"")
		t.Errorf("  <ADJUST PROCESS HARD LIMITS IF NECESSARY>")
		t.Errorf("    sudo /usr/libexec/PlistBuddy /Library/LaunchAgents/com.launchd.maxfiles.plist -c \"add Label string com.launchd.maxfiles\" -c \"add ProgramArguments array\" -c \"add ProgramArguments: string launchctl\" -c \"add ProgramArguments: string limit\" -c \"add ProgramArguments: string maxfiles\" -c \"add ProgramArguments: string 5120\" -c \"add ProgramArguments: string unlimited\" -c \"add RunAtLoad bool true\"")
		t.Errorf("  <RESTART>")
		t.Errorf("  <ADJUST PROCESS SOFT LIMITS IF NECESSARY>")
		t.Errorf("    ulimit -n 2560")
		t.Errorf("On Linux:")
		t.Fatalf("  ulimit -n 2560")
	}

	signalHandlerIsArmedWG.Add(1)
	ramswiftDoneChan = make(chan bool, 1)
	go ramswift.Daemon("/dev/null", testConfStrings, &signalHandlerIsArmedWG, ramswiftDoneChan, unix.SIGTERM)

	signalHandlerIsArmedWG.Wait()

	err = transitions.Up(testConfMap)
	if nil != err {
		t.Fatalf("transitions.Up() failed: %v", err)
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
	_ = <-ramswiftDoneChan

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
