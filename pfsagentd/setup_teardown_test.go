package main

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/proxyfsd"
	"github.com/swiftstack/ProxyFS/ramswift"
)

type testDaemonGlobalsStruct struct {
	proxyfsdErrChan  chan error
	proxyfsdWG       sync.WaitGroup
	ramswiftDoneChan chan bool
}

var testDaemonGlobals testDaemonGlobalsStruct

func testSetup(t *testing.T) {
	var (
		err                            error
		ramswiftSignalHandlerIsArmedWG sync.WaitGroup
		testConfMap                    conf.ConfMap
		testConfStrings                []string
		testDir                        string
	)

	testDir, err = ioutil.TempDir(os.TempDir(), "pfsagentd_test_")
	if nil != err {
		t.Fatalf("ioutil.TempDir() failed: %v", err)
	}

	err = os.Chdir(testDir)
	if nil != err {
		t.Fatalf("os.Chdir() failed: %v", err)
	}

	err = os.Mkdir("ProxyFSMountPointPath", 0777) // Agent.FUSEMountPointPath
	if nil != err {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}

	err = os.Mkdir("PfsAgentMountPointPath", 0777) // Volume:CommonVolume.FUSEMountPointName
	if nil != err {
		t.Fatalf("os.Mkdir() failed: %v", err)
	}

	testConfStrings = []string{
		"Agent.FUSEVolumeName=CommonVolume",
		"Agent.FUSEMountPointPath=PfsAgentMountPointPath",
		"Agent.FUSEUnMountRetryDelay=100ms",
		"Agent.FUSEUnMountRetryCap=100",
		"Agent.SwiftAuthURL=http://" + testSwiftProxyAddr + "/auth/v1.0",
		"Agent.SwiftAuthUser=test:tester",
		"Agent.SwiftAuthKey=testing",
		"Agent.SwiftAccountName=AUTH_test",
		"Agent.SwiftTimeout=10s",
		"Agent.SwiftRetryLimit=10",
		"Agent.SwiftRetryDelay=1s",
		"Agent.SwiftRetryExpBackoff=1.4",
		"Agent.SwiftConnectionPoolSize=100",
		"Agent.ReadCacheLineSize=1048576",
		"Agent.ReadCacheLineCount=1000",
		"Agent.ReadPlanLineSize=1048576",
		"Agent.ReadPlanLineCount=1000",
		"Agent.LogFilePath=",
		"Agent.LogToConsole=true",
		"Agent.TraceEnabled=false",

		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",

		"StatsLogger.Period=10m",

		"Logging.LogFilePath=/dev/null",
		"Logging.LogToConsole=false",

		"Peer:Peer0.PublicIPAddr=127.0.0.1",
		"Peer:Peer0.PrivateIPAddr=127.0.0.1",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",

		"Cluster.WhoAmI=Peer0",
		"Cluster.Peers=Peer0",
		"Cluster.ServerGuid=a66488e9-a051-4ff7-865d-87bfb84cc2ae",
		"Cluster.PrivateClusterUDPPort=18123",
		"Cluster.UDPPacketSendSize=1400",
		"Cluster.UDPPacketRecvSize=1500",
		"Cluster.UDPPacketCapPerMessage=5",
		"Cluster.HeartBeatDuration=1s",
		"Cluster.HeartBeatMissLimit=3",
		"Cluster.MessageQueueDepthPerPeer=4",
		"Cluster.MaxRequestDuration=1s",
		"Cluster.LivenessCheckRedundancy=2",

		"HTTPServer.TCPPort=53461",

		"SwiftClient.NoAuthIPAddr=127.0.0.1",
		"SwiftClient.NoAuthTCPPort=35262",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=1",
		"SwiftClient.RetryLimitObject=1",
		"SwiftClient.RetryDelay=10ms",
		"SwiftClient.RetryDelayObject=10ms",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",

		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainerStoragePolicy=silver",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainerNamePrefix=Replicated3Way_",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainersPerPeer=10",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.MaxObjectsPerContainer=1000000",

		"Volume:CommonVolume.FSID=1",
		"Volume:CommonVolume.FUSEMountPointName=ProxyFSMountPointPath",
		"Volume:CommonVolume.NFSExportName=CommonExport",
		"Volume:CommonVolume.SMBShareName=CommonShare",
		"Volume:CommonVolume.PrimaryPeer=Peer0",
		"Volume:CommonVolume.AccountName=AUTH_CommonAccount",
		"Volume:CommonVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:CommonVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:CommonVolume.CheckpointInterval=10s",
		"Volume:CommonVolume.DefaultPhysicalContainerLayout=PhysicalContainerLayoutReplicated3Way",
		"Volume:CommonVolume.MaxFlushSize=10000000",
		"Volume:CommonVolume.MaxFlushTime=10s",
		"Volume:CommonVolume.NonceValuesToReserve=100",
		"Volume:CommonVolume.MaxEntriesPerDirNode=32",
		"Volume:CommonVolume.MaxExtentsPerFileNode=32",
		"Volume:CommonVolume.MaxInodesPerMetadataNode=32",
		"Volume:CommonVolume.MaxLogSegmentsPerMetadataNode=64",
		"Volume:CommonVolume.MaxDirFileNodesPerMetadataNode=16",
		"Volume:CommonVolume.MaxBytesInodeCache=100000",
		"Volume:CommonVolume.InodeCacheEvictInterval=1s",
		"Volume:CommonVolume.AutoFormat=true",

		"VolumeGroup:CommonVolumeGroup.VolumeList=CommonVolume",
		"VolumeGroup:CommonVolumeGroup.VirtualIPAddr=",
		"VolumeGroup:CommonVolumeGroup.PrimaryPeer=Peer0",
		"VolumeGroup:CommonVolumeGroup.ReadCacheLineSize=1000000",
		"VolumeGroup:CommonVolumeGroup.ReadCacheWeight=100",

		"FSGlobals.VolumeGroupList=CommonVolumeGroup",
		"FSGlobals.TryLockBackoffMin=100us",
		"FSGlobals.TryLockBackoffMax=300us",
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

		"JSONRPCServer.TCPPort=12346",     // 12346 instead of 12345 so that test can run if proxyfsd is already running
		"JSONRPCServer.FastTCPPort=32346", // ...and similarly here...
		"JSONRPCServer.DataPathLogging=false",

		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",
	}

	ramswiftSignalHandlerIsArmedWG.Add(1)
	testDaemonGlobals.ramswiftDoneChan = make(chan bool, 1)

	go ramswift.Daemon("/dev/null", testConfStrings, &ramswiftSignalHandlerIsArmedWG, testDaemonGlobals.ramswiftDoneChan, unix.SIGUSR1)

	ramswiftSignalHandlerIsArmedWG.Wait()

	testDaemonGlobals.proxyfsdErrChan = make(chan error, 1) // Must be buffered to avoid race

	go proxyfsd.Daemon("/dev/null", testConfStrings, testDaemonGlobals.proxyfsdErrChan, &testDaemonGlobals.proxyfsdWG, []string{}, unix.SIGUSR2)

	err = <-testDaemonGlobals.proxyfsdErrChan
	if nil != err {
		t.Fatalf("proxyfsd.Daemon() startup failed: %v", err)
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	globals.logFile = nil
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true

	startSwiftProxyEmulator(t, testConfMap)

	initializeGlobals(testConfMap)

	performMount()
}

func testTeardown(t *testing.T) {
	var (
		err     error
		testDir string
	)

	performUnmount()

	unix.Kill(unix.Getpid(), unix.SIGUSR2)

	err = <-testDaemonGlobals.proxyfsdErrChan

	testDaemonGlobals.proxyfsdWG.Wait()

	if nil != err {
		t.Fatalf("proxyfsd.Daemon() exited with error: %v", err)
	}

	unix.Kill(unix.Getpid(), unix.SIGUSR1)

	_ = <-testDaemonGlobals.ramswiftDoneChan

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

	stopSwiftProxyEmulator()
}
