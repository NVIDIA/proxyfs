package main

import (
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/proxyfsd"
	"github.com/swiftstack/ProxyFS/ramswift"
)

const (
	testDaemonStartPollInterval = 1 * time.Second
	testProxyFSDaemonHTTPPort   = "54321"
	testProxyFSDaemonIPAddr     = "127.0.0.1"
	testSwiftNoAuthIPAddr       = "127.0.0.1"
	testSwiftNoAuthPort         = "54322"
	testSwiftProxyAddr          = "localhost:38080"
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
		infoResponse                   *http.Response
		ramswiftSignalHandlerIsArmedWG sync.WaitGroup
		testConfMap                    conf.ConfMap
		testConfStrings                []string
		testDir                        string
		versionResponse                *http.Response
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
		"Agent.SwiftTimeout=20s",
		"Agent.SwiftRetryLimit=2",
		"Agent.SwiftRetryDelay=1s",
		"Agent.SwiftRetryExpBackoff=1.4",
		"Agent.SwiftConnectionPoolSize=200",
		"Agent.FetchExtentsFromFileOffset=32",
		"Agent.FetchExtentsBeforeFileOffset=0",
		"Agent.ReadCacheLineSize=1048576",
		"Agent.ReadCacheLineCount=1000",
		"Agent.SharedFileLimit=1000",
		"Agent.ExclusiveFileLimit=100",
		"Agent.DirtyFileLimit=50",
		"Agent.MaxFlushSize=10485760",
		"Agent.MaxFlushTime=10s",
		"Agent.LogFilePath=",
		"Agent.LogToConsole=false",
		"Agent.TraceEnabled=false",
		"Agent.HTTPServerIPAddr=127.0.0.1",
		"Agent.HTTPServerTCPPort=54323",
		"Agent.ReadDirPlusEnabled=false",
		"Agent.XAttrEnabled=false",
		"Agent.EntryDuration=10s",
		"Agent.AttrDuration=10s",
		"Agent.AttrBlockSize=65536",
		"Agent.ReaddirMaxEntries=1024",
		"Agent.FUSEMaxBackground=100",
		"Agent.FUSECongestionThreshhold=0",
		"Agent.FUSEMaxWrite=131072", // Linux max... 128KiB is good enough for testing

		"Stats.IPAddr=localhost",
		"Stats.UDPPort=54324",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",

		"StatsLogger.Period=0m",
		"StatsLogger.Verbose=false",

		"Logging.LogFilePath=/dev/null",
		"Logging.LogToConsole=false",

		"Peer:Peer0.PublicIPAddr=" + testProxyFSDaemonIPAddr,
		"Peer:Peer0.PrivateIPAddr=" + testProxyFSDaemonIPAddr,
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",

		"Cluster.WhoAmI=Peer0",
		"Cluster.Peers=Peer0",
		"Cluster.ServerGuid=a66488e9-a051-4ff7-865d-87bfb84cc2ae",
		"Cluster.PrivateClusterUDPPort=54325",
		"Cluster.UDPPacketSendSize=1400",
		"Cluster.UDPPacketRecvSize=1500",
		"Cluster.UDPPacketCapPerMessage=5",
		"Cluster.HeartBeatDuration=1s",
		"Cluster.HeartBeatMissLimit=3",
		"Cluster.MessageQueueDepthPerPeer=4",
		"Cluster.MaxRequestDuration=1s",
		"Cluster.LivenessCheckRedundancy=2",

		"HTTPServer.TCPPort=" + testProxyFSDaemonHTTPPort,

		"SwiftClient.NoAuthIPAddr=" + testSwiftNoAuthIPAddr,
		"SwiftClient.NoAuthTCPPort=" + testSwiftNoAuthPort,
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
		"Volume:CommonVolume.NFSExportClientMapList=CommonVolumeNFSClient0",
		"Volume:CommonVolume.SMBShareName=CommonShare",
		"Volume:CommonVolume.PrimaryPeer=Peer0",
		"Volume:CommonVolume.AccountName=AUTH_test",
		"Volume:CommonVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:CommonVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:CommonVolume.CheckpointInterval=10s",
		"Volume:CommonVolume.DefaultPhysicalContainerLayout=PhysicalContainerLayoutReplicated3Way",
		"Volume:CommonVolume.MaxFlushSize=10485760",
		"Volume:CommonVolume.MaxFlushTime=10s",
		"Volume:CommonVolume.FileDefragmentChunkSize=10485760",
		"Volume:CommonVolume.FileDefragmentChunkDelay=10ms",
		"Volume:CommonVolume.NonceValuesToReserve=100",
		"Volume:CommonVolume.MaxEntriesPerDirNode=32",
		"Volume:CommonVolume.MaxExtentsPerFileNode=32",
		"Volume:CommonVolume.MaxInodesPerMetadataNode=32",
		"Volume:CommonVolume.MaxLogSegmentsPerMetadataNode=64",
		"Volume:CommonVolume.MaxDirFileNodesPerMetadataNode=16",
		"Volume:CommonVolume.MaxBytesInodeCache=100000",
		"Volume:CommonVolume.InodeCacheEvictInterval=1s",
		"Volume:CommonVolume.AutoFormat=true",

		"NFSClientMap:CommonVolumeNFSClient0.ClientPattern=*",
		"NFSClientMap:CommonVolumeNFSClient0.AccessMode=rw",
		"NFSClientMap:CommonVolumeNFSClient0.RootSquash=no_root_squash",
		"NFSClientMap:CommonVolumeNFSClient0.Secure=insecure",

		"VolumeGroup:CommonVolumeGroup.VolumeList=CommonVolume",
		"VolumeGroup:CommonVolumeGroup.VirtualIPAddr=",
		"VolumeGroup:CommonVolumeGroup.PrimaryPeer=Peer0",
		"VolumeGroup:CommonVolumeGroup.ReadCacheLineSize=1000000",
		"VolumeGroup:CommonVolumeGroup.ReadCacheWeight=100",

		"FSGlobals.VolumeGroupList=CommonVolumeGroup",
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

		"JSONRPCServer.TCPPort=54326",
		"JSONRPCServer.FastTCPPort=54327",
		"JSONRPCServer.RetryRPCPort=54328",
		"JSONRPCServer.RetryRPCTTLCompleted=10s",
		"JSONRPCServer.RetryRPCAckTrim=10ms",
		"JSONRPCServer.DataPathLogging=false",
	}

	testConfStrings = append(testConfStrings, "RamSwiftInfo.MaxAccountNameLength="+strconv.FormatUint(testMaxAccountNameLength, 10))
	testConfStrings = append(testConfStrings, "RamSwiftInfo.MaxContainerNameLength="+strconv.FormatUint(testMaxContainerNameLength, 10))
	testConfStrings = append(testConfStrings, "RamSwiftInfo.MaxObjectNameLength="+strconv.FormatUint(testMaxObjectNameLength, 10))
	testConfStrings = append(testConfStrings, "RamSwiftInfo.AccountListingLimit="+strconv.FormatUint(testAccountListingLimit, 10))
	testConfStrings = append(testConfStrings, "RamSwiftInfo.ContainerListingLimit="+strconv.FormatUint(testContainerListingLimit, 10))

	ramswiftSignalHandlerIsArmedWG.Add(1)
	testDaemonGlobals.ramswiftDoneChan = make(chan bool, 1)

	go ramswift.Daemon("/dev/null", testConfStrings, &ramswiftSignalHandlerIsArmedWG, testDaemonGlobals.ramswiftDoneChan, unix.SIGUSR1)

	ramswiftSignalHandlerIsArmedWG.Wait()

	for {
		infoResponse, err = http.Get("http://" + testSwiftNoAuthIPAddr + ":" + testSwiftNoAuthPort + "/info")
		if (nil == err) && (http.StatusOK == infoResponse.StatusCode) {
			break
		}

		time.Sleep(testDaemonStartPollInterval)
	}

	testDaemonGlobals.proxyfsdErrChan = make(chan error, 1) // Must be buffered to avoid race

	go proxyfsd.Daemon("/dev/null", testConfStrings, testDaemonGlobals.proxyfsdErrChan, &testDaemonGlobals.proxyfsdWG, []string{}, unix.SIGUSR2)

	err = <-testDaemonGlobals.proxyfsdErrChan
	if nil != err {
		t.Fatalf("proxyfsd.Daemon() startup failed: %v", err)
	}

	for {
		versionResponse, err = http.Get("http://" + testProxyFSDaemonIPAddr + ":" + testProxyFSDaemonHTTPPort + "/version")
		if (nil == err) && (http.StatusOK == versionResponse.StatusCode) {
			break
		}

		time.Sleep(testDaemonStartPollInterval)
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

	doMountProxyFS()

	performMountFUSE()
}

func testTeardown(t *testing.T) {
	var (
		err     error
		testDir string
	)

	performUnmountFUSE()

	doUnmountProxyFS()

	uninitializeGlobals()

	_ = unix.Kill(unix.Getpid(), unix.SIGUSR2)

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
