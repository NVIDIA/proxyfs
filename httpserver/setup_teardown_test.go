package httpserver

import (
	"sync"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/transitions"
)

var (
	ramswiftDoneChan chan bool // our global ramswiftDoneChan used during testTeardown() to know ramswift is, indeed, down
	testConfMap      conf.ConfMap
)

func testSetup(t *testing.T) {
	var (
		err                    error
		signalHandlerIsArmedWG sync.WaitGroup
		testConfMapStrings     []string
		testConfUpdateStrings  []string
	)

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
		"SwiftClient.ChunkedConnectionPoolSize=256",
		"SwiftClient.NonChunkedConnectionPoolSize=64",
		"Peer:Peer0.PublicIPAddr=127.0.0.1",
		"Peer:Peer0.PrivateIPAddr=127.0.0.1",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",
		"Cluster.Peers=Peer0",
		"Cluster.WhoAmI=Peer0",
		"Cluster.PrivateClusterUDPPort=18123",
		"Cluster.UDPPacketSendSize=1400",
		"Cluster.UDPPacketRecvSize=1500",
		"Cluster.UDPPacketCapPerMessage=5",
		"Cluster.HeartBeatDuration=1s",
		"Cluster.HeartBeatMissLimit=3",
		"Cluster.MessageQueueDepthPerPeer=4",
		"Cluster.MaxRequestDuration=1s",
		"Cluster.LivenessCheckRedundancy=2",
		"FSGlobals.VolumeGroupList=",
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
		"HTTPServer.TCPPort=53461",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		t.Fatalf("conf.MakeConfMapFromStrings() failed: %v", err)
	}

	err = testConfMap.UpdateFromStrings(testConfUpdateStrings)
	if nil != err {
		t.Fatalf("testConfMap.UpdateFromStrings(testConfUpdateStrings) failed: %v", err)
	}

	signalHandlerIsArmedWG.Add(1)
	ramswiftDoneChan = make(chan bool, 1)
	go ramswift.Daemon("/dev/null", testConfMapStrings, &signalHandlerIsArmedWG, ramswiftDoneChan, unix.SIGTERM)

	signalHandlerIsArmedWG.Wait()

	err = transitions.Up(testConfMap)
	if nil != err {
		t.Fatalf("transitions.Up() failed: %v", err)
	}
}

func testTeardown(t *testing.T) {
	var (
		err error
	)

	err = transitions.Down(testConfMap)
	if nil != err {
		t.Fatalf("transitions.Down() failed: %v", err)
	}

	_ = syscall.Kill(syscall.Getpid(), unix.SIGTERM)
	_ = <-ramswiftDoneChan
}
