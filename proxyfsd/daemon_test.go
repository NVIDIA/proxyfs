// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package proxyfsd

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/ramswift"
)

func TestMain(m *testing.M) {
	mRunReturn := m.Run()
	os.Exit(mRunReturn)
}

func TestDaemon(t *testing.T) {
	var (
		bytesWritten                          uint64
		confMapStrings                        []string
		confMapStringsWithAutoFormatTrueAdded []string
		createdFileInodeNumber                inode.InodeNumber
		err                                   error
		errChan                               chan error
		execArgs                              []string
		ramswiftDoneChan                      chan bool
		ramswiftSignalHandlerIsArmedWG        sync.WaitGroup
		readData                              []byte
		testVersion                           uint64
		testVersionConfFile                   *os.File
		testVersionConfFileName               string
		toReadFileInodeNumber                 inode.InodeNumber
		volumeHandle                          fs.VolumeHandle
		wg                                    sync.WaitGroup
	)

	// Setup a ramswift instance leveraging test config

	confMapStrings = []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",

		"StatsLogger.Period=0m",
		"StatsLogger.Verbose=false",

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
		"Cluster.LivenessCheckerEnabled=true",
		"Cluster.LivenessCheckRedundancy=2",

		"ProxyfsDebug.DebugServerPort=6058",

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
		"Volume:CommonVolume.FUSEMountPointName=CommonMountPoint",
		"Volume:CommonVolume.NFSExportClientMapList=CommonVolumeNFSClient0",
		"Volume:CommonVolume.SMBShareName=CommonShare",
		"Volume:CommonVolume.PrimaryPeer=Peer0",
		"Volume:CommonVolume.AccountName=AUTH_CommonAccount",
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
		"Volume:CommonVolume.ActiveLeaseEvictLowLimit=5000",
		"Volume:CommonVolume.ActiveLeaseEvictHighLimit=5010",

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
		"FSGlobals.CoalesceElementChunkSize=16",
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

		"JSONRPCServer.TCPPort=12346",     // 12346 instead of 12345 so that test can run if proxyfsd is already running
		"JSONRPCServer.FastTCPPort=32346", // ...and similarly here...
		"JSONRPCServer.DataPathLogging=false",
		"JSONRPCServer.MinLeaseDuration=250ms",
		"JSONRPCServer.LeaseInterruptInterval=250ms",
		"JSONRPCServer.LeaseInterruptLimit=20",

		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",
	}

	testVersionConfFile, err = ioutil.TempFile(os.TempDir(), "proxyfsdTest_")
	if nil != err {
		t.Fatalf("ioutil.TempFile() failed: %v", err)
	}
	testVersionConfFileName = testVersionConfFile.Name()

	_, err = testVersionConfFile.WriteString("[TestVersionSection]\n")
	if nil != err {
		t.Fatalf("testVersionConfFile.WriteString() [case 1] failed: %v", err)
	}
	_, err = testVersionConfFile.WriteString("Version: 1\n")
	if nil != err {
		t.Fatalf("testVersionConfFile.WriteString() [case 2] failed: %v", err)
	}

	err = testVersionConfFile.Close()
	if nil != err {
		t.Fatalf("testVersionConfFile.Close() [case 1] failed: %v", err)
	}

	ramswiftSignalHandlerIsArmedWG.Add(1)
	ramswiftDoneChan = make(chan bool, 1)

	go ramswift.Daemon(testVersionConfFileName, confMapStrings, &ramswiftSignalHandlerIsArmedWG, ramswiftDoneChan, unix.SIGINT)

	ramswiftSignalHandlerIsArmedWG.Wait()

	// Launch an instance of proxyfsd using above config with Volume:CommonVolume.AutoFormat=true

	errChan = make(chan error, 1) // Must be buffered to avoid race

	confMapStringsWithAutoFormatTrueAdded = append(confMapStrings, "Volume:CommonVolume.AutoFormat=true")
	execArgs = append([]string{"daemon_test", "/dev/null"}, confMapStringsWithAutoFormatTrueAdded...)
	go Daemon("/dev/null", confMapStringsWithAutoFormatTrueAdded, errChan, &wg,
		execArgs, unix.SIGTERM, unix.SIGHUP)

	err = <-errChan
	if nil != err {
		t.Fatalf("Daemon() startup failed [case 1a]: %v", err)
	}

	// Write to the volume (with no flush so that only time-based/restart flush is performed)

	volumeHandle, err = fs.FetchVolumeHandleByVolumeName("CommonVolume")
	if nil != err {
		t.Fatalf("fs.FetchVolumeHandleByVolumeName() failed [case 1]: %v", err)
	}

	createdFileInodeNumber, err = volumeHandle.Create(
		inode.InodeRootUserID,
		inode.InodeGroupID(0),
		nil,
		inode.RootDirInodeNumber,
		"TestFile",
		inode.R_OK|inode.W_OK,
	)
	if nil != err {
		t.Fatalf("fs.Create() failed: %v", err)
	}

	bytesWritten, err = volumeHandle.Write(
		inode.InodeRootUserID,
		inode.InodeGroupID(0),
		nil,
		createdFileInodeNumber,
		0,
		[]byte{0x00, 0x01, 0x02, 0x03},
		nil,
	)
	if nil != err {
		t.Fatalf("fs.Write() failed: %v", err)
	}
	if 4 != bytesWritten {
		t.Fatalf("fs.Write() returned unexpected bytesWritten")
	}

	// Verify written data before restart

	toReadFileInodeNumber, err = volumeHandle.Lookup(
		inode.InodeRootUserID,
		inode.InodeGroupID(0),
		nil,
		inode.RootDirInodeNumber,
		"TestFile",
	)
	if nil != err {
		t.Fatalf("fs.Lookup() failed [case 1]: %v", err)
	}

	readData, err = volumeHandle.Read(
		inode.InodeRootUserID,
		inode.InodeGroupID(0),
		nil,
		toReadFileInodeNumber,
		0,
		4,
		nil,
	)
	if nil != err {
		t.Fatalf("fs.Read() failed [case 1]: %v", err)
	}
	if 0 != bytes.Compare([]byte{0x00, 0x01, 0x02, 0x03}, readData) {
		t.Fatalf("fs.Read() returned unexpected readData [case 1]")
	}

	// Send ourself a SIGTERM to signal normal termination of mainWithArgs()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	err = <-errChan

	wg.Wait() // wait for services to go Down()

	if nil != err {
		t.Fatalf("Daemon() exited with error [case 1b]: == %v", err)
	}

	// Relaunch an instance of proxyfsd (without Volume:TestVolume.AutoFormat=true)

	errChan = make(chan error, 1) // Must be buffered to avoid race

	execArgs = append([]string{"daemon_test", testVersionConfFileName}, confMapStrings...)
	go Daemon(testVersionConfFileName, confMapStrings, errChan, &wg,
		execArgs, unix.SIGTERM, unix.SIGHUP)

	err = <-errChan
	if nil != err {
		t.Fatalf("Daemon() startup failed [case 2a]: %v", err)
	}

	// Verify written data after restart

	volumeHandle, err = fs.FetchVolumeHandleByVolumeName("CommonVolume")
	if nil != err {
		t.Fatalf("fs.FetchVolumeHandleByVolumeName() failed [case 2]: %v", err)
	}

	toReadFileInodeNumber, err = volumeHandle.Lookup(
		inode.InodeRootUserID,
		inode.InodeGroupID(0),
		nil,
		inode.RootDirInodeNumber,
		"TestFile",
	)
	if nil != err {
		t.Fatalf("fs.Lookup() failed [case 2]: %v", err)
	}

	readData, err = volumeHandle.Read(
		inode.InodeRootUserID,
		inode.InodeGroupID(0),
		nil,
		toReadFileInodeNumber,
		0,
		4,
		nil,
	)
	if nil != err {
		t.Fatalf("fs.Read() failed [case 2]: %v", err)
	}
	if 0 != bytes.Compare([]byte{0x00, 0x01, 0x02, 0x03}, readData) {
		t.Fatalf("fs.Read() returned unexpected readData [case 2]")
	}

	// Verify [TestVersionSection]Version == 1

	testVersion = fetchTestVersionSectionDotVersion(t)
	if 1 != testVersion {
		t.Fatalf("Before SIGHUP, fetchTestVersionSectionDotVersion() should have returned 1")
	}

	// Update testVersionConfFileName to bump [TestVersionSection]Version to 2

	testVersionConfFile, err = os.OpenFile(testVersionConfFileName, os.O_TRUNC|os.O_WRONLY, 0)
	if nil != err {
		t.Fatalf("os.OpenFile() failed: %v", err)
	}

	_, err = testVersionConfFile.WriteString("[TestVersionSection]\n")
	if nil != err {
		t.Fatalf("testVersionConfFile.WriteString() [case 3] failed: %v", err)
	}
	_, err = testVersionConfFile.WriteString("Version: 2\n")
	if nil != err {
		t.Fatalf("testVersionConfFile.WriteString() [case 4] failed: %v", err)
	}

	err = testVersionConfFile.Close()
	if nil != err {
		t.Fatalf("testVersionConfFile.Close() [case 2] failed: %v", err)
	}

	// Send ourself a SIGHUP to signal reload of testVersionConfFileName
	//   and wait a sufficient amount of time for the reload to complete

	unix.Kill(unix.Getpid(), unix.SIGHUP)
	time.Sleep(time.Second)

	// Verify [TestVersionSection]Version == 2

	testVersion = fetchTestVersionSectionDotVersion(t)
	if 2 != testVersion {
		t.Fatalf("After SIGHUP, fetchTestVersionSectionDotVersion() should have returned 2")
	}

	// Send ourself a SIGTERM to signal normal termination of mainWithArgs()

	unix.Kill(unix.Getpid(), unix.SIGTERM)

	err = <-errChan

	wg.Wait() // wait for services to go Down()

	if nil != err {
		t.Fatalf("Daemon() exited with error [case 2b]: %v", err)
	}

	// Send ourself a SIGINT to also terminate ramswift

	unix.Kill(unix.Getpid(), unix.SIGINT)

	_ = <-ramswiftDoneChan

	// Clean up

	err = os.Remove(testVersionConfFileName)
	if nil != err {
		t.Fatalf("os.Remove(testVersionConfFileName) failed: %v", err)
	}
}

func fetchTestVersionSectionDotVersion(t *testing.T) (version uint64) {
	var (
		body                                         []byte
		bodySections                                 map[string]interface{}
		err                                          error
		ok                                           bool
		resp                                         *http.Response
		testVersionSection                           interface{}
		testVersionSectionMap                        map[string]interface{}
		testVersionSectionMapVersion                 interface{}
		testVersionSectionMapVersionSlice            []interface{}
		testVersionSectionMapVersionSliceElementZero interface{}
		versionAsString                              string
	)

	resp, err = http.Get("http://127.0.0.1:53461/config")
	if nil != err {
		t.Fatalf("http.Get() failed: %v", err)
	}
	body, err = ioutil.ReadAll(resp.Body)
	if nil != err {
		t.Fatalf("ioutil.ReadAll() failed: %v", err)
	}
	err = resp.Body.Close()
	if nil != err {
		t.Fatalf("resp.Body.Close() failed: %v", err)
	}
	bodySections = make(map[string]interface{})
	err = json.Unmarshal(body, &bodySections)
	if nil != err {
		t.Fatalf("json.Unmarshal() failed: %v", err)
	}
	testVersionSection, ok = bodySections["TestVersionSection"]
	if !ok {
		t.Fatalf("bodySections[\"TestVersionSection\"] not found")
	}
	testVersionSectionMap, ok = testVersionSection.(map[string]interface{})
	if !ok {
		t.Fatalf("testVersionSection.(map[string]interface{}) failed")
	}
	testVersionSectionMapVersion, ok = testVersionSectionMap["Version"]
	if !ok {
		t.Fatalf("testVersionSectionMap[\"Version\"] not found")
	}
	testVersionSectionMapVersionSlice, ok = testVersionSectionMapVersion.([]interface{})
	if !ok {
		t.Fatalf("testVersionSectionMapVersion.([]interface{}) failed")
	}
	if 1 != len(testVersionSectionMapVersionSlice) {
		t.Fatalf("testVersionSectionMapVersionSlice should have a single element")
	}
	testVersionSectionMapVersionSliceElementZero = testVersionSectionMapVersionSlice[0]
	versionAsString, ok = testVersionSectionMapVersionSliceElementZero.(string)
	if !ok {
		t.Fatalf("testVersionSectionMapVersionSliceElementZero.(string) failed")
	}
	version, err = strconv.ParseUint(versionAsString, 10, 64)
	if nil != err {
		t.Fatalf("strconv(versionAsString, 10, 64) failed: %v", err)
	}

	return
}
