// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package jrpcfs

import (
	"crypto/tls"
	"crypto/x509/pkix"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/fs"
	"github.com/NVIDIA/proxyfs/icert/icertpkg"
	"github.com/NVIDIA/proxyfs/inode"
	"github.com/NVIDIA/proxyfs/logger"
	"github.com/NVIDIA/proxyfs/ramswift"
	"github.com/NVIDIA/proxyfs/transitions"
)

// Shorthand for our testing debug log id; global to the package
var test_debug = logger.DbgTesting

const testVer = "/v1/"
const testAccountName = "AN_account"
const testContainerName = "test_container"
const testVerAccountName = testVer + testAccountName
const testVerAccountContainerName = testVerAccountName + "/" + testContainerName
const testAccountName2 = "AN_account2"

const testRequireSlashesInPathsToProperlySort = false

type testTLSCertsStruct struct {
	caCertPEMBlock       []byte
	caKeyPEMBlock        []byte
	endpointCertPEMBlock []byte
	endpointKeyPEMBlock  []byte
	endpointTLSCert      tls.Certificate
	caCertFile           string
	caKeyFile            string
	endpointCertFile     string
	endpointKeyFile      string
}

var testTLSCerts *testTLSCertsStruct

func TestMain(m *testing.M) {
	//setup, run, teardown, exit

	cleanupFuncs := testSetup()

	makeSomeFilesAndSuch()

	verdict := m.Run()

	for _, cleanupFunc := range cleanupFuncs {
		cleanupFunc()
	}

	os.Exit(verdict)
}

func testSetup() []func() {
	var (
		cleanupFuncs           []func()
		cleanupTempDir         func()
		confStrings            []string
		doneChan               chan bool
		err                    error
		signalHandlerIsArmedWG sync.WaitGroup
		tempDir                string
		testConfMap            conf.ConfMap
	)

	cleanupFuncs = make([]func(), 0)

	tempDir, err = ioutil.TempDir("", "jrpcfs_test")
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}
	cleanupTempDir = func() {
		_ = os.RemoveAll(tempDir)
	}
	cleanupFuncs = append(cleanupFuncs, cleanupTempDir)

	testTLSCerts = &testTLSCertsStruct{
		caCertFile:       tempDir + "/caCertFile",
		caKeyFile:        tempDir + "/caKeyFile",
		endpointCertFile: tempDir + "/endpoingCertFile",
		endpointKeyFile:  tempDir + "/endpointKeyFile",
	}

	testTLSCerts.caCertPEMBlock, testTLSCerts.caKeyPEMBlock, err = icertpkg.GenCACert(
		icertpkg.GenerateKeyAlgorithmEd25519,
		pkix.Name{
			Organization:  []string{"Test Organization CA"},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		time.Hour,
		testTLSCerts.caCertFile,
		testTLSCerts.caKeyFile)
	if nil != err {
		panic(fmt.Errorf("icertpkg.GenCACert() failed: %v", err))
	}

	testTLSCerts.endpointCertPEMBlock, testTLSCerts.endpointKeyPEMBlock, err = icertpkg.GenEndpointCert(
		icertpkg.GenerateKeyAlgorithmEd25519,
		pkix.Name{
			Organization:  []string{"Test Organization Endpoint"},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		[]string{},
		[]net.IP{net.ParseIP("127.0.0.1")},
		time.Hour,
		testTLSCerts.caCertPEMBlock,
		testTLSCerts.caKeyPEMBlock,
		testTLSCerts.endpointCertFile,
		testTLSCerts.endpointKeyFile)
	if nil != err {
		panic(fmt.Errorf("icertpkg.genEndpointCert() failed: %v", err))
	}

	testTLSCerts.endpointTLSCert, err = tls.X509KeyPair(testTLSCerts.endpointCertPEMBlock, testTLSCerts.endpointKeyPEMBlock)
	if nil != err {
		panic(fmt.Errorf("tls.LoadX509KeyPair() failed: %v", err))
	}

	confStrings = []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"FSGlobals.VolumeGroupList=JrpcfsTestVolumeGroup",
		"FSGlobals.CheckpointHeaderConsensusAttempts=5",
		"FSGlobals.MountRetryLimit=6",
		"FSGlobals.MountRetryDelay=1s",
		"FSGlobals.MountRetryExpBackoff=2",
		"FSGlobals.LogCheckpointHeaderPosts=true",
		"FSGlobals.TryLockBackoffMin=10ms",
		"FSGlobals.TryLockBackoffMax=50ms",
		"FSGlobals.TryLockSerializationThreshhold=5",
		"FSGlobals.SymlinkMax=8",
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
		"SwiftClient.NoAuthIPAddr=127.0.0.1",
		"SwiftClient.NoAuthTCPPort=35262",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=3",
		"SwiftClient.RetryLimitObject=3",
		"SwiftClient.RetryDelay=10ms",
		"SwiftClient.RetryDelayObject=10ms",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=256",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",
		"Peer:Peer0.PublicIPAddr=127.0.0.1",
		"Peer:Peer0.PrivateIPAddr=127.0.0.1",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",
		"Cluster.Peers=Peer0",
		"Cluster.WhoAmI=Peer0",
		"Volume:SomeVolume.FSID=1",
		"Volume:SomeVolume.AccountName=" + testAccountName,
		"Volume:SomeVolume.AutoFormat=true",
		"Volume:SomeVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:SomeVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:SomeVolume.CheckpointInterval=10s",
		"Volume:SomeVolume.DefaultPhysicalContainerLayout=SomeContainerLayout",
		"Volume:SomeVolume.MaxFlushSize=10027008",
		"Volume:SomeVolume.MaxFlushTime=2s",
		"Volume:SomeVolume.FileDefragmentChunkSize=10027008",
		"Volume:SomeVolume.FileDefragmentChunkDelay=2ms",
		"Volume:SomeVolume.NonceValuesToReserve=100",
		"Volume:SomeVolume.MaxEntriesPerDirNode=32",
		"Volume:SomeVolume.MaxExtentsPerFileNode=32",
		"Volume:SomeVolume.MaxInodesPerMetadataNode=32",
		"Volume:SomeVolume.MaxLogSegmentsPerMetadataNode=64",
		"Volume:SomeVolume.MaxDirFileNodesPerMetadataNode=16",
		"Volume:SomeVolume.MaxBytesInodeCache=100000",
		"Volume:SomeVolume.InodeCacheEvictInterval=1s",
		"Volume:SomeVolume.ActiveLeaseEvictLowLimit=2",
		"Volume:SomeVolume.ActiveLeaseEvictHighLimit=4",
		"Volume:SomeVolume2.FSID=2",
		"Volume:SomeVolume2.AccountName=" + testAccountName2,
		"Volume:SomeVolume2.AutoFormat=true",
		"Volume:SomeVolume2.CheckpointContainerName=.__checkpoint__",
		"Volume:SomeVolume2.CheckpointContainerStoragePolicy=gold",
		"Volume:SomeVolume2.CheckpointInterval=10s",
		"Volume:SomeVolume2.DefaultPhysicalContainerLayout=SomeContainerLayout2",
		"Volume:SomeVolume2.MaxFlushSize=10027008",
		"Volume:SomeVolume2.MaxFlushTime=2s",
		"Volume:SomeVolume2.FileDefragmentChunkSize=10027008",
		"Volume:SomeVolume2.FileDefragmentChunkDelay=2ms",
		"Volume:SomeVolume2.NonceValuesToReserve=100",
		"Volume:SomeVolume2.MaxEntriesPerDirNode=32",
		"Volume:SomeVolume2.MaxExtentsPerFileNode=32",
		"Volume:SomeVolume2.MaxInodesPerMetadataNode=32",
		"Volume:SomeVolume2.MaxLogSegmentsPerMetadataNode=64",
		"Volume:SomeVolume2.MaxDirFileNodesPerMetadataNode=16",
		"Volume:SomeVolume2.MaxBytesInodeCache=100000",
		"Volume:SomeVolume2.InodeCacheEvictInterval=1s",
		"Volume:SomeVolume2.ActiveLeaseEvictLowLimit=5000",
		"Volume:SomeVolume2.ActiveLeaseEvictHighLimit=5010",
		"VolumeGroup:JrpcfsTestVolumeGroup.VolumeList=SomeVolume,SomeVolume2",
		"VolumeGroup:JrpcfsTestVolumeGroup.VirtualIPAddr=",
		"VolumeGroup:JrpcfsTestVolumeGroup.PrimaryPeer=Peer0",
		"VolumeGroup:JrpcfsTestVolumeGroup.ReadCacheLineSize=1000000",
		"VolumeGroup:JrpcfsTestVolumeGroup.ReadCacheWeight=100",
		"PhysicalContainerLayout:SomeContainerLayout.ContainerStoragePolicy=silver",
		"PhysicalContainerLayout:SomeContainerLayout.ContainerNamePrefix=kittens",
		"PhysicalContainerLayout:SomeContainerLayout.ContainersPerPeer=10",
		"PhysicalContainerLayout:SomeContainerLayout.MaxObjectsPerContainer=1000000",
		"PhysicalContainerLayout:SomeContainerLayout2.ContainerStoragePolicy=silver",
		"PhysicalContainerLayout:SomeContainerLayout2.ContainerNamePrefix=puppies",
		"PhysicalContainerLayout:SomeContainerLayout2.ContainersPerPeer=10",
		"PhysicalContainerLayout:SomeContainerLayout2.MaxObjectsPerContainer=1000000",
		"Logging.LogFilePath=/dev/null",
		"Logging.LogToConsole=false",
		"JSONRPCServer.TCPPort=12346",      // 12346 instead of 12345 so that test can run if proxyfsd is already running
		"JSONRPCServer.FastTCPPort=32346",  // ...and similarly here...
		"JSONRPCServer.RetryRPCPort=32357", // ...and similarly here...
		"JSONRPCServer.RetryRPCTTLCompleted=10s",
		"JSONRPCServer.RetryRPCAckTrim=10ms",
		"JSONRPCServer.RetryRPCCertFilePath=" + testTLSCerts.endpointCertFile,
		"JSONRPCServer.RetryRPCKeyFilePath=" + testTLSCerts.endpointKeyFile,
		"JSONRPCServer.DataPathLogging=false",
		"JSONRPCServer.MinLeaseDuration=100ms",
		"JSONRPCServer.LeaseInterruptInterval=100ms",
		"JSONRPCServer.LeaseInterruptLimit=5",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}

	signalHandlerIsArmedWG.Add(1)
	doneChan = make(chan bool)
	go ramswift.Daemon("/dev/null", confStrings, &signalHandlerIsArmedWG, doneChan, unix.SIGTERM)

	signalHandlerIsArmedWG.Wait()

	err = transitions.Up(testConfMap)
	if nil != err {
		panic(fmt.Sprintf("transitions.Up() failed: %v", err))
	}

	return cleanupFuncs
}

func makeSomeFilesAndSuch() {
	// we should have enough stuff up now that we can actually make
	// some files and directories and such
	volumeHandle, err := fs.FetchVolumeHandleByVolumeName("SomeVolume")
	if nil != err {
		panic(fmt.Sprintf("failed to mount SomeVolume: %v", err))
	}

	cInode := fsMkDir(volumeHandle, inode.RootDirInodeNumber, "c")
	cNestedInode := fsMkDir(volumeHandle, inode.RootDirInodeNumber, "c-nested")
	fsCreateSymlink(volumeHandle, inode.RootDirInodeNumber, "c-symlink", "c")

	err = volumeHandle.MiddlewarePost("", "c", []byte("metadata for c"), []byte{})
	if err != nil {
		panic(err)
	}
	_ = fsMkDir(volumeHandle, inode.RootDirInodeNumber, "c-no-metadata")
	_ = fsMkDir(volumeHandle, cInode, "empty-directory")

	readmeInode := fsCreateFile(volumeHandle, cInode, "README")
	_, err = volumeHandle.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil, readmeInode, 0, []byte("who am I kidding? nobody reads these."), nil)
	err = volumeHandle.MiddlewarePost("", "c/README", []byte("metadata for c/README"), []byte{})
	if err != nil {
		panic(err)
	}

	animalsInode := fsMkDir(volumeHandle, cInode, "animals")
	files := map[string]string{
		"dog.txt":      "dog goes woof",
		"cat.txt":      "cat goes meow",
		"bird.txt":     "bird goes tweet",
		"mouse.txt":    "mouse goes squeak",
		"cow.txt":      "cow goes moo",
		"frog.txt":     "frog goes croak",
		"elephant.txt": "elephant goes toot",
	}
	for fileName, fileContents := range files {
		fileInode := fsCreateFile(volumeHandle, animalsInode, fileName)

		_, err = volumeHandle.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil, fileInode, 0, []byte(fileContents), nil)
		if err != nil {
			panic(fmt.Sprintf("failed to write file %s: %v", fileName, err))
		}
	}

	plantsInode := fsMkDir(volumeHandle, cInode, "plants")
	ino := fsCreateFile(volumeHandle, cInode, "plants-README")
	_, err = volumeHandle.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil, ino, 0, []byte("nah"), nil)
	if err != nil {
		panic(fmt.Sprintf("failed to write file plants-README: %v", err))
	}

	files = map[string]string{
		// Random contents of varying lengths.
		"aloe.txt":     "skiameter-interlope",
		"banana.txt":   "ring ring ring ring ring ring ring bananaphone",
		"cherry.txt":   "archegonium-nonresidentiary",
		"eggplant.txt": "bowk-unruled",
	}

	for fileName, fileContents := range files {
		fileInode := fsCreateFile(volumeHandle, plantsInode, fileName)

		_, err = volumeHandle.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil, fileInode, 0, []byte(fileContents), nil)
		if err != nil {
			panic(fmt.Sprintf("failed to write file %s: %v", fileName, err))
		}
	}

	fsCreateSymlink(volumeHandle, cInode, "plants-symlink", "plants")
	fsCreateSymlink(volumeHandle, plantsInode, "eggplant.txt-symlink", "eggplant.txt")

	// Put some deeply nested things in c-nested. This listing is a
	// shortened version of a real directory tree that exposed a bug.
	fsCreateFile(volumeHandle, cNestedInode, ".DS_Store")
	dotGitInode := fsMkDir(volumeHandle, cNestedInode, ".git")
	fsCreateFile(volumeHandle, dotGitInode, ".DS_Store")
	fsCreateFile(volumeHandle, dotGitInode, "COMMIT_EDITMSG")
	fsCreateFile(volumeHandle, dotGitInode, "FETCH_HEAD")
	fsCreateFile(volumeHandle, dotGitInode, "HEAD")
	fsCreateFile(volumeHandle, dotGitInode, "ORIG_HEAD")
	fsCreateFile(volumeHandle, dotGitInode, "index")
	dotGitHooks := fsMkDir(volumeHandle, dotGitInode, "hooks")
	fsCreateFile(volumeHandle, dotGitHooks, ".DS_Store")
	fsCreateFile(volumeHandle, dotGitHooks, "applypatch-msg.sample")
	fsCreateFile(volumeHandle, dotGitHooks, "commit-msg.sample")
	dotGitLogs := fsMkDir(volumeHandle, dotGitInode, "logs")
	fsCreateFile(volumeHandle, dotGitLogs, ".DS_Store")
	fsCreateFile(volumeHandle, dotGitLogs, "HEAD")
	dotGitLogsRefs := fsMkDir(volumeHandle, dotGitLogs, "refs")
	fsCreateFile(volumeHandle, dotGitLogsRefs, ".DS_Store")
	fsCreateFile(volumeHandle, dotGitLogsRefs, "stash")
	dotGitLogsRefsHeads := fsMkDir(volumeHandle, dotGitLogsRefs, "heads")
	fsCreateFile(volumeHandle, dotGitLogsRefsHeads, ".DS_Store")
	fsCreateFile(volumeHandle, dotGitLogsRefsHeads, "development")
	fsCreateFile(volumeHandle, dotGitLogsRefsHeads, "stable")

	aInode := fsMkDir(volumeHandle, cNestedInode, "a")
	fsCreateFile(volumeHandle, aInode, "b-1")
	fsCreateFile(volumeHandle, aInode, "b-2")
	abInode := fsMkDir(volumeHandle, aInode, "b")
	fsCreateFile(volumeHandle, abInode, "c-1")
	fsCreateFile(volumeHandle, abInode, "c-2")
	abcInode := fsMkDir(volumeHandle, abInode, "c")
	fsCreateFile(volumeHandle, abcInode, "d-1")
	fsCreateFile(volumeHandle, abcInode, "d-2")

	// SomeVolume2 is set up for testing account listings
	volumeHandle2, err := fs.FetchVolumeHandleByVolumeName("SomeVolume2")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "alpha")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "bravo")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "charlie")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "delta")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "echo")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "foxtrot")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "golf")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "hotel")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "india")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "juliet")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "kilo")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "lima")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "mancy")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "november")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "oscar")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "papa")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "quebec")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "romeo")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "sierra")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "tango")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "uniform")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "victor")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "whiskey")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "xray")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "yankee")
	_ = fsMkDir(volumeHandle2, inode.RootDirInodeNumber, "zulu")
	_ = fsCreateFile(volumeHandle2, inode.RootDirInodeNumber, "alice.txt")
	_ = fsCreateFile(volumeHandle2, inode.RootDirInodeNumber, "bob.txt")
	_ = fsCreateFile(volumeHandle2, inode.RootDirInodeNumber, "carol.txt")
}

func fsStatPath(accountName string, path string) fs.Stat {
	_, _, _, _, volumeHandle, err := parseVirtPath(accountName)
	if err != nil {
		panic(err)
	}
	ino, err := volumeHandle.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, path)
	if err != nil {
		panic(err)
	}
	stats, err := volumeHandle.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, ino)
	if err != nil {
		panic(err)
	}
	return stats
}

func fsMkDir(volumeHandle fs.VolumeHandle, parentDirInode inode.InodeNumber, newDirName string) (createdInode inode.InodeNumber) {
	createdInode, err := volumeHandle.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, parentDirInode, newDirName, inode.PosixModePerm)
	if err != nil {
		panic(fmt.Sprintf("failed to create %v: %v", newDirName, err))
	}
	return
}

func fsCreateFile(volumeHandle fs.VolumeHandle, parentDirInode inode.InodeNumber, newFileName string) (createdInode inode.InodeNumber) {
	createdInode, err := volumeHandle.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, parentDirInode, newFileName, inode.PosixModePerm)
	if err != nil {
		panic(fmt.Sprintf("failed to create file %v: %v", newFileName, err))
	}
	return
}

func fsCreateSymlink(volumeHandle fs.VolumeHandle, parentDirInode inode.InodeNumber, symlinkName string, symlinkTarget string) {
	_, err := volumeHandle.Symlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, parentDirInode, symlinkName, symlinkTarget)
	if err != nil {
		panic(fmt.Sprintf("failed to create symlink %s -> %s: %v", symlinkName, symlinkTarget, err))
	}
	return
}
