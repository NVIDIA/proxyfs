package jrpcfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/swiftstack/conf"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

// Shorthand for our testing debug log id; global to the package
var test_debug = logger.DbgTesting

const testVer = "/v1/"
const testAccountName = "AN_account"
const testContainerName = "test_container"
const testVerAccountName = testVer + testAccountName
const testVerAccountContainerName = testVerAccountName + "/" + testContainerName
const testAccountName2 = "AN_account2"

func testSetup() []func() {
	var err error

	cleanupFuncs := make([]func(), 0)

	confStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"FSGlobals.VolumeList=SomeVolume,SomeVolume2",
		"SwiftClient.NoAuthTCPPort=45262",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=10",
		"SwiftClient.RetryDelay=50ms",
		"SwiftClient.RetryExpBackoff=2.0",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=256",
		"Peer0.PrivateIPAddr=localhost",
		"Cluster.Peers=Peer0",
		"Cluster.WhoAmI=Peer0",
		"SomeVolume.FSID=1",
		"SomeVolume.PrimaryPeer=Peer0",
		"SomeVolume.AccountName=" + testAccountName,
		"SomeVolume.CheckpointContainerName=.__checkpoint__",
		"SomeVolume.CheckpointInterval=10s",
		"SomeVolume.CheckpointIntervalsPerCompaction=100",
		"SomeVolume.PhysicalContainerLayoutList=SomeContainerLayout",
		"SomeVolume.DefaultPhysicalContainerLayout=SomeContainerLayout",
		"SomeVolume.FlowControl=JrpcfsTestFlowControl",
		"SomeVolume.NonceValuesToReserve=100",
		"SomeVolume2.FSID=2",
		"SomeVolume2.PrimaryPeer=Peer0",
		"SomeVolume2.AccountName=" + testAccountName2,
		"SomeVolume2.CheckpointContainerName=.__checkpoint__",
		"SomeVolume2.CheckpointInterval=10s",
		"SomeVolume2.CheckpointIntervalsPerCompaction=100",
		"SomeVolume2.PhysicalContainerLayoutList=SomeContainerLayout2",
		"SomeVolume2.DefaultPhysicalContainerLayout=SomeContainerLayout2",
		"SomeVolume2.FlowControl=JrpcfsTestFlowControl",
		"SomeVolume2.NonceValuesToReserve=100",
		"JrpcfsTestFlowControl.MaxFlushSize=10027008",
		"JrpcfsTestFlowControl.MaxFlushTime=2s",
		"JrpcfsTestFlowControl.ReadCacheLineSize=1000000",
		"JrpcfsTestFlowControl.ReadCacheTotalSize=100000000",
		"SomeContainerLayout.ContainerStoragePolicyIndex=0",
		"SomeContainerLayout.ContainerNamePrefix=kittens",
		"SomeContainerLayout.ContainersPerPeer=1000",
		"SomeContainerLayout.MaxObjectsPerContainer=1000000",
		"SomeContainerLayout2.ContainerStoragePolicyIndex=0",
		"SomeContainerLayout2.ContainerNamePrefix=puppies",
		"SomeContainerLayout2.ContainersPerPeer=1234",
		"SomeContainerLayout2.MaxObjectsPerContainer=1234567",
	}

	tempDir, err := ioutil.TempDir("", "jrpcfs_test")
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}
	cleanupTempDir := func() {
		_ = os.RemoveAll(tempDir)
	}
	cleanupFuncs = append(cleanupFuncs, cleanupTempDir)

	testConfMap, err := conf.MakeConfMapFromStrings(confStrings)
	if nil != err {
		panic(fmt.Sprintf("failed in testSetup: %v", err))
	}

	doneChan := make(chan bool)
	go ramswift.Daemon(testConfMap, nil, doneChan)

	err = stats.Up(testConfMap)
	if nil != err {
		panic(fmt.Sprintf("failed to bring up stats: %v", err))
	}

	err = swiftclient.Up(testConfMap)
	if err != nil {
		panic(fmt.Sprintf("failed to bring up swiftclient: %v", err))
	}

	err = headhunter.Up(testConfMap)
	if nil != err {
		panic(fmt.Sprintf("failed to bring up headhunter: %v", err))
	}

	err = dlm.Up(testConfMap)
	if nil != err {
		panic(fmt.Sprintf("failed to bring up headhunter: %v", err))
	}

	err = fs.Up(testConfMap)
	if nil != err {
		panic(fmt.Sprintf("failed to bring up fs: %v", err))
	}

	err = inode.Up(testConfMap)
	if nil != err {
		panic(fmt.Sprintf("failed to bring up inode: %v", err))
	}

	// Unfortunately, we cannot call the jrpcfs Up() method here since it will start the RPC server.
	// Therefore, we have to do this step here.
	if globals.bimodalMountMap == nil {
		globals.bimodalMountMap = make(map[string]fs.MountHandle)
	}

	return cleanupFuncs
}

func fsStatPath(accountName string, path string) fs.Stat {
	_, _, _, _, mountHandle, err := mountIfNotMounted(accountName)
	if err != nil {
		panic(err)
	}
	ino, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, path)
	if err != nil {
		panic(err)
	}
	stats, err := mountHandle.Getstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, ino)
	if err != nil {
		panic(err)
	}
	return stats
}

func fsMkDir(mountHandle fs.MountHandle, parentDirInode inode.InodeNumber, newDirName string) (createdInode inode.InodeNumber) {
	createdInode, err := mountHandle.Mkdir(inode.InodeRootUserID, inode.InodeRootGroupID, nil, parentDirInode, newDirName, inode.PosixModePerm)
	if err != nil {
		panic(fmt.Sprintf("failed to create %v: %v", newDirName, err))
	}
	return
}

func fsCreateFile(mountHandle fs.MountHandle, parentDirInode inode.InodeNumber, newFileName string) (createdInode inode.InodeNumber) {
	createdInode, err := mountHandle.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, parentDirInode, newFileName, inode.PosixModePerm)
	if err != nil {
		panic(fmt.Sprintf("failed to create file %v: %v", newFileName, err))
	}
	return
}

func fsCreateSymlink(mountHandle fs.MountHandle, parentDirInode inode.InodeNumber, symlinkName string, symlinkTarget string) {
	_, err := mountHandle.Symlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, parentDirInode, symlinkName, symlinkTarget)
	if err != nil {
		panic(fmt.Sprintf("failed to create symlink %s -> %s: %v", symlinkName, symlinkTarget, err))
	}
	return
}

func middlewareCreateContainer(t *testing.T, server *Server, fullPathContainer string, expectedError blunder.FsError) {
	assert := assert.New(t)

	// Create a container for testing
	createRequest := CreateContainerRequest{
		VirtPath: fullPathContainer,
	}
	createResponse := CreateContainerReply{}
	err := server.RpcCreateContainer(&createRequest, &createResponse)
	assert.True(blunder.Is(err, expectedError))
}

func middlewareDeleteObject(server *Server, nameObject string) (err error) {
	deleteRequest := DeleteReq{
		VirtPath: testVerAccountContainerName + "/" + nameObject,
	}
	deleteResponse := DeleteReply{}
	err = server.RpcDelete(&deleteRequest, &deleteResponse)
	return err
}

func middlewarePost(server *Server, virtPath string, newMetaData []byte, oldMetaData []byte) (err error) {
	PostRequest := MiddlewarePostReq{
		VirtPath:    virtPath,
		NewMetaData: newMetaData,
		OldMetaData: oldMetaData,
	}
	PostResponse := MiddlewarePostReply{}
	err = server.RpcPost(&PostRequest, &PostResponse)
	return err
}

func middlewarePutLocation(t *testing.T, server *Server, newPutPath string, expectedError blunder.FsError) (physPath string) {
	assert := assert.New(t)

	putLocationReq := PutLocationReq{
		VirtPath: newPutPath,
	}
	putLocationReply := PutLocationReply{}
	err := server.RpcPutLocation(&putLocationReq, &putLocationReply)
	assert.True(blunder.Is(err, expectedError))

	return putLocationReply.PhysPath
}

func makeSomeFilesAndSuch() {
	// we should have enough stuff up now that we can actually make
	// some files and directories and such
	mountHandle, err := fs.Mount("SomeVolume", fs.MountOptions(0))
	if nil != err {
		panic(fmt.Sprintf("failed to mount SomeVolume: %v", err))
	}

	cInode := fsMkDir(mountHandle, inode.RootDirInodeNumber, "c")
	cNestedInode := fsMkDir(mountHandle, inode.RootDirInodeNumber, "c-nested")
	fsCreateSymlink(mountHandle, inode.RootDirInodeNumber, "c-symlink", "c")

	err = mountHandle.MiddlewarePost("", "c", []byte("metadata for c"), []byte{})
	if err != nil {
		panic(err)
	}
	_ = fsMkDir(mountHandle, inode.RootDirInodeNumber, "c-no-metadata")
	_ = fsMkDir(mountHandle, cInode, "empty-directory")

	readmeInode := fsCreateFile(mountHandle, cInode, "README")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, readmeInode, 0, []byte("who am I kidding? nobody reads these."), nil)

	animalsInode := fsMkDir(mountHandle, cInode, "animals")
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
		fileInode := fsCreateFile(mountHandle, animalsInode, fileName)

		_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInode, 0, []byte(fileContents), nil)
		if err != nil {
			panic(fmt.Sprintf("failed to write file %s: %v", fileName, err))
		}
	}

	plantsInode := fsMkDir(mountHandle, cInode, "plants")
	ino := fsCreateFile(mountHandle, cInode, "plants-README")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, ino, 0, []byte("nah"), nil)
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
		fileInode := fsCreateFile(mountHandle, plantsInode, fileName)

		_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInode, 0, []byte(fileContents), nil)
		if err != nil {
			panic(fmt.Sprintf("failed to write file %s: %v", fileName, err))
		}
	}

	fsCreateSymlink(mountHandle, cInode, "plants-symlink", "plants")
	fsCreateSymlink(mountHandle, plantsInode, "eggplant.txt-symlink", "eggplant.txt")

	// Put some deeply nested things in c-nested. This listing is a
	// shortened version of a real directory tree that exposed a bug.
	fsCreateFile(mountHandle, cNestedInode, ".DS_Store")
	dotGitInode := fsMkDir(mountHandle, cNestedInode, ".git")
	fsCreateFile(mountHandle, dotGitInode, ".DS_Store")
	fsCreateFile(mountHandle, dotGitInode, "COMMIT_EDITMSG")
	fsCreateFile(mountHandle, dotGitInode, "FETCH_HEAD")
	fsCreateFile(mountHandle, dotGitInode, "HEAD")
	fsCreateFile(mountHandle, dotGitInode, "ORIG_HEAD")
	fsCreateFile(mountHandle, dotGitInode, "index")
	dotGitHooks := fsMkDir(mountHandle, dotGitInode, "hooks")
	fsCreateFile(mountHandle, dotGitHooks, ".DS_Store")
	fsCreateFile(mountHandle, dotGitHooks, "applypatch-msg.sample")
	fsCreateFile(mountHandle, dotGitHooks, "commit-msg.sample")
	dotGitLogs := fsMkDir(mountHandle, dotGitInode, "logs")
	fsCreateFile(mountHandle, dotGitLogs, ".DS_Store")
	fsCreateFile(mountHandle, dotGitLogs, "HEAD")
	dotGitLogsRefs := fsMkDir(mountHandle, dotGitLogs, "refs")
	fsCreateFile(mountHandle, dotGitLogsRefs, ".DS_Store")
	fsCreateFile(mountHandle, dotGitLogsRefs, "stash")
	dotGitLogsRefsHeads := fsMkDir(mountHandle, dotGitLogsRefs, "heads")
	fsCreateFile(mountHandle, dotGitLogsRefsHeads, ".DS_Store")
	fsCreateFile(mountHandle, dotGitLogsRefsHeads, "development")
	fsCreateFile(mountHandle, dotGitLogsRefsHeads, "stable")

	// SomeVolume2 is set up for testing account listings
	mountHandle2, err := fs.Mount("SomeVolume2", fs.MountOptions(0))
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "alpha")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "bravo")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "charlie")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "delta")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "echo")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "foxtrot")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "golf")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "hotel")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "india")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "juliet")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "kilo")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "lima")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "mancy")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "november")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "oscar")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "papa")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "quebec")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "romeo")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "sierra")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "tango")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "uniform")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "victor")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "whiskey")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "xray")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "yankee")
	_ = fsMkDir(mountHandle2, inode.RootDirInodeNumber, "zulu")
	_ = fsCreateFile(mountHandle2, inode.RootDirInodeNumber, "alice.txt")
	_ = fsCreateFile(mountHandle2, inode.RootDirInodeNumber, "bob.txt")
	_ = fsCreateFile(mountHandle2, inode.RootDirInodeNumber, "carol.txt")
}

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

func TestRpcHead(t *testing.T) {
	s := &Server{}

	testRpcHeadExistingContainerWithMetadata(t, s)
	testRpcHeadExistingContainerWithoutMetadata(t, s)
	testRpcHeadAbsentContainer(t, s)
	testRpcHeadObjectSymlink(t, s)
	testRpcHeadObjectFile(t, s)
}

func testRpcHeadExistingContainerWithMetadata(t *testing.T, server *Server) {
	assert := assert.New(t)

	request := HeadReq{
		VirtPath: testVerAccountName + "/" + "c",
	}
	response := HeadReply{}
	err := server.RpcHead(&request, &response)

	assert.Nil(err)
	assert.Equal([]byte("metadata for c"), response.Metadata)
}

func testRpcHeadExistingContainerWithoutMetadata(t *testing.T, server *Server) {
	assert := assert.New(t)

	request := HeadReq{
		VirtPath: testVerAccountName + "/" + "c-no-metadata",
	}
	response := HeadReply{}
	err := server.RpcHead(&request, &response)

	assert.Nil(err)
	assert.Equal([]byte(""), response.Metadata)
}

func testRpcHeadAbsentContainer(t *testing.T, server *Server) {
	assert := assert.New(t)

	request := HeadReq{
		VirtPath: testVerAccountName + "/" + "sir-not-appearing-in-this-test",
	}
	response := HeadReply{}
	err := server.RpcHead(&request, &response)

	assert.NotNil(err)
}

func testRpcHeadObjectSymlink(t *testing.T, server *Server) {
	assert := assert.New(t)

	request := HeadReq{
		VirtPath: testVerAccountName + "/c/plants-symlink/eggplant.txt-symlink",
	}
	response := HeadReply{}
	err := server.RpcHead(&request, &response)

	assert.Nil(err)
	assert.Equal([]byte(""), response.Metadata)
	assert.Equal(uint64(12), response.FileSize)
	assert.Equal(false, response.IsDir)
}

func testRpcHeadObjectFile(t *testing.T, server *Server) {
	assert := assert.New(t)

	request := HeadReq{
		VirtPath: testVerAccountName + "/c/plants/eggplant.txt",
	}
	response := HeadReply{}
	err := server.RpcHead(&request, &response)

	assert.Nil(err)
	assert.Equal([]byte(""), response.Metadata)
	assert.Equal(uint64(12), response.FileSize)
	assert.Equal(false, response.IsDir)

	statResult := fsStatPath(testVerAccountName, "/c/plants/eggplant.txt")

	assert.Equal(statResult[fs.StatINum], response.InodeNumber)
	assert.Equal(statResult[fs.StatNumWrites], response.NumWrites)
}

func TestRpcGetContainerMetadata(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)

	// Just get one entry; this test really only cares about the
	// metadata
	request := GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c",
		Marker:     "",
		MaxEntries: 1,
	}
	response := GetContainerReply{}
	err := server.RpcGetContainer(&request, &response)
	assert.Nil(err)
	assert.Equal([]byte("metadata for c"), response.Metadata)
}

func TestRpcGetContainerNested(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)

	// Get a container listing with a limit of fewer than the total number
	request := GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c-nested",
		Marker:     "",
		MaxEntries: 10000,
	}
	response := GetContainerReply{}
	err := server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(22, len(response.ContainerEntries))
	ents := response.ContainerEntries
	assert.Equal(".DS_Store", ents[0].Basename)
	assert.Equal(".git", ents[1].Basename)
	assert.Equal(".git/.DS_Store", ents[2].Basename)
	assert.Equal(".git/COMMIT_EDITMSG", ents[3].Basename)
	assert.Equal(".git/FETCH_HEAD", ents[4].Basename)
	assert.Equal(".git/HEAD", ents[5].Basename)
	assert.Equal(".git/ORIG_HEAD", ents[6].Basename)
	assert.Equal(".git/hooks", ents[7].Basename)
	assert.Equal(".git/hooks/.DS_Store", ents[8].Basename)
	assert.Equal(".git/hooks/applypatch-msg.sample", ents[9].Basename)
	assert.Equal(".git/hooks/commit-msg.sample", ents[10].Basename)
	assert.Equal(".git/index", ents[11].Basename)
	assert.Equal(".git/logs", ents[12].Basename)
	assert.Equal(".git/logs/.DS_Store", ents[13].Basename)
	assert.Equal(".git/logs/HEAD", ents[14].Basename)
	assert.Equal(".git/logs/refs", ents[15].Basename)
	assert.Equal(".git/logs/refs/.DS_Store", ents[16].Basename)
	assert.Equal(".git/logs/refs/heads", ents[17].Basename)
	assert.Equal(".git/logs/refs/heads/.DS_Store", ents[18].Basename)
	assert.Equal(".git/logs/refs/heads/development", ents[19].Basename)
	assert.Equal(".git/logs/refs/heads/stable", ents[20].Basename)
	assert.Equal(".git/logs/refs/stash", ents[21].Basename)
}

func TestRpcGetContainerPrefix(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)

	request := GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c-nested",
		Marker:     "",
		MaxEntries: 10000,
		Prefix:     ".git/logs/refs/",
	}
	response := GetContainerReply{}
	err := server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(6, len(response.ContainerEntries))
	ents := response.ContainerEntries
	assert.Equal(".git/logs/refs/.DS_Store", ents[0].Basename)
	assert.Equal(".git/logs/refs/heads", ents[1].Basename)
	assert.Equal(".git/logs/refs/heads/.DS_Store", ents[2].Basename)
	assert.Equal(".git/logs/refs/heads/development", ents[3].Basename)
	assert.Equal(".git/logs/refs/heads/stable", ents[4].Basename)
	assert.Equal(".git/logs/refs/stash", ents[5].Basename)

	// Try with a prefix that starts mid-filename
	request = GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c-nested",
		Marker:     "",
		MaxEntries: 10000,
		Prefix:     ".git/logs/re",
	}
	response = GetContainerReply{}
	err = server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(7, len(response.ContainerEntries))
	ents = response.ContainerEntries
	assert.Equal(".git/logs/refs", ents[0].Basename)
	assert.Equal(".git/logs/refs/.DS_Store", ents[1].Basename)
	assert.Equal(".git/logs/refs/heads", ents[2].Basename)
	assert.Equal(".git/logs/refs/heads/.DS_Store", ents[3].Basename)
	assert.Equal(".git/logs/refs/heads/development", ents[4].Basename)
	assert.Equal(".git/logs/refs/heads/stable", ents[5].Basename)
	assert.Equal(".git/logs/refs/stash", ents[6].Basename)
}

func TestRpcGetContainerPrefixAndMarker(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)

	request := GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c-nested",
		Marker:     ".git/logs/refs/heads",
		MaxEntries: 10000,
		Prefix:     ".git/logs/refs/",
	}
	response := GetContainerReply{}
	err := server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(4, len(response.ContainerEntries))
	ents := response.ContainerEntries
	assert.Equal(".git/logs/refs/heads/.DS_Store", ents[0].Basename)
	assert.Equal(".git/logs/refs/heads/development", ents[1].Basename)
	assert.Equal(".git/logs/refs/heads/stable", ents[2].Basename)
	assert.Equal(".git/logs/refs/stash", ents[3].Basename)
}

func TestRpcGetContainerPaginated(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)

	// Get a container listing with a limit of fewer than the total number
	request := GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c",
		Marker:     "",
		MaxEntries: 5,
	}
	response := GetContainerReply{}
	err := server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(5, len(response.ContainerEntries))
	ents := response.ContainerEntries

	// These values are set in the test setup.
	assert.Equal("README", ents[0].Basename)
	assert.Equal(uint64(37), ents[0].FileSize)
	assert.Equal(false, ents[0].IsDir)

	assert.Equal("animals", ents[1].Basename)
	assert.Equal(uint64(0), ents[1].FileSize)
	assert.Equal(true, ents[1].IsDir)

	assert.Equal("animals/bird.txt", ents[2].Basename)
	assert.Equal(uint64(15), ents[2].FileSize)
	assert.Equal(false, ents[2].IsDir)

	assert.Equal("animals/cat.txt", ents[3].Basename)
	assert.Equal(uint64(13), ents[3].FileSize)
	assert.Equal(false, ents[3].IsDir)

	assert.Equal("animals/cow.txt", ents[4].Basename)
	assert.Equal(uint64(12), ents[4].FileSize)
	assert.Equal(false, ents[4].IsDir)

	// We'll spot-check one file and one directory
	statResult := fsStatPath(testVerAccountName, "c/animals/cat.txt")
	assert.Equal(statResult[fs.StatMTime], ents[3].ModificationTime)
	assert.Equal(statResult[fs.StatNumWrites], ents[3].NumWrites)
	assert.Equal(statResult[fs.StatINum], ents[3].InodeNumber)

	statResult = fsStatPath(testVerAccountName, "c/animals")
	assert.Equal(statResult[fs.StatMTime], ents[1].ModificationTime)
	assert.Equal(statResult[fs.StatNumWrites], ents[1].NumWrites)
	assert.Equal(statResult[fs.StatINum], ents[1].InodeNumber)

	// Next page of results:
	request = GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c",
		Marker:     "animals/cow.txt",
		MaxEntries: 5,
	}
	response = GetContainerReply{}
	err = server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(5, len(response.ContainerEntries))

	ents = response.ContainerEntries
	assert.Equal("animals/dog.txt", ents[0].Basename)
	assert.Equal("animals/elephant.txt", ents[1].Basename)
	assert.Equal("animals/frog.txt", ents[2].Basename)
	assert.Equal("animals/mouse.txt", ents[3].Basename)
	assert.Equal("empty-directory", ents[4].Basename)

	// Last page: it's shorter than 10 results, but that shouldn't
	// break anything.
	request = GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c",
		Marker:     "empty-directory",
		MaxEntries: 10,
	}
	response = GetContainerReply{}
	err = server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(8, len(response.ContainerEntries))

	ents = response.ContainerEntries
	assert.Equal("plants", ents[0].Basename)
	assert.Equal("plants-README", ents[1].Basename)
	assert.Equal("plants-symlink", ents[2].Basename)
	assert.Equal("plants/aloe.txt", ents[3].Basename)
	assert.Equal("plants/banana.txt", ents[4].Basename)
	assert.Equal("plants/cherry.txt", ents[5].Basename)
	assert.Equal("plants/eggplant.txt", ents[6].Basename)
	assert.Equal("plants/eggplant.txt-symlink", ents[7].Basename)

	// Some Swift clients keep asking for container listings until
	// they see an empty page, which will result in RpcGetContainer
	// being called with a marker equal to the last object. This
	// should simply return 0 results.
	request = GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c",
		Marker:     "plants/eggplant.txt-symlink",
		MaxEntries: 5,
	}
	response = GetContainerReply{}
	err = server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(0, len(response.ContainerEntries))

	// If a client sends a marker that comes _after_ every object,
	// that should also return zero results.
	request = GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c",
		Marker:     "zzzzzzzzzzzzzz",
		MaxEntries: 5,
	}
	response = GetContainerReply{}
	err = server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(0, len(response.ContainerEntries))
}

func TestRpcGetContainerZeroLimit(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)

	request := GetContainerReq{
		VirtPath:   "/v1/AN_account/c",
		Marker:     "",
		MaxEntries: 0,
	}
	response := GetContainerReply{}
	err := server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(len(response.ContainerEntries), 0)
}

func TestRpcGetContainerSymlink(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)

	request := GetContainerReq{
		VirtPath:   testVerAccountName + "/" + "c-symlink",
		Marker:     "",
		MaxEntries: 1,
	}
	response := GetContainerReply{}
	err := server.RpcGetContainer(&request, &response)

	assert.Nil(err)
	assert.Equal(1, len(response.ContainerEntries))
	ents := response.ContainerEntries

	assert.Equal("README", ents[0].Basename)
	assert.Equal(uint64(37), ents[0].FileSize)
	assert.Equal(false, ents[0].IsDir)
}

func TestRpcGetAccount(t *testing.T) {
	assert := assert.New(t)
	server := &Server{}

	request := GetAccountReq{
		VirtPath:   "/v1/" + testAccountName2,
		Marker:     "",
		MaxEntries: 5,
	}
	response := GetAccountReply{}
	err := server.RpcGetAccount(&request, &response)

	assert.Nil(err)
	assert.Equal(len(response.AccountEntries), 5)
	assert.Equal("alpha", response.AccountEntries[0].Basename)
	assert.Equal("bravo", response.AccountEntries[1].Basename)
	assert.Equal("charlie", response.AccountEntries[2].Basename)
	assert.Equal("delta", response.AccountEntries[3].Basename)
	assert.Equal("echo", response.AccountEntries[4].Basename)

	// Marker query starts listing in the middle
	request = GetAccountReq{
		VirtPath:   "/v1/" + testAccountName2,
		Marker:     "lima",
		MaxEntries: 3,
	}
	response = GetAccountReply{}
	err = server.RpcGetAccount(&request, &response)

	assert.Nil(err)
	assert.Equal(3, len(response.AccountEntries))
	assert.Equal("mancy", response.AccountEntries[0].Basename)
	assert.Equal("november", response.AccountEntries[1].Basename)
	assert.Equal("oscar", response.AccountEntries[2].Basename)

	// Asking past the end is not an error, just empty
	request = GetAccountReq{
		VirtPath:   "/v1/" + testAccountName2,
		Marker:     "zulu",
		MaxEntries: 3,
	}
	response = GetAccountReply{}
	err = server.RpcGetAccount(&request, &response)

	assert.Nil(err)
	assert.Equal(0, len(response.AccountEntries))
}

func TestRpcBasicApi(t *testing.T) {
	s := &Server{}

	testRpcDelete(t, s)
	testRpcPost(t, s)
	testNameLength(t, s)
}

func testRpcDelete(t *testing.T, server *Server) {
	assert := assert.New(t)

	middlewareCreateContainer(t, server, testVerAccountContainerName, blunder.SuccessError)

	// Create an object which is a directory and see if we can delete it via bimodal.
	_, _, _, _, mountHandle, err := mountIfNotMounted(testVerAccountName)
	assert.Nil(err)

	cInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, testContainerName)
	assert.Nil(err)

	var emptyDir string = "empty-directory"
	_ = fsMkDir(mountHandle, cInode, emptyDir)

	err = middlewareDeleteObject(server, emptyDir)
	assert.Nil(err)

	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, emptyDir)
	assert.NotNil(err)

	// Now create an object which is a file and see if we can delete it via bimodal.
	var emptyFile string = "empty-file"
	_ = fsCreateFile(mountHandle, cInode, emptyFile)

	err = middlewareDeleteObject(server, emptyFile)
	assert.Nil(err)

	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, emptyFile)
	assert.NotNil(err)

	// Now create a directory with one file in it and prove we can remove file and
	// then directory.
	var aDir string = "dir1"
	aDirInode := fsMkDir(mountHandle, cInode, aDir)
	_ = fsCreateFile(mountHandle, aDirInode, emptyFile)

	err = middlewareDeleteObject(server, aDir+"/"+emptyFile)
	assert.Nil(err)

	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, "/"+aDir+"/"+emptyFile)
	assert.NotNil(err)

	err = middlewareDeleteObject(server, aDir)
	assert.Nil(err)

	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, "/"+aDir)
	assert.NotNil(err)

	// Now delete the container
	deleteRequest := DeleteReq{
		VirtPath: testVerAccountContainerName,
	}
	deleteResponse := DeleteReply{}
	err = server.RpcDelete(&deleteRequest, &deleteResponse)
	assert.Nil(err)

	// Put it back to be nice to other test cases
	middlewareCreateContainer(t, server, testVerAccountContainerName, blunder.SuccessError)
}

func TestRpcDeleteSymlinks(t *testing.T) {
	s := &Server{}
	assert := assert.New(t)

	containerName := "unmaniac-imparticipable"

	// Test setup:
	// Within our container, we've got the following:
	//
	// top-level.txt
	// d1
	// d1/snap
	// d1/snap-symlink -> snap
	// d1/crackle
	// d1/pop
	// d1-symlink -> d1
	mountHandle, err := fs.Mount("SomeVolume", fs.MountOptions(0))
	if nil != err {
		panic(fmt.Sprintf("failed to mount SomeVolume: %v", err))
	}

	containerInode := fsMkDir(mountHandle, inode.RootDirInodeNumber, containerName)

	tlInode := fsCreateFile(mountHandle, containerInode, "top-level.txt")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, tlInode, 0, []byte("conusance-callboy"), nil)

	d1Inode := fsMkDir(mountHandle, containerInode, "d1")
	files := map[string]string{
		"snap":    "contents of snap",
		"crackle": "contents of crackle",
		"pop":     "contents of pop",
	}
	for fileName, fileContents := range files {
		fileInode := fsCreateFile(mountHandle, d1Inode, fileName)
		_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInode, 0, []byte(fileContents), nil)
		if err != nil {
			panic(fmt.Sprintf("failed to write file %s: %v", fileName, err))
		}
	}

	fsCreateSymlink(mountHandle, containerInode, "d1-symlink", "d1")
	fsCreateSymlink(mountHandle, d1Inode, "snap-symlink", "snap")
	fsCreateSymlink(mountHandle, d1Inode, "pop-symlink", "./pop")

	fsCreateSymlink(mountHandle, d1Inode, "dot-symlink", ".")

	// Symlinks in the directory portion of the name are followed
	deleteRequest := DeleteReq{
		VirtPath: testVerAccountName + "/" + containerName + "/d1-symlink/crackle",
	}
	deleteResponse := DeleteReply{}
	err = s.RpcDelete(&deleteRequest, &deleteResponse)
	assert.Nil(err)

	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/d1/crackle")
	assert.NotNil(err)
	assert.True(blunder.Is(err, blunder.NotFoundError))

	// Symlinks in the file portion of the name (i.e. the last
	// segment) are not followed
	deleteRequest = DeleteReq{
		VirtPath: testVerAccountName + "/" + containerName + "/d1-symlink/snap-symlink",
	}
	deleteResponse = DeleteReply{}
	err = s.RpcDelete(&deleteRequest, &deleteResponse)
	assert.Nil(err)

	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/d1/snap")
	assert.Nil(err)
	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/d1/snap-symlink")
	assert.NotNil(err)
	assert.True(blunder.Is(err, blunder.NotFoundError))

	// Symlinks ending with "." don't cause problems
	deleteRequest = DeleteReq{
		VirtPath: testVerAccountName + "/" + containerName + "/d1-symlink/dot-symlink/pop-symlink",
	}
	deleteResponse = DeleteReply{}
	err = s.RpcDelete(&deleteRequest, &deleteResponse)
	assert.Nil(err)

	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/d1/pop")
	assert.Nil(err)
	_, err = mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/d1/pop-symlink")
	assert.NotNil(err)
	assert.True(blunder.Is(err, blunder.NotFoundError))
}

func testRpcPost(t *testing.T, server *Server) {
	assert := assert.New(t)

	// We assume that the container already exists since currently we cannot
	// delete the container.

	_, _, _, _, mountHandle, err := mountIfNotMounted(testVerAccountName)
	assert.Nil(err)

	// POST to account with empty string for account
	var virtPath string = testVer
	newContMetaData := []byte("account metadata")
	oldContMetaData := []byte("")
	err = middlewarePost(server, virtPath, newContMetaData, oldContMetaData)
	assert.True(blunder.Is(err, blunder.AccountNotModifiable))

	// POST to account
	virtPath = testVerAccountName
	newContMetaData = []byte("account metadata")
	oldContMetaData = []byte("")
	err = middlewarePost(server, virtPath, newContMetaData, oldContMetaData)
	assert.True(blunder.Is(err, blunder.AccountNotModifiable))

	// POST to account/container
	virtPath = testVerAccountContainerName
	newContMetaData = []byte("container metadata")
	oldContMetaData = []byte("")
	err = middlewarePost(server, virtPath, newContMetaData, oldContMetaData)
	assert.Nil(err)

	// Try POST again with garbage metadata and make sure receive an error
	virtPath = testVerAccountContainerName
	newContMetaData = []byte("container metadata")
	oldContMetaData = []byte("incorrect metadata")
	err = middlewarePost(server, virtPath, newContMetaData, oldContMetaData)
	assert.True(blunder.Is(err, blunder.OldMetaDataDifferent))

	// Try POST one more time with valid version of old metadata and make sure no error.
	virtPath = testVerAccountContainerName
	newContMetaData = []byte("container metadata with more stuff")
	oldContMetaData = []byte("container metadata")
	err = middlewarePost(server, virtPath, newContMetaData, oldContMetaData)
	assert.Nil(err)

	// Now POST to account/container/object after creating an object which
	// is a directory and one which is a file.
	cInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, testContainerName)
	assert.Nil(err)
	var emptyDir string = "empty-directory"
	_ = fsMkDir(mountHandle, cInode, emptyDir)
	var emptyFile string = "empty-file"
	_ = fsCreateFile(mountHandle, cInode, emptyFile)
	var emptyFileSymlink string = "empty-file-symlink"
	fsCreateSymlink(mountHandle, cInode, emptyFileSymlink, emptyFile)

	virtPath = testVerAccountContainerName + "/" + emptyDir
	newContMetaData = []byte("object emptyDir metadata")
	oldContMetaData = []byte("")
	err = middlewarePost(server, virtPath, newContMetaData, oldContMetaData)
	assert.Nil(err)

	virtPath = testVerAccountContainerName + "/" + emptyFile
	newContMetaData = []byte("object emptyFile metadata")
	oldContMetaData = []byte("")
	err = middlewarePost(server, virtPath, newContMetaData, oldContMetaData)
	assert.Nil(err)

	// POST to a symlink follows it
	virtPath = testVerAccountContainerName + "/" + emptyFileSymlink
	oldContMetaData = newContMetaData
	newContMetaData = []byte("object emptyFile metadata take 2")
	err = middlewarePost(server, virtPath, newContMetaData, oldContMetaData)
	assert.Nil(err)

	headResponse, err := mountHandle.MiddlewareHeadResponse(testContainerName + "/" + emptyFile)
	assert.Nil(err)
	assert.Equal(newContMetaData, headResponse.Metadata)

	// Cleanup objects
	err = middlewareDeleteObject(server, emptyDir)
	assert.Nil(err)
	err = middlewareDeleteObject(server, emptyFile)
	assert.Nil(err)
}

func testNameLength(t *testing.T, server *Server) {

	// Try to create a container with a name which is one larger than fs.FilePathMax
	tooLongOfAString := make([]byte, (fs.FilePathMax + 1))
	for i := 0; i < (fs.FilePathMax + 1); i++ {
		tooLongOfAString[i] = 'A'
	}
	tooLongOfAPathName := testVerAccountName + "/" + string(tooLongOfAString)
	middlewareCreateContainer(t, server, tooLongOfAPathName, blunder.NameTooLongError)

	// Now try to create an objectName which is too long
	tooLongOfAFileName := make([]byte, (fs.FileNameMax + 1))
	for i := 0; i < (fs.FileNameMax + 1); i++ {
		tooLongOfAFileName[i] = 'A'
	}
	longFileName := testVerAccountContainerName + "/" + string(tooLongOfAFileName)

	_ = middlewarePutLocation(t, server, longFileName, blunder.NameTooLongError)
}

// Tests for RpcPutLocation and RpcPutComplete together; an object PUT
// calls both
func testPutObjectSetup(t *testing.T) (*assert.Assertions, *Server, string, fs.MountHandle) {
	// Just some common setup crud

	// We can't delete containers, so we grab a name and hope that it
	// doesn't already exist. (We're using ramswift for tests, so it's
	// almost certainly okay.)
	containerName := fmt.Sprintf("mware-TestPutObject-%d", time.Now().UnixNano())

	mountHandle, err := fs.Mount("SomeVolume", fs.MountOptions(0))
	if nil != err {
		panic(fmt.Sprintf("failed to mount SomeVolume: %v", err))
	}
	fsMkDir(mountHandle, inode.RootDirInodeNumber, containerName)

	assert := assert.New(t)

	server := &Server{}

	return assert, server, containerName, mountHandle
}

// Helper function to put a file into Swift using RpcPutLocation / RpcPutComplete plus an HTTP PUT request
func putFileInSwift(server *Server, virtPath string, objData []byte, objMetadata []byte) error {

	// Ask where to put it
	putLocationReq := PutLocationReq{
		VirtPath: virtPath,
	}
	putLocationResp := PutLocationReply{}

	err := server.RpcPutLocation(&putLocationReq, &putLocationResp)
	if err != nil {
		return err
	}

	// Put it there
	pathParts := strings.SplitN(putLocationResp.PhysPath, "/", 5)
	// pathParts[0] is empty, pathParts[1] is "v1"
	pAccount, pContainer, pObject := pathParts[2], pathParts[3], pathParts[4]

	putContext, err := swiftclient.ObjectFetchChunkedPutContext(pAccount, pContainer, pObject)
	if err != nil {
		return err
	}

	err = putContext.SendChunk(objData)
	if err != nil {
		return err
	}

	err = putContext.Close()
	if err != nil {
		return err
	}

	// Tell proxyfs about it
	putCompleteReq := PutCompleteReq{
		VirtPath:    virtPath,
		PhysPaths:   []string{putLocationResp.PhysPath},
		PhysLengths: []uint64{uint64(len(objData))},
		Metadata:    objMetadata,
	}
	putCompleteResp := PutCompleteReply{}

	err = server.RpcPutComplete(&putCompleteReq, &putCompleteResp)
	if err != nil {
		return err
	}
	return nil
}

func TestPutObjectSimple(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	objName := "toplevel.bin"
	objData := []byte("hello world\n")
	objMetadata := []byte("{\"metadata for\": \"" + objName + "\"}")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	err := putFileInSwift(server, objVirtPath, objData, objMetadata)
	assert.Nil(err) // sanity check

	// The file should exist now, so we can verify its attributes
	theInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/"+objName)
	assert.Nil(err)
	contents, err := mountHandle.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, theInode, 0, 99999, nil)
	assert.Nil(err)
	assert.Equal(objData, contents)

	headResponse, err := mountHandle.MiddlewareHeadResponse(containerName + "/" + objName)
	assert.Nil(err)
	assert.Equal([]byte(objMetadata), headResponse.Metadata)
}

func TestPutObjectInAllNewSubdirs(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	objName := "d1/d2/d3/d4/nested.bin"
	objData := []byte("hello nested world\n")
	objMetadata := []byte("nested metadata")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	err := putFileInSwift(server, objVirtPath, objData, objMetadata)
	assert.Nil(err) // sanity check

	// The file should exist now, so we can verify its attributes
	theInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/"+objName)
	assert.Nil(err)
	contents, err := mountHandle.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, theInode, 0, 99999, nil)
	assert.Nil(err)
	assert.Equal(objData, contents)
}

func TestPutObjectInSomeNewSubdirs(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	// make d1 and d1/d2, but leave creation of the rest to the RPC call
	containerInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName)
	if err != nil {
		panic(err)
	}
	d1Inode := fsMkDir(mountHandle, containerInode, "exists-d1")
	_ = fsMkDir(mountHandle, d1Inode, "exists-d2")

	objName := "exists-d1/exists-d2/d3/d4/nested.bin"
	objData := []byte("hello nested world\n")
	objMetadata := []byte("nested metadata")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	err = putFileInSwift(server, objVirtPath, objData, objMetadata)
	assert.Nil(err) // sanity check

	// The file should exist now, so we can verify its attributes
	theInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/"+objName)
	assert.Nil(err)
	contents, err := mountHandle.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, theInode, 0, 99999, nil)
	assert.Nil(err)
	assert.Equal(objData, contents)
}

func TestPutObjectOverwriteFile(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	objName := "overwritten.bin"
	objData1 := []byte("hello world 1\n")
	objData2 := []byte("hello world 2\n")
	objMetadata := []byte("{\"metadata for\": \"" + objName + "\"}")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	err := putFileInSwift(server, objVirtPath, objData1, objMetadata)
	assert.Nil(err) // sanity check
	err = putFileInSwift(server, objVirtPath, objData2, objMetadata)
	assert.Nil(err)

	theInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/"+objName)
	assert.Nil(err)
	contents, err := mountHandle.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, theInode, 0, 99999, nil)
	assert.Nil(err)
	assert.Equal(objData2, contents)
}

func TestPutObjectOverwriteEmptyDirectory(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	objName := "was-a-dir"
	objData := []byte("sialemesis-pseudembryo")
	objMetadata := []byte("I'm So Meta, Even This Acronym")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	containerInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName)
	if err != nil {
		panic(err)
	}
	_ = fsMkDir(mountHandle, containerInode, "was-a-dir")

	err = putFileInSwift(server, objVirtPath, objData, objMetadata)
	assert.Nil(err) // sanity check
}

func TestPutObjectOverwriteNonEmptyDirectory(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	objName := "dir-with-stuff-in-it"
	objData := []byte("irrelevant")
	objMetadata := []byte("won't get written")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	containerInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName)
	if err != nil {
		panic(err)
	}
	dirInodeNumber := fsMkDir(mountHandle, containerInode, "dir-with-stuff-in-it")

	fileInodeNumber := fsCreateFile(mountHandle, dirInodeNumber, "stuff.txt")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInodeNumber, 0, []byte("churches, lead, small rocks, apples"), nil)
	if err != nil {
		panic(err)
	}

	err = putFileInSwift(server, objVirtPath, objData, objMetadata)
	assert.NotNil(err)
	assert.Equal(fmt.Sprintf("errno: %d", blunder.IsDirError), err.Error())
}

func TestPutObjectSymlinkedDir(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	containerInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName)
	if err != nil {
		panic(err)
	}
	d1Inode := fsMkDir(mountHandle, containerInode, "d1")
	d2Inode := fsMkDir(mountHandle, d1Inode, "d2")
	fsCreateSymlink(mountHandle, d1Inode, "d2-symlink", "./d2")
	fsCreateSymlink(mountHandle, d1Inode, "dot-symlink", ".")
	fsCreateSymlink(mountHandle, d2Inode, "abs-container-symlink", "/"+containerName)

	objName := "d1/d2-symlink/abs-container-symlink/d1/dot-symlink/dot-symlink/d2/d3/thing.dat"
	objData := []byte("kamik-defensory")
	objMetadata := []byte("{}")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	err = putFileInSwift(server, objVirtPath, objData, objMetadata)
	assert.Nil(err) // sanity check

	// The file should exist now, so we can verify its attributes
	theInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/"+"d1/d2/d3/thing.dat")
	assert.Nil(err)
	contents, err := mountHandle.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, theInode, 0, 99999, nil)
	assert.Nil(err)
	assert.Equal(objData, contents)
}

func TestPutObjectOverwriteSymlink(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	containerInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName)
	if err != nil {
		panic(err)
	}
	fsCreateSymlink(mountHandle, containerInode, "thing.dat", "somewhere-else")

	objName := "thing.dat"
	objData := []byte("cottontop-aleuroscope")
	objMetadata := []byte("{}")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	err = putFileInSwift(server, objVirtPath, objData, objMetadata)
	assert.Nil(err) // sanity check

	// The file should exist now, so we can verify its attributes
	theInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/"+"thing.dat")
	assert.Nil(err)
	contents, err := mountHandle.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, theInode, 0, 99999, nil)
	assert.Nil(err)
	assert.Equal(objData, contents)
}

func TestPutObjectFileInDirPath(t *testing.T) {
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	objName := "d1/d2/actually-a-file/d3/d4/stuff.txt"
	objData := []byte("irrelevant")
	objMetadata := []byte("won't get written")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	containerInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName)
	if err != nil {
		panic(err)
	}
	d1InodeNumber := fsMkDir(mountHandle, containerInode, "d1")
	d2InodeNumber := fsMkDir(mountHandle, d1InodeNumber, "d2")

	fileInodeNumber := fsCreateFile(mountHandle, d2InodeNumber, "actually-a-file")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInodeNumber, 0, []byte("not a directory"), nil)
	if err != nil {
		panic(err)
	}

	err = putFileInSwift(server, objVirtPath, objData, objMetadata)
	assert.NotNil(err)
	assert.Equal(fmt.Sprintf("errno: %d", blunder.NotDirError), err.Error())
}

func TestPutObjectCompound(t *testing.T) {
	// In this test, we put data into two different log segments, but
	// the data is for the same file
	assert, server, containerName, mountHandle := testPutObjectSetup(t)

	objName := "helloworld.txt"
	objMetadata := []byte("{}")
	objVirtPath := testVerAccountName + "/" + containerName + "/" + objName

	///
	var physPaths []string
	var physLengths []uint64

	// Put the first half
	putLocationReq := PutLocationReq{
		VirtPath: objVirtPath,
	}
	putLocationResp := PutLocationReply{}

	err := server.RpcPutLocation(&putLocationReq, &putLocationResp)
	if err != nil {
		panic(err)
	}

	physPaths = append(physPaths, putLocationResp.PhysPath)
	physLengths = append(physLengths, uint64(6))
	pathParts := strings.SplitN(putLocationResp.PhysPath, "/", 5)
	// pathParts[0] is empty, pathParts[1] is "v1"
	pAccount, pContainer, pObject := pathParts[2], pathParts[3], pathParts[4]

	putContext, err := swiftclient.ObjectFetchChunkedPutContext(pAccount, pContainer, pObject)
	if err != nil {
		panic(err)
	}

	err = putContext.SendChunk([]byte("hello "))
	if err != nil {
		panic(err)
	}

	err = putContext.Close()
	if err != nil {
		panic(err)
	}

	// Put the second half
	putLocationReq = PutLocationReq{
		VirtPath: objVirtPath,
	}
	putLocationResp = PutLocationReply{}

	err = server.RpcPutLocation(&putLocationReq, &putLocationResp)
	if err != nil {
		panic(err)
	}

	physPaths = append(physPaths, putLocationResp.PhysPath)
	physLengths = append(physLengths, uint64(6))
	pathParts = strings.SplitN(putLocationResp.PhysPath, "/", 5)
	pAccount, pContainer, pObject = pathParts[2], pathParts[3], pathParts[4]

	putContext, err = swiftclient.ObjectFetchChunkedPutContext(pAccount, pContainer, pObject)
	if err != nil {
		panic(err)
	}

	err = putContext.SendChunk([]byte("world!"))
	if err != nil {
		panic(err)
	}

	err = putContext.Close()
	if err != nil {
		panic(err)
	}

	// Tell proxyfs about it
	putCompleteReq := PutCompleteReq{
		VirtPath:    objVirtPath,
		PhysPaths:   physPaths,
		PhysLengths: physLengths,
		Metadata:    objMetadata,
	}
	putCompleteResp := PutCompleteReply{}

	err = server.RpcPutComplete(&putCompleteReq, &putCompleteResp)
	assert.Nil(err)
	if err != nil {
		panic(err)
	}

	// The file should exist now, so we can verify its attributes
	theInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerName+"/"+objName)
	assert.Nil(err)
	contents, err := mountHandle.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, theInode, 0, 99999, nil)
	assert.Nil(err)
	assert.Equal([]byte("hello world!"), contents)
	assert.Equal(uint64(theInode), putCompleteResp.InodeNumber)
	// 2 is the number of log segments we wrote
	assert.Equal(uint64(2), putCompleteResp.NumWrites)

	headResponse, err := mountHandle.MiddlewareHeadResponse(containerName + "/" + objName)
	assert.Nil(err)
	assert.Equal([]byte(objMetadata), headResponse.Metadata)

	statResult, err := mountHandle.Getstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, theInode)
	assert.Nil(err)
	assert.Equal(statResult[fs.StatMTime], putCompleteResp.ModificationTime)
}

func TestIsAccountBimodal(t *testing.T) {
	assert := assert.New(t)
	server := Server{}

	request := IsAccountBimodalReq{
		AccountName: testAccountName,
	}
	response := IsAccountBimodalReply{}

	err := server.RpcIsAccountBimodal(&request, &response)
	assert.Nil(err)
	assert.True(response.IsBimodal)

	request = IsAccountBimodalReq{
		AccountName: testAccountName + "-adenoacanthoma-preperceptive",
	}
	response = IsAccountBimodalReply{}

	err = server.RpcIsAccountBimodal(&request, &response)
	assert.Nil(err)
	assert.False(response.IsBimodal)
}

func TestRpcGetObjectMetadata(t *testing.T) {
	// This tests the other, non-read-plan things returned by RpcGetObject.
	// We're not actually going to test any read plans here; that is tested elsewhere.
	server := &Server{}
	assert := assert.New(t)
	mountHandle, err := fs.Mount("SomeVolume", fs.MountOptions(0))
	if nil != err {
		panic(fmt.Sprintf("failed to mount SomeVolume: %v", err))
	}

	containerName := "covetingly-ahead"

	cInode := fsMkDir(mountHandle, inode.RootDirInodeNumber, containerName)
	readmeInode := fsCreateFile(mountHandle, cInode, "README")

	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, readmeInode, 0, []byte("unsurpassably-Rigelian"), nil)
	if err != nil {
		panic(err)
	}

	statResult, err := mountHandle.Getstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, readmeInode)
	if err != nil {
		panic(err)
	}

	req := GetObjectReq{VirtPath: "/v1/AN_account/" + containerName + "/README"}
	reply := GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.Nil(err)
	assert.Equal(uint64(22), reply.FileSize)
	assert.Equal(statResult[fs.StatMTime], reply.ModificationTime)
}

func TestRpcGetObjectSymlinkFollowing(t *testing.T) {
	// This tests the symlink-following abilities of RpcGetObject.
	// We're not actually going to test any read plans here; that is tested elsewhere.
	server := &Server{}
	assert := assert.New(t)
	mountHandle, err := fs.Mount("SomeVolume", fs.MountOptions(0))
	if nil != err {
		panic(fmt.Sprintf("failed to mount SomeVolume: %v", err))
	}

	// Our filesystem:
	//
	//  /
	//  /c1
	//  /c1/kitten.png
	//  /c1/symlink-1 -> kitten.png
	//  /c1/symlink-2 -> symlink-1
	//  /c1/symlink-3 -> symlink-2
	//  /c1/symlink-4 -> symlink-3
	//  /c1/symlink-5 -> symlink-4
	//  /c1/symlink-6 -> symlink-5
	//  /c1/symlink-7 -> symlink-6
	//  /c1/symlink-8 -> symlink-7
	//  /c1/symlink-9 -> symlink-8
	//  /c2/10-bytes
	//  /c2/symlink-10-bytes -> 10-bytes
	//  /c2/symlink-20-bytes -> /c3/20-bytes
	//  /c2/symlink-20-bytes-indirect -> symlink-20-bytes
	//  /c3/20-bytes
	//  /c3/symlink-20-bytes-double-indirect -> /c2/symlink-20-bytes-indirect
	//  /c3/cycle-a -> cycle-b
	//  /c3/cycle-b -> cycle-c
	//  /c3/cycle-c -> cycle-a
	//  /c3/symlink-c2 -> /c2
	//  /c4
	//  /c4/d1
	//  /c4/symlink-d1 -> d1
	//  /c4/d1/d2
	//  /c4/d1/symlink-d2 -> d2
	//  /c4/d1/d2/symlink-kitten.png -> /c1/kitten.png

	c1Inode := fsMkDir(mountHandle, inode.RootDirInodeNumber, "c1")
	c2Inode := fsMkDir(mountHandle, inode.RootDirInodeNumber, "c2")
	c3Inode := fsMkDir(mountHandle, inode.RootDirInodeNumber, "c3")
	c4Inode := fsMkDir(mountHandle, inode.RootDirInodeNumber, "c4")

	fileInode := fsCreateFile(mountHandle, c1Inode, "kitten.png")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInode, 0, []byte("if this were a real kitten, it would be cute"), nil)
	if err != nil {
		panic(err)
	}

	fsCreateSymlink(mountHandle, c1Inode, "symlink-1", "kitten.png")
	fsCreateSymlink(mountHandle, c1Inode, "symlink-2", "symlink-1")
	fsCreateSymlink(mountHandle, c1Inode, "symlink-3", "symlink-2")
	fsCreateSymlink(mountHandle, c1Inode, "symlink-4", "symlink-3")
	fsCreateSymlink(mountHandle, c1Inode, "symlink-5", "symlink-4")
	fsCreateSymlink(mountHandle, c1Inode, "symlink-6", "symlink-5")
	fsCreateSymlink(mountHandle, c1Inode, "symlink-7", "symlink-6")
	fsCreateSymlink(mountHandle, c1Inode, "symlink-8", "symlink-7")
	fsCreateSymlink(mountHandle, c1Inode, "symlink-9", "symlink-8")

	fileInode = fsCreateFile(mountHandle, c2Inode, "10-bytes")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInode, 0, []byte("abcdefghij"), nil)
	if err != nil {
		panic(err)
	}

	fsCreateSymlink(mountHandle, c2Inode, "symlink-10-bytes", "10-bytes")
	fsCreateSymlink(mountHandle, c2Inode, "symlink-20-bytes", "/c3/20-bytes")
	fsCreateSymlink(mountHandle, c2Inode, "symlink-20-bytes-indirect", "symlink-20-bytes")

	fileInode = fsCreateFile(mountHandle, c3Inode, "20-bytes")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInode, 0, []byte("abcdefghijklmnopqrst"), nil)
	if err != nil {
		panic(err)
	}

	fsCreateSymlink(mountHandle, c3Inode, "symlink-20-bytes-double-indirect", "/c2/symlink-20-bytes-indirect")
	fsCreateSymlink(mountHandle, c3Inode, "symlink-c2", "/c2")
	fsCreateSymlink(mountHandle, c3Inode, "cycle-a", "cycle-b")
	fsCreateSymlink(mountHandle, c3Inode, "cycle-b", "cycle-c")
	fsCreateSymlink(mountHandle, c3Inode, "cycle-c", "cycle-a")

	c4d1Inode := fsMkDir(mountHandle, c4Inode, "d1")
	c4d1d2Inode := fsMkDir(mountHandle, c4d1Inode, "d2")
	fsCreateSymlink(mountHandle, c4Inode, "symlink-d1", "d1")
	fsCreateSymlink(mountHandle, c4d1Inode, "symlink-d2", "d2")
	fsCreateSymlink(mountHandle, c4d1d2Inode, "symlink-kitten.png", "/c1/kitten.png")
	// Test setup complete

	// Test following a single symlink to a file in the same directory
	req := GetObjectReq{VirtPath: "/v1/AN_account/c1/symlink-1"}
	reply := GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.Nil(err)
	assert.Equal(uint64(44), reply.FileSize) // size of kitten.png

	// Test following a symlink with an absolute path in it (we treat
	// "/" as the root of this filesystem, which is probably not
	// helpful for symlinks created on a filesystem mounted somewhere
	// like /mnt/smb-vol, but it's the best we've got)
	req = GetObjectReq{VirtPath: "/v1/AN_account/c2/symlink-20-bytes"}
	reply = GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.Nil(err)
	assert.Equal(uint64(20), reply.FileSize)

	// Test a chain with relative and absolute paths in it
	req = GetObjectReq{VirtPath: "/v1/AN_account/c3/symlink-20-bytes-double-indirect"}
	reply = GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.Nil(err)
	assert.Equal(uint64(20), reply.FileSize)

	// Test following a pair of symlinks to a file in the same directory
	req = GetObjectReq{VirtPath: "/v1/AN_account/c1/symlink-2"}
	reply = GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.Nil(err)
	assert.Equal(uint64(44), reply.FileSize) // size of kitten.png

	// Test following a max-length (8) chain of symlinks to a file
	req = GetObjectReq{VirtPath: "/v1/AN_account/c1/symlink-8"}
	reply = GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.Nil(err)
	assert.Equal(uint64(44), reply.FileSize) // size of kitten.png

	// Test following a too-long (9) chain of symlinks to a file
	req = GetObjectReq{VirtPath: "/v1/AN_account/c1/symlink-9"}
	reply = GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.NotNil(err)
	assert.Equal(fmt.Sprintf("errno: %d", blunder.TooManySymlinksError.Value()), err.Error())

	// Test following a cycle: it should look just like an over-length chain
	req = GetObjectReq{VirtPath: "/v1/AN_account/c3/cycle-a"}
	reply = GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.NotNil(err)
	assert.Equal(fmt.Sprintf("errno: %d", blunder.TooManySymlinksError.Value()), err.Error())

	// Test following a symlink to a directory
	req = GetObjectReq{VirtPath: "/v1/AN_account/c3/symlink-c2"}
	reply = GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.NotNil(err)
	assert.Equal(fmt.Sprintf("errno: %d", blunder.IsDirError.Value()), err.Error())

	// Test following a path where some directory components are symlinks
	req = GetObjectReq{VirtPath: "/v1/AN_account/c4/symlink-d1/symlink-d2/symlink-kitten.png"}
	reply = GetObjectReply{}
	err = server.RpcGetObject(&req, &reply)
	assert.Nil(err)
	assert.Equal(uint64(44), reply.FileSize) // size of kitten.png
}

func TestRpcPutContainer(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)
	_, err := fs.Mount("SomeVolume", fs.MountOptions(0))
	if nil != err {
		panic(fmt.Sprintf("failed to mount SomeVolume: %v", err))
	}

	containerName := "rpc-put-container-Chartreux-sulphurea"
	containerPath := testVerAccountName + "/" + containerName
	containerMetadata := []byte("some metadata")
	newMetadata := []byte("some new metadata")
	req := PutContainerReq{
		VirtPath:    containerPath,
		OldMetadata: []byte{},
		NewMetadata: containerMetadata,
	}
	reply := PutContainerReply{}

	err = server.RpcPutContainer(&req, &reply)
	assert.Nil(err)

	// Check metadata
	headRequest := HeadReq{
		VirtPath: containerPath,
	}
	headReply := HeadReply{}
	err = server.RpcHead(&headRequest, &headReply)
	assert.Nil(err)
	assert.Equal(containerMetadata, headReply.Metadata)

	// Can't update the metadata unless you know what's there
	req = PutContainerReq{
		VirtPath:    containerPath,
		OldMetadata: []byte{}, // doesn't match what's there
		NewMetadata: newMetadata,
	}
	reply = PutContainerReply{}
	err = server.RpcPutContainer(&req, &reply)
	assert.NotNil(err)
	assert.Equal(fmt.Sprintf("errno: %d", blunder.TryAgainError), err.Error())

	// Now update the metadata
	req = PutContainerReq{
		VirtPath:    containerPath,
		OldMetadata: containerMetadata,
		NewMetadata: newMetadata,
	}
	reply = PutContainerReply{}

	err = server.RpcPutContainer(&req, &reply)
	assert.Nil(err)

	headRequest = HeadReq{
		VirtPath: containerPath,
	}
	headReply = HeadReply{}
	err = server.RpcHead(&headRequest, &headReply)
	assert.Nil(err)
	assert.Equal(newMetadata, headReply.Metadata)
}

func TestRpcCoalesce(t *testing.T) {
	server := &Server{}
	assert := assert.New(t)
	mountHandle, err := fs.Mount("SomeVolume", fs.MountOptions(0))
	if nil != err {
		panic(fmt.Sprintf("failed to mount SomeVolume: %v", err))
	}

	containerAName := "rpc-coalesce-A-catagmatic-invincibly"
	containerAPath := testVerAccountName + "/" + containerAName
	containerBName := "rpc-coalesce-B-galeproof-palladium"

	destinationPath := containerAPath + "/" + "combined-file"

	containerAInode := fsMkDir(mountHandle, inode.RootDirInodeNumber, containerAName)
	containerBInode := fsMkDir(mountHandle, inode.RootDirInodeNumber, containerBName)

	containerADir1Inode := fsMkDir(mountHandle, containerAInode, "dir1")
	containerADir1Dir2Inode := fsMkDir(mountHandle, containerADir1Inode, "dir2")

	fileA1Path := "/" + containerAName + "/dir1/dir2/a1"
	fileA1Inode := fsCreateFile(mountHandle, containerADir1Dir2Inode, "a1")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileA1Inode, 0, []byte("red "), nil)
	if err != nil {
		panic(err)
	}

	// Element paths are relative to the account, but the destination path is absolute. It's a little weird, but it
	// means we don't have to worry about element paths pointing to different accounts.
	fileA2Path := "/" + containerAName + "/dir1/dir2/a2"
	fileA2Inode := fsCreateFile(mountHandle, containerADir1Dir2Inode, "a2")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileA2Inode, 0, []byte("orange "), nil)
	if err != nil {
		panic(err)
	}

	fileBPath := "/" + containerBName + "/b"
	fileBInode := fsCreateFile(mountHandle, containerBInode, "b")
	_, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileBInode, 0, []byte("yellow"), nil)
	if err != nil {
		panic(err)
	}

	timeBeforeRequest := uint64(time.Now().UnixNano())

	coalesceRequest := CoalesceReq{
		VirtPath: destinationPath,
		ElementAccountRelativePaths: []string{
			fileA1Path,
			fileA2Path,
			fileBPath,
		},
	}
	coalesceReply := CoalesceReply{}
	err = server.RpcCoalesce(&coalesceRequest, &coalesceReply)
	assert.Nil(err)

	combinedInode, err := mountHandle.LookupPath(inode.InodeRootUserID, inode.InodeRootGroupID, nil, containerAName+"/combined-file")
	assert.Nil(err)
	assert.Equal(uint64(combinedInode), coalesceReply.InodeNumber)
	assert.True(coalesceReply.NumWrites > 0)
	assert.True(coalesceReply.ModificationTime > 0)
	assert.True(coalesceReply.ModificationTime > timeBeforeRequest)

	combinedContents, err := mountHandle.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, combinedInode, 0, 99999, nil)
	assert.Nil(err)
	assert.Equal([]byte("red orange yellow"), combinedContents)
}
