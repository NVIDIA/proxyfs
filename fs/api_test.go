package fs

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

// our global mountStruct to be used in tests
var mS *mountStruct

func testSetup() (err error) {
	testDir, err := ioutil.TempDir(os.TempDir(), "ProxyFS_test_fs_")
	if nil != err {
		return
	}

	err = os.Chdir(testDir)
	if nil != err {
		return
	}

	err = os.Mkdir("TestVolume", os.ModePerm)

	testConfMapStrings := []string{
		"Stats.IPAddr=localhost",
		"Stats.UDPPort=52184",
		"Stats.BufferLength=100",
		"Stats.MaxLatency=1s",
		"Logging.LogFilePath=proxyfsd.log",
		"SwiftClient.NoAuthTCPPort=45262",
		"SwiftClient.Timeout=10s",
		"SwiftClient.RetryLimit=5",
		"SwiftClient.RetryLimitObject=5",
		"SwiftClient.RetryDelay=1s",
		"SwiftClient.RetryDelayObject=1s",
		"SwiftClient.RetryExpBackoff=1.2",
		"SwiftClient.RetryExpBackoffObject=2.0",
		"SwiftClient.ChunkedConnectionPoolSize=64",
		"SwiftClient.NonChunkedConnectionPoolSize=32",
		"SwiftClient.StarvationCallbackFrequency=100ms",
		"FlowControl:TestFlowControl.MaxFlushSize=10000000",
		"FlowControl:TestFlowControl.MaxFlushTime=10s",
		"FlowControl:TestFlowControl.ReadCacheLineSize=1000000",
		"FlowControl:TestFlowControl.ReadCacheWeight=100",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainerStoragePolicy=silver",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainerNamePrefix=Replicated3Way_",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.ContainersPerPeer=1000",
		"PhysicalContainerLayout:PhysicalContainerLayoutReplicated3Way.MaxObjectsPerContainer=1000000",
		"Peer:Peer0.PrivateIPAddr=localhost",
		"Peer:Peer0.ReadCacheQuotaFraction=0.20",
		"Cluster.Peers=Peer0",
		"Cluster.WhoAmI=Peer0",
		"Volume:TestVolume.FSID=1",
		"Volume:TestVolume.PrimaryPeer=Peer0",
		"Volume:TestVolume.AccountName=CommonAccount",
		"Volume:TestVolume.CheckpointContainerName=.__checkpoint__",
		"Volume:TestVolume.CheckpointContainerStoragePolicy=gold",
		"Volume:TestVolume.CheckpointInterval=10s",
		"Volume:TestVolume.DefaultPhysicalContainerLayout=PhysicalContainerLayoutReplicated3Way",
		"Volume:TestVolume.FlowControl=TestFlowControl",
		"Volume:TestVolume.NonceValuesToReserve=100",
		"Volume:TestVolume.MaxEntriesPerDirNode=32",
		"Volume:TestVolume.MaxExtentsPerFileNode=32",
		"Volume:TestVolume.MaxInodesPerMetadataNode=32",
		"Volume:TestVolume.MaxLogSegmentsPerMetadataNode=64",
		"Volume:TestVolume.MaxDirFileNodesPerMetadataNode=16",
		"FSGlobals.VolumeList=TestVolume",
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
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
		"RamSwiftInfo.AccountListingLimit=10000",
		"RamSwiftInfo.ContainerListingLimit=10000",
	}

	testConfMap, err := conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		return
	}

	signalHandlerIsArmed := false
	doneChan := make(chan bool, 1)
	go ramswift.Daemon("/dev/null", testConfMapStrings, &signalHandlerIsArmed, doneChan, unix.SIGTERM)

	for !signalHandlerIsArmed {
		time.Sleep(100 * time.Millisecond)
	}

	err = logger.Up(testConfMap)
	if nil != err {
		return
	}

	err = evtlog.Up(testConfMap)
	if nil != err {
		logger.Down()
		return
	}

	err = stats.Up(testConfMap)
	if nil != err {
		evtlog.Down()
		logger.Down()
		return
	}

	err = dlm.Up(testConfMap)
	if nil != err {
		stats.Down()
		evtlog.Down()
		logger.Down()
		return
	}

	err = swiftclient.Up(testConfMap)
	if err != nil {
		dlm.Down()
		stats.Down()
		evtlog.Down()
		logger.Down()
		return err
	}

	err = headhunter.Format(testConfMap, "TestVolume")
	if nil != err {
		swiftclient.Down()
		dlm.Down()
		stats.Down()
		evtlog.Down()
		logger.Down()
		return
	}

	err = headhunter.Up(testConfMap)
	if nil != err {
		swiftclient.Down()
		dlm.Down()
		stats.Down()
		evtlog.Down()
		logger.Down()
		return
	}

	err = inode.Up(testConfMap)
	if nil != err {
		headhunter.Down()
		swiftclient.Down()
		dlm.Down()
		stats.Down()
		evtlog.Down()
		logger.Down()
		return
	}

	err = Up(testConfMap)
	if nil != err {
		inode.Down()
		headhunter.Down()
		swiftclient.Down()
		dlm.Down()
		stats.Down()
		evtlog.Down()
		logger.Down()
		return
	}

	err = nil
	return
}

func testTeardown() (err error) {
	Down()
	inode.Down()
	headhunter.Down()
	swiftclient.Down()
	dlm.Down()
	stats.Down()
	evtlog.Down()
	logger.Down()

	testDir, err := os.Getwd()
	if nil != err {
		return
	}

	err = os.Chdir("..")
	if nil != err {
		return
	}

	err = os.RemoveAll(testDir)
	if nil != err {
		return
	}

	err = nil
	return
}

// TODO: Enhance this to do a stat() as well and check number of files
func expectDirectory(t *testing.T, userID inode.InodeUserID, groupID inode.InodeGroupID, inodeNum inode.InodeNumber, expectedEntries []string) {
	readdirEntries, numEntries, moreEntries, err := mS.Readdir(userID, groupID, nil, inodeNum, "", 0, 0)
	if nil != err {
		t.Fatalf("Readdir() [#1] returned error: %v", err)
	}
	if uint64(len(expectedEntries)) != numEntries {
		t.Fatalf("Readdir() [#1] returned unexpected number of entries (%v) -  should have been %v", numEntries, len(expectedEntries))
	}
	if moreEntries {
		t.Fatalf("Readdir() [#1] returned moreEntries == true... should have been false")
	}

	entriesFound := make(map[string]bool)
	for i := uint64(0); i < numEntries; i++ {
		entriesFound[readdirEntries[i].Basename] = true
	}

	for i := 0; i < len(expectedEntries); i++ {
		expected := expectedEntries[i]
		_, found := entriesFound[expected]
		if !found {
			t.Errorf("Expected entry %s not found in readdirEntries", expected)
		}
	}
}

func createTestDirectory(t *testing.T, dirname string) (dirInode inode.InodeNumber) {
	var err error

	// Get root dir inode number
	rootDirInodeNumber := inode.RootDirInodeNumber

	dirInode, err = mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, dirname, inode.PosixModePerm)
	if nil != err {
		t.Fatalf("Mkdir() returned error: %v", err)
	}

	return dirInode
}

func TestMain(m *testing.M) {
	flag.Parse()

	err := testSetup()
	if nil != err {
		fmt.Fprintf(os.Stderr, "fs test setup failed: %v\n", err)
		os.Exit(1)
	}

	mountHandle, err := Mount("TestVolume", MountOptions(0))
	if err != nil {
		logger.ErrorfWithError(err, "Expected to successfully mount TestVolume")
		os.Exit(1)
	}
	mS = mountHandle.(*mountStruct)

	testResults := m.Run()

	err = testTeardown()
	if nil != err {
		fmt.Fprintf(os.Stderr, "fs test teardown failed: %v\n", err)
		os.Exit(1)
	}

	os.Exit(testResults)
}

// TODO:  Ultimately, each of these tests should at least run in their own directory
//        a la createTestDirectory(), or preferably some stronger effort should be
//        made to insulate them from each other.
func TestCreateAndLookup(t *testing.T) {
	rootDirInodeNumber := inode.RootDirInodeNumber
	basename := "create_lookup.test"

	createdFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Unexpectedly couldn't create file: %v", err)
	}

	foundFileInodeNumber, err := mS.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename)
	if err != nil {
		t.Fatalf("Unexpectedly failed to look up %v", basename)
	}

	if createdFileInodeNumber != foundFileInodeNumber {
		t.Fatalf("Expected created inode number %v to equal found inode number %v", createdFileInodeNumber, foundFileInodeNumber)
	}

	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename)
	if nil != err {
		t.Fatalf("Unlink() returned error: %v", err)
	}
}

func TestGetstat(t *testing.T) {
	rootDirInodeNumber := inode.RootDirInodeNumber
	basename := "getstat.test"
	timeBeforeCreation := uint64(time.Now().UnixNano())

	inodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("couldn't create file: %v", err)
	}

	stat, err := mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber)
	if err != nil {
		t.Fatalf("couldn't stat inode %v: %v", inodeNumber, err)
	}

	if !(math.Abs(float64(int64(timeBeforeCreation)-int64(stat[StatCRTime]))) < 0.1*1000000000) { // nanoseconds
		t.Errorf("unexpectedly skewed StatCRTime %v is not close to %v", stat[StatCRTime], timeBeforeCreation)
	}
	if !(math.Abs(float64(int64(timeBeforeCreation)-int64(stat[StatMTime]))) < 0.1*1000000000) { // nanoseconds
		t.Errorf("unexpectedly skewed StatMTime %v is not close to %v", stat[StatMTime], timeBeforeCreation)
	}
	if stat[StatSize] != 0 {
		t.Errorf("expected size to be 0")
	}
	if stat[StatNLink] != 1 {
		t.Errorf("expected number of links to be one, got %v", stat[StatNLink])
	}

	// TODO: perform a write, check that size has changed accordingly
	// TODO: make and delete hardlinks, check that link count has changed accordingly

	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename)
	if nil != err {
		t.Fatalf("Unlink() returned error: %v", err)
	}
}

// TestAllAPIPositiveCases() follows the following "positive" test steps:
//
//    Mount          A                                    : mount the specified test Volume (must be empty)
//    Mkdir          A/B/                                 : create a subdirectory within Volume directory
//    Create      #1 A/C                                  : create and open a normal file within Volume directory
//    Lookup      #1 A/C                                  : fetch the inode name of the just created normal file
//    Write          A/C                                  : write something to normal file
//    Read           A/C                                  : read back what was just written to normal file
//    Getstat     #1 A/C                                  : check the current size of the normal file
//    Resize         A/C                                  : truncate the file
//    Getstat     #2 A/C                                  : verify the size of the normal file is now zero
//    Symlink        A/D->A/C                             : create a symlink to the normal file
//    Lookup      #2 A/D                                  : fetch the inode name of the just created symlink
//    Readsymlink    A/D                                  : read the symlink to ensure it points to the normal file
//    Lookup      #3 A/B/                                 : fetch the inode name of the subdirectory
//    Create      #2 A/B/E                                : create a normal file within subdirectory
//    Readdir     #1 A/B/ (prev == "",  max_entries == 0) : ensure we get only ".", "..", and "E"
//    Statfs         A                                    : should report A has 4 "files" (normal & symlink) and 1 directory "ideally"
//    Unlink      #1 A/B/E                                : delete the normal file within the subdirectory
//    Readdir     #2 A/   (prev == "",  max_entries == 3) : ensure we get only ".", ".." & "B"
//    Readdir     #3 A/   (prev == "B", max_entries == 3) : ensure we get only "C" & "D"
//    Unlink      #2 A/D                                  : delete the symlink
//    Unlink      #3 A/C                                  : delete the normal file
//    Unlink      #4 A/B                                  : delete the subdirectory
//    Unmount        A                                    : unmount the Volume
//
// TODO: Rename(), Link() tests

var tempVolumeName string // TODO: This is currently the local file system full path

func TestAllAPIPositiveCases(t *testing.T) {
	var err error

	// Get root dir inode number
	rootDirInodeNumber := inode.RootDirInodeNumber

	//    Mkdir          A/B/                                 : create a subdirectory within Volume directory
	_, err = mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, "TestSubDirectory", inode.PosixModePerm)
	//	newDirInodeNum, err := mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, "TestSubDirectory")
	if nil != err {
		t.Fatalf("Mkdir() returned error: %v", err)
	}

	//    Create      #1 A/C                                  : create and open a normal file within Volume directory
	basename := "TestNormalFile"
	createdFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() [#1] returned error: %v", err)
	}

	//    Lookup      #1 A/C                                  : fetch the inode name of the just created normal file
	foundFileInodeNumber, err := mS.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename)
	if err != nil {
		t.Fatalf("Lookup() [#1] returned error: %v", err)
	}
	if createdFileInodeNumber != foundFileInodeNumber {
		t.Fatalf("Expected created inode number %v to equal found inode number %v", createdFileInodeNumber, foundFileInodeNumber)
	}

	//    Write          A/C                                  : write something to normal file
	bufToWrite := []byte{0x41, 0x42, 0x43}
	write_rspSize, err := mS.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil, createdFileInodeNumber, 0, bufToWrite, nil)
	if nil != err {
		t.Fatalf("Write() returned error: %v", err)
	}
	if uint64(len(bufToWrite)) != write_rspSize {
		t.Fatalf("Write() expected to write %v bytes but actually wrote %v bytes", len(bufToWrite), write_rspSize)
	}

	// don't forget to flush
	err = mS.Flush(inode.InodeRootUserID, inode.InodeGroupID(0), nil, createdFileInodeNumber)
	if err != nil {
		t.Fatalf("Flush() returned error: %v", err)
	}

	//    Read           A/C                                  : read back what was just written to normal file
	read_buf, err := mS.Read(inode.InodeRootUserID, inode.InodeGroupID(0), nil, createdFileInodeNumber, 0, uint64(len(bufToWrite)), nil)
	if nil != err {
		t.Fatalf("Read() returned error: %v", err)
	}
	if len(bufToWrite) != len(read_buf) {
		t.Fatalf("Read() expected to read %v bytes but actually read %v bytes", len(bufToWrite), len(read_buf))
	}
	if 0 != bytes.Compare(bufToWrite, read_buf) {
		t.Fatalf("Read() returned data different from what was written")
	}

	//    Getstat     #1 A/C                                  : check the current size of the normal file
	getstat_1_rspStat, err := mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, foundFileInodeNumber)
	if nil != err {
		t.Fatalf("Getstat() returned error: %v", err)
	}
	getstat_1_size, getstat_1_size_ok := getstat_1_rspStat[StatSize]
	if !getstat_1_size_ok {
		t.Fatalf("Getstat() returned no StatSize")
	}
	if uint64(len(bufToWrite)) != getstat_1_size {
		t.Fatalf("Getstat() returned StatSize == %v instead of the expected %v", getstat_1_size, len(bufToWrite))
	}

	//    Resize         A/C                                  : truncate the file
	err = mS.Resize(inode.InodeRootUserID, inode.InodeGroupID(0), nil, foundFileInodeNumber, 0)
	if nil != err {
		t.Fatalf("Resize() returned error: %v", err)
	}

	//    Getstat     #2 A/C                                  : verify the size of the normal file is now zero
	getstat_2_rspStat, err := mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, foundFileInodeNumber)
	if nil != err {
		t.Fatalf("Getstat() [#2] returned error: %v", err)
	}
	getstat_2_size, getstat_2_size_ok := getstat_2_rspStat[StatSize]
	if !getstat_2_size_ok {
		t.Fatalf("Getstat() [#2] returned no StatSize")
	}
	if 0 != getstat_2_size {
		t.Fatalf("Getstat() [#2] returned StatSize == %v instead of the expected %v", getstat_2_size, 0)
	}

	//    Symlink        A/D->A/C                             : create a symlink to the normal file
	createdSymlinkInodeNumber, err := mS.Symlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, "TestSymlink", "TestNormalFile")
	if nil != err {
		t.Fatalf("Symlink() returned error: %v", err)
	}

	//    Lookup      #2 A/D                                  : fetch the inode name of the just created symlink
	lookup_2_inodeHandle, err := mS.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, "TestSymlink")
	if nil != err {
		t.Fatalf("Lookup() [#2] returned error: %v", err)
	}
	if lookup_2_inodeHandle != createdSymlinkInodeNumber {
		t.Fatalf("Lookup() [#2] returned unexpected InodeNumber")
	}

	//    Readsymlink    A/D                                  : read the symlink to ensure it points to the normal file
	readsymlink_target, err := mS.Readsymlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lookup_2_inodeHandle)
	if nil != err {
		t.Fatalf("Readsymlink() returned error: %v", err)
	}
	if 0 != strings.Compare("TestNormalFile", readsymlink_target) {
		t.Fatalf("Readsymlink() data different from what was written")
	}

	//    Lookup      #3 A/B/                                 : fetch the inode name of the subdirectory
	lookup_3_inodeHandle, err := mS.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, "TestSubDirectory")
	if nil != err {
		t.Fatalf("Lookup() [#3] returned error: %v", err)
	}

	//    Create      #2 A/B/E                                : create a normal file within subdirectory
	testSubDirectoryFileInode, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lookup_3_inodeHandle, "TestSubDirectoryFile", inode.PosixModePerm)
	if nil != err {
		t.Fatalf("Create() [#2] returned error: %v", err)
	}

	//    Readdir and examine contents
	entriesExpected := []string{".", "..", "TestSubDirectoryFile"}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), lookup_3_inodeHandle, entriesExpected)

	//    Link A/B/E
	err = mS.Link(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lookup_3_inodeHandle, "TestSubDirectoryFileHardLink", testSubDirectoryFileInode)
	if nil != err {
		t.Fatalf("Link() returned error: %v", err)
	}

	entriesExpected = []string{".", "..", "TestSubDirectoryFile", "TestSubDirectoryFileHardLink"}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), lookup_3_inodeHandle, entriesExpected)

	//    Unlink      #1 A/B/E                                : delete the normal file within the subdirectory
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lookup_3_inodeHandle, "TestSubDirectoryFile")
	if nil != err {
		t.Fatalf("Unlink() [#1] returned error: %v", err)
	}

	entriesExpected = []string{".", "..", "TestSubDirectoryFileHardLink"}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), lookup_3_inodeHandle, entriesExpected)

	//    Unlink      #1.5 A/B/E                                : delete the normal file within the subdirectory
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lookup_3_inodeHandle, "TestSubDirectoryFileHardLink")
	if nil != err {
		t.Fatalf("Unlink() [#1.5] returned error: %v", err)
	}

	entriesExpected = []string{".", "..", "TestSymlink", "TestNormalFile", "TestSubDirectory"}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), rootDirInodeNumber, entriesExpected)

	//    Unlink      #2 A/D                                  : delete the symlink
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, "TestSymlink")

	if nil != err {
		t.Fatalf("Unlink() [#2] returned error: %v", err)
	}

	//    Unlink      #3 A/C                                  : delete the normal file
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, "TestNormalFile")
	if nil != err {
		t.Fatalf("Unlink() [#3] returned error: %v", err)
	}

	//    Rmdir       #4 A/B                                  : delete the subdirectory
	err = mS.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, "TestSubDirectory")
	if nil != err {
		t.Fatalf("Unlink() [#4] returned error: %v", err)
	}

	entriesExpected = []string{".", ".."}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), rootDirInodeNumber, entriesExpected)
}

// TODO: flesh this out with other boundary condition testing for Link
func TestBadLinks(t *testing.T) {
	testDirInode := createTestDirectory(t, "BadLinks")

	validFile := "PerfectlyValidFile"
	validFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testDirInode, validFile, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	nameTooLong := strings.Repeat("x", FileNameMax+1)
	err = mS.Link(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testDirInode, nameTooLong, validFileInodeNumber)
	if nil != err {
		if blunder.IsNot(err, blunder.NameTooLongError) {
			t.Fatalf("Link() returned error %v, expected %v(%d).", blunder.Errno(err), blunder.NameTooLongError, blunder.NameTooLongError.Value())
		}
	} else {
		t.Fatal("Link() unexpectedly succeeded on too-long filename!")
	}

	entriesExpected := []string{".", "..", validFile}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), testDirInode, entriesExpected)
}

func TestMkdir(t *testing.T) {
	testDirInode := createTestDirectory(t, "Mkdir")
	longButLegalFilename := strings.Repeat("x", FileNameMax)
	nameTooLong := strings.Repeat("x", FileNameMax+1)

	_, err := mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testDirInode, nameTooLong, inode.PosixModePerm)
	if nil != err {
		if blunder.IsNot(err, blunder.NameTooLongError) {
			t.Fatalf("Mkdir() returned error %v, expected %v(%d).", blunder.Errno(err), blunder.NameTooLongError, blunder.NameTooLongError.Value())
		}
	} else {
		t.Fatal("Mkdir() unexpectedly succeeded on too-long filename!")
	}

	_, err = mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testDirInode, longButLegalFilename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Mkdir() returned error: %v", err)
	}

	entriesExpected := []string{".", "..", longButLegalFilename}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), testDirInode, entriesExpected)

	longButLegalFullPath := "/Mkdir/" + longButLegalFilename
	ino, err := mS.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, longButLegalFullPath)
	if err != nil {
		t.Fatalf("LookupPath() returned error: %v", err)
	}

	_, err = mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(ino))
	if err != nil {
		t.Fatalf("GetStat() returned error: %v", err)
	}
}

// TODO: flesh this out with other boundary condition testing for Rename
func TestBadRename(t *testing.T) {
	testDirInode := createTestDirectory(t, "BadRename")
	nameTooLong := strings.Repeat("x", FileNameMax+1)

	validFile := "PerfectlyValidFile"
	_, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testDirInode, validFile, inode.PosixModePerm)
	if nil != err {
		t.Fatalf("Create() returned error: %v", err)
	}

	// Try to rename a valid file to a name that is too long
	err = mS.Rename(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testDirInode, validFile, testDirInode, nameTooLong)
	if nil != err {
		if blunder.IsNot(err, blunder.NameTooLongError) {
			t.Fatalf("Link() returned error %v, expected %v(%d).", blunder.Errno(err), blunder.NameTooLongError, blunder.NameTooLongError.Value())
		}
	} else {
		t.Fatal("Link() unexpectedly succeeded on too-long filename!")
	}

	entriesExpected := []string{".", "..", validFile}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), testDirInode, entriesExpected)

	// Try to rename a nonexistent file with a name that is too long
	err = mS.Rename(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testDirInode, nameTooLong, testDirInode, "AlsoAGoodFilename")
	if nil != err {
		if blunder.IsNot(err, blunder.NameTooLongError) {
			t.Fatalf("Link() returned error %v, expected %v(%d).", blunder.Errno(err), blunder.NameTooLongError, blunder.NameTooLongError.Value())
		}
	} else {
		t.Fatal("Link() unexpectedly succeeded on too-long filename!")
	}

	entriesExpected = []string{".", "..", validFile}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeGroupID(0), testDirInode, entriesExpected)
}

func TestBadChownChmod(t *testing.T) {
	var err error

	// Get root dir inode number
	rootDirInodeNumber := inode.RootDirInodeNumber

	// Create file to play with
	basename := "TestFile"
	createdFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() %v returned error: %v", basename, err)
	}

	// Since we are playing some games with size of mode/userid/groupid, make sure that we
	// correctly handle cases where the value is > uint32
	var tooBigForUint32 uint64 = math.MaxUint32 + 7<<48

	// Validate too-big Mode
	stat := make(Stat)
	stat[StatMode] = tooBigForUint32
	err = mS.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, createdFileInodeNumber, stat)
	if blunder.IsNot(err, blunder.InvalidFileModeError) {
		t.Fatalf("Setstat() %v returned error %v, expected %v(%d).", basename, blunder.Errno(err), blunder.InvalidFileModeError, blunder.InvalidFileModeError.Value())
	}
	delete(stat, StatMode)

	// Validate too-big UserID
	stat[StatUserID] = tooBigForUint32
	err = mS.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, createdFileInodeNumber, stat)
	if blunder.Errno(err) != int(blunder.InvalidFileModeError) {
		t.Fatalf("Setstat() %v returned error %v, expected %v(%d).", basename, blunder.Errno(err), blunder.InvalidFileModeError, blunder.InvalidFileModeError.Value())
	}
	delete(stat, StatUserID)

	// Validate too-big GroupID
	stat[StatGroupID] = tooBigForUint32
	err = mS.Setstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, createdFileInodeNumber, stat)
	if blunder.Errno(err) != int(blunder.InvalidFileModeError) {
		t.Fatalf("Setstat() %v returned error %v, expected %v(%d).", basename, blunder.Errno(err), blunder.InvalidFileModeError, blunder.InvalidFileModeError.Value())
	}
	delete(stat, StatGroupID)
}

func TestFlock(t *testing.T) {
	var err error

	rootDirInodeNumber := inode.RootDirInodeNumber

	// Create file to play with
	basename := "TestLockFile"
	lockFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() %v returned error: %v", basename, err)
	}

	// Resize the file to a 1M so that we can apply byte range locks:
	err = mS.Resize(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, 1024*1024)
	if err != nil {
		t.Fatalf("Resize() %v returned error: %v", basename, err)
	}

	// Write lock test:
	var lock FlockStruct
	lock.Type = syscall.F_WRLCK
	lock.Start = 0
	lock.Len = 0
	lock.Pid = 1

	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Write lock on file failed: %v", err)
	}

	lock.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Unlock on file failed: %v", blunder.Errno(err))
	}

	lock.Type = syscall.F_WRLCK
	lock.Pid = 1
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Write lock on file failed: %v", err)
	}

	// Try another write lock from a different pid, it should fail:
	var lock1 FlockStruct
	lock1 = lock
	lock1.Pid = 2
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock1)
	if blunder.Errno(err) != int(blunder.TryAgainError) {
		t.Fatalf("Write lock on a locked file should fail with EAGAIN instead got : %v", err)
	}

	// Lock again from pid1, it should succeed:
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Relocking from same PID on file failed: %v", err)
	}

	lock.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Unlock failed : %v", err)
	}

	// Read lock test:
	lock.Type = syscall.F_RDLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Read lock pid - 1 failed: %v", err)
	}

	lock1.Type = syscall.F_RDLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock1)
	if err != nil {
		t.Fatalf("Read lock pid - 2 failed: %v", err)
	}

	// Try write lock it should fail:
	lock3 := lock
	lock3.Type = syscall.F_WRLCK
	lock3.Pid = 3
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock3)
	if blunder.Errno(err) != int(blunder.TryAgainError) {
		t.Fatalf("Write lock should have failed with EAGAIN instead got - %v", err)
	}

	lock11 := lock1
	lock11.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock11)
	if err != nil {
		t.Fatalf("Unlock of (readlock) - 2 failed: %v", err)
	}

	lock01 := lock
	lock01.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock01)
	if err != nil {
		t.Fatalf("Unlock of (readlock) - 1 failed: %v", err)
	}

	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock3)
	if err != nil {
		t.Fatalf("Write lock should have succeeded instead got - %v", err.Error())
	}

	lock3.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock3)
	if err != nil {
		t.Fatalf("Unlock of (write after read) failed: %v", err)
	}

	// Multiple Range lock testing:

	var lock10 FlockStruct
	lock10.Pid = 1
	lock10.Start = 100
	lock10.Len = 100
	lock10.Type = syscall.F_WRLCK
	lock10.Whence = 0

	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock10)
	if err != nil {
		t.Fatalf("Range test failed to lock range (100 - 200), err %v", err)
	}

	lock201 := lock10
	lock201.Pid = 2
	lock201.Type = syscall.F_RDLCK
	lock201.Start = 10
	lock201.Len = 10
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock201)
	if err != nil {
		t.Fatalf("Range test failed to read lock range (10 - 20) by pid2, err %v", err)
	}

	lock202 := lock201
	lock202.Start = 90
	lock202.Len = 10
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock202)
	if err != nil {
		t.Fatalf("Range test failed to read lock range (90 - 100) by pid2, err %v", err)
	}

	lock203 := lock202
	lock203.Start = 80
	lock203.Len = 40
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock203)
	if err == nil {
		t.Fatalf("Range test read lock of range (80 - 120) should have failed for pid2  err %v", err)
	}

	lock204 := lock203
	lock204.Start = 180
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock204)
	if err == nil {
		t.Fatalf("Range test read lock of range (180 - 220) should have failed for pid2  err %v", err)
	}

	lock205 := lock204
	lock205.Start = 200
	lock205.Len = 10
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock205)
	if err != nil {
		t.Fatalf("Range test read lock of range (200 - 210) should have succeeded for pid2  err %v", err)
	}

	lock206 := lock205
	lock206.Start = 240
	lock206.Len = 10
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock206)
	if err != nil {
		t.Fatalf("Range test read lock of range (240 - 250) should have succeeded for pid2  err %v", err)
	}

	lock101 := lock10
	lock101.Type = syscall.F_RDLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock101)
	if err != nil {
		t.Fatalf("Range test converting write lock to read lock of pid1 range 100 - 200 failed, err %v", err)
	}

	// Now, lock 203 and 204 should succceed.
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock203)
	if err != nil {
		t.Fatalf("Range test read lock of range (80 - 120) should have succeeded for pid2  err %v", err)
	}

	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock204)
	if err != nil {
		t.Fatalf("Range test read lock of range (180 - 220) should have succeeded for pid2  err %v", err)
	}

	lock30 := lock10
	lock30.Pid = 3
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock30)
	if err == nil {
		t.Fatalf("Range test write lock of range 100 - 200 should have failed for pid3 err %v", err)
	}

	lock102 := lock10
	lock102.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock102)
	if err != nil {
		t.Fatalf("Range test unlock of range 100 - 200 for pid1 should have succeeded, err - %v", err)
	}

	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock30)
	if err == nil {
		t.Fatalf("Range test write lock of range 100 - 200 should have failed for pid3 err %v", err)
	}

	lock207 := lock10
	lock207.Type = syscall.F_UNLCK
	lock207.Pid = 2
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock207)
	if err != nil {
		t.Fatalf("Range test unlock of range 100 - 200 for pid2 should have succeeded, err - %v", err)
	}

	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock30)
	if err != nil {
		t.Fatalf("Range test write lock of range 100 - 200 should have succeeded for pid3 err %v", err)
	}

	lock301 := lock30
	lock301.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock301)
	if err != nil {
		t.Fatalf("Range test unlock of range 100 - 200 should have succeeded for pid3 err %v", err)
	}

	lock2u1 := lock201
	lock2u1.Type = syscall.F_UNLCK
	lock2u1.Start = 0
	lock2u1.Len = 150
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock2u1)
	if err != nil {
		t.Fatalf("Range test unlock of range 0 - 150 should have succeeded for pid2 err %v", err)
	}

	lock2u2 := lock2u1
	lock2u2.Start = 150
	lock2u2.Len = 150
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_SETLK, &lock2u2)
	if err != nil {
		t.Fatalf("Range test unlock of range 150 - 300 should have succeeded for pid2 err %v", err)
	}

	lock30.Start = 0
	lock30.Len = 250
	lock30.Type = syscall.F_WRLCK
	lockHeld, err := mS.Flock(inode.InodeRootUserID, inode.InodeGroupID(0), nil, lockFileInodeNumber, syscall.F_GETLK, &lock30)
	if err != nil {
		t.Fatalf("Range test GET write lock of range 0 - 250 should have succeeded for pid3 err %v lockHeld %+v", err, lockHeld)
	}

	if lock30.Type != syscall.F_UNLCK {
		t.Fatalf("GetLock should have succeeded for range 0 - 250 for pid 3, err %v", err)
	}

	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, rootDirInodeNumber, basename)
	if err != nil {
		t.Fatalf("Unlink() %v returned error: %v", basename, err)
	}
}

// Verify that the file system API works correctly with stale inode numbers,
// as can happen if an NFS client cache gets out of sync because another NFS
// client as removed a file or directory.
func TestStaleInodes(t *testing.T) {
	var (
		rootDirInodeNumber   inode.InodeNumber = inode.RootDirInodeNumber
		testDirname          string            = "stale_inodes_test"
		testFileName         string            = "valid_file"
		staleDirName         string            = "dir"
		staleFileName        string            = "file"
		testDirInodeNumber   inode.InodeNumber
		testFileInodeNumber  inode.InodeNumber
		staleDirInodeNumber  inode.InodeNumber
		staleFileInodeNumber inode.InodeNumber
		err                  error
	)

	// scratchpad directory for testing
	testDirInodeNumber, err = mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		rootDirInodeNumber, testDirname, 0755)
	if nil != err {
		t.Fatalf("Mkdir() '%s' returned error: %v", testDirname, err)
	}

	// create a valid test file
	testFileInodeNumber, err = mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, testFileName, 0644)
	if nil != err {
		t.Fatalf("Create() '%s' returned error: %v", testFileName, err)
	}

	// get an inode number that used to belong to a dirctory
	_, err = mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, staleDirName, 0755)
	if nil != err {
		t.Fatalf("Mkdir() '%s' returned error: %v", testDirname, err)
	}
	staleDirInodeNumber, err = mS.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, staleDirName)
	if err != nil {
		t.Fatalf("Unexpectedly failed to look up of '%s': %v", testDirname, err)
	}
	err = mS.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, staleDirName)
	if nil != err {
		t.Fatalf("Rmdir() of '%s' returned error: %v", staleDirName, err)
	}

	// get an inode number that used to belong to a file (it shouldn't
	// really matter which type of file the inode used to be, but it doesn't
	// hurt to have two to play with)
	_, err = mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, staleFileName, 0644)
	if nil != err {
		t.Fatalf("Mkdir() '%s' returned error: %v", testDirname, err)
	}
	staleFileInodeNumber, err = mS.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, staleFileName)
	if err != nil {
		t.Fatalf("Unexpectedly failed to look up of '%s': %v", testDirname, err)
	}
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, staleFileName)
	if nil != err {
		t.Fatalf("Unlink() of '%s' returned error: %v", staleFileName, err)
	}

	// Stat
	_, err = mS.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, staleFileInodeNumber)
	if nil == err {
		t.Fatalf("Getstat() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Getstat() should have failed with NotFoundError, instead got: %v", err)
	}

	// Mkdir
	_, err = mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "TestSubDirectory", 0755)
	if nil == err {
		t.Fatalf("Mkdir() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Mkdir() should have failed with NotFoundError, instead got: %v", err)
	}

	// Rmdir
	err = mS.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "fubar")
	if nil == err {
		t.Fatalf("Rmdir() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Rmdir() should have failed with NotFoundError, instead got: %v", err)
	}

	// Create
	_, err = mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "fubar", 0644)
	if nil == err {
		t.Fatalf("Create() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Create() should have failed with NotFoundError, instead got: %v", err)
	}

	// Lookup
	_, err = mS.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "fubar")
	if nil == err {
		t.Fatalf("Lookup() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Lookup() should have failed with NotFoundError, instead got: %v", err)
	}

	// Write
	bufToWrite := []byte{0x41, 0x42, 0x43}
	_, err = mS.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleFileInodeNumber, 0, bufToWrite, nil)
	if nil == err {
		t.Fatalf("Write() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Write() should have failed with NotFoundError, instead got: %v", err)
	}

	// Read
	_, err = mS.Read(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleFileInodeNumber, 0, uint64(len(bufToWrite)), nil)
	if nil == err {
		t.Fatalf("Read() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Read() should have failed with NotFoundError, instead got: %v", err)
	}

	// Trunc
	err = mS.Resize(inode.InodeRootUserID, inode.InodeGroupID(0), nil, staleFileInodeNumber, 77)
	if nil == err {
		t.Fatalf("Resize() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Resize() should have failed with NotFoundError, instead got: %v", err)
	}

	// Symlink
	_, err = mS.Symlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "TestSymlink", "fubar")
	if nil == err {
		t.Fatalf("Symlink() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Symlink() should have failed with NotFoundError, instead got: %v", err)
	}

	// Readsymlink (that we didn't create)
	_, err = mS.Readsymlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, staleFileInodeNumber)
	if nil == err {
		t.Fatalf("Readsymlink() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Readsymlink() should have failed with NotFoundError, instead got: %v", err)
	}

	// Readdir
	_, _, _, err = mS.Readdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "", 0, 0)
	if nil == err {
		t.Fatalf("Readdir() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Readdir() should have failed with NotFoundError, instead got: %v", err)
	}

	// Link -- two cases, one with stale directory and one with stale file
	err = mS.Link(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "fubar", testFileInodeNumber)
	if nil == err {
		t.Fatalf("Link(1) should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Link(1) should have failed with NotFoundError, instead got: %v", err)
	}

	err = mS.Link(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, testFileName, staleFileInodeNumber)
	if nil == err {
		t.Fatalf("Link(2) should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Link(2) should have failed with NotFoundError, instead got: %v", err)
	}

	// Unlink
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "fubar")
	if nil == err {
		t.Fatalf("Unlink() should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Unlink() should have failed with NotFoundError, instead got: %v", err)
	}

	// Rename -- two cases, one with stale src directory and one with stale dest
	err = mS.Rename(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, "fubar", staleDirInodeNumber, "barfu")
	if nil == err {
		t.Fatalf("Rename(1) should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Rename(1) should have failed with NotFoundError, instead got: %v", err)
	}

	err = mS.Rename(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		staleDirInodeNumber, "fubar", testDirInodeNumber, "barfu")
	if nil == err {
		t.Fatalf("Rename(2) should not have returned success")
	}
	if blunder.IsNot(err, blunder.NotFoundError) {
		t.Fatalf("Rename(2) should have failed with NotFoundError, instead got: %v", err)
	}

	// cleanup test file and directory
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		testDirInodeNumber, testFileName)
	if nil != err {
		t.Fatalf("Unlink() of '%s' returned error: %v", testFileName, err)
	}
	err = mS.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil,
		rootDirInodeNumber, testDirname)
	if nil != err {
		t.Fatalf("Rmdir() of '%s' returned error: %v", testDirname, err)
	}
}
