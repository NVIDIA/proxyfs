package fs

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"syscall"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/platform"
	"github.com/swiftstack/ProxyFS/ramswift"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/conf"
)

// our global mountStruct to be used in tests
var mS *mountStruct

func testSetup() (err error) {
	testDir, err := ioutil.TempDir(os.TempDir(), "ProxyFS_test_fs_")
	if nil != err {
		return
	}

	if platform.IsLinux {
		cmd := exec.Command("sudo", "mount", "-t", "ramfs", "-o", "size=512m", "ext4", testDir)
		err = cmd.Run()
		if nil != err {
			return
		}

		cmd = exec.Command("sudo", "chmod", "777", testDir)
		err = cmd.Run()
		if nil != err {
			return
		}
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
		"TestFlowControl.MaxFlushSize=10000000",
		"TestFlowControl.MaxFlushTime=10s",
		"TestFlowControl.ReadCacheLineSize=1000000",
		"TestFlowControl.ReadCacheWeight=100",
		"PhysicalContainerLayoutReplicated3Way.ContainerStoragePolicyIndex=0",
		"PhysicalContainerLayoutReplicated3Way.ContainerNamePrefix=Replicated3Way_",
		"PhysicalContainerLayoutReplicated3Way.ContainersPerPeer=1000",
		"PhysicalContainerLayoutReplicated3Way.MaxObjectsPerContainer=1000000",
		"Peer0.PrivateIPAddr=localhost",
		"Peer0.ReadCacheQuotaFraction=0.20",
		"Cluster.Peers=Peer0",
		"Cluster.WhoAmI=Peer0",
		"TestVolume.FSID=1",
		"TestVolume.PrimaryPeer=Peer0",
		"TestVolume.AccountName=CommonAccount",
		"TestVolume.CheckpointContainerName=.__checkpoint__",
		"TestVolume.CheckpointInterval=10s",
		"TestVolume.CheckpointIntervalsPerCompaction=100",
		"TestVolume.DefaultPhysicalContainerLayout=PhysicalContainerLayoutReplicated3Way",
		"TestVolume.FlowControl=TestFlowControl",
		"TestVolume.NonceValuesToReserve=100",
		"FSGlobals.VolumeList=TestVolume",
		"RamSwiftInfo.MaxAccountNameLength=256",
		"RamSwiftInfo.MaxContainerNameLength=256",
		"RamSwiftInfo.MaxObjectNameLength=1024",
	}

	testConfMap, err := conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		return
	}

	signalHandlerIsArmed := false
	doneChan := make(chan bool, 1)
	go ramswift.Daemon("/dev/null", testConfMapStrings, &signalHandlerIsArmed, doneChan)

	err = stats.Up(testConfMap)
	if nil != err {
		return
	}

	err = logger.Up(testConfMap)
	if nil != err {
		return
	}

	err = swiftclient.Up(testConfMap)
	if err != nil {
		headhunter.Down()
		stats.Down()
		return err
	}

	err = headhunter.Up(testConfMap)
	if nil != err {
		stats.Down()
		return
	}

	err = inode.Up(testConfMap)
	if nil != err {
		swiftclient.Down()
		headhunter.Down()
		stats.Down()
		return
	}

	err = dlm.Up(testConfMap)
	if nil != err {
		inode.Down()
		swiftclient.Down()
		headhunter.Down()
		stats.Down()
		return
	}

	err = Up(testConfMap)
	if nil != err {
		dlm.Down()
		inode.Down()
		swiftclient.Down()
		headhunter.Down()
		stats.Down()
		return
	}

	err = nil
	return
}

func testTeardown() (err error) {
	Down()
	inode.Down()
	swiftclient.Down()
	headhunter.Down()
	logger.Down()
	stats.Down()

	testDir, err := os.Getwd()
	if nil != err {
		return
	}

	err = os.Chdir("..")
	if nil != err {
		return
	}

	if platform.IsLinux {
		cmd := exec.Command("sudo", "umount", testDir)
		err = cmd.Run()
		if nil != err {
			return
		}
	} else {
		err = os.RemoveAll(testDir)
		if nil != err {
			return
		}
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

	dirInode, err = mS.Mkdir(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, dirname, inode.PosixModePerm)
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

	createdFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Unexpectedly couldn't create file: %v", err)
	}

	foundFileInodeNumber, err := mS.Lookup(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename)
	if err != nil {
		t.Fatalf("Unexpectedly failed to look up %v", basename)
	}

	if createdFileInodeNumber != foundFileInodeNumber {
		t.Fatalf("Expected created inode number %v to equal found inode number %v", createdFileInodeNumber, foundFileInodeNumber)
	}

	err = mS.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename)
	if nil != err {
		t.Fatalf("Unlink() returned error: %v", err)
	}
}

func TestGetstat(t *testing.T) {
	rootDirInodeNumber := inode.RootDirInodeNumber
	basename := "getstat.test"
	timeBeforeCreation := uint64(time.Now().UnixNano())

	inodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("couldn't create file: %v", err)
	}

	stat, err := mS.Getstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, inodeNumber)
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

	err = mS.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename)
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
	_, err = mS.Mkdir(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, "TestSubDirectory", inode.PosixModePerm)
	//	newDirInodeNum, err := mS.Mkdir(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, "TestSubDirectory")
	if nil != err {
		t.Fatalf("Mkdir() returned error: %v", err)
	}

	//    Create      #1 A/C                                  : create and open a normal file within Volume directory
	basename := "TestNormalFile"
	createdFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() [#1] returned error: %v", err)
	}

	//    Lookup      #1 A/C                                  : fetch the inode name of the just created normal file
	foundFileInodeNumber, err := mS.Lookup(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename)
	if err != nil {
		t.Fatalf("Lookup() [#1] returned error: %v", err)
	}
	if createdFileInodeNumber != foundFileInodeNumber {
		t.Fatalf("Expected created inode number %v to equal found inode number %v", createdFileInodeNumber, foundFileInodeNumber)
	}

	//    Write          A/C                                  : write something to normal file
	bufToWrite := []byte{0x41, 0x42, 0x43}
	write_rspSize, err := mS.Write(inode.InodeRootUserID, inode.InodeRootGroupID, nil, createdFileInodeNumber, 0, bufToWrite, nil)
	if nil != err {
		t.Fatalf("Write() returned error: %v", err)
	}
	if uint64(len(bufToWrite)) != write_rspSize {
		t.Fatalf("Write() expected to write %v bytes but actually wrote %v bytes", len(bufToWrite), write_rspSize)
	}

	// don't forget to flush
	err = mS.Flush(inode.InodeRootUserID, inode.InodeRootGroupID, nil, createdFileInodeNumber)
	if err != nil {
		t.Fatalf("Flush() returned error: %v", err)
	}

	//    Read           A/C                                  : read back what was just written to normal file
	read_buf, err := mS.Read(inode.InodeRootUserID, inode.InodeRootGroupID, nil, createdFileInodeNumber, 0, uint64(len(bufToWrite)), nil)
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
	getstat_1_rspStat, err := mS.Getstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, foundFileInodeNumber)
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
	err = mS.Resize(inode.InodeRootUserID, inode.InodeRootGroupID, nil, foundFileInodeNumber, 0)
	if nil != err {
		t.Fatalf("Resize() returned error: %v", err)
	}

	//    Getstat     #2 A/C                                  : verify the size of the normal file is now zero
	getstat_2_rspStat, err := mS.Getstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, foundFileInodeNumber)
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
	createdSymlinkInodeNumber, err := mS.Symlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, "TestSymlink", "TestNormalFile")
	if nil != err {
		t.Fatalf("Symlink() returned error: %v", err)
	}

	//    Lookup      #2 A/D                                  : fetch the inode name of the just created symlink
	lookup_2_inodeHandle, err := mS.Lookup(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, "TestSymlink")
	if nil != err {
		t.Fatalf("Lookup() [#2] returned error: %v", err)
	}
	if lookup_2_inodeHandle != createdSymlinkInodeNumber {
		t.Fatalf("Lookup() [#2] returned unexpected InodeNumber")
	}

	//    Readsymlink    A/D                                  : read the symlink to ensure it points to the normal file
	readsymlink_target, err := mS.Readsymlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lookup_2_inodeHandle)
	if nil != err {
		t.Fatalf("Readsymlink() returned error: %v", err)
	}
	if 0 != strings.Compare("TestNormalFile", readsymlink_target) {
		t.Fatalf("Readsymlink() data different from what was written")
	}

	//    Lookup      #3 A/B/                                 : fetch the inode name of the subdirectory
	lookup_3_inodeHandle, err := mS.Lookup(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, "TestSubDirectory")
	if nil != err {
		t.Fatalf("Lookup() [#3] returned error: %v", err)
	}

	//    Create      #2 A/B/E                                : create a normal file within subdirectory
	testSubDirectoryFileInode, err := mS.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lookup_3_inodeHandle, "TestSubDirectoryFile", inode.PosixModePerm)
	if nil != err {
		t.Fatalf("Create() [#2] returned error: %v", err)
	}

	//    Readdir and examine contents
	entriesExpected := []string{".", "..", "TestSubDirectoryFile"}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeRootGroupID, lookup_3_inodeHandle, entriesExpected)

	//    Link A/B/E
	err = mS.Link(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lookup_3_inodeHandle, "TestSubDirectoryFileHardLink", testSubDirectoryFileInode)
	if nil != err {
		t.Fatalf("Link() returned error: %v", err)
	}

	entriesExpected = []string{".", "..", "TestSubDirectoryFile", "TestSubDirectoryFileHardLink"}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeRootGroupID, lookup_3_inodeHandle, entriesExpected)

	//    Unlink      #1 A/B/E                                : delete the normal file within the subdirectory
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lookup_3_inodeHandle, "TestSubDirectoryFile")
	if nil != err {
		t.Fatalf("Unlink() [#1] returned error: %v", err)
	}

	entriesExpected = []string{".", "..", "TestSubDirectoryFileHardLink"}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeRootGroupID, lookup_3_inodeHandle, entriesExpected)

	//    Unlink      #1.5 A/B/E                                : delete the normal file within the subdirectory
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lookup_3_inodeHandle, "TestSubDirectoryFileHardLink")
	if nil != err {
		t.Fatalf("Unlink() [#1.5] returned error: %v", err)
	}

	entriesExpected = []string{".", "..", "TestSymlink", "TestNormalFile", "TestSubDirectory"}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeRootGroupID, rootDirInodeNumber, entriesExpected)

	//    Unlink      #2 A/D                                  : delete the symlink
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, "TestSymlink")

	if nil != err {
		t.Fatalf("Unlink() [#2] returned error: %v", err)
	}

	//    Unlink      #3 A/C                                  : delete the normal file
	err = mS.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, "TestNormalFile")
	if nil != err {
		t.Fatalf("Unlink() [#3] returned error: %v", err)
	}

	//    Rmdir       #4 A/B                                  : delete the subdirectory
	err = mS.Rmdir(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, "TestSubDirectory")
	if nil != err {
		t.Fatalf("Unlink() [#4] returned error: %v", err)
	}

	entriesExpected = []string{".", ".."}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeRootGroupID, rootDirInodeNumber, entriesExpected)
}

// TODO: flesh this out with other boundary condition testing for Link
func TestBadLinks(t *testing.T) {
	testDirInode := createTestDirectory(t, "BadLinks")

	validFile := "PerfectlyValidFile"
	validFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, testDirInode, validFile, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	nameTooLong := strings.Repeat("x", FileNameMax+1)
	err = mS.Link(inode.InodeRootUserID, inode.InodeRootGroupID, nil, testDirInode, nameTooLong, validFileInodeNumber)
	if nil != err {
		if blunder.IsNot(err, blunder.NameTooLongError) {
			t.Fatalf("Link() returned error %v, expected %v(%d).", blunder.Errno(err), blunder.NameTooLongError, blunder.NameTooLongError.Value())
		}
	} else {
		t.Fatal("Link() unexpectedly succeeded on too-long filename!")
	}

	entriesExpected := []string{".", "..", validFile}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeRootGroupID, testDirInode, entriesExpected)
}

// TODO: flesh this out with other boundary condition testing for Rename
func TestBadRename(t *testing.T) {
	testDirInode := createTestDirectory(t, "BadRename")
	nameTooLong := strings.Repeat("x", FileNameMax+1)

	validFile := "PerfectlyValidFile"
	_, err := mS.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, testDirInode, validFile, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	// Try to rename a valid file to a name that is too long
	err = mS.Rename(inode.InodeRootUserID, inode.InodeRootGroupID, nil, testDirInode, validFile, testDirInode, nameTooLong)
	if nil != err {
		if blunder.IsNot(err, blunder.NameTooLongError) {
			t.Fatalf("Link() returned error %v, expected %v(%d).", blunder.Errno(err), blunder.NameTooLongError, blunder.NameTooLongError.Value())
		}
	} else {
		t.Fatal("Link() unexpectedly succeeded on too-long filename!")
	}

	entriesExpected := []string{".", "..", validFile}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeRootGroupID, testDirInode, entriesExpected)

	// Try to rename a nonexistent file with a name that is too long
	err = mS.Rename(inode.InodeRootUserID, inode.InodeRootGroupID, nil, testDirInode, nameTooLong, testDirInode, "AlsoAGoodFilename")
	if nil != err {
		if blunder.IsNot(err, blunder.NameTooLongError) {
			t.Fatalf("Link() returned error %v, expected %v(%d).", blunder.Errno(err), blunder.NameTooLongError, blunder.NameTooLongError.Value())
		}
	} else {
		t.Fatal("Link() unexpectedly succeeded on too-long filename!")
	}

	entriesExpected = []string{".", "..", validFile}
	expectDirectory(t, inode.InodeRootUserID, inode.InodeRootGroupID, testDirInode, entriesExpected)
}

func TestBadChownChmod(t *testing.T) {
	var err error

	// Get root dir inode number
	rootDirInodeNumber := inode.RootDirInodeNumber

	// Create file to play with
	basename := "TestFile"
	createdFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() %v returned error: %v", basename, err)
	}

	// Since we are playing some games with size of mode/userid/groupid, make sure that we
	// correctly handle cases where the value is > uint32
	var tooBigForUint32 uint64 = math.MaxUint32 + 7<<48

	// Validate too-big Mode
	stat := make(Stat)
	stat[StatMode] = tooBigForUint32
	err = mS.Setstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, createdFileInodeNumber, stat)
	if blunder.IsNot(err, blunder.InvalidFileModeError) {
		t.Fatalf("Setstat() %v returned error %v, expected %v(%d).", basename, blunder.Errno(err), blunder.InvalidFileModeError, blunder.InvalidFileModeError.Value())
	}
	delete(stat, StatMode)

	// Validate too-big UserID
	stat[StatUserID] = tooBigForUint32
	err = mS.Setstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, createdFileInodeNumber, stat)
	if blunder.Errno(err) != int(blunder.InvalidFileModeError) {
		t.Fatalf("Setstat() %v returned error %v, expected %v(%d).", basename, blunder.Errno(err), blunder.InvalidFileModeError, blunder.InvalidFileModeError.Value())
	}
	delete(stat, StatUserID)

	// Validate too-big GroupID
	stat[StatGroupID] = tooBigForUint32
	err = mS.Setstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, createdFileInodeNumber, stat)
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
	lockFileInodeNumber, err := mS.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename, inode.PosixModePerm)
	if err != nil {
		t.Fatalf("Create() %v returned error: %v", basename, err)
	}

	// Resize the file to a 1M so that we can apply byte range locks:
	err = mS.Resize(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, 1024*1024)
	if err != nil {
		t.Fatalf("Resize() %v returned error: %v", basename, err)
	}

	// Write lock test:
	var lock FlockStruct
	lock.Type = syscall.F_WRLCK
	lock.Start = 0
	lock.Len = 0
	lock.Pid = 1

	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Write lock on file failed: %v", err)
	}

	lock.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Unlock on file failed: %v", blunder.Errno(err))
	}

	lock.Type = syscall.F_WRLCK
	lock.Pid = 1
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Write lock on file failed: %v", err)
	}

	// Try another write lock from a different pid, it should fail:
	lock.Pid = 2
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if blunder.Errno(err) != int(blunder.TryAgainError) {
		t.Fatalf("Write lock on a locked file should fail with EAGAIN instead got : %v", err)
	}

	// Lock again, it should succeed:
	//lock.Pid = 1
	//_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	//if err != nil {
	//	t.Fatalf("Relocking from same PID on file failed: %v", err)
	//}

	lock.Type = syscall.F_UNLCK
	lock.Pid = 1
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Unlock failed : %v", err)
	}

	// Read lock test:
	lock.Type = syscall.F_RDLCK
	lock.Pid = 1
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Read lock pid - 1 failed: %v", err)
	}

	lock.Pid = 2
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Read lock pid - 2 failed: %v", err)
	}

	// Try write lock it should fail:
	lock.Type = syscall.F_WRLCK
	lock.Pid = 3
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if blunder.Errno(err) != int(blunder.TryAgainError) {
		t.Fatalf("Write lock should have failed with EAGAIN instead got - %v", err)
	}

	lock.Pid = 2
	lock.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Unlock of (readlock) - 2 failed: %v", err)
	}

	lock.Pid = 1
	lock.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Unlock of (readlock) - 1 failed: %v", err)
	}

	lock.Type = syscall.F_WRLCK
	lock.Pid = 3
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Write lock should have succeeded instead got - %v", err)
	}

	lock.Type = syscall.F_UNLCK
	_, err = mS.Flock(inode.InodeRootUserID, inode.InodeRootGroupID, nil, lockFileInodeNumber, syscall.F_SETLK, &lock)
	if err != nil {
		t.Fatalf("Unlock of (write after read) failed: %v", err)
	}

	err = mS.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, rootDirInodeNumber, basename)
	if err != nil {
		t.Fatalf("Unlink() %v returned error: %v", basename, err)
	}
}
