package fs

import (
	"testing"

	"github.com/swiftstack/ProxyFS/inode"
)

func TestResolvePath(t *testing.T) {
	var (
		canonicalizedPathSplit   []string
		dirA                     inode.InodeNumber
		dirEntryBasename         string
		dirEntryInodeNumber      inode.InodeNumber
		dirEntryInodeType        inode.InodeType
		dirInodeNumber           inode.InodeNumber
		err                      error
		fileA                    inode.InodeNumber
		fileB                    inode.InodeNumber
		heldLocks                *heldLocksStruct
		foundInMap               bool
		reCanonicalizedPathSplit []string
		retryRequired            bool
		symlinkA                 inode.InodeNumber
		testContainer            inode.InodeNumber
	)

	// Validate canonicalizePath() & reCanonicalizePathForSymlink()

	canonicalizedPathSplit, err = canonicalizePath("//.//a/b/c/../d")
	if nil != err {
		t.Fatalf("canonicalizePath(\"//.//a/b/c/../d\") failed: %v", err)
	}
	if (3 != len(canonicalizedPathSplit)) ||
		("a" != canonicalizedPathSplit[0]) ||
		("b" != canonicalizedPathSplit[1]) ||
		("d" != canonicalizedPathSplit[2]) {
		t.Fatalf("canonicalizedPathSplit(\"//.//a/b/c/../d\") returned unexpected results: %v", canonicalizedPathSplit)
	}

	reCanonicalizedPathSplit, err = reCanonicalizePathForSymlink(canonicalizedPathSplit, 1, "e/f")
	if nil != err {
		t.Fatalf("reCanonicalizePathForSymlink(canonicalizedPathSplit, 1, \"e/f\") failed: %v", err)
	}
	if (4 != len(reCanonicalizedPathSplit)) ||
		("a" != reCanonicalizedPathSplit[0]) ||
		("e" != reCanonicalizedPathSplit[1]) ||
		("f" != reCanonicalizedPathSplit[2]) ||
		("d" != reCanonicalizedPathSplit[3]) {
		t.Fatalf("reCanonicalizePathForSymlink(canonicalizedPathSplit, 1, \"e/f\") returned unexpected results: %v", reCanonicalizedPathSplit)
	}

	// Build out directory hierarchy underneath "/TestResolvePathContainer/":
	//
	//   /
	//     TestResolvePathContainer/
	//                               FileA
	//                               SimlinkA --> FileA
	//                               SimlinkB --> ../DirA
	//
	// During testing, a PUT of /TestResolvePathContainer/SimlinkB/FileB
	//   will implicitly create /TestResolvePathContainer/DirA/FileB:
	//
	//   /
	//     TestResolvePathContainer/
	//                               FileA
	//                               SimlinkA --> FileA
	//                               SimlinkB --> ../DirA
	//                               DirA\
	//                                                    FileB

	testSetup(t, false)

	testContainer, err = testMountStruct.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, "TestResolvePathContainer", inode.PosixModePerm)
	if nil != err {
		t.Fatalf("Mkdir(,,,,\"TestResolvePathContainer\",) failed: %v", err)
	}
	fileA, err = testMountStruct.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testContainer, "FileA", inode.PosixModePerm)
	if nil != err {
		t.Fatalf("Create(,,,,\"FileA\",) failed: %v", err)
	}
	symlinkA, err = testMountStruct.Symlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testContainer, "SymlinkA", "FileA")
	if nil != err {
		t.Fatalf("Symlink(,,,,\"SymlinkA\",) failed: %v", err)
	}

	// GET or HEAD on "/TestResolvePathContainer/SymlinkA" should find FileA

	heldLocks = newHeldLocks()

	dirInodeNumber, dirEntryInodeNumber, dirEntryBasename, dirEntryInodeType, retryRequired, err =
		testMountStruct.resolvePath(
			inode.RootDirInodeNumber,
			"/TestResolvePathContainer/SymlinkA",
			heldLocks,
			resolvePathFollowDirEntrySymlinks|
				resolvePathFollowDirSymlinks)

	if nil != err {
		t.Fatalf("resolvePath() for GET or HEAD failed: %v", err)
	}
	if (testContainer != dirInodeNumber) ||
		(fileA != dirEntryInodeNumber) ||
		("FileA" != dirEntryBasename) ||
		(inode.FileType != dirEntryInodeType) ||
		retryRequired ||
		(0 != len(heldLocks.exclusive)) ||
		(1 != len(heldLocks.shared)) {
		t.Fatalf("resolvePath() for GET or HEAD returned unexpected results")
	}

	_, foundInMap = heldLocks.shared[fileA]
	if !foundInMap {
		t.Fatalf("resolvePath() for GET or HEAD returned heldLocks.shared missing fileA")
	}

	heldLocks.free()

	// POST or COALESCE (destPath) on "/TestResolvePathContainer/SymlinkA" should find FileA

	heldLocks = newHeldLocks()

	dirInodeNumber, dirEntryInodeNumber, dirEntryBasename, dirEntryInodeType, retryRequired, err =
		testMountStruct.resolvePath(
			inode.RootDirInodeNumber,
			"/TestResolvePathContainer/SymlinkA",
			heldLocks,
			resolvePathFollowDirEntrySymlinks|
				resolvePathFollowDirSymlinks|
				resolvePathCreateMissingPathElements|
				resolvePathRequireExclusiveLockOnDirEntryInode)

	if nil != err {
		t.Fatalf("resolvePath() for POST or COALESCE (destPath) failed: %v", err)
	}
	if (testContainer != dirInodeNumber) ||
		(fileA != dirEntryInodeNumber) ||
		("FileA" != dirEntryBasename) ||
		(inode.FileType != dirEntryInodeType) ||
		retryRequired ||
		(1 != len(heldLocks.exclusive)) ||
		(0 != len(heldLocks.shared)) {
		t.Fatalf("resolvePath() for POST or COALESCE (destPath) returned unexpected results")
	}

	_, foundInMap = heldLocks.exclusive[fileA]
	if !foundInMap {
		t.Fatalf("resolvePath() for POST or COALESCE (destPath) returned heldLocks.exclusive missing fileA")
	}

	heldLocks.free()

	// DELETE on "/TestResolvePathContainer/SymlinkA" should find SymlinkA

	heldLocks = newHeldLocks()

	dirInodeNumber, dirEntryInodeNumber, dirEntryBasename, dirEntryInodeType, retryRequired, err =
		testMountStruct.resolvePath(
			inode.RootDirInodeNumber,
			"/TestResolvePathContainer/SymlinkA",
			heldLocks,
			resolvePathFollowDirSymlinks|
				resolvePathRequireExclusiveLockOnDirEntryInode|
				resolvePathRequireExclusiveLockOnDirInode)

	if nil != err {
		t.Fatalf("resolvePath() for DELETE failed: %v", err)
	}
	if (testContainer != dirInodeNumber) ||
		(symlinkA != dirEntryInodeNumber) ||
		("SymlinkA" != dirEntryBasename) ||
		(inode.SymlinkType != dirEntryInodeType) ||
		retryRequired ||
		(2 != len(heldLocks.exclusive)) ||
		(0 != len(heldLocks.shared)) {
		t.Fatalf("resolvePath() for DELETE returned unexpected results")
	}

	_, foundInMap = heldLocks.exclusive[symlinkA]
	if !foundInMap {
		t.Fatalf("resolvePath() for DELETE returned heldLocks.shared missing symlinkA")
	}
	_, foundInMap = heldLocks.exclusive[testContainer]
	if !foundInMap {
		t.Fatalf("resolvePath() for DELETE returned heldLocks.shared missing symlinkA")
	}

	heldLocks.free()

	// PUT on /TestResolvePathContainer/SimlinkB/FileB should create /TestResolvePathContainer/DirA/FileB

	heldLocks = newHeldLocks()

	dirInodeNumber, dirEntryInodeNumber, dirEntryBasename, dirEntryInodeType, retryRequired, err =
		testMountStruct.resolvePath(
			inode.RootDirInodeNumber,
			"/TestResolvePathContainer/DirA/FileB",
			heldLocks,
			resolvePathFollowDirEntrySymlinks|
				resolvePathFollowDirSymlinks|
				resolvePathCreateMissingPathElements|
				resolvePathRequireExclusiveLockOnDirEntryInode)

	if nil != err {
		t.Fatalf("resolvePath() for PUT (thru symlink & missing dir) failed: %v", err)
	}
	if ("FileB" != dirEntryBasename) ||
		(inode.FileType != dirEntryInodeType) ||
		retryRequired ||
		(3 != len(heldLocks.exclusive)) ||
		(0 != len(heldLocks.shared)) {
		t.Fatalf("resolvePath() for PUT (thru symlink & missing dir) returned unexpected results")
	}

	dirA = dirInodeNumber
	fileB = dirEntryInodeNumber

	_, foundInMap = heldLocks.exclusive[fileB]
	if !foundInMap {
		t.Fatalf("resolvePath() for PUT (thru symlink & missing dir) returned heldLocks.exclusive missing fileB")
	}
	_, foundInMap = heldLocks.exclusive[dirA]
	if !foundInMap {
		t.Fatalf("resolvePath() for PUT (thru symlink & missing dir) returned heldLocks.exclusive missing dirA")
	}
	_, foundInMap = heldLocks.exclusive[testContainer]
	if !foundInMap {
		t.Fatalf("resolvePath() for PUT (thru symlink & missing dir) returned heldLocks.exclusive missing testContainer")
	}

	heldLocks.free()

	dirEntryInodeNumber, err = testMountStruct.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testContainer, "DirA")
	if nil != err {
		t.Fatalf("Lookup(,,,,\"DirA\") failed: %v", err)
	}
	if dirEntryInodeNumber != dirA {
		t.Fatalf("Lookup(,,,,\"DirA\") returned 0x%016X... expected 0x%016X", dirEntryInodeNumber, dirA)
	}

	dirEntryInodeNumber, err = testMountStruct.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirA, "FileB")
	if nil != err {
		t.Fatalf("Lookup(,,,,\"FileB\") failed: %v", err)
	}
	if dirEntryInodeNumber != fileB {
		t.Fatalf("Lookup(,,,,\"FileB\") returned 0x%016X... expected 0x%016X", dirEntryInodeNumber, fileB)
	}

	// Destroy directory hierachy underneath "/TestResolvePathContainer/"

	err = testMountStruct.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirA, "FileB")
	if nil != err {
		t.Fatalf("Unlink(,,,,\"FileB\") failed: %v", err)
	}
	err = testMountStruct.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testContainer, "DirA")
	if nil != err {
		t.Fatalf("Rmdir(,,,,\"DirA\") failed: %v", err)
	}
	err = testMountStruct.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testContainer, "SymlinkA")
	if nil != err {
		t.Fatalf("Unlink(,,,,\"SymlinkA\") failed: %v", err)
	}
	err = testMountStruct.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, testContainer, "FileA")
	if nil != err {
		t.Fatalf("Unlink(,,,,\"FileA\") failed: %v", err)
	}
	err = testMountStruct.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, "TestResolvePathContainer")
	if nil != err {
		t.Fatalf("Rmdir(,,,,\"TestResolvePathContainer\") failed: %v", err)
	}

	testTeardown(t)
}

type testCanonicalizePathItemStruct struct {
	path                   string
	shouldSucceed          bool
	canonicalizedPathSplit []string
	dirInodeIndex          int
}

func TestCanonicalizePath(t *testing.T) {
	var (
		canonicalizedPathSplit   []string
		containerInodeNumber     inode.InodeNumber
		dirInodeIndex            int
		directoryInodeNumber     inode.InodeNumber
		err                      error
		pathSplitIndex           int
		testCanonicalizePathItem *testCanonicalizePathItemStruct
		testCanonicalizePathList []*testCanonicalizePathItemStruct
	)

	testSetup(t, false)

	_, err = testMountStruct.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, "RootFileName", inode.PosixModePerm)
	if nil != err {
		t.Fatal(err)
	}
	containerInodeNumber, err = testMountStruct.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, "ContainerName", inode.PosixModePerm)
	if nil != err {
		t.Fatal(err)
	}
	_, err = testMountStruct.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, containerInodeNumber, "ContainerFileName", inode.PosixModePerm)
	if nil != err {
		t.Fatal(err)
	}
	directoryInodeNumber, err = testMountStruct.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, containerInodeNumber, "DirectoryName", inode.PosixModePerm)
	if nil != err {
		t.Fatal(err)
	}
	_, err = testMountStruct.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, directoryInodeNumber, "DirectoryFileName", inode.PosixModePerm)
	if nil != err {
		t.Fatal(err)
	}

	testCanonicalizePathList = []*testCanonicalizePathItemStruct{
		&testCanonicalizePathItemStruct{
			path:                   "",
			shouldSucceed:          false,
			canonicalizedPathSplit: []string{}, // Not actually returned
			dirInodeIndex:          0,          // Not actually returned
		},
		&testCanonicalizePathItemStruct{
			path:                   "MissingNameInRoot",
			shouldSucceed:          true,
			canonicalizedPathSplit: []string{"MissingNameInRoot"},
			dirInodeIndex:          -1,
		},
		&testCanonicalizePathItemStruct{
			path:                   "RootFileName",
			shouldSucceed:          true,
			canonicalizedPathSplit: []string{"RootFileName"},
			dirInodeIndex:          -1,
		},
		&testCanonicalizePathItemStruct{
			path:                   "ContainerName",
			shouldSucceed:          true,
			canonicalizedPathSplit: []string{"ContainerName"},
			dirInodeIndex:          0,
		},
		&testCanonicalizePathItemStruct{
			path:                   "ContainerName/MissingNameInContainer",
			shouldSucceed:          true,
			canonicalizedPathSplit: []string{"ContainerName", "MissingNameInContainer"},
			dirInodeIndex:          0,
		},
		&testCanonicalizePathItemStruct{
			path:                   "ContainerName/DirectoryName",
			shouldSucceed:          true,
			canonicalizedPathSplit: []string{"ContainerName", "DirectoryName"},
			dirInodeIndex:          1,
		},
		&testCanonicalizePathItemStruct{
			path:                   "ContainerName/DirectoryName/MissingNameInDirectory",
			shouldSucceed:          true,
			canonicalizedPathSplit: []string{"ContainerName", "DirectoryName", "MissingNameInDirectory"},
			dirInodeIndex:          1,
		},
		&testCanonicalizePathItemStruct{
			path:                   "ContainerName/DirectoryName/DirectoryFileName",
			shouldSucceed:          true,
			canonicalizedPathSplit: []string{"ContainerName", "DirectoryName", "DirectoryFileName"},
			dirInodeIndex:          1,
		},
		&testCanonicalizePathItemStruct{
			path:                   "ContainerName/DirectoryName/MissingNameInDirectory/MissingSubDirectoryName",
			shouldSucceed:          true,
			canonicalizedPathSplit: []string{"ContainerName", "DirectoryName", "MissingNameInDirectory", "MissingSubDirectoryName"},
			dirInodeIndex:          2,
		},
	}

	for _, testCanonicalizePathItem = range testCanonicalizePathList {
		canonicalizedPathSplit, dirInodeIndex, err = testMountStruct.canonicalizePathAndLocateLeafDirInode(testCanonicalizePathItem.path)
		if testCanonicalizePathItem.shouldSucceed {
			if nil == err {
				if len(canonicalizedPathSplit) != len(testCanonicalizePathItem.canonicalizedPathSplit) {
					t.Fatalf("canonicalizePathAndLocateLeafDirInode(\"%s\") received unexpected canonicalizedPathSplit: %v", testCanonicalizePathItem.path, canonicalizedPathSplit)
				}
				for pathSplitIndex = range canonicalizedPathSplit {
					if canonicalizedPathSplit[pathSplitIndex] != testCanonicalizePathItem.canonicalizedPathSplit[pathSplitIndex] {
						t.Fatalf("canonicalizePathAndLocateLeafDirInode(\"%s\") received unexpected canonicalizedPathSplit: %v", testCanonicalizePathItem.path, canonicalizedPathSplit)
					}
				}
				if dirInodeIndex != testCanonicalizePathItem.dirInodeIndex {
					t.Fatalf("canonicalizePathAndLocateLeafDirInode(\"%s\") received unexpected canonicalizedPathSplit: %v", testCanonicalizePathItem.path, dirInodeIndex)
				}
			} else {
				t.Fatalf("canonicalizePathAndLocateLeafDirInode(\"%s\") unexpectadly failed: %v", testCanonicalizePathItem.path, err)
			}
		} else { // !testCanonicalizePathItem.shouldSucceed
			if nil == err {
				t.Fatalf("canonicalizePathAndLocateLeafDirInode(\"%s\") unexpectadly succeeded", testCanonicalizePathItem.path)
			}
		}
	}

	err = testMountStruct.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, directoryInodeNumber, "DirectoryFileName")
	if nil != err {
		t.Fatal(err)
	}
	err = testMountStruct.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, containerInodeNumber, "DirectoryName")
	if nil != err {
		t.Fatal(err)
	}
	err = testMountStruct.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, containerInodeNumber, "ContainerFileName")
	if nil != err {
		t.Fatal(err)
	}
	err = testMountStruct.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, "ContainerName")
	if nil != err {
		t.Fatal(err)
	}
	err = testMountStruct.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, "RootFileName")
	if nil != err {
		t.Fatal(err)
	}

	testTeardown(t)
}
