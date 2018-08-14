package inode

import (
	"crypto/rand"
	"sync"
	"testing"
)

const (
	testStressNumWorkers       uint64 = 100
	testStressNumBlocksPerFile uint64 = 100
	testStressNumBytesPerBlock uint64 = 1
)

type testStressFileWritesGlobalsStruct struct {
	volumeHandle  VolumeHandle
	writeBlock    []byte
	childrenStart sync.WaitGroup
	childrenDone  sync.WaitGroup
}

var testStressFileWritesGlobals testStressFileWritesGlobalsStruct

func TestStressFileWritesWhileStarved(t *testing.T) {
	testStressFileWrites(t, true)
}

func TestStressFileWritesWhileNotStarved(t *testing.T) {
	testStressFileWrites(t, false)
}

func testStressFileWrites(t *testing.T, starvationMode bool) {
	var (
		err         error
		workerIndex uint64
	)

	testSetup(t, starvationMode)

	testStressFileWritesGlobals.volumeHandle, err = FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") failed: %v", err)
	}

	testStressFileWritesGlobals.writeBlock = make([]byte, testStressNumBytesPerBlock)
	rand.Read(testStressFileWritesGlobals.writeBlock)

	testStressFileWritesGlobals.childrenStart.Add(1)
	testStressFileWritesGlobals.childrenDone.Add(int(testStressNumWorkers))

	for workerIndex = 0; workerIndex < testStressNumWorkers; workerIndex++ {
		go testStressFileWritesWorker(t, workerIndex)
	}

	testStressFileWritesGlobals.childrenStart.Done()

	testStressFileWritesGlobals.childrenDone.Wait()

	testTeardown(t)
}

func testStressFileWritesWorker(t *testing.T, workerIndex uint64) {
	var (
		err             error
		fileBlockNumber uint64
		fileInodeNumber InodeNumber
	)

	fileInodeNumber, err = testStressFileWritesGlobals.volumeHandle.CreateFile(InodeMode(0000), InodeRootUserID, InodeGroupID(0))
	if nil != err {
		t.Fatalf("testStressFileWritesGlobals.volumeHandle.CreateFile() failed: %v", err)
	}

	testStressFileWritesGlobals.childrenStart.Wait()

	for fileBlockNumber = 0; fileBlockNumber < testStressNumBlocksPerFile; fileBlockNumber++ {
		err = testStressFileWritesGlobals.volumeHandle.Write(
			fileInodeNumber,
			fileBlockNumber*testStressNumBytesPerBlock,
			testStressFileWritesGlobals.writeBlock,
			nil)
		if nil != err {
			t.Fatalf("Write() failed: %v", err)
		}
	}

	err = testStressFileWritesGlobals.volumeHandle.Flush(fileInodeNumber, true)
	if nil != err {
		t.Fatalf("Flush() failed: %v", err)
	}

	err = testStressFileWritesGlobals.volumeHandle.Destroy(fileInodeNumber)
	if nil != err {
		t.Fatalf("Destroy() failed: %v", err)
	}

	testStressFileWritesGlobals.childrenDone.Done()
}
