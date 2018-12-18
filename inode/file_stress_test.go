package inode

import (
	"crypto/rand"
	"sync"
	"testing"
)

type testStressFileWritesGlobalsStruct struct {
	testStressNumWorkers        uint64
	testStressNumFlushesPerFile uint64
	testStressNumBlocksPerFlush uint64
	testStressNumBytesPerBlock  uint64
	volumeHandle                VolumeHandle
	writeBlock                  []byte
	childrenStart               sync.WaitGroup
	childrenDone                sync.WaitGroup
}

var testStressFileWritesGlobals testStressFileWritesGlobalsStruct

func TestStressFileWritesWhileStarved(t *testing.T) {
	testStressFileWritesGlobals.testStressNumWorkers = 100
	testStressFileWritesGlobals.testStressNumFlushesPerFile = 4 // Currently, each triggers a checkpoint as well
	testStressFileWritesGlobals.testStressNumBlocksPerFlush = 20
	testStressFileWritesGlobals.testStressNumBytesPerBlock = 1

	testStressFileWrites(t, true)
}

func TestStressFileWritesWhileNotStarved(t *testing.T) {
	testStressFileWritesGlobals.testStressNumWorkers = 100
	testStressFileWritesGlobals.testStressNumFlushesPerFile = 10 // Currently, each triggers a checkpoint as well
	testStressFileWritesGlobals.testStressNumBlocksPerFlush = 20
	testStressFileWritesGlobals.testStressNumBytesPerBlock = 1

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

	testStressFileWritesGlobals.writeBlock = make([]byte, testStressFileWritesGlobals.testStressNumBytesPerBlock)
	rand.Read(testStressFileWritesGlobals.writeBlock)

	testStressFileWritesGlobals.childrenStart.Add(1)
	testStressFileWritesGlobals.childrenDone.Add(int(testStressFileWritesGlobals.testStressNumWorkers))

	for workerIndex = 0; workerIndex < testStressFileWritesGlobals.testStressNumWorkers; workerIndex++ {
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
		fileFlushNumber uint64
		fileInodeNumber InodeNumber
	)

	fileInodeNumber, err = testStressFileWritesGlobals.volumeHandle.CreateFile(InodeMode(0000), InodeRootUserID, InodeGroupID(0))
	if nil != err {
		t.Fatalf("testStressFileWritesGlobals.volumeHandle.CreateFile() failed: %v", err)
	}

	testStressFileWritesGlobals.childrenStart.Wait()

	for fileFlushNumber = 0; fileFlushNumber < testStressFileWritesGlobals.testStressNumFlushesPerFile; fileFlushNumber++ {
		for fileBlockNumber = 0; fileBlockNumber < testStressFileWritesGlobals.testStressNumBlocksPerFlush; fileBlockNumber++ {
			err = testStressFileWritesGlobals.volumeHandle.Write(
				fileInodeNumber,
				fileBlockNumber*testStressFileWritesGlobals.testStressNumBytesPerBlock,
				testStressFileWritesGlobals.writeBlock,
				nil)
			if nil != err {
				t.Fatalf("Write() failed: %v", err)
			}
		}

		err = testStressFileWritesGlobals.volumeHandle.Flush(fileInodeNumber, false)
		if nil != err {
			t.Fatalf("Flush() failed: %v", err)
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
