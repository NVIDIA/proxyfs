package fs

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/utils"
)

//
// Code related to multiple test threads.
//

func TestStressMetaDataOpsWhileStarved(t *testing.T) {
	testStressMetaDataOps(t, true)
}

func TestStressMetaDataOpsWhileNotStarved(t *testing.T) {
	testStressMetaDataOps(t, false)
}

func testStressMetaDataOps(t *testing.T, starvationMode bool) {
	testSetup(t, starvationMode)

	globalSyncPt = make(chan testRequest)

	if testing.Short() {
		t.Skip("skipping stress test.")
	}
	testTwoThreadsCreateUnlink(t)
	testTwoThreadsCreateCreate(t)
	testMultiThreadCreate(t)
	testMultiThreadCreateAndLookup(t)
	testMultiThreadCreateAndReaddir(t)

	// Only run if we are not starved
	//
	// If we are starved, the test is killed because it uses too much memory
	// since we do not drain the memory fast enough.
	if !starvationMode {
		testCreateWriteNoFlush(t)
	}

	testTeardown(t)
}

type testOpTyp int

const (
	nilTestOp testOpTyp = iota
	createTestOp
	createLoopTestOp
	lookupPathLoopTestOp
	mkdirTestOp
	readdirLoopTestOp
	rmdirTestOp
	stopThreadTestOp
	unlinkTestOp
	unlinkLoopTestOp
	writeNoFlushLoopTestOp
)

type testRequest struct {
	opType           testOpTyp // Operation type
	name1            string
	loopCount        int // Number of times to do operation. 0 = infinite
	minimumLoopCount int // Minimum number of times to do infinite operation.
	inodeNumber      inode.InodeNumber
	offset           uint64
	length           uint64
	t                *testing.T
}

type testResponse struct {
	err         error
	inodeNumber inode.InodeNumber
}

// Per thread structure storing channel information
type threadInfo struct {
	sync.Mutex
	startedNode      chan bool
	requestForThread chan *testRequest
	operationStatus  chan *testResponse
	endLoop          bool // Flag used to signal an infinite loop test to stop
}

var globalSyncPt chan testRequest // Channel used to synchronize test threads to simulate multiple threads

// Map of threads and channels used for communication
var threadMap map[int]*threadInfo

// Setup thread stuctures based on number of threads test wants
func setupThreadMap(threadCount int) {
	threadMap = make(map[int]*threadInfo)

	for i := 0; i < threadCount; i++ {
		thread := &threadInfo{startedNode: make(chan bool), requestForThread: make(chan *testRequest), operationStatus: make(chan *testResponse)}
		threadMap[i] = thread
	}
}

func setupThreads(threadCount int) {
	setupThreadMap(threadCount)

	// Start threads and wait for them
	for i := range threadMap {
		go threadNode(i)
		_ = <-threadMap[i].startedNode
	}
}

func stopThreads(t *testing.T) {
	for i := range threadMap {

		// Tell thread to exit
		request := &testRequest{opType: stopThreadTestOp, t: t, inodeNumber: inode.RootDirInodeNumber}
		sendRequestToThread(i, t, request)
	}
}

func loopOp(fileRequest *testRequest, threadID int, inodeNumber inode.InodeNumber) (err error) {
	name1 := fileRequest.name1
	loopCount := fileRequest.loopCount
	minimumLoopCount := fileRequest.minimumLoopCount

	// Loop doing operation loopCount times.  If it is an infinite loop we loop until signaled to stop.
	//
	// minimumLoopCount is used with infiniteLoop to make sure the loop executes at least minimumLoopCount times
	// before returning.
	var infiniteLoopCount int // Useful for debugging
	var localLoopCount int
	for {
		fName := name1 + "-" + strconv.Itoa(localLoopCount)
		switch fileRequest.opType {
		case createLoopTestOp:
			_, err = mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber, fName, inode.PosixModePerm)
		case lookupPathLoopTestOp:
			_, err = mS.LookupPath(inode.InodeRootUserID, inode.InodeGroupID(0), nil, fName)
		case readdirLoopTestOp:
			areMoreEntries := true
			lastBasename := ""
			var maxEntries uint64 = 10
			var totalEntriesRead uint64 // Useful for debugging
			for areMoreEntries {
				dirEnts, numEntries, more, errShadow := mS.Readdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber, lastBasename, maxEntries, 0)
				lastBasename = dirEnts[len(dirEnts)-1].Basename
				areMoreEntries = more
				err = errShadow
				totalEntriesRead = totalEntriesRead + numEntries
			}
		case unlinkLoopTestOp:
			err = mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber, fName)
		case writeNoFlushLoopTestOp:
			_, _ = mS.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber, fileRequest.offset, bufToWrite, nil)
		}
		localLoopCount++
		infiniteLoopCount++

		// The infinite loop case breaks when control thread signals this thread to stop
		// and we have at least hit our minimumLoopCount.
		if (loopCount == 0) && (localLoopCount >= minimumLoopCount) {
			threadMap[threadID].Lock()
			if threadMap[threadID].endLoop == true {
				threadMap[threadID].Unlock()
				break
			}
			threadMap[threadID].Unlock()
		} else {
			if localLoopCount == loopCount {
				break
			}
		}
	}
	return err
}

// Test thread.  Just waits on channel and does operation requested.
func threadNode(threadID int) {

	// Tell control thread we are up and set channel to read.
	threadMap[threadID].startedNode <- true
	var request chan *testRequest
	request = threadMap[threadID].requestForThread

	// Wait for an operation
	for {
		fileRequest := <-request
		name1 := fileRequest.name1
		inodeNumber := fileRequest.inodeNumber

		switch fileRequest.opType {
		case stopThreadTestOp:
			return

		case createTestOp:
			response := &testResponse{}
			response.inodeNumber, response.err = mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber,
				name1, inode.PosixModePerm)
			threadMap[threadID].operationStatus <- response

		case createLoopTestOp:
			// Loop creating files loopCount times.
			err := loopOp(fileRequest, threadID, inodeNumber)
			response := &testResponse{err: err}
			threadMap[threadID].operationStatus <- response

		case lookupPathLoopTestOp:
			// Loop doing LookupPath of files loopCount times.
			err := loopOp(fileRequest, threadID, inodeNumber)
			response := &testResponse{err: err}
			threadMap[threadID].operationStatus <- response

		case mkdirTestOp:
			newInodeNumber, err := mS.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber, name1, inode.PosixModePerm)
			response := &testResponse{err: err, inodeNumber: newInodeNumber}
			threadMap[threadID].operationStatus <- response

		case readdirLoopTestOp:
			// Loop doing readdir of files loopCount times.
			err := loopOp(fileRequest, threadID, inodeNumber)
			response := &testResponse{err: err}
			threadMap[threadID].operationStatus <- response

		case rmdirTestOp:
			err := mS.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber, name1)
			response := &testResponse{err: err}
			threadMap[threadID].operationStatus <- response

		case unlinkTestOp:
			err := mS.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber, name1)
			response := &testResponse{err: err}
			threadMap[threadID].operationStatus <- response

		case unlinkLoopTestOp:
			// Loop unlinking files loopCount times.
			err := loopOp(fileRequest, threadID, inodeNumber)
			response := &testResponse{err: err}
			threadMap[threadID].operationStatus <- response

		case writeNoFlushLoopTestOp:
			// Loop writing and rewriting a file loopCount times.
			err := loopOp(fileRequest, threadID, inodeNumber)
			response := &testResponse{err: err}
			threadMap[threadID].operationStatus <- response
		}
	}
}

// Set flag telling thread doing infinite loop to exit.
func setEndLoopFlag(threadID int) {
	threadMap[threadID].Lock()
	threadMap[threadID].endLoop = true
	threadMap[threadID].Unlock()
}

func sendRequestToThread(threadID int, t *testing.T, request *testRequest) {
	// Clear endLoop flag before sending request
	threadMap[threadID].Lock()
	threadMap[threadID].endLoop = false
	threadMap[threadID].Unlock()

	threadMap[threadID].requestForThread <- request

	// We do not wait until the operation completes before returning.
}

// Test that two threads can grab a lock *exclusive* and the second thread
// only gets lock after first one has done Unlock().
func testTwoThreadsCreateUnlink(t *testing.T) {
	var numThreads = 2

	// Initialize worker threads
	setupThreads(numThreads)

	// Tell thread 0 to loop creating files of the pattern "testfile*"
	request := &testRequest{opType: createLoopTestOp, t: t, name1: "testfile",
		inodeNumber: inode.RootDirInodeNumber}
	sendRequestToThread(0, t, request)

	// Create the file from thread 1
	request = &testRequest{opType: createTestOp, t: t, name1: "TestNormalFile",
		inodeNumber: inode.RootDirInodeNumber}
	sendRequestToThread(1, t, request)
	_ = <-threadMap[1].operationStatus

	// Unlink the file from thread 1
	request = &testRequest{opType: unlinkTestOp, t: t, name1: "TestNormalFile",
		inodeNumber: inode.RootDirInodeNumber}
	sendRequestToThread(1, t, request)
	_ = <-threadMap[1].operationStatus

	// Tell thread 0 to stop creating files
	setEndLoopFlag(0)
	_ = <-threadMap[0].operationStatus

	// Stop worker threads
	stopThreads(t)
}

// Test that two threads can grab a lock *exclusive* and the second thread
// only gets lock after first one has done Unlock().
func testTwoThreadsCreateCreate(t *testing.T) {
	var numThreads = 2

	// Initialize worker threads
	setupThreads(numThreads)

	for i := 0; i < numThreads; i++ {
		// Tell thread 0 to loop creating files of the pattern "testfile*"
		request := &testRequest{opType: createLoopTestOp, t: t, name1: "testfile-" + strconv.Itoa(i),
			inodeNumber: inode.RootDirInodeNumber}
		sendRequestToThread(i, t, request)
	}

	time.Sleep(100 * time.Millisecond)

	// Tell threads to stop creating files
	for i := 0; i < numThreads; i++ {
		setEndLoopFlag(i)
		_ = <-threadMap[i].operationStatus
	}

	// Stop worker threads
	stopThreads(t)
}

// Test that two threads can grab a lock *exclusive* and the second thread
// only gets lock after first one has done Unlock().
func testMultiThreadCreate(t *testing.T) {
	var numThreads = 3
	nameOfTest := utils.GetFnName()

	// Initialize worker threads
	setupThreads(numThreads)

	// Unlink existing files
	for i := 0; i < numThreads; i++ {
		request := &testRequest{opType: unlinkLoopTestOp, t: t, name1: nameOfTest + "-" + strconv.Itoa(i),
			inodeNumber: inode.RootDirInodeNumber, loopCount: 5}
		sendRequestToThread(i, t, request)
	}
	// Wait for unlinkLoopTestOp to complete
	for i := 0; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Create files
	for i := 0; i < numThreads; i++ {
		request := &testRequest{opType: createLoopTestOp, t: t, name1: nameOfTest + "-" + strconv.Itoa(i),
			inodeNumber: inode.RootDirInodeNumber, loopCount: 5}
		sendRequestToThread(i, t, request)
	}
	// Wait for createLoopTestOp to complete
	for i := 0; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Now unlink the files
	for i := 0; i < numThreads; i++ {
		request := &testRequest{opType: unlinkLoopTestOp, t: t, name1: nameOfTest + "-" + strconv.Itoa(i),
			inodeNumber: inode.RootDirInodeNumber, loopCount: 5}
		sendRequestToThread(i, t, request)
	}
	// Wait for unlinkLoopTestOp to complete
	for i := 0; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Stop worker threads
	stopThreads(t)
}

// Test one thread doing Create() in loop and two threads
// doing Lookup()
func testMultiThreadCreateAndLookup(t *testing.T) {
	var numThreads = 3
	nameOfTest := utils.GetFnName()

	// Initialize worker threads
	setupThreads(numThreads)

	// Create a subdirectory to use
	request1 := &testRequest{opType: mkdirTestOp, t: t, name1: nameOfTest + "-subdir",
		inodeNumber: inode.RootDirInodeNumber}
	sendRequestToThread(0, t, request1)
	mkdirResponse := <-threadMap[0].operationStatus

	// Tell thread 0 to loop creating files of the pattern nameOfTest
	request2 := &testRequest{opType: createLoopTestOp, t: t, name1: nameOfTest,
		inodeNumber: mkdirResponse.inodeNumber, loopCount: 10}
	sendRequestToThread(0, t, request2)

	// Tell thread 1 to loop doing 35 Lookups
	request3 := &testRequest{opType: lookupPathLoopTestOp, t: t, name1: nameOfTest,
		inodeNumber: mkdirResponse.inodeNumber, loopCount: 35}
	sendRequestToThread(1, t, request3)

	// Tell thread 2 to loop doing 35 Lookups
	request4 := &testRequest{opType: lookupPathLoopTestOp, t: t, name1: nameOfTest,
		inodeNumber: mkdirResponse.inodeNumber, loopCount: 35}
	sendRequestToThread(2, t, request4)

	// Wait for threads to complete
	for i := 0; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Tell thread 0 to loop unlinking test files created during test
	// and wait for it to complete
	request5 := &testRequest{opType: unlinkLoopTestOp, t: t, name1: nameOfTest,
		inodeNumber: mkdirResponse.inodeNumber, loopCount: 10}
	sendRequestToThread(0, t, request5)
	_ = <-threadMap[0].operationStatus

	// Remove subdirectory
	request6 := &testRequest{opType: rmdirTestOp, t: t, name1: nameOfTest + "-subdir",
		inodeNumber: inode.RootDirInodeNumber}
	sendRequestToThread(0, t, request6)
	_ = <-threadMap[0].operationStatus

	// Stop worker threads
	stopThreads(t)
}

// Test one thread doing Create() in loop and nine other threads doing Readdir
func testMultiThreadCreateAndReaddir(t *testing.T) {
	var numThreads = 10
	nameOfTest := utils.GetFnName()

	// Initialize worker threads
	setupThreads(numThreads)

	// Create a subdirectory to use
	request1 := &testRequest{opType: mkdirTestOp, t: t, name1: nameOfTest + "-subdir",
		inodeNumber: inode.RootDirInodeNumber}
	sendRequestToThread(0, t, request1)
	mkdirResponse := <-threadMap[0].operationStatus

	// Tell thread 0 to loop creating files of the pattern nameOfTest in the subdirectory.
	// Create a minimum of at least 1000 before stopping.
	request2 := &testRequest{opType: createLoopTestOp, t: t, name1: nameOfTest,
		inodeNumber: mkdirResponse.inodeNumber, minimumLoopCount: 1000}
	sendRequestToThread(0, t, request2)

	// Pause a few milliseconds between operations
	time.Sleep(10 * time.Millisecond)

	// Tell threads 1 to numThreads to loop doing 35 readdirs
	for i := 1; i < numThreads; i++ {
		request3 := &testRequest{opType: readdirLoopTestOp, t: t, name1: nameOfTest,
			inodeNumber: mkdirResponse.inodeNumber, loopCount: 35}
		sendRequestToThread(i, t, request3)
	}

	// Wait until threads 1 to numThreads complete
	for i := 1; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Tell thread 0 to stop doing Creates in an infinite loop
	setEndLoopFlag(0)

	// Wait for thread 0 to complete
	_ = <-threadMap[0].operationStatus

	// Now tell thread 1 to do one more readdirLoopTestOp to make sure we can read 1000 entries
	request4 := &testRequest{opType: readdirLoopTestOp, t: t, name1: nameOfTest,
		inodeNumber: mkdirResponse.inodeNumber, loopCount: 1}
	sendRequestToThread(1, t, request4)
	_ = <-threadMap[1].operationStatus

	// Stop worker threads
	stopThreads(t)
}

var bufToWrite []byte

// Test numThreads doing create(), write() and no flush
func testCreateWriteNoFlush(t *testing.T) {
	// NOTE: This test uses a lot of memory and will cause a OOM.  Be careful
	// increasing numThreads, size of write buffer and number of overwrites.
	var numThreads = 125
	fileInodes := make([]inode.InodeNumber, numThreads) // Map to store each inode created
	nameOfTest := utils.GetFnName()

	// Initialize worker threads
	setupThreads(numThreads)

	// Create a subdirectory to use
	request4 := &testRequest{opType: mkdirTestOp, t: t, name1: nameOfTest + "-subdir",
		inodeNumber: inode.RootDirInodeNumber}
	sendRequestToThread(0, t, request4)
	mkdirResponse := <-threadMap[0].operationStatus

	// Create files used for writes
	for i := 0; i < numThreads; i++ {
		request5 := &testRequest{opType: createTestOp, t: t, name1: nameOfTest + "-" + strconv.Itoa(i),
			inodeNumber: mkdirResponse.inodeNumber, loopCount: 5}
		sendRequestToThread(i, t, request5)
	}
	// Wait for createTestOp to complete and store inode number created
	for i := 0; i < numThreads; i++ {
		response := <-threadMap[i].operationStatus
		fileInodes[i] = response.inodeNumber
	}

	var bufLen uint64 = 11 * 1024 * 1024
	bufToWrite = make([]byte, bufLen, bufLen)

	// Write to files without doing a flush.  We write 11MB starting from offset 0.
	// We rewrite the same location numOverWrites times.
	numOverWrites := 1
	minNumberOfLoops := 1
	writeOffset := uint64(0)
	for i := 0; i < numThreads; i++ {
		request6 := &testRequest{opType: writeNoFlushLoopTestOp, t: t, name1: nameOfTest + "-" + strconv.Itoa(i),
			inodeNumber: fileInodes[i], loopCount: numOverWrites, minimumLoopCount: minNumberOfLoops,
			offset: writeOffset, length: bufLen}
		sendRequestToThread(i, t, request6)
	}

	// Wait until threads complete
	for i := 0; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Stop worker threads
	stopThreads(t)
}
