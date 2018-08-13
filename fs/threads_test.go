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

func TestStress(t *testing.T) {
	testSetup(t, false)

	globalSyncPt = make(chan testRequest)

	if testing.Short() {
		t.Skip("skipping stress test.")
	}
	testTwoThreadsCreateUnlink(t)
	testTwoThreadsCreateCreate(t)
	testMultiThreadCreate(t)
	testMultiThreadCreateAndLookup(t)
	testMultiThreadCreateAndReaddir(t)

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
	unlinkTestOp
	unlinkLoopTestOp
	stopThreadTestOp
)

type testRequest struct {
	opType           testOpTyp // Operation type
	name1            string
	loopCount        int // Number of times to do operation. 0 = infinite
	minimumLoopCount int // Minimum number of times to do infinite operation.
	inodeNumber      inode.InodeNumber
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
		sendRequestToThread(i, t, stopThreadTestOp, inode.RootDirInodeNumber, "", 0, 0)
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
			_, err := mS.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inodeNumber, name1, inode.PosixModePerm)
			response := &testResponse{err: err}
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
		}
	}
}

// Set flag telling thread doing infinite loop to exit.
func setEndLoopFlag(threadID int) {
	threadMap[threadID].Lock()
	threadMap[threadID].endLoop = true
	threadMap[threadID].Unlock()
}

func sendRequestToThread(threadID int, t *testing.T, operation testOpTyp, inodeNumber inode.InodeNumber, name1 string, loopCount int, minimumLoopCount int) {

	// Clear endLoop flag before sending request
	threadMap[threadID].Lock()
	threadMap[threadID].endLoop = false
	threadMap[threadID].Unlock()

	request := &testRequest{opType: operation, t: t, name1: name1, loopCount: loopCount, minimumLoopCount: minimumLoopCount, inodeNumber: inodeNumber}
	threadMap[threadID].requestForThread <- request

	// We do not wait until the operation completes before returning.
}

// Test that two threads can grab a lock *exclusive* and the second thread
// only gets lock after first one has done Unlock().
func testTwoThreadsCreateUnlink(t *testing.T) {
	var numThreads int = 2

	// Initialize worker threads
	setupThreads(numThreads)

	// Tell thread 0 to loop creating files of the pattern "testfile*"
	sendRequestToThread(0, t, createLoopTestOp, inode.RootDirInodeNumber, "testfile", 0, 0)

	// Create the file from thread 1
	sendRequestToThread(1, t, createTestOp, inode.RootDirInodeNumber, "TestNormalFile", 0, 0)
	_ = <-threadMap[1].operationStatus

	// Unlink the file from thread 1
	sendRequestToThread(1, t, unlinkTestOp, inode.RootDirInodeNumber, "TestNormalFile", 0, 0)
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
	var numThreads int = 2

	// Initialize worker threads
	setupThreads(numThreads)

	for i := 0; i < numThreads; i++ {
		// Tell thread 0 to loop creating files of the pattern "testfile*"
		sendRequestToThread(i, t, createLoopTestOp, inode.RootDirInodeNumber, "testfile-"+strconv.Itoa(i), 0, 0)
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
	var numThreads int = 3
	nameOfTest := utils.GetFnName()

	// Initialize worker threads
	setupThreads(numThreads)

	// Unlink existing files
	for i := 0; i < numThreads; i++ {
		sendRequestToThread(i, t, unlinkLoopTestOp, inode.RootDirInodeNumber, nameOfTest+"-"+strconv.Itoa(i), 5, 0)
	}
	// Wait for unlinkLoopTestOp to complete
	for i := 0; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Create files
	for i := 0; i < numThreads; i++ {
		sendRequestToThread(i, t, createLoopTestOp, inode.RootDirInodeNumber, nameOfTest+"-"+strconv.Itoa(i), 5, 0)
	}
	// Wait for createLoopTestOp to complete
	for i := 0; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Now unlink the files
	for i := 0; i < numThreads; i++ {
		sendRequestToThread(i, t, unlinkLoopTestOp, inode.RootDirInodeNumber, nameOfTest+"-"+strconv.Itoa(i), 5, 0)
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
	var numThreads int = 3
	nameOfTest := utils.GetFnName()

	// Initialize worker threads
	setupThreads(numThreads)

	// Create a subdirectory to use
	sendRequestToThread(0, t, mkdirTestOp, inode.RootDirInodeNumber, nameOfTest+"-subdir", 0, 0)
	mkdirResponse := <-threadMap[0].operationStatus

	// Tell thread 0 to loop creating files of the pattern nameOfTest
	sendRequestToThread(0, t, createLoopTestOp, mkdirResponse.inodeNumber, nameOfTest, 10, 0)

	// Tell thread 1 to loop doing 35 Lookups
	sendRequestToThread(1, t, lookupPathLoopTestOp, mkdirResponse.inodeNumber, nameOfTest, 35, 0)

	// Tell thread 2 to loop doing 35 Lookups
	sendRequestToThread(2, t, lookupPathLoopTestOp, mkdirResponse.inodeNumber, nameOfTest, 35, 0)

	// Wait for threads to complete
	for i := 0; i < numThreads; i++ {
		_ = <-threadMap[i].operationStatus
	}

	// Tell thread 0 to loop unlinking test files created during test
	// and wait for it to complete
	sendRequestToThread(0, t, unlinkLoopTestOp, mkdirResponse.inodeNumber, nameOfTest, 10, 0)
	_ = <-threadMap[0].operationStatus

	// Remove subdirectory
	sendRequestToThread(0, t, rmdirTestOp, inode.RootDirInodeNumber, nameOfTest+"-subdir", 0, 0)
	_ = <-threadMap[0].operationStatus

	// Stop worker threads
	stopThreads(t)
}

// Test one thread doing Create() in loop and nine other threads doing Readdir
func testMultiThreadCreateAndReaddir(t *testing.T) {
	var numThreads int = 10
	nameOfTest := utils.GetFnName()

	// Initialize worker threads
	setupThreads(numThreads)

	// Create a subdirectory to use
	sendRequestToThread(0, t, mkdirTestOp, inode.RootDirInodeNumber, nameOfTest+"-subdir", 0, 0)
	mkdirResponse := <-threadMap[0].operationStatus

	// Tell thread 0 to loop creating files of the pattern nameOfTest in the subdirectory.
	// Create a minimum of at least 1000 before stopping.
	sendRequestToThread(0, t, createLoopTestOp, mkdirResponse.inodeNumber, nameOfTest, 0, 1000)

	// Pause a few milliseconds between operations
	time.Sleep(10 * time.Millisecond)

	// Tell threads 1 to numThreads to loop doing 35 readdirs
	for i := 1; i < numThreads; i++ {
		sendRequestToThread(i, t, readdirLoopTestOp, mkdirResponse.inodeNumber, nameOfTest, 35, 0)
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
	sendRequestToThread(1, t, readdirLoopTestOp, mkdirResponse.inodeNumber, nameOfTest, 1, 0)
	_ = <-threadMap[1].operationStatus

	// Stop worker threads
	stopThreads(t)
}
