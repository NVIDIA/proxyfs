package dlm

import (
	"flag"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/transitions"
)

// Test string for passing inode 1
var s1 string = strconv.Itoa(1)

// Mutex for protecting global variables
var mutex sync.Mutex

type testOpTyp int

const (
	nilTestOp testOpTyp = iota
	readLock
	writeLock
	tryReadLock
	tryWriteLock
	unlock
	stopThread
)

type testReq struct {
	lockID string
	typ    testOpTyp // Operation type - writeLock, etc
	t      *testing.T
}

// Per thread structure storing channel information
type threadInfo struct {
	startedNode      chan bool
	requestForThread chan *testReq
}

var globalSyncPt chan testReq // Channel used to synchronize test threads to simulate multiple threads

var testConfMap conf.ConfMap

// Largely stolen from fs/api_test.go
func testSetup() (err error) {
	testDir, err := ioutil.TempDir(os.TempDir(), "ProxyFS_test_ldlm_")
	if nil != err {
		return
	}

	err = os.Chdir(testDir)
	if nil != err {
		return
	}

	err = os.Mkdir("TestVolume", os.ModePerm)

	testConfMap := conf.MakeConfMap()

	// Setup channel used to synchronize multiple test thread operations
	globalSyncPt = make(chan testReq)

	testConfMapStrings := []string{
		"Logging.LogFilePath=/dev/null",
		"Cluster.WhoAmI=nobody",
		"FSGlobals.VolumeGroupList=",
	}

	testConfMap, err = conf.MakeConfMapFromStrings(testConfMapStrings)
	if nil != err {
		return
	}

	err = transitions.Up(testConfMap)
	if nil != err {
		return
	}

	return
}

// Largely stolen from fs/api_test.go
func testTeardown() (err error) {
	err = transitions.Down(testConfMap)
	if nil != err {
		return
	}

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

	return
}

// Largely stolen from fs/api_test.go
func TestMain(m *testing.M) {
	flag.Parse()

	err := testSetup()
	if nil != err {
		logger.ErrorWithError(err)
	}

	testResults := m.Run()

	err = testTeardown()
	if nil != err {
		logger.ErrorWithError(err)
	}

	os.Exit(testResults)
}

// Test basic lock primitives
func TestLockAPI(t *testing.T) {

	// TODO - how cleanup lockMap here between test runs?
	testSimpleLocks(t)
	testTwoThreadsExclLocking(t)
	testTwoThreadsSharedLocking(t)
	testTwoThreadsAndExclToShared(t)
	testTwoThreadsAndSharedToExcl(t)
	test100ThreadsSharedLocking(t)
	test100ThreadsExclLocking(t)
}

// Test basic WriteLock, ReadLock and Unlock
func testSimpleLocks(t *testing.T) {
	assert := assert.New(t)

	myCookie := GenerateCallerID()
	myRwLock := &RWLockStruct{LockID: s1, Notify: nil, LockCallerID: myCookie}

	myRwLock.WriteLock()
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)
	assert.Equal(IsLockHeld(s1, myCookie, WRITELOCK), true)
	assert.Equal(IsLockHeld(s1, myCookie, ANYLOCK), true)
	assert.Equal(IsLockHeld(s1, myCookie, READLOCK), false)

	myRwLock.Unlock()
	waitCountOwners(s1, 0)
	waitCountWaiters(s1, 0)

	myRwLock.ReadLock()
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)
	assert.Equal(IsLockHeld(s1, myCookie, WRITELOCK), false)
	assert.Equal(IsLockHeld(s1, myCookie, ANYLOCK), true)
	assert.Equal(IsLockHeld(s1, myCookie, READLOCK), true)

	myRwLock.Unlock()
	waitCountOwners(s1, 0)
	waitCountWaiters(s1, 0)

	// Try locks
	err := myRwLock.TryWriteLock()
	assert.Nil(err, "TryWriteLock() should work if no lock owner.")
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)

	myRwLock.Unlock()
	waitCountOwners(s1, 0)
	waitCountWaiters(s1, 0)

	err = myRwLock.TryReadLock()
	assert.Nil(err, "TryReadLock() should work if no lock owner.")
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)

	myRwLock.Unlock()
	waitCountOwners(s1, 0)
	waitCountWaiters(s1, 0)
	assert.Equal(IsLockHeld(s1, myCookie, WRITELOCK), false)
	assert.Equal(IsLockHeld(s1, myCookie, ANYLOCK), false)
	assert.Equal(IsLockHeld(s1, myCookie, READLOCK), false)
}

//
// Code related to multiple test threads.
//

// Thread which currently owns the lock.
//
// This is a map indexed by a string of the inodeNumber
var currentLockOwner map[string]uint64

// Map of threads and channels used for communication
var threadMap map[uint64]*threadInfo

// Setup thread stuctures based on number of threads test wants
func setupThreadMap(threadCount uint64) {
	threadMap = make(map[uint64]*threadInfo)
	currentLockOwner = make(map[string]uint64)

	for i := uint64(0); i < threadCount; i++ {
		thread := &threadInfo{startedNode: make(chan bool), requestForThread: make(chan *testReq)}
		threadMap[i] = thread
	}
}

func setupThreads(threadCount uint64) {
	setupThreadMap(threadCount)

	// Start threads and wait for them
	for i := range threadMap {
		go threadNode(i)
		_ = <-threadMap[i].startedNode
	}
}

func stopThreads(t *testing.T) {
	for i := range threadMap {
		sendRequestToThread(i, t, stopThread, s1)
	}
}

// Test thread.  Just waits on channel and does operation requested.
func threadNode(threadID uint64) {

	// Tell control thread we are up and set channel to read.
	threadMap[threadID].startedNode <- true
	var request chan *testReq
	request = threadMap[threadID].requestForThread

	var myLockMap map[string]*RWLockStruct
	myLockMap = make(map[string]*RWLockStruct)

	// Get cookie which track this go routine
	myCookie := GenerateCallerID()

	// Wait for a command
	for {
		lockRequest := <-request
		assert := assert.New(lockRequest.t)
		assert.NotNil(lockRequest.lockID)

		switch lockRequest.typ {
		case stopThread:
			return

		case writeLock:
			// Init lock structure and add to map
			//			assert := assert.New(lockRequest.t)
			myRwLock := &RWLockStruct{LockID: lockRequest.lockID, Notify: nil, LockCallerID: myCookie}
			myLockMap[lockRequest.lockID] = myRwLock

			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, WRITELOCK), false)
			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, ANYLOCK), false)
			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, READLOCK), false)

			err := myRwLock.WriteLock()
			assert.Nil(err, "No error from WriteLock().")

			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, WRITELOCK), true)
			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, ANYLOCK), true)
			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, READLOCK), false)

			mutex.Lock()
			currentLockOwner[lockRequest.lockID] = threadID
			mutex.Unlock()

		case readLock:
			// Init lock structure and add to map
			myRwLock := &RWLockStruct{LockID: lockRequest.lockID, Notify: nil, LockCallerID: myCookie}
			myLockMap[lockRequest.lockID] = myRwLock

			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, WRITELOCK), false)
			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, ANYLOCK), false)
			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, READLOCK), false)

			err := myRwLock.ReadLock()
			assert.Nil(err, "No error from ReadLock().")

			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, WRITELOCK), false)
			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, ANYLOCK), true)
			assert.Equal(IsLockHeld(lockRequest.lockID, myCookie, READLOCK), true)

			mutex.Lock()
			currentLockOwner[lockRequest.lockID] = threadID
			mutex.Unlock()

		case tryWriteLock:
			// Init lock structure and add to map
			myRwLock := &RWLockStruct{LockID: lockRequest.lockID, Notify: nil, LockCallerID: myCookie}
			myLockMap[lockRequest.lockID] = myRwLock

			err := myRwLock.TryWriteLock()
			if err != nil {
				assert.True(blunder.Is(err, blunder.TryAgainError))

				mutex.Lock()
				currentLockOwner[lockRequest.lockID] = threadID
				mutex.Unlock()
			}

		case tryReadLock:
			// Init lock structure and add to map
			myRwLock := &RWLockStruct{LockID: lockRequest.lockID, Notify: nil, LockCallerID: myCookie}
			myLockMap[lockRequest.lockID] = myRwLock

			err := myRwLock.TryReadLock()
			if err != nil {
				assert.True(blunder.Is(err, blunder.TryAgainError))

				mutex.Lock()
				currentLockOwner[lockRequest.lockID] = threadID
				mutex.Unlock()
			}

		case unlock:
			// Lookup lock in map
			myRwLock := myLockMap[lockRequest.lockID]
			assert.NotNil(myRwLock)
			assert.NotNil(myRwLock.LockID)
			assert.Equal(IsLockHeld(myRwLock.LockID, myCookie, ANYLOCK), true)

			err := myRwLock.Unlock()
			assert.Nil(err, "No error from Unlock()().")

			delete(myLockMap, lockRequest.lockID)

			// We do not clear currentLockOwner here since that would be a race condition with a thread
			// which has just been granted the lock.  It is possible that it could grab the lock and set
			// currentLockOwner before we can grab the mutex.
		}
	}
}

func sendRequestToThread(threadID uint64, t *testing.T, operation testOpTyp, lockID string) {
	request := &testReq{typ: operation, lockID: lockID, t: t}
	threadMap[threadID].requestForThread <- request

	// We do not wait until the operation completes before returning.
}

// Test that two threads can grab a lock *exclusive* and the second thread
// only gets lock after first one has done Unlock().
func testTwoThreadsExclLocking(t *testing.T) {
	var numThreads uint64 = 2

	// Initialize worker threads
	setupThreads(numThreads)

	// Lock *exclusive* from thread 0 and wait until lock is owned.
	sendRequestToThread(0, t, writeLock, s1)
	waitCountOwners(s1, 1)

	// Send *exclusive* from thread 1, this will block until thread 0 does unlock.
	// We just wait until we see a thread waiting for the lock.
	sendRequestToThread(1, t, writeLock, s1)
	waitCountWaiters(s1, 1)

	// Release lock from thread 0.  This should grant lock to thread 1.
	sendRequestToThread(0, t, unlock, s1)

	// Block until the lock is granted to thread 1.
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 1)

	sendRequestToThread(1, t, unlock, s1)
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 0)

	// Stop worker threads
	stopThreads(t)
}

// Test that two threads can grab a lock shared.
func testTwoThreadsSharedLocking(t *testing.T) {
	var numThreads uint64 = 2

	// Initialize worker threads
	setupThreads(numThreads)

	// Lock *shared* from thread 0 and wait until lock is owned.
	sendRequestToThread(0, t, readLock, s1)
	waitCountOwners(s1, 1)

	// Send *shared* from thread 1.  This should be granted right away.
	sendRequestToThread(1, t, readLock, s1)
	waitCountOwners(s1, 2)
	waitCountWaiters(s1, 0)

	// Release lock from thread 0. Owners should just decrease by 1.
	sendRequestToThread(0, t, unlock, s1)
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)

	sendRequestToThread(1, t, unlock, s1)
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 0)

	// Stop worker threads
	stopThreads(t)
}

// Test that 100 threads can grab a lock shared.
func test100ThreadsSharedLocking(t *testing.T) {
	var numThreads uint64 = 100

	// Initialize worker threads
	setupThreads(numThreads)

	var i uint64
	for i = 0; i < numThreads; i++ {
		sendRequestToThread(i, t, readLock, s1)
		waitCountOwners(s1, (i + 1))
		waitCountWaiters(s1, 0)
	}

	currentOwners := numThreads
	for i = 0; i < numThreads; i++ {
		sendRequestToThread(i, t, unlock, s1)
		currentOwners--
		waitCountOwners(s1, currentOwners)
		waitCountWaiters(s1, 0)
	}

	// Stop worker threads
	stopThreads(t)
}

// Force 100 threads to grab the lock exclusively.  The operation
// should be serialized.
func test100ThreadsExclLocking(t *testing.T) {
	var numThreads uint64 = 100

	// Initialize worker threads
	setupThreads(numThreads)

	var i uint64
	for i = 0; i < numThreads; i++ {
		sendRequestToThread(i, t, writeLock, s1)
		waitCountOwners(s1, 1)
		waitCountWaiters(s1, i)
	}
	waitCountWaiters(s1, numThreads-1)

	// Any of the threads could get the lock once thread0 releases it.
	//
	// Therefore, we have to find the thread which currently owns the lock.
	//
	// We do this by having the thread that grabs the lock save its threadID
	// in the global currentLockOwner map.
	waiters := numThreads - 1
	var prevOwner int64 = -1
	for i = 0; i < numThreads; i++ {

	waitForLockAcquire:
		mutex.Lock()
		owner, ok := currentLockOwner[s1]
		if !ok {
			mutex.Unlock()
			// Lock is not yet held, let us wait
			time.Sleep(5 * time.Millisecond)
			goto waitForLockAcquire
		}
		mutex.Unlock()

		if int64(owner) == prevOwner {
			// We ran before the next caller is able to grab the lock. Let us wait and retry again:
			time.Sleep(5 * time.Millisecond)
			goto waitForLockAcquire
		}

		prevOwner = int64(owner)
		sendRequestToThread(owner, t, unlock, s1)

		// Wait until next thread picks up lock
		if waiters > 0 {
			waiters--
			waitCountWaiters(s1, waiters)
		}
	}
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 0)

	// Stop worker threads
	stopThreads(t)
}

// Test that if one thread grabs the lock exclusive and a second thread attempts
// to grab shared, the shared lock request is granted after the first thread unlocks.
func testTwoThreadsAndExclToShared(t *testing.T) {
	var numThreads uint64 = 2

	// Initialize worker threads
	setupThreads(numThreads)

	// Lock *exclusive* from thread 0 and wait until lock is owned.
	sendRequestToThread(0, t, writeLock, s1)
	waitCountOwners(s1, 1)

	// Send *shared* from thread 1, this will block until thread 0 does unlock.
	// We just wait until we see a thread waiting for the lock.
	sendRequestToThread(1, t, readLock, s1)
	waitCountWaiters(s1, 1)

	// Release lock from thread 0.  This should grant lock to thread 1.
	sendRequestToThread(0, t, unlock, s1)

	// Block until the lock is granted to thread 1.
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 1)

	sendRequestToThread(1, t, unlock, s1)
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 0)

	// Stop worker threads
	stopThreads(t)
}

// Test that if one thread grabs the lock shared and a second thread attempts
// to grab exclusive, the exclusive lock request is granted after the first thread unlocks.
func testTwoThreadsAndSharedToExcl(t *testing.T) {
	var numThreads uint64 = 2

	// Initialize worker threads
	setupThreads(numThreads)

	// Lock *exclusive* from thread 0 and wait until lock is owned.
	sendRequestToThread(0, t, readLock, s1)
	waitCountOwners(s1, 1)

	// Send *shared* from thread 1, this will block until thread 0 does unlock.
	// We just wait until we see a thread waiting for the lock.
	sendRequestToThread(1, t, writeLock, s1)
	waitCountWaiters(s1, 1)

	// Release lock from thread 0.  This should grant lock to thread 1.
	sendRequestToThread(0, t, unlock, s1)

	// Block until the lock is granted to thread 1.
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 1)

	sendRequestToThread(1, t, unlock, s1)
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 0)

	// Stop worker threads
	stopThreads(t)
}

// Test that if lock is held *exclusive* TryWriteLock() and TryReadLock() fail.
func testTryFailsIfHeldExclusive(t *testing.T) {
	var numThreads uint64 = 2

	// Initialize worker threads
	setupThreads(numThreads)

	// Lock *exclusive* from thread 0 and wait until lock is owned.
	sendRequestToThread(0, t, writeLock, s1)
	waitCountOwners(s1, 1)
	mutex.Lock()
	lockOwner := currentLockOwner[s1]
	mutex.Unlock()
	assert := assert.New(t)
	assert.Equal(lockOwner, 0, "Lock should be owned by thread 0.")

	// Try write lock which should fail and there should not be any waiters.
	sendRequestToThread(1, t, tryWriteLock, s1)
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)
	mutex.Lock()
	lockOwner = currentLockOwner[s1]
	mutex.Unlock()
	assert.Equal(lockOwner, 0, "Lock should be owned by thread 0.")

	// Try read lock which should fail and there should not be any waiters.
	sendRequestToThread(1, t, tryReadLock, s1)
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)
	mutex.Lock()
	lockOwner = currentLockOwner[s1]
	mutex.Unlock()
	assert.Equal(lockOwner, 0, "Lock should be owned by thread 0.")

	// Release lock from thread 0. Owners should now be 0.
	sendRequestToThread(0, t, unlock, s1)
	waitCountOwners(s1, 0)
	waitCountWaiters(s1, 0)

	// Now try write lock from thread 1. This should work.
	sendRequestToThread(1, t, tryWriteLock, s1)
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)
	mutex.Lock()
	lockOwner = currentLockOwner[s1]
	mutex.Unlock()
	assert.Equal(lockOwner, 1, "Lock should be owned by thread 1.")

	// Unlock from thread 1 and try a read lock from thread 1.  This should work.
	sendRequestToThread(1, t, unlock, s1)
	waitCountOwners(s1, 0)
	waitCountWaiters(s1, 0)

	sendRequestToThread(1, t, tryReadLock, s1)
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)
	mutex.Lock()
	lockOwner = currentLockOwner[s1]
	mutex.Unlock()
	assert.Equal(lockOwner, 1, "Lock should be owned by thread 1.")

	// A try of a write lock from thread 0 should fail if the lock is held shared by thread 1.
	sendRequestToThread(0, t, tryWriteLock, s1)
	waitCountOwners(s1, 1)
	waitCountWaiters(s1, 0)
	mutex.Lock()
	lockOwner = currentLockOwner[s1]
	mutex.Unlock()
	assert.Equal(lockOwner, 1, "Lock should be owned by thread 1.")

	// Release the lock
	sendRequestToThread(1, t, unlock, s1)
	waitCountWaiters(s1, 0)
	waitCountOwners(s1, 0)

	// Stop worker threads
	stopThreads(t)
}
