// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package inode

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/trackedlock"
)

const (
	testDirStressBasenamePrefix = "__TestDirStress_" // To be appended with Nonce + "_" + ThreadIndex + "_" + LinkIndex (for this Thread)

	testDirStressDelayBetweenLinkAndUnlink time.Duration = 1 * time.Microsecond
	testDirStressDelayBetweenThreadStarts  time.Duration = 1 * time.Microsecond

	testDirStressNumIterations        uint64 = 3
	testDirStressNumLinksPerIteration uint64 = 3
	testDirStressNumThreads           uint64 = 30

	testDirStressVolumeName = "TestVolume"
)

type testDirStressGlobalsStruct struct {
	trackedlock.Mutex // Used to emulate the exclusive lock used in package fs on RootDirInodeNumber
	nonce             uint64
	waitGroup         sync.WaitGroup
	err               []error
}

var testDirStressGlobals = &testDirStressGlobalsStruct{}

func TestDirStressWhileStarved(t *testing.T) {
	testDirStress(t, true)
}

func TestDirStressWhileNotStarved(t *testing.T) {
	testDirStress(t, false)
}

func testDirStress(t *testing.T, starvationMode bool) {
	var (
		err                    error
		headhunterVolumeHandle headhunter.VolumeHandle
		threadIndex            uint64
	)

	testSetup(t, starvationMode)

	headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(testDirStressVolumeName)
	if nil != err {
		t.Fatalf("headhunter.FetchVolumeHandle(\"%s\") failed: %v", testDirStressVolumeName, err)
	}

	testDirStressGlobals.nonce = headhunterVolumeHandle.FetchNonce()

	testDirStressGlobals.waitGroup.Add(int(testDirStressNumThreads))

	testDirStressGlobals.err = make([]error, testDirStressNumThreads)

	for threadIndex = uint64(0); threadIndex < testDirStressNumThreads; threadIndex++ {
		time.Sleep(testDirStressDelayBetweenThreadStarts)

		go testDirStressThread(threadIndex)
	}

	testDirStressGlobals.waitGroup.Wait()

	for _, err = range testDirStressGlobals.err {
		if nil != err {
			t.Fatal(err)
		}
	}

	testTeardown(t)
}

func testDirStressThread(threadIndex uint64) {
	var (
		basename             string
		err                  error
		fileInodeNumber      InodeNumber
		iteration            uint64
		linkIndex            uint64
		toDestroyInodeNumber InodeNumber
		volumeHandle         VolumeHandle
	)

	defer testDirStressGlobals.waitGroup.Done()

	volumeHandle, err = FetchVolumeHandle(testDirStressVolumeName)
	if nil != err {
		testDirStressGlobals.Lock()
		testDirStressGlobals.err[threadIndex] = fmt.Errorf("FetchVolumeHandle(\"%s\") failed: %v", testDirStressVolumeName, err)
		testDirStressGlobals.Unlock()
		return
	}

	for iteration = uint64(0); iteration < testDirStressNumIterations; iteration++ {
		fileInodeNumber, err = volumeHandle.CreateFile(PosixModePerm, InodeUserID(0), InodeGroupID(0))
		if nil != err {
			testDirStressGlobals.Lock()
			testDirStressGlobals.err[threadIndex] = fmt.Errorf("volumeHandle.CreateFile(PosixModePerm, InodeUserID(0), InodeGroupID(0)) failed: %v", err)
			testDirStressGlobals.Unlock()
			return
		}

		for linkIndex = uint64(0); linkIndex < testDirStressNumLinksPerIteration; linkIndex++ {
			basename = fmt.Sprintf("%s%016X_%016X_%016X", testDirStressBasenamePrefix, testDirStressGlobals.nonce, threadIndex, linkIndex)

			testDirStressGlobals.Lock()
			err = volumeHandle.Link(RootDirInodeNumber, basename, fileInodeNumber, false)
			if nil != err {
				testDirStressGlobals.err[threadIndex] = fmt.Errorf("volumeHandle.Link(RootDirInodeNumber, \"%s\", fileInodeNumber, false) failed: %v", basename, err)
				testDirStressGlobals.Unlock()
				return
			}
			testDirStressGlobals.Unlock()

			time.Sleep(testDirStressDelayBetweenLinkAndUnlink)

			testDirStressGlobals.Lock()
			toDestroyInodeNumber, err = volumeHandle.Unlink(RootDirInodeNumber, basename, false)
			if nil != err {
				testDirStressGlobals.err[threadIndex] = fmt.Errorf("volumeHandle.Unlink(RootDirInodeNumber, \"%s\", false) failed: %v", basename, err)
				testDirStressGlobals.Unlock()
				return
			}
			// Note that the originally created fileInode is not in any Directory, so unlinking the only dirEntry to it drops LinkCount to zero
			if fileInodeNumber != toDestroyInodeNumber {
				testDirStressGlobals.err[threadIndex] = fmt.Errorf("volumeHandle.Unlink(RootDirInodeNumber, \"%s\", false) should have returned toDestroyInodeNumber == fileInodeNumber", basename)
				testDirStressGlobals.Unlock()
				return
			}
			testDirStressGlobals.Unlock()
		}

		err = volumeHandle.Destroy(fileInodeNumber)
		if nil != err {
			testDirStressGlobals.Lock()
			testDirStressGlobals.err[threadIndex] = fmt.Errorf("volumeHandle.Destroy(fileInodeNumber) failed: %v", err)
			testDirStressGlobals.Unlock()
			return
		}
	}

	testDirStressGlobals.Lock()
	testDirStressGlobals.err[threadIndex] = nil
	testDirStressGlobals.Unlock()
}
