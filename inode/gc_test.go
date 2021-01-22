// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package inode

import (
	"testing"
	"time"

	"github.com/swiftstack/ProxyFS/swiftclient"
)

type testObjectLocationStruct struct {
	accountName   string
	containerName string
	objectName    string
}

func TestEmptySegmentDeletion(t *testing.T) {
	testSetup(t, false)

	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") failed: %v", err)
	}

	volume := testVolumeHandle.(*volumeStruct)

	ino, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	// '$$$$$$$$$$$$$$$$'
	ourBytes := []byte{0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24}

	// repeatedly write and flush to create some log segments
	for i := 0; i < 5; i++ {
		err := testVolumeHandle.Write(ino, uint64(i*16), ourBytes, nil)
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		err = testVolumeHandle.Flush(ino, false)
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}
	}

	// inspect log segment map
	ourInode, ok, err := volume.fetchInode(ino)
	if nil != err {
		t.Fatalf("fetchInode(ino==0x%016X) failed: %v", ino, err)
	}
	if !ok {
		t.Fatalf("fetchInode(ino==0x%016X) returned !ok", ino)
	}
	segmentNumbers := make([]uint64, 0, 5)
	segmentObjectLocations := make([]testObjectLocationStruct, 0, 5)
	for segmentNumber := range ourInode.LogSegmentMap {
		segmentNumbers = append(segmentNumbers, segmentNumber)
		containerName, objectName, _, getObjectLocationErr := volume.getObjectLocationFromLogSegmentNumber(segmentNumber)
		if nil != getObjectLocationErr {
			t.Fatalf("expected to be able to get log segment 0x%016X", segmentNumber)
		}
		segmentObjectLocations = append(segmentObjectLocations, testObjectLocationStruct{volume.accountName, containerName, objectName})
	}

	// overwrite it
	err = testVolumeHandle.Write(ino, 0, make([]byte, 16*5), nil)
	if err != nil {
		t.Fatalf("expected to be able to write to inode #%v", ino)
	}
	err = testVolumeHandle.Flush(ino, false)
	if err != nil {
		t.Fatalf("expected to be able to flush inode #%v", ino)
	}

	// wait for headhunter checkpoint which blocks deletes
	err = volume.headhunterVolumeHandle.DoCheckpoint()
	if nil != err {
		t.Errorf("expected volume.headhunterVolumeHandle.DoCheckpoint() to succeed: got %v", err)
	}

	// the deletions are async; give them a (literal) second
	time.Sleep(1 * time.Second)

	for _, deletedSegmentNumber := range segmentNumbers {
		_, getLogSegmentContainerErr := volume.getLogSegmentContainer(deletedSegmentNumber)
		if getLogSegmentContainerErr == nil {
			t.Errorf("expected volume.getLogSegmentContainer() to fail for allegedly-deleted log segment 0x%016X", deletedSegmentNumber)
		}
	}

	// the headhunter records got deleted, and ...
	for _, segmentObjectLocation := range segmentObjectLocations {
		var objectContentLengthErr error

		// wait for up to 20 sec for the object to be (async) deleted
		for try := 0; try < 20; try++ {
			_, objectContentLengthErr = swiftclient.ObjectContentLength(segmentObjectLocation.accountName, segmentObjectLocation.containerName, segmentObjectLocation.objectName)
			//t.Logf("verifying object delete for %v/%v try %d err '%v'\n", segmentObjectLocation.containerName, segmentObjectLocation.objectName, try, objectContentLengthErr)
			if objectContentLengthErr != nil {
				break
			}
			time.Sleep(time.Second)
		}
		if objectContentLengthErr == nil {
			t.Errorf("expected ObjectContentLength() to fail for allegedly-deleted log segment object at %s/%s/%s", segmentObjectLocation.accountName, segmentObjectLocation.containerName, segmentObjectLocation.objectName)
		}
	}
	// the underlying log segment objects got deleted!! Yaaay~! ☆✦❤

	testTeardown(t)
}
