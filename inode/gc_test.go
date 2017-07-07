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

func TestPhaseOneEmptySegmentDeletion(t *testing.T) {
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
	ourInode, ok := volume.inodeCache[ino]
	if !ok {
		t.Fatalf("expected to find inode #%v in cache", ino)
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
		_, objectGetErr := swiftclient.ObjectGet(segmentObjectLocation.accountName, segmentObjectLocation.containerName, segmentObjectLocation.objectName, 0, 16)
		if objectGetErr == nil {
			t.Errorf("expected object GET to fail for allegedly-deleted log segment object at %s/%s/%s", segmentObjectLocation.accountName, segmentObjectLocation.containerName, segmentObjectLocation.objectName)
		}
	}
	// the underlying log segment objects got deleted!! Yaaay~! ☆✦❤
}
