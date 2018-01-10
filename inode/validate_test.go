package inode

import (
	"strings"
	"testing"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

// Helper to fetch a volume handle and create a file for a test. Return the
// volume handle and the file inode number.
func volumeAndFileInoForTest(t *testing.T) (VolumeHandle, InodeNumber) {
	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if err != nil {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") should have worked - got error: %v", err)
	}

	fileInodeNumber, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	err = testVolumeHandle.WriteAsSlice(fileInodeNumber, 0, []byte{0x00, 0x01, 0x02, 0x03, 0x04}, nil)
	if err != nil {
		t.Fatalf("WriteAsSlice(fileInodeNumber, 0, []byte{0x00, 0x01, 0x02, 0x03, 0x04}) failed: %v", err)
	}

	err = testVolumeHandle.Flush(fileInodeNumber, true)
	if err != nil {
		t.Fatalf("Flush of inode %v failed", fileInodeNumber)
	}

	return testVolumeHandle, fileInodeNumber
}

func TestValidate(t *testing.T) {
	testVolumeHandle, fileInodeNumber := volumeAndFileInoForTest(t)

	// Validation succeeds if nothing has gone wrong
	err := testVolumeHandle.Validate(fileInodeNumber)
	if err != nil {
		t.Fatalf("Validate() failed on presumably-good inode")
	}

	// Now let's artificially make something go wrong and confirm that
	// Validate() catches it!

	testVolume := testVolumeHandle.(*volumeStruct)

	// Grab the InodeRec
	inodeRec, ok, err := testVolume.headhunterVolumeHandle.GetInodeRec(uint64(fileInodeNumber))
	if err != nil || !ok {
		t.Fatalf("failed to fetch InodeRec for inode 0x%016X", fileInodeNumber)
	}

	// Corrupt the non-preamble part
	for i := len(globals.inodeRecDefaultPreambleBuf); i < len(inodeRec); i++ {
		inodeRec[i] = 0xff
	}

	// And put it back
	err = testVolume.headhunterVolumeHandle.PutInodeRec(uint64(fileInodeNumber), inodeRec)
	if err != nil {
		t.Fatalf("failed to save corrupted InodeRec for inode 0x%016X", fileInodeNumber)
	}

	// Now remove fileInodeNumber from inodeCache
	testVolume.Lock()
	delete(testVolume.inodeCache, fileInodeNumber)
	testVolume.Unlock()

	// Try to Validate, observe that it fails
	validationErr := testVolumeHandle.Validate(fileInodeNumber)
	if validationErr == nil {
		t.Fatalf("expected validation to fail")
	}
	if blunder.IsNot(validationErr, blunder.CorruptInodeError) {
		t.Fatalf("expected validation error %q to have error value %v, actual value was %v", validationErr, blunder.CorruptInodeError, blunder.FsError(blunder.Errno(validationErr)).String())
	}

	// Try to fetch from disk, observe that corruption was marked in the headhunter database
	_, ok, corruptionErr := testVolume.fetchOnDiskInode(fileInodeNumber)
	if corruptionErr == nil && ok {
		t.Fatalf("expected not to get inode pointer when fetching presumptively corrupt inode %v", fileInodeNumber)
	}
}

func TestValidateFileExtents(t *testing.T) {
	testVolumeHandle, fileInodeNumber := volumeAndFileInoForTest(t)

	// then let's write some more data into the file so that there will be more segments to verify
	testVolumeHandle.WriteAsSlice(fileInodeNumber, 2, []byte{0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06}, nil)
	testVolumeHandle.Flush(fileInodeNumber, true)
	testVolumeHandle.WriteAsSlice(fileInodeNumber, 12, []byte{0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07}, nil)
	testVolumeHandle.WriteAsSlice(fileInodeNumber, 22, []byte{0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08}, nil)
	testVolumeHandle.Flush(fileInodeNumber, true)
	testVolumeHandle.WriteAsSlice(fileInodeNumber, 32, []byte{0x09, 0x09, 0x09, 0x09, 0x09, 0x09, 0x09}, nil)
	testVolumeHandle.WriteAsSlice(fileInodeNumber, 39, []byte{0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A, 0x0A}, nil)
	testVolumeHandle.WriteAsSlice(fileInodeNumber, 48, []byte{0x0B, 0x0B, 0x0B, 0x0B, 0x0B, 0x0B}, nil)
	testVolumeHandle.Flush(fileInodeNumber, false)

	err := testVolumeHandle.Validate(fileInodeNumber)
	if err != nil {
		t.Fatalf("Validate() failed on presumably-good inode")
	}

	// let's get a read plan and then sneakily sabotage the log segments
	var zero uint64
	readPlan, err := testVolumeHandle.GetReadPlan(fileInodeNumber, &zero, nil)
	if err != nil {
		t.Fatalf("failed to get read plan for inode %v", fileInodeNumber)
	}

	// go delete the LogSegment referenced in the 1st readPlan step (that must exist)

	if 0 == len(readPlan) {
		t.Fatalf("readPlan should have contained at least one entry")
	}

	readPlanStep := readPlan[0]

	deleteErr := swiftclient.ObjectDeleteSync(readPlanStep.AccountName, readPlanStep.ContainerName, readPlanStep.ObjectName)
	if nil != deleteErr {
		t.Fatalf("HTTP DELETE %v should have worked... failed: %v", readPlanStep.ObjectPath, deleteErr)
	}

	err = testVolumeHandle.Validate(fileInodeNumber)
	if err == nil {
		t.Fatalf("expected validation to fail!")
	}
	// TODO: validate error type more rigorously with blunder &c.
	if !strings.Contains(err.Error(), "returned HTTP StatusCode 404") {
		t.Fatalf("expected error to contain 'returned HTTP StatusCode 404'")
	}

}
