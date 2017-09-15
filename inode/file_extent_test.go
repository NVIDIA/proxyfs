package inode

import (
	"bytes"
	cryptoRand "crypto/rand"
	"fmt"
	"math/big"
	mathRand "math/rand"
	"testing"

	"github.com/swiftstack/sortedmap"
)

const (
	sizeOfTestFile      uint64 = 0x01000000 // 16 MiB
	maxExtentLength     uint64 = 0x00001000 //  4 KiB
	numExtentOverwrites uint64 = 0x00001000 //  4 Ki
	numZeroLengthReads  uint64 = 0x00001000 //  4 Ki

	pseudoRandom     = false
	pseudoRandomSeed = int64(0)
)

var (
	randSource                *mathRand.Rand // A source for pseudo-random numbers (if selected)
	inMemoryFileInodeContents []byte         // A value of 0x00 means the byte has never been written
	byteToWrite               = byte(0x01)   // Incremented for each write, wrapping from 0xFF to 0x01 per above
	curExtentOverwrites       = uint64(0)    // During write phase, loop until == numExtentOverwrites
	curFileSize               = uint64(0)    // Tracks the highest offset actually written
)

func testChooseUint64(t *testing.T, mustBeLessThan uint64) (u64 uint64) {
	if pseudoRandom {
		if nil == randSource {
			randSource = mathRand.New(mathRand.NewSource(pseudoRandomSeed))
		}

		u64 = uint64(randSource.Int63n(int64(mustBeLessThan)))
	} else {
		mustBeLessThanBigIntPtr := big.NewInt(int64(mustBeLessThan))
		u64BigIntPtr, err := cryptoRand.Int(cryptoRand.Reader, mustBeLessThanBigIntPtr)
		if nil != err {
			t.Fatalf("rand.Int(rand.Reader, mustBeLessThanBigIntPtr) returned error == \"%v\"", err)
		}

		u64 = u64BigIntPtr.Uint64()
	}

	return
}

func testChooseExtentStart(t *testing.T) (offset uint64) {
	offset = testChooseUint64(t, sizeOfTestFile)

	return
}

func testChooseExtentSize(t *testing.T) (length uint64) {
	length = testChooseUint64(t, maxExtentLength) + 1

	return
}

func testChooseExtent(t *testing.T) (offset uint64, length uint64) {
	offset = testChooseExtentStart(t)
	length = testChooseExtentSize(t)

	if (offset + length) > sizeOfTestFile {
		length = sizeOfTestFile - offset
	}

	return
}

func testPopulateFileInode(t *testing.T, testVolumeHandle VolumeHandle, fileInodeNumber InodeNumber) {
	for curExtentOverwrites < numExtentOverwrites {
		offset, length := testChooseExtent(t)

		overwriteOccurred := false

		for i := offset; i < (offset + length); i++ {
			if byte(0x00) != inMemoryFileInodeContents[i] {
				overwriteOccurred = true
			}

			inMemoryFileInodeContents[i] = byteToWrite
		}
		if overwriteOccurred {
			curExtentOverwrites++
		}

		err := testVolumeHandle.Write(fileInodeNumber, offset, inMemoryFileInodeContents[offset:(offset+length)], nil)
		if nil != err {
			t.Fatalf("Write(fileInodeNumber, offset, inMemoryFileInodeContents[offset:(offset+length)]) failed: %v", err)
		}

		if (offset + length) > curFileSize {
			curFileSize = offset + length
		}

		if byte(0xFF) == byteToWrite {
			byteToWrite = byte(0x01)
		} else {
			byteToWrite++
		}
	}
}

func testValidateFileInodeBPlusTree(t *testing.T, testVolumeHandle VolumeHandle, fileInodeNumber InodeNumber) {
	// Fetch actual fileInode

	fileInode, err := (testVolumeHandle.(*volumeStruct)).fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		t.Fatalf("testVolumeHandle.fetchInodeType(fileInodeNumber, FileType) failed: %v", err)
	}

	// Fetch extents B+Tree

	extents := fileInode.payload.(sortedmap.BPlusTree)
	extentsLen, err := extents.Len()
	if nil != err {
		t.Fatalf("extents.Len() failed: %v", err)
	}

	// Walk extents B+Tree (by index) ensuring coherence with:
	//   1 - All keys (fileOffset) are monotonically increasing
	//   2 - All keys agree with the fileOffset field of their values
	//   3 - All values (extents) don't overlap
	//   4 - GetByKey(fileOffset) returns the same extent

	if 0 == extentsLen {
		return
	}

	var lastExtentTerminationFileOffset uint64

	for extentIndex := 0; extentIndex < extentsLen; extentIndex++ {
		// Fetch current extent
		keyByIndex, valueByIndex, ok, err := extents.GetByIndex(extentIndex)
		if nil != err {
			t.Fatalf("extents.GetByIndex(extentIndex) failed: %v", err)
		}
		if !ok {
			err = fmt.Errorf("extents.GetByIndex(extentIndex) unexpectedly returned !ok")
			t.Fatalf(err.Error())
		}
		fileOffsetByIndex, ok := keyByIndex.(uint64)
		if !ok {
			err = fmt.Errorf("keyByIndex.(uint64) unexpectedly returned !ok")
			t.Fatalf(err.Error())
		}
		extentByIndex, ok := valueByIndex.(*fileExtentStruct)
		if !ok {
			err = fmt.Errorf("valueByIndex.(*fileExtent) unexpectedly returned !ok")
			t.Fatalf(err.Error())
		}

		// Validate extent
		if fileOffsetByIndex != extentByIndex.FileOffset {
			err = fmt.Errorf("fileOffsetByIndex != extentByIndex.fileOffset")
			t.Fatalf(err.Error())
		}
		if 0 == extentByIndex.Length {
			err = fmt.Errorf("0 == extentByIndex.length")
			t.Fatalf(err.Error())
		}

		valueByKey, ok, err := extents.GetByKey(fileOffsetByIndex)
		if nil != err {
			t.Fatalf("extents.GetByKey(fileOffsetByIndex) failed: %v", err)
		}
		if !ok {
			err = fmt.Errorf("extents.GetByKey(fileOffsetByIndex) unexpectedly returned !ok")
			t.Fatalf(err.Error())
		}
		extentByKey, ok := valueByKey.(*fileExtentStruct)
		if !ok {
			err = fmt.Errorf("valueByKey.(*fileExtent) unexpectedly returned !ok")
			t.Fatalf(err.Error())
		}
		if fileOffsetByIndex != extentByKey.FileOffset {
			err = fmt.Errorf("fileOffsetByIndex != extentByKey.fileOffset")
			t.Fatalf(err.Error())
		}

		if 0 < extentIndex {
			// Ensure this extent strictly follows prior extent
			if lastExtentTerminationFileOffset > fileOffsetByIndex {
				err = fmt.Errorf("(lastExtentFileOffset + lastExtentLength) > fileOffsetByIndex")
				t.Fatalf(err.Error())
			}
		}

		// Save this extent's lastExtentTerminationFileOffset for next iteration
		lastExtentTerminationFileOffset = fileOffsetByIndex + extentByIndex.Length
	}

	// If we reach here, extents is valid
}

func testVerifyFileInodeBPlusTree(t *testing.T, testVolumeHandle VolumeHandle, fileInodeNumber InodeNumber) {
	// Fetch actual fileInode

	fileInode, err := (testVolumeHandle.(*volumeStruct)).fetchInodeType(fileInodeNumber, FileType)
	if nil != err {
		t.Fatalf("testVolumeHandle.fetchInodeType(fileInodeNumber, FileType) failed: %v", err)
	}

	// Fetch extents B+Tree

	extents := fileInode.payload.(sortedmap.BPlusTree)
	extentsLen, err := extents.Len()
	if nil != err {
		t.Fatalf("extents.Len() failed: %v", err)
	}

	if 0 == extentsLen {
		// Verify entire inMemoryFileInodeContents is all zeroes
		for _, byteValue := range inMemoryFileInodeContents {
			if 0 != byteValue {
				err = fmt.Errorf("0 != byteValue [case 1]")
				t.Fatalf(err.Error())
			}
		}

		// If we reach here, inMemoryFileInodeContents was all zeroes and (this trivial) extents is verified
		return
	}

	var lastExtentTerminationFileOffset uint64

	for extentIndex := 0; extentIndex < extentsLen; extentIndex++ {
		// Fetch current extent
		key, value, ok, err := extents.GetByIndex(extentIndex)
		if nil != err {
			t.Fatalf("extents.GetByIndex(extentIndex) failed: %v", err)
		}
		if !ok {
			err = fmt.Errorf("extents.GetByIndex(extentIndex) unexpectedly returned !ok")
			t.Fatalf(err.Error())
		}
		fileOffset, ok := key.(uint64)
		if !ok {
			err = fmt.Errorf("key.(uint64) unexpectedly returned !ok")
			t.Fatalf(err.Error())
		}
		extent, ok := value.(*fileExtentStruct)
		if !ok {
			err = fmt.Errorf("value.(*fileExtent) unexpectedly returned !ok")
			t.Fatalf(err.Error())
		}

		// Verify preceeding hole (if any) in extents matches all zeroes in inMemoryFileInodeContents
		for _, byteValue := range inMemoryFileInodeContents[lastExtentTerminationFileOffset:fileOffset] {
			if 0 != byteValue {
				err = fmt.Errorf("0 != byteValue [case 2]")
				t.Fatalf(err.Error())
			}
		}

		// Update lastExtentTerminationFileOffset for next iteration but used for non-zero check below as well
		lastExtentTerminationFileOffset = fileOffset + extent.Length

		// Verify extent matches non-zeroes in inMemoryFileInodeContents
		for _, byteValue := range inMemoryFileInodeContents[fileOffset:lastExtentTerminationFileOffset] {
			if 0 == byteValue {
				err = fmt.Errorf("0 == byteValue")
				t.Fatalf(err.Error())
			}
		}
	}

	// Verify inMemoryFileInodeContents agrees that lastExtentTerminationFileOffset is EOF
	for _, byteValue := range inMemoryFileInodeContents[lastExtentTerminationFileOffset:] {
		if 0 != byteValue {
			err = fmt.Errorf("0 != byteValue [case 3]")
			t.Fatalf(err.Error())
		}
	}

	// If we reach here, extents is verified
}

func testCondenseByteSlice(buf []byte) (condensedString string) {
	type countValueTupleStruct struct {
		count uint64
		value byte
	}

	var countValueTupleSlice []*countValueTupleStruct

	for _, value := range buf {
		countValueTupleSliceLen := len(countValueTupleSlice)
		if (0 == countValueTupleSliceLen) || (value != countValueTupleSlice[countValueTupleSliceLen-1].value) {
			countValueTupleSlice = append(countValueTupleSlice, &countValueTupleStruct{count: 1, value: value})
		} else {
			countValueTupleSlice[countValueTupleSliceLen-1].count++
		}
	}

	var condensedByteSlice []byte

	condensedByteSlice = append(condensedByteSlice, '[')

	for countValueTupleIndex, countValueTuple := range countValueTupleSlice {
		if 0 < countValueTupleIndex {
			condensedByteSlice = append(condensedByteSlice, ',', ' ')
		}

		valueAsString := fmt.Sprintf("0x%02x", countValueTuple.value)

		if 1 == countValueTuple.count {
			condensedByteSlice = append(condensedByteSlice, []byte(valueAsString)...)
		} else {
			countAsString := fmt.Sprintf("0x%x", countValueTuple.count)
			condensedByteSlice = append(condensedByteSlice, []byte(countAsString+"("+valueAsString+")")...)
		}
	}

	condensedByteSlice = append(condensedByteSlice, ']')

	condensedString = string(condensedByteSlice[:])

	return
}

func testVerifyFileInodeContents(t *testing.T, testVolumeHandle VolumeHandle, fileInodeNumber InodeNumber) {
	// Verify written sections of fileInode match inMemoryFileInodeContents (i.e. non-0x00 bytes all match)

	offset := uint64(0)
	length := uint64(0)
	currentlyScanningWrittenBytes := false

	for (offset + length) < curFileSize {
		if currentlyScanningWrittenBytes {
			if byte(0x00) == inMemoryFileInodeContents[offset+length] {
				readBuf, err := testVolumeHandle.Read(fileInodeNumber, offset, length, nil)
				if nil != err {
					t.Fatalf("Read(fileInodeNumber, offset, length) [case 1] failed: %v", err)
				}

				if bytes.Compare(readBuf, inMemoryFileInodeContents[offset:(offset+length)]) != 0 {
					t.Fatalf("Read(fileInodeNumber, offset, length) [case 1] returned unexpected []byte:\n  expected %v\n       got %v", testCondenseByteSlice(inMemoryFileInodeContents[offset:(offset+length)]), testCondenseByteSlice(readBuf))
				}

				offset += length
				length = uint64(0)
				currentlyScanningWrittenBytes = false
			} else {
				length++
			}
		} else {
			if byte(0x00) == inMemoryFileInodeContents[offset+length] {
				offset++
			} else {
				length = uint64(1)
				currentlyScanningWrittenBytes = true
			}
		}
	}

	if currentlyScanningWrittenBytes {
		readBuf, err := testVolumeHandle.Read(fileInodeNumber, offset, length, nil)
		if nil != err {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 2] failed: %v", err)
		}

		if 0 != bytes.Compare(readBuf, inMemoryFileInodeContents[offset:(offset+length)]) {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 2] returned unexpected []byte:\n  expected %v\n       got %v", testCondenseByteSlice(inMemoryFileInodeContents[offset:(offset+length)]), testCondenseByteSlice(readBuf))
		}
	}

	// Walk through entire fileInode verifying entire inMemoryFileInodeContents

	offset = uint64(0)

	for offset < curFileSize {
		length = testChooseExtentSize(t)

		if offset+length > curFileSize {
			length = curFileSize - offset
		}

		readBuf, err := testVolumeHandle.Read(fileInodeNumber, offset, length, nil)
		if nil != err {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 3] failed: %v", err)
		}

		if 0 != bytes.Compare(readBuf, inMemoryFileInodeContents[offset:(offset+length)]) {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 3] returned unexpected []byte:\n  expected %v\n       got %v", testCondenseByteSlice(inMemoryFileInodeContents[offset:(offset+length)]), testCondenseByteSlice(readBuf))
		}

		offset += length
	}

	// Issue numZeroLengthReads zero-length reads using both Read() & GetReadPlan() for likely valid offsets

	length = uint64(0)

	for i := uint64(0); i < numZeroLengthReads; i++ {
		offset = testChooseExtentStart(t)

		readBuf, err := testVolumeHandle.Read(fileInodeNumber, offset, length, nil)
		if nil != err {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 4] failed: %v", err)
		}

		if 0 != len(readBuf) {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 4] returned unexpected []byte:\n  expected %v\n       got %v", testCondenseByteSlice(inMemoryFileInodeContents[offset:(offset+length)]), testCondenseByteSlice(readBuf))
		}

		readPlan, err := testVolumeHandle.GetReadPlan(fileInodeNumber, &offset, &length)
		if nil != err {
			t.Fatalf("GetReadPlan(fileInodeNumber, offset, length) [case 4] failed: %v", err)
		}

		if 0 != len(readPlan) {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 4] returned unexpected readPlan of length %v", len(readPlan))
		}
	}

	// Issue numZeroLengthReads zero-length reads using both Read() & GetReadPlan() for definitely invalid offsets

	length = uint64(0)

	for i := uint64(0); i < numZeroLengthReads; i++ {
		offset = testChooseExtentStart(t) + sizeOfTestFile

		readBuf, err := testVolumeHandle.Read(fileInodeNumber, offset, length, nil)
		if nil != err {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 5] failed: %v", err)
		}

		if 0 != len(readBuf) {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 5] returned unexpected []byte:\n  expected %v\n       got %v", testCondenseByteSlice(inMemoryFileInodeContents[offset:(offset+length)]), testCondenseByteSlice(readBuf))
		}

		readPlan, err := testVolumeHandle.GetReadPlan(fileInodeNumber, &offset, &length)
		if nil != err {
			t.Fatalf("GetReadPlan(fileInodeNumber, offset, length) [case 5] failed: %v", err)
		}

		if 0 != len(readPlan) {
			t.Fatalf("Read(fileInodeNumber, offset, length) [case 5] returned unexpected readPlan of length %v", len(readPlan))
		}
	}
}

func TestFileExtents(t *testing.T) {
	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") should have worked - got error: %v", err)
	}

	fileInodeNumber, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	inMemoryFileInodeContents = make([]byte, sizeOfTestFile) // Initially all 0x00... meaning no bytes written

	testPopulateFileInode(t, testVolumeHandle, fileInodeNumber)

	testValidateFileInodeBPlusTree(t, testVolumeHandle, fileInodeNumber)

	testVerifyFileInodeBPlusTree(t, testVolumeHandle, fileInodeNumber)

	// One might expect to be able to call `testVerifyFileInodeContents` here,
	// but we haven't flushed yet.

	err = testVolumeHandle.Flush(fileInodeNumber, false)
	if nil != err {
		t.Fatalf("Flush(fileInodeNumber, false) failed: %v", err)
	}

	testVerifyFileInodeContents(t, testVolumeHandle, fileInodeNumber)

	err = testVolumeHandle.Purge(fileInodeNumber)
	if nil != err {
		t.Fatalf("Purge(fileInodeNumber) [case one] failed: %v", err)
	}

	testVerifyFileInodeContents(t, testVolumeHandle, fileInodeNumber)

	err = testVolumeHandle.Purge(fileInodeNumber)
	if nil != err {
		t.Fatalf("Purge(fileInodeNumber) [case two] failed: %v", err)
	}

	err = testVolumeHandle.Destroy(fileInodeNumber)
	if nil != err {
		t.Fatalf("Destroy(fileInodeNumber) failed: %v", err)
	}
}

func TestWriteFileExtentAtExtantOffset(t *testing.T) {
	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") should have worked - got error: %v", err)
	}

	fileInodeNumber, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	fileInode, err := (testVolumeHandle.(*volumeStruct)).fetchInode(fileInodeNumber)
	if err != nil {
		t.Fatalf("testVolumeHandle.fetchInode() failed: %v", err)
	}
	if fileInode.InodeType == FreeType {
		t.Fatalf("testVolumeHandle.fetchInode() returned a free inode")
	}

	extents := fileInode.payload.(sortedmap.BPlusTree)

	err = testVolumeHandle.Write(fileInodeNumber, 0, make([]byte, 20), nil)
	if nil != err {
		t.Fatalf("Write(fileInodeNumber, 0, make([]byte, 20)) failed: %v", err)
	}

	err = testVolumeHandle.Write(fileInodeNumber, 5, []byte("aaaa"), nil) // 4 bytes
	if nil != err {
		t.Fatalf("Write failed: %v", err)
	}

	// At this point, our file B+-tree should have three extents starting at
	// file offsets 0, 5, and 5+4=9.

	expectedOffsets := []uint64{0, 5, 9}
	expectedLengths := []uint64{5, 4, 11}
	for i := 0; i < 3; i++ {
		_, value, ok, err := extents.GetByIndex(i)
		if nil != err {
			t.Fatal(err)
		}
		extantExtent := value.(*fileExtentStruct)
		if !ok {
			t.Fatalf("expected to be able to get extent")
		}
		if expectedOffsets[i] != extantExtent.FileOffset {
			t.Fatalf("expected extent to be at offset %v, got %v", expectedOffsets[i], extantExtent.FileOffset)
		}
		if expectedLengths[i] != extantExtent.Length {
			t.Fatalf("expected extent length %v, got %v", expectedLengths[i], extantExtent.Length)
		}
	}

	err = testVolumeHandle.Write(fileInodeNumber, 9, []byte("bbb"), nil)
	if nil != err {
		t.Fatalf("Overwrite failed: %v", err)
	}

}

func TestOverwriteIncludesBeginningOfLastExtent(t *testing.T) {
	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") should have worked - got error: %v", err)
	}

	fileInodeNumber, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	err = testVolumeHandle.Write(fileInodeNumber, 0, make([]byte, 20), nil)
	if nil != err {
		t.Fatalf("Write(fileInodeNumber, 0, make([]byte, 20)) failed: %v", err)
	}

	err = testVolumeHandle.Write(fileInodeNumber, 5, []byte("aaaa"), nil) // 4 bytes
	if nil != err {
		t.Fatalf("Write failed: %v", err)
	}

	err = testVolumeHandle.Write(fileInodeNumber, 3, []byte("bbbbbbbbbb"), nil)
	if nil != err {
		t.Fatalf("Write failed: %v", err)
	}

}

func TestReadYourWrite(t *testing.T) {
	testVolumeHandle, err := FetchVolumeHandle("TestVolume")
	if nil != err {
		t.Fatalf("FetchVolumeHandle(\"TestVolume\") should have worked - got error: %v", err)
	}

	fileInodeNumber, err := testVolumeHandle.CreateFile(PosixModePerm, 0, 0)
	if nil != err {
		t.Fatalf("CreateFile() failed: %v", err)
	}

	ourBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	err = testVolumeHandle.Write(fileInodeNumber, 0, ourBytes, nil)
	if nil != err {
		t.Fatalf("Write(fileInodeNumber, 0, []byte{1, 2, 3, 4, 5, 6, 7, 8}) failed: %v", err)
	}
	readBuf, err := testVolumeHandle.Read(fileInodeNumber, 0, 8, nil)
	if err != nil {
		t.Fatalf("Read(fileInodeNumber, 0, 8) failed: %v", err)
	}

	if bytes.Compare(ourBytes, readBuf) != 0 {
		t.Fatalf("read after write didn't work: expected %v, got %v", ourBytes, readBuf)
	}
}
