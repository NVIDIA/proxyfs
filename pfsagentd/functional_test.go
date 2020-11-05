package main

import (
	"bytes"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"testing"
)

var (
	testFile         *os.File
	testFileContents []byte
	testFilePath     string
	testFileLastByte byte
)

func TestSimpleWriteReadClose(t *testing.T) {
	const (
		testFileName       = "testSimpleWriteReadCloseFileName"
		totalBytes   int64 = 100
		randSeed     int64 = 0x0123456789ABCDEF
	)
	var (
		err         error
		readBackBuf []byte
		writtenBuf  []byte
	)

	testSetup(t)

	testFilePath = globals.config.FUSEMountPointPath + "/" + testFileName

	testFile, err = os.Create(testFilePath)
	if nil != err {
		t.Fatalf("os.Create(\"%s\") failed: %v", testFilePath, err)
	}

	writtenBuf = make([]byte, totalBytes)

	rand.Seed(randSeed)

	_, _ = rand.Read(writtenBuf)

	_, err = testFile.WriteAt(writtenBuf, 0)
	if nil != err {
		t.Fatalf("testFile.WriteAt(writtenBuf, 0) failed: %v", err)
	}

	readBackBuf = make([]byte, totalBytes)

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) after Sync() failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's")
	}

	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() failed: %v", err)
	}

	err = os.Remove(testFilePath)
	if nil != err {
		t.Fatalf("os.Remove(testFilePath) failed: %v", err)
	}

	testTeardown(t)
}

func TestRandomOverwrites(t *testing.T) {
	const (
		testFileName       = "testRandomeOverwritesFileName"
		totalBytes   int64 = 1000
		minWriteSize int64 = 1
		maxWriteSize int64 = 10
		numWrites    int64 = 10000
		randSeed     int64 = 0x0123456789ABCDEF
	)
	var (
		err           error
		readBackBuf   []byte
		toWriteBuf    []byte
		toWriteBufLen int64
		toWriteOffset int64
		writeNumber   int64
		writtenBuf    []byte
	)

	testSetup(t)

	testFilePath = globals.config.FUSEMountPointPath + "/" + testFileName

	testFile, err = os.Create(testFilePath)
	if nil != err {
		t.Fatalf("os.Create(\"%s\") failed: %v", testFilePath, err)
	}

	writtenBuf = make([]byte, totalBytes)

	rand.Seed(randSeed)

	_, _ = rand.Read(writtenBuf)

	_, err = testFile.WriteAt(writtenBuf, 0)
	if nil != err {
		t.Fatalf("testFile.WriteAt(writtenBuf, 0) failed: %v", err)
	}

	toWriteBuf = make([]byte, maxWriteSize)

	for writeNumber = 0; writeNumber < numWrites; writeNumber++ {
		toWriteBufLen = minWriteSize + rand.Int63n(maxWriteSize-minWriteSize+1)
		toWriteOffset = rand.Int63n(totalBytes - toWriteBufLen + 1)
		_, _ = rand.Read(toWriteBuf[:toWriteBufLen])

		_, err = testFile.WriteAt(toWriteBuf[:toWriteBufLen], toWriteOffset)
		if nil != err {
			t.Fatalf("testFile.WriteAt(toWriteBuf[:toWriteBufLen], toWriteOffset) after Create() failed: %v", err)
		}

		_ = copy(writtenBuf[toWriteOffset:(toWriteOffset+toWriteBufLen)], toWriteBuf[:toWriteBufLen])
	}

	readBackBuf = make([]byte, totalBytes)

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) before Sync() failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's before Sync()")
	}

	err = testFile.Sync()
	if nil != err {
		t.Fatalf("testFile.Sync() after write pass 1 failed: %v", err)
	}

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) after Sync() failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's after Sync()")
	}

	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() after Sync() failed: %v", err)
	}

	testFile, err = os.OpenFile(testFilePath, os.O_RDWR, 0)
	if nil != err {
		t.Fatalf("os.OpenFile(\"%s\",,) failed: %v", testFilePath, err)
	}

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) after Open() failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's after Sync()")
	}

	for writeNumber = 0; writeNumber < numWrites; writeNumber++ {
		toWriteBufLen = minWriteSize + rand.Int63n(maxWriteSize-minWriteSize+1)
		toWriteOffset = rand.Int63n(totalBytes - toWriteBufLen + 1)
		_, _ = rand.Read(toWriteBuf[:toWriteBufLen])

		_, err = testFile.WriteAt(toWriteBuf[:toWriteBufLen], toWriteOffset)
		if nil != err {
			t.Fatalf("testFile.WriteAt(toWriteBuf[:toWriteBufLen], toWriteOffset) after Open() failed: %v", err)
		}

		_ = copy(writtenBuf[toWriteOffset:(toWriteOffset+toWriteBufLen)], toWriteBuf[:toWriteBufLen])
	}

	_, err = testFile.ReadAt(readBackBuf, 0)
	if nil != err {
		t.Fatalf("testFile.ReadAt(readBackBuf, 0) after overwrite failed: %v", err)
	}

	if 0 != bytes.Compare(writtenBuf, readBackBuf) {
		t.Fatalf("bytes.Compare(writtenBuf, readBackBuf) reports un-equal buf's after overwrite")
	}

	err = testFile.Sync()
	if nil != err {
		t.Fatalf("testFile.Sync() after write pass 2 failed: %v", err)
	}

	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() after Open() failed: %v", err)
	}

	err = os.Remove(testFilePath)
	if nil != err {
		t.Fatalf("os.Remove(testFilePath) failed: %v", err)
	}

	testTeardown(t)
}

func optionallyReopenTestFile(t *testing.T, prefix string, reopenBeforeEachWrite bool, step string) {
	var (
		err error
	)

	err = testFile.Close()
	if nil != err {
		t.Fatalf("%s: testFile.Close() before \"%s\" failed: %v", prefix, step, err)
	}

	testFile, err = os.OpenFile(testFilePath, os.O_RDWR, 0)
	if nil != err {
		t.Fatalf("%s: os.OpenFile(\"%s\",,) before \"%s\" failed: %v", prefix, testFilePath, step, err)
	}
}

func writeNextByteSlice(t *testing.T, prefix string, offset int, toWriteLen int, step string) {
	var (
		err        error
		extension  int
		index      int
		toWriteBuf []byte
	)

	extension = (offset + toWriteLen) - len(testFileContents)

	if 0 < extension {
		testFileContents = append(testFileContents, make([]byte, extension)...)
	}

	toWriteBuf = make([]byte, toWriteLen)

	for index = 0; index < toWriteLen; index++ {
		if math.MaxUint8 == testFileLastByte {
			testFileLastByte = 1
		} else {
			testFileLastByte++
		}
		toWriteBuf[index] = testFileLastByte
		testFileContents[offset+index] = testFileLastByte
	}

	_, err = testFile.WriteAt(toWriteBuf, int64(offset))
	if nil != err {
		t.Fatalf("%s: testFile.WriteAt(,0) for \"%s\" failed: %v", prefix, step, err)
	}
}

func optionallyFlushTestFile(t *testing.T, prefix string, flushAfterEachWrite bool, step string) {
	var (
		err error
	)

	if flushAfterEachWrite {
		err = testFile.Sync()
		if nil != err {
			t.Fatalf("%s: testFile.Sync() for \"%s\" failed: %v", prefix, step, err)
		}
	}
}

func verifyTestFileContents(t *testing.T, prefix string, step string) {
	var (
		err         error
		readBackBuf []byte
	)

	_, err = testFile.Seek(0, os.SEEK_SET)
	if nil != err {
		t.Fatalf("%s: testFile.Seek(0, os.SEEK_SET) for \"%s\" failed: %v", prefix, step, err)
	}
	readBackBuf, err = ioutil.ReadAll(testFile)
	if nil != err {
		t.Fatalf("%s: ioutil.ReadAll(testFile) for \"%s\" failed: %v", prefix, step, err)
	}
	if 0 != bytes.Compare(readBackBuf, testFileContents) {
		t.Fatalf("%s: bytes.Compare(readBackBuf, testFileContents) for \"%s\" reports unequal bufs", prefix, step)
	}
}

func testExhaustiveOverwrites(t *testing.T, prefix string, reopenBeforeEachWrite bool, flushAfterEachWrite bool) {
	// Non-overlapping extent

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Non-overlapping extent")
	writeNextByteSlice(t, prefix, 0, 3, "Non-overlapping extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Non-overlapping extent")
	verifyTestFileContents(t, prefix, "Non-overlapping extent")

	// Overlapping existing extent precisely

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping existing extent precisely")
	writeNextByteSlice(t, prefix, 0, 3, "Overlapping existing extent precisely")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping existing extent precisely")
	verifyTestFileContents(t, prefix, "Overlapping existing extent precisely")

	// Overlapping the right of an existing extent

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping the right of an existing extent")
	writeNextByteSlice(t, prefix, 2, 3, "Overlapping the right of an existing extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping the right of an existing extent")
	verifyTestFileContents(t, prefix, "Overlapping the right of an existing extent")

	// Overlapping the middle of an existing extent

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping the middle of an existing extent")
	writeNextByteSlice(t, prefix, 1, 3, "Overlapping the middle of an existing extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping the middle of an existing extent")
	verifyTestFileContents(t, prefix, "Overlapping the middle of an existing extent")

	// Overlapping the left of an existing extent

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping the left of an existing extent")
	writeNextByteSlice(t, prefix, 8, 3, "Overlapping the left of an existing extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping the left of an existing extent")
	verifyTestFileContents(t, prefix, "Overlapping the left of an existing extent")
	writeNextByteSlice(t, prefix, 6, 3, "Overlapping the left of an existing extent")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping the left of an existing extent")
	verifyTestFileContents(t, prefix, "Overlapping the left of an existing extent")

	// Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left

	optionallyReopenTestFile(t, prefix, reopenBeforeEachWrite, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	writeNextByteSlice(t, prefix, 12, 3, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	verifyTestFileContents(t, prefix, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	writeNextByteSlice(t, prefix, 1, 13, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	optionallyFlushTestFile(t, prefix, flushAfterEachWrite, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
	verifyTestFileContents(t, prefix, "Overlapping one existing extent on the right, another existing extent entirely, & a 3rd existing extent on the left")
}

func TestExhaustiveOverwrites(t *testing.T) {
	const (
		testFileName = "testExhaustiveOverwritesFileName"
	)
	var (
		err error
	)

	testSetup(t)

	testFilePath = globals.config.FUSEMountPointPath + "/" + testFileName

	testFile, err = os.Create(testFilePath)
	if nil != err {
		t.Fatalf("os.Create(\"%s\") failed: %v", testFilePath, err)
	}
	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() failed: %v", err)
	}
	testFile, err = os.OpenFile(testFilePath, os.O_RDWR, 0)
	if nil != err {
		t.Fatalf("os.OpenFile(\"%s\",,) failed: %v", testFilePath, err)
	}

	testFileContents = []byte{}

	testExhaustiveOverwrites(t, "Phase1", false, false)
	testExhaustiveOverwrites(t, "Phase2", false, true)
	testExhaustiveOverwrites(t, "Phase2", true, false)
	testExhaustiveOverwrites(t, "Phase2", true, true)

	err = testFile.Close()
	if nil != err {
		t.Fatalf("testFile.Close() failed: %v", err)
	}

	err = os.Remove(testFilePath)
	if nil != err {
		t.Fatalf("os.Remove(testFilePath) failed: %v", err)
	}

	testTeardown(t)
}
