// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/NVIDIA/fission"
)

const (
	testInodeStressDisplayUpdateInterval             = time.Duration(time.Second)
	testInodeStressFileNamePrefix                    = "_inode_stress_"
	testInodeStressFileSize                   uint64 = 100 // UNDO 1000000
	testInodeStressMaxExtentSize              uint64 = 4   // UNDO 10000
	testInodeStressMinExtentSize              uint64 = 1
	testInodeStressNumExtentWritesPerFile     uint64 = 100 // UNDO 1000
	testInodeStressNumExtentWritesPerFlush    uint64 = 1   // UNDO 50 // 0 means only perform Flush    function at the end
	testInodeStressNumExtentWritesPerValidate uint64 = 1   // UNDO 100 // 0 means only perform Validate function at the end
	testInodeStressNumFiles                   uint64 = 1   // UNDO 10
)

type testInodeStressGlobalsStruct struct {
	sync.Mutex                 // Protects err
	sync.WaitGroup             // Signaled by each retiring testInodeStresser
	numExtentWritesPerFlush    uint64
	numExtentWritesPerValidate uint64
	numExtentsToWriteInTotal   uint64
	numExtentsWrittenInTotal   uint64
	err                        []error
}

var testInodeStressGlobals *testInodeStressGlobalsStruct

func TestInodeStress(t *testing.T) {
	var (
		err                error
		fissionErrChan     chan error
		progressPercentage uint64
		stresserIndex      uint64
	)

	// Perform baseline setup

	testSetup(t)

	fissionErrChan = make(chan error, 1)

	err = initializeGlobals(testGlobals.confMap, fissionErrChan)
	if nil != err {
		t.Fatalf("initializeGlobals(testGlobals.confMap, fissionErrChan) failed: %v", err)
	}

	err = startRPCHandler()
	if nil != err {
		t.Fatalf("startRPCHandler() failed: %v", err)
	}

	// Setup testInodeStressGlobals

	testInodeStressGlobals = &testInodeStressGlobalsStruct{
		err: make([]error, 0, testInodeStressNumFiles),
	}

	if testInodeStressNumExtentWritesPerFlush == 0 {
		testInodeStressGlobals.numExtentWritesPerFlush = testInodeStressNumExtentWritesPerFile
	} else {
		testInodeStressGlobals.numExtentWritesPerFlush = testInodeStressNumExtentWritesPerFlush
	}

	if testInodeStressNumExtentWritesPerFlush == 0 {
		testInodeStressGlobals.numExtentWritesPerValidate = testInodeStressNumExtentWritesPerFile
	} else {
		testInodeStressGlobals.numExtentWritesPerValidate = testInodeStressNumExtentWritesPerFlush
	}

	testInodeStressGlobals.numExtentsToWriteInTotal = testInodeStressNumFiles * testInodeStressNumExtentWritesPerFile
	testInodeStressGlobals.numExtentsWrittenInTotal = 0

	// Launch InodeStresser goroutines

	testInodeStressGlobals.Add(int(testInodeStressNumFiles))

	for stresserIndex = 0; stresserIndex < testInodeStressNumFiles; stresserIndex++ {
		go testInodeStresser(stresserIndex)
	}

	// Monitor InodeStresser goroutines

	for {
		time.Sleep(testInodeStressDisplayUpdateInterval)
		progressPercentage = 100 * atomic.LoadUint64(&testInodeStressGlobals.numExtentsWrittenInTotal) / testInodeStressGlobals.numExtentsToWriteInTotal
		testInodeStressGlobals.Lock()
		if len(testInodeStressGlobals.err) > 0 {
			for _, err = range testInodeStressGlobals.err {
				t.Error(err)
			}
			t.Fatalf("...exiting...")
		}
		testInodeStressGlobals.Unlock()
		t.Logf("Progress: %3d%%", progressPercentage)
		if progressPercentage == 100 {
			break
		}
	}

	// Wait for goroutines to cleanly exit

	testInodeStressGlobals.Wait()

	// Cleanly shutdown

	err = stopRPCHandler()
	if nil != err {
		t.Fatalf("stopRPCHandler() failed: %v", err)
	}

	err = uninitializeGlobals()
	if nil != err {
		t.Fatalf("uninitializeGlobals() failed: %v", err)
	}

	testTeardown(t)
}

type testInodeStresserContext struct {
	fileName string
	NodeID   uint64
	FH       uint64
	written  []byte
}

func testInodeStresser(stresserIndex uint64) {
	var (
		b                                byte
		createIn                         *fission.CreateIn
		createOut                        *fission.CreateOut
		err                              error
		errno                            syscall.Errno
		extentIndex                      uint64
		inHeader                         *fission.InHeader
		mustBeLessThanBigIntPtr          *big.Int
		numExtentWritesSinceLastFlush    uint64
		numExtentWritesSinceLastValidate uint64
		offset                           uint64
		size                             uint32
		releaseIn                        *fission.ReleaseIn
		tISC                             *testInodeStresserContext
		u64BigIntPtr                     *big.Int
		unlinkIn                         *fission.UnlinkIn
	)

	// Construct this instance's testInodeStresserContext

	tISC = &testInodeStresserContext{
		fileName: fmt.Sprintf("%s%04X", testInodeStressFileNamePrefix, stresserIndex),
		written:  make([]byte, testInodeStressFileSize),
	}

	inHeader = &fission.InHeader{
		NodeID: 1,
		UID:    0,
		GID:    0,
	}
	createIn = &fission.CreateIn{
		Mode:  0600,
		UMask: 0666,
		Name:  []byte(tISC.fileName),
	}

	createOut, errno = globals.DoCreate(inHeader, createIn)
	if errno != 0 {
		testInodeStressGlobals.Lock()
		testInodeStressGlobals.err = append(testInodeStressGlobals.err, fmt.Errorf("globals.DoCreate(inHeader, createIn) returned errno: %v", errno))
		testInodeStressGlobals.Unlock()

		testInodeStressGlobals.Done()

		runtime.Goexit()
	}

	tISC.NodeID = createOut.EntryOut.NodeID
	tISC.FH = createOut.FH

	err = tISC.writeAt(uint64(0), uint32(testInodeStressFileSize), 0x00)
	if err != nil {
		testInodeStressGlobals.Lock()
		testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
		testInodeStressGlobals.Unlock()

		testInodeStressGlobals.Done()

		runtime.Goexit()
	}

	err = tISC.validate()
	if err != nil {
		testInodeStressGlobals.Lock()
		testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
		testInodeStressGlobals.Unlock()

		testInodeStressGlobals.Done()

		runtime.Goexit()
	}

	// Perform extent writes

	b = 0x00
	numExtentWritesSinceLastFlush = 0
	numExtentWritesSinceLastValidate = 0

	for extentIndex = 0; extentIndex < testInodeStressNumExtentWritesPerFile; extentIndex++ {
		// Pick a size value such that testInodeStressMinExtentSize <= size <= testInodeStressMaxExtentSize

		mustBeLessThanBigIntPtr = big.NewInt(int64(testInodeStressMaxExtentSize - testInodeStressMinExtentSize + 1))
		u64BigIntPtr, err = rand.Int(rand.Reader, mustBeLessThanBigIntPtr)
		if err != nil {
			testInodeStressGlobals.Lock()
			testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
			testInodeStressGlobals.Unlock()

			testInodeStressGlobals.Done()

			runtime.Goexit()
		}

		size = uint32(testInodeStressMinExtentSize + u64BigIntPtr.Uint64())

		// Pick an offset value such that 0 <= offset <= (testInodeStressFileSize - size)

		mustBeLessThanBigIntPtr = big.NewInt(int64(testInodeStressFileSize) - int64(size))
		u64BigIntPtr, err = rand.Int(rand.Reader, mustBeLessThanBigIntPtr)
		if err != nil {
			testInodeStressGlobals.Lock()
			testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
			testInodeStressGlobals.Unlock()

			testInodeStressGlobals.Done()

			runtime.Goexit()
		}

		offset = u64BigIntPtr.Uint64()

		// Pick next b value (skipping 0x00 for as-yet-un-over-written bytes)

		b++
		if b == 0x00 {
			b = 0x01
		}

		// Now perform the selected overwrite

		err = tISC.writeAt(offset, size, b)
		if err != nil {
			testInodeStressGlobals.Lock()
			testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
			testInodeStressGlobals.Unlock()

			testInodeStressGlobals.Done()

			runtime.Goexit()
		}

		// Perform a flush if required

		numExtentWritesSinceLastFlush++

		if testInodeStressNumExtentWritesPerFlush == numExtentWritesSinceLastFlush {
			err = tISC.flush()
			if err != nil {
				testInodeStressGlobals.Lock()
				testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
				testInodeStressGlobals.Unlock()

				testInodeStressGlobals.Done()

				runtime.Goexit()
			}

			numExtentWritesSinceLastFlush = 0
		}

		// Perform a validate if required

		numExtentWritesSinceLastValidate++

		if testInodeStressNumExtentWritesPerValidate == numExtentWritesSinceLastValidate {
			err = tISC.validate()
			if err != nil {
				testInodeStressGlobals.Lock()
				testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
				testInodeStressGlobals.Unlock()

				testInodeStressGlobals.Done()

				runtime.Goexit()
			}

			numExtentWritesSinceLastValidate = 0
		}

		// Finally, update testInodeStressGlobals.numExtentsWrittenInTotal

		testInodeStressGlobals.Lock()
		testInodeStressGlobals.numExtentsWrittenInTotal++
		testInodeStressGlobals.Unlock()
	}

	// Do one final tISC.flush call if necessary to flush final writes

	if 0 < numExtentWritesSinceLastFlush {
		err = tISC.flush()
		if err != nil {
			testInodeStressGlobals.Lock()
			testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
			testInodeStressGlobals.Unlock()

			testInodeStressGlobals.Done()

			runtime.Goexit()
		}
	}

	// Do one final tISC.validate call if necessary to validate final writes

	if 0 < numExtentWritesSinceLastValidate {
		err = tISC.validate()
		if err != nil {
			testInodeStressGlobals.Lock()
			testInodeStressGlobals.err = append(testInodeStressGlobals.err, err)
			testInodeStressGlobals.Unlock()

			testInodeStressGlobals.Done()

			runtime.Goexit()
		}
	}

	// Clean up and exit

	inHeader = &fission.InHeader{
		NodeID: tISC.NodeID,
		UID:    0,
		GID:    0,
	}
	releaseIn = &fission.ReleaseIn{
		FH: tISC.FH,
	}

	errno = globals.DoRelease(inHeader, releaseIn)
	if errno != 0 {
		testInodeStressGlobals.Lock()
		testInodeStressGlobals.err = append(testInodeStressGlobals.err, fmt.Errorf("globals.DoRelease(inHeader, releaseIn) returned errno: %v", errno))
		testInodeStressGlobals.Unlock()

		testInodeStressGlobals.Done()

		runtime.Goexit()
	}

	inHeader = &fission.InHeader{
		NodeID: 1,
		UID:    0,
		GID:    0,
	}
	unlinkIn = &fission.UnlinkIn{
		Name: []byte(tISC.fileName),
	}

	errno = globals.DoUnlink(inHeader, unlinkIn)
	if errno != 0 {
		testInodeStressGlobals.Lock()
		testInodeStressGlobals.err = append(testInodeStressGlobals.err, fmt.Errorf("globals.DoUnlink(inHeader, unlinkIn) returned errno: %v", errno))
		testInodeStressGlobals.Unlock()

		testInodeStressGlobals.Done()

		runtime.Goexit()
	}

	testInodeStressGlobals.Done()
}

func (tISC *testInodeStresserContext) writeAt(offset uint64, size uint32, b byte) (err error) {
	var (
		errno    syscall.Errno
		i        uint32
		inHeader *fission.InHeader
		writeIn  *fission.WriteIn
	)

	inHeader = &fission.InHeader{
		NodeID: tISC.NodeID,
	}
	writeIn = &fission.WriteIn{
		FH:     tISC.FH,
		Offset: offset,
		Size:   size,
		Data:   make([]byte, size),
	}

	for i = 0; i < size; i++ {
		writeIn.Data[i] = b
		tISC.written[offset+uint64(i)] = b
	}

	_, errno = globals.DoWrite(inHeader, writeIn)
	if errno == 0 {
		err = nil
	} else {
		err = fmt.Errorf("globals.DoWrite(inHeader, writeIn) returned errno: %v", errno)
	}

	return
}

func (tISC *testInodeStresserContext) flush() (err error) {
	var (
		errno    syscall.Errno
		inHeader *fission.InHeader
		flushIn  *fission.FlushIn
	)

	inHeader = &fission.InHeader{
		NodeID: tISC.NodeID,
	}
	flushIn = &fission.FlushIn{
		FH: tISC.FH,
	}

	errno = globals.DoFlush(inHeader, flushIn)
	if errno == 0 {
		err = nil
	} else {
		err = fmt.Errorf("globals.DoFlush(inHeader, fSyncIn) returned errno: %v", errno)
	}

	return
}

func (tISC *testInodeStresserContext) validate() (err error) {
	var (
		errno    syscall.Errno
		i        int
		inHeader *fission.InHeader
		readIn   *fission.ReadIn
		readOut  *fission.ReadOut
	)

	inHeader = &fission.InHeader{
		NodeID: tISC.NodeID,
	}
	readIn = &fission.ReadIn{
		FH:     tISC.FH,
		Offset: 0,
		Size:   uint32(testInodeStressFileSize),
	}

	readOut, errno = globals.DoRead(inHeader, readIn)
	if errno == 0 {
		if bytes.Equal(tISC.written, readOut.Data) {
			err = nil
		} else {
			err = fmt.Errorf("Miscompare in fileName %s\n", tISC.fileName)
			for i = range tISC.written {
				if tISC.written[i] != readOut.Data[i] {
					err = fmt.Errorf("First miscompare in %s at position %v", tISC.fileName, i)
					return
				}
			}
		}
	} else {
		err = fmt.Errorf("globals.DoRead(inHeader, readIn) returned errno: %v", errno)
	}

	return
}
