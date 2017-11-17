package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/utils"
)

var (
	displayUpdateInterval      time.Duration
	filePathPrefix             string
	fileSize                   uint64
	maxExtentSize              uint64
	minExtentSize              uint64
	numExtentsToWriteInTotal   uint64
	numExtentsToWritePerFile   uint32
	numExtentWritesPerFlush    uint32
	numExtentWritesPerValidate uint32
	numExtentsWrittenInTotal   uint64
	numFiles                   uint16
	waitGroup                  sync.WaitGroup
)

func main() {
	var (
		args                               []string
		confMap                            conf.ConfMap
		displayUpdateIntervalAsString      string
		displayUpdateIntervalPad           string
		err                                error
		filePathPrefixPad                  string
		fileSizeAsString                   string
		fileSizePad                        string
		maxExtentSizeAsString              string
		maxExtentSizePad                   string
		maxParameterStringLen              int
		minExtentSizeAsString              string
		minExtentSizePad                   string
		numExtentsToWritePerFileAsString   string
		numExtentsToWritePerFilePad        string
		numExtentWritesPerFlushAsString    string
		numExtentWritesPerFlushPad         string
		numExtentWritesPerValidateAsString string
		numExtentWritesPerValidatePad      string
		numFilesAsString                   string
		numFilesPad                        string
		progressPercentage                 uint64
		stresserIndex                      uint16
	)

	// Parse arguments

	args = os.Args[1:]

	// Read in the program's os.Arg[1]-specified (and required) .conf file
	if 0 == len(args) {
		log.Fatalf("no .conf file specified")
	}

	confMap, err = conf.MakeConfMapFromFile(args[0])
	if nil != err {
		log.Fatalf("failed to load config: %v", err)
	}

	// Update confMap with any extra os.Args supplied
	err = confMap.UpdateFromStrings(args[1:])
	if nil != err {
		log.Fatalf("failed to load config overrides: %v", err)
	}

	// TODO: Remove call to utils.AdjustConfSectionNamespacingAsNecessary() when appropriate
	err = utils.AdjustConfSectionNamespacingAsNecessary(confMap)
	if nil != err {
		log.Fatalf("utils.AdjustConfSectionNamespacingAsNecessary() failed: %v", err)
	}

	// Process resultant confMap

	filePathPrefix, err = confMap.FetchOptionValueString("StressParameters", "FilePathPrefix")
	if nil != err {
		log.Fatal(err)
	}

	numFiles, err = confMap.FetchOptionValueUint16("StressParameters", "NumFiles")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == numFiles {
		log.Fatalf("NumFiles must be > 0")
	}

	fileSize, err = confMap.FetchOptionValueUint64("StressParameters", "FileSize")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == fileSize {
		log.Fatalf("FileSize must be > 0")
	}
	if fileSize > math.MaxInt64 {
		log.Fatalf("FileSize(%v) must be <= math.MaxInt64(%v)", fileSize, math.MaxInt64)
	}

	minExtentSize, err = confMap.FetchOptionValueUint64("StressParameters", "MinExtentSize")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == minExtentSize {
		log.Fatalf("MinExtentSize must be > 0")
	}
	if minExtentSize > fileSize {
		log.Fatalf("MinExtentSize(%v) must be <= FileSize(%v)", minExtentSize, fileSize)
	}

	maxExtentSize, err = confMap.FetchOptionValueUint64("StressParameters", "MaxExtentSize")
	if nil != err {
		log.Fatal(err)
	}
	if maxExtentSize < minExtentSize {
		log.Fatalf("MaxExtentSize(%v) must be >= MinExtentSize(%v)", maxExtentSize, minExtentSize)
	}
	if maxExtentSize > fileSize {
		log.Fatalf("MaxExtentSize(%v) must be <= FileSize(%v)", maxExtentSize, fileSize)
	}

	numExtentsToWritePerFile, err = confMap.FetchOptionValueUint32("StressParameters", "NumExtentsToWritePerFile")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == numExtentsToWritePerFile {
		log.Fatalf("NumExtentsToWritePerFile must be > 0")
	}

	numExtentWritesPerFlush, err = confMap.FetchOptionValueUint32("StressParameters", "NumExtentWritesPerFlush")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == numExtentWritesPerFlush {
		numExtentWritesPerFlush = numExtentsToWritePerFile
	}

	numExtentWritesPerValidate, err = confMap.FetchOptionValueUint32("StressParameters", "NumExtentWritesPerValidate")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == numExtentWritesPerValidate {
		numExtentWritesPerValidate = numExtentsToWritePerFile
	}

	displayUpdateInterval, err = confMap.FetchOptionValueDuration("StressParameters", "DisplayUpdateInterval")
	if nil != err {
		log.Fatal(err)
	}

	// Display parameters

	numFilesAsString = fmt.Sprintf("%d", numFiles)
	fileSizeAsString = fmt.Sprintf("%d", fileSize)
	minExtentSizeAsString = fmt.Sprintf("%d", minExtentSize)
	maxExtentSizeAsString = fmt.Sprintf("%d", maxExtentSize)
	numExtentsToWritePerFileAsString = fmt.Sprintf("%d", numExtentsToWritePerFile)
	numExtentWritesPerFlushAsString = fmt.Sprintf("%d", numExtentWritesPerFlush)
	numExtentWritesPerValidateAsString = fmt.Sprintf("%d", numExtentWritesPerValidate)
	displayUpdateIntervalAsString = fmt.Sprintf("%s", displayUpdateInterval)

	maxParameterStringLen = len(filePathPrefix)
	if len(numFilesAsString) > maxParameterStringLen {
		maxParameterStringLen = len(numFilesAsString)
	}
	if len(fileSizeAsString) > maxParameterStringLen {
		maxParameterStringLen = len(fileSizeAsString)
	}
	if len(minExtentSizeAsString) > maxParameterStringLen {
		maxParameterStringLen = len(minExtentSizeAsString)
	}
	if len(maxExtentSizeAsString) > maxParameterStringLen {
		maxParameterStringLen = len(maxExtentSizeAsString)
	}
	if len(numExtentsToWritePerFileAsString) > maxParameterStringLen {
		maxParameterStringLen = len(numExtentsToWritePerFileAsString)
	}
	if len(numExtentWritesPerFlushAsString) > maxParameterStringLen {
		maxParameterStringLen = len(numExtentWritesPerFlushAsString)
	}
	if len(numExtentWritesPerValidateAsString) > maxParameterStringLen {
		maxParameterStringLen = len(numExtentWritesPerValidateAsString)
	}
	if len(displayUpdateIntervalAsString) > maxParameterStringLen {
		maxParameterStringLen = len(displayUpdateIntervalAsString)
	}

	filePathPrefixPad = strings.Repeat(" ", maxParameterStringLen-len(filePathPrefix))
	numFilesPad = strings.Repeat(" ", maxParameterStringLen-len(numFilesAsString))
	fileSizePad = strings.Repeat(" ", maxParameterStringLen-len(fileSizeAsString))
	minExtentSizePad = strings.Repeat(" ", maxParameterStringLen-len(minExtentSizeAsString))
	maxExtentSizePad = strings.Repeat(" ", maxParameterStringLen-len(maxExtentSizeAsString))
	numExtentsToWritePerFilePad = strings.Repeat(" ", maxParameterStringLen-len(numExtentsToWritePerFileAsString))
	numExtentWritesPerFlushPad = strings.Repeat(" ", maxParameterStringLen-len(numExtentWritesPerFlushAsString))
	numExtentWritesPerValidatePad = strings.Repeat(" ", maxParameterStringLen-len(numExtentWritesPerValidateAsString))
	displayUpdateIntervalPad = strings.Repeat(" ", maxParameterStringLen-len(displayUpdateIntervalAsString))

	fmt.Println("[StressParameters]")
	fmt.Printf("FilePathPrefix:             %s%s\n", filePathPrefixPad, filePathPrefix)
	fmt.Printf("NumFiles:                   %s%s\n", numFilesPad, numFilesAsString)
	fmt.Printf("FileSize:                   %s%s\n", fileSizePad, fileSizeAsString)
	fmt.Printf("MinExtentSize:              %s%s\n", minExtentSizePad, minExtentSizeAsString)
	fmt.Printf("MaxExtentSize:              %s%s\n", maxExtentSizePad, maxExtentSizeAsString)
	fmt.Printf("NumExtentsToWritePerFile:   %s%s\n", numExtentsToWritePerFilePad, numExtentsToWritePerFileAsString)
	fmt.Printf("NumExtentWritesPerFlush:    %s%s\n", numExtentWritesPerFlushPad, numExtentWritesPerFlushAsString)
	fmt.Printf("NumExtentWritesPerValidate: %s%s\n", numExtentWritesPerValidatePad, numExtentWritesPerValidateAsString)
	fmt.Printf("DisplayUpdateInterval:      %s%s\n", displayUpdateIntervalPad, displayUpdateIntervalAsString)

	// Setup monitoring parameters

	numExtentsToWriteInTotal = uint64(numFiles) * uint64(numExtentsToWritePerFile)
	numExtentsWrittenInTotal = uint64(0)

	// Launch fileStresser goroutines

	waitGroup.Add(int(numFiles))

	for stresserIndex = 0; stresserIndex < numFiles; stresserIndex++ {
		go fileStresser(stresserIndex)
	}

	// Monitor fileStresser goroutines

	for {
		time.Sleep(displayUpdateInterval)
		progressPercentage = 100 * atomic.LoadUint64(&numExtentsWrittenInTotal) / numExtentsToWriteInTotal
		fmt.Printf("\rProgress: %3d%%", progressPercentage)
		if 100 == progressPercentage {
			break
		}
	}

	waitGroup.Wait()

	fmt.Println("... done!")
}

type fileStresserContext struct {
	filePath string
	file     *os.File
	written  []byte
}

func fileStresser(stresserIndex uint16) {
	var (
		b                                uint8
		err                              error
		extentIndex                      uint32
		fSC                              *fileStresserContext
		l                                int64
		mustBeLessThanBigIntPtr          *big.Int
		numExtentWritesSinceLastFlush    uint32
		numExtentWritesSinceLastValidate uint32
		off                              int64
		u64BigIntPtr                     *big.Int
	)

	// Construct this instance's fileStresserContext

	fSC = &fileStresserContext{
		filePath: fmt.Sprintf("%s%04X", filePathPrefix, stresserIndex),
		written:  make([]byte, fileSize),
	}

	fSC.file, err = os.OpenFile(fSC.filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if nil != err {
		log.Fatal(err)
	}

	fSC.fileStresserWriteAt(int64(0), int64(fileSize), 0x00)
	fSC.fileStresserValidate()

	// Perform extent writes

	b = 0x00
	numExtentWritesSinceLastFlush = 0
	numExtentWritesSinceLastValidate = 0

	for extentIndex = 0; extentIndex < numExtentsToWritePerFile; extentIndex++ {
		// Pick an l value such that minExtentSize <= l <= maxExtentSize
		mustBeLessThanBigIntPtr = big.NewInt(int64(maxExtentSize - minExtentSize + 1))
		u64BigIntPtr, err = rand.Int(rand.Reader, mustBeLessThanBigIntPtr)
		if nil != err {
			log.Fatal(err)
		}
		l = int64(minExtentSize) + u64BigIntPtr.Int64()

		// Pick an off value such that 0 <= off <= (fileSize - l)
		mustBeLessThanBigIntPtr = big.NewInt(int64(int64(fileSize) - l))
		u64BigIntPtr, err = rand.Int(rand.Reader, mustBeLessThanBigIntPtr)
		if nil != err {
			log.Fatal(err)
		}
		off = u64BigIntPtr.Int64()

		// Pick next b value (skipping 0x00 for as-yet-un-over-written bytes)
		b++
		if 0x00 == b {
			b = 0x01
		}

		fSC.fileStresserWriteAt(off, l, b)

		numExtentWritesSinceLastFlush++

		if numExtentWritesPerFlush == numExtentWritesSinceLastFlush {
			fSC.fileStresserFlush()
			numExtentWritesSinceLastFlush = 0
		}

		numExtentWritesSinceLastValidate++

		if numExtentWritesPerValidate == numExtentWritesSinceLastValidate {
			fSC.fileStresserValidate()
			numExtentWritesSinceLastValidate = 0
		}

		atomic.AddUint64(&numExtentsWrittenInTotal, uint64(1))
	}

	// Do one final fileStresserFlush() call if necessary to flush final writes

	if 0 < numExtentWritesSinceLastFlush {
		fSC.fileStresserFlush()
	}

	// Do one final fileStresserValidate() call if necessary to validate final writes

	if 0 < numExtentWritesSinceLastValidate {
		fSC.fileStresserValidate()
	}

	// Clean up and exit

	err = fSC.file.Close()
	if nil != err {
		log.Fatal(err)
	}
	err = os.Remove(fSC.filePath)
	if nil != err {
		log.Fatal(err)
	}

	waitGroup.Done()
}

func (fSC *fileStresserContext) fileStresserWriteAt(off int64, l int64, b byte) {
	buf := make([]byte, l)
	for i := int64(0); i < l; i++ {
		buf[i] = b
		fSC.written[off+i] = b
	}
	_, err := fSC.file.WriteAt(buf, off)
	if nil != err {
		log.Fatal(err)
	}
}

func (fSC *fileStresserContext) fileStresserFlush() {
	err := fSC.file.Sync()
	if nil != err {
		log.Fatal(err)
	}
}

func (fSC *fileStresserContext) fileStresserValidate() {
	buf := make([]byte, fileSize)
	_, err := fSC.file.ReadAt(buf, int64(0))
	if nil != err {
		log.Fatal(err)
	}
	if 0 != bytes.Compare(fSC.written, buf) {
		log.Fatalf("Miscompare in filePath %s\n", fSC.filePath)
	}
}
