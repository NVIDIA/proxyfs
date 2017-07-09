package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swiftstack/conf"
)

var (
	displayUpdateInterval      time.Duration
	fileNamePrefix             string
	fileSize                   uint64
	maxExtentSize              uint64
	minExtentSize              uint64
	numExtentsToWriteInTotal   uint64
	numExtentsToWritePerFile   uint32
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
		fileNamePrefixPad                  string
		fileSizeAsString                   string
		fileSizePad                        string
		maxExtentSizeAsString              string
		maxExtentSizePad                   string
		maxParameterStringLen              int
		minExtentSizeAsString              string
		minExtentSizePad                   string
		numExtentsToWritePerFileAsString   string
		numExtentsToWritePerFilePad        string
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

	// Process resultant confMap

	fileNamePrefix, err = confMap.FetchOptionValueString("Parameters", "FileNamePrefix")
	if nil != err {
		log.Fatal(err)
	}

	numFiles, err = confMap.FetchOptionValueUint16("Parameters", "NumFiles")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == numFiles {
		log.Fatalf("NumFiles must be > 0")
	}

	fileSize, err = confMap.FetchOptionValueUint64("Parameters", "FileSize")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == numFiles {
		log.Fatalf("FileSize must be > 0")
	}

	minExtentSize, err = confMap.FetchOptionValueUint64("Parameters", "MinExtentSize")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == minExtentSize {
		log.Fatalf("MinExtentSize must be > 0")
	}
	if minExtentSize > fileSize {
		log.Fatalf("MinExtentSize(%v) must be <= FileSize(%v)", minExtentSize, fileSize)
	}

	maxExtentSize, err = confMap.FetchOptionValueUint64("Parameters", "MaxExtentSize")
	if nil != err {
		log.Fatal(err)
	}
	if maxExtentSize < minExtentSize {
		log.Fatalf("MaxExtentSize(%v) must be >= MinExtentSize(%v)", maxExtentSize, minExtentSize)
	}
	if maxExtentSize > fileSize {
		log.Fatalf("MaxExtentSize(%v) must be <= FileSize(%v)", maxExtentSize, fileSize)
	}

	numExtentsToWritePerFile, err = confMap.FetchOptionValueUint32("Parameters", "NumExtentsToWritePerFile")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == numExtentsToWritePerFile {
		log.Fatalf("NumExtentsToWritePerFile must be > 0")
	}

	numExtentWritesPerValidate, err = confMap.FetchOptionValueUint32("Parameters", "NumExtentWritesPerValidate")
	if nil != err {
		log.Fatal(err)
	}
	if 0 == numExtentWritesPerValidate {
		numExtentWritesPerValidate = numExtentsToWritePerFile
	}

	displayUpdateInterval, err = confMap.FetchOptionValueDuration("Parameters", "DisplayUpdateInterval")
	if nil != err {
		log.Fatal(err)
	}

	// Display parameters

	numFilesAsString = fmt.Sprintf("%d", numFiles)
	fileSizeAsString = fmt.Sprintf("0x%016X", fileSize)
	minExtentSizeAsString = fmt.Sprintf("0x%016X", minExtentSize)
	maxExtentSizeAsString = fmt.Sprintf("0x%016X", maxExtentSize)
	numExtentsToWritePerFileAsString = fmt.Sprintf("%d", numExtentsToWritePerFile)
	numExtentWritesPerValidateAsString = fmt.Sprintf("%d", numExtentWritesPerValidate)
	displayUpdateIntervalAsString = fmt.Sprintf("%s", displayUpdateInterval)

	maxParameterStringLen = len(fileNamePrefix)
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
	if len(numExtentWritesPerValidateAsString) > maxParameterStringLen {
		maxParameterStringLen = len(numExtentWritesPerValidateAsString)
	}
	if len(displayUpdateIntervalAsString) > maxParameterStringLen {
		maxParameterStringLen = len(displayUpdateIntervalAsString)
	}

	fileNamePrefixPad = strings.Repeat(" ", maxParameterStringLen-len(fileNamePrefix))
	numFilesPad = strings.Repeat(" ", maxParameterStringLen-len(numFilesAsString))
	fileSizePad = strings.Repeat(" ", maxParameterStringLen-len(fileSizeAsString))
	minExtentSizePad = strings.Repeat(" ", maxParameterStringLen-len(minExtentSizeAsString))
	maxExtentSizePad = strings.Repeat(" ", maxParameterStringLen-len(maxExtentSizeAsString))
	numExtentsToWritePerFilePad = strings.Repeat(" ", maxParameterStringLen-len(numExtentsToWritePerFileAsString))
	numExtentWritesPerValidatePad = strings.Repeat(" ", maxParameterStringLen-len(numExtentWritesPerValidateAsString))
	displayUpdateIntervalPad = strings.Repeat(" ", maxParameterStringLen-len(displayUpdateIntervalAsString))

	fmt.Println("[Parameters]")
	fmt.Printf("FileNamePrefix:             %s%s\n", fileNamePrefixPad, fileNamePrefix)
	fmt.Printf("NumFiles:                   %s%s\n", numFilesPad, numFilesAsString)
	fmt.Printf("FileSize:                   %s%s\n", fileSizePad, fileSizeAsString)
	fmt.Printf("MinExtentSize:              %s%s\n", minExtentSizePad, minExtentSizeAsString)
	fmt.Printf("MaxExtentSize:              %s%s\n", maxExtentSizePad, maxExtentSizeAsString)
	fmt.Printf("NumExtentsToWritePerFile:   %s%s\n", numExtentsToWritePerFilePad, numExtentsToWritePerFileAsString)
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

func fileStresser(stresserIndex uint16) {
	// TODO: For now, just act like we are done immediately :-)
	atomic.AddUint64(&numExtentsWrittenInTotal, uint64(numExtentsToWritePerFile))
	waitGroup.Done()
}
