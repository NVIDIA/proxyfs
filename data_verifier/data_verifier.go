package main

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	var (
		tgtDir      string
		refFile     string
		numThreads  int
		ioSize      int
		flushSize   int
		syncOnWrite bool
	)

	flag.StringVar(&tgtDir, "tgtDir", "", "Target Directory for writing test files")
	flag.StringVar(&refFile, "refFile", "", "Reference file for reading test data")
	flag.IntVar(&numThreads, "numThreads", 1, "Number of writer threads")
	flag.BoolVar(&syncOnWrite, "sync", false, "Sync After Every write")
	flag.IntVar(&ioSize, "ioSize", 65536, "IO Size for reading and writing")
	flag.IntVar(&flushSize, "flushSize", 1048576, "Outstanding data before explicit flush")

	flag.Parse()

	if tgtDir == "" {
		fmt.Printf("Target Directory for writing test files must be specified\n")
		return
	}

	if refFile == "" {
		fmt.Printf("Reference File for reading data must be specified\n")
		return
	}

	f, err := os.Open(refFile)
	if err != nil {
		fmt.Printf("Unable to open reference file: %s %v\n", refFile, err)
	}

	f.Close()

	openFlags := os.O_CREATE | os.O_RDWR | os.O_TRUNC
	if syncOnWrite {
		openFlags |= os.O_SYNC
	}

	var wg sync.WaitGroup

	fmt.Printf("Starting data verfier test : number of threads - %v\n", numThreads)
	for idx := 0; idx < numThreads; idx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer fmt.Printf("Exiting thread %v\n", idx)

			buf := make([]byte, ioSize)
			fmt.Printf("Thread - Starting write phase %v\n", idx)
			inF, err := os.Open(refFile)
			if err != nil {
				fmt.Printf("Thread idx %v failed to open ref file %s %v exiting..\n", idx, refFile, err)
				return
			}

			outPath := tgtDir + "/" + strconv.Itoa(idx)
			outF, err := os.OpenFile(outPath, openFlags, os.ModePerm)
			if err != nil {
				fmt.Printf("Thread idx %v failed to create out file %s %v exiting.. \n", idx, outPath, err)
				return
			}

			inFstat, err := inF.Stat()
			var readDuration, writeDuration, syncDuration time.Duration
			var timeStart time.Time

			for i := int64(0); i < inFstat.Size(); i += int64(ioSize) {
				reqSize := int64(math.Min(float64(inFstat.Size()-i), float64(ioSize)))
				_, err = inF.Read(buf[:reqSize])
				if err != nil {
					fmt.Printf("Thread idx %v failed to read ref file %v at offset %v err %v\n", idx, refFile, i, err)
					return
				}

				timeStart = time.Now()
				_, err = outF.Write(buf[:reqSize])
				if err != nil {
					fmt.Printf("Thread idx %v failed to write to tgt file %v at offset %v err %v\n", idx, outPath, i, err)
					return
				}
				writeDuration += time.Since(timeStart)

				if (int64(i)+reqSize)%int64(flushSize) == 0 {
					timeStart = time.Now()
					err = outF.Sync()
					if err != nil {
						fmt.Printf("Thread idx %v failed to sync tgt file %v offset %v err %v\n", idx, outPath, i, err)
						return
					}

					syncDuration = time.Since(timeStart)
				}

			}

			timeStart = time.Now()
			err = outF.Sync()
			if err != nil {
				fmt.Printf("Thread idx %v failed to sync tgt file %v before closing, err %v\n", idx, outPath, err)
				return
			}
			syncDuration += time.Since(timeStart)
			err = inF.Close()
			if err != nil {
				fmt.Printf("Thread idx %v failed to close the reference file - err %v\n", idx, err)
				return
			}

			err = outF.Close()
			if err != nil {
				fmt.Printf("Thread idx %v failed to close the file - err %v\n", idx, err)
			}

			fmt.Printf("Thread %v starting verify phase\n", idx)
			inF, err = os.Open(refFile)
			if err != nil {
				fmt.Printf("Thread idx %v failed to open ref file %s %v exiting..\n", idx, refFile, err)
				return
			}

			outF, err = os.Open(outPath)
			if err != nil {
				fmt.Printf("Thread idx %v failed to create out file %s %v exiting.. \n", idx, outPath, err)
				return
			}

			refBuf := make([]byte, ioSize)

			for i := int64(0); i < inFstat.Size(); i += int64(ioSize) {
				reqSize := int64(math.Min(float64(inFstat.Size()-i), float64(ioSize)))
				_, err = inF.Read(buf[:reqSize])
				if err != nil {
					fmt.Printf("Thread idx %v failed to read ref file %v at offset %v err %v\n", idx, refFile, i, err)
					return
				}

				timeStart = time.Now()
				_, err = outF.Read(refBuf[:reqSize])
				if err != nil {
					fmt.Printf("Thread idx %v failed to read tgt file %v at offset %v err %v\n", idx, outPath, i, err)
					return
				}

				readDuration += time.Since(timeStart)

				if bytes.Compare(buf[:reqSize], refBuf[:reqSize]) != 0 {
					fmt.Printf("Data mismatch between %v and %v in range %v to %v\n", refFile, outPath, i, i+reqSize)
					return
				}
			}

			inF.Close()
			outF.Close()

			fmt.Printf("Thread: %v Write time %v Sync time %v Read time %v File Size %v\n", idx, writeDuration, syncDuration, readDuration, inFstat.Size())
		}(idx)
	}

	wg.Wait()

	fmt.Printf("Completed data verifier test\n")
}
