package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/fuse"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

const (
	basenamePrefix = "__pfsworkout_"
)

type rwTimesStruct struct {
	writeDuration time.Duration
	readDuration  time.Duration
}

type rwSizeEachStruct struct {
	name             string
	KiB              uint64
	dirTimes         rwTimesStruct
	fuseTimes        rwTimesStruct
	fsTimes          rwTimesStruct
	inodeTimes       rwTimesStruct
	swiftclientTimes rwTimesStruct
	MountHandle      fs.MountHandle    // Only used if all threads use same file
	FileInodeNumber  inode.InodeNumber // Only used if all threads use same file
	ObjectPath       string            // Only used if all threads use same object
}

var (
	dirPath                string
	doNextStepChan         chan bool
	mountPointName         string
	mutex                  sync.Mutex
	rwSizeTotal            uint64
	stepErrChan            chan error
	volumeList             []string
	volumeName             string
	headhunterVolumeHandle headhunter.VolumeHandle
)

func usage(file *os.File) {
	fmt.Fprintf(file, "Usage:\n")
	fmt.Fprintf(file, "    %v d threads rw-size-in-mb dir-path\n", os.Args[0])
	fmt.Fprintf(file, "    %v [mfisru] threads rw-size-in-mb conf-file [section.option=value]*\n", os.Args[0])
	fmt.Fprintf(file, "  where:\n")
	fmt.Fprintf(file, "    d                       run tests against specified target directory\n")
	fmt.Fprintf(file, "    m                       run tests against FUSE mount point\n")
	fmt.Fprintf(file, "    f                       run tests against package fs\n")
	fmt.Fprintf(file, "    i                       run tests against package inode\n")
	fmt.Fprintf(file, "    s                       run tests against package swiftclient\n")
	fmt.Fprintf(file, "    r                       run tests with random I/O instead of sequential\n")
	fmt.Fprintf(file, "    u                       run multiple readers/writers on same file against packages\n")
	fmt.Fprintf(file, "    threads                 number of threads (currently must be '1')\n")
	fmt.Fprintf(file, "    rw-size-in-mb           number of MiB per thread per test case\n")
	fmt.Fprintf(file, "    dir-path                target directory\n")
	fmt.Fprintf(file, "    conf-file               input to conf.MakeConfMapFromFile()\n")
	fmt.Fprintf(file, "    [section.option=value]* optional input to conf.UpdateFromStrings()\n")
	fmt.Fprintf(file, "\n")
	fmt.Fprintf(file, "Note: At least one of f, i, or s must be specified\n")
	fmt.Fprintf(file, "\n")
	fmt.Fprintf(file, "The default is a sequential test on a different file per thread.\n")
	fmt.Fprintf(file, "    r specifies that the I/O is random for fs and inodee packages instead of sequential.\n")
	fmt.Fprintf(file, "    u specifies that all threads operate on the same file.\n")
}

func main() {
	var (
		confMap conf.ConfMap

		proxyfsRequired = false
		fsRequired      = false
		fuseRequired    = false

		doDirWorkout         = false
		doFuseWorkout        = false
		doFsWorkout          = false
		doInodeWorkout       = false
		doSwiftclientWorkout = false
		doSameFile           = false
		doRandomIO           = false

		timeBeforeWrites time.Time
		timeAfterWrites  time.Time
		timeBeforeReads  time.Time
		timeAfterReads   time.Time

		bandwidthNumerator float64

		rwSizeEachArray = [...]*rwSizeEachStruct{
			&rwSizeEachStruct{name: " 4 KiB", KiB: 4},
			&rwSizeEachStruct{name: " 8 KiB", KiB: 8},
			&rwSizeEachStruct{name: "16 KiB", KiB: 16},
			&rwSizeEachStruct{name: "32 KiB", KiB: 32},
			&rwSizeEachStruct{name: "64 KiB", KiB: 64},
		}
	)

	// Parse arguments

	if 5 > len(os.Args) {
		usage(os.Stderr)
		os.Exit(1)
	}

	for _, workoutSelector := range os.Args[1] {
		switch workoutSelector {
		case 'd':
			doDirWorkout = true
		case 'm':
			proxyfsRequired = true
			fsRequired = true
			fuseRequired = true

			doFuseWorkout = true
		case 'f':
			proxyfsRequired = true
			fsRequired = true

			doFsWorkout = true
		case 'i':
			proxyfsRequired = true
			doInodeWorkout = true
		case 's':
			proxyfsRequired = true
			doSwiftclientWorkout = true
		case 'r':
			proxyfsRequired = true
			fsRequired = true
			doRandomIO = true
		case 'u':
			proxyfsRequired = true
			fsRequired = true
			doSameFile = true
		default:
			fmt.Fprintf(os.Stderr, "workoutSelector ('%v') must be one of 'd', 'm', 'f', 'i', or 's'\n", string(workoutSelector))
			os.Exit(1)
		}
	}

	if doDirWorkout {
		if doFuseWorkout || doFsWorkout || doInodeWorkout || doSwiftclientWorkout {
			fmt.Fprintf(os.Stderr, "workoutSelectors cannot include both 'd' and any of 'm', 'f', 'i', or 's'\n")
			os.Exit(1)
		}
	} else {
		if !(doFuseWorkout || doFsWorkout || doInodeWorkout || doSwiftclientWorkout) {
			fmt.Fprintf(os.Stderr, "workoutSelectors must include at least one of 'm', 'f', 'i', or 's' when 'd' is not selected")
			os.Exit(1)
		}
	}

	threads, err := strconv.ParseUint(os.Args[2], 10, 64)
	if nil != err {
		fmt.Fprintf(os.Stderr, "strconv.ParseUint(\"%v\", 10, 64) failed: %v\n", os.Args[2], err)
		os.Exit(1)
	}
	if 0 == threads {
		fmt.Fprintf(os.Stderr, "threads must be a positive number\n")
		os.Exit(1)
	}

	rwSizeTotalMiB, err := strconv.ParseUint(os.Args[3], 10, 64)
	if nil != err {
		fmt.Fprintf(os.Stderr, "strconv.ParseUint(\"%v\", 10, 64) failed: %v\n", os.Args[3], err)
		os.Exit(1)
	}

	rwSizeTotal = rwSizeTotalMiB * 1024 * 1024

	if doDirWorkout {
		dirPath = os.Args[4]
	} else {
		confMap, err = conf.MakeConfMapFromFile(os.Args[4])
		if nil != err {
			fmt.Fprintf(os.Stderr, "conf.MakeConfMapFromFile(\"%v\") failed: %v\n", os.Args[4], err)
			os.Exit(1)
		}

		if 5 < len(os.Args) {
			err = confMap.UpdateFromStrings(os.Args[5:])
			if nil != err {
				fmt.Fprintf(os.Stderr, "confMap.UpdateFromStrings(%#v) failed: %v\n", os.Args[5:], err)
				os.Exit(1)
			}
		}

		// TODO: Remove call to utils.AdjustConfSectionNamespacingAsNecessary() when appropriate
		err = utils.AdjustConfSectionNamespacingAsNecessary(confMap)
		if nil != err {
			fmt.Fprintf(os.Stderr, "utils.AdjustConfSectionNamespacingAsNecessary() failed: %v\n", err)
			os.Exit(1)
		}

		volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
		if nil != err {
			fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v\n", err)
			os.Exit(1)
		}
		if 1 > len(volumeList) {
			fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") returned empty volumeList")
			os.Exit(1)
		}
	}

	if proxyfsRequired {
		// Start up needed ProxyFS components

		err = logger.Up(confMap)
		if nil != err {
			fmt.Fprintf(os.Stderr, "logger.Up() failed: %v\n", err)
			os.Exit(1)
		}

		err = stats.Up(confMap)
		if nil != err {
			fmt.Fprintf(os.Stderr, "stats.Up() failed: %v\n", err)
			os.Exit(1)
		}

		err = dlm.Up(confMap)
		if nil != err {
			fmt.Fprintf(os.Stderr, "dlm.Up() failed: %v\n", err)
			os.Exit(1)
		}

		err = swiftclient.Up(confMap)
		if nil != err {
			fmt.Fprintf(os.Stderr, "swiftclient.Up() failed: %v\n", err)
			os.Exit(1)
		}

		err = headhunter.Up(confMap)
		if nil != err {
			fmt.Fprintf(os.Stderr, "headhunter.Up() failed: %v\n", err)
			os.Exit(1)
		}

		err = inode.Up(confMap)
		if nil != err {
			fmt.Fprintf(os.Stderr, "inode.Up() failed: %v\n", err)
			os.Exit(1)
		}

		if fsRequired {
			err = fs.Up(confMap)
			if nil != err {
				fmt.Fprintf(os.Stderr, "fs.Up() failed: %v\n", err)
				os.Exit(1)
			}

			if fuseRequired {
				err = fuse.Up(confMap)
				if nil != err {
					fmt.Fprintf(os.Stderr, "fuse.Up() failed: %v\n", err)
					os.Exit(1)
				}
			}
		}
	}

	// Select first "active" volumeName in volumeList by attempting to mount each (if required)

	if doFuseWorkout || doFsWorkout || doInodeWorkout || doSwiftclientWorkout {
		for _, volumeName = range volumeList {
			headhunterVolumeHandle, err = headhunter.FetchVolumeHandle(volumeName)
			if nil == err {
				break
			} else {
				headhunterVolumeHandle = nil
			}
		}

		if nil == headhunterVolumeHandle {
			fmt.Fprintf(os.Stderr, "headhunter.FetchVolumeHandle() failed on every volumeName in volumeList: %v\n", volumeList)
			os.Exit(1)
		}

		mountPointName, err = confMap.FetchOptionValueString(volumeName, "FUSEMountPointName")
		if nil != err {
			fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueString(\"%v\", \"FUSEMountPointName\") failed: %v\n", volumeName, err)
			os.Exit(1)
		}
	}

	// Perform tests

	stepErrChan = make(chan error, threads)
	doNextStepChan = make(chan bool, threads)

	if doDirWorkout {
		for _, rwSizeEach := range rwSizeEachArray {
			// Do initialization step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				go dirWorkout(rwSizeEach, threadIndex)
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "dirWorkout() initialization step returned: %v\n", err)
					os.Exit(1)
				}
			}
			// Do writes step
			timeBeforeWrites = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "dirWorkout() write step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterWrites = time.Now()
			// Do reads step
			timeBeforeReads = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "dirWorkout() read step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterReads = time.Now()
			// Do shutdown step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "dirWorkout() shutdown step returned: %v\n", err)
					os.Exit(1)
				}
			}

			rwSizeEach.dirTimes.writeDuration = timeAfterWrites.Sub(timeBeforeWrites)
			rwSizeEach.dirTimes.readDuration = timeAfterReads.Sub(timeBeforeReads)
		}
	}

	if doFuseWorkout {
		for _, rwSizeEach := range rwSizeEachArray {
			// Do initialization step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				go fuseWorkout(rwSizeEach, threadIndex)
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "fuseWorkout() initialization step returned: %v\n", err)
					os.Exit(1)
				}
			}
			// Do writes step
			timeBeforeWrites = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "fuseWorkout() write step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterWrites = time.Now()
			// Do reads step
			timeBeforeReads = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "fuseWorkout() read step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterReads = time.Now()
			// Do shutdown step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "fuseWorkout() shutdown step returned: %v\n", err)
					os.Exit(1)
				}
			}

			rwSizeEach.fuseTimes.writeDuration = timeAfterWrites.Sub(timeBeforeWrites)
			rwSizeEach.fuseTimes.readDuration = timeAfterReads.Sub(timeBeforeReads)
		}
	}

	if doFsWorkout {
		for _, rwSizeEach := range rwSizeEachArray {
			var fileName string

			// If we are doing the operations on the same file for all threads, create the file now.
			if doSameFile {
				// Save off MountID and FileInodeNumber in rwSizeEach since all threads need this
				err, rwSizeEach.MountHandle, rwSizeEach.FileInodeNumber, fileName = createFsFile()
				if nil != err {
					// In an error, no point in continuing.  Just break from this for loop.
					break
				}
			}

			// Do initialization step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				go fsWorkout(rwSizeEach, threadIndex, doSameFile, doRandomIO)
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "fsWorkout() initialization step returned: %v\n", err)
					os.Exit(1)
				}
			}
			// Do writes step
			timeBeforeWrites = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "fsWorkout() write step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterWrites = time.Now()
			// Do reads step
			timeBeforeReads = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "fsWorkout() read step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterReads = time.Now()
			// Do shutdown step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "fsWorkout() shutdown step returned: %v\n", err)
					os.Exit(1)
				}
			}

			// Remove file if all threads used same file
			if doSameFile {
				_ = unlinkFsFile(rwSizeEach.MountHandle, fileName)
			}

			rwSizeEach.fsTimes.writeDuration = timeAfterWrites.Sub(timeBeforeWrites)
			rwSizeEach.fsTimes.readDuration = timeAfterReads.Sub(timeBeforeReads)
		}
	}

	if doInodeWorkout {
		for _, rwSizeEach := range rwSizeEachArray {
			// If we are doing the operations on the same object for all threads, create the object now.
			if doSameFile {
				err, rwSizeEach.FileInodeNumber = createInode()
				if nil != err {
					// In an error, no point in continuing.  Just break from this for loop.
					break
				}
			}

			// Do initialization step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				go inodeWorkout(rwSizeEach, threadIndex, doSameFile, doRandomIO)
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "inodeWorkout() initialization step returned: %v\n", err)
					os.Exit(1)
				}
			}
			// Do writes step
			timeBeforeWrites = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "inodeWorkout() write step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterWrites = time.Now()
			// Do reads step
			timeBeforeReads = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "inodeWorkout() read step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterReads = time.Now()
			// Do shutdown step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "inodeWorkout() shutdown step returned: %v\n", err)
					os.Exit(1)
				}
			}

			// Remove inode if all threads use same inode
			if doSameFile {
				_ = destroyInode(rwSizeEach.FileInodeNumber)
			}

			rwSizeEach.inodeTimes.writeDuration = timeAfterWrites.Sub(timeBeforeWrites)
			rwSizeEach.inodeTimes.readDuration = timeAfterReads.Sub(timeBeforeReads)
		}
	}

	if doSwiftclientWorkout {
		for _, rwSizeEach := range rwSizeEachArray {

			// Create object used by all threads
			if doSameFile {
				err, rwSizeEach.ObjectPath = createObject()
				if nil != err {
					// In an error, no point in continuing.  Just break from this for loop.
					break
				}
			}

			// Do initialization step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				go swiftclientWorkout(rwSizeEach, threadIndex, doSameFile)
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "swiftclientWorkout() initialization step returned: %v\n", err)
					os.Exit(1)
				}
			}
			// Do writes step
			timeBeforeWrites = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "swiftclientWorkout() write step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterWrites = time.Now()
			// Do reads step
			timeBeforeReads = time.Now()
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "swiftclientWorkout() read step returned: %v\n", err)
					os.Exit(1)
				}
			}
			timeAfterReads = time.Now()
			// Do shutdown step
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				doNextStepChan <- true
			}
			for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
				err = <-stepErrChan
				if nil != err {
					fmt.Fprintf(os.Stderr, "swiftclientWorkout() shutdown step returned: %v\n", err)
					os.Exit(1)
				}
			}

			// Remove object if all threads use same object
			if doSameFile {
				_ = deleteObject(rwSizeEach.ObjectPath)
			}

			rwSizeEach.swiftclientTimes.writeDuration = timeAfterWrites.Sub(timeBeforeWrites)
			rwSizeEach.swiftclientTimes.readDuration = timeAfterReads.Sub(timeBeforeReads)
		}
	}

	if proxyfsRequired {
		// Stop ProxyFS components launched above

		if fuseRequired {
			err = fuse.Down()
			if nil != err {
				fmt.Fprintf(os.Stderr, "fuse.Down() failed: %v\n", err)
				os.Exit(1)
			}
		}

		if fsRequired {
			err = fs.Down()
			if nil != err {
				fmt.Fprintf(os.Stderr, "fs.Down() failed: %v\n", err)
				os.Exit(1)
			}
		}

		err = inode.Down()
		if nil != err {
			fmt.Fprintf(os.Stderr, "inode.Down() failed: %v\n", err)
			os.Exit(1)
		}

		err = swiftclient.Down()
		if nil != err {
			fmt.Fprintf(os.Stderr, "swiftclient.Down() failed: %v\n", err)
			os.Exit(1)
		}

		err = headhunter.Down()
		if nil != err {
			fmt.Fprintf(os.Stderr, "headhunter.Down() failed: %v\n", err)
			os.Exit(1)
		}

		err = dlm.Down()
		if nil != err {
			fmt.Fprintf(os.Stderr, "dlm.Down() failed: %v\n", err)
			os.Exit(1)
		}

		err = stats.Down()
		if nil != err {
			fmt.Fprintf(os.Stderr, "stats.Down() failed: %v\n", err)
			os.Exit(1)
		}

		err = logger.Down()
		if nil != err {
			fmt.Fprintf(os.Stderr, "logger.Down() failed: %v\n", err)
			os.Exit(1)
		}
	}

	// Report results

	bandwidthNumerator = float64(threads*rwSizeTotal) / float64(1024*1024)

	var fileAccess = "Sequential"
	var threadFile = "different files"
	if doSameFile {
		threadFile = "same file"
	}
	if doRandomIO {
		fileAccess = "Random"
	}

	fmt.Printf("   I/O type: %v and %v per thread\n", fileAccess, threadFile)
	fmt.Printf("   (in MiB/sec)   ")
	for _, rwSizeEach := range rwSizeEachArray {
		fmt.Printf("   %s", rwSizeEach.name)
	}
	fmt.Println()

	if doDirWorkout {
		fmt.Printf("dir          read ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.dirTimes.readDuration.Seconds())
		}
		fmt.Println()
		fmt.Printf("            write ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.dirTimes.writeDuration.Seconds())
		}
		fmt.Println()
	}

	if doFuseWorkout {
		fmt.Printf("fuse         read ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.fuseTimes.readDuration.Seconds())
		}
		fmt.Println()
		fmt.Printf("            write ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.fuseTimes.writeDuration.Seconds())
		}
		fmt.Println()
	}

	if doFsWorkout {
		fmt.Printf("fs           read ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.fsTimes.readDuration.Seconds())
		}
		fmt.Println()
		fmt.Printf("            write ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.fsTimes.writeDuration.Seconds())
		}
		fmt.Println()
	}

	if doInodeWorkout {
		fmt.Printf("inode        read ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.inodeTimes.readDuration.Seconds())
		}
		fmt.Println()
		fmt.Printf("            write ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.inodeTimes.writeDuration.Seconds())
		}
		fmt.Println()
	}

	if doSwiftclientWorkout {
		fmt.Printf("swiftclient  read ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.swiftclientTimes.readDuration.Seconds())
		}
		fmt.Println()
		fmt.Printf("            write ")
		for _, rwSizeEach := range rwSizeEachArray {
			fmt.Printf(" %8.2f", bandwidthNumerator/rwSizeEach.swiftclientTimes.writeDuration.Seconds())
		}
		fmt.Println()
	}
}

func dirWorkout(rwSizeEach *rwSizeEachStruct, threadIndex uint64) {
	fileName := fmt.Sprintf("%s/%s%016X", dirPath, basenamePrefix, threadIndex)

	file, err := os.Create(fileName)
	if nil != err {
		stepErrChan <- fmt.Errorf("os.Create(\"%v\") failed: %v\n", fileName, err)
		return
	}

	rwSizeRequested := rwSizeEach.KiB * 1024

	bufWritten := make([]byte, rwSizeRequested)
	for i := uint64(0); i < rwSizeRequested; i++ {
		bufWritten[i] = 0
	}

	bufRead := make([]byte, rwSizeRequested)

	stepErrChan <- nil
	_ = <-doNextStepChan

	for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
		_, err = file.WriteAt(bufWritten, int64(rwOffset))
		if nil != err {
			stepErrChan <- fmt.Errorf("file.WriteAt(bufWritten, int64(rwOffset)) failed: %v\n", err)
			return
		}
	}

	err = file.Sync()
	if nil != err {
		stepErrChan <- fmt.Errorf("file.Sync() failed: %v\n", err)
		return
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
		_, err = file.ReadAt(bufRead, int64(rwOffset))
		if nil != err {
			stepErrChan <- fmt.Errorf("file.ReadAt(bufRead, int64(rwOffset)) failed: %v\n", err)
			return
		}
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	err = file.Close()
	if nil != err {
		stepErrChan <- fmt.Errorf("file.Close() failed: %v\n", err)
		return
	}
	err = os.Remove(fileName)
	if nil != err {
		stepErrChan <- fmt.Errorf("os.Remove(fileName) failed: %v\n", err)
		return
	}

	stepErrChan <- nil
}

func fuseWorkout(rwSizeEach *rwSizeEachStruct, threadIndex uint64) {
	nonce, err := headhunterVolumeHandle.FetchNonce()
	if nil != err {
		stepErrChan <- fmt.Errorf("headhunter.FetchNonce() failed: %v\n", err)
		return
	}

	fileName := fmt.Sprintf("%s/%s%016X", mountPointName, basenamePrefix, nonce)

	file, err := os.Create(fileName)
	if nil != err {
		stepErrChan <- fmt.Errorf("os.Create(\"%v\") failed: %v\n", fileName, err)
		return
	}

	rwSizeRequested := rwSizeEach.KiB * 1024

	bufWritten := make([]byte, rwSizeRequested)
	for i := uint64(0); i < rwSizeRequested; i++ {
		bufWritten[i] = 0
	}

	bufRead := make([]byte, rwSizeRequested)

	stepErrChan <- nil
	_ = <-doNextStepChan

	for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
		_, err = file.WriteAt(bufWritten, int64(rwOffset))
		if nil != err {
			stepErrChan <- fmt.Errorf("file.WriteAt(bufWritten, int64(rwOffset)) failed: %v\n", err)
			return
		}
	}

	err = file.Sync()
	if nil != err {
		stepErrChan <- fmt.Errorf("file.Sync() failed: %v\n", err)
		return
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
		_, err = file.ReadAt(bufRead, int64(rwOffset))
		if nil != err {
			stepErrChan <- fmt.Errorf("file.ReadAt(bufRead, int64(rwOffset)) failed: %v\n", err)
			return
		}
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	err = file.Close()
	if nil != err {
		stepErrChan <- fmt.Errorf("file.Close() failed: %v\n", err)
		return
	}
	err = os.Remove(fileName)
	if nil != err {
		stepErrChan <- fmt.Errorf("os.Remove(fileName) failed: %v\n", err)
		return
	}

	stepErrChan <- nil
}

func createFsFile() (err error, mountHandle fs.MountHandle, fileInodeNumber inode.InodeNumber, fileName string) {
	mountHandle, err = fs.Mount(volumeName, fs.MountOptions(0))
	if nil != err {
		stepErrChan <- fmt.Errorf("fs.Mount(\"%v\", fs.MountOptions(0), \"\") failed: %v\n", volumeName, err)
		return
	}

	nonce, err := headhunterVolumeHandle.FetchNonce()
	if nil != err {
		stepErrChan <- fmt.Errorf("headhunter.FetchNonce() failed: %v\n", err)
		return
	}

	fileName = fmt.Sprintf("%s%016X", basenamePrefix, nonce)

	fileInodeNumber, err = mountHandle.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, inode.RootDirInodeNumber, fileName, inode.PosixModePerm)
	if nil != err {
		stepErrChan <- fmt.Errorf("fs.Create(,,,, fileName==\"%s\", inode.PosixModePerm) failed: %v\n", fileName, err)
		return
	}
	return
}

func unlinkFsFile(mountHandle fs.MountHandle, fileName string) (err error) {
	err = mountHandle.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, inode.RootDirInodeNumber, fileName)
	if nil != err {
		stepErrChan <- fmt.Errorf("fs.Unlink(,,,, rootInodeNumber, \"%v\") failed: %v\n", fileName, err)
		return
	}
	return
}

func fsWorkout(rwSizeEach *rwSizeEachStruct, threadIndex uint64, doSameFile bool, doRandomIO bool) {
	var err error
	var mountHandle fs.MountHandle
	var fileInodeNumber inode.InodeNumber
	var fileName string

	if !doSameFile {
		// Create the file for this thread
		err, mountHandle, fileInodeNumber, fileName = createFsFile()
		if nil != err {
			return
		}
	} else {
		// File was already created during main()
		mountHandle = rwSizeEach.MountHandle
		fileInodeNumber = rwSizeEach.FileInodeNumber
	}
	rwSizeRequested := rwSizeEach.KiB * 1024

	bufWritten := make([]byte, rwSizeRequested)
	for i := uint64(0); i < rwSizeRequested; i++ {
		bufWritten[i] = 0
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	if doRandomIO {
		var rwOffset int64

		// Calculate number of I/Os to do since we cannot use size of file in the random case.
		var numberIOsNeeded uint64 = rwSizeTotal / rwSizeRequested
		for i := uint64(0); i < numberIOsNeeded; i++ {
			// For the first I/O, we set it to (rwSizeTotal - rwSizeRequested).  This guarantees that we write
			// the full size of the buffer.
			if i == 0 {
				rwOffset = int64(rwSizeTotal - rwSizeRequested)
			} else {

				// Pick a random offset within the buffer.  We back off from end of buffer by rwSizeRequested
				// to make sure we do not go past end of file.
				rwOffset = rand.Int63n(int64(rwSizeTotal - rwSizeRequested))
			}
			rwSizeDelivered, err := mountHandle.WriteAsSlice(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInodeNumber, uint64(rwOffset), bufWritten, nil)
			if nil != err {
				stepErrChan <- fmt.Errorf("fs.WriteAsSlice(,,,, fileInodeNumber, rwOffset, bufWritten) failed: %v\n", err)
				return
			}
			if rwSizeRequested != rwSizeDelivered {
				stepErrChan <- fmt.Errorf("fs.WriteAsSlice(,,,, fileInodeNumber, rwOffset, bufWritten) failed to transfer all requested bytes\n")
				return
			}
		}
	} else {

		for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
			rwSizeDelivered, err := mountHandle.WriteAsSlice(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInodeNumber, rwOffset, bufWritten, nil)
			if nil != err {
				stepErrChan <- fmt.Errorf("fs.WriteAsSlice(,,,, fileInodeNumber, rwOffset, bufWritten) failed: %v\n", err)
				return
			}
			if rwSizeRequested != rwSizeDelivered {
				stepErrChan <- fmt.Errorf("fs.WriteAsSlice(,,,, fileInodeNumber, rwOffset, bufWritten) failed to transfer all requested bytes\n")
				return
			}
		}
	}

	err = mountHandle.Flush(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInodeNumber)
	if nil != err {
		stepErrChan <- fmt.Errorf("fs.Flush(,,,, fileInodeNumber) failed: %v\n", err)
		return
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	if doRandomIO {
		// Calculate number of I/Os to do since we cannot use size of file in the random case.
		var numberIOsNeeded uint64 = rwSizeTotal / rwSizeRequested
		for i := uint64(0); i < numberIOsNeeded; i++ {

			// Calculate random offset
			rwOffset := uint64(rand.Int63n(int64(rwSizeTotal - rwSizeRequested)))

			bufRead, err := mountHandle.ReadReturnSlice(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInodeNumber, rwOffset, rwSizeRequested, nil)
			if nil != err {
				stepErrChan <- fmt.Errorf("fs.ReadReturnSlice(,,,, fileInodeNumber, rwOffset, rwSizeRequested) failed: %v\n", err)
				return
			}
			if rwSizeRequested != uint64(len(bufRead)) {
				stepErrChan <- fmt.Errorf("fs.ReadReturnSlice(,,,, fileInodeNumber, rwOffset, rwSizeRequested) failed to transfer all requested bytes\n")
				return
			}
		}
	} else {
		for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
			bufRead, err := mountHandle.ReadReturnSlice(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInodeNumber, rwOffset, rwSizeRequested, nil)
			if nil != err {
				stepErrChan <- fmt.Errorf("fs.ReadReturnSlice(,,,, fileInodeNumber, rwOffset, rwSizeRequested) failed: %v\n", err)
				return
			}
			if rwSizeRequested != uint64(len(bufRead)) {
				stepErrChan <- fmt.Errorf("fs.ReadReturnSlice(,,,, fileInodeNumber, rwOffset, rwSizeRequested) failed to transfer all requested bytes\n")
				return
			}
		}
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	if !doSameFile {
		err = unlinkFsFile(mountHandle, fileName)
		if nil != err {
			return
		}
	}

	stepErrChan <- nil
}

func createInode() (err error, fileInodeNumber inode.InodeNumber) {
	mutex.Lock()
	volumeHandle, err := inode.FetchVolumeHandle(volumeName)
	mutex.Unlock()
	if nil != err {
		stepErrChan <- fmt.Errorf("inode.FetchVolumeHandle(\"%v\") failed: %v\n", volumeName, err)
		return
	}

	fileInodeNumber, err = volumeHandle.CreateFile(inode.PosixModePerm, inode.InodeUserID(0), inode.InodeGroupID(0))
	if nil != err {
		stepErrChan <- fmt.Errorf("volumeHandle.CreateFile(inode.PosixModePerm, inode.InodeUserID(0), inode.InodeGroupID(0)) failed: %v\n", err)
		return
	}
	return
}

func destroyInode(fileInodeNumber inode.InodeNumber) (err error) {
	mutex.Lock()
	volumeHandle, err := inode.FetchVolumeHandle(volumeName)
	mutex.Unlock()
	if nil != err {
		stepErrChan <- fmt.Errorf("inode.FetchVolumeHandle(\"%v\") failed: %v\n", volumeName, err)
		return
	}
	err = volumeHandle.Purge(fileInodeNumber)
	if nil != err {
		stepErrChan <- fmt.Errorf("volumeHandle.Purge(fileInodeNumber) failed: %v\n", err)
		return
	}
	err = volumeHandle.Destroy(fileInodeNumber)
	if nil != err {
		stepErrChan <- fmt.Errorf("volumeHandle.Destroy(fileInodeNumber) failed: %v\n", err)
		return
	}
	return
}

func inodeWorkout(rwSizeEach *rwSizeEachStruct, threadIndex uint64, doSameFile bool, doRandomIO bool) {
	mutex.Lock()
	volumeHandle, err := inode.FetchVolumeHandle(volumeName)
	mutex.Unlock()

	if nil != err {
		stepErrChan <- fmt.Errorf("inode.FetchVolumeHandle(\"%v\") failed: %v\n", volumeName, err)
		return
	}

	var fileInodeNumber inode.InodeNumber
	if !doSameFile {
		err, fileInodeNumber = createInode()
		if nil != err {
			return
		}
	} else {
		fileInodeNumber = rwSizeEach.FileInodeNumber
	}

	rwSizeRequested := rwSizeEach.KiB * 1024

	bufWritten := make([]byte, rwSizeRequested)
	for i := uint64(0); i < rwSizeRequested; i++ {
		bufWritten[i] = 0
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	if doRandomIO {
		var rwOffset int64

		// Calculate number of I/Os to do since we cannot use size of file in the random case.
		var numberIOsNeeded uint64 = rwSizeTotal / rwSizeRequested
		for i := uint64(0); i < numberIOsNeeded; i++ {

			// For the first I/O, we set it to (rwSizeTotal - rwSizeRequested).  This guarantees that we write
			// the full size of the buffer.
			if i == 0 {
				rwOffset = int64(rwSizeTotal - rwSizeRequested)
			} else {

				// Pick a random offset within the buffer.  We back off from end of buffer by rwSizeRequested
				// to make sure we do not go past end of file.
				rwOffset = rand.Int63n(int64(rwSizeTotal - rwSizeRequested))
			}
			err = volumeHandle.WriteAsSlice(fileInodeNumber, uint64(rwOffset), bufWritten, nil)
			if nil != err {
				stepErrChan <- fmt.Errorf("volumeHandle.WriteAsSlice(fileInodeNumber, rwOffset, bufWritten) failed: %v\n", err)
				return
			}
		}
	} else {
		for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
			err = volumeHandle.WriteAsSlice(fileInodeNumber, rwOffset, bufWritten, nil)
			if nil != err {
				stepErrChan <- fmt.Errorf("volumeHandle.WriteAsSlice(fileInodeNumber, rwOffset, bufWritten) failed: %v\n", err)
				return
			}
		}
	}

	err = volumeHandle.Flush(fileInodeNumber, false)
	if nil != err {
		stepErrChan <- fmt.Errorf("volumeHandle.Flush(rwSizeEach.FileInodeNumber, false) failed: %v\n", err)
		return
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	if doRandomIO {
		// Calculate number of I/Os to do since we cannot use size of file in the random case.
		var numberIOsNeeded uint64 = rwSizeTotal / rwSizeRequested
		for i := uint64(0); i < numberIOsNeeded; i++ {

			// Calculate random offset
			rwOffset := uint64(rand.Int63n(int64(rwSizeTotal - rwSizeRequested)))
			bufRead, err := volumeHandle.ReadReturnSlice(fileInodeNumber, rwOffset, rwSizeRequested, nil)
			if nil != err {
				stepErrChan <- fmt.Errorf("volumeHandle.ReadReturnSlice(rwSizeEach.FileInodeNumber, rwOffset, rwSizeRequested) failed: %v\n", err)
				return
			}
			if rwSizeRequested != uint64(len(bufRead)) {
				stepErrChan <- fmt.Errorf("volumeHandle.ReadReturnSlice(rwSizeEach.FileInodeNumber, rwOffset, rwSizeRequested) failed to transfer all requested bytes\n")
				return
			}
		}
	} else {
		for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
			bufRead, err := volumeHandle.ReadReturnSlice(fileInodeNumber, rwOffset, rwSizeRequested, nil)
			if nil != err {
				stepErrChan <- fmt.Errorf("volumeHandle.ReadReturnSlice(rwSizeEach.FileInodeNumber, rwOffset, rwSizeRequested) failed: %v\n", err)
				return
			}
			if rwSizeRequested != uint64(len(bufRead)) {
				stepErrChan <- fmt.Errorf("volumeHandle.ReadReturnSlice(rwSizeEach.FileInodeNumber, rwOffset, rwSizeRequested) failed to transfer all requested bytes\n")
				return
			}
		}
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	if !doSameFile {
		err = destroyInode(fileInodeNumber)
		if nil != err {
			return
		}
	}

	stepErrChan <- nil
}

func createObject() (err error, objectPath string) {
	mutex.Lock()
	volumeHandle, err := inode.FetchVolumeHandle(volumeName)
	mutex.Unlock()
	if nil != err {
		stepErrChan <- fmt.Errorf("inode.FetchVolumeHandle(\"%v\") failed: %v\n", volumeName, err)
		return
	}

	objectPath, err = volumeHandle.ProvisionObject()
	if nil != err {
		stepErrChan <- fmt.Errorf("volumeHandle.ProvisionObject() failed: %v\n", err)
		return
	}
	return
}

func deleteObject(objectPath string) (err error) {
	accountName, containerName, objectName, err := utils.PathToAcctContObj(objectPath)
	if nil != err {
		stepErrChan <- fmt.Errorf("utils.PathToAcctContObj(\"%v\") failed: %v\n", objectPath, err)
		return
	}

	err = swiftclient.ObjectDeleteSync(accountName, containerName, objectName)
	if nil != err {
		stepErrChan <- fmt.Errorf("swiftclient.ObjectDeleteSync(\"%v\", \"%v\", \"%v\") failed: %v\n", accountName, containerName, objectName, err)
		return
	}
	return
}

func swiftclientWorkout(rwSizeEach *rwSizeEachStruct, threadIndex uint64, doSameFile bool) {
	var err error
	var objectPath string
	if !doSameFile {
		err, objectPath = createObject()
		if nil != err {
			return
		}
	} else {
		objectPath = rwSizeEach.ObjectPath
	}

	accountName, containerName, objectName, err := utils.PathToAcctContObj(objectPath)
	if nil != err {
		stepErrChan <- fmt.Errorf("utils.PathToAcctContObj(\"%v\") failed: %v\n", objectPath, err)
		return
	}

	rwSizeRequested := rwSizeEach.KiB * 1024

	bufWritten := make([]byte, rwSizeRequested)
	for i := uint64(0); i < rwSizeRequested; i++ {
		bufWritten[i] = 0
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	chunkedPutContext, err := swiftclient.ObjectFetchChunkedPutContext(accountName, containerName, objectName)
	if nil != err {
		stepErrChan <- fmt.Errorf("swiftclient.ObjectFetchChunkedPutContext(\"%v\", \"%v\", \"%v\") failed: %v\n", accountName, containerName, objectName, err)
		return
	}

	for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
		err = chunkedPutContext.SendChunkAsSlice(bufWritten)
		if nil != err {
			stepErrChan <- fmt.Errorf("chunkedPutContext.SendChunkAsSlice(bufWritten) failed: %v\n", err)
			return
		}
	}

	err = chunkedPutContext.Close()
	if nil != err {
		stepErrChan <- fmt.Errorf("chunkedPutContext.Close() failed: %v\n", err)
		return
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	for rwOffset := uint64(0); rwOffset < rwSizeTotal; rwOffset += rwSizeRequested {
		bufRead, err := swiftclient.ObjectGetReturnSlice(accountName, containerName, objectName,
			rwOffset, rwSizeRequested)
		if nil != err {
			stepErrChan <- fmt.Errorf("swiftclient.ObjectGetReturnSlice(\"%v\", \"%v\", \"%v\", rwOffset, rwSizeRequested) failed: %v\n", accountName, containerName, objectName, err)
			return
		}
		if rwSizeRequested != uint64(len(bufRead)) {
			stepErrChan <- fmt.Errorf("swiftclient.ObjectGetReturnSlice(\"%v\", \"%v\", \"%v\", rwOffset, rwSizeRequested) failed to transfer all requested bytes\n", accountName, containerName, objectName)
			return
		}
	}

	stepErrChan <- nil
	_ = <-doNextStepChan

	if !doSameFile {
		err = deleteObject(objectPath)
	}

	stepErrChan <- nil
}
