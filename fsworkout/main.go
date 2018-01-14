package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
)

const (
	dirInodeNamePrefix  = "__fsworkout_dir_"
	fileInodeNamePrefix = "__fsworkout_file_"
)

var (
	doNextStepChan  chan bool
	inodesPerThread uint64
	measureCreate   bool
	measureDestroy  bool
	measureStat     bool
	mountHandle     fs.MountHandle
	perThreadDir    bool
	rootDirMutex    sync.Mutex
	stepErrChan     chan error
	threads         uint64
	volumeHandle    inode.VolumeHandle
	volumeName      string
)

func usage(file *os.File) {
	fmt.Fprintf(file, "Usage:\n")
	fmt.Fprintf(file, "    %v [cCsSdD] threads inodes-per-thread conf-file [section.option=value]*\n", os.Args[0])
	fmt.Fprintf(file, "  where:\n")
	fmt.Fprintf(file, "    c                       run create  test in common root dir\n")
	fmt.Fprintf(file, "    C                       run create  test in per thread  dir\n")
	fmt.Fprintf(file, "    s                       run stat    test in common root dir\n")
	fmt.Fprintf(file, "    S                       run stat    test in per thread  dir\n")
	fmt.Fprintf(file, "    d                       run destroy test in common root dir\n")
	fmt.Fprintf(file, "    D                       run destroy test in per thread  dir\n")
	fmt.Fprintf(file, "    threads                 number of threads\n")
	fmt.Fprintf(file, "    inodes-per-thread       number of inodes each thread will reference\n")
	fmt.Fprintf(file, "    conf-file               input to conf.MakeConfMapFromFile()\n")
	fmt.Fprintf(file, "    [section.option=value]* optional input to conf.UpdateFromStrings()\n")
	fmt.Fprintf(file, "\n")
	fmt.Fprintf(file, "Note: Precisely one test selector must be specified\n")
	fmt.Fprintf(file, "      It is expected that c, s, then d are run in sequence\n")
	fmt.Fprintf(file, "                  or that C, S, then D are run in sequence\n")
	fmt.Fprintf(file, "      It is expected that cleanproxyfs is run before & after the sequence\n")
}

func main() {
	var (
		confMap                      conf.ConfMap
		durationOfMeasuredOperations time.Duration
		err                          error
		latencyPerOpInMilliSeconds   float64
		opsPerSecond                 float64
		timeAfterMeasuredOperations  time.Time
		timeBeforeMeasuredOperations time.Time
		volumeList                   []string
	)

	// Parse arguments

	if 5 > len(os.Args) {
		usage(os.Stderr)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "c":
		measureCreate = true
	case "C":
		measureCreate = true
		perThreadDir = true
	case "s":
		measureStat = true
	case "S":
		measureStat = true
		perThreadDir = true
	case "d":
		measureDestroy = true
	case "D":
		measureDestroy = true
		perThreadDir = true
	default:
		fmt.Fprintf(os.Stderr, "os.Args[1] ('%v') must be one of 'c', 'C', 'r', 'R', 'd', or 'D'\n", os.Args[1])
		os.Exit(1)
	}

	threads, err = strconv.ParseUint(os.Args[2], 10, 64)
	if nil != err {
		fmt.Fprintf(os.Stderr, "strconv.ParseUint(\"%v\", 10, 64) of threads failed: %v\n", os.Args[2], err)
		os.Exit(1)
	}
	if 0 == threads {
		fmt.Fprintf(os.Stderr, "threads must be a positive number\n")
		os.Exit(1)
	}

	inodesPerThread, err = strconv.ParseUint(os.Args[3], 10, 64)
	if nil != err {
		fmt.Fprintf(os.Stderr, "strconv.ParseUint(\"%v\", 10, 64) of inodes-per-thread failed: %v\n", os.Args[3], err)
		os.Exit(1)
	}
	if 0 == inodesPerThread {
		fmt.Fprintf(os.Stderr, "inodes-per-thread must be a positive number\n")
		os.Exit(1)
	}

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

	// Start up needed ProxyFS components

	err = evtlog.Up(confMap)
	if nil != err {
		fmt.Fprintf(os.Stderr, "evtlog.Up() failed: %v\n", err)
		os.Exit(1)
	}

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

	err = fs.Up(confMap)
	if nil != err {
		fmt.Fprintf(os.Stderr, "fs.Up() failed: %v\n", err)
		os.Exit(1)
	}

	// Select first "active" volumeName in volumeList by attempting to mount each

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v\n", err)
		os.Exit(1)
	}
	if 1 > len(volumeList) {
		fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") returned empty volumeList")
		os.Exit(1)
	}

	for _, volumeName = range volumeList {
		volumeHandle, err = inode.FetchVolumeHandle(volumeName)
		if nil == err {
			break
		} else {
			volumeHandle = nil
		}
	}

	if nil == volumeHandle {
		fmt.Fprintf(os.Stderr, "inode.FetchVolumeHandle() failed on every volumeName in volumeList: %v\n", volumeList)
		os.Exit(1)
	}

	mountHandle, err = fs.Mount(volumeName, fs.MountOptions(0))
	if nil != err {
		fmt.Fprintf(os.Stderr, "fs.Mount(\"%value\",,) failed: %v\n", volumeName, err)
		os.Exit(1)
	}

	// Perform tests

	stepErrChan = make(chan error, 0)   //threads)
	doNextStepChan = make(chan bool, 0) //threads)

	// Do initialization step
	for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
		go fsWorkout(threadIndex)
	}
	for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
		err = <-stepErrChan
		if nil != err {
			fmt.Fprintf(os.Stderr, "fsWorkout() initialization step returned: %v\n", err)
			os.Exit(1)
		}
	}

	// Do measured operations step
	timeBeforeMeasuredOperations = time.Now()
	for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
		doNextStepChan <- true
	}
	for threadIndex := uint64(0); threadIndex < threads; threadIndex++ {
		err = <-stepErrChan
		if nil != err {
			fmt.Fprintf(os.Stderr, "fsWorkout() measured operations step returned: %v\n", err)
			os.Exit(1)
		}
	}
	timeAfterMeasuredOperations = time.Now()

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

	// Stop ProxyFS components launched above

	err = fs.Down()
	if nil != err {
		fmt.Fprintf(os.Stderr, "fs.Down() failed: %v\n", err)
		os.Exit(1)
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

	err = evtlog.Down()
	if nil != err {
		fmt.Fprintf(os.Stderr, "evtlog.Down() failed: %v\n", err)
		os.Exit(1)
	}

	// Report results

	durationOfMeasuredOperations = timeAfterMeasuredOperations.Sub(timeBeforeMeasuredOperations)

	opsPerSecond = float64(threads*inodesPerThread*1000*1000*1000) / float64(durationOfMeasuredOperations.Nanoseconds())
	latencyPerOpInMilliSeconds = float64(durationOfMeasuredOperations.Nanoseconds()) / float64(inodesPerThread*1000*1000)

	fmt.Printf("opsPerSecond = %10.2f\n", opsPerSecond)
	fmt.Printf("latencyPerOp = %10.2f ms\n", latencyPerOpInMilliSeconds)
}

func fsWorkout(threadIndex uint64) {
	var (
		dirInodeName    string
		dirInodeNumber  inode.InodeNumber
		err             error
		fileInodeName   []string
		fileInodeNumber inode.InodeNumber
		i               uint64
	)

	// Do initialization step
	if perThreadDir {
		dirInodeName = fmt.Sprintf("%s%016X", dirInodeNamePrefix, threadIndex)
		if measureCreate {
			dirInodeNumber, err = mountHandle.Mkdir(inode.InodeRootUserID, inode.InodeRootGroupID, nil, inode.RootDirInodeNumber, dirInodeName, inode.PosixModePerm)
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
		} else { // measureStat || measureDestroy
			dirInodeNumber, err = mountHandle.Lookup(inode.InodeRootUserID, inode.InodeRootGroupID, nil, inode.RootDirInodeNumber, dirInodeName)
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
		}
	} else { // !perThreadDir
		dirInodeNumber = inode.RootDirInodeNumber
	}
	fileInodeName = make([]string, inodesPerThread)
	for i = 0; i < inodesPerThread; i++ {
		fileInodeName[i] = fmt.Sprintf("%s%016X_%016X", fileInodeNamePrefix, threadIndex, i)
	}

	// Indicate initialization step is done
	stepErrChan <- nil

	// Await signal to proceed with measured operations step
	_ = <-doNextStepChan

	// Do measured operations
	for i = 0; i < inodesPerThread; i++ {
		if measureCreate {
			fileInodeNumber, err = mountHandle.Create(inode.InodeRootUserID, inode.InodeRootGroupID, nil, dirInodeNumber, fileInodeName[i], inode.PosixModePerm)
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
		} else if measureStat {
			fileInodeNumber, err = mountHandle.Lookup(inode.InodeRootUserID, inode.InodeRootGroupID, nil, dirInodeNumber, fileInodeName[i])
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
			_, err = mountHandle.Getstat(inode.InodeRootUserID, inode.InodeRootGroupID, nil, fileInodeNumber)
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
		} else { // measureDestroy
			err = mountHandle.Unlink(inode.InodeRootUserID, inode.InodeRootGroupID, nil, dirInodeNumber, fileInodeName[i])
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
		}
	}

	// Indicate measured operations step is done
	stepErrChan <- nil

	// Await signal to proceed with shutdown step
	_ = <-doNextStepChan

	// Do shutdown step
	if perThreadDir && measureDestroy {
		err = mountHandle.Rmdir(inode.InodeRootUserID, inode.InodeRootGroupID, nil, inode.RootDirInodeNumber, dirInodeName)
		if nil != err {
			stepErrChan <- err
			runtime.Goexit()
		}
	}

	// Indicate shutdown step is done
	stepErrChan <- nil
}
