package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/trackedlock"
	"github.com/swiftstack/ProxyFS/transitions"
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
	rootDirMutex    trackedlock.Mutex
	stepErrChan     chan error
	threads         uint64
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
		primaryPeer                  string
		threadIndex                  uint64
		timeAfterMeasuredOperations  time.Time
		timeBeforeMeasuredOperations time.Time
		volumeGroupToCheck           string
		volumeGroupToUse             string
		volumeGroupList              []string
		volumeList                   []string
		whoAmI                       string
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

	// Upgrade confMap if necessary
	err = transitions.UpgradeConfMapIfNeeded(confMap)
	if nil != err {
		fmt.Fprintf(os.Stderr, "Failed to upgrade config: %v", err)
		os.Exit(1)
	}

	// Start up needed ProxyFS components

	err = transitions.Up(confMap)
	if nil != err {
		fmt.Fprintf(os.Stderr, "transitions.Up() failed: %v\n", err)
		os.Exit(1)
	}

	// Select first Volume of the first "active" VolumeGroup in [FSGlobals]VolumeGroupList
	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueString(\"Cluster\", \"WhoAmI\") failed: %v\n", err)
		os.Exit(1)
	}

	volumeGroupList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeGroupList")
	if nil != err {
		fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeGroupList\") failed: %v\n", err)
		os.Exit(1)
	}

	volumeGroupToUse = ""

	for _, volumeGroupToCheck = range volumeGroupList {
		primaryPeer, err = confMap.FetchOptionValueString("VolumeGroup:"+volumeGroupToCheck, "PrimaryPeer")
		if nil != err {
			fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueString(\"VolumeGroup:%s\", \"PrimaryPeer\") failed: %v\n", volumeGroupToCheck, err)
			os.Exit(1)
		}
		if whoAmI == primaryPeer {
			volumeGroupToUse = volumeGroupToCheck
			break
		}
	}

	if "" == volumeGroupToUse {
		fmt.Fprintf(os.Stderr, "confMap didn't contain an \"active\" VolumeGroup")
		os.Exit(1)
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("VolumeGroup:"+volumeGroupToUse, "VolumeList")
	if nil != err {
		fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueStringSlice(\"VolumeGroup:%s\", \"PrimaryPeer\") failed: %v\n", volumeGroupToUse, err)
		os.Exit(1)
	}
	if 1 > len(volumeList) {
		fmt.Fprintf(os.Stderr, "confMap.FetchOptionValueStringSlice(\"VolumeGroup:%s\", \"VolumeList\") returned empty volumeList", volumeGroupToUse)
		os.Exit(1)
	}

	volumeName = volumeList[0]

	mountHandle, err = fs.MountByVolumeName(volumeName, fs.MountOptions(0))
	if nil != err {
		fmt.Fprintf(os.Stderr, "fs.MountByVolumeName(\"%value\",) failed: %v\n", volumeName, err)
		os.Exit(1)
	}

	// Perform tests

	stepErrChan = make(chan error, 0)   //threads)
	doNextStepChan = make(chan bool, 0) //threads)

	// Do initialization step
	for threadIndex = uint64(0); threadIndex < threads; threadIndex++ {
		go fsWorkout(threadIndex)
	}
	for threadIndex = uint64(0); threadIndex < threads; threadIndex++ {
		err = <-stepErrChan
		if nil != err {
			fmt.Fprintf(os.Stderr, "fsWorkout() initialization step returned: %v\n", err)
			os.Exit(1)
		}
	}

	// Do measured operations step
	timeBeforeMeasuredOperations = time.Now()
	for threadIndex = uint64(0); threadIndex < threads; threadIndex++ {
		doNextStepChan <- true
	}
	for threadIndex = uint64(0); threadIndex < threads; threadIndex++ {
		err = <-stepErrChan
		if nil != err {
			fmt.Fprintf(os.Stderr, "fsWorkout() measured operations step returned: %v\n", err)
			os.Exit(1)
		}
	}
	timeAfterMeasuredOperations = time.Now()

	// Do shutdown step
	for threadIndex = uint64(0); threadIndex < threads; threadIndex++ {
		doNextStepChan <- true
	}
	for threadIndex = uint64(0); threadIndex < threads; threadIndex++ {
		err = <-stepErrChan
		if nil != err {
			fmt.Fprintf(os.Stderr, "fsWorkout() shutdown step returned: %v\n", err)
			os.Exit(1)
		}
	}

	// Stop ProxyFS components launched above

	err = transitions.Down(confMap)
	if nil != err {
		fmt.Fprintf(os.Stderr, "transitions.Down() failed: %v\n", err)
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
			dirInodeNumber, err = mountHandle.Mkdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, dirInodeName, inode.PosixModePerm)
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
		} else { // measureStat || measureDestroy
			dirInodeNumber, err = mountHandle.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, dirInodeName)
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
			fileInodeNumber, err = mountHandle.Create(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirInodeNumber, fileInodeName[i], inode.PosixModePerm)
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
		} else if measureStat {
			fileInodeNumber, err = mountHandle.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirInodeNumber, fileInodeName[i])
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
			_, err = mountHandle.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, fileInodeNumber)
			if nil != err {
				stepErrChan <- err
				runtime.Goexit()
			}
		} else { // measureDestroy
			err = mountHandle.Unlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirInodeNumber, fileInodeName[i])
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
		err = mountHandle.Rmdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.RootDirInodeNumber, dirInodeName)
		if nil != err {
			stepErrChan <- err
			runtime.Goexit()
		}
	}

	// Indicate shutdown step is done
	stepErrChan <- nil
}
