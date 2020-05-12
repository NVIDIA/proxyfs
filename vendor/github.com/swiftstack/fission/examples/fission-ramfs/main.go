package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/fission"
)

const (
	fuseSubtype = "fission-ramfs"

	initOutFlagsNearlyAll = uint32(0) |
		fission.InitFlagsAsyncRead |
		fission.InitFlagsFileOps |
		fission.InitFlagsAtomicOTrunc |
		fission.InitFlagsBigWrites |
		fission.InitFlagsAutoInvalData |
		fission.InitFlagsDoReadDirPlus |
		fission.InitFlagsReaddirplusAuto |
		fission.InitFlagsParallelDirops |
		fission.InitFlagsMaxPages |
		fission.InitFlagsExplicitInvalData

	initOutMaxBackgound         = uint16(100)
	initOutCongestionThreshhold = uint16(0)
	initOutMaxWrite             = uint32(128 * 1024) // 128KiB... the max write size in Linux FUSE at this time

	attrBlkSize = uint32(4096)

	entryValidSec  = uint64(10)
	entryValidNSec = uint32(0)

	attrValidSec  = uint64(10)
	attrValidNSec = uint32(0)

	tryLockBackoffMin = time.Duration(time.Second) // time.Duration(100 * time.Microsecond)
	tryLockBackoffMax = time.Duration(time.Second) // time.Duration(300 * time.Microsecond)

	accessROK = syscall.S_IROTH // surprisingly not defined as syscall.R_OK
	accessWOK = syscall.S_IWOTH // surprisingly not defined as syscall.W_OK
	accessXOK = syscall.S_IXOTH // surprisingly not defined as syscall.X_OK

	accessMask       = syscall.S_IRWXO // used to mask Owner, Group, or Other RWX bits
	accessOwnerShift = 6
	accessGroupShift = 3
	accessOtherShift = 0
)

type tryLockStruct struct {
	lockChan chan struct{} // initialized with make(chan struct{}, 1) to have the lock initially granted
}

type grantedLockSetStruct struct {
	set map[*tryLockStruct]uint64 // Value is # of times it is locked
}

type inodeStruct struct {
	tryLock     *tryLockStruct
	attr        fission.Attr       // (attr.Mode&syscall.S_IFMT) must be one of syscall.{S_IFDIR|S_IFREG|S_IFLNK}
	xattrMap    sortedmap.LLRBTree // key is xattr Name ([]byte); value is xattr Data ([]byte)
	dirEntryMap sortedmap.LLRBTree // [S_IFDIR only] key is basename of dirEntry ([]byte); value is attr.Ino (uint64)
	fileData    []byte             // [S_IFREG only] zero-filled up to attr.Size contents of file
	symlinkData []byte             // [S_IFLNK only] target path of symlink
}

type xattrMapDummyStruct struct{}
type dirEntryMapDummyStruct struct{}

type alreadyLoggedIgnoringStruct struct {
	setAttrInValidFH bool
}

type globalsStruct struct {
	tryLock               *tryLockStruct
	programPath           string
	mountPoint            string
	programName           string
	volumeName            string
	logger                *log.Logger
	errChan               chan error
	xattrMapDummy         *xattrMapDummyStruct
	dirEntryMapDummy      *dirEntryMapDummyStruct
	inodeMap              map[uint64]*inodeStruct // key is inodeStruct.atr.Ino
	lastNodeID            uint64                  // valid NodeID's start at 1... but 1 is the RootDir NodeID
	alreadyLoggedIgnoring alreadyLoggedIgnoringStruct
	volume                fission.Volume
}

var globals globalsStruct

func main() {
	var (
		err             error
		ok              bool
		rootInode       *inodeStruct
		signalChan      chan os.Signal
		unixTimeNowNSec uint32
		unixTimeNowSec  uint64
	)

	if 2 != len(os.Args) {
		fmt.Printf("Usage: %s <mount_point>\n", os.Args[0])
		os.Exit(0)
	}

	globals.tryLock = makeTryLock()

	globals.programPath = os.Args[0]
	globals.mountPoint = os.Args[1]

	globals.programName = path.Base(globals.programPath)
	globals.volumeName = path.Base(globals.mountPoint)

	globals.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime) // |log.Lmicroseconds|log.Lshortfile

	globals.errChan = make(chan error, 1)

	globals.xattrMapDummy = nil
	globals.dirEntryMapDummy = nil

	unixTimeNowSec, unixTimeNowNSec = unixTimeNow()

	rootInode = &inodeStruct{
		tryLock: makeTryLock(),
		attr: fission.Attr{
			Ino:       1,
			Size:      0,
			Blocks:    0,
			ATimeSec:  unixTimeNowSec,
			MTimeSec:  unixTimeNowSec,
			CTimeSec:  unixTimeNowSec,
			ATimeNSec: unixTimeNowNSec,
			MTimeNSec: unixTimeNowNSec,
			CTimeNSec: unixTimeNowNSec,
			Mode:      uint32(syscall.S_IFDIR | syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO),
			NLink:     2,
			UID:       0,
			GID:       0,
			RDev:      0,
			BlkSize:   attrBlkSize,
			Padding:   0,
		},
		xattrMap:    sortedmap.NewLLRBTree(sortedmap.CompareByteSlice, globals.xattrMapDummy),
		dirEntryMap: sortedmap.NewLLRBTree(sortedmap.CompareByteSlice, globals.dirEntryMapDummy),
		fileData:    nil,
		symlinkData: nil,
	}

	ok, err = rootInode.dirEntryMap.Put([]byte("."), uint64(1))
	if nil != err {
		globals.logger.Printf("rootInode.dirEntryMap.Put([]byte(\".\"), uint64(1)) failed: %v", err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("rootInode.dirEntryMap.Put([]byte(\".\"), uint64(1)) returned !ok")
		os.Exit(1)
	}
	ok, err = rootInode.dirEntryMap.Put([]byte(".."), uint64(1))
	if nil != err {
		globals.logger.Printf("rootInode.dirEntryMap.Put([]byte(\"..\"), uint64(1)) failed: %v", err)
		os.Exit(1)
	}
	if !ok {
		globals.logger.Printf("rootInode.dirEntryMap.Put([]byte(\"..\"), uint64(1)) returned !ok")
		os.Exit(1)
	}

	globals.inodeMap = make(map[uint64]*inodeStruct)

	globals.inodeMap[1] = rootInode

	globals.lastNodeID = uint64(1) // since we used NodeID 1 for the RootDir NodeID

	globals.alreadyLoggedIgnoring.setAttrInValidFH = false

	globals.volume = fission.NewVolume(globals.volumeName, globals.mountPoint, fuseSubtype, initOutMaxWrite, &globals, globals.logger, globals.errChan)

	err = globals.volume.DoMount()
	if nil != err {
		globals.logger.Printf("fission.DoMount() failed: %v", err)
		os.Exit(1)
	}

	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	select {
	case _ = <-signalChan:
		// Normal termination due to one of the above registered signals
	case err = <-globals.errChan:
		// Unexpected exit of /dev/fuse read loop since it's before we call DoUnmount()
		globals.logger.Printf("unexpected exit of /dev/fuse read loop: %v", err)
	}

	err = globals.volume.DoUnmount()
	if nil != err {
		globals.logger.Printf("fission.DoUnmount() failed: %v", err)
		os.Exit(2)
	}
}

func makeTryLock() (tryLock *tryLockStruct) {
	tryLock = &tryLockStruct{lockChan: make(chan struct{}, 1)}
	tryLock.lockChan <- struct{}{}
	return
}

func makeGrantedLockSet() (grantedLockSet *grantedLockSetStruct) {
	grantedLockSet = &grantedLockSetStruct{set: make(map[*tryLockStruct]uint64)}
	return
}

func (grantedLockSet *grantedLockSetStruct) get(tryLock *tryLockStruct) {
	var (
		lockCount uint64
		ok        bool
	)

	lockCount, ok = grantedLockSet.set[tryLock]

	if ok {
		lockCount++
		grantedLockSet.set[tryLock] = lockCount
		return
	}

	_ = <-tryLock.lockChan

	grantedLockSet.set[tryLock] = 1
}

func (grantedLockSet *grantedLockSetStruct) try(tryLock *tryLockStruct) (granted bool) {
	var (
		lockCount uint64
		ok        bool
	)

	lockCount, ok = grantedLockSet.set[tryLock]

	if ok {
		lockCount++
		grantedLockSet.set[tryLock] = lockCount
		granted = true
		return
	}

	select {
	case _ = <-tryLock.lockChan:
		granted = true
		grantedLockSet.set[tryLock] = 1
	default:
		granted = false
	}

	return
}

func (grantedLockSet *grantedLockSetStruct) free(tryLock *tryLockStruct) {
	var (
		lockCount uint64
	)

	lockCount = grantedLockSet.set[tryLock]

	lockCount--

	if 0 == lockCount {
		tryLock.lockChan <- struct{}{}
		delete(grantedLockSet.set, tryLock)
	} else {
		grantedLockSet.set[tryLock] = lockCount
	}
}

func (grantedLockSet *grantedLockSetStruct) freeAll(andDelay bool) {
	var (
		tryLock        *tryLockStruct
		tryLockBackoff time.Duration
	)

	for tryLock = range grantedLockSet.set {
		tryLock.lockChan <- struct{}{}
		delete(grantedLockSet.set, tryLock)
	}

	if andDelay {
		tryLockBackoff = tryLockBackoffMin + time.Duration(rand.Int63n(int64(tryLockBackoffMax)-int64(tryLockBackoffMin)+1))
		time.Sleep(tryLockBackoff)
	}
}

func unixTimeToGoTime(unixTimeSec uint64, unixTimeNSec uint32) (goTime time.Time) {
	goTime = time.Unix(int64(unixTimeSec), int64(unixTimeNSec))
	return
}

func goTimeToUnixTime(goTime time.Time) (unixTimeSec uint64, unixTimeNSec uint32) {
	var (
		unixTime uint64
	)
	unixTime = uint64(goTime.UnixNano())
	unixTimeSec = unixTime / 1e9
	unixTimeNSec = uint32(unixTime - (unixTimeSec * 1e9))
	return
}

func unixTimeNow() (unixTimeNowSec uint64, unixTimeNowNSec uint32) {
	unixTimeNowSec, unixTimeNowNSec = goTimeToUnixTime(time.Now())
	return
}

func cloneByteSlice(inBuf []byte) (outBuf []byte) {
	outBuf = make([]byte, len(inBuf))
	if 0 != len(inBuf) {
		_ = copy(outBuf, inBuf)
	}
	return
}

func (dummy *xattrMapDummyStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString = string(key.([]byte)[:])
	err = nil
	return
}

func (dummy *xattrMapDummyStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsString = fmt.Sprintf("%v", value.([]byte))
	err = nil
	return
}

func (dummy *dirEntryMapDummyStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString = string(key.([]byte)[:])
	err = nil
	return
}

func (dummy *dirEntryMapDummyStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsString = fmt.Sprintf("%d", value.(uint64))
	err = nil
	return
}
