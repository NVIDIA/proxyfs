package fission

import (
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

type volumeStruct struct {
	volumeName        string
	mountpointDirPath string
	fuseSubtype       string
	initOutMaxWrite   uint32
	callbacks         Callbacks
	logger            *log.Logger
	errChan           chan error
	devFuseFDReadSize uint32 // InHeaderSize + WriteInSize + InitOut.MaxWrite
	devFuseFDReadPool sync.Pool
	devFuseFD         int
	devFuseFile       *os.File
	devFuseFDReaderWG sync.WaitGroup
	callbacksWG       sync.WaitGroup
}

func newVolume(volumeName string, mountpointDirPath string, fuseSubtype string, initOutMaxWrite uint32, callbacks Callbacks, logger *log.Logger, errChan chan error) (volume *volumeStruct) {
	volume = &volumeStruct{
		volumeName:        volumeName,
		mountpointDirPath: mountpointDirPath,
		fuseSubtype:       fuseSubtype,
		initOutMaxWrite:   initOutMaxWrite,
		callbacks:         callbacks,
		logger:            logger,
		errChan:           errChan,
		devFuseFDReadSize: InHeaderSize + WriteInFixedPortionSize + initOutMaxWrite,
	}

	volume.devFuseFDReadPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, volume.devFuseFDReadSize) // len == cap
		},
	}

	return
}

func (volume *volumeStruct) devFuseFDReadPoolGet() (devFuseFDReadBuf []byte) {
	devFuseFDReadBuf = volume.devFuseFDReadPool.Get().([]byte)
	return
}

func (volume *volumeStruct) devFuseFDReadPoolPut(devFuseFDReadBuf []byte) {
	devFuseFDReadBuf = devFuseFDReadBuf[:cap(devFuseFDReadBuf)] // len == cap
	volume.devFuseFDReadPool.Put(devFuseFDReadBuf)
}

func (volume *volumeStruct) devFuseFDReader() {
	var (
		bytesRead        int
		devFuseFDReadBuf []byte
		err              error
	)

	for {
		devFuseFDReadBuf = volume.devFuseFDReadPoolGet()

	RetrySyscallRead:
		bytesRead, err = syscall.Read(volume.devFuseFD, devFuseFDReadBuf)
		if nil != err {
			// First check for EINTR

			if 0 == strings.Compare("interrupted system call", err.Error()) {
				goto RetrySyscallRead
			}

			// Now that we are not retrying syscall.Read(), discard devFuseFDReadBuf

			volume.devFuseFDReadPoolPut(devFuseFDReadBuf)

			if 0 == strings.Compare("operation not permitted", err.Error()) {
				// Special case... simply retry the Read
				continue
			}

			// Time to exit...but first await outstanding Callbacks

			volume.callbacksWG.Wait()
			volume.devFuseFDReaderWG.Done()

			// Signal errChan that we are exiting (passing <nil> if due to close of volume.devFuseFD)

			if 0 == strings.Compare("no such device", err.Error()) {
				volume.errChan <- nil
			} else if 0 == strings.Compare("operation not supported by device", err.Error()) {
				volume.errChan <- nil
			} else {
				volume.logger.Printf("Exiting due to /dev/fuse Read err: %v", err)
				volume.errChan <- err
			}

			return
		}

		devFuseFDReadBuf = devFuseFDReadBuf[:bytesRead]

		// Dispatch goroutine to process devFuseFDReadBuf

		volume.callbacksWG.Add(1)
		go volume.processDevFuseFDReadBuf(devFuseFDReadBuf)
	}
}

func (volume *volumeStruct) processDevFuseFDReadBuf(devFuseFDReadBuf []byte) {
	var (
		inHeader *InHeader
	)

	if len(devFuseFDReadBuf) < InHeaderSize {
		// All we can do is just drop it
		volume.logger.Printf("Read malformed message from /dev/fuse")
		volume.devFuseFDReadPoolPut(devFuseFDReadBuf)
		volume.callbacksWG.Done()
		return
	}

	inHeader = &InHeader{
		Len:     *(*uint32)(unsafe.Pointer(&devFuseFDReadBuf[0])),
		OpCode:  *(*uint32)(unsafe.Pointer(&devFuseFDReadBuf[4])),
		Unique:  *(*uint64)(unsafe.Pointer(&devFuseFDReadBuf[8])),
		NodeID:  *(*uint64)(unsafe.Pointer(&devFuseFDReadBuf[16])),
		UID:     *(*uint32)(unsafe.Pointer(&devFuseFDReadBuf[24])),
		GID:     *(*uint32)(unsafe.Pointer(&devFuseFDReadBuf[28])),
		PID:     *(*uint32)(unsafe.Pointer(&devFuseFDReadBuf[32])),
		Padding: *(*uint32)(unsafe.Pointer(&devFuseFDReadBuf[36])),
	}

	switch inHeader.OpCode {
	case OpCodeLookup:
		volume.doLookup(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeForget:
		volume.doForget(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeGetAttr:
		volume.doGetAttr(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeSetAttr:
		volume.doSetAttr(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeReadLink:
		volume.doReadLink(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeSymLink:
		volume.doSymLink(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeMkNod:
		volume.doMkNod(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeMkDir:
		volume.doMkDir(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeUnlink:
		volume.doUnlink(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeRmDir:
		volume.doRmDir(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeRename:
		volume.doRename(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeLink:
		volume.doLink(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeOpen:
		volume.doOpen(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeRead:
		volume.doRead(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeWrite:
		volume.doWrite(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeStatFS:
		volume.doStatFS(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeRelease:
		volume.doRelease(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeFSync:
		volume.doFSync(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeSetXAttr:
		volume.doSetXAttr(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeGetXAttr:
		volume.doGetXAttr(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeListXAttr:
		volume.doListXAttr(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeRemoveXAttr:
		volume.doRemoveXAttr(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeFlush:
		volume.doFlush(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeInit:
		volume.doInit(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeOpenDir:
		volume.doOpenDir(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeReadDir:
		volume.doReadDir(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeReleaseDir:
		volume.doReleaseDir(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeFSyncDir:
		volume.doFSyncDir(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeGetLK:
		volume.doGetLK(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeSetLK:
		volume.doSetLK(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeSetLKW:
		volume.doSetLKW(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeAccess:
		volume.doAccess(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeCreate:
		volume.doCreate(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeInterrupt:
		volume.doInterrupt(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeBMap:
		volume.doBMap(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeDestroy:
		volume.doDestroy(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeIoCtl:
		volume.devFuseFDWriter(inHeader, syscall.EINVAL)
	case OpCodePoll:
		volume.doPoll(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeBatchForget:
		volume.doBatchForget(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeFAllocate:
		volume.doFAllocate(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeReadDirPlus:
		volume.doReadDirPlus(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeRename2:
		volume.doRename2(inHeader, devFuseFDReadBuf[InHeaderSize:])
	case OpCodeLSeek:
		volume.doLSeek(inHeader, devFuseFDReadBuf[InHeaderSize:])
	default:
		volume.devFuseFDWriter(inHeader, syscall.ENOSYS)
	}

	volume.devFuseFDReadPoolPut(devFuseFDReadBuf)
	volume.callbacksWG.Done()
}

func (volume *volumeStruct) devFuseFDWriter(inHeader *InHeader, errno syscall.Errno, bufs ...[]byte) {
	var (
		buf          []byte
		bytesWritten uintptr
		iovec        []syscall.Iovec
		iovecSpan    uintptr
		outHeader    []byte
	)

	// First, log any syscall.ENOSYS responses

	if syscall.ENOSYS == errno {
		volume.logger.Printf("Read unsupported/unrecognized message OpCode == %v", inHeader.OpCode)
	}

	// Construct outHeader w/out knowing iovecSpan and put it in iovec[0]

	outHeader = make([]byte, OutHeaderSize)

	iovecSpan = uintptr(OutHeaderSize)

	*(*uint32)(unsafe.Pointer(&outHeader[0])) = uint32(iovecSpan) // Updated later
	*(*int32)(unsafe.Pointer(&outHeader[4])) = -int32(errno)
	*(*uint64)(unsafe.Pointer(&outHeader[8])) = inHeader.Unique

	iovec = make([]syscall.Iovec, 1, len(bufs)+1)

	iovec[0] = syscall.Iovec{Base: &outHeader[0], Len: uint64(OutHeaderSize)}

	// Construct iovec elements for supplied bufs (if any)

	for _, buf = range bufs {
		if 0 != len(buf) {
			iovec = append(iovec, syscall.Iovec{Base: &buf[0], Len: uint64(len(buf))})
			iovecSpan += uintptr(len(buf))
		}
	}

	// Now go back and update outHeader

	*(*uint32)(unsafe.Pointer(&outHeader[0])) = uint32(iovecSpan)

	// Finally, send iovec to /dev/fuse

RetrySyscallWriteV:
	bytesWritten, _, errno = syscall.Syscall(
		syscall.SYS_WRITEV,
		uintptr(volume.devFuseFD),
		uintptr(unsafe.Pointer(&iovec[0])),
		uintptr(len(iovec)))
	if 0 == errno {
		if bytesWritten != iovecSpan {
			volume.logger.Printf("Write to /dev/fuse returned bad bytesWritten: %v", bytesWritten)
		}
	} else {
		if syscall.EINTR == errno {
			goto RetrySyscallWriteV
		}
		volume.logger.Printf("Write to /dev/fuse returned bad errno: %v", errno)
	}
}

func cloneByteSlice(inBuf []byte, andTrimTrailingNullByte bool) (outBuf []byte) {
	outBuf = make([]byte, len(inBuf))
	if 0 != len(inBuf) {
		_ = copy(outBuf, inBuf)
		if andTrimTrailingNullByte && (0 == outBuf[len(outBuf)-1]) {
			outBuf = outBuf[:len(outBuf)-1]
		}
	}
	return
}
