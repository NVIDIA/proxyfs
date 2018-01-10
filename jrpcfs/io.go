package jrpcfs

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
	"unsafe"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/refcntpool"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/utils"
)

// Server handle, used to track io-related ops
var qserver *Server

var ioListener net.Listener

func ioServerUp(ipAddr string, fastPortString string) {
	var err error

	qserver = NewServer()

	ioListener, err = net.Listen("tcp", net.JoinHostPort(ipAddr, fastPortString))
	if err != nil {
		logger.ErrorfWithError(err, "net.Listen %s:%s failed", ipAddr, fastPortString)
		return
	}

	//logger.Infof("Starting to listen on %s:%s", ipAddr, fastPortString)
	go ioServerLoop()
}

func ioServerLoop() {
	for {
		conn, err := ioListener.Accept()
		if err != nil {
			logger.ErrorfWithError(err, "net.Accept failed for IO listener\n")
			return
		}

		go ioHandle(conn)
	}
}

func ioServerDown() {
	DumpIfNecessary(qserver)
	dumpRunningWorkers()
	stopServerProfiling(qserver)
	ioListener.Close()
}

var debugConcurrency = false

var concWorkerLock sync.Mutex
var numConcWorkers int = 0 // number of concurrent workers
var hwmConcWorkers int = 0 // high water mark of concurrent workers

var timesIncWorkers int = 0
var timesDecWorkers int = 0
var timesLevelEntered []int = make([]int, 128)
var timesLevelExited []int = make([]int, 128)

var concDuration []time.Duration = make([]time.Duration, 128)
var concStopwatch []utils.Stopwatch = make([]utils.Stopwatch, 128)

func enterWorkerLevel(level int) {
	timesLevelEntered[level]++

	concStopwatch[level].Restart()
}

func exitWorkerLevel(level int) {
	timesLevelExited[level]++

	if concStopwatch[level].IsRunning {
		concStopwatch[level].Stop()
		concDuration[level] += concStopwatch[level].Elapsed()
	}
}

func incRunningWorkers() {
	globals.gate.RLock()

	if debugConcurrency {
		concWorkerLock.Lock()
		timesIncWorkers++

		// Record how long we spent in the previous level
		exitWorkerLevel(numConcWorkers)

		numConcWorkers++

		// Time how long we are at this level start the clock
		enterWorkerLevel(numConcWorkers)

		if numConcWorkers > hwmConcWorkers {
			hwmConcWorkers = numConcWorkers
		}
		concWorkerLock.Unlock()
	}
}

func decRunningWorkers() {
	if debugConcurrency {
		concWorkerLock.Lock()
		timesDecWorkers++

		// Record how long we spent in the previous level
		exitWorkerLevel(numConcWorkers)

		numConcWorkers--

		// Time how long we are at this level start the clock
		enterWorkerLevel(numConcWorkers)

		concWorkerLock.Unlock()
	}

	globals.gate.RUnlock()
}

// It'd be nice if the time package supported a check for nil duration, but it doesn't.
var nilDuration time.Duration

func dumpRunningWorkers() {
	if !debugConcurrency {
		return
	}

	var totalTimeMs int64 = 0

	fmt.Printf("running workers: %d, max running workers: %d\n", numConcWorkers, hwmConcWorkers)

	for i := range concDuration {
		if concDuration[i] != nilDuration {
			timeMs := concDuration[i].Nanoseconds() / int64(time.Millisecond)
			totalTimeMs += int64(i) * timeMs
			fmt.Printf("  %v workers: %v ms\n", i, timeMs)
		}
	}

	fmt.Printf("  total worker-thread runtime: %v ms\n", totalTimeMs)

	fmt.Printf("  times incWorkers called: %v decWorkers called: %v\n", timesIncWorkers, timesDecWorkers)
	for i := range timesLevelEntered {
		if timesLevelEntered[i] > 0 {
			fmt.Printf("  level %v entered: %d exited %v\n", i, timesLevelEntered[i], timesLevelExited[i])
		}
	}
}

// Variable to control debug output
var printDebugLogs bool = false
var debugPutGet bool = false

func getRequest(conn net.Conn, ctx *ioContext) (err error) {
	// "cast" request to bytes before reading into it
	reqBytes := makeBytesReq(&ctx.req)

	bytesRead, err := io.ReadFull(conn, reqBytes)
	if err != nil {
		if err != io.EOF {
			logger.Errorf("Failed to read request from the socket: %v", err)
		}
		return err
	}

	// "cast" bytes to request and copy them to &ctx.req
	makeReq(reqBytes, &ctx.req)

	if debugPutGet {
		logger.Infof("Got %v bytes, request: %+v", bytesRead, ctx.req)
	}

	if ctx.req.opType == 1001 {
		// Write op
		ctx.op = WriteOp
	} else if ctx.req.opType == 1002 {
		// Read op
		ctx.op = ReadOp
	} else {
		return fmt.Errorf("getRequest: unsupported op %v!", ctx.req.opType)
	}

	// get a reference counted buffer to hold the read or write data
	ctx.data = qserver.refCntBufPools.GetRefCntBuf(int(ctx.req.length))

	// For writes, get write data
	if ctx.op == WriteOp {
		if debugPutGet {
			logger.Infof("Reading %v bytes of write data, ctx.data len is %v.",
				ctx.req.length, len(ctx.data.Buf))
		}

		// set the size of the buffer to the data to be read
		ctx.data.Buf = ctx.data.Buf[0:ctx.req.length]
		_, err = io.ReadFull(conn, ctx.data.Buf)
		if err != nil {
			logger.Infof("Failed to read write buffer from the socket uint64_t.")
			return err
		}
		// NOTE: Suppress for now, will be counted in next event
		//profiler.AddEventNow("after get write buf")
	}

	return nil
}

func putResponseWrite(conn net.Conn, buf []byte) (err error) {
	var (
		currentIndex    = int(0)
		limitIndex      = len(buf)
		numBytesWritten int
	)

	err = nil // default... in case cur == len(buf) already

	for currentIndex < limitIndex {
		numBytesWritten, err = conn.Write(buf[currentIndex:])
		if nil != err {
			return
		}
		currentIndex += numBytesWritten
	}

	return
}

func putResponse(conn net.Conn, ctx *ioContext) (err error) {
	var (
		respBytes []byte
	)

	if (ctx.op != ReadOp) && (ctx.op != WriteOp) {
		// We only support read and write
		return fmt.Errorf("putResponse: unsupported opType %v", ctx.op)
	}

	// NOTE: the far end expects errno, ioSize, (if a read), a buffer

	// "cast" response to bytes, copy it, and assing to respBytes
	respBytes = makeBytesResp(&ctx.resp)

	// Send response header
	err = putResponseWrite(conn, respBytes)
	if nil != err {
		logger.Infof("putResponse() failed to send ctx.resp: %v", err)
		return
	}

	// If (non-zero length) Read Payload, send it as well
	if (ctx.op == ReadOp) && (len(ctx.data.Buf) > 0) {
		err = putResponseWrite(conn, ctx.data.Buf)
		if nil != err {
			logger.Infof("putResponse() failed to send ctx.data: %v", err)
			return
		}
	}

	err = nil
	return
}

func putUint64(conn net.Conn, fieldName string, value uint64, field []byte) (err error) {
	return putUint64Profiled(conn, fieldName, value, field, nil)
}

// Rather than allocate byte slice to write from each time we are called, we
// try to be more efficient by reusing a byte slice that is passed in by the caller.
func putUint64Profiled(conn net.Conn, fieldName string, value uint64, field []byte, profiler *utils.Profiler) (err error) {

	// "cast" value to bytes before sending it
	valBytes := makeBytesUint64(value)
	bytesWanted := len(valBytes)

	nBytes, err := conn.Write(valBytes)
	profiler.AddEventNow("after conn.Write")
	if err != nil {
		logger.Infof("Failed to write %v", fieldName)
		return
	}
	if nBytes != bytesWanted {
		logger.Infof("Error, wanted %v bytes, got %v", bytesWanted, nBytes)
		return fmt.Errorf("Error, wanted %v bytes, got %v", bytesWanted, nBytes)
	}

	if debugPutGet {
		logger.Infof("Wrote %v bytes, %v = 0x%x.", nBytes, fieldName, value)
	}
	return
}

type ioRequest struct {
	opType  uint64
	mountID uint64
	inodeID uint64
	offset  uint64
	length  uint64
}

type ioResponse struct {
	errno  uint64 //out
	ioSize uint64 //out
}

type ioContext struct {
	op   OpType
	req  ioRequest
	resp ioResponse
	// read/writeData buf* (in: write; out: read)
	// Ideally this would be a pointer (?)
	data *refcntpool.RefCntBuf
}

const ioRequestSize int = 8 * 5
const ioResponseSize int = 8 * 2

func makeBytesReq(req *ioRequest) []byte {
	mem := *(*[ioRequestSize]byte)(unsafe.Pointer(req))
	return mem[:]
}

func makeBytesResp(resp *ioResponse) []byte {
	mem := *(*[ioResponseSize]byte)(unsafe.Pointer(resp))
	return mem[:]
}

func makeBytesUint64(value uint64) []byte {
	mem := *(*[8]byte)(unsafe.Pointer(&value))
	return mem[:]
}

func makeReq(bytes []byte, req *ioRequest) {
	*req = *(*ioRequest)(unsafe.Pointer(&bytes[0]))
}

func ioHandle(conn net.Conn) {
	var (
		mountHandle fs.MountHandle
	)

	// NOTE: Allocate 64k buffer and context on the stack; this function runs in a goroutine
	//       and only processes one request at a time.
	//
	//var dataStorage [64 * 1024]byte
	ctxStorage := ioContext{op: InvalidOp}
	ctx := &ctxStorage
	// XXX TODO: no sync.Pool for now, just alloc on the stack

	if printDebugLogs {
		logger.Infof("got a connection - starting read/write io thread")
	}

	for {
		if printDebugLogs {
			logger.Infof("Waiting for RPC request; ctx.data size is %v.", len(ctx.data.Buf))
		}

		// XXX TODO: no sync.Pool for now, just alloc on the stack
		// Get a context struct to use
		//ctx := ioContextPool.Get()

		// Get RPC request
		err := getRequest(conn, ctx)
		// NOTE: Suppress this for now, we're seeing not much time spent up to here
		//profiler.AddEventNow("after get request")
		if err != nil {
			//logger.Infof("Connection terminated; returning.")
			return
		}

		// Wait until here to increment this as a worker; else we count wait time as work time.
		incRunningWorkers()

		// Taking stats *after* socket read, because otherwise we unintentionally count wait time.
		profiler := utils.NewProfilerIf(doProfiling, "") // We don't know the op type yet, gets set by SaveProfiler().

		if debugPutGet {
			logger.Infof("Got request: %+v", ctx.req)
		}

		switch ctx.op {
		case WriteOp:
			if globals.dataPathLogging || printDebugLogs {
				logger.Tracef(">> ioWrite in.{InodeHandle:{MountID:%v InodeNumber:%v} Offset:%v Buf.size:%v Buf.<buffer not printed>",
					ctx.req.mountID, ctx.req.inodeID, ctx.req.offset, len(ctx.data.Buf))
			}

			profiler.AddEventNow("before fs.WriteAsSlice()")
			mountHandle, err = lookupMountHandle(ctx.req.mountID)
			if err == nil {
				ctx.resp.ioSize, err = mountHandle.WriteAsSlice(
					inode.InodeRootUserID, inode.InodeRootGroupID, nil,
					inode.InodeNumber(ctx.req.inodeID), ctx.req.offset, ctx.data.Buf, profiler)
			}
			profiler.AddEventNow("after fs.WriteAsSlice()")

			stats.IncrementOperationsAndBucketedBytes(stats.JrpcfsIoWrite, ctx.resp.ioSize)

			if globals.dataPathLogging || printDebugLogs {
				logger.Tracef("<< ioWrite errno:%v out.Size:%v", ctx.resp.errno, ctx.resp.ioSize)
			}

		case ReadOp:
			if globals.dataPathLogging || printDebugLogs {
				logger.Tracef(">> ioRead in.{InodeHandle:{MountID:%v InodeNumber:%v} Offset:%v Length:%v}", ctx.req.mountID, ctx.req.inodeID, ctx.req.offset, ctx.req.length)
			}

			profiler.AddEventNow("before fs.Read()")
			mountHandle, err = lookupMountHandle(ctx.req.mountID)
			if err == nil {
				ctx.data.Buf, err = mountHandle.ReadReturnSlice(inode.InodeRootUserID, inode.InodeRootGroupID, nil, inode.InodeNumber(ctx.req.inodeID), ctx.req.offset, ctx.req.length, profiler)
			}
			profiler.AddEventNow("after fs.Read()")

			// Set io size in response
			ctx.resp.ioSize = uint64(len(ctx.data.Buf))

			stats.IncrementOperationsAndBucketedBytes(stats.JrpcfsIoRead, ctx.resp.ioSize)

			if globals.dataPathLogging || printDebugLogs {
				logger.Tracef("<< ioRead errno:%v out.Buf.size:%v out.Buf.<buffer not printed>",
					ctx.resp.errno, len(ctx.data.Buf))
			}

		default:
			// Hmmm, this should have been caught by getRequest...
			logger.Errorf("Error, unsupported op %v", ctx.op)
			decRunningWorkers()
			return
		}

		// Set error in context
		ctx.resp.errno = uint64(blunder.Errno(err))

		// Write response
		err = putResponse(conn, ctx)

		// XXX TODO: Enable if we want to see this event specifically.
		//           Otherwise this will show up under "remaining time".
		//profiler.AddEventNow("after rpc send response")
		if err != nil {
			logger.InfoWithError(err, "ioHandle(): putResponse() failed")
		}

		// Save profiler with server op stats. Close it first so that save time isn't counted.
		profiler.Close()
		SaveProfiler(qserver, ctx.op, profiler)

		// XXX TODO: no sync.Pool for now, just alloc on the stack
		// Return context struct to pool
		//		ioContextPool.Put(ctx)

		// Reset data buffer size in ctx to its full size
		// ctx.data = dataStorage[:0]
		ctx.data.Release()
		ctx.data = nil

		if printDebugLogs {
			logger.Infof("Done with op, back to beginning")
		}

		decRunningWorkers()
	}
}
