package retryrpc

import (
	"container/list"
	"fmt"
	"io"
	"net"
	"unsafe"

	"github.com/swiftstack/ProxyFS/logger"
)

// TODO - remove
// Variable to control debug output
var printDebugLogs bool = false
var debugPutGet bool = false

// TODO - Algorithm - Standard server stuff
// 1. goroutine GR1 accepts new connection and does "go GR2(conn)
// 2. GR2 adds request to server.pendingRequest, unmarshals
//    request and does "go GR3(process RPC)"
// 3. GR2 proceses the RPC, grabs server lock and moves request from
//    server.pendingRequest to server.completedRequest, releases lock
//    and then sends response to client if socket is still up.
// 4. request stays on server.completedRequest until s.completedTTL exceeded

// TODO - do we need to retransmit responses in order?
// What ordering guarantees do we need to enforce?
// TODO - test if Register has been called???
func (server *Server) run() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			if !server.halting {
				logger.ErrorfWithError(err, "net.Accept failed for Retry RPC listener\n")
			}
			server.listenersWG.Done()
			return
		}

		server.connWG.Add(1)

		server.connLock.Lock()
		elm := server.connections.PushBack(conn)
		server.connLock.Unlock()

		go func(myConn net.Conn, myElm *list.Element) {
			processRequest(myConn)
			server.connLock.Lock()
			server.connections.Remove(myElm)

			// There is a race condition where the connection could have been
			// closed in Down().  However, closing it twice is okay.
			myConn.Close()
			server.connLock.Unlock()
			server.connWG.Done()
		}(conn, elm)
	}

}

var dummyReq Request
var sizeOfLenField = ReqLenTyp(unsafe.Sizeof(dummyReq.Len))

func getRequest(conn net.Conn, req *Request) (err error) {
	// Read in the length of the request first
	buf := make([]byte, sizeOfLenField)
	bytesRead, err := io.ReadFull(conn, buf)

	if (ReqLenTyp(bytesRead) != sizeOfLenField) || (err != nil) {
		fmt.Printf("getRequest() - ReadFull() of length returned bytesRead: %v err: %v\n",
			bytesRead, err)
		return
	}

	// Now read the rest of the structure off the wire.
	var sizeOfJSON ReqLenTyp
	sizeOfJSON = *(*ReqLenTyp)(unsafe.Pointer(&buf[0]))
	jsonBuf := make([]byte, sizeOfJSON)
	bytesRead, err = io.ReadFull(conn, jsonBuf)

	// TODO - error handling if err != nil

	// Now unmarshal the jsonBuf
	fmt.Printf("getRequest() - buffer read is: %v\n", jsonBuf)

	/*
		reqBytes := makeBytesReq(&ctx.req)

		bytesRead, err := io.ReadFull(conn, reqBytes)
		if err != nil {
			if err != io.EOF {
				logger.Errorf("Failed to read request from the socket: %v", err)
			}
			return err
		}

		// "cast" bytes to request
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

		// For writes, get write data
		if ctx.op == WriteOp {
			if debugPutGet {
				logger.Infof("Reading %v bytes of write data, ctx.data len is %v.", ctx.req.length, len(ctx.data))
			}

			ctx.data = make([]byte, ctx.req.length)

			_, err = io.ReadFull(conn, ctx.data)
			if err != nil {
				logger.Infof("Failed to read write buffer from the socket uint64_t.")
				return err
			}
			// NOTE: Suppress for now, will be counted in next event
			//profiler.AddEventNow("after get write buf")
		}
	*/

	return nil
}

func processRequest(conn net.Conn) {
	// NOTE: This function runs in a goroutine and only processes
	//               one request at a time.
	reqStorage := Request{}
	req := &reqStorage

	if printDebugLogs {
		logger.Infof("got a connection - starting read/write io thread")
	}

	for {
		/*
			if printDebugLogs {
				logger.Infof("Waiting for RPC request; req.data size is %v.", len(ctx.data))
			}
		*/

		// Get RPC request
		err := getRequest(conn, req)
		fmt.Printf("processRequest() - getRequest() returned err: %v\n", err)

		if debugPutGet {
			logger.Infof("Got request: %+v", req)
		}

		/*
			switch ctx.op {
			case WriteOp:
				if globals.dataPathLogging || printDebugLogs {
					logger.Tracef(">> ioWrite in.{InodeHandle:{MountID:%v InodeNumber:%v} Offset:%v Buf.size:%v Buf.<buffer not printed>",
						ctx.req.mountID, ctx.req.inodeID, ctx.req.offset, len(ctx.data))
				}

				profiler.AddEventNow("before fs.Write()")
				mountHandle, err = lookupMountHandleByMountIDAsByteArray(ctx.req.mountID)
				if err == nil {
					ctx.resp.ioSize, err = mountHandle.Write(inode.InodeRootUserID, inode.InodeGroupID(0), nil, inode.InodeNumber(ctx.req.inodeID), ctx.req.offset, ctx.data, profiler)
				}
				profiler.AddEventNow("after fs.Write()")

				stats.IncrementOperationsAndBucketedBytes(stats.JrpcfsIoWrite, ctx.resp.ioSize)

				if globals.dataPathLogging || printDebugLogs {
					logger.Tracef("<< ioWrite errno:%v out.Size:%v", ctx.resp.errno, ctx.resp.ioSize)
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
				decRunningWorkers()
				return
			}

			// Save profiler with server op stats. Close it first so that save time isn't counted.
			profiler.Close()
			SaveProfiler(qserver, ctx.op, profiler)

			// XXX TODO: no sync.Pool for now, just alloc on the stack
			// Return context struct to pool
			//              ioContextPool.Put(ctx)

			// Reset data buffer size in ctx to its full size
			// ctx.data = dataStorage[:0]
			ctx.data = nil

			if printDebugLogs {
				logger.Infof("Done with op, back to beginning")
			}

			decRunningWorkers()
		*/
	}
}
