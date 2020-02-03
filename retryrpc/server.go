package retryrpc

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/swiftstack/ProxyFS/logger"
	"golang.org/x/sys/unix"
)

// Variable to control debug output
var printDebugLogs bool = false
var debugPutGet bool = false

// TODO - test if Register has been called???

func (server *Server) run() {
	defer server.goroutineWG.Done()
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			if !server.halting {
				logger.ErrorfWithError(err, "net.Accept failed for Retry RPC listener")
			}
			server.listenersWG.Done()
			return
		}

		server.connWG.Add(1)

		server.connLock.Lock()
		elm := server.connections.PushBack(conn)
		server.connLock.Unlock()

		server.goroutineWG.Add(1)
		go func(myConn net.Conn, myElm *list.Element) {
			defer server.goroutineWG.Done()

			server.serviceClient(myConn)

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

func keyString(myUniqueID string, rID requestID) string {
	return fmt.Sprintf("%v:%v", myUniqueID, rID)
}

// First check if we already completed this request by looking on completed
// and pending queues.
//
// If the request is on the pending queue then update the
// net.Conn so the response will be sent to the caller.
//
// Otherwise, call the RPC.  callRPCAndMarshal() takes care
// of adding the request to the pending queue.
func (server *Server) findQOrCallRPC(cCtx *connCtx, mui *myUniqueInfo, buf []byte, jReq *jsonRequest) (err error) {
	queueKey := keyString(jReq.MyUniqueID, jReq.RequestID)

	// First check if we already completed this request by looking at
	// completed and pending queues.  Update pending queue with new
	// net.Conn if needed.

	// TODO - be careful that drop locks appropriately!!
	mui.Lock()
	v, ok := mui.completedRequest[queueKey]
	if ok {
		// Already have answer for this in completedRequest queue.
		// Just return the results.
		mui.Unlock()
		returnResults(v, cCtx, jReq)

	} else {
		_, ok2 := mui.pendingRequest[queueKey]
		if ok2 {
			// Already on pending queue.  Replace the connCtx in the pending queue so
			// that the goroutine completing the task sends the response back to the
			// most recent connection.  Any prior connections will have been closed by
			// the client before creating a new connection.
			//
			// This goroutine simply returns.
			mui.buildAndQPendingCtx(queueKey, buf, cCtx, true)
			mui.Unlock()

		} else {
			// On neither queue - must be new request

			// Call the RPC and return the results.  callRPCAndMarshal()
			// does both for us.
			//
			// We pass buf to the call because the request will have to
			// be unmarshaled again to retrieve the parameters specific to
			// the RPC.
			mui.Unlock()

			// TODO - why return err here if already returned results to
			// client?
			err = server.callRPCAndMarshal(cCtx, mui, buf, jReq, queueKey)
		}
	}

	return
}

// processRequest is given a request from the client.
func (server *Server) processRequest(cCtx *connCtx, buf []byte) {
	defer server.goroutineWG.Done()

	// We first unmarshal the raw buf to find the method
	//
	// Next we unmarshal again with the request structure specific
	// to the RPC.
	jReq := jsonRequest{}
	unmarErr := json.Unmarshal(buf, &jReq)
	if unmarErr != nil {
		logger.Errorf("Unmarshal of buf failed with err: %v\n", unmarErr)
		return
	}

	// TODO - check if already have server.perUniqueIDInfo[myUniqueID]
	// if not, create maps for them AND highestReplySeen
	// TODO - review this locking again....

	logger.Infof("processRequest() - highestReplySeen: %v\n", jReq.HighestReplySeen)
	cCtx.Lock()
	puii, ok := server.perUniqueIDInfo[jReq.MyUniqueID]
	if !ok {
		mui := &myUniqueInfo{}
		mui.pendingRequest = make(map[string]*pendingCtx)
		mui.completedRequest = make(map[string]*ioReply)
		mui.completedRequestLRU = list.New()
		server.perUniqueIDInfo[jReq.MyUniqueID] = mui
		puii = mui
	}
	cCtx.Unlock()

	puii.Lock()
	if jReq.HighestReplySeen > puii.highestReplySeen {
		puii.highestReplySeen = jReq.HighestReplySeen
	}
	puii.Unlock()

	// Complete the request either by looking in completed queue,
	// updating cCtx of pending queue entry or by calling the
	// RPC.
	err := server.findQOrCallRPC(cCtx, puii, buf, &jReq)
	if err != nil {
		logger.Errorf("findQOrCallRPC returned err: %v", err)
	}
}

// serviceClient gets called when we accept a new connection.
// This means we have a new client connection.
func (server *Server) serviceClient(conn net.Conn) {
	var (
		halting bool
	)

	cCtx := &connCtx{conn: conn}

	if printDebugLogs {
		logger.Infof("got a connection - starting read/write io thread")
	}

	for {
		// Get RPC request
		buf, getErr := getIO(conn)
		if getErr != nil {
			server.Lock()
			halting = server.halting
			server.Unlock()
			logger.Infof("serviceClient - getIO returned err: %v - halting: %v",
				getErr, halting)

			// Drop response on the floor.   Client will either reconnect or
			// this response will age out of the queues.
			return
		}

		// No sense blocking the read of the next request,
		// push the work off on processRequest().
		//
		// Writes back on the socket wil have to be serialized so
		// pass the per connection context.
		server.goroutineWG.Add(1)
		go server.processRequest(cCtx, buf)
	}
}

func (mui *myUniqueInfo) buildAndQPendingCtx(queueKey string, buf []byte, cCtx *connCtx, lockHeld bool) {
	pc := &pendingCtx{buf: buf, cCtx: cCtx}

	if lockHeld == false {
		mui.Lock()
	}

	mui.pendingRequest[queueKey] = pc

	if lockHeld == false {
		mui.Unlock()
	}
}

// TODO - review the locking here to make it simplier

// callRPCAndMarshal calls the RPC and returns results to requestor
func (server *Server) callRPCAndMarshal(cCtx *connCtx, mui *myUniqueInfo, buf []byte, jReq *jsonRequest, queueKey string) (err error) {

	// Setup the reply structure with common fields
	reply := &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

	// Queue the request
	mui.buildAndQPendingCtx(queueKey, buf, cCtx, false)

	ma := server.svrMap[jReq.Method]
	if ma != nil {

		// Another unmarshal of buf to find the parameters specific to
		// this RPC
		typOfReq := ma.request.Elem()
		dummyReq := reflect.New(typOfReq).Interface()

		sReq := svrRequest{}
		sReq.Params[0] = dummyReq
		err = json.Unmarshal(buf, &sReq)
		if err != nil {
			logger.Errorf("Unmarshal sReq: %+v returned err: %v", sReq, err)
			return
		}
		req := reflect.ValueOf(dummyReq)

		// Create the reply structure
		typOfReply := ma.reply.Elem()
		myReply := reflect.New(typOfReply)

		// Call the method
		function := ma.methodPtr.Func
		returnValues := function.Call([]reflect.Value{server.receiver, req, myReply})

		// The return value for the method is an error.
		errInter := returnValues[0].Interface()
		if errInter == nil {
			jReply.Result = myReply.Elem().Interface()
		} else {
			e, ok := errInter.(error)
			if !ok {
				logger.PanicfWithError(err, "Call returnValues invalid cast errInter: %+v", errInter)
			}
			jReply.ErrStr = e.Error()
		}
	} else {
		// Method does not exist
		jReply.ErrStr = fmt.Sprintf("errno: %d", unix.ENOENT)
	}

	// Convert response into JSON for return trip
	reply.JResult, err = json.Marshal(jReply)
	if err != nil {
		logger.PanicfWithError(err, "Unable to marshal jReply: %+v", jReply)

	}

	lruEntry := completedLRUEntry{queueKey: queueKey, timeCompleted: time.Now()}

	mui.Lock()
	// connCtx may have changed while we dropped the lock due to new connection or
	// the RPC may have completed.
	//
	// Pull the current one from pendingRequest if queueKey exists
	pendingCtx := mui.pendingRequest[queueKey]
	if pendingCtx != nil {
		currentCCtx := pendingCtx.cCtx

		mui.completedRequest[queueKey] = reply
		mui.completedRequestLRU.PushBack(lruEntry)
		delete(mui.pendingRequest, queueKey)
		mui.Unlock()

		// Now return the results
		returnResults(reply, currentCCtx, jReq)
	} else {
		// pendingRequest was already completed
		mui.Unlock()
	}

	return
}

func returnResults(reply *ioReply, cCtx *connCtx,
	jReq *jsonRequest) {

	// Now write the response back to the client
	//
	// Serialize multiple goroutines writing on socket back to client
	// by grabbing a mutex on the context

	// We may not have the correct reply structure if we are retransmitting.
	//
	// Consider this scenario:
	//
	// t0 receive RPC on socket#1 and put on pending queue
	// t1 before the RPC is processed, the client disconnects
	// t2 client reconnects with socket#2
	// t3 client resends RPC
	// t4 server receives RPC and checks if the RPC is on pending queue.
	// It is but still lists socket#1.
	//
	// The fix is the second thread replaces the contextCtx in the pending queue
	// entry.  When the first thread completes the RPC, it pulls the most recent
	// pendingCtx off the pending queue and uses those contents to return the result.
	if reply != nil {

		// Write Len back
		cCtx.Lock()
		setupHdrReply(reply)
		binErr := binary.Write(cCtx.conn, binary.BigEndian, reply.Hdr)
		if binErr != nil {
			cCtx.Unlock()
			logger.Errorf("SERVER: binary.Write failed err: %v", binErr)
			return
		}

		// Write JSON reply
		bytesWritten, writeErr := cCtx.conn.Write(reply.JResult)
		if writeErr != nil {
			logger.Errorf("SERVER: conn.Write failed - bytesWritten: %v err: %v",
				bytesWritten, writeErr)
		}
		cCtx.Unlock()
	}
}

// Remove entries older than server.completedTTL
//
// This gets called every ~10 minutes to clean out older entries.
//
// In parallel, there are entries trimmed as the
func (server *Server) trimCompleted(t time.Time) {

	var (
		numItems int
	)

	/*
		l := list.New()

		server.Lock()
		for e := server.completedRequestLRU.Front(); e != nil; e = e.Next() {
			eTime := e.Value.(completedLRUEntry).timeCompleted.Add(server.completedLongTTL)
			if eTime.Before(t) {
				delete(server.completedRequest, e.Value.(completedLRUEntry).queueKey)

				// Push on local list so don't delete while iterating
				l.PushBack(e)
			} else {
				// Oldest is in front so just break
				break
			}
		}

		numItems = l.Len()

		// Now delete from LRU using the local list
		for e2 := l.Front(); e2 != nil; e2 = e2.Next() {
			tmpE := server.completedRequestLRU.Front()
			_ = server.completedRequestLRU.Remove(tmpE)

		}
		server.Unlock()
	*/
	logger.Infof("Completed RetryRPCs - Total items trimmed: %v", numItems)
}

// Close sockets to client so that goroutines wakeup from blocked
// reads and let the server exit.
func (server *Server) closeClientConn() {
	server.connLock.Lock()
	for e := server.connections.Front(); e != nil; e = e.Next() {
		conn := e.Value.(net.Conn)
		conn.Close()
	}
	server.connLock.Unlock()
}
