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
	rID := jReq.RequestID

	// First check if we already completed this request by looking at
	// completed and pending queues.  Update pending queue with new
	// net.Conn if needed.

	// TODO - be careful that drop locks appropriately!!
	mui.Lock()
	v, ok := mui.completedRequest[rID]
	if ok {
		// Already have answer for this in completedRequest queue.
		// Just return the results.
		mui.Unlock()
		returnResults(v, cCtx, jReq)

	} else {
		_, ok2 := mui.pendingRequest[rID]
		if ok2 {
			// Already on pending queue.  Replace the connCtx in the pending queue so
			// that the goroutine completing the task sends the response back to the
			// most recent connection.  Any prior connections will have been closed by
			// the client before creating a new connection.
			//
			// This goroutine simply returns.
			mui.buildAndQPendingCtx(rID, buf, cCtx, true)
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
			err = server.callRPCAndMarshal(cCtx, mui, buf, jReq, rID)
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

	logger.Infof("processRequest() - highestReplySeen: %v\n", jReq.HighestReplySeen)
	server.Lock()
	mui, ok := server.perUniqueIDInfo[jReq.MyUniqueID]
	if !ok {
		m := &myUniqueInfo{}
		m.pendingRequest = make(map[requestID]*pendingCtx)
		m.completedRequest = make(map[requestID]*completedEntry)
		m.completedRequestLRU = list.New()
		server.perUniqueIDInfo[jReq.MyUniqueID] = m
		mui = m
	}
	server.Unlock()

	mui.Lock()
	if jReq.HighestReplySeen > mui.highestReplySeen {
		mui.highestReplySeen = jReq.HighestReplySeen
	}
	mui.Unlock()

	// Complete the request either by looking in completed queue,
	// updating cCtx of pending queue entry or by calling the
	// RPC.
	err := server.findQOrCallRPC(cCtx, mui, buf, &jReq)
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

// TODO - review the locking here to make it simplier

// callRPCAndMarshal calls the RPC and returns results to requestor
func (server *Server) callRPCAndMarshal(cCtx *connCtx, mui *myUniqueInfo, buf []byte, jReq *jsonRequest, rID requestID) (err error) {

	// Setup the reply structure with common fields
	reply := &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

	// Queue the request
	mui.buildAndQPendingCtx(rID, buf, cCtx, false)

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

	lruEntry := completedLRUEntry{requestID: rID, timeCompleted: time.Now()}

	mui.Lock()
	// connCtx may have changed while we dropped the lock due to new connection or
	// the RPC may have completed.
	//
	// Pull the current one from pendingRequest if queueKey exists
	pendingCtx := mui.pendingRequest[rID]
	if pendingCtx != nil {
		currentCCtx := pendingCtx.cCtx

		ce := &completedEntry{reply: reply}

		mui.completedRequest[rID] = ce
		le := mui.completedRequestLRU.PushBack(lruEntry)
		ce.lruElem = le
		delete(mui.pendingRequest, rID)
		mui.Unlock()

		// Now return the results
		returnResults(ce, currentCCtx, jReq)
	} else {
		// pendingRequest was already completed
		mui.Unlock()
	}

	return
}

func returnResults(ce *completedEntry, cCtx *connCtx,
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
	if ce.reply != nil {

		// Write Len back
		cCtx.Lock()
		setupHdrReply(ce.reply)
		binErr := binary.Write(cCtx.conn, binary.BigEndian, ce.reply.Hdr)
		if binErr != nil {
			cCtx.Unlock()
			logger.Errorf("SERVER: binary.Write failed err: %v", binErr)
			return
		}

		// Write JSON reply
		bytesWritten, writeErr := cCtx.conn.Write(ce.reply.JResult)
		if writeErr != nil {
			logger.Errorf("SERVER: conn.Write failed - bytesWritten: %v err: %v",
				bytesWritten, writeErr)
		}
		cCtx.Unlock()
	}
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

// Loop through all clients and trim up to already ACKed
func (server *Server) trimCompleted(t time.Time, long bool) {
	var (
		totalItems int
	)

	server.Lock()
	if long {
		l := list.New()
		for k, v := range server.perUniqueIDInfo {
			n := server.trimTLLBased(k, v, t)
			totalItems += n

			v.Lock()
			if v.isEmpty() {
				l.PushBack(k)
			}
			v.Unlock()

		}

		// If the client is no longer active - delete it's entry
		//
		// We can only do the check if we are currently holding the
		// lock.
		for e := l.Front(); e != nil; e = e.Next() {
			key := e.Value.(string)
			v := server.perUniqueIDInfo[key]

			v.Lock()
			if v.isEmpty() {
				delete(server.perUniqueIDInfo, key)
			}
			v.Unlock()
		}
		logger.Infof("Trimmed completed RetryRpcs - Total: %v", totalItems)
	} else {
		for k, v := range server.perUniqueIDInfo {
			n := server.trimAClientBasedACK(k, v)
			totalItems += n
		}
	}
	server.Unlock()
}

// Walk through client and trim completedRequest based either
// on TTL or RequestID acknowledgement from client.
//
// NOTE: We assume Server Lock is held
func (server *Server) trimAClientBasedACK(uniqueID string, mui *myUniqueInfo) (numItems int) {

	mui.Lock()

	// Remove from completedRequest completedRequestLRU
	for h := mui.previousHighestReplySeen + 1; h <= mui.highestReplySeen; h++ {
		v := mui.completedRequest[h]
		mui.completedRequestLRU.Remove(v.lruElem)
		delete(mui.completedRequest, h)
		numItems++
	}

	// Keep track of how far we have trimmed for next run
	mui.previousHighestReplySeen = mui.highestReplySeen
	mui.Unlock()
	return
}

// Remove completedRequest/completedRequestLRU entries older than server.completedTTL
//
// This gets called every ~10 minutes to clean out older entries.
//
// NOTE: We assume Server Lock is held
func (server *Server) trimTLLBased(uniqueID string, mui *myUniqueInfo, t time.Time) (numItems int) {

	l := list.New()

	mui.Lock()
	for e := mui.completedRequestLRU.Front(); e != nil; e = e.Next() {
		eTime := e.Value.(completedLRUEntry).timeCompleted.Add(server.completedLongTTL)
		if eTime.Before(t) {
			delete(mui.completedRequest, e.Value.(completedLRUEntry).requestID)

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
		tmpE := mui.completedRequestLRU.Front()
		_ = mui.completedRequestLRU.Remove(tmpE)

	}
	mui.Unlock()
	return
}
