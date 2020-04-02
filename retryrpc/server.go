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

		// The first message sent on this socket by the client is the uniqueID.
		// Read that first and create the relevant structures before calling
		// serviceClient().  We do the cleanup in this routine because there are
		// race conditions with noisy clients repeatedly reconnected.
		//
		// Those race conditions are resolved if we serialize the recovery.
		cCtx := &connCtx{conn: conn}
		ci, err := server.getAndCreateClientID(cCtx)
		if err != nil {
			fmt.Printf("=====>>>> getAndCreateClientID() returned err: %v\n", err)
			// Socket already had an error - just loop back
			continue
		}

		server.goroutineWG.Add(1)
		go func(myConn net.Conn, myElm *list.Element) {
			defer server.goroutineWG.Done()

			server.serviceClient(ci, cCtx)

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

// processRequest is given a request from the client.
func (server *Server) processRequest(ci *clientInfo, myConnCtx *connCtx, buf []byte) {
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

	// TODO - this gets removed!!!
	/*
			server.Lock()
			ci, ok := server.perClientInfo[jReq.MyUniqueID]
			if !ok {
				// First time we have seen this client
				c := &clientInfo{cCtx: myConnCtx}
				c.completedRequest = make(map[requestID]*completedEntry)
				c.completedRequestLRU = list.New()
				c.drainingCond = sync.NewCond(&c.drainingMutex)
				server.perClientInfo[jReq.MyUniqueID] = c
				ci = c
				myConnCtx.Lock()
				myConnCtx.ci = ci
				myConnCtx.Unlock()
			}
			server.Unlock()

		ci.Lock()
		currentCtx := ci.cCtx

		// Check if existing client with new connection.  If so,
		// wait for prior RPCs to complete before proceeding.
		if currentCtx.conn != myConnCtx.conn {

			// Serialize multiple goroutines processing same NEW connection.
			if ci.drainingRPCs == true {
				ci.Unlock()

				// This goroutine is not the first to see new connection.
				// Wait for first goroutine to finish.
				ci.drainingMutex.Lock()
				ci.drainingCond.Wait()
				ci.drainingMutex.Unlock()

				ci.Lock()
			} else {
				// First goroutine to see new connection
				ci.drainingRPCs = true
				ci.Unlock()

				// New socket - block until threads from PRIOR connection
				// complete to make the recovery more predictable
				currentCtx.activeRPCsWG.Wait()

				// RPCs from PRIOR socket have completed - now take over with new
				// connection.
				//
				// Two different goroutines using same NEW connection could be racing.
				// Therefore, only update ci.cCtx if we have not already updated it.
				ci.Lock()
				ci.cCtx = myConnCtx

				// Wakeup other goroutines trying to process new connection
				ci.drainingMutex.Lock()
				ci.drainingCond.Broadcast()
				ci.drainingMutex.Unlock()
			}
		}
	*/

	ci.Lock()

	// Keep track of the highest consecutive requestID seen
	// by client.  We use this to trim completedRequest list.
	//
	// Messages could arrive out of order so only update if
	// the new request is giving us a higher value.
	if jReq.HighestReplySeen > ci.highestReplySeen {
		ci.highestReplySeen = jReq.HighestReplySeen
	}

	// First check if we already completed this request by looking at
	// completed queue.
	var localIOR ioReply // Local copy to avoid racing retransmit threads
	rID := jReq.RequestID
	ce, ok := ci.completedRequest[rID]
	if ok {
		// Already have answer for this in completedRequest queue.
		// Just return the results.
		setupHdrReply(ce.reply)
		localIOR = *ce.reply
		ci.Unlock()

	} else {
		ci.Unlock()

		// Call the RPC and return the results.
		//
		// We pass buf to the call because the request will have to
		// be unmarshaled again to retrieve the parameters specific to
		// the RPC.
		ior := server.callRPCAndFormatReply(buf, &jReq)

		// We had to drop the lock before calling the RPC since it
		// could block.
		ci.Lock()

		// Update completed queue
		ce := &completedEntry{reply: ior}
		localIOR = *ce.reply

		ci.completedRequest[rID] = ce
		setupHdrReply(ce.reply)
		lruEntry := completedLRUEntry{requestID: rID, timeCompleted: time.Now()}
		le := ci.completedRequestLRU.PushBack(lruEntry)
		ce.lruElem = le
		ci.Unlock()
	}

	// Write results on socket back to client...
	returnResults(&localIOR, myConnCtx)

	myConnCtx.activeRPCsWG.Done()
}

// getAndCreateClientID reads the first message off the new connection.
// The client will first it's uniqueID in a message.
// TODO:
// 1. read myUniqueID off socket
// 2. verify not exiting ... return error...
// 3. unmarshal message
// 4. create clientInfo struct in map
// 5. return ci and error as needed...
//
// TODO - client side - in Dial must resend clientInfo (both first time and
// after retransmit!!!)
func (server *Server) getAndCreateClientID(cCtx *connCtx) (ci *clientInfo, err error) {

	fmt.Printf("getAndCreateClientID() - called cCtx: %v\n", cCtx)

	// myUniqueID off wire
	buf, getErr := getIO(uint64(0), cCtx.conn)
	if getErr != nil {
		err = getErr
		return
	}

	var connUniqueID string
	err = json.Unmarshal(buf, &connUniqueID)
	if err != nil {
		return
	}

	fmt.Printf("getAndCreateClientID for clientID: %v cCtx: %v\n", connUniqueID, cCtx)

	// Check if this is the first time we have seen this client
	server.Lock()
	lci, ok := server.perClientInfo[connUniqueID]
	if !ok {
		// First time we have seen this client
		c := &clientInfo{cCtx: cCtx, myUniqueID: connUniqueID}
		c.completedRequest = make(map[requestID]*completedEntry)
		c.completedRequestLRU = list.New()
		//c.drainingCond = sync.NewCond(&c.drainingMutex)
		server.perClientInfo[connUniqueID] = c
		server.Unlock()
		ci = c
		cCtx.Lock()
		cCtx.ci = ci
		cCtx.Unlock()
	} else {
		server.Unlock()
		ci = lci

		// Wait for RPCs on existing connection to finish before
		// setting backpointer in ci to the new connection.
		ci.cCtx.activeRPCsWG.Wait()

		// Set cCtx back pointer to ci
		ci.Lock()
		cCtx.Lock()
		cCtx.ci = ci
		cCtx.Unlock()

		ci.cCtx = cCtx
		ci.Unlock()
	}

	return ci, err
}

// serviceClient gets called when we accept a new connection.
// This means we have a new client connection.
//
// This could be either a new client OR it could mean a different
// connection from an existing client.
func (server *Server) serviceClient(ci *clientInfo, cCtx *connCtx) {
	var (
		halting bool
	)

	/*
		cCtx := &connCtx{conn: conn}
	*/

	if printDebugLogs {
		logger.Infof("got a connection - starting read/write io thread")
	}
	fmt.Printf("serviceClient() - got a connection - starting read/write io thread for cCtx: %v\n", cCtx)

	/*
		// The first message sent on this socket by the client is the uniqueID.
		// Read that first and create the relevant structures before processing
		// any messages.
		ci, err := server.getAndCreateClientID(cCtx)
		if err != nil {
			return
		}
	*/

	// NOTE: At this point we know:
	//
	// 1. If this is the first connection from this client then we have
	//    created clientInfo
	//
	// 2. If we had a prior connection from the client, it has been
	//    cleaned up and all prior RPCs have completed.  We can use
	//    the clientInfo from server.perClientInfo[] without worrying .
	for {
		// Get RPC request
		buf, getErr := getIO(uint64(0), cCtx.conn)
		if getErr != nil {
			server.Lock()
			halting = server.halting
			server.Unlock()
			logger.Infof("serviceClient - getIO returned err: %v - halting: %v",
				getErr, halting)

			// TODO - probaby should just conn.Close() here !!!

			// Drop response on the floor.   Client will either reconnect or
			// this response will age out of the queues.
			return
		}

		server.Lock()
		if server.halting == true {
			server.Unlock()
			return
		}

		// Keep track of how many processRequest() goroutines we have
		// so that we can wait until they complete when handling retransmits.
		cCtx.activeRPCsWG.Add(1)
		server.Unlock()

		// No sense blocking the read of the next request,
		// push the work off on processRequest().
		//
		// Writes back on the socket wil have to be serialized so
		// pass the per connection context.
		server.goroutineWG.Add(1)
		go server.processRequest(ci, cCtx, buf)
	}
}

// TODO - review the locking here to make it simplier

// callRPCAndMarshal calls the RPC and returns results to requestor
func (server *Server) callRPCAndFormatReply(buf []byte, jReq *jsonRequest) (ior *ioReply) {
	var (
		err error
	)

	// Setup the reply structure with common fields
	reply := &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

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
			logger.PanicfWithError(err, "Unmarshal sReq: %+v", sReq)
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
		// TODO - figure out if this is the correct error

		// Method does not exist
		jReply.ErrStr = fmt.Sprintf("errno: %d", unix.ENOENT)
	}

	// Convert response into JSON for return trip
	reply.JResult, err = json.Marshal(jReply)
	if err != nil {
		logger.PanicfWithError(err, "Unable to marshal jReply: %+v", jReply)
	}

	return reply
}

func returnResults(ior *ioReply, cCtx *connCtx) {

	// Now write the response back to the client
	//
	// Serialize multiple goroutines writing on socket back to client
	// by grabbing a mutex on the context

	// Write Len back
	cCtx.Lock()
	cCtx.conn.SetDeadline(time.Now().Add(deadlineIO))
	binErr := binary.Write(cCtx.conn, binary.BigEndian, ior.Hdr)
	if binErr != nil {
		cCtx.Unlock()
		logger.Errorf("SERVER: binary.Write failed err: %v", binErr)
		// TODO - close cCtx.conn ?
		return
	}

	// Write JSON reply
	cCtx.conn.SetDeadline(time.Now().Add(deadlineIO))
	bytesWritten, writeErr := cCtx.conn.Write(ior.JResult)
	if writeErr != nil {
		logger.Errorf("SERVER: conn.Write failed - bytesWritten: %v err: %v",
			bytesWritten, writeErr)
		// TODO - close cCtx.conn ?
	}
	cCtx.Unlock()
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
		for k, v := range server.perClientInfo {
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
			v := server.perClientInfo[key]

			v.Lock()
			if v.isEmpty() {
				delete(server.perClientInfo, key)
			}
			v.Unlock()
		}
		logger.Infof("Trimmed completed RetryRpcs - Total: %v", totalItems)
	} else {
		for k, v := range server.perClientInfo {
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
func (server *Server) trimAClientBasedACK(uniqueID string, ci *clientInfo) (numItems int) {

	ci.Lock()

	// Remove from completedRequest completedRequestLRU
	for h := ci.previousHighestReplySeen + 1; h <= ci.highestReplySeen; h++ {
		v, ok := ci.completedRequest[h]
		if ok {
			ci.completedRequestLRU.Remove(v.lruElem)
			delete(ci.completedRequest, h)
			numItems++
		}
	}

	// Keep track of how far we have trimmed for next run
	ci.previousHighestReplySeen = ci.highestReplySeen
	ci.Unlock()
	return
}

// Remove completedRequest/completedRequestLRU entries older than server.completedTTL
//
// This gets called every ~10 minutes to clean out older entries.
//
// NOTE: We assume Server Lock is held
func (server *Server) trimTLLBased(uniqueID string, ci *clientInfo, t time.Time) (numItems int) {

	l := list.New()

	ci.Lock()
	for e := ci.completedRequestLRU.Front(); e != nil; e = e.Next() {
		eTime := e.Value.(completedLRUEntry).timeCompleted.Add(server.completedLongTTL)
		if eTime.Before(t) {
			delete(ci.completedRequest, e.Value.(completedLRUEntry).requestID)

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
		tmpE := ci.completedRequestLRU.Front()
		_ = ci.completedRequestLRU.Remove(tmpE)

	}
	ci.Unlock()
	return
}
