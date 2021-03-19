// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package retryrpc

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/logger"
	"golang.org/x/sys/unix"
)

// Variable to control debug output
var printDebugLogs bool = false

// TODO - test if Register has been called???

func (server *Server) closeClient(myConn net.Conn, myElm *list.Element) {

	server.connLock.Lock()
	server.connections.Remove(myElm)

	// There is a race condition where the connection could have been
	// closed in Down().  However, closing it twice is okay.
	myConn.Close()
	server.connLock.Unlock()
	server.connWG.Done()
}

func (server *Server) run() {
	defer server.goroutineWG.Done()
	for {
		conn, err := server.tlsListener.Accept()
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

		// TODO - update comment per two cases initialDial() and reDial()....

		// The first message sent on this socket by the client is the uniqueID.
		//
		// Read that first and create the relevant structures before calling
		// serviceClient().  We do the cleanup in this routine because there are
		// race conditions with noisy clients repeatedly reconnecting.
		//
		// Those race conditions are resolved if we serialize the recovery.
		cCtx := &connCtx{conn: conn}
		cCtx.cond = sync.NewCond(&cCtx.Mutex)
		ci, err := server.getClientIDAndWait(cCtx)
		if err != nil {
			// Socket already had an error - just loop back
			logger.Warnf("getClientIDAndWait() from client addr: %v returned err: %v\n", conn.RemoteAddr(), err)

			// Sleep to block over active clients from pounding on us
			time.Sleep(1 * time.Second)

			server.closeClient(conn, elm)
			continue
		}

		server.goroutineWG.Add(1)
		go func(myConn net.Conn, myElm *list.Element) {
			defer server.goroutineWG.Done()

			logger.Infof("Servicing client: %v address: %v", ci.myUniqueID, myConn.RemoteAddr())
			server.serviceClient(ci, cCtx)

			logger.Infof("Closing client: %v address: %v", ci.myUniqueID, myConn.RemoteAddr())
			server.closeClient(conn, elm)

			// The clientInfo for this client will first be trimmed and then later
			// deleted from the list of server.perClientInfo by the TTL trimmer.

		}(conn, elm)
	}
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

	ci.Lock()

	// Keep track of the highest consecutive requestID seen
	// by client.  We use this to trim completedRequest list.
	//
	// Messages could arrive out of order so only update if
	// the new request is giving us a higher value.
	if jReq.HighestReplySeen > ci.highestReplySeen {
		ci.highestReplySeen = jReq.HighestReplySeen
	}
	ci.stats.RPCattempted.Add(1)

	// First check if we already completed this request by looking at
	// completed queue.
	var localIOR ioReply
	rID := jReq.RequestID
	ce, ok := ci.completedRequest[rID]
	if ok {
		// Already have answer for this in completedRequest queue.
		// Just return the results.
		setupHdrReply(ce.reply, RPC)
		localIOR = *ce.reply
		ci.stats.RPCretried.Add(1)
		ci.Unlock()

	} else {
		ci.Unlock()

		// Call the RPC and return the results.
		//
		// We pass buf to the call because the request will have to
		// be unmarshaled again to retrieve the parameters specific to
		// the RPC.
		startRPC := time.Now()
		ior := server.callRPCAndFormatReply(buf, ci.myUniqueID, &jReq)
		ci.stats.RPCLenUsec.Add(uint64(time.Since(startRPC) / time.Microsecond))
		ci.stats.RPCcompleted.Add(1)

		// We had to drop the lock before calling the RPC since it
		// could block.
		ci.Lock()
		dur := time.Since(startRPC)
		if dur > ci.stats.longestRPC {
			ci.stats.longestRPCMethod = jReq.Method
			ci.stats.longestRPC = dur
		}

		// Update completed queue
		ce := &completedEntry{reply: ior}
		ci.completedRequest[rID] = ce
		ci.stats.AddCompleted.Add(1)
		setupHdrReply(ce.reply, RPC)
		localIOR = *ce.reply
		sz := uint64(len(ior.JResult))
		if sz > ci.stats.largestReplySize {
			ci.stats.largestReplySizeMethod = jReq.Method
			ci.stats.largestReplySize = sz
		}
		ci.stats.ReplySize.Add(sz)
		lruEntry := completedLRUEntry{requestID: rID, timeCompleted: time.Now()}
		le := ci.completedRequestLRU.PushBack(lruEntry)
		ce.lruElem = le
		ci.Unlock()
	}

	// Write results on socket back to client...
	server.returnResults(&localIOR, myConnCtx)

	myConnCtx.activeRPCsWG.Done()
}

// TODO - update this comment for initialDial() vs reDial() cases!!!
// should we rename function????

//
// getClientIDAndWait reads the first message off the new connection.
// The client sends its uniqueID before sending it's first RPC.
//
// Then getClientIDAndWait waits until any outstanding RPCs on a prior
// connection have completed before proceeding.
//
// This avoids race conditions when there are cascading retransmits.
// The algorithm is:
//
// 1. Client sends UniqueID to server when the socket is first open
// 2. After accepting new socket, the server waits for the UniqueID from
//    the client.
// 3. If this is the first connection from the client, the server creates a
//    clientInfo structure for the client and proceeds to wait for RPCs.
// 4. If this is a client returning on a new socket, the server blocks
//    until all outstanding RPCs and related goroutines have completed for the
//    client on the previous connection.
// 5. Additionally, the server blocks on accepting new connections
//    (which could be yet another reconnect for the same client) until the
//    previous connection has closed down.
func (server *Server) getClientIDAndWait(cCtx *connCtx) (ci *clientInfo, err error) {
	buf, msgType, getErr := getIO(uint64(0), server.deadlineIO, cCtx.conn)
	if getErr != nil {
		err = getErr
		return
	}

	if (msgType != PassID) && (msgType != AskMyUniqueID) {
		err = fmt.Errorf("Server expecting msgType PassID or AskMyUniqueID and received: %v", msgType)
		logger.PanicfWithError(err, "")
		return
	}

	if msgType == PassID {
		var connUniqueID string
		err = json.Unmarshal(buf, &connUniqueID)
		if err != nil {
			logger.PanicfWithError(err, "Unmarshal returned error")
			return
		}

		// Check if this is the first time we have seen this client
		server.Lock()
		lci, ok := server.perClientInfo[connUniqueID]
		if !ok {
			err = fmt.Errorf("Server - msgType PassID but can't find uniqueID in perClientInfo")
			logger.PanicfWithError(err, "getClientIDAndWait() buf: %v connUniqueID: %v err: %+v", buf, connUniqueID, err)
			server.Unlock()
		} else {
			server.Unlock()
			ci = lci

			// Wait for the serviceClient() goroutine from a prior connection to exit
			// before proceeding.
			ci.cCtx.Lock()
			for !ci.cCtx.serviceClientExited {
				ci.cCtx.cond.Wait()
			}
			ci.cCtx.Unlock()

			// Now wait for any outstanding RPCs to complete
			ci.cCtx.activeRPCsWG.Wait()

			// Set cCtx back pointer to ci
			ci.Lock()
			cCtx.Lock()
			cCtx.ci = ci
			cCtx.Unlock()

			ci.cCtx = cCtx
			ci.Unlock()
		}
	} else {
		var (
			localIOR ioReply
		)

		// First time we have seen this client

		// Create a unique client ID and return to client
		server.Lock()
		server.clientIDNonce++
		newUniqueID := fmt.Sprintf("unqClnt-%v", server.clientIDNonce)

		// Setup new client data structures
		c := &clientInfo{cCtx: cCtx, myUniqueID: newUniqueID}
		c.completedRequest = make(map[requestID]*completedEntry)
		c.completedRequestLRU = list.New()
		server.perClientInfo[newUniqueID] = c
		server.Unlock()
		ci = c
		cCtx.Lock()
		cCtx.ci = ci
		cCtx.Unlock()

		bucketstats.Register("proxyfs.retryrpc", c.myUniqueID, &c.stats)

		// Build reply and send back to client
		localIOR.JResult = []byte(newUniqueID)
		setupHdrReply(&localIOR, ReturnUniqueID)

		server.returnResults(&localIOR, cCtx)
	}

	return ci, err
}

// serviceClient gets called when we accept a new connection.
func (server *Server) serviceClient(ci *clientInfo, cCtx *connCtx) {
	for {
		// Get RPC request
		buf, msgType, getErr := getIO(uint64(0), server.deadlineIO, cCtx.conn)
		if !os.IsTimeout(getErr) && getErr != nil {

			// Drop response on the floor.   Client will either reconnect or
			// this response will age out of the queues.
			cCtx.Lock()
			cCtx.serviceClientExited = true
			cCtx.cond.Broadcast()
			cCtx.Unlock()
			return
		}

		server.Lock()
		if server.halting {
			server.Unlock()
			return
		}

		if os.IsTimeout(getErr) {
			server.Unlock()
			continue
		}

		if msgType != RPC {
			// TODO - panic? log message....
			fmt.Printf("serviceClient() received invalid msgType: %v - dropping\n", msgType)
			continue
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

// callRPCAndMarshal calls the RPC and returns results to requestor
func (server *Server) callRPCAndFormatReply(buf []byte, clientID string, jReq *jsonRequest) (ior *ioReply) {
	var (
		err          error
		returnValues []reflect.Value
		typOfReq     reflect.Type
		dummyReq     interface{}
	)

	// Setup the reply structure with common fields
	reply := &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

	ma := server.svrMap[jReq.Method]
	if ma != nil {

		// Another unmarshal of buf to find the parameters specific to
		// this RPC
		typOfReq = ma.request.Elem()
		dummyReq = reflect.New(typOfReq).Interface()

		sReq := svrRequest{}
		sReq.Params[0] = dummyReq
		err = json.Unmarshal(buf, &sReq)
		if err != nil {
			logger.PanicfWithError(err, "Unmarshal sReq: %+v", sReq)
			return
		}
		req := reflect.ValueOf(dummyReq)
		cid := reflect.ValueOf(clientID)

		// Create the reply structure
		typOfReply := ma.reply.Elem()
		myReply := reflect.New(typOfReply)

		// Call the method
		function := ma.methodPtr.Func
		if ma.passClientID {
			returnValues = function.Call([]reflect.Value{server.receiver, cid, req, myReply})
		} else {
			returnValues = function.Call([]reflect.Value{server.receiver, req, myReply})
		}

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

func (server *Server) returnResults(ior *ioReply, cCtx *connCtx) {

	// Now write the response back to the client
	//
	// Serialize multiple goroutines writing on socket back to client
	// by grabbing a mutex on the context

	// Write Len back
	cCtx.Lock()
	cCtx.conn.SetDeadline(time.Now().Add(server.deadlineIO))
	binErr := binary.Write(cCtx.conn, binary.BigEndian, ior.Hdr)
	if binErr != nil {
		cCtx.Unlock()
		// Conn will be closed when serviceClient() returns
		return
	}

	// Write JSON reply
	//
	// In error case - Conn will be closed when serviceClient() returns
	cCtx.conn.SetDeadline(time.Now().Add(server.deadlineIO))
	cnt, e := cCtx.conn.Write(ior.JResult)
	if e != nil {
		logger.Infof("returnResults() returned err: %v cnt: %v length of JResult: %v", e, cnt, len(ior.JResult))
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
		for k, ci := range server.perClientInfo {
			n := server.trimTLLBased(ci, t)
			totalItems += n

			ci.Lock()
			if ci.isEmpty() {
				l.PushBack(k)
			}
			ci.Unlock()

		}

		// If the client is no longer active - delete it's entry
		//
		// We can only do the check if we are currently holding the
		// lock.
		for e := l.Front(); e != nil; e = e.Next() {
			key := e.Value.(string)
			ci := server.perClientInfo[key]

			ci.Lock()
			ci.cCtx.Lock()
			if ci.isEmpty() && ci.cCtx.serviceClientExited {
				bucketstats.UnRegister("proxyfs.retryrpc", ci.myUniqueID)
				delete(server.perClientInfo, key)
				logger.Infof("Trim - DELETE inactive clientInfo with ID: %v", ci.myUniqueID)
			}
			ci.cCtx.Unlock()
			ci.Unlock()
		}
		logger.Infof("Trimmed completed RetryRpcs - Total: %v", totalItems)
	} else {
		for k, ci := range server.perClientInfo {
			n := server.trimAClientBasedACK(k, ci)
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
			ci.stats.RmCompleted.Add(1)
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
func (server *Server) trimTLLBased(ci *clientInfo, t time.Time) (numItems int) {
	ci.Lock()
	for e := ci.completedRequestLRU.Front(); e != nil; {
		eTime := e.Value.(completedLRUEntry).timeCompleted.Add(server.completedLongTTL)
		if eTime.Before(t) {
			delete(ci.completedRequest, e.Value.(completedLRUEntry).requestID)
			ci.stats.RmCompleted.Add(1)

			eTmp := e
			e = e.Next()
			_ = ci.completedRequestLRU.Remove(eTmp)
			numItems++
		} else {
			// Oldest is in front so just break
			break
		}
	}
	s := ci.stats
	logger.Infof("ID: %v largestReplySize: %v largestReplySizeMethod: %v longest RPC: %v longest RPC Method: %v",
		ci.myUniqueID, s.largestReplySize, s.largestReplySizeMethod, s.longestRPC, s.longestRPCMethod)

	ci.Unlock()
	return
}
