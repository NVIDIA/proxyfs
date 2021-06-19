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

	"golang.org/x/sys/unix"
)

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
	var (
		conn net.Conn
		err  error
	)

	defer server.goroutineWG.Done()
	for {
		if len(server.tlsCertificate.Certificate) == 0 {
			conn, err = server.netListener.Accept()
		} else {
			conn, err = server.tlsListener.Accept()
		}
		if err != nil {
			if !server.halting {
				server.logger.Printf("net.Accept failed for Retry RPC listener - err: %v\n", err)
			}
			server.listenersWG.Done()
			return
		}

		server.connWG.Add(1)

		server.connLock.Lock()
		elm := server.connections.PushBack(conn)
		server.connLock.Unlock()

		// The server first calculates a uniqueID for this client and writes it back
		// on the socket.
		//
		// Create the relevant data structures and write out the uniqueID before calling
		// serviceClient().  We do the cleanup in this routine because there are
		// race conditions with noisy clients repeatedly reconnecting.
		//
		// Those race conditions are resolved if we serialize the recovery.
		cCtx := &connCtx{conn: conn}
		cCtx.cond = sync.NewCond(&cCtx.Mutex)

		// TODO move getClientIDAndWait() in the goroutine...
		ci, err := server.getClientIDAndWait(cCtx)
		if err != nil {
			// Socket already had an error - just loop back
			server.logger.Printf("getClientIDAndWait() from client addr: %v returned err: %v\n", conn.RemoteAddr(), err)

			// Sleep to block over active clients from pounding on us
			time.Sleep(1 * time.Second)

			server.closeClient(conn, elm)
			continue
		}

		server.goroutineWG.Add(1)
		go func(myCi *clientInfo, myCCtx *connCtx, myConn net.Conn, myElm *list.Element) {
			defer server.goroutineWG.Done()

			server.logger.Printf("Servicing client: %v address: %v\n", myCi.myUniqueID, myConn.RemoteAddr())
			server.serviceClient(myCi, myCCtx)

			server.logger.Printf("Closing client: %v address: %v\n", myCi.myUniqueID, myConn.RemoteAddr())
			server.closeClient(myConn, myElm)

			// The clientInfo for this client will first be trimmed and then later
			// deleted from the list of server.perClientInfo by the TTL trimmer.

		}(ci, cCtx, conn, elm)
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
		server.logger.Printf("Unmarshal of buf failed with err: %v\n", unmarErr)
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
		ior := server.callRPCAndFormatReply(buf, ci, &jReq)
		ci.stats.CallWrapRPCUsec.Add(uint64(time.Since(startRPC).Microseconds()))
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
		ci.stats.TrimAddCompleted.Add(1)
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
//
// If the client is new, it will ask for a UniqueID.   This routine will
// generate the new UniqueID and return it.
//
// If the client is existing, it will send it's UniqueID so that we
// can use that to look up it's data structures.
//
// Then getClientIDAndWait waits until any outstanding RPCs on a prior
// connection have completed before proceeding.
//
// This avoids race conditions when there are cascading retransmits.
// The algorithm is:
//
// 1. Client sends UniqueID to server when the connection is reestablished.
// 2. After accepting new socket, the server waits for the UniqueID from
//    the client.
// 3. If this is a client returning on a new socket, the server blocks
//    until all outstanding RPCs and related goroutines have completed for the
//    client on the previous connection.
func (server *Server) getClientIDAndWait(cCtx *connCtx) (ci *clientInfo, err error) {
	buf, msgType, getErr := getIO(uint64(0), server.deadlineIO, cCtx.conn)
	if getErr != nil {
		err = getErr
		return
	}

	if (msgType != PassID) && (msgType != AskMyUniqueID) {
		server.logger.Fatalf("Server expecting msgType PassID or AskMyUniqueID and received: %v\n", msgType)
		return
	}

	if msgType == PassID {

		// Returning client
		var connUniqueID uint64
		err = json.Unmarshal(buf, &connUniqueID)
		if err != nil {
			server.logger.Fatalf("Unmarshal returned error: %v", err)
			return
		}

		server.Lock()
		lci, ok := server.perClientInfo[connUniqueID]
		if !ok {
			// This could happen if the TTL is set to low and then client has not really died.

			// TODO - tell the client they need to panic since it is better for the client to
			// panic then the server.
			err = fmt.Errorf("Server - msgType PassID but can't find uniqueID in perClientInfo")
			server.logger.Fatalf("getClientIDAndWait() buf: %v connUniqueID: %v err: %+v", buf, connUniqueID, err)
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

		// First time we have seen this client
		var (
			localIOR ioReply
		)

		// Create a unique client ID and return to client
		server.Lock()
		server.clientIDNonce++
		newUniqueID := server.clientIDNonce

		// Setup new client data structures
		c := initClientInfo(cCtx, newUniqueID, server)

		server.perClientInfo[newUniqueID] = c
		server.Unlock()
		ci = c
		cCtx.Lock()
		cCtx.ci = ci
		cCtx.Unlock()

		// Build reply and send back to client
		var e error
		localIOR.JResult, e = json.Marshal(newUniqueID)
		if e != nil {
			server.logger.Fatalf("Marshal of newUniqueID: %v failed with err: %v", newUniqueID, e)
		}
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
			server.logger.Fatalf("serviceClient() received invalid msgType: %v", msgType)
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
func (server *Server) callRPCAndFormatReply(buf []byte, ci *clientInfo, jReq *jsonRequest) (ior *ioReply) {
	var (
		err          error
		returnValues []reflect.Value
		typOfReq     reflect.Type
		dummyReq     interface{}
	)

	// Setup the reply structure with common fields
	ior = &ioReply{}
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
			server.logger.Fatalf("Unmarshal sReq: %+v err: %v", sReq, err)
			return
		}
		req := reflect.ValueOf(dummyReq)
		cid := reflect.ValueOf(ci.myUniqueID)

		// Create the reply structure
		typOfReply := ma.reply.Elem()
		myReply := reflect.New(typOfReply)

		// Call the method
		function := ma.methodPtr.Func
		t := time.Now()
		if ma.passClientID {
			returnValues = function.Call([]reflect.Value{server.receiver, cid, req, myReply})
		} else {
			returnValues = function.Call([]reflect.Value{server.receiver, req, myReply})
		}
		ci.setMethodStats(jReq.Method, uint64(time.Since(t).Microseconds()))

		// The return value for the method is an error.
		errInter := returnValues[0].Interface()
		if errInter == nil {
			jReply.Result = myReply.Elem().Interface()
		} else {
			e, ok := errInter.(error)
			if !ok {
				server.logger.Fatalf("Call returnValues invalid cast errInter: %+v", errInter)
			}
			jReply.ErrStr = e.Error()
		}
	} else {
		// TODO - figure out if this is the correct error

		// Method does not exist
		jReply.ErrStr = fmt.Sprintf("errno: %d", unix.ENOENT)
	}

	// Convert response into JSON for return trip
	ior.JResult, err = json.Marshal(jReply)
	if err != nil {
		server.logger.Fatalf("Unable to marshal jReply: %+v err: %v", jReply, err)
	}

	return
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
		server.logger.Printf("returnResults() returned err: %v cnt: %v length of JResult: %v\n", e, cnt, len(ior.JResult))
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
			key := e.Value.(uint64)
			ci := server.perClientInfo[key]

			ci.Lock()
			ci.cCtx.Lock()
			if ci.isEmpty() && ci.cCtx.serviceClientExited {
				ci.unregsiterMethodStats(server)
				delete(server.perClientInfo, key)
				server.logger.Printf("Trim - DELETE inactive clientInfo with ID: %v\n", ci.myUniqueID)
			}
			ci.cCtx.Unlock()
			ci.Unlock()
		}
		server.logger.Printf("Trimmed completed RetryRpcs - Total: %v\n", totalItems)
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
func (server *Server) trimAClientBasedACK(uniqueID uint64, ci *clientInfo) (numItems int) {

	ci.Lock()

	// Remove from completedRequest completedRequestLRU
	for h := ci.previousHighestReplySeen + 1; h <= ci.highestReplySeen; h++ {
		v, ok := ci.completedRequest[h]
		if ok {
			ci.completedRequestLRU.Remove(v.lruElem)
			delete(ci.completedRequest, h)
			ci.stats.TrimRmCompleted.Add(1)
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
			ci.stats.TrimRmCompleted.Add(1)

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
	server.logger.Printf("ID: %v largestReplySize: %v largestReplySizeMethod: %v longest RPC: %v longest RPC Method: %v\n",
		ci.myUniqueID, s.largestReplySize, s.largestReplySizeMethod, s.longestRPC, s.longestRPCMethod)

	ci.Unlock()
	return
}
