package retryrpc

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sync"

	"github.com/swiftstack/ProxyFS/logger"
)

// TODO - remove
// Variable to control debug output
var printDebugLogs bool = true
var debugPutGet bool = false

// TODO - do we need to retransmit responses in order?
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

func keyString(myUniqueID string, requestID uint64) string {
	return fmt.Sprintf("%v:%v", myUniqueID, requestID)
}

// First check if we already completed this request by looking on completed
// and pending queues.
//
// If the request is on the pending queue then update the
// net.Conn so the response will be sent to the caller.
//
// Otherwise, call the RPC.  callRPCAndMarshal() takes care
// of adding the request to the pending queue.
func (server *Server) findQOrCallRPC(buf []byte, jReq *jsonRequest) (reply *ioReply, err error) {
	queueKey := keyString(jReq.MyUniqueID, jReq.RequestID)

	// First check if we already completed this request by looking at
	// completed and pending queues.  Update pending queue with new
	// net.Conn if needed.

	// TODO - be careful that drop locks appropriately!!
	server.Lock()
	v, ok := server.completedRequest[queueKey]
	if ok {
		// Already have answer for this in completedRequest queue.
		// Just return the results.
		server.Unlock()
		logger.Infof("findQOrCallRPC - request on completedRequest Q - key: %v value: %+v",
			queueKey, v)
		reply = v

	} else {
		v2, ok2 := server.pendingRequest[queueKey]
		if ok2 {
			// TODO - on pending queue - update return socket so when complete gets returned
			// to correct socket.   Be careful to hold mutex while writing on the socket.
			server.Unlock()
			logger.Infof("findQOrCallRPC - request on pending Q - key: %v value: %+v",
				queueKey, v2)

		} else {
			// On neither queue - must be new request

			// Call the RPC - and return an already marshaled response
			//
			// We pass buf to the call because the request will have to
			// be unmarshaled again to retrieve the parameters specific to
			// the RPC.
			server.Unlock()
			reply, err = server.callRPCAndMarshal(buf, jReq, queueKey)
		}
	}

	return
}

func (server *Server) processRequest(conn net.Conn, buf []byte, writeMutex *sync.Mutex) {
	defer server.goroutineWG.Done()

	// We first unmarshal the raw buf to find the method
	//
	// Next we unmarshal again with the request structure specific
	// to the RPC.
	jReq := jsonRequest{}
	unmarErr := json.Unmarshal(buf, &jReq)
	if unmarErr != nil {
		fmt.Printf("SERVER: Unmarshal of buf failed with err: %v\n", unmarErr)
		return
	}

	// First check if we already completed this request by looking at
	// completed and pending queues.  Update pending queue with new
	// net.Conn if needed.
	reply, err := server.findQOrCallRPC(buf, &jReq)
	// TODO - error handling

	// Now write the response back to the client
	//
	// Serialize multiple goroutines writing back with writeMutex

	// Write Len back
	writeMutex.Lock()
	setupHdrReply(reply)
	err = binary.Write(conn, binary.BigEndian, reply.Hdr)
	if err != nil {
		fmt.Printf("SERVER: binary.Write failed err: %v\n", err)
	}

	// Send JSON reply
	bytesWritten, writeErr := conn.Write(reply.JResult)
	if writeErr != nil {
		fmt.Printf("SERVER: conn.Write failed - bytesWritten: %v err: %v\n",
			bytesWritten, err)
	}
	writeMutex.Unlock()
}

func (server *Server) serviceClient(conn net.Conn) {
	var (
		halting    bool
		writeMutex sync.Mutex
	)

	if printDebugLogs {
		logger.Infof("got a connection - starting read/write io thread")
	}

	for {
		// Get RPC request
		buf, getErr := getIO(conn, "SERVER")
		if getErr != nil {
			server.Lock()
			halting = server.halting
			server.Unlock()
			logger.Infof("serviceClient - getIO returned err: %v - halting: %v",
				getErr, halting)
			// TODO - error handling!!!
			// Retry??? Drop on floor since other side failed???
			// Retransmit case - just return and wait for new connection for this
			// client?
			return
		}

		// No sense blocking the read of the next request,
		// push the work off on processRequest().
		//
		// Writes back on the socket have to be serialized so
		// pass the per connection mutex.
		server.goroutineWG.Add(1)
		go server.processRequest(conn, buf, &writeMutex)
	}
}

// callRPCAndMarshal calls the RPC and returns an already marshalled reply
func (server *Server) callRPCAndMarshal(buf []byte, jReq *jsonRequest, queueKey string) (reply *ioReply, err error) {

	// Setup the reply structure with common fields
	reply = &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

	// Queue the request
	server.Lock()
	server.pendingRequest[queueKey] = buf
	server.Unlock()

	ma := server.svrMap[jReq.Method]
	/* Debugging
	fmt.Printf("MA======: method: %v request: %v reply %v\n", jReq.Method, ma.request, ma.reply)
	*/

	// Another unmarshal of buf to find the parameters specific to
	// this RPC
	typOfReq := ma.request.Elem()
	dummyReq := reflect.New(typOfReq).Interface()

	sReq := svrRequest{}
	sReq.Params[0] = dummyReq
	err = json.Unmarshal(buf, &sReq)
	if err != nil {
		// TODO - error handling
		return
	}
	req := reflect.ValueOf(dummyReq)

	// Create the reply structure
	typOfReply := ma.reply.Elem()
	myReply := reflect.New(typOfReply)

	/* For debugging
	fmt.Printf("-------------->>>>> reflect.TypeOf(tp.Elem()): %v\n",
		reflect.TypeOf(myReply.Elem().Interface()))
	*/

	// Call the method
	function := ma.methodPtr.Func
	returnValues := function.Call([]reflect.Value{server.receiver, req, myReply})

	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	if jReply.Err == nil {
		jReply.Result = myReply.Elem().Interface()
	} else {
		jReply.Err = errInter.(error)
	}

	// Convert response into JSON for return trip
	reply.JResult, err = json.Marshal(jReply)

	server.Lock()
	server.completedRequest[queueKey] = reply
	delete(server.pendingRequest, queueKey)
	server.Unlock()

	return
}
