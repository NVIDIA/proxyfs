package retryrpc

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"reflect"

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

			server.processRequest(myConn)
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

func (server *Server) processRequest(conn net.Conn) {

	// NOTE: This function runs in a goroutine and only processes
	//               one request at a time.
	if printDebugLogs {
		logger.Infof("got a connection - starting read/write io thread")
	}

	for {
		// Get RPC request
		buf, getErr := getIO(conn, "SERVER")
		if getErr != nil {
			// TODO - error handling!!!
			// Retry??? Drop on floor since other side failed???
			// Retransmit case?
			return
		}

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

		// Call the RPC - and return an already marshaled response
		//
		// We pass buf to the call because the request will have to
		// be unmarshaled again to retrieve the parameters specific to
		// the RPC.
		reply, err := server.callRPCAndMarshal(buf, &jReq)
		/*
			fmt.Printf("Server: RPC Result: %v err: %v\n", string(reply.JResult), err)
		*/

		// Now write the response back to the client

		// Write Len back
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
	}
}

// callRPCAndMarshal calls the RPC and returns an already marshalled reply
func (server *Server) callRPCAndMarshal(buf []byte, jReq *jsonRequest) (reply *ioReply, err error) {

	// Setup the reply structure with common fields
	reply = &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

	// TODO TODO - first look onn completed or pending queue!
	// May have already been completed and can be returned.
	// If on pending queue - update the return path to reference
	// the current call net.Conn!!!

	// Queue the request
	server.Lock()
	server.pendingRequest[rid] = buf
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
	server.completedRequest[rid] = reply
	delete(server.pendingRequest, rid)
	server.Unlock()

	return
}
