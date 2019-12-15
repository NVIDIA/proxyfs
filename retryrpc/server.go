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

		go func(myConn net.Conn, myElm *list.Element) {
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
		// to the RPC.  (There is no way I know to pass an interface
		// over JSON and have it be unmarshalled.)
		//
		jReq := jsonRequest{}
		unmarErr := json.Unmarshal(buf, &jReq)
		if unmarErr != nil {
			fmt.Printf("SERVER: Unmarshal of buf failed with err: %v\n", unmarErr)
			return
		}

		// Call the RPC - and return an already marshaled response
		// We pass buf to the call because the request will have to
		// be unmarshaled again to retrieve the parameters specific to
		// the RPC.

		// TODO - reply, err := server.callRPC(buf, &jReq)

		reply, err := server.callRPCReflect(buf, &jReq)
		fmt.Printf("Server: RPC Result: %v err: %v\n", string(reply.JResult), err)

		// Now write the response back to the client

		// Write Len back
		reply.Len = int64(len(reply.JResult))
		err = binary.Write(conn, binary.BigEndian, reply.Len)
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

// callRPC calls the RPC and returns an already marshalled reply
func (server *Server) callRPC(buf []byte, jReq *jsonRequest) (reply *ioReply, err error) {

	// TODO - how handle RpcMount?

	// Setup the reply structure with common fields
	reply = &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

	// Queue the request
	server.Lock()
	server.pendingRequest[rid] = buf
	server.Unlock()

	/*
		// TODO - start of reflect changes....
		ma := server.serviceMap[jReq.Method]
		fmt.Printf("MA======: %+v\n", ma)

		// TODO - end of reflect changes....
	*/

	switch jReq.Method {
	/*
			case "Server.RpcChmod":
				q := breq.Params[0].(ChmodRequest)
		                p := Reply{}
		                err = s.RpcChmod(&q, &p)
		                if err != nil {
		                        breply.Error = err.Error()
		                        return
		                }
		                breply.Result[0] = p

					case "Server.RpcChown":
						q := breq.Params[0].(ChownRequest)
						p := Reply{}
						err = s.RpcChown(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcCreate":
						q := breq.Params[0].(CreateRequest)
						// TODO - which is correct? p := Reply{}
						p := InodeReply{}
						err = s.RpcCreate(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcFetchExtentMapChunk":
						q := breq.Params[0].(FetchExtentMapChunkRequest)
						p := FetchExtentMapChunkReply{}
						err = s.RpcFetchExtentMapChunk(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcGetStat":
						q := breq.Params[0].(GetStatRequest)
						p := StatStruct{}
						err = s.RpcGetStat(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcGetXAttr":
						q := breq.Params[0].(GetXAttrRequest)
						p := GetXAttrReply{}
						err = s.RpcGetXAttr(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcLink":
						q := breq.Params[0].(LinkRequest)
						p := Reply{}
						err = s.RpcLink(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcListXAttr":
						q := breq.Params[0].(ListXAttrRequest)
						p := ListXAttrReply{}
						err = s.RpcListXAttr(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcLookup":
						q := breq.Params[0].(LookupRequest)
						p := InodeReply{}
						err = s.RpcLookup(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcMkdir":
						q := breq.Params[0].(MkdirRequest)
						// TODO - which is correct? p := Reply{}
						p := InodeReply{}
						err = s.RpcMkdir(&q, &p)
						if err != nil {
							breply.Error = err.Error()
							return
						}
						breply.Result[0] = p

					case "Server.RpcMountByAccountName":
						// TODO - how do this?  Need this?
	*/

	/*
		case "Server.RpcPing":
			// Another unmarshal of buf to find the parameters specific to
			// this RPC
			paramsReq := pingJSONReq{}
			err = json.Unmarshal(buf, &paramsReq)
			if err != nil {
				// TODO - error handling
				return
			}

			// Now actually call the RPC
			p := jrpcfs.PingReply{}
			r := paramsReq.Params[0]

			// TODO - look it up in serviceMap or remove serviceMap???
			err = server.jrpcfs.RpcPing(&r, &p)
			if err != nil {
				jReply.Err = err
				// TODO - have to marshal the response!!!
				return
			}
			jReply.Result = p
	*/

	/*
		case "Server.RpcProvisionObject":
			q := breq.Params[0].(ProvisionObjectRequest)
			p := ProvisionObjectReply{}
			err = s.RpcProvisionObject(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcReadSymlink":
			q := breq.Params[0].(ReadSymlinkRequest)
			p := ReadSymlinkReply{}
			err = s.RpcReadSymlink(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcReaddirByLoc":
			q := breq.Params[0].(ReaddirByLocRequest)
			p := ReaddirReply{}
			err = s.RpcReaddirByLoc(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcRemoveXAttr":
			q := breq.Params[0].(RemoveXAttrRequest)
			p := Reply{}
			err = s.RpcRemoveXAttr(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcRename":
			q := breq.Params[0].(RenameRequest)
			p := Reply{}
			err = s.RpcRename(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcResize":
			q := breq.Params[0].(ResizeRequest)
			p := Reply{}
			err = s.RpcResize(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcRmdir":
			q := breq.Params[0].(UnlinkRequest)
			p := Reply{}
			err = s.RpcRmdir(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcSetTime":
			q := breq.Params[0].(SetTimeRequest)
			p := Reply{}
			err = s.RpcSetTime(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcSetXAttr":
			q := breq.Params[0].(SetXAttrRequest)
			p := Reply{}
			err = s.RpcSetXAttr(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcStatVFS":
			q := breq.Params[0].(StatVFSRequest)
			p := StatVFS{}
			err = s.RpcStatVFS(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcSymlink":
			q := breq.Params[0].(SymlinkRequest)
			p := Reply{}
			err = s.RpcSymlink(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcUnlink":
			q := breq.Params[0].(UnlinkRequest)
			p := Reply{}
			err = s.RpcUnlink(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p

		case "Server.RpcWrote":
			q := breq.Params[0].(WroteRequest)
			p := WroteReply{}
			err = s.RpcWrote(&q, &p)
			if err != nil {
				breply.Error = err.Error()
				return
			}
			breply.Result[0] = p
	*/

	default:
		fmt.Printf("Invalid tunnel request method: %v\n", jReq.Method)
	}

	// Convert response into JSON for return trip..
	reply.JResult, err = json.Marshal(jReply)

	server.Lock()
	server.completedRequest[rid] = reply
	delete(server.pendingRequest, rid)
	server.Unlock()

	return
}

// callRPCReflect calls the RPC and returns an already marshalled reply
func (server *Server) callRPCReflect(buf []byte, jReq *jsonRequest) (reply *ioReply, err error) {

	// TODO - how handle RpcMount?

	// Setup the reply structure with common fields
	reply = &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

	// Queue the request
	server.Lock()
	server.pendingRequest[rid] = buf
	server.Unlock()

	ma := server.svrMap[jReq.Method]
	fmt.Printf("MA======: method: %v request: %v reply %v\n", jReq.Method, ma.request, ma.reply)

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
	fmt.Printf("11111\n")
	fmt.Printf("-------------->>>>> reflect.TypeOf(tp.Elem()): %v\n",
		reflect.TypeOf(myReply.Elem().Interface()))

	fmt.Printf("22222\n")

	// Call the method
	function := ma.methodPtr.Func
	fmt.Printf("33333 req: %v\n", req)
	fmt.Printf("33333 req.Elem(): %v\n", req.Elem())
	returnValues := function.Call([]reflect.Value{server.receiver, req, myReply})
	fmt.Printf("444444 req: %+v reply: %+v\n", req, myReply)
	fmt.Printf("555555 reply: %v TypeOf cast: %v \n", myReply, reflect.TypeOf(myReply))

	fmt.Printf("returnValues: %v\n", returnValues)

	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	if jReply.Err == nil {
		jReply.Result = myReply.Elem().Interface()
	} else {
		jReply.Err = errInter.(error)
	}

	// Convert response into JSON for return trip..
	reply.JResult, err = json.Marshal(jReply)

	server.Lock()
	server.completedRequest[rid] = reply
	delete(server.pendingRequest, rid)
	server.Unlock()

	return
}
