package retryrpc

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"

	"github.com/swiftstack/ProxyFS/jrpcfs"
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

		fmt.Printf("Accept conn: %v\n", conn)

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
		/*
			if printDebugLogs {
				logger.Infof("Waiting for RPC request; req.data size is %v.", len(ctx.data))
			}
		*/

		// Get RPC request
		buf, getErr := getIO(conn, "SERVER")
		if getErr != nil {
			// TODO - error handling!!!
			// Retry??? Drop on floor since other side failed???
			// Retransmit case?
			return
		}

		// TODO - error handling
		if debugPutGet {
			logger.Infof("Got raw request: %+v", buf)
		}

		// Call the RPC - and return an already marshaled response
		reply, err := server.callRPC(buf)
		fmt.Printf("Server: RPC returned jReply: %+v \n\tResult: %v err: %v\n",
			reply, string(reply.JResult), err)

		// Now write the response back to the client
		// TODO - do this in a Goroutine?   How will error handling work?
		// TODO - handle Len first in binary and then write the other fields....

		// Write Len back
		reply.Len = int64(len(reply.JResult))
		err = binary.Write(conn, binary.BigEndian, reply.Len)
		if err != nil {
			fmt.Println("SERVER: binary.Write failed:", err)
		}
		fmt.Printf("SERVER: Wrote reply length: %v err: %v\n", reply.Len, err)

		// Send JSON reply
		bytesWritten, writeErr := conn.Write(reply.JResult)
		fmt.Printf("SERVER: Wrote RPC REQEUST with bytesWritten: %v writeErr:  %v\n", bytesWritten, writeErr)
	}
}

// callRPC calls the RPC and returns an already marshalled reply
func (server *Server) callRPC(buf []byte) (reply *ioReply, err error) {

	// TODO - how handle RpcMount?

	// We first unmarshal the raw buf to find the method
	//
	// Next we unmarshal again with the request structure specific
	// to the RPC.  (There is no way I know to pass an interface
	// over JSON and have it be unmarshalled.)
	//
	fmt.Printf("getRequest() - buffer read is: %v\n", string(buf))
	jReq := jsonRequest{}
	err = json.Unmarshal(buf, &jReq)
	if err != nil {
		fmt.Printf("SERVER: Unmarshal of buf failed with err: %v\n", err)
		return
	}

	// Setup the reply structure with common fields
	reply = &ioReply{}
	rid := jReq.RequestID
	jReply := &jsonReply{MyUniqueID: jReq.MyUniqueID, RequestID: rid}

	// Queue the request
	server.Lock()
	server.pendingRequest[rid] = buf
	server.Unlock()

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
		jReply.Result[0] = p

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
