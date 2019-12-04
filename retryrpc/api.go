// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

import (
	"container/list"
	"sync"
	"time"
)

// Request is the structure sent
type Request struct {
	Len       uint64 // Length of this struct including len field
	RequestID uint64 // Unique ID of this request
	MountID   uint64 // Mount ID
	JReq      []byte // JSON containing request
}

// Response is the structure returned
type Response struct {
	Len uint64 // Length of this struct including len field
	Res []byte // JSON response - only valid if err != nil
	Err error  // Status of request
}

type completedRequest struct {
	completedTime time.Time
	request       *Request
}

// Server tracks the state of the server
type Server struct {
	sync.Mutex
	completedTTL      time.Duration // How long a completed request stays on queue
	serviceMap        map[string]*func(request interface{}, response interface{})
	connections       *list.List                   // List of outstanding connections
	pendingRequests   map[string]*Request          // Request which have not been completed yet.
	completedRequests map[string]*completedRequest // Request which have been completed.  Will age out
	ipaddr            string                       // IP address server listens too
	port              int                          // Port of server
}

// NewServer creates the Server object
func NewServer(ttl time.Duration, ipaddr string, port int) *Server {
	s := &Server{}
	s.serviceMap = make(map[string]*func(request interface{}, response interface{}))
	s.connections = list.New()
	s.completedTTL = ttl
	s.ipaddr = ipaddr
	s.port = port
	return s
}

// Register creates the map of server methods
func (server *Server) Register() (err error) {

	// TODO - build the serviceMap similiar to how net/rpc Register does it...
	// serviceMap looks like serviceMap["RpcPing"]*rpcPing()
	return
}

// Run server loop, accept connections, read request, run RPC method and
// return the results.
func (server *Server) Run() {
	// TODO - Algorithm - Standard server stuff
	// 1. goroutine which accepts new connection and creates goroutine W
	// 2. goroutine W adds request to server.pendingRequest, unmarshals
	//    request, processes RPC, puts request on server.completedRequest
	//    and under same lock removes from server.pendingRequest.
	// 3. Results are sent back to client if socket still up
	// 4. request stays on server.completedRequest until s.completedTTL exceeded

}

// Close stops the server
func (server *Server) Close() {
	// TODO - blocks until the server is shut down

}

// Client methods

// Client tracking structure
type Client struct {
	currentRequestID uint64 // Last request ID - start from clock
	// tick at mount and increment from there?
	// Handle reset of time?
	myUniqueID         uint64              // Unique ID across all clients
	subsetID           uint64              // Subset of unique ID (mount ID)
	outstandingRequest map[uint64]*Request // Map of outstanding requests sent
	// or to be sent to server
}

// NewClient returns a Client structure
func NewClient(myUniqueID uint64) *Client {
	// TODO - if restart client, Client Request ID will be 0.   How know the server
	// has removed these client IDs from it's queue?  Race condition...
	c := &Client{myUniqueID: myUniqueID}
	c.outstandingRequest = make(map[uint64]*Request)
	return c
}

// Send the request and block until it has completed
func (client *Client) Send(method string, rpcRequest interface{}) (response *Response, err error) {

	// TODO - Algorithm:
	// 1. marshal method and args into JSON and put into Request struct
	// 2. put request on client.outstandingRequests
	// 3. Send request on socket to server and have goroutine block
	//    on socket waiting for result
	//    a. if read result then remove from queue and return result
	//    b. if get error (which one?) on socket then resend request.
	//       will have to make sure have enough info in request to make
	//       the operation idempotent.   Assume client retries until
	//       server comes back up?   Wait for failover to a peer?
	//       Assume using VIP on proxyfs node?
	// 4. Should we block forever? How kill?

	// TODO - TODO - what if RCP was completed on Server1 and before response,
	// proxyfsd fails over to Server2?   Client will resend - not idempotent!!!

	/*
		sendRequest := makeRPC(method, rpcRequest)
		fmt.Printf("sendRequest: %v\n", sendRequest)
	*/

	return
}

// Close gracefully shuts down the client.   This allows the Server
// to remove Client request on completed queue.
func (client *Client) Close() {

}
