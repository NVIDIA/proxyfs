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
	return
}

// Run server loop, accept connections, read request, run RPC method and
// return the results.
func (server *Server) Run() {

}

// Close stops the server
func (server *Server) Close() {
	// TODO - blocks until the server is shut down

}

// Client methods

// Client tracking structure
type Client struct {
	mountID          uint64
	currentRequestID uint64
}

// NewClient returns a Client structure
func NewClient(mountID uint64) *Client {
	// TODO - if restart client, Client Request ID will be 0.   How know the server
	// has removed these client IDs from it's queue?  Race condition...
	c := &Client{mountID: mountID}
	return c
}

// MakeRPC takes the RPC method and arguments and returns a Request struct
func MakeRPC(method string, args ...interface{}) (request *Request) {

	// TODO - RequestID, MountID, JReq

	// This will:
	// 1. bump the client request ID
	// 2. Store the RPC method and args in JSON and store in JReq
	// 3. Store the mountID
	// 4. set the length of the request
	// 5. return the request
	request = &Request{}
	return
}

// Send the request and block until it has completed
func (client *Client) Send(request *Request) (response *Response, err error) {

	// TODO - puts request on queue and sends, only when request returns do we take it
	// off the queue
	// if connection drops, get new connection and resend the request
	return
}

// Close gracefully shuts down the client.   This allows the Server
// to remove Client request on completed queue.
func (client *Client) Close() {

}
