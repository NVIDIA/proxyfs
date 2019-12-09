// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
//
// NOTE: This package does handle cases where the server process dies.  There
// are still gaps where a server may complete an RPC and die before returning
// a response.

package retryrpc

import (
	"container/list"
	"fmt"
	"net"
	"reflect"
	"sync"
	"syscall"
	"time"
)

// Request is the structure sent
type Request struct {
	Len  int64  // Length of this struct including len field
	JReq []byte // JSON containing request
}

// Response is the structure returned
type Response struct {
	Len    uint64 // Length of this struct including len field
	Result []byte // JSON response - only valid if err != nil
	Err    error  // Status of request
	Errno  syscall.Errno
}

type completedRequest struct {
	completedTime time.Time
	request       *Request
}

// ReqResKey uniquely identifies a client request from a client
type ReqResKey struct {
	uniqueID        string
	clientRequestID uint64
}

// Server tracks the state of the server
type Server struct {
	sync.Mutex
	completedTTL      time.Duration // How long a completed request stays on queue
	serviceMap        map[string]*reflect.Method
	pendingRequests   map[ReqResKey]*Request          // Request which have not been completed yet.
	completedRequests map[ReqResKey]*completedRequest // Request which have been completed.  Will age out
	ipaddr            string                          // IP address server listens too
	port              int                             // Port of server
	listener          net.Listener

	halting     bool
	connLock    sync.Mutex
	connections *list.List
	connWG      sync.WaitGroup
	listeners   []net.Listener
	listenersWG sync.WaitGroup
}

// NewServer creates the Server object
func NewServer(ttl time.Duration, ipaddr string, port int) *Server {
	s := &Server{}
	s.serviceMap = make(map[string]*reflect.Method)
	s.completedTTL = ttl
	s.ipaddr = ipaddr
	s.port = port
	s.connections = list.New()
	return s
}

// Register creates the map of server methods
func (server *Server) Register(retrySvr interface{}) (err error) {

	// Find all the methods associated with retrySvr and put into serviceMap
	// TODO - build the serviceMap similiar to how net/rpc Register does it...
	// serviceMap looks like serviceMap["RpcPing"]*rpcPing()

	return server.register(retrySvr)
}

// Start listener
func (server *Server) Start() (l net.Listener, err error) {
	ps := fmt.Sprintf("%d", server.port)
	server.listener, err = net.Listen("tcp", net.JoinHostPort(server.ipaddr, ps))
	server.listenersWG.Add(1)

	return server.listener, err
}

// Run server loop, accept connections, read request, run RPC method and
// return the results.
func (server *Server) Run() {
	// TODO - Algorithm - Standard server stuff
	// 1. goroutine GR1 accepts new connection and does "go GR2(conn)
	// 2. GR2 adds request to server.pendingRequest, unmarshals
	//    request and does "go GR3(process RPC)"
	// 3. GR2 proceses the RPC, grabs server lock and moves request from
	//    server.pendingRequest to server.completedRequest, releases lock
	//    and then sends response to client if socket is still up.
	// 4. request stays on server.completedRequest until s.completedTTL exceeded

	// TODO - do we need to retransmit responses in order?
	// What ordering guarantees do we need to enforce?

	server.run()
}

// Close stops the server
func (server *Server) Close() {
	_ = server.listener.Close()

	// TODO - blocks until the server is shut down
}

// Client methods

// Client tracking structure
type Client struct {
	sync.Mutex
	currentRequestID uint64 // Last request ID - start from clock
	// tick at mount and increment from there?
	// Handle reset of time?
	myUniqueID         string              // Unique ID across all clients
	outstandingRequest map[uint64]*Request // Map of outstanding requests sent
	// or to be sent to server
	tcpConn *net.TCPConn // Our connection to the server
}

// NewClient returns a Client structure
func NewClient(myUniqueID string) *Client {
	// TODO - if restart client, Client Request ID will be 0.   How know the server
	// has removed these client IDs from it's queue?  Race condition...
	c := &Client{myUniqueID: myUniqueID}
	c.outstandingRequest = make(map[uint64]*Request)
	return c
}

// TODO - TODO - split client and server packages since two loggers....

// Dial sets up connection to server
func (client *Client) Dial(ipaddr string, port int) (err error) {

	portStr := fmt.Sprintf("%d", port)
	hostPort := net.JoinHostPort(ipaddr, portStr)

	tcpAddr, resolvErr := net.ResolveTCPAddr("tcp", hostPort)
	if resolvErr != nil {
		fmt.Printf("ResolveTCPAddr returned err: %v\n", resolvErr)
		err = resolvErr
		return
	}

	client.tcpConn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Printf("unable to net.DialTCP(\"tcp\", nil, \"%s\"): %v", hostPort, err)
		return
	}

	return
}

// Send the request and block until it has completed
func (client *Client) Send(method string, rpcRequest interface{}) (response *Response, err error) {

	return client.send(method, rpcRequest)
}

// Close gracefully shuts down the client.   This allows the Server
// to remove Client request on completed queue.
func (client *Client) Close() {

}
