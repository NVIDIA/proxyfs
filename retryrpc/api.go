package retryrpc

// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
//
// NOTE: This package does handle cases where the server process dies.  There
// are still gaps where a server may complete an RPC and die before returning
// a response.

import (
	"container/list"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/logger"
)

// Server tracks the state of the server
type Server struct {
	sync.Mutex
	completedTTL time.Duration          // How long a completed request stays on queue
	svrMap       map[string]*methodArgs // Key: Method name
	ipaddr       string                 // IP address server listens too
	port         int                    // Port of server
	listener     net.Listener

	halting             bool
	goroutineWG         sync.WaitGroup // Used to track outstanding goroutines
	connLock            sync.Mutex
	connections         *list.List // TODO - how used?
	connWG              sync.WaitGroup
	listeners           []net.Listener
	listenersWG         sync.WaitGroup
	receiver            reflect.Value          // Package receiver being served
	pendingRequest      map[string]*pendingCtx // Key: "MyUniqueID:RequestID"
	completedRequest    map[string]*ioReply    // Key: "MyUniqueID:RequestID"
	completedRequestLRU *list.List             // LRU used to remove completed request in ticker
	completedTickerDone chan bool
	completedTicker     *time.Ticker
	completedDoneWG     sync.WaitGroup
}

// NewServer creates the Server object
func NewServer(ttl time.Duration, ipaddr string, port int) *Server {
	s := &Server{}
	s.svrMap = make(map[string]*methodArgs)
	s.pendingRequest = make(map[string]*pendingCtx)
	s.completedRequest = make(map[string]*ioReply)
	s.completedRequestLRU = list.New()
	s.completedTickerDone = make(chan bool)
	s.completedTTL = ttl
	s.ipaddr = ipaddr
	s.port = port
	s.connections = list.New()
	return s
}

// Register creates the map of server methods
func (server *Server) Register(retrySvr interface{}) (err error) {

	// Find all the methods associated with retrySvr and put into serviceMap
	server.receiver = reflect.ValueOf(retrySvr)
	return server.register(retrySvr)
}

// Start listener
func (server *Server) Start() (l net.Listener, err error) {
	ps := fmt.Sprintf("%d", server.port)
	server.listener, err = net.Listen("tcp", net.JoinHostPort(server.ipaddr, ps))
	server.listenersWG.Add(1)

	// Start ticker which removes older completedRequests
	server.completedTicker = time.NewTicker(server.completedTTL)
	server.completedDoneWG.Add(1)
	go func() {
		for {
			select {
			case <-server.completedTickerDone:
				server.completedDoneWG.Done()
				return
			case t := <-server.completedTicker.C:
				logger.Infof("Before trimCompleted()")
				server.trimCompleted(t)
				logger.Infof("After trimCompleted()")
			}
		}
	}()

	return server.listener, err
}

// Run server loop, accept connections, read request, run RPC method and
// return the results.
func (server *Server) Run() {
	server.goroutineWG.Add(1)
	go server.run()
}

// Close stops the server
func (server *Server) Close() {
	server.Lock()
	server.halting = true
	server.Unlock()

	_ = server.listener.Close()

	server.listenersWG.Wait()
	server.goroutineWG.Wait()

	x := len(server.pendingRequest)
	if len(server.pendingRequest) != 0 {
		fmt.Printf("retryrpc.Close() - pendingRequest non-zero - count: %v\n", x)
	}

	server.completedTicker.Stop()
	server.completedTickerDone <- true
	server.completedDoneWG.Wait()
}

// CompletedCnt returns count of pendingRequests
//
// This is only useful for testing.
func (server *Server) CompletedCnt() int {
	return len(server.completedRequest)
}

// PendingCnt returns count of pendingRequests
//
// This is only useful for testing.
func (server *Server) PendingCnt() int {
	return len(server.pendingRequest)
}

// Client methods

// Client tracking structure
type Client struct {
	sync.Mutex
	halting          bool   // TODO - not implemented yet
	currentRequestID uint64 // Last request ID - start from clock
	// tick at mount and increment from there?
	// Handle reset of time?
	myUniqueID         string             // Unique ID across all clients
	outstandingRequest map[uint64]*reqCtx // Map of outstanding requests sent
	// or to be sent to server.  Key is assigned from currentRequestID
	tcpConn     *net.TCPConn   // Our connection to the server
	goroutineWG sync.WaitGroup // Used to track outstanding goroutines
}

// NewClient returns a Client structure
func NewClient(myUniqueID string) *Client {

	// TODO - if restart client, Client Request ID will be 0.   How know the server
	// has removed these client IDs from it's queue?  Race condition...
	c := &Client{myUniqueID: myUniqueID}
	c.outstandingRequest = make(map[uint64]*reqCtx)

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

	// Start readResponse goroutine to read responses from server
	client.goroutineWG.Add(1)
	go client.readReplies()

	return
}

// Send the request and block until it has completed
func (client *Client) Send(method string, request interface{}, reply interface{}) (err error) {

	return client.send(method, request, reply)
}

// Close gracefully shuts down the client
func (client *Client) Close() {

	// Set halting flag and then close our socket to server.
	// This will cause the blocked getIO() in readReplies() to return.
	client.Lock()
	client.halting = true
	client.Unlock()
	client.tcpConn.Close()

	// Wait for the goroutines to return
	client.goroutineWG.Wait()
}
