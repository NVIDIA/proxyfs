package retryrpc

// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
//
// NOTE: This package does handle cases where the server process dies.  There
// are still gaps where a server may complete an RPC and die before returning
// a response.

import (
	"container/list"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/logger"
)

// ServerCreds tracks the root CA and the
// server CA
type ServerCreds struct {
	RootCAx509CertificatePEM []byte
	serverTLSCertificate     tls.Certificate
}

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
	Creds               *ServerCreds
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
	var (
		err error
	)
	server := &Server{ipaddr: ipaddr, port: port, completedTTL: ttl}
	server.svrMap = make(map[string]*methodArgs)
	server.pendingRequest = make(map[string]*pendingCtx)
	server.completedRequest = make(map[string]*ioReply)
	server.completedRequestLRU = list.New()
	server.completedTickerDone = make(chan bool)
	server.connections = list.New()

	server.Creds, err = constructServerCreds(ipaddr)
	if err != nil {
		logger.Errorf("Construction of server credentials failed with err: %v", err)
		panic(err)
	}

	return server
}

// Register creates the map of server methods
func (server *Server) Register(retrySvr interface{}) (err error) {

	// Find all the methods associated with retrySvr and put into serviceMap
	server.receiver = reflect.ValueOf(retrySvr)
	return server.register(retrySvr)
}

// Start listener
func (server *Server) Start() (err error) {
	portStr := fmt.Sprintf("%d", server.port)
	hostPortStr := net.JoinHostPort(server.ipaddr, portStr)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{server.Creds.serverTLSCertificate},
	}

	server.listener, err = tls.Listen("tcp", hostPortStr, tlsConfig)
	if nil != err {
		err = fmt.Errorf("tls.Listen() failed: %v", err)
		return
	}

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
				server.trimCompleted(t)
			}
		}
	}()

	return err
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

	err := server.listener.Close()
	if err != nil {
		logger.Errorf("server.listener.Close() returned err: %v", err)
	}

	server.listenersWG.Wait()

	// Now close the client sockets to wakeup our blocked readers
	server.closeClientConn()
	server.goroutineWG.Wait()

	server.Lock()
	x := len(server.pendingRequest)
	server.Unlock()
	if x != 0 {
		logger.Errorf("pendingRequest count is: %v when should be zero", x)
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
	halting          bool
	currentRequestID uint64 // Last request ID - start from clock
	// tick at mount and increment from there?
	// Handle reset of time?
	myUniqueID         string             // Unique ID across all clients
	outstandingRequest map[uint64]*reqCtx // Map of outstanding requests sent
	// or to be sent to server.  Key is assigned from currentRequestID
	tlsConn      *tls.Conn // Our connection to the server
	x509CertPool *x509.CertPool
	tlsConfig    *tls.Config
	goroutineWG  sync.WaitGroup // Used to track outstanding goroutines
}

// NewClient returns a Client structure
func NewClient(myUniqueID string) *Client {

	// TODO - if restart client, Client Request ID will be 0.   How know the server
	// has removed these client IDs from it's queue?  Race condition...
	c := &Client{myUniqueID: myUniqueID}
	c.outstandingRequest = make(map[uint64]*reqCtx)

	return c
}

// TODO - pass loggers to Client and Server objects

// Dial sets up connection to server
func (client *Client) Dial(ipaddr string, port int, rootCAx509CertificatePEM []byte) (err error) {
	portStr := fmt.Sprintf("%d", port)
	hostPortStr := net.JoinHostPort(ipaddr, portStr)

	client.x509CertPool = x509.NewCertPool()

	// Add cert for root CA to our pool
	ok := client.x509CertPool.AppendCertsFromPEM(rootCAx509CertificatePEM)
	if !ok {
		err = fmt.Errorf("x509CertPool.AppendCertsFromPEM() returned !ok")
		return
	}

	client.tlsConfig = &tls.Config{
		RootCAs: client.x509CertPool,
	}

	// Now dial the server
	client.tlsConn, err = tls.Dial("tcp", hostPortStr, client.tlsConfig)
	if nil != err {
		err = fmt.Errorf("tls.Dial() failed: %v", err)
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
	client.tlsConn.Close()

	// Wait for the goroutines to return
	client.goroutineWG.Wait()
}
