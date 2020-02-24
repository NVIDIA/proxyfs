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
	"io/ioutil"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/google/btree"
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
	completedLongTTL time.Duration          // How long a completed request stays on queue
	completedAckTrim time.Duration          // How frequently trim requests acked by client
	svrMap           map[string]*methodArgs // Key: Method name
	ipaddr           string                 // IP address server listens too
	port             int                    // Port of server
	listener         net.Listener

	halting              bool
	goroutineWG          sync.WaitGroup // Used to track outstanding goroutines
	connLock             sync.Mutex
	connections          *list.List
	connWG               sync.WaitGroup
	listeners            []net.Listener
	Creds                *ServerCreds
	listenersWG          sync.WaitGroup
	receiver             reflect.Value          // Package receiver being served
	perClientInfo        map[string]*clientInfo // Key: "clientID".  Tracks clients
	completedTickerDone  chan bool
	completedLongTicker  *time.Ticker // Longer ~10 minute timer to trim
	completedShortTicker *time.Ticker // Shorter ~100ms timer to trim known completed
	completedDoneWG      sync.WaitGroup
}

// NewServer creates the Server object
func NewServer(ttl time.Duration, shortTrim time.Duration, ipaddr string, port int) *Server {
	var (
		err error
	)
	server := &Server{ipaddr: ipaddr, port: port, completedLongTTL: ttl, completedAckTrim: shortTrim}
	server.svrMap = make(map[string]*methodArgs)
	server.perClientInfo = make(map[string]*clientInfo)
	server.completedTickerDone = make(chan bool)
	server.connections = list.New()

	server.Creds, err = constructServerCreds(ipaddr)
	if err != nil {
		logger.Errorf("Construction of server credentials failed with err: %v", err)
		panic(err)
	}

	// TODO - remove this once we integrate pfs-retryrpc into stress
	// test framework
	//
	// Write our key to a file so test utilities can pick it up without using
	// the mount API
	ioutil.WriteFile("/tmp/cert.pem", server.Creds.RootCAx509CertificatePEM, 0700)

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
	server.completedLongTicker = time.NewTicker(server.completedLongTTL)
	// Start ticker which removes requests already ACKed by client
	server.completedShortTicker = time.NewTicker(server.completedAckTrim)
	server.completedDoneWG.Add(1)
	go func() {
		for {
			select {
			case <-server.completedTickerDone:
				server.completedDoneWG.Done()
				return
			case tl := <-server.completedLongTicker.C:
				server.trimCompleted(tl, true)
			case ts := <-server.completedShortTicker.C:
				server.trimCompleted(ts, false)
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

// SendCallback sends a message to clientID so that clientID contacts
// the RPC server.
//
// The assumption is that this callback only gets called when the server has
// an async message for the client
func (server *Server) SendCallback(clientID string, msg []byte) {
	// TODO
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

	server.completedLongTicker.Stop()
	server.completedShortTicker.Stop()
	server.completedTickerDone <- true
	server.completedDoneWG.Wait()
}

// CloseClientConn - This is debug code to cause some connections to be closed
// It is called from a stress test case to cause retransmits
func (server *Server) CloseClientConn() {
	logger.Infof("CloseClientConn() called --------")
	if server == nil {
		return
	}
	server.connLock.Lock()
	for c := server.connections.Front(); c != nil; c = c.Next() {
		conn := c.Value.(net.Conn)
		conn.Close()
	}
	server.connLock.Unlock()
}

// CompletedCnt returns count of pendingRequests
//
// This is only useful for testing.
func (server *Server) CompletedCnt() (totalCnt int) {
	for _, v := range server.perClientInfo {
		totalCnt += v.completedCnt()
	}
	return
}

// PendingCnt returns count of pendingRequests
//
// This is only useful for testing.
func (server *Server) PendingCnt() (totalCnt int) {
	for _, v := range server.perClientInfo {
		totalCnt += v.pendingCnt()
	}
	return
}

// Client methods
type clientState int

const (
	// INITIAL means the Client struct has just been created
	INITIAL clientState = iota + 1
	// DISCONNECTED means the Client has lost the connection to the server
	DISCONNECTED
	// CONNECTED means the Client is connected to the server
	CONNECTED
	// RETRANSMITTING means a goroutine is in the middle of recovering
	// from a loss of a connection with the server
	RETRANSMITTING
)

type connectionTracker struct {
	state                    clientState
	genNum                   uint64 // Generation number of tlsConn - avoid racing recoveries
	tlsConfig                *tls.Config
	tlsConn                  *tls.Conn // Our connection to the server
	x509CertPool             *x509.CertPool
	rootCAx509CertificatePEM []byte
	hostPortStr              string
}

// Client tracking structure
type Client struct {
	sync.Mutex
	halting          bool
	currentRequestID requestID // Last request ID - start from clock
	// tick at mount and increment from there?
	// Handle reset of time?
	connection         connectionTracker
	myUniqueID         string                // Unique ID across all clients
	outstandingRequest map[requestID]*reqCtx // Map of outstanding requests sent
	// or to be sent to server.  Key is assigned from currentRequestID
	highestConsecutive requestID // Highest requestID that can be
	// trimmed
	bt          *btree.BTree   // btree of requestID's acked
	goroutineWG sync.WaitGroup // Used to track outstanding goroutines
}

// ClientCallbacks contains the methods required when supporting
// callbacks from the Server.
type ClientCallbacks interface {
	Interrupt(payload []byte)
}

// TODO - pass loggers to Client and Server objects

// NewClient returns a Client structure
//
// If the server wants to send an async message to the client
// it uses the Interrupt method defined in cb
//
// NOTE: It is assumed that if a client calls NewClient(), it will
// always use a unique myUniqueID.   Otherwise, the server may have
// old entries.
//
// TODO - purge cache of old entries on server and/or use different
// starting point for requestID.
func NewClient(myUniqueID string, ipaddr string, port int, rootCAx509CertificatePEM []byte, cb *ClientCallbacks) (client *Client, err error) {

	client = &Client{myUniqueID: myUniqueID}
	portStr := fmt.Sprintf("%d", port)
	client.connection.state = INITIAL
	client.connection.hostPortStr = net.JoinHostPort(ipaddr, portStr)
	client.outstandingRequest = make(map[requestID]*reqCtx)
	client.connection.x509CertPool = x509.NewCertPool()
	client.bt = btree.New(2)

	// Add cert for root CA to our pool
	ok := client.connection.x509CertPool.AppendCertsFromPEM(rootCAx509CertificatePEM)
	if !ok {
		err = fmt.Errorf("x509CertPool.AppendCertsFromPEM() returned !ok")
		return nil, err
	}

	return client, err
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
	if client.connection.state == CONNECTED {
		client.connection.state = INITIAL
		client.connection.tlsConn.Close()
	}
	client.Unlock()

	// Wait for the goroutines to return
	client.goroutineWG.Wait()
}
