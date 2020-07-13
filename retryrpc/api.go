package retryrpc

// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
//
// NOTE: This package does handle cases where the server process dies.  There
// are still gaps where a server may complete an RPC and die before returning
// a response.

import (
	"container/list"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/swiftstack/ProxyFS/bucketstats"
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
	netListener      net.Listener
	tlsListener      net.Listener

	halting              bool
	goroutineWG          sync.WaitGroup // Used to track outstanding goroutines
	connLock             sync.Mutex
	connections          *list.List
	connWG               sync.WaitGroup
	Creds                *ServerCreds
	listenersWG          sync.WaitGroup
	receiver             reflect.Value          // Package receiver being served
	perClientInfo        map[string]*clientInfo // Key: "clientID".  Tracks clients
	completedTickerDone  chan bool
	completedLongTicker  *time.Ticker // Longer ~10 minute timer to trim
	completedShortTicker *time.Ticker // Shorter ~100ms timer to trim known completed
	deadlineIO           time.Duration
	keepAlivePeriod      time.Duration
	completedDoneWG      sync.WaitGroup
	dontStartTrimmers    bool // Used for testing
}

// ServerConfig is used to configure a retryrpc Server
type ServerConfig struct {
	LongTrim          time.Duration // How long the results of an RPC are stored on a Server before removed
	ShortTrim         time.Duration // How frequently completed and ACKed RPCs results are removed from Server
	IPAddr            string        // IP Address that Server uses to listen
	Port              int           // Port that Server uses to listen
	DeadlineIO        time.Duration // How long I/Os on sockets wait even if idle
	KeepAlivePeriod   time.Duration // How frequently a KEEPALIVE is sent
	dontStartTrimmers bool          // Used for testing
}

// NewServer creates the Server object
func NewServer(config *ServerConfig) *Server {
	var (
		err error
	)
	server := &Server{ipaddr: config.IPAddr, port: config.Port, completedLongTTL: config.LongTrim,
		completedAckTrim: config.ShortTrim, deadlineIO: config.DeadlineIO,
		keepAlivePeriod: config.KeepAlivePeriod, dontStartTrimmers: config.dontStartTrimmers}
	server.svrMap = make(map[string]*methodArgs)
	server.perClientInfo = make(map[string]*clientInfo)
	server.completedTickerDone = make(chan bool)
	server.connections = list.New()

	server.Creds, err = constructServerCreds(server.ipaddr)
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

	listenConfig := &net.ListenConfig{KeepAlive: server.keepAlivePeriod}
	server.netListener, err = listenConfig.Listen(context.Background(), "tcp", hostPortStr)
	if nil != err {
		err = fmt.Errorf("tls.Listen() failed: %v", err)
		return
	}

	server.tlsListener = tls.NewListener(server.netListener, tlsConfig)

	server.listenersWG.Add(1)

	// Some of the unit tests disable starting trimmers
	if !server.dontStartTrimmers {
		// Start ticker which removes older completedRequests
		server.completedLongTicker = time.NewTicker(server.completedLongTTL)
		// Start ticker which removes requests already ACKed by client
		server.completedShortTicker = time.NewTicker(server.completedAckTrim)
	}
	server.completedDoneWG.Add(1)
	if !server.dontStartTrimmers {
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
	} else {
		go func() {
			for {
				select {
				case <-server.completedTickerDone:
					server.completedDoneWG.Done()
					return
				}
			}
		}()
	}

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
//
// The message is "best effort" - if we fail to write on socket then the
// message is silently dropped on floor.
func (server *Server) SendCallback(clientID string, msg []byte) {

	// TODO - what if client no longer in list of current clients?
	var (
		localIOR ioReply
	)
	server.Lock()
	lci, ok := server.perClientInfo[clientID]
	if !ok {
		fmt.Printf("SERVER: SendCallback() - unable to find client UniqueID: %v\n", clientID)
		server.Unlock()
		return
	}
	server.Unlock()

	lci.Lock()
	currentCtx := lci.cCtx
	lci.Unlock()

	localIOR.JResult = msg
	setupHdrReply(&localIOR, Upcall)

	server.returnResults(&localIOR, currentCtx)
}

// Close stops the server
func (server *Server) Close() {
	server.Lock()
	server.halting = true
	server.Unlock()

	err := server.tlsListener.Close()
	if err != nil {
		logger.Errorf("server.tlsListener.Close() returned err: %v", err)
	}

	server.listenersWG.Wait()

	server.goroutineWG.Wait()

	// Now close the client sockets to wakeup them up
	server.closeClientConn()

	if !server.dontStartTrimmers {
		server.completedLongTicker.Stop()
		server.completedShortTicker.Stop()
	}
	server.completedTickerDone <- true
	server.completedDoneWG.Wait()

	// Cleanup bucketstats so that unit tests can run
	for _, ci := range server.perClientInfo {
		ci.Lock()
		bucketstats.UnRegister("proxyfs.retryrpc", ci.myUniqueID)
		ci.Unlock()

	}
}

// CloseClientConn - This is debug code to cause some connections to be closed
// It is called from a stress test case to cause retransmits
func (server *Server) CloseClientConn() {
	if server == nil {
		return
	}
	server.connLock.Lock()
	for c := server.connections.Front(); c != nil; c = c.Next() {
		conn := c.Value.(net.Conn)
		/* DEBUG code
		fmt.Printf("SERVER - closing localaddr conn: %v remoteaddr: %v\n", conn.LocalAddr().String(), conn.RemoteAddr().String())
		*/
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
	myUniqueID         string      // Unique ID across all clients
	cb                 interface{} // Callbacks to client
	deadlineIO         time.Duration
	keepAlivePeriod    time.Duration
	outstandingRequest map[requestID]*reqCtx // Map of outstanding requests sent
	// or to be sent to server.  Key is assigned from currentRequestID
	highestConsecutive requestID // Highest requestID that can be
	// trimmed
	bt          *btree.BTree   // btree of requestID's acked
	goroutineWG sync.WaitGroup // Used to track outstanding goroutines
	stats       clientSideStatsInfo
}

// ClientCallbacks contains the methods required when supporting
// callbacks from the Server.
type ClientCallbacks interface {
	Interrupt(payload []byte)
}

// ClientConfig is used to configure a retryrpc Client
type ClientConfig struct {
	MyUniqueID               string
	IPAddr                   string        // IP Address of Server
	Port                     int           // Port of Server
	RootCAx509CertificatePEM []byte        // Root certificate
	Callbacks                interface{}   // Structure implementing ClientCallbacks
	DeadlineIO               time.Duration // How long I/Os on sockets wait even if idle
	KeepAlivePeriod          time.Duration // How frequently a KEEPALIVE is sent
}

// TODO - pass loggers to Cient and Server objects

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
func NewClient(config *ClientConfig) (client *Client, err error) {

	client = &Client{myUniqueID: config.MyUniqueID, cb: config.Callbacks,
		keepAlivePeriod: config.KeepAlivePeriod, deadlineIO: config.DeadlineIO}
	portStr := fmt.Sprintf("%d", config.Port)
	client.connection.state = INITIAL
	client.connection.hostPortStr = net.JoinHostPort(config.IPAddr, portStr)
	client.outstandingRequest = make(map[requestID]*reqCtx)
	client.connection.x509CertPool = x509.NewCertPool()
	client.bt = btree.New(2)

	// Add cert for root CA to our pool
	ok := client.connection.x509CertPool.AppendCertsFromPEM(config.RootCAx509CertificatePEM)
	if !ok {
		err = fmt.Errorf("x509CertPool.AppendCertsFromPEM() returned !ok")
		return nil, err
	}

	bucketstats.Register("proxyfs.retryrpc", client.GetStatsGroupName(), &client.stats)

	return client, err
}

// Send the request and block until it has completed
func (client *Client) Send(method string, request interface{}, reply interface{}) (err error) {

	return client.send(method, request, reply)
}

// GetStatsGroupName returns the bucketstats GroupName for this client
func (client *Client) GetStatsGroupName() (s string) {

	return clientSideGroupPrefix + client.myUniqueID
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
	bucketstats.UnRegister("proxyfs.retryrpc", client.GetStatsGroupName())

}
