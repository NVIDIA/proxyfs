// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

import (
	"container/list"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/logger"
)

// PayloadProtocols defines the supported protocols for the payload
type PayloadProtocols int

// Support payload protocols
const (
	JSON PayloadProtocols = 1
)

const (
	currentRetryVersion = 1
)

type requestID uint64

// Useful stats for the clientInfo instance
type statsInfo struct {
	AddCompleted           bucketstats.Total           // Number added to completed list
	RmCompleted            bucketstats.Total           // Number removed from completed list
	RPCLenUsec             bucketstats.BucketLog2Round // Tracks length of RPCs
	ReplySize              bucketstats.BucketLog2Round // Tracks completed RPC reply size
	longestRPC             time.Duration               // Time of longest RPC
	longestRPCMethod       string                      // Method of longest RPC
	largestReplySize       uint64                      // Tracks largest RPC reply size
	largestReplySizeMethod string                      // Method of largest RPC reply size completed
	RPCattempted           bucketstats.Total           // Number of RPCs attempted - may be completed or in process
	RPCcompleted           bucketstats.Total           // Number of RPCs which completed - incremented after call returns
	RPCretried             bucketstats.Total           // Number of RPCs which were just pulled from completed list
}

// Server side data structure storing per client information
// such as completed requests, etc
type clientInfo struct {
	sync.Mutex
	cCtx                     *connCtx                      // Current connCtx for client
	myUniqueID               string                        // Unique ID of this client
	completedRequest         map[requestID]*completedEntry // Key: "RequestID"
	completedRequestLRU      *list.List                    // LRU used to remove completed request in ticker
	highestReplySeen         requestID                     // Highest consectutive requestID client has seen
	previousHighestReplySeen requestID                     // Previous highest consectutive requestID client has seen
	stats                    statsInfo
}

type completedEntry struct {
	reply   *ioReply
	lruElem *list.Element
}

// connCtx tracks a conn which has been accepted.
//
// It also contains the lock used for serialization when
// reading or writing on the socket.
type connCtx struct {
	sync.Mutex
	conn                net.Conn
	activeRPCsWG        sync.WaitGroup // WaitGroup tracking active RPCs from this client on this connection
	cond                *sync.Cond     // Signal waiting goroutines that serviceClient() has exited
	serviceClientExited bool
	ci                  *clientInfo // Back pointer to the CI
}

// pendingCtx tracks an individual request from a client
type pendingCtx struct {
	lock sync.Mutex
	buf  []byte   // Request
	cCtx *connCtx // Most recent connection to return results
}

// methodArgs defines the method provided by the RPC server
// as well as the request type and reply type arguments
type methodArgs struct {
	methodPtr *reflect.Method
	request   reflect.Type
	reply     reflect.Type
}

// completedLRUEntry tracks time entry was completed for
// expiration from cache
type completedLRUEntry struct {
	requestID     requestID
	timeCompleted time.Time
}

// Magic number written at the end of the ioHeader.   Used
// to detect if the complete header has been read.
const headerMagic uint32 = 0xCAFEFEED

// MsgType is the type of message being sent
type MsgType uint16

const (
	// RPC represents an RPC from client to server
	RPC MsgType = iota + 1
	// Upcall represents an upcall from server to client
	Upcall
	// PassID is the message sent by the client to identify itself to server
	PassID
)

// ioHeader is the header sent on the socket
type ioHeader struct {
	Len      uint32 // Number of bytes following header
	Protocol uint16
	Version  uint16
	Type     MsgType
	Magic    uint32 // Magic number - if invalid means have not read complete header
}

// ioRequest tracks fields written on wire
type ioRequest struct {
	Hdr  ioHeader
	JReq []byte // JSON containing request
}

// ioReply is the structure returned over the wire
type ioReply struct {
	Hdr     ioHeader
	JResult []byte // JSON containing response
}

// internalSetIDRequest is the structure sent over the wire
// when the connection is first made.   This is how the server
// learns the client ID
type internalSetIDRequest struct {
	Hdr        ioHeader
	MyUniqueID []byte // Client unique ID as byte
}

type replyCtx struct {
	err error
}

// reqCtx exists on the client and tracks a request passed to Send()
type reqCtx struct {
	ioreq    ioRequest // Wrapped request passed to Send()
	rpcReply interface{}
	answer   chan replyCtx
	genNum   uint64 // Generation number of socket when request sent
}

// jsonRequest is used to marshal an RPC request in/out of JSON
type jsonRequest struct {
	MyUniqueID       string         `json:"myuniqueid"`       // ID of client
	RequestID        requestID      `json:"requestid"`        // ID of this request
	HighestReplySeen requestID      `json:"highestReplySeen"` // Used to trim completedRequests on server
	Method           string         `json:"method"`
	Params           [1]interface{} `json:"params"`
}

// jsonReply is used to marshal an RPC response in/out of JSON
type jsonReply struct {
	MyUniqueID string      `json:"myuniqueid"` // ID of client
	RequestID  requestID   `json:"requestid"`  // ID of this request
	ErrStr     string      `json:"errstr"`
	Result     interface{} `json:"result"`
}

// svrRequest is used with jsonRequest when we unmarshal the
// parameters passed in an RPC.  This is how we get the rpcReply
// structure specific to the RPC
type svrRequest struct {
	Params [1]interface{} `json:"params"`
}

// svrReply is used with jsonReply when we marshal the reply
type svrResponse struct {
	Result interface{} `json:"result"`
}

func buildIoRequest(jReq jsonRequest) (ioreq *ioRequest, err error) {
	ioreq = &ioRequest{}
	ioreq.JReq, err = json.Marshal(jReq)
	if err != nil {
		return nil, err
	}
	ioreq.Hdr.Len = uint32(len(ioreq.JReq))
	ioreq.Hdr.Protocol = uint16(JSON)
	ioreq.Hdr.Version = currentRetryVersion
	ioreq.Hdr.Type = RPC
	ioreq.Hdr.Magic = headerMagic
	return
}

func setupHdrReply(ioreply *ioReply, t MsgType) {
	ioreply.Hdr.Len = uint32(len(ioreply.JResult))
	ioreply.Hdr.Protocol = uint16(JSON)
	ioreply.Hdr.Version = currentRetryVersion
	ioreply.Hdr.Type = t
	ioreply.Hdr.Magic = headerMagic
	return
}

func buildSetIDRequest(myUniqueID string) (isreq *internalSetIDRequest, err error) {
	isreq = &internalSetIDRequest{}
	isreq.MyUniqueID, err = json.Marshal(myUniqueID)
	if err != nil {
		return nil, err
	}
	isreq.Hdr.Len = uint32(len(isreq.MyUniqueID))
	isreq.Hdr.Protocol = uint16(JSON)
	isreq.Hdr.Version = currentRetryVersion
	isreq.Hdr.Type = PassID
	isreq.Hdr.Magic = headerMagic
	return
}

func getIO(genNum uint64, deadlineIO time.Duration, conn net.Conn) (buf []byte, msgType MsgType, err error) {
	if printDebugLogs {
		logger.Infof("conn: %v", conn)
	}

	// Read in the header of the request first
	var hdr ioHeader

	conn.SetDeadline(time.Now().Add(deadlineIO))
	err = binary.Read(conn, binary.BigEndian, &hdr)
	if err != nil {
		return
	}

	if hdr.Magic != headerMagic {
		err = fmt.Errorf("Incomplete read of header")
		return
	}

	if hdr.Len == 0 {
		err = fmt.Errorf("hdr.Len == 0")
		return
	}
	msgType = hdr.Type

	// Now read the rest of the structure off the wire.
	var numBytes int
	buf = make([]byte, hdr.Len)
	conn.SetDeadline(time.Now().Add(deadlineIO))
	numBytes, err = io.ReadFull(conn, buf)
	if err != nil {
		err = fmt.Errorf("Incomplete read of body")
		return
	}

	if hdr.Len != uint32(numBytes) {
		err = fmt.Errorf("Incomplete read of body")
		return
	}

	return
}

// constructServerCreds will generate root CA cert and server cert
//
// It is assumed that this is called on the "server" process and
// the caller will provide a mechanism to pass
// serverCreds.rootCAx509CertificatePEMkeys to the "clients".
func constructServerCreds(serverIPAddrAsString string) (serverCreds *ServerCreds, err error) {
	var (
		commonX509NotAfter            time.Time
		commonX509NotBefore           time.Time
		rootCAEd25519PrivateKey       ed25519.PrivateKey
		rootCAEd25519PublicKey        ed25519.PublicKey
		rootCAx509CertificateDER      []byte
		rootCAx509CertificateTemplate *x509.Certificate
		rootCAx509SerialNumber        *big.Int
		serverEd25519PrivateKey       ed25519.PrivateKey
		serverEd25519PrivateKeyDER    []byte
		serverEd25519PrivateKeyPEM    []byte
		serverEd25519PublicKey        ed25519.PublicKey
		serverX509CertificateDER      []byte
		serverX509CertificatePEM      []byte
		serverX509CertificateTemplate *x509.Certificate
		serverX509SerialNumber        *big.Int
		timeNow                       time.Time
	)

	serverCreds = &ServerCreds{}

	timeNow = time.Now()

	// TODO - what should the length of this be?  What if we want to eject a client
	// from the server?  How would that work?
	//
	// Do we even want the root CA at all?
	commonX509NotBefore = time.Date(timeNow.Year()-1, time.January, 1, 0, 0, 0, 0, timeNow.Location())
	commonX509NotAfter = time.Date(timeNow.Year()+99, time.January, 1, 0, 0, 0, 0, timeNow.Location())

	rootCAx509SerialNumber, err = rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		err = fmt.Errorf("rand.Int() [1] failed: %v", err)
		return
	}

	rootCAx509CertificateTemplate = &x509.Certificate{
		SerialNumber: rootCAx509SerialNumber,
		Subject: pkix.Name{
			Organization: []string{"CA Organization"},
			CommonName:   "Root CA",
		},
		NotBefore:             commonX509NotBefore,
		NotAfter:              commonX509NotAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Generate public and private key
	rootCAEd25519PublicKey, rootCAEd25519PrivateKey, err = ed25519.GenerateKey(nil)
	if err != nil {
		err = fmt.Errorf("ed25519.GenerateKey() [1] failed: %v", err)
		return
	}

	// Create the certificate with the keys
	rootCAx509CertificateDER, err = x509.CreateCertificate(rand.Reader,
		rootCAx509CertificateTemplate, rootCAx509CertificateTemplate, rootCAEd25519PublicKey, rootCAEd25519PrivateKey)
	if err != nil {
		err = fmt.Errorf("x509.CreateCertificate() [1] failed: %v", err)
		return
	}

	serverCreds.RootCAx509CertificatePEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootCAx509CertificateDER})

	serverX509SerialNumber, err = rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		err = fmt.Errorf("rand.Int() [2] failed: %v", err)
		return
	}

	serverX509CertificateTemplate = &x509.Certificate{
		SerialNumber: serverX509SerialNumber,
		Subject: pkix.Name{
			Organization: []string{"Server Organization"},
			CommonName:   "Server",
		},
		NotBefore:   commonX509NotBefore,
		NotAfter:    commonX509NotAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP(serverIPAddrAsString)},
	}

	// Generate the server public/private keys
	serverEd25519PublicKey, serverEd25519PrivateKey, err = ed25519.GenerateKey(nil)
	if err != nil {
		err = fmt.Errorf("ed25519.GenerateKey() [2] failed: %v", err)
		return
	}

	// Create the server certificate with the server public/private keys
	serverX509CertificateDER, err = x509.CreateCertificate(rand.Reader, serverX509CertificateTemplate, rootCAx509CertificateTemplate, serverEd25519PublicKey, rootCAEd25519PrivateKey)
	if err != nil {
		err = fmt.Errorf("x509.CreateCertificate() [2] failed: %v", err)
		return
	}

	serverX509CertificatePEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverX509CertificateDER})

	serverEd25519PrivateKeyDER, err = x509.MarshalPKCS8PrivateKey(serverEd25519PrivateKey)
	if err != nil {
		err = fmt.Errorf("x509.MarshalPKCS8PrivateKey() failed: %v", err)
		return
	}

	serverEd25519PrivateKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverEd25519PrivateKeyDER})

	serverCreds.serverTLSCertificate, err = tls.X509KeyPair(serverX509CertificatePEM, serverEd25519PrivateKeyPEM)

	return
}
