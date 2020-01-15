package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"sync"
	"time"
)

type serverCredsStruct struct {
	rootCAx509CertificatePEM []byte
	serverTLSCertificate     tls.Certificate
}

func constructServerCreds(serverIPAddrAsString string) (serverCreds *serverCredsStruct, err error) {
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

	serverCreds = &serverCredsStruct{}

	timeNow = time.Now()

	commonX509NotBefore = time.Date(timeNow.Year()-1, time.January, 1, 0, 0, 0, 0, timeNow.Location())
	commonX509NotAfter = time.Date(timeNow.Year()+99, time.January, 1, 0, 0, 0, 0, timeNow.Location())

	rootCAx509SerialNumber, err = rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if nil != err {
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

	rootCAEd25519PublicKey, rootCAEd25519PrivateKey, err = ed25519.GenerateKey(nil)
	if nil != err {
		err = fmt.Errorf("ed25519.GenerateKey() [1] failed: %v", err)
		return
	}

	rootCAx509CertificateDER, err = x509.CreateCertificate(rand.Reader, rootCAx509CertificateTemplate, rootCAx509CertificateTemplate, rootCAEd25519PublicKey, rootCAEd25519PrivateKey)
	if nil != err {
		err = fmt.Errorf("x509.CreateCertificate() [1] failed: %v", err)
		return
	}

	serverCreds.rootCAx509CertificatePEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootCAx509CertificateDER})

	serverX509SerialNumber, err = rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if nil != err {
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

	serverEd25519PublicKey, serverEd25519PrivateKey, err = ed25519.GenerateKey(nil)
	if nil != err {
		err = fmt.Errorf("ed25519.GenerateKey() [2] failed: %v", err)
		return
	}

	serverX509CertificateDER, err = x509.CreateCertificate(rand.Reader, serverX509CertificateTemplate, rootCAx509CertificateTemplate, serverEd25519PublicKey, rootCAEd25519PrivateKey)
	if nil != err {
		err = fmt.Errorf("x509.CreateCertificate() [2] failed: %v", err)
		return
	}

	serverX509CertificatePEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverX509CertificateDER})

	serverEd25519PrivateKeyDER, err = x509.MarshalPKCS8PrivateKey(serverEd25519PrivateKey)
	if nil != err {
		err = fmt.Errorf("x509.MarshalPKCS8PrivateKeyx509.CreateCertificate() failed: %v", err)
		return
	}

	serverEd25519PrivateKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverEd25519PrivateKeyDER})

	serverCreds.serverTLSCertificate, err = tls.X509KeyPair(serverX509CertificatePEM, serverEd25519PrivateKeyPEM)

	return
}

func main() {
	const (
		serverIPAddrAsString  string = "127.0.0.1"
		serverTLSPortAsString string = "54321"
	)

	var (
		clientAndServerDoneWG sync.WaitGroup
		clientErr             error
		err                   error
		serverCreds           *serverCredsStruct
		serverErr             error
		serverReadyWG         sync.WaitGroup
	)

	serverCreds, err = constructServerCreds(serverIPAddrAsString)
	if nil != err {
		fmt.Printf("constructServerCreds() failed: %v\n", err)
		panic(err)
	}

	clientAndServerDoneWG.Add(2)

	serverReadyWG.Add(1)

	go server(serverIPAddrAsString, serverTLSPortAsString, serverCreds.serverTLSCertificate, &serverReadyWG, &clientAndServerDoneWG, &serverErr)

	serverReadyWG.Wait()

	if nil != serverErr {
		fmt.Printf("server() failed: %v\n", serverErr)
		os.Exit(1)
	}

	go client(serverIPAddrAsString, serverTLSPortAsString, serverCreds.rootCAx509CertificatePEM, &clientAndServerDoneWG, &clientErr)

	clientAndServerDoneWG.Wait()

	if nil != clientErr {
		fmt.Printf("client() failed: %v\n", clientErr)
	}
	if nil != serverErr {
		fmt.Printf("server() failed: %v\n", serverErr)
	}
	if (nil != clientErr) || (nil != serverErr) {
		os.Exit(1)
	}
}

func server(serverIPAddrAsString string, serverTLSPortAsString string, serverTLSCertificate tls.Certificate, serverReadyWG *sync.WaitGroup, serverDoneWG *sync.WaitGroup, serverErr *error) {
	var (
		buf                       []byte
		err                       error
		netConn                   net.Conn
		netListener               net.Listener
		serverTLSAddrPortAsString string
		serverTLSConfig           *tls.Config
	)

	*serverErr = nil

	serverTLSAddrPortAsString = net.JoinHostPort(serverIPAddrAsString, serverTLSPortAsString)

	serverTLSConfig = &tls.Config{
		Certificates: []tls.Certificate{serverTLSCertificate},
	}

	netListener, err = tls.Listen("tcp", serverTLSAddrPortAsString, serverTLSConfig)
	if nil != err {
		*serverErr = fmt.Errorf("tls.Listen() failed: %v", err)
		serverReadyWG.Done()
		return
	}

	*serverErr = nil

	serverReadyWG.Done()

	netConn, err = netListener.Accept()
	if nil != err {
		*serverErr = fmt.Errorf("netListener.Accept() failed: %v", err)
		serverDoneWG.Done()
		return
	}

	buf = make([]byte, 512)

	_, err = netConn.Read(buf)
	if nil != err {
		*serverErr = fmt.Errorf("netConn.Read() failed: %v", err)
		serverDoneWG.Done()
		return
	}

	fmt.Printf("server(): read from client: %s\n", string(buf[:]))

	buf = []byte("Server Answer")

	fmt.Printf("server(): sending to client: %s\n", string(buf[:]))

	_, err = netConn.Write(buf)
	if nil != err {
		*serverErr = fmt.Errorf("netConn.Write() failed: %v", err)
		serverDoneWG.Done()
		return
	}

	err = netConn.Close()
	if nil != err {
		*serverErr = fmt.Errorf("netConn.Close() failed: %v", err)
		serverDoneWG.Done()
		return
	}

	err = netListener.Close()
	if nil != err {
		*serverErr = fmt.Errorf("netListener.Close() failed: %v", err)
		serverDoneWG.Done()
		return
	}

	*serverErr = nil

	serverDoneWG.Done()
}

func client(serverIPAddrAsString string, serverTLSPortAsString string, rootCAx509CertificatePEM []byte, clientDoneWG *sync.WaitGroup, clientErr *error) {
	var (
		buf                       []byte
		clientTLSConfig           *tls.Config
		err                       error
		ok                        bool
		serverTLSAddrPortAsString string
		tlsConn                   *tls.Conn
		x509CertPool              *x509.CertPool
	)

	serverTLSAddrPortAsString = net.JoinHostPort(serverIPAddrAsString, serverTLSPortAsString)

	x509CertPool = x509.NewCertPool()

	ok = x509CertPool.AppendCertsFromPEM(rootCAx509CertificatePEM)
	if !ok {
		*clientErr = fmt.Errorf("x509CertPool.AppendCertsFromPEM() returned !ok")
		clientDoneWG.Done()
		return
	}

	clientTLSConfig = &tls.Config{
		RootCAs: x509CertPool,
	}

	tlsConn, err = tls.Dial("tcp", serverTLSAddrPortAsString, clientTLSConfig)
	if nil != err {
		*clientErr = fmt.Errorf("tls.Dial() failed: %v", err)
		clientDoneWG.Done()
		return
	}

	buf = []byte("Client Question")

	fmt.Printf("client(): sending to server: %s\n", string(buf[:]))

	_, err = tlsConn.Write(buf)
	if nil != err {
		*clientErr = fmt.Errorf("tlsConn.Write() failed: %v", err)
		clientDoneWG.Done()
		return
	}

	buf = make([]byte, 512)

	_, err = tlsConn.Read(buf)
	if nil != err {
		*clientErr = fmt.Errorf("tlsConn.Read() failed: %v", err)
		clientDoneWG.Done()
		return
	}

	fmt.Printf("client(): read from server: %s\n", string(buf[:]))

	err = tlsConn.Close()
	if nil != err {
		*clientErr = fmt.Errorf("tlsConn.Close() failed: %v", err)
		clientDoneWG.Done()
		return
	}

	*clientErr = nil

	clientDoneWG.Done()
}
