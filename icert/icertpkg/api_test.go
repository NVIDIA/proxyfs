// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package icertpkg

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	testOrganizationCA       = "Test Organization CA"
	testOrganizationEndpoint = "Test Organization Endpoint"

	testCertificateTTL = time.Hour

	testV4DomainName = "localhost"
	testV6DomainName = "localhost6"
	testIPv4Address  = "127.0.0.1"
	testIPv6Address  = "::1"
	testTLSPort      = "9443"

	testTempDirPattern = "icertpkg_*"

	testCACertPEMFileName     = "ca_cert.pem"
	testCAKeyPEMFileName      = "ca_key.pem"
	testCACombinedPEMFileName = "ca_combined.pem"

	testIPAddressCertPEMFileName     = "ip_address_cert.pem"
	testIPAddressKeyPEMFileName      = "ip_address_key.pem"
	testIPAddressCombinedPEMFileName = "ip_address_combined.pem"

	testClientMsg = "ping\n"
	testServerMsg = "pong\n"
)

func TestEd25519DistinctCertAndKeyFiles(t *testing.T) {
	testAPI(t, GenerateKeyAlgorithmEd25519, true)
}
func TestEd25519CombinedCertAndKeyFile(t *testing.T) {
	testAPI(t, GenerateKeyAlgorithmEd25519, false)
}
func TestRSADistinctCertAndKeyFiles(t *testing.T) {
	testAPI(t, GenerateKeyAlgorithmRSA, true)
}
func TestRSACombinedCertAndKeyFile(t *testing.T) {
	testAPI(t, GenerateKeyAlgorithmRSA, false)
}

func testAPI(t *testing.T, generateKeyAlgorithm string, combined bool) {
	var (
		caCertPEM         []byte
		caCertPEMFilePath string
		caCertPool        *x509.CertPool
		// caKeyPEM                []byte
		caKeyPEMFilePath string
		clientErr        error
		clientTLSConfig  *tls.Config
		clientWG         sync.WaitGroup
		// endpointCertPEM         []byte
		endpointCertPEMFilePath string
		// endpointKeyPEM          []byte
		endpointKeyPEMFilePath string
		err                    error
		ipAddressPort          string
		ok                     bool
		tempDir                string
		serverErr              error
		serverNetListener      net.Listener
		serverTLSCertificate   tls.Certificate
		serverWG               sync.WaitGroup
	)

	tempDir, err = ioutil.TempDir("", testTempDirPattern)
	if nil != err {
		t.Fatalf("ioutil.TempDir(\"\", \"%s\") failed: %v", testTempDirPattern, err)
	}
	defer func(t *testing.T, tempDir string) {
		var (
			err error
		)

		err = os.RemoveAll(tempDir)
		if nil != err {
			t.Fatalf("os.RemoveAll(\"%s\") failed: %v", tempDir, err)
		}
	}(t, tempDir)

	if combined {
		caCertPEMFilePath = filepath.Join(tempDir, testCACombinedPEMFileName)
		caKeyPEMFilePath = caCertPEMFilePath
		endpointCertPEMFilePath = filepath.Join(tempDir, testIPAddressCombinedPEMFileName)
		endpointKeyPEMFilePath = endpointCertPEMFilePath
	} else {
		caCertPEMFilePath = filepath.Join(tempDir, testCACertPEMFileName)
		caKeyPEMFilePath = filepath.Join(tempDir, testCAKeyPEMFileName)
		endpointCertPEMFilePath = filepath.Join(tempDir, testIPAddressCertPEMFileName)
		endpointKeyPEMFilePath = filepath.Join(tempDir, testIPAddressKeyPEMFileName)
	}

	_, _, err = GenCACert(
		// caCertPEM, caKeyPEM, err = GenCACert(
		generateKeyAlgorithm,
		pkix.Name{
			Organization:  []string{testOrganizationCA},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		testCertificateTTL,
		caCertPEMFilePath,
		caKeyPEMFilePath)
	if nil != err {
		t.Fatalf("GenCACert() failed: %v", err)
	}

	_, _, err = GenEndpointCert(
		// endpointCertPEM, endpointKeyPEM, err = GenEndpointCert(
		generateKeyAlgorithm,
		pkix.Name{
			Organization:  []string{testOrganizationEndpoint},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		[]string{testV4DomainName, testV6DomainName},
		[]net.IP{net.ParseIP(testIPv4Address), net.ParseIP(testIPv6Address)},
		testCertificateTTL,
		caCertPEMFilePath,
		caKeyPEMFilePath,
		// caCertPEM,
		// caKeyPEM,
		endpointCertPEMFilePath,
		endpointKeyPEMFilePath)
	if nil != err {
		t.Fatalf("genEndpointCert() failed: %v", err)
	}

	// ipAddressPort = net.JoinHostPort(testV4DomainName, testTLSPort)
	// ipAddressPort = net.JoinHostPort(testV6DomainName, testTLSPort)
	ipAddressPort = net.JoinHostPort(testIPv4Address, testTLSPort)
	// ipAddressPort = net.JoinHostPort(testIPv6Address, testTLSPort)

	serverTLSCertificate, err = tls.LoadX509KeyPair(endpointCertPEMFilePath, endpointKeyPEMFilePath)
	if nil != err {
		t.Fatalf("tls.LoadX509KeyPair() failed: %v", err)
	}
	// serverTLSCertificate, err = tls.X509KeyPair(endpointCertPEM, endpointKeyPEM)
	// if nil != err {
	// 	t.Fatalf("tls.X509KeyPair() failed: %v", err)
	// }

	serverNetListener, err = tls.Listen("tcp", ipAddressPort, &tls.Config{Certificates: []tls.Certificate{serverTLSCertificate}})
	if nil != err {
		t.Fatalf("tls.Listen() failed: %v", err)
	}

	caCertPEM, err = ioutil.ReadFile(caCertPEMFilePath)
	if nil != err {
		t.Fatalf("ioutil.ReadFile(caCertPEMFilePath) failed: %v", err)
	}

	caCertPool = x509.NewCertPool()
	ok = caCertPool.AppendCertsFromPEM(caCertPEM)
	if !ok {
		t.Fatalf("caCertPool.AppendCertsFromPEM(caCertPEM) returned !ok")
	}

	clientTLSConfig = &tls.Config{RootCAs: caCertPool}

	serverWG.Add(1)

	go func() {
		var (
			bufioReader *bufio.Reader
			netConn     net.Conn
			receivedMsg string
		)

		netConn, serverErr = serverNetListener.Accept()
		if nil != serverErr {
			serverWG.Done()
			return
		}

		bufioReader = bufio.NewReader(netConn)

		for {
			receivedMsg, serverErr = bufioReader.ReadString('\n')
			if io.EOF == serverErr {
				serverErr = nil
				break
			} else if nil != serverErr {
				break
			} else {
				t.Logf("Server received %s", receivedMsg)
				_, serverErr = netConn.Write([]byte(testServerMsg))
				if nil != serverErr {
					break
				}
			}
		}

		_ = netConn.Close()

		serverWG.Done()
	}()

	clientWG.Add(1)

	go func() {
		var (
			bufioReader *bufio.Reader
			receivedMsg string
			tlsConn     *tls.Conn
		)

		tlsConn, clientErr = tls.Dial("tcp", ipAddressPort, clientTLSConfig)
		if nil != clientErr {
			clientWG.Done()
			return
		}

		_, clientErr = tlsConn.Write([]byte(testClientMsg))
		if nil != clientErr {
			_ = tlsConn.Close()
			clientWG.Done()
			return
		}

		bufioReader = bufio.NewReader(tlsConn)

		receivedMsg, serverErr = bufioReader.ReadString('\n')
		if nil != err {
			_ = tlsConn.Close()
			clientWG.Done()
			return
		}

		t.Logf("Client received %s", receivedMsg)

		_ = tlsConn.Close()

		clientWG.Done()
	}()

	clientWG.Wait()

	if nil != clientErr {
		t.Fatalf("client failed to successfully tls.Dial(): %v", clientErr)
	}

	_ = serverNetListener.Close()

	serverWG.Wait()

	if nil != serverErr {
		t.Fatalf("server failed to successfully serverNetListener.Accept(): %v", serverErr)
	}
}
