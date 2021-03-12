// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package icertpkg

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
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
	testOrganizationCA        = "Test Organization CA"
	testOrganizationIPAddress = "Test Organization IP Address"

	testCertificateTTL = time.Hour

	testIPAddress = "127.0.0.1"
	testTLSPort   = "9443"

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
		caCertPEM                []byte
		caCertPemFilePath        string
		caCertPool               *x509.CertPool
		caKeyPemFilePath         string
		clientErr                error
		clientTLSConfig          *tls.Config
		clientWG                 sync.WaitGroup
		err                      error
		ipAddressCertPemFilePath string
		ipAddressKeyPemFilePath  string
		ipAddressPort            string
		ok                       bool
		tempDir                  string
		serverErr                error
		serverNetListener        net.Listener
		serverTLSCertificate     tls.Certificate
		serverWG                 sync.WaitGroup
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
		caCertPemFilePath = filepath.Join(tempDir, testCACombinedPEMFileName)
		caKeyPemFilePath = caCertPemFilePath
		ipAddressCertPemFilePath = filepath.Join(tempDir, testIPAddressCombinedPEMFileName)
		ipAddressKeyPemFilePath = ipAddressCertPemFilePath
	} else {
		caCertPemFilePath = filepath.Join(tempDir, testCACertPEMFileName)
		caKeyPemFilePath = filepath.Join(tempDir, testCAKeyPEMFileName)
		ipAddressCertPemFilePath = filepath.Join(tempDir, testIPAddressCertPEMFileName)
		ipAddressKeyPemFilePath = filepath.Join(tempDir, testIPAddressKeyPEMFileName)
	}

	err = GenCACert(testOrganizationCA, generateKeyAlgorithm, testCertificateTTL, caCertPemFilePath, caKeyPemFilePath)
	if nil != err {
		t.Fatalf("GenCACert() failed: %v", err)
	}

	err = GenIPAddressCert(testOrganizationIPAddress, testIPAddress, generateKeyAlgorithm, testCertificateTTL, caCertPemFilePath, caKeyPemFilePath, ipAddressCertPemFilePath, ipAddressKeyPemFilePath)
	if nil != err {
		t.Fatalf("GenCert() failed: %v", err)
	}

	ipAddressPort = net.JoinHostPort(testIPAddress, testTLSPort)

	serverTLSCertificate, err = tls.LoadX509KeyPair(ipAddressCertPemFilePath, ipAddressKeyPemFilePath)
	if nil != err {
		t.Fatalf("tls.LoadX509KeyPair() failed: %v", err)
	}

	serverNetListener, err = tls.Listen("tcp", ipAddressPort, &tls.Config{Certificates: []tls.Certificate{serverTLSCertificate}})
	if nil != err {
		t.Fatalf("tls.Listen() failed: %v", err)
	}

	caCertPEM, err = ioutil.ReadFile(caCertPemFilePath)
	if nil != err {
		t.Fatalf("ioutil.ReadFile(caCertPemFilePath) failed: %v", err)
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
