// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package retryrpc

import (
	"bytes"
	"crypto/tls"
	"crypto/x509/pkix"
	"fmt"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/icert/icertpkg"
)

const (
	testIPAddr = "127.0.0.1"
	testPort   = 24456
)

type testTLSCertsStruct struct {
	caCertPEMBlock       []byte
	caKeyPEMBlock        []byte
	endpointCertPEMBlock []byte
	endpointKeyPEMBlock  []byte
	endpointTLSCert      tls.Certificate
}

func newLogger() *log.Logger {
	return log.New(&logBuf, "", 0)
}

var (
	logBuf       bytes.Buffer
	testTLSCerts *testTLSCertsStruct
)

// Utility function to initialize testTLSCerts
func testTLSCertsAllocate(t *testing.T) {
	var (
		err error
	)

	testTLSCerts = &testTLSCertsStruct{}

	testTLSCerts.caCertPEMBlock, testTLSCerts.caKeyPEMBlock, err = icertpkg.GenCACert(
		icertpkg.GenerateKeyAlgorithmEd25519,
		pkix.Name{
			Organization:  []string{"Test Organization CA"},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		time.Hour,
		"",
		"")
	if nil != err {
		t.Fatalf("icertpkg.GenCACert() failed: %v", err)
	}

	testTLSCerts.endpointCertPEMBlock, testTLSCerts.endpointKeyPEMBlock, err = icertpkg.GenEndpointCert(
		icertpkg.GenerateKeyAlgorithmEd25519,
		pkix.Name{
			Organization:  []string{"Test Organization Endpoint"},
			Country:       []string{},
			Province:      []string{},
			Locality:      []string{},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		[]string{},
		[]net.IP{net.ParseIP("127.0.0.1")},
		time.Hour,
		testTLSCerts.caCertPEMBlock,
		testTLSCerts.caKeyPEMBlock,
		"",
		"")

	if nil != err {
		t.Fatalf("icertpkg.genEndpointCert() failed: %v", err)
	}

	testTLSCerts.endpointTLSCert, err = tls.X509KeyPair(testTLSCerts.endpointCertPEMBlock, testTLSCerts.endpointKeyPEMBlock)
	if nil != err {
		t.Fatalf("tls.LoadX509KeyPair() failed: %v", err)
	}
}

// Test basic retryrpc primitives
//
// This unit test exists here since it uses jrpcfs which would be a
// circular dependency if the test was in retryrpc.
func TestTCPRetryRPC(t *testing.T) {
	testTLSCerts = nil
	testRegister(t, false)
	testServer(t, false)
	testBtree(t, false)
	testStatsAndBucketstats(t)
}

func TestTLSRetryRPC(t *testing.T) {
	testTLSCertsAllocate(t) // Must be first - initializes certificate
	testRegister(t, true)
	testServer(t, true)
	testBtree(t, true)
	testStatsAndBucketstats(t)
}

func getNewServer(lt time.Duration, dontStartTrimmers bool, useTLS bool) (rrSvr *Server) {
	var (
		config *ServerConfig
	)

	if useTLS {
		config = &ServerConfig{
			LongTrim:          lt,
			ShortTrim:         100 * time.Millisecond,
			DNSOrIPAddr:       testIPAddr,
			Port:              testPort,
			DeadlineIO:        60 * time.Second,
			KeepAlivePeriod:   60 * time.Second,
			TLSCertificate:    testTLSCerts.endpointTLSCert,
			Logger:            newLogger(),
			dontStartTrimmers: dontStartTrimmers,
		}
	} else {
		config = &ServerConfig{
			LongTrim:          lt,
			ShortTrim:         100 * time.Millisecond,
			DNSOrIPAddr:       testIPAddr,
			Port:              testPort,
			DeadlineIO:        60 * time.Second,
			KeepAlivePeriod:   60 * time.Second,
			TLSCertificate:    tls.Certificate{},
			Logger:            newLogger(),
			dontStartTrimmers: dontStartTrimmers,
		}
	}

	// Create a new RetryRPC Server.  Completed request will live on
	// completedRequests for 10 seconds.
	rrSvr = NewServer(config)

	return
}

// Test register function finds all expected functions
func testRegister(t *testing.T, useTLS bool) {
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new TestPingServer - needed for calling RPCs
	myJrpcfs := &TestPingServer{}

	rrSvr := getNewServer(10*time.Second, false, useTLS)
	assert.NotNil(rrSvr)

	// Register the Server - sets up the methods supported by the
	// server
	err := rrSvr.Register(myJrpcfs)
	assert.Nil(err)

	// Make sure we discovered the correct functions
	assert.Equal(4, len(rrSvr.svrMap))
}

// Test basic Server creation and deletion
func testServer(t *testing.T, useTLS bool) {
	var (
		clientConfig *ClientConfig
	)

	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new TestPingServer - needed for calling RPCs
	myJrpcfs := &TestPingServer{}

	rrSvr := getNewServer(10*time.Second, false, useTLS)
	assert.NotNil(rrSvr)

	// Register the Server - sets up the methods supported by the
	// server
	err := rrSvr.Register(myJrpcfs)
	assert.Nil(err)

	// Start listening for requests on the ipaddr/port
	startErr := rrSvr.Start()
	assert.Nil(startErr, "startErr is not nil")

	// Tell server to start accepting and processing requests
	rrSvr.Run()

	// Now - setup a client to send requests to the server
	if useTLS {
		clientConfig = &ClientConfig{
			DNSOrIPAddr:              testIPAddr,
			Port:                     testPort,
			RootCAx509CertificatePEM: testTLSCerts.caCertPEMBlock,
			Callbacks:                nil,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
			Logger:                   newLogger(),
		}
	} else {
		clientConfig = &ClientConfig{
			DNSOrIPAddr:              testIPAddr,
			Port:                     testPort,
			RootCAx509CertificatePEM: nil,
			Callbacks:                nil,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
			Logger:                   newLogger(),
		}
	}
	rrClnt, newErr := NewClient(clientConfig)
	assert.NotNil(rrClnt)
	assert.Nil(newErr)

	// Send an RPC which should return success
	pingRequest := &TestPingReq{Message: "Ping Me!"}
	pingReply := &TestPingReply{}
	sendErr := rrClnt.Send("RpcTestPing", pingRequest, pingReply)
	assert.Nil(sendErr)
	assert.Equal("pong 8 bytes", pingReply.Message)
	assert.Equal(1, rrSvr.CompletedCnt())

	// Send an RPC which expects the client ID and which should return success
	pingRequest = &TestPingReq{Message: "Ping Me!"}
	pingReply = &TestPingReply{}
	sendErr = rrClnt.Send("RpcTestPingWithClientID", pingRequest, pingReply)
	assert.Nil(sendErr)
	assert.Equal("Client ID: 1 pong 8 bytes", pingReply.Message)
	assert.Equal(2, rrSvr.CompletedCnt())

	// Send an RPC which should return an error
	pingRequest = &TestPingReq{Message: "Ping Me!"}
	pingReply = &TestPingReply{}
	sendErr = rrClnt.Send("RpcTestPingWithError", pingRequest, pingReply)
	assert.NotNil(sendErr)

	assert.Equal(3, rrSvr.CompletedCnt())

	// Send an RPC which should return an error
	pingRequest = &TestPingReq{Message: "Ping Me!"}
	pingReply = &TestPingReply{}
	sendErr = rrClnt.Send("RpcTestInvalidMethod", pingRequest, pingReply)
	assert.NotNil(sendErr)

	assert.Equal(4, rrSvr.CompletedCnt())

	// Stop the client before exiting
	rrClnt.Close()

	// Stop the server before exiting
	rrSvr.Close()
}

func testBtree(t *testing.T, useTLS bool) {
	var (
		clientConfig *ClientConfig
	)

	assert := assert.New(t)

	rrSvr := getNewServer(10*time.Second, false, useTLS)
	assert.NotNil(rrSvr)

	// Setup a client - we only will be targeting the btree
	if useTLS {
		clientConfig = &ClientConfig{
			DNSOrIPAddr:              testIPAddr,
			Port:                     testPort,
			RootCAx509CertificatePEM: testTLSCerts.caCertPEMBlock,
			Callbacks:                nil,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
			Logger:                   newLogger(),
		}
	} else {
		clientConfig = &ClientConfig{
			DNSOrIPAddr:              testIPAddr,
			Port:                     testPort,
			RootCAx509CertificatePEM: nil,
			Callbacks:                nil,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
			Logger:                   newLogger(),
		}
	}
	client, newErr := NewClient(clientConfig)
	assert.NotNil(client)
	assert.Nil(newErr)

	// Simulate requests completing out of order
	client.updateHighestConsecutiveNum(requestID(10))
	client.updateHighestConsecutiveNum(requestID(5))
	client.updateHighestConsecutiveNum(requestID(11))

	client.setHighestConsecutive()
	assert.Equal(requestID(0), client.highestConsecutive)

	// Now fillin first gap
	client.updateHighestConsecutiveNum(requestID(4))
	client.updateHighestConsecutiveNum(requestID(3))
	client.updateHighestConsecutiveNum(requestID(2))
	client.updateHighestConsecutiveNum(requestID(1))
	assert.Equal(int(3), client.bt.Len())

	client.setHighestConsecutive()
	assert.Equal(int(3), client.bt.Len())
	assert.Equal(requestID(5), client.highestConsecutive)

	// Now fillin next set of gaps
	client.updateHighestConsecutiveNum(requestID(6))
	client.updateHighestConsecutiveNum(requestID(7))
	client.updateHighestConsecutiveNum(requestID(8))
	client.updateHighestConsecutiveNum(requestID(9))
	assert.Equal(int(1), client.bt.Len())

	client.setHighestConsecutive()
	assert.Equal(int(1), client.bt.Len())
	assert.Equal(requestID(11), client.highestConsecutive)
}

// Per pfsagent statistics
type clientStats struct {
	AddCompleted           bucketstats.Total           // Number added to completed list
	RmCompleted            bucketstats.Total           // Number removed from completed list
	TimeOfRPCUsec          bucketstats.BucketLog2Round // Average times of RPCs
	LongestRPCMethod       string                      // Method of longest RPC
	ReplySize              bucketstats.BucketLog2Round // Largest RPC reply size completed
	LargestReplySizeMethod string                      // Method of largest RPC reply size completed
	RPCcompleted           bucketstats.Total           // Number of RPCs which completed - incremented after call returns
	RPCretried             bucketstats.Total           // Number of RPCs which were just pulled from completed list
	RPCattempted           bucketstats.Total           // Number of RPCs attempted - may be completed or in process
	RPCinprocess           bucketstats.Total           // Number of RPCs presently calling RPC - decremented when completed
}

// Test use of bucketstats package
func testStatsAndBucketstats(t *testing.T) {
	var (
		myClient1       clientStats
		myUniqueClient1 = "1111111"

		myClient2       clientStats
		myUniqueClient2 = "2222222"
	)

	// Register from bucketstats from pfsagent #1
	bucketstats.Register("proxyfs.retryrpc", myUniqueClient1, &myClient1)

	// Register from bucketstats from pfsagent #2
	bucketstats.Register("proxyfs.retryrpc", myUniqueClient2, &myClient2)

	// Completed list stats
	myClient1.AddCompleted.Add(1)
	myClient1.RmCompleted.Add(1)

	// RPC counts
	myClient1.RPCcompleted.Add(1)
	myClient1.RPCretried.Add(1)
	myClient1.RPCattempted.Add(1)
	myClient1.RPCinprocess.Add(1)

	// Track duration of all RPCs in a graph
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	myClient1.TimeOfRPCUsec.Add(uint64(time.Since(start).Microseconds()))
	myClient1.ReplySize.Add(8192)

	// Example of pfsagent #2
	myClient2.RPCcompleted.Add(1)
	myClient2.RPCretried.Add(1)
	myClient2.RPCattempted.Add(1)
	myClient2.RPCinprocess.Add(1)

	// Dump stats
	/* DEBUG ONLY -
	fmt.Printf("pfsagent #1: %s\n", bucketstats.SprintStats(bucketstats.StatFormatParsable1, "proxyfs.retryrpc", myUniqueClient1))
	fmt.Printf("pfsagent #2: %s\n", bucketstats.SprintStats(bucketstats.StatFormatParsable1, "proxyfs.retryrpc", myUniqueClient2))
	*/

	// Unregister clients from bucketstats
	bucketstats.UnRegister("proxyfs.retryrpc", myUniqueClient1)
	bucketstats.UnRegister("proxyfs.retryrpc", myUniqueClient2)
}

// Unit test to show per method bucketstats
// bucketstats does not automatically register stats in a map, etc.
type methodStatsDemo struct {
	Method        string                      // Name of method
	Count         bucketstats.Total           // Number of times this method called
	TimeOfRPCCall bucketstats.BucketLog2Round // Length of time of this method of RPC
}

type methodClientStats struct {
	AddCompleted   bucketstats.Total           // Number added to completed list
	PerMethodStats map[string]*methodStatsDemo // Example of per method stats
}

func mAndN(name string, method string) string {
	return name + "-" + method
}

func TestMethodStatsAndBucketstats(t *testing.T) {
	var (
		myClient1       methodClientStats
		myUniqueClient1 = "1111111"
		method          = "method"
	)

	// Register from bucketstats from pfsagent #1
	bucketstats.Register("proxyfs.retryrpc", myUniqueClient1, &myClient1)

	// Register per method stats
	myClient1.PerMethodStats = make(map[string]*methodStatsDemo)
	for i := 0; i < 10; i++ {
		msd := &methodStatsDemo{}
		ms := method + strconv.Itoa(i)
		myClient1.PerMethodStats[ms] = msd
		bucketstats.Register("proxyfs.retryrpc", mAndN(myUniqueClient1, ms), msd)
	}

	// Completed list stats
	myClient1.AddCompleted.Add(1)

	// Per method stats
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 10; i++ {
		ms := method + strconv.Itoa(i)
		msd, _ := myClient1.PerMethodStats[ms]
		msd.Count.Add(uint64(i))
		msd.TimeOfRPCCall.Add(uint64(time.Since(start).Microseconds()))
		myClient1.PerMethodStats[ms] = msd
	}

	// Dump stats
	fmt.Printf("pfsagent #1: %s\n", bucketstats.SprintStats(bucketstats.StatFormatParsable1, "proxyfs.retryrpc", myUniqueClient1))
	for i := 0; i < 10; i++ {
		ms := method + strconv.Itoa(i)
		fmt.Printf("method: %s\n", bucketstats.SprintStats(bucketstats.StatFormatParsable1, "proxyfs.retryrpc", mAndN(myUniqueClient1, ms)))
	}

	// Unregister clients from bucketstats
	bucketstats.UnRegister("proxyfs.retryrpc", myUniqueClient1)
	for i := 0; i < 10; i++ {
		ms := method + strconv.Itoa(i)
		bucketstats.UnRegister("proxyfs.retryrpc", mAndN(myUniqueClient1, ms))
	}
}
