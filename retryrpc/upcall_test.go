// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package retryrpc

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test Upcall() functionality
func TestTCPUpCall(t *testing.T) {
	testTLSCerts = nil
	testUpCall(t, false)
}

func TestTLSUpCall(t *testing.T) {
	testTLSCertsAllocate(t)
	testUpCall(t, true)
}

type MyClient struct {
	sync.Mutex
	cond        *sync.Cond // Signal that received Interrupt() callback
	sawCallback bool       // True if Interrupt() was called
}

func (cb *MyClient) Interrupt(payload []byte) {
	cb.Lock()
	cb.sawCallback = true
	cb.cond.Broadcast()
	cb.Unlock()
	return
}

// Test getting an upcallbasic Server creation and deletion
func testUpCall(t *testing.T, useTLS bool) {
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
	cb := &MyClient{}
	cb.cond = sync.NewCond(&cb.Mutex)

	if useTLS {
		clientConfig = &ClientConfig{
			DNSOrIPAddr:              testIPAddr,
			Port:                     testPort,
			RootCAx509CertificatePEM: testTLSCerts.caCertPEMBlock,
			Callbacks:                cb,
			DeadlineIO:               60 * time.Second,
			KeepAlivePeriod:          60 * time.Second,
			Logger:                   newLogger(),
		}
	} else {
		clientConfig = &ClientConfig{
			DNSOrIPAddr:              testIPAddr,
			Port:                     testPort,
			RootCAx509CertificatePEM: nil,
			Callbacks:                cb,
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

	// Send an upcall() from the server to the client and
	// verify that client method is called.
	var msg1 []byte = []byte("server msg back to client")
	rrSvr.SendCallback(rrClnt.GetMyUniqueID(), msg1)

	// Wait until Interrupt() was called
	cb.Lock()
	for cb.sawCallback != true {
		cb.cond.Wait()
	}
	cb.Unlock()

	// Dump stats
	/* DEBUG ONLY -
	fmt.Printf("pfsagent dump stats: %s\n", bucketstats.SprintStats(bucketstats.StatFormatParsable1, "proxyfs.retryrpc", rrClnt.GetStatsGroupName()))
	*/

	// Stop the client before exiting
	rrClnt.Close()

	// Stop the server before exiting
	rrSvr.Close()
}
