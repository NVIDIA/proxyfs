package retryrpc

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/retryrpc/rpctest"
)

// Test basic retryrpc primitives
//
// This unit test exists here since it uses jrpcfs which would be a
// circular dependency if the test was in retryrpc.
func TestRetryRPC(t *testing.T) {

	//	testServer(t)
	testBtree(t)
}

type MyType struct {
	field1 int
}

type MyRequest struct {
	Field1 int
}

type MyResponse struct {
	Error error
}

func (m *MyType) ExportedFunction(request MyRequest, response *MyResponse) (err error) {
	request.Field1 = 1
	return
}

func (m *MyType) unexportedFunction(i int) {
	m.field1 = i
}

// Test basic Server creation and deletion
func testServer(t *testing.T) {
	var (
		ipaddr = "127.0.0.1"
		port   = 24456
	)
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create new rpctest server - needed for calling
	// RPCs
	myJrpcfs := rpctest.NewServer()

	// Create a new RetryRPC Server.  Completed request will live on
	// completedRequests for 10 seconds.
	rrSvr := NewServer(10*time.Second, ipaddr, port)
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
	rrClnt, newErr := NewClient("client 1", ipaddr, port, rrSvr.Creds.RootCAx509CertificatePEM)
	assert.NotNil(rrClnt)
	assert.Nil(newErr)

	// Send an RPC which should return success
	pingRequest := &rpctest.PingReq{Message: "Ping Me!"}
	pingReply := &rpctest.PingReply{}
	sendErr := rrClnt.Send("RpcPing", pingRequest, pingReply)
	assert.Nil(sendErr)
	assert.Equal("pong 8 bytes", pingReply.Message)

	assert.Equal(0, rrSvr.PendingCnt())
	assert.Equal(1, rrSvr.CompletedCnt())

	// Send an RPC which should return an error
	pingRequest = &rpctest.PingReq{Message: "Ping Me!"}
	pingReply = &rpctest.PingReply{}
	sendErr = rrClnt.Send("RpcPingWithError", pingRequest, pingReply)
	assert.NotNil(sendErr)

	assert.Equal(0, rrSvr.PendingCnt())
	assert.Equal(2, rrSvr.CompletedCnt())

	// Send an RPC which should return an error
	pingRequest = &rpctest.PingReq{Message: "Ping Me!"}
	pingReply = &rpctest.PingReply{}
	sendErr = rrClnt.Send("RpcInvalidMethod", pingRequest, pingReply)
	assert.NotNil(sendErr)

	assert.Equal(0, rrSvr.PendingCnt())
	assert.Equal(3, rrSvr.CompletedCnt())

	// Stop the client before exiting
	rrClnt.Close()

	// Stop the server before exiting
	rrSvr.Close()
}

type RequestID uint64

// Less tests whether the current item is less than the given argument.
//
// This must provide a strict weak ordering.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
// hold one of either a or b in the tree).
func (a RequestID) Less(b btree.Item) bool {
	return a < b.(RequestID)
}

// printBTree is for debugging
func printBTree(tr *btree.BTree, msg string) {
	tr.Ascend(func(a btree.Item) bool {
		r := a.(RequestID)
		fmt.Printf("%v =========== - r is: %v\n", msg, r)
		return true
	})

}

func setHighestConsecutive(highestConsecutiveNum *RequestID, tr *btree.BTree) {
	tr.AscendGreaterOrEqual(*highestConsecutiveNum, func(a btree.Item) bool {
		r := a.(RequestID)
		c := *highestConsecutiveNum

		// If this item is a consecutive number then keep going.
		// Otherwise stop the Ascend now
		c++
		if r == c {
			*highestConsecutiveNum = r
		} else {
			// If we are past the first leaf and we do not have
			// consecutive numbers than break now instead of going
			// through rest of tree
			if r != tr.Min() {
				return false
			}
		}
		return true
	})

	// Now trim the btree up to highestConsecutiveNum
	m := tr.Min()
	if m != nil {
		i := m.(RequestID)
		for ; i < *highestConsecutiveNum; i++ {
			tr.Delete(i)
		}
	}
}

func testBtree(t *testing.T) {

	highestConsecutiveNum := RequestID(0)

	assert := assert.New(t)
	tr := btree.New(2)

	// Simulate requests completing out of order
	tr.ReplaceOrInsert(RequestID(10))
	tr.ReplaceOrInsert(RequestID(5))
	tr.ReplaceOrInsert(RequestID(11))

	setHighestConsecutive(&highestConsecutiveNum, tr)
	assert.Equal(RequestID(0), highestConsecutiveNum)

	// Now fillin first gap
	tr.ReplaceOrInsert(RequestID(4))
	tr.ReplaceOrInsert(RequestID(3))
	tr.ReplaceOrInsert(RequestID(2))
	tr.ReplaceOrInsert(RequestID(1))
	assert.Equal(int(7), tr.Len())

	setHighestConsecutive(&highestConsecutiveNum, tr)
	assert.Equal(int(3), tr.Len())
	assert.Equal(RequestID(5), highestConsecutiveNum)

	// Now fillin next set of gaps
	tr.ReplaceOrInsert(RequestID(6))
	tr.ReplaceOrInsert(RequestID(7))
	tr.ReplaceOrInsert(RequestID(8))
	tr.ReplaceOrInsert(RequestID(9))
	assert.Equal(int(7), tr.Len())

	setHighestConsecutive(&highestConsecutiveNum, tr)
	assert.Equal(int(1), tr.Len())
	assert.Equal(RequestID(11), highestConsecutiveNum)
}
