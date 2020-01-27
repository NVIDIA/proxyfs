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
	//fmt.Printf("\tLess() - a: %v b.(RequestID): %v return: %v\n", a, b.(RequestID), a < b.(RequestID))
	return a < b.(RequestID)
}

/* TODO - test case
1. simulate responses coming in out of order - 5, 10, 20, 1, 3, 2, etc
2. find "highest consecutive RequestID - (no gaps in numbers)"
3. simulate gaps being filled
4. simulate deleting and asserting that have correct numbers
5. evaluate storing outstandingRequest as btree or having only numbers in btree
*/

func setHighestConsecutive(highestConsecutiveNum *RequestID, tr *btree.BTree) {
	fmt.Printf("setHighestConsecutive()\n")
	tr.Ascend(func(a btree.Item) bool {
		r := a.(RequestID)
		/*
			c := *highestConsecutiveNum
		*/
		fmt.Printf("r is: %v\n", r)

		/*
			// If this item is a consecutive number then keep going.
			// Otherwise stop the Ascend now
			c++
			if r == *highestConsecutiveNum {
				*highestConsecutiveNum = r
				fmt.Printf("highestConsecutiveNum now: %v\n", highestConsecutiveNum)
				return true
			}
		*/
		if r == tr.Max() {
			return false
		}
		return true
	})
	return
}

func wrapper(tr *btree.BTree, r RequestID) {
	//fmt.Printf("BEFORE - ReplaceOrInsert(): %v - Min(): %v\n", r, tr.Min())
	tr.ReplaceOrInsert(r)
	fmt.Printf("AFTER - ReplaceOrInsert(): %v - Min(): %v\n", r, tr.Min())
}

func testBtree(t *testing.T) {

	highestConsecutiveNum := RequestID(0)

	assert := assert.New(t)
	tr := btree.New(2)

	// Simulate requests completing out of order
	wrapper(tr, RequestID(10))
	wrapper(tr, RequestID(5))
	wrapper(tr, RequestID(11))

	fmt.Printf("minimum value: %v\n", tr.Min())

	setHighestConsecutive(&highestConsecutiveNum, tr)
	assert.Equal(RequestID(0), highestConsecutiveNum)

	// Now fillin first gap
	wrapper(tr, RequestID(4))
	wrapper(tr, RequestID(3))
	wrapper(tr, RequestID(2))
	wrapper(tr, RequestID(1))
	fmt.Printf("new minimum value: %v len: %v\n", tr.Min(), tr.Len())
	assert.Equal(int(7), tr.Len())

	setHighestConsecutive(&highestConsecutiveNum, tr)
	assert.Equal(RequestID(5), highestConsecutiveNum)

	/*
		for i := RequestID(0); i < 10; i++ {
			tr.ReplaceOrInsert(i)
		}
		fmt.Println("len:       ", tr.Len())
		fmt.Println("get3:      ", tr.Get(RequestID(3)))
		fmt.Println("get100:    ", tr.Get(RequestID(100)))
		fmt.Println("del4:      ", tr.Delete(RequestID(4)))
		fmt.Println("del100:    ", tr.Delete(RequestID(100)))
		fmt.Println("replace5:  ", tr.ReplaceOrInsert(RequestID(5)))
		fmt.Println("replace100:", tr.ReplaceOrInsert(RequestID(100)))
		fmt.Println("min:       ", tr.Min())
		fmt.Println("delmin:    ", tr.DeleteMin())
		fmt.Println("max:       ", tr.Max())
		fmt.Println("delmax:    ", tr.DeleteMax())
		fmt.Println("len:       ", tr.Len())
	*/
}
