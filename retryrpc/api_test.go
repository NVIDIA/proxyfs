package retryrpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swiftstack/ProxyFS/jrpcfs"
)

// Test basic retryrpc primitives
func TestRetryRPC(t *testing.T) {

	testServer(t)
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
	assert := assert.New(t)
	zero := 0
	assert.Equal(0, zero)

	// Create a new Server.  Completed request will live on
	// completedRequests for 5 seconds.
	s := NewServer(5*time.Second, "127.0.0.1", 666)
	assert.NotNil(s)

	// Register the Server - sets up the methods supported by the
	// server
	// TODO - how do this while supporting several different methods???
	// Must pass interface{} somewhere....
	dummyVar := &MyType{field1: 1}
	err := s.Register(dummyVar)
	assert.Nil(err)

	// Server starts listening for requests on the ipaddr/port
	go s.Run()

	// TODO - do some work.....

	// Now - setup a client to send requests to the server
	c := NewClient("client 1")
	assert.NotNil(c)

	pingRequest := &jrpcfs.PingReq{Message: "Ping Me!"}
	_, sendErr := c.Send("Server.RpcPing", pingRequest)
	assert.Nil(sendErr)

	// Stop the server before exiting
	s.Close()

}
