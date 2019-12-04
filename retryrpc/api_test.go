package retryrpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test basic retryrpc primitives
func TestRetryRPC(t *testing.T) {

	testServer(t)
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
	err := s.Register()
	assert.Nil(err)

	// Server starts listening for requests on the ipaddr/port
	go s.Run()

	// TODO - do some work.....

	// Now - setup a client to send requests to the server
	c := NewClient(1)
	assert.NotNil(c)

	// Make a new request and send to server
	// TODO - how create the JSON??? varadic arguments....
	request := MakeRPC("Server.RpcPing", "this is a message")
	assert.NotNil(request)

	_, sendErr := c.Send(request)
	assert.Nil(sendErr)

	// Stop the server before exiting
	s.Close()

}
