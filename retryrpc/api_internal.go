// Package retryrpc provides a client and server RPC model which survives
// lost connections on either the client or the server.
package retryrpc

import ()

// makeRPC takes the RPC method and arguments and returns a Request struct
// TODO - how use mountID, subsetID???
func makeRPC(method string, args ...interface{}) (request *Request) {

	// TODO - RequestID, MountID, JReq

	// This will:
	// 1. bump the client request ID
	// 2. Store the RPC method and args in JSON and store in JReq
	// 3. Store the mountID
	// 4. set the length of the request
	// 5. return the request
	request = &Request{}
	return
}
