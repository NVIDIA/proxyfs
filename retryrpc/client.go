package retryrpc

import "encoding/json"

// TODO - Algorithm:
// 1. marshal method and args into JSON and put into Request struct
// 2. put request on client.outstandingRequests
// 3. Send request on socket to server and have goroutine block
//    on socket waiting for result
//    a. if read result then remove from queue and return result
//    b. if get error (which one?) on socket then resend request.
//       will have to make sure have enough info in request to make
//       the operation idempotent.   Assume client retries until
//       server comes back up?   Wait for failover to a peer?
//       Assume using VIP on proxyfs node?
// 4. Should we block forever? How kill?

// TODO - TODO - what if RCP was completed on Server1 and before response,
// proxyfsd fails over to Server2?   Client will resend - not idempotent!!!

/*
	sendRequest := makeRPC(method, rpcRequest)
	fmt.Printf("sendRequest: %v\n", sendRequest)
*/

// TODO - do we need to retransmit these requests in order?
func (client *Client) send(method string, rpcRequest interface{}) (response *Response, err error) {
	var crID uint64

	// Put request data into structure to be be marshaled into JSON
	jreq := jsonRequest{Method: method}
	jreq.Params[0] = rpcRequest
	jreq.MyUniqueID = client.myUniqueID

	client.Lock()
	client.currentRequestID++
	crID = client.currentRequestID
	jreq.RequestID = client.currentRequestID
	client.Unlock()

	req := Request{}
	req.JReq, err = json.Marshal(jreq)

	// Keep track of requests we are sending so we can resend them later as
	// needed.
	client.Lock()
	client.outstandingRequest[crID] = &req
	client.Unlock()

	// Now send the request to the server and retry operation if it fails
	return
}
