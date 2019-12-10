package retryrpc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

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
// TODO - add mutex around client.tcpConn so that only one
// reader or writer

/*
gotDisconnect - who calls - on read/write failure - could be holding lock!!!
===> should only be two outstanding - one writer and one reader....
1. set flag that reconnecting so others don't do it
2. reconnect
3. loop thru outstanding list of requests incomplete and resend with
   goroutines which send and wait result and write on chan from send()
4.

*/

/*
 readResponse - reads complete response (only one thread)
 1. block on read on socket - (how handle disconnects?)
 2. when gets reply issues a goroutine and goes and reads next one
 3. goroutine unmarshals request, looks it up, marks done and
	then sends reply on channel back to send()
 4. who strips it from channel???
*/

/*
Send() algorithm
1. build ctx including channel for reply struct
2. go doRequest()
		- doRequest() sends request and queues it.... what order???
		- matter if holding lock the whole time???
3. wait reply on channel
*/
func (client *Client) send(method string, rpcRequest interface{}) (reply *Reply, err error) {
	var crID uint64

	// TODO - Serialize writes on the socket to the server

	// Put request data into structure to be be marshaled into JSON
	jreq := jsonRequest{Method: method}
	jreq.Params[0] = rpcRequest
	jreq.MyUniqueID = client.myUniqueID

	// Grab ours under the lock since it could change once
	// we drop the lock
	client.Lock()
	client.currentRequestID++
	crID = client.currentRequestID
	jreq.RequestID = client.currentRequestID
	client.Unlock()

	req := Request{}
	req.JReq, err = json.Marshal(jreq)
	req.Len = int64(len(req.JReq))

	// Keep track of requests we are sending so we can resend them later as
	// needed.
	client.Lock()
	client.outstandingRequest[crID] = &req
	client.Unlock()

	// Now send the request to the server and retry operation if it fails
	// We need to grab the mutex here to prevent intermixed I/Os
	client.Lock()

	// Send length - how do whole request in one I/O?
	//
	// This is how you hton() in Golang
	err = binary.Write(client.tcpConn, binary.BigEndian, req.Len)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	fmt.Printf("CLIENT: Wrote req length: %v err: %v\n", req.Len, err)

	// Send JSON request
	bytesWritten, writeErr := client.tcpConn.Write(req.JReq)
	fmt.Printf("CLIENT: Wrote RPC REQEUST with bytesWritten: %v writeErr: %v\n", bytesWritten, writeErr)

	// Drop the lock once we wrote the request.
	client.Unlock()

	// TODO - move this into a goroutine.... use channel to pick up our response
	// since we could see anyones.....

	// Wait reply
	buf, getErr := getIO(client.tcpConn, "CLIENT")
	if getErr != nil {
		// TODO - error handling!
		err = getErr
		return
	}

	// Unmarshal back once to get the header fields
	jReply := jsonReply{}
	err = json.Unmarshal(buf, &jReply)
	if err != nil {
		fmt.Printf("CLIENT: Unmarshal of buf failed with err: %v\n", err)
		return
	}

	// Now unmarshal again to get the result field
	pingReply := pingJSONReply{}
	err = json.Unmarshal(buf, &pingReply)
	if err != nil {
		fmt.Printf("CLIENT: Unmarshal of buf failed with err: %v\n", err)
		return
	}

	// Remove from client.outstandingRequest
	client.Lock()
	delete(client.outstandingRequest, crID)
	client.Unlock()

	fmt.Printf("CLIENT: jReply: %+v\n", jReply)
	fmt.Printf("CLIENT: pingReply.Result: %+v\n", pingReply.Result)

	return
}
