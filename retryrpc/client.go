package retryrpc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/logger"
)

// TODO - what if RPC was completed on Server1 and before response,
// proxyfsd fails over to Server2?   Client will resend - not idempotent!!!

// TODO - do we need to retransmit these requests in order?

/*
 * TODO - implement this...

gotDisconnect - who calls - on read/write failure - could be holding lock!!!
===> should only be two outstanding - one writer and one reader....
1. set flag that reconnecting so others don't do it
2. reconnect
3. loop thru outstanding list of requests incomplete and resend with
   goroutines which send and wait result and write on chan from send()
*/

//
// Send algorithm is:
// 1. Build ctx including channel for reply struct
// 2. Call goroutine to do marshalling and sending of
//    request to server
// 3. Wait on channel in reply struct for result
// 4. readResponses goroutine will read response on socket
//    and call a goroutine to do unmarshalling and notification
func (client *Client) send(method string, rpcRequest interface{}, rpcReply interface{}) (err error) {
	var crID uint64

	// Put request data into structure to be be marshaled into JSON
	jreq := jsonRequest{Method: method}
	jreq.Params[0] = rpcRequest
	jreq.MyUniqueID = client.myUniqueID

	// Grab RequestID under the lock since it could change once
	// we drop the lock
	client.Lock()
	if client.halting == true {
		client.Unlock()
		e := fmt.Errorf("Calling retryrpc.Send() without dialing")
		logger.PanicfWithError(e, "")
		return
	}
	client.currentRequestID++
	crID = client.currentRequestID
	jreq.RequestID = client.currentRequestID
	client.Unlock()

	// Setup ioreq to write structure on socket to server
	ioreq, err := buildIoRequest(method, jreq)
	if err != nil {
		return err
	}

	// Create context to wait result and to handle retransmits
	ctx := &reqCtx{ioreq: *ioreq, rpcReply: rpcReply}
	ctx.answer = make(chan replyCtx)

	// Keep track of requests we are sending so we can resend them later as
	// needed.
	client.Lock()
	client.outstandingRequest[crID] = ctx
	client.Unlock()

	client.goroutineWG.Add(1)
	go client.sendToServer(crID, ctx)

	// Now wait for response
	answer := <-ctx.answer

	return answer.err
}

// sendToServer packages the request and marshals it before
// sending to server.
//
// TODO - if the send fails, resend the request via the
// retransmit thread
func (client *Client) sendToServer(crID uint64, ctx *reqCtx) {
	defer client.goroutineWG.Done()
	var err error

	// TODO - retransmit ctx in gotDisconnect() routine

	// Now send the request to the server.
	// We need to grab the mutex here to serialize writes
	client.Lock()

	// Keep track of requests we are sending so we can resend them later
	// as needed.   We queue the request first since we may get an error
	// we can just return.
	//
	// That should be okay since the restransmit goroutine will walk the
	// outstandingRequests queue and resend the request.
	client.outstandingRequest[crID] = ctx

	// Send length - how do whole request in one I/O?
	//
	// This is how you hton() in Golang
	err = binary.Write(client.tcpConn, binary.BigEndian, ctx.ioreq.Hdr)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		client.Unlock()

		// TODO - retransimit code should retry operation and make it
		// transparent to caller.   For now, just return the error.

		// Legacy code requires the errno to be returned this way.
		e := fmt.Errorf("Error: %d", unix.ENOTCONN)
		sendReply(ctx, e)
		return
	}
	/*
		fmt.Printf("CLIENT: Wrote ioreq length: %v err: %v\n", ctx.ioreq.Hdr.Len, err)
	*/

	// Send JSON request
	bytesWritten, writeErr := client.tcpConn.Write(ctx.ioreq.JReq)
	if bytesWritten != len(ctx.ioreq.JReq) {
		fmt.Printf("CLIENT: PARTIAL Write! bytesWritten is: %v len(ctx.ioreq.JReq): %v writeErr: %v\n",
			bytesWritten, len(ctx.ioreq.JReq), writeErr)
	}
	if writeErr != nil {
		// TODO - handle disconnect or other error????
		fmt.Printf("CLIENT: Failed WRITE - HANLDE disconnect or other error??\n")
		client.Unlock()

		// TODO - retransimit code should retry operation and make it
		// transparent to caller.   For now, just return the error.
		// TODO - put in proper format with "Err: 128"
		e := fmt.Errorf("Error: %d", unix.ENOTCONN)
		sendReply(ctx, e)
		return
	}

	// Drop the lock once we wrote the request.
	client.Unlock()
	return
}

func sendReply(ctx *reqCtx, err error) {

	// Give reply to blocked send()
	r := replyCtx{err: err}
	ctx.answer <- r
}

func (client *Client) notifyReply(buf []byte) {
	defer client.goroutineWG.Done()

	// Unmarshal once to get the header fields
	jReply := jsonReply{}
	err := json.Unmarshal(buf, &jReply)
	if err != nil {
		// Don't have ctx to reply - only can panic
		e := fmt.Errorf("notifyReply failed to unmarshal buf: %+v err: %v", buf, err)
		logger.PanicfWithError(e, "")
		return
	}

	// Remove request from client.outstandingRequest
	//
	// We do it here since we need to retrieve the RequestID from the
	// original request anyway.
	crID := jReply.RequestID
	client.Lock()
	ctx := client.outstandingRequest[crID]
	delete(client.outstandingRequest, crID)
	client.Unlock()

	// Unmarshal the buf into the original reply structure
	m := svrResponse{Result: ctx.rpcReply}
	unmarshalErr := json.Unmarshal(buf, &m)
	if unmarshalErr != nil {
		sendReply(ctx, unmarshalErr)
		return
	}

	// Give reply to blocked send()
	r := replyCtx{err: err}
	ctx.answer <- r
}

// readReplies is a goroutine dedicated to reading responses from the server.
//
// As soon as it reads a complete response, it launches a goroutine to process
// the response and notify the blocked Send().
//
// TODO - if the read fails - attempt start of retransmitThread???
func (client *Client) readReplies() {
	defer client.goroutineWG.Done()

	for {

		// Wait reply from server
		buf, getErr := getIO(client.tcpConn)

		// This must happen before checking error
		if client.halting {
			break
		}

		if getErr != nil {
			// TODO - error handling!
			// call retransmit thread???
			logger.Infof("readReplies() - getIO() returned: %v\n", getErr)
			continue
		}

		// We have a reply - let a goroutine do the unmarshalling and
		// sending the reply to blocked Send()
		client.goroutineWG.Add(1)
		go client.notifyReply(buf)
	}

	return
}
