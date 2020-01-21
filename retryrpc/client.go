package retryrpc

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/swiftstack/ProxyFS/logger"
)

// TODO - what if RPC was completed on Server1 and before response,
// proxyfsd fails over to Server2?   Client will resend - not idempotent!!!

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

	client.Lock()
	if client.connection.state == INITIAL {

		// TODO - should this keep retrying the dial?   I assume if it fails
		// the first time then things will not recover...
		err = client.dial()
		if err != nil {
			return
		}
	}
	client.Unlock()

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
// At this point, the client will retry the request until either
// completes OR the client is shutdown.
func (client *Client) sendToServer(crID uint64, ctx *reqCtx) {

	defer client.goroutineWG.Done()

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

	// Record generation number of connection.  It is used during
	// retransmit to prevent multiple goroutines from closing the
	// connection and opening a new socket when only one is needed.
	ctx.genNum = client.connection.genNum

	// TODO - proper place for this comment???
	// At this point we cannot fail - keep retrying...

	// Send length - how do whole request in one I/O?
	//
	// This is how you hton() in Golang
	err := binary.Write(client.connection.tlsConn, binary.BigEndian, ctx.ioreq.Hdr)
	if err != nil {
		// TODO - make this a log message...
		fmt.Println("binary.Write failed:", err)
		client.Unlock()

		// Just return - the retransmit code will start another
		// sendToServer() goroutine
		client.retransmit(ctx.genNum)
		return
	}

	// Send JSON request
	bytesWritten, writeErr := client.connection.tlsConn.Write(ctx.ioreq.JReq)

	if bytesWritten != len(ctx.ioreq.JReq) {
		// TODO - make this a log message
		fmt.Printf("CLIENT: PARTIAL Write! bytesWritten is: %v len(ctx.ioreq.JReq): %v writeErr: %v\n",
			bytesWritten, len(ctx.ioreq.JReq), writeErr)
	}
	if writeErr != nil {
		client.Unlock()

		// Just return - the retransmit code will start another
		// sendToServer() goroutine
		client.retransmit(ctx.genNum)
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
		e := fmt.Errorf("notifyReply failed to unmarshal buf: %+v err: %v", string(buf), err)
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

	if ctx != nil {
		client.completedRequest[crID] = ctx
	} else {

		// TODO - remove this code since no way to trim
		// completedRequest data structure....
		// Verify we have already seen this request
		_, ok := client.completedRequest[crID]
		if !ok {
			fmt.Printf("SAW reply for request which is NOT on outstanding OR completed request map!!!\n")
		} else {
			fmt.Printf("SAW reply for request which IS on completed request map!!!\n")
			client.Unlock()
			return
		}

	}
	client.Unlock()

	if ctx == nil {
		fmt.Printf("buf: %v len(buf): %v jReply: %+v\n", string(buf), len(buf), jReply)
	}

	// Unmarshal the buf into the original reply structure
	m := svrResponse{Result: ctx.rpcReply}
	unmarshalErr := json.Unmarshal(buf, &m)
	if unmarshalErr != nil {
		e := fmt.Errorf("notifyReply failed to unmarshal buf: %v err: %v", string(buf), unmarshalErr)
		logger.PanicfWithError(e, "")
		return
	}

	// Give reply to blocked send() - most developers test for nil err so
	// only set it if there is an error
	r := replyCtx{}
	if jReply.ErrStr != "" {
		r.err = fmt.Errorf("%v", jReply.ErrStr)
	}
	ctx.answer <- r
}

// readReplies is a goroutine dedicated to reading responses from the server.
//
// As soon as it reads a complete response, it launches a goroutine to process
// the response and notify the blocked Send().
func (client *Client) readReplies() {
	defer client.goroutineWG.Done()

	client.Lock()
	tlsConn := client.connection.tlsConn
	genNum := client.connection.genNum
	client.Unlock()

	for {

		// Wait reply from server
		buf, getErr := getIO(tlsConn)

		// This must happen before checking error
		if client.halting {
			return
		}

		if getErr != nil {
			// If we had an error reading call retransmit() and exit
			// the goroutine.  retransmit()/dial() will have already
			// started another readReplies() goroutine.
			logger.Infof("readReplies() - getIO() returned: %v", getErr)
			client.retransmit(genNum)
			return
		}

		// We have a reply - let a goroutine do the unmarshalling and
		// sending the reply to blocked Send()
		client.goroutineWG.Add(1)
		go client.notifyReply(buf)
	}
}

// retransmit is called when a socket related error occurs on the
// connection to the server.
//
// If we have not started recovery, set Client.state = DISCONNECTED,
// call client.dial(), if successfult set Client.state = CONNECTED
// and resend all outstandingRequests to the server.
//
// If Client.state = DISCONNECTED, means we already started recovery
// .... race where our request is not sent????  maybe not since we
// resend outstandingRequests with state CONNECTED...... what is
// locking... logging so can see it....
// TODO - call this routine in sendToServer error case....
// already holding lock?????

// TODO -
// need gorLaunch() which increment waitGroup and starts goroutine

func (client *Client) retransmit(genNum uint64) {
	client.Lock()

	// TODO - add check for genNum and making sure only retransmit if
	// our genNum is higher than current!!!!
	if genNum < client.connection.genNum {
		fmt.Printf("RETRY RETRANSMIT!!!! - but already recovered!!!!\n")
		client.Unlock()
		return
	}

	// If we are the first goroutine to notice the error on the
	// socket - close the connection and block trying to reconnect.
	if (client.connection.state == CONNECTED) && (client.halting == false) {
		fmt.Printf("RETRY RETRANSMIT!!!!\n")
		client.connection.tlsConn.Close()
		client.connection.state = DISCONNECTED

		for {
			err := client.dial()
			// If we were able to connect then break - otherwise retry
			// after a delay
			if err == nil {
				break
			}
			client.Unlock()
			time.Sleep(100 * time.Millisecond)
			client.Lock()
		}

		client.connection.state = CONNECTED

		for crID, ctx := range client.outstandingRequest {
			client.goroutineWG.Add(1)

			// Note that we are holding the lock so these
			// goroutines will block until we release it.
			client.goroutineWG.Add(1)
			go client.sendToServer(crID, ctx)
		}

	}
	client.Unlock()
}

// dial sets up connection to server
// It is assumed that the client lock is held.
func (client *Client) dial() (err error) {

	client.connection.tlsConfig = &tls.Config{
		RootCAs: client.connection.x509CertPool,
	}

	// Now dial the server
	tlsConn, dialErr := tls.Dial("tcp", client.connection.hostPortStr, client.connection.tlsConfig)
	if dialErr != nil {
		err = fmt.Errorf("tls.Dial() failed: %v", dialErr)
		return
	}

	client.connection.tlsConn = tlsConn
	client.connection.state = CONNECTED
	client.connection.genNum++

	// Start readResponse goroutine to read responses from server
	client.goroutineWG.Add(1)
	go client.readReplies()

	return
}
