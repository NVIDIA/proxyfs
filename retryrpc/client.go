package retryrpc

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/btree"
	"github.com/swiftstack/ProxyFS/logger"
)

// TODO - what if RPC was completed on Server1 and before response,
// proxyfsd fails over to Server2?   Client will resend - not idempotent
// This is outside of our initial requirements but something we should
// review.

//
// Send algorithm is:
// 1. Build ctx including channel for reply struct
// 2. Call goroutine to do marshalling and sending of
//    request to server
// 3. Wait on channel in reply struct for result
// 4. readResponses goroutine will read response on socket
//    and call a goroutine to do unmarshalling and notification
func (client *Client) send(method string, rpcRequest interface{}, rpcReply interface{}) (err error) {
	var crID requestID

	client.Lock()
	if client.connection.state == INITIAL {

		for {
			err = client.dial()
			if err == nil {
				break
			}
			client.Unlock()
			time.Sleep(100 * time.Millisecond)
			client.Lock()
		}
	}

	// Put request data into structure to be be marshaled into JSON
	jreq := jsonRequest{Method: method, HighestReplySeen: client.highestConsecutive}
	jreq.Params[0] = rpcRequest
	jreq.MyUniqueID = client.myUniqueID

	if client.halting == true {
		client.Unlock()
		e := fmt.Errorf("Calling retryrpc.Send() without dialing")
		logger.PanicfWithError(e, "")
		return
	}
	client.currentRequestID++
	crID = client.currentRequestID
	jreq.RequestID = crID
	client.Unlock()

	// Setup ioreq to write structure on socket to server
	ioreq, err := buildIoRequest(method, jreq)
	if err != nil {
		e := fmt.Errorf("Client buildIoRequest returned err: %v", err)
		logger.PanicfWithError(e, "")
		return err
	}

	// Create context to wait result and to handle retransmits
	ctx := &reqCtx{ioreq: *ioreq, rpcReply: rpcReply}
	ctx.answer = make(chan replyCtx)

	client.goroutineWG.Add(1)
	go client.sendToServer(crID, ctx, true)

	// Now wait for response
	answer := <-ctx.answer

	return answer.err
}

// sendToServer packages the request and marshals it before
// sending to server.
//
// At this point, the client will retry the request until either it
// completes OR the client is shutdown.
func (client *Client) sendToServer(crID requestID, ctx *reqCtx, queue bool) {

	defer client.goroutineWG.Done()

	// Now send the request to the server.
	// We need to grab the mutex here to serialize writes on socket
	client.Lock()

	// Keep track of requests we are sending so we can resend them later
	// as needed.   We queue the request first since we may get an error
	// we can just return.
	//
	// That should be okay since the restransmit goroutine will walk the
	// outstandingRequests queue and resend the request.
	//
	// Don't queue the request if we are retransmitting....
	if queue == true {
		client.outstandingRequest[crID] = ctx
	}

	// Record generation number of connection.  It is used during
	// retransmit to prevent multiple goroutines from closing the
	// connection and opening a new socket when only one is needed.
	ctx.genNum = client.connection.genNum

	// Send header
	client.connection.tlsConn.SetDeadline(time.Now().Add(deadlineIO))
	err := binary.Write(client.connection.tlsConn, binary.BigEndian, ctx.ioreq.Hdr)
	if err != nil {
		client.Unlock()

		// Just return - the retransmit code will start another
		// sendToServer() goroutine
		client.retransmit(ctx.genNum)
		return
	}

	// Send JSON request
	client.connection.tlsConn.SetDeadline(time.Now().Add(deadlineIO))
	bytesWritten, writeErr := client.connection.tlsConn.Write(ctx.ioreq.JReq)

	if (bytesWritten != len(ctx.ioreq.JReq)) || (writeErr != nil) {
		/* TODO - log message?
		fmt.Printf("CLIENT: PARTIAL Write! bytesWritten is: %v len(ctx.ioreq.JReq): %v writeErr: %v\n",
			bytesWritten, len(ctx.ioreq.JReq), writeErr)
		*/
		client.Unlock()

		// Just return - the retransmit code will start another
		// sendToServer() goroutine
		client.retransmit(ctx.genNum)
		return
	}

	client.Unlock()
	return
}

func (client *Client) notifyReply(buf []byte, genNum uint64) {
	defer client.goroutineWG.Done()

	// Unmarshal once to get the header fields
	jReply := jsonReply{}
	err := json.Unmarshal(buf, &jReply)
	if err != nil {
		// Don't have ctx to reply.  Assume read garbage on socket and
		// reconnect.

		// TODO - make log message
		e := fmt.Errorf("notifyReply failed to unmarshal buf: %+v err: %v", string(buf), err)
		fmt.Printf("%v\n", e)

		client.retransmit(genNum)
		return
	}

	// Remove request from client.outstandingRequest
	//
	// We do it here since we need to retrieve the RequestID from the
	// original request anyway.
	crID := jReply.RequestID
	client.Lock()
	// If this message is from an old socket - throw it away
	// since the request was resent.
	if client.connection.genNum != genNum {
		client.Unlock()
		fmt.Printf("===========    notifyReply() saw old message - throwing away\n")
		return
	}
	ctx, ok := client.outstandingRequest[crID]

	if !ok {
		// Saw reply for request which is no longer on outstandingRequest list
		// Can happen if handling retransmit
		client.Unlock()
		return
	}

	// Unmarshal the buf into the original reply structure
	m := svrResponse{Result: ctx.rpcReply}
	unmarshalErr := json.Unmarshal(buf, &m)
	if unmarshalErr != nil {
		e := fmt.Errorf("notifyReply failed to unmarshal buf: %v err: %v ctx: %v", string(buf), unmarshalErr, ctx)
		fmt.Printf("%v\n", e)

		// Assume read garbage on socket - close the socket and reconnect
		client.retransmit(genNum)
		client.Unlock()
		return
	}

	delete(client.outstandingRequest, crID)

	// Give reply to blocked send() - most developers test for nil err so
	// only set it if there is an error
	r := replyCtx{}
	if jReply.ErrStr != "" {
		r.err = fmt.Errorf("%v", jReply.ErrStr)
	}
	client.Unlock()
	ctx.answer <- r

	// Fork off a goroutine to update highestConsecutiveNum
	go client.updateHighestConsecutiveNum(crID)
}

// readReplies is a goroutine dedicated to reading responses from the server.
//
// As soon as it reads a complete response, it launches a goroutine to process
// the response and notify the blocked Send().
func (client *Client) readReplies(callingGenNum uint64, tlsConn *tls.Conn) {
	defer client.goroutineWG.Done()

	for {

		// Wait reply from server
		buf, getErr := getIO(callingGenNum, tlsConn)

		// This must happen before checking error
		client.Lock()
		if client.halting {
			client.Unlock()
			return
		}
		client.Unlock()

		if getErr != nil {
			// If we had an error reading socket - call retransmit() and exit
			// the goroutine.  retransmit()/dial() will have already
			// started another readReplies() goroutine.
			client.retransmit(callingGenNum)
			return
		}

		// We have a reply - let a goroutine do the unmarshalling and
		// sending the reply to blocked Send() so that this routine
		// can read the next response.
		client.goroutineWG.Add(1)
		go client.notifyReply(buf, callingGenNum)
	}
}

// retransmit is called when a socket related error occurs on the
// connection to the server.
func (client *Client) retransmit(genNum uint64) {
	client.Lock()

	// Check if we are already processing the socket error via
	// another goroutine.  If it is - return now.
	//
	// Since the original request is on client.outstandingRequest it will
	// have been resent by the first goroutine to encounter the error.
	if (genNum != client.connection.genNum) || (client.connection.state == RETRANSMITTING) {
		client.Unlock()
		return
	}

	if client.halting == true {
		client.Unlock()
		return
	}

	// We are the first goroutine to notice the error on the
	// socket - close the connection and start trying to reconnect.
	client.connection.tlsConn.Close()
	client.connection.state = RETRANSMITTING

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
		// While the lock was dropped we may be halting....
		if client.halting == true {
			client.Unlock()
			return
		}
	}

	client.connection.state = CONNECTED

	for crID, ctx := range client.outstandingRequest {
		// Note that we are holding the lock so these
		// goroutines will block until we release it.
		client.goroutineWG.Add(1)
		go client.sendToServer(crID, ctx, false)
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

	if client.connection.tlsConn != nil {
		client.connection.tlsConn.Close()
		client.connection.tlsConn = nil
	}
	client.connection.tlsConn = tlsConn
	client.connection.state = CONNECTED
	client.connection.genNum++

	// Start readResponse goroutine to read responses from server
	client.goroutineWG.Add(1)
	go client.readReplies(client.connection.genNum, tlsConn)

	return
}

// Less tests whether the current item is less than the given argument.
//
// This must provide a strict weak ordering.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
// hold one of either a or b in the tree).
//
// NOTE: It is assumed client lock is held when this is called.
func (a requestID) Less(b btree.Item) bool {
	return a < b.(requestID)
}

// printBTree prints the btree contents and is only for debugging
//
// NOTE: It is assumed client lock is held when this is called.
func printBTree(tr *btree.BTree, msg string) {
	tr.Ascend(func(a btree.Item) bool {
		r := a.(requestID)
		fmt.Printf("%v =========== - r is: %v\n", msg, r)
		return true
	})

}

// It is assumed the client lock is already held
func (client *Client) setHighestConsecutive() {
	client.bt.AscendGreaterOrEqual(client.highestConsecutive, func(a btree.Item) bool {
		r := a.(requestID)
		c := client.highestConsecutive

		// If this item is a consecutive number then keep going.
		// Otherwise stop the Ascend now
		c++
		if r == c {
			client.highestConsecutive = r
		} else {
			// If we are past the first leaf and we do not have
			// consecutive numbers than break now instead of going
			// through rest of tree
			if r != client.bt.Min() {
				return false
			}
		}
		return true
	})

	// Now trim the btree up to highestConsecutiveNum
	m := client.bt.Min()
	if m != nil {
		i := m.(requestID)
		for ; i < client.highestConsecutive; i++ {
			client.bt.Delete(i)
		}
	}
}

// updateHighestConsecutiveNum takes the requestID and calculates the
// highestConsective request ID we have seen.  This is done by putting
// the requestID into a btree of completed requestIDs.  Then calculating
// the highest consective number seen and updating Client.
func (client *Client) updateHighestConsecutiveNum(crID requestID) {
	client.Lock()
	client.bt.ReplaceOrInsert(crID)
	client.setHighestConsecutive()
	client.Unlock()
}
