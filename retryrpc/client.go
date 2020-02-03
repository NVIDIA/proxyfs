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

		// TODO - should this keep retrying the dial?
		err = client.dial()
		if err != nil {
			return
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
// At this point, the client will retry the request until either it
// completes OR the client is shutdown.
func (client *Client) sendToServer(crID requestID, ctx *reqCtx) {

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
	client.outstandingRequest[crID] = ctx

	// Record generation number of connection.  It is used during
	// retransmit to prevent multiple goroutines from closing the
	// connection and opening a new socket when only one is needed.
	ctx.genNum = client.connection.genNum

	// Send header
	err := binary.Write(client.connection.tlsConn, binary.BigEndian, ctx.ioreq.Hdr)
	if err != nil {
		client.Unlock()

		// Just return - the retransmit code will start another
		// sendToServer() goroutine
		client.retransmit(ctx.genNum)
		return
	}

	// Send JSON request
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

// Wakeup the blocked send by writing err back on channel
func sendReply(ctx *reqCtx, err error) {

	r := replyCtx{err: err}
	ctx.answer <- r
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
	ctx := client.outstandingRequest[crID]

	if ctx == nil {
		// Saw reply for request which is no longer on outstandingRequest list
		// Can happen if handling retransmit
		client.Unlock()
		return
	}

	// Unmarshal the buf into the original reply structure
	m := svrResponse{Result: ctx.rpcReply}
	unmarshalErr := json.Unmarshal(buf, &m)
	if unmarshalErr != nil {
		e := fmt.Errorf("notifyReply failed to unmarshal buf: %v err: %v", string(buf), unmarshalErr)
		fmt.Printf("%v\n", e)

		// Assume read garbage on socket - close the socket and reconnect
		client.retransmit(genNum)
		client.Unlock()
		return
	}

	delete(client.outstandingRequest, crID)
	client.Unlock()

	// Fork off a goroutine to update highestConsecutiveNum
	go client.updateHighestConsecutiveNum(crID)

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
			// If we had an error reading socket - call retransmit() and exit
			// the goroutine.  retransmit()/dial() will have already
			// started another readReplies() goroutine.
			client.retransmit(genNum)
			return
		}

		// We have a reply - let a goroutine do the unmarshalling and
		// sending the reply to blocked Send() so that this routine
		// can read the next response.
		client.goroutineWG.Add(1)
		go client.notifyReply(buf, genNum)
	}
}

// retransmit is called when a socket related error occurs on the
// connection to the server.
func (client *Client) retransmit(genNum uint64) {
	client.Lock()

	// Check if we are already processing the socket closing via
	// another goroutine.
	if genNum < client.connection.genNum {
		client.Unlock()
		return
	}

	// If we are the first goroutine to notice the error on the
	// socket - close the connection and block trying to reconnect.
	if (client.connection.state == CONNECTED) && (client.halting == false) {
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
