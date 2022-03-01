// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package retryrpc

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/google/btree"

	"github.com/NVIDIA/proxyfs/bucketstats"
)

const (
	ConnectionRetryDelayMultiplier = 2
	ConnectionRetryInitialDelay    = 100 * time.Millisecond
	ConnectionRetryLimit           = 8
)

const (
	// Prefix used for bucketstats of client
	clientSideGroupPrefix = "ClientSide-"
)

// Useful stats for the client side
type clientSideStatsInfo struct {
	RetransmitsStarted      bucketstats.Total           // Number of retransmits attempted
	SendCalled              bucketstats.Total           // Number of times Send called
	ReplyCalled             bucketstats.Total           // Number of times receive Reply to RPC
	UpcallCalled            bucketstats.Total           // Number of times received an Upcall
	SeesIOAndSignalsChannel bucketstats.BucketLog2Round // Tracks time after returnReply() -> getIO and reply channel
	TimeSendRPCUsec         bucketstats.BucketLog2Round // Tracks time when Send() called and returned
	SendToServer            bucketstats.BucketLog2Round // Tracks time takes to send message to server
}

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
	var (
		connectionRetryCount int
		connectionRetryDelay time.Duration
		crID                 requestID
	)

	client.stats.SendCalled.Add(1)

	client.Lock()
	if client.connection.state == INITIAL {

		connectionRetryCount = 0
		connectionRetryDelay = ConnectionRetryInitialDelay

		for {
			err = client.initialDial()
			if err == nil {

				// Now that we have a connection - we can setup bucketstats
				if client.connection.state == CONNECTED {
					bucketstats.Register(bucketStatsPkgName, client.GetStatsGroupName(), &client.stats)
				}
				break
			}
			client.Unlock()
			connectionRetryCount++
			if connectionRetryCount > ConnectionRetryLimit {
				client.logger.Fatalf("In send(), ConnectionRetryLimit (%v) on calling dial() exceeded", ConnectionRetryLimit)
			}
			client.logger.Printf("initialDial() failed; retrying: %v", err)
			time.Sleep(connectionRetryDelay)
			connectionRetryDelay *= ConnectionRetryDelayMultiplier
			client.Lock()
			if client.connection.state != INITIAL {
				break
			}
		}
	}

	// Put request data into structure to be be marshaled into JSON
	jreq := jsonRequest{Method: method, HighestReplySeen: client.highestConsecutive}
	jreq.Params[0] = rpcRequest
	jreq.MyUniqueID = client.myUniqueID

	if client.halting {
		client.Unlock()
		client.logger.Fatalf("Calling retryrpc.Send() without dialing")
		return
	}
	client.currentRequestID++
	crID = client.currentRequestID
	jreq.RequestID = crID
	client.Unlock()

	// Setup ioreq to write structure on socket to server
	ioreq, err := buildIoRequest(&jreq)
	if err != nil {
		client.logger.Fatalf("Client buildIoRequest returned err: %v", err)
		return err
	}

	// Create context to wait result and to handle retransmits
	ctx := &reqCtx{ioreq: ioreq, rpcReply: &rpcReply, startTime: time.Now()}
	ctx.answer = make(chan replyCtx)

	// Send request to server.
	//
	// We use a goroutine to be consistent with the way sendToServer() is called
	// in the retransmit() case.
	client.goroutineWG.Add(1)
	go client.sendToServer(crID, ctx, true)

	// Now wait for response
	answer := <-ctx.answer
	client.stats.TimeSendRPCUsec.Add(uint64(time.Duration(time.Since(ctx.startTime).Microseconds())))

	return answer.err
}

// sendToServer packages the request and marshals it before
// sending to server.
//
// At this point, the client will retry the request until either it
// completes OR the client is shutdown.
func (client *Client) sendToServer(crID requestID, ctx *reqCtx, queue bool) {
	defer client.goroutineWG.Done()

	startTime := time.Now()

	// Now send the request to the server.
	// We need to GRAB THE MUTEX HERE TO SERIALIZE WRITES on socket
	client.Lock()

	// Keep track of requests we are sending so we can resend them later
	// as needed.   We queue the request first since we may get an error
	// we can just return.
	//
	// That should be okay since the restransmit goroutine will walk the
	// outstandingRequests queue and resend the request.
	//
	// Don't queue the request if we are retransmitting....
	if queue {
		client.outstandingRequest[crID] = ctx
	}

	// Record generation number of connection.  It is used during
	// retransmit to prevent multiple goroutines from closing the
	// connection and opening a new socket when only one is needed.
	ctx.genNum = client.connection.genNum

	// The connection state may have changed between when this goroutine
	// was scheduled and when it grabbed the client lock.
	//
	// After we have queued the request, verify the state again before
	// attempting to use the connection.  If we are not CONNECTED, return
	// since we must already be in RETRANSMITTING. Since the request is
	// on the queue, it will be retried automatically.
	if client.connection.state != CONNECTED {
		client.Unlock()
		return
	}

	// Send header
	client.connection.SetDeadline(time.Now().Add(client.deadlineIO))
	err := binary.Write(client.connection.ioWriter(), binary.BigEndian, ctx.ioreq.Hdr)
	if err != nil {
		genNum := ctx.genNum
		client.Unlock()

		// Just return - the retransmit code will start another
		// sendToServer() goroutine
		client.retransmit(genNum)
		return
	}

	// Send JSON request
	client.connection.SetDeadline(time.Now().Add(client.deadlineIO))
	bytesWritten, writeErr := client.connection.Write(ctx.ioreq.JReq)

	if (bytesWritten != len(ctx.ioreq.JReq)) || (writeErr != nil) {
		/* TODO - log message?
		fmt.Printf("CLIENT: PARTIAL Write! bytesWritten is: %v len(ctx.ioreq.JReq): %v writeErr: %v\n",
			bytesWritten, len(ctx.ioreq.JReq), writeErr)
		*/
		genNum := ctx.genNum
		client.Unlock()

		// Just return - the retransmit code will start another
		// sendToServer() goroutine
		client.retransmit(genNum)
		return
	}

	client.Unlock()
	client.stats.SendToServer.Add(uint64(time.Duration(time.Since(startTime).Microseconds())))
}

func (client *Client) notifyReply(buf []byte, genNum uint64, recvResponse time.Time) {
	defer client.goroutineWG.Done()

	// Unmarshal once to get the header fields
	jReply := jsonReply{}
	err := json.Unmarshal(buf, &jReply)
	if err != nil {
		// Don't have ctx to reply.  Assume read garbage on socket and
		// reconnect.

		client.logger.Printf("notifyReply failed to unmarshal buf: %+v err: %v", string(buf), err)
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
		return
	}
	ctx, ok := client.outstandingRequest[crID]

	if !ok {
		// Saw reply for request which is no longer on outstandingRequest list
		// Can happen if handling retransmit
		client.Unlock()
		return
	}

	// Carefully drop lock while we are unmarshaling...
	client.Unlock()

	// Unmarshal the buf into the original reply structure
	m := svrResponse{Result: ctx.rpcReply}
	unmarshalErr := json.Unmarshal(buf, &m)
	if unmarshalErr != nil {
		client.logger.Printf("notifyReply failed to unmarshal buf: %v err: %v ctx: %v", string(buf), unmarshalErr, ctx)

		// Assume read garbage on socket - close the socket and reconnect
		// Drop client lock since retransmit() will acquire it.
		client.retransmit(genNum)
		return
	}

	client.Lock()

	// We dropped the lock above so we have to again check if we hit a retransmit case
	// and the socket was closed.
	//
	// If this message is from an old socket - throw it away since the request was resent.
	if client.connection.genNum != genNum {
		client.Unlock()
		return
	}

	_, ok = client.outstandingRequest[crID]
	if !ok {
		// Saw reply for request which is no longer on outstandingRequest list
		// Can happen if handling retransmit
		client.Unlock()
		return
	}
	delete(client.outstandingRequest, crID)
	client.Unlock()

	// Give reply to blocked send() - most developers test for nil err so
	// only set it if there is an error
	r := replyCtx{}
	if jReply.ErrStr != "" {
		r.err = fmt.Errorf("%v", jReply.ErrStr)
	}
	ctx.answer <- r
	client.stats.SeesIOAndSignalsChannel.Add(uint64(time.Duration(time.Since(recvResponse).Microseconds())))

	// Fork off a goroutine to update highestConsecutiveNum
	go client.updateHighestConsecutiveNum(crID)
}

// readReplies is a goroutine dedicated to reading responses from the server.
//
// As soon as it reads a complete response, it launches a goroutine to process
// the response and notify the blocked Send().
func (client *Client) readReplies(nC net.Conn, callingGenNum uint64) {
	defer client.goroutineWG.Done()
	var localCnt int

	for {
		// Capture copy of net.Conn safely - we might be closing/halting
		client.Lock()
		if client.halting {
			client.Unlock()
			return
		}
		client.Unlock()

		if nC == nil {
			return
		}

		// Wait reply from server
		buf, msgType, getErr := getIO(callingGenNum, client.deadlineIO, nC)

		// Since we reacquired lock - check if now halting
		client.Lock()
		if client.halting {
			client.Unlock()
			return
		}
		localCnt = len(client.outstandingRequest)
		client.Unlock()

		// Ignore timeouts on idle connections while reading header
		//
		// We consider a connection to be idle if we have no outstanding requests when
		// we get the timeout.   Otherwise, we call retransmit.
		if os.IsTimeout(getErr) && localCnt == 0 {
			continue
		}

		if getErr != nil {

			// If we had an error reading socket - call retransmit() and exit
			// the goroutine.  retransmit()/dial() will start another
			// readReplies() goroutine.
			client.retransmit(callingGenNum)
			return
		}
		recvResponse := time.Now()

		// Figure out what type of message it is
		switch msgType {
		case RPC:
			// We have a reply to an RPC - let a goroutine do the unmarshalling
			// and sending the reply to blocked Send() so that this routine
			// can read the next response.
			client.goroutineWG.Add(1)
			go client.notifyReply(buf, callingGenNum, recvResponse)
			client.stats.ReplyCalled.Add(1)

		case Upcall:

			// Spawn off goroutine to call callback
			client.goroutineWG.Add(1)
			go func(buf []byte) {
				client.cb.(ClientCallbacks).Interrupt(buf)
				client.goroutineWG.Done()
			}(buf)
			client.stats.UpcallCalled.Add(1)

		default:
			client.logger.Printf("CLIENT - invalid msgType: %v", msgType)
		}
	}
}

// retransmit is called when a socket related error occurs on the
// connection to the server.
func (client *Client) retransmit(genNum uint64) {
	var (
		connectionRetryCount int
		connectionRetryDelay time.Duration
	)

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

	if client.halting {
		client.Unlock()
		return
	}

	// We are the first goroutine to notice the error on the
	// socket - close the connection and start trying to reconnect.
	_ = client.connection.Close()
	client.connection.state = RETRANSMITTING
	client.stats.RetransmitsStarted.Add(1)

	connectionRetryCount = 0
	connectionRetryDelay = ConnectionRetryInitialDelay

	for {
		err := client.reDial()
		// If we were able to connect then break - otherwise retry
		// after a delay
		if err == nil {
			break
		}
		client.Unlock()
		connectionRetryCount++
		if connectionRetryCount > ConnectionRetryLimit {
			client.logger.Fatalf("In retransmit(), ConnectionRetryLimit (%v) on calling dial() exceeded", ConnectionRetryLimit)
		}
		time.Sleep(connectionRetryDelay)
		connectionRetryDelay *= ConnectionRetryDelayMultiplier
		client.Lock()
		// While the lock was dropped we may be halting....
		if client.halting {
			client.Unlock()
			return
		}
	}

	for crID, ctx := range client.outstandingRequest {
		// Note that we are holding the lock so these
		// goroutines will block until we release it.
		client.goroutineWG.Add(1)
		go client.sendToServer(crID, ctx, false)
	}
	client.Unlock()
}

// Get myUniqueID from server.   This is called when the client is
// creating this connection and wants a unique ID from the server.
//
// NOTE: Client lock is already held during this call.
func (client *Client) getMyUniqueID() (err error) {

	// Setup ioreq to write structure on socket to server
	iinreq, err := buildINeedIDRequest()
	if err != nil {
		client.logger.Fatalf("Client buildINeedIDRequest returned err: %v", err)
		return err
	}

	// We only have the header to send
	client.connection.SetDeadline(time.Now().Add(client.deadlineIO))
	err = binary.Write(client.connection.ioWriter(), binary.BigEndian, iinreq.Hdr)
	if err != nil {
		return
	}

	// Ask the server for the unique ID.
	// If we error, just return it and let caller close the connection.
	client.myUniqueID, err = client.readClientID(client.connection.genNum)
	if err != nil {
		return
	}

	return
}

// Send myUniqueID to server.   This is called when the client already
// knows myUniqueID
//
// NOTE: Client lock is already held during this call.
func (client *Client) sendMyInfo() (err error) {

	// Setup ioreq to write structure on socket to server
	isreq, err := buildSetIDRequest(client.myUniqueID)
	if err != nil {
		client.logger.Fatalf("Client buildSetIDRequest returned err: %v", err)
		return err
	}

	// Send header
	client.connection.SetDeadline(time.Now().Add(client.deadlineIO))
	err = binary.Write(client.connection.ioWriter(), binary.BigEndian, isreq.Hdr)
	if err != nil {
		return
	}

	// We are an existing client - send "MyUniqueID"
	client.connection.SetDeadline(time.Now().Add(client.deadlineIO))
	bytesWritten, writeErr := client.connection.Write(isreq.MyUniqueID)

	if uint32(bytesWritten) != isreq.Hdr.Len {
		e := fmt.Errorf("sendMyInfo length incorrect")
		err = e
		return
	}

	if writeErr != nil {
		err = writeErr
		return
	}

	// Nothing is sent back from server

	return
}

// reDial sets up a connection to the server as a result of
// retransmit being called.
//
// This is different than initialDial() which gets called the
// first time send() is called.
//
// It is assumed that the client lock is held.
//
// NOTE: Client lock is held
func (client *Client) reDial() (err error) {
	var entryState = client.connection.state

	// Now dial the server
	if client.connection.useTLS {
		client.connection.tlsConfig = &tls.Config{
			RootCAs: client.connection.x509CertPool,
		}

		d := &net.Dialer{KeepAlive: client.keepAlivePeriod}
		tlsConn, dialErr := tls.DialWithDialer(d, "tcp", client.connection.hostPortStr, client.connection.tlsConfig)
		if dialErr != nil {
			err = fmt.Errorf("tls.Dial() failed: %v", dialErr)
			return
		}

		client.connection.CloseIfOpen()

		client.connection.tlsConn = tlsConn
	} else {
		netConn, dialErr := net.Dial("tcp", client.connection.hostPortStr)
		if dialErr != nil {
			err = fmt.Errorf("net.Dial() failed: %v", dialErr)
			return
		}

		client.connection.CloseIfOpen()

		client.connection.netConn = netConn
	}

	client.connection.state = CONNECTED
	client.connection.genNum++

	// Send myUniqueID to server.   If this fails the dial will
	// be retried.
	err = client.sendMyInfo()
	if err != nil {
		_ = client.connection.Close()
		client.connection.state = entryState
		return
	}

	// Start readResponse goroutine to read responses from server
	//
	// Pass copy of connection used since a new readReplies() will be
	// started with a new connection as needed.
	nC := client.connection.castToNetConn()
	client.goroutineWG.Add(1)
	go client.readReplies(nC, client.connection.genNum)

	return
}

// readClientID reads unique client ID response from server
//
// Client lock is held
//
// NOTE: Client lock is held
func (client *Client) readClientID(callingGenNum uint64) (myUniqueID uint64, err error) {

	// Wait reply from server
	buf, msgType, getErr := getIO(callingGenNum, client.deadlineIO, client.connection.castToNetConn())

	// This must happen before checking error
	if client.halting {
		return
	}

	// Upper layer will close connection as a result of any error
	if getErr != nil {
		err = getErr
		return
	}

	if msgType != ReturnUniqueID {
		client.logger.Fatalf("CLIENT - invalid msgType: %v", msgType)
		return
	}

	err = json.Unmarshal(buf, &myUniqueID)
	if err != nil {
		client.logger.Fatalf("Unmarshal of buf: %v to myUniqueID failed with err: %v", buf, err)
	}
	return
}

// initialDial does the initial dial of the server and retrieves the
// unique ID for this connection from the server.
//
// If the connection fails and retransmit() is called then we will
// redial with reDial() instead of initialDial().
//
// The reason for the different dial function is that in one case
// we already know myUniqueId and in the other case we must get
// myUniqueId from the server.
//
// It is assumed that the client lock is held.
//
// NOTE: Client lock is held
func (client *Client) initialDial() (err error) {
	var entryState = client.connection.state

	// Now dial the server

	if client.connection.useTLS {
		client.connection.tlsConfig = &tls.Config{
			RootCAs: client.connection.x509CertPool,
		}

		d := &net.Dialer{KeepAlive: client.keepAlivePeriod}
		tlsConn, dialErr := tls.DialWithDialer(d, "tcp", client.connection.hostPortStr, client.connection.tlsConfig)
		if dialErr != nil {
			err = fmt.Errorf("tls.Dial() failed: %v", dialErr)
			return
		}

		client.connection.CloseIfOpen()

		client.connection.tlsConn = tlsConn
	} else {
		netConn, dialErr := net.Dial("tcp", client.connection.hostPortStr)
		if dialErr != nil {
			err = fmt.Errorf("net.Dial() failed: %v", dialErr)
			return
		}

		client.connection.CloseIfOpen()

		client.connection.netConn = netConn
	}

	client.connection.state = CONNECTED
	client.connection.genNum++

	// Ask server for my unique ID
	//
	// If this fails the dial will be retried.
	err = client.getMyUniqueID()
	if err != nil {
		_ = client.connection.Close()
		client.connection.state = entryState
		return
	}

	// Start readResponse goroutine to read responses from server
	//
	// Pass copy of connection used since a new readReplies() will be
	// started with a new connection as needed.
	nC := client.connection.castToNetConn()
	client.goroutineWG.Add(1)
	go client.readReplies(nC, client.connection.genNum)

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

// Below are wrapper func's on connectionTracker structs that perform
// the underlying/requested operation on the netConn or tlsConn based
// on the value of useTLS.

func (cT *connectionTracker) Close() (err error) {
	if cT.useTLS {
		err = cT.tlsConn.Close()
		cT.tlsConn = nil
	} else {
		err = cT.netConn.Close()
		cT.netConn = nil
	}

	return
}

func (cT *connectionTracker) CloseIfOpen() (err error) {
	if cT.useTLS {
		if nil != cT.tlsConn {
			err = cT.tlsConn.Close()
			cT.tlsConn = nil
		}
	} else {
		if nil != cT.netConn {
			err = cT.netConn.Close()
			cT.netConn = nil
		}
	}

	return
}

func (cT *connectionTracker) SetDeadline(t time.Time) (err error) {
	if cT.useTLS {
		err = cT.tlsConn.SetDeadline(t)
	} else {
		err = cT.netConn.SetDeadline(t)
	}

	return
}

func (cT *connectionTracker) Write(b []byte) (n int, err error) {
	if cT.useTLS {
		n, err = cT.tlsConn.Write(b)
	} else {
		n, err = cT.netConn.Write(b)
	}

	return
}

func (cT *connectionTracker) castToNetConn() (nC net.Conn) {
	if cT.useTLS {
		nC = cT.tlsConn
	} else {
		nC = cT.netConn
	}

	return
}

func (cT *connectionTracker) ioWriter() (w io.Writer) {
	if cT.useTLS {
		w = cT.tlsConn
	} else {
		w = cT.netConn
	}

	return
}
