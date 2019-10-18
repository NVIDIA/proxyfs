package swiftclient

import (
	"bytes"
	"container/list"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

const swiftVersion = "v1"

func drainConnections() {
	var (
		connection *connectionStruct
		volumeName string
	)

	for volumeName, connection = range globals.reservedChunkedConnection {
		_ = connection.tcpConn.Close()
		delete(globals.reservedChunkedConnection, volumeName)
	}

	globals.chunkedConnectionPool.Lock()
	// The following should not be necessary so, as such, will remain commented out
	/*
		for 0 < globals.chunkedConnectionPool.poolInUse {
			globals.chunkedConnectionPool.Unlock()
			time.Sleep(100 * time.Millisecond)
			globals.chunkedConnectionPool.Lock()
		}
	*/
	for 0 < globals.chunkedConnectionPool.lifoIndex {
		globals.chunkedConnectionPool.lifoIndex--
		connection = globals.chunkedConnectionPool.lifoOfAvailableConnections[globals.chunkedConnectionPool.lifoIndex]
		globals.chunkedConnectionPool.lifoOfAvailableConnections[globals.chunkedConnectionPool.lifoIndex] = nil
		_ = connection.tcpConn.Close()
	}
	globals.chunkedConnectionPool.Unlock()

	globals.nonChunkedConnectionPool.Lock()
	// The following should not be necessary so, as such, will remain commented out
	/*
		for 0 < globals.nonChunkedConnectionPool.poolInUse {
			globals.nonChunkedConnectionPool.Unlock()
			time.Sleep(100 * time.Millisecond)
			globals.nonChunkedConnectionPool.Lock()
		}
	*/
	for 0 < globals.nonChunkedConnectionPool.lifoIndex {
		globals.nonChunkedConnectionPool.lifoIndex--
		connection = globals.nonChunkedConnectionPool.lifoOfAvailableConnections[globals.nonChunkedConnectionPool.lifoIndex]
		globals.nonChunkedConnectionPool.lifoOfAvailableConnections[globals.nonChunkedConnectionPool.lifoIndex] = nil
		_ = connection.tcpConn.Close()
	}
	globals.nonChunkedConnectionPool.Unlock()
}

func getStarvationParameters() (starvationParameters *StarvationParameters) {
	starvationParameters = &StarvationParameters{
		ChunkedConnectionPoolCapacity:    globals.chunkedConnectionPool.poolCapacity,
		NonChunkedConnectionPoolCapacity: globals.nonChunkedConnectionPool.poolCapacity,
	}

	globals.chunkedConnectionPool.Lock()
	starvationParameters.ChunkedConnectionPoolInUse = globals.chunkedConnectionPool.poolInUse
	starvationParameters.ChunkedConnectionPoolNumWaiters = globals.chunkedConnectionPool.numWaiters
	globals.chunkedConnectionPool.Unlock()

	globals.nonChunkedConnectionPool.Lock()
	starvationParameters.NonChunkedConnectionPoolInUse = globals.nonChunkedConnectionPool.poolInUse
	starvationParameters.NonChunkedConnectionPoolNumWaiters = globals.nonChunkedConnectionPool.numWaiters
	globals.nonChunkedConnectionPool.Unlock()

	return
}

func acquireChunkedConnection(useReserveForVolumeName string) (connection *connectionStruct, err error) {
	var (
		connectionToBeCreated           bool
		cv                              *sync.Cond
		ok                              bool
		swiftChunkedStarvationCallbacks uint64
		wasStalled                      bool
	)

	if "" != useReserveForVolumeName {
		globals.reservedChunkedConnectionMutex.Lock()

		// track this statistic once per call to acquireChunkedConnection()
		freeConnections := globals.chunkedConnectionPool.poolCapacity - globals.chunkedConnectionPool.poolInUse
		globals.ObjectPutCtxtFreeConnection.Add(uint64(freeConnections))

		connection, ok = globals.reservedChunkedConnection[useReserveForVolumeName]
		if ok {
			// Reuse connection from globals.reservedChunkedConnection map...removing it from there since it's in use

			delete(globals.reservedChunkedConnection, useReserveForVolumeName)

			globals.reservedChunkedConnectionMutex.Unlock()

			stats.IncrementOperations(&stats.SwiftChunkedConnsReuseOps)
		} else {
			// No connection available...create a new one

			globals.reservedChunkedConnectionMutex.Unlock()

			connection = &connectionStruct{connectionNonce: globals.connectionNonce, reserveForVolumeName: useReserveForVolumeName}

			err = openConnection(fmt.Sprintf("swiftclient.acquireChunkedConnection(\"%v\")",
				useReserveForVolumeName), connection)
			if err != nil {
				connection = nil

				// if openConnection() failed then the cached connections are
				// likely broken and using them will create further mischief
				drainConnections()
			}

			stats.IncrementOperations(&stats.SwiftChunkedConnsCreateOps)
		}

		return
	}

	connectionToBeCreated = false
	wasStalled = false

	globals.chunkedConnectionPool.Lock()

	// track this statistic once per call to acquireChunkedConnection()
	freeConnections := globals.chunkedConnectionPool.poolCapacity - globals.chunkedConnectionPool.poolInUse
	globals.ObjectPutCtxtFreeConnection.Add(uint64(freeConnections))

	swiftChunkedStarvationCallbacks = 0

	for {
		if globals.chunkedConnectionPool.poolInUse < globals.chunkedConnectionPool.poolCapacity {
			break
		}

		wasStalled = true

		globals.chunkedConnectionPool.numWaiters++

		if nil == globals.starvationCallback {
			// Wait for a connection to be released before retrying
			cv = sync.NewCond(&globals.chunkedConnectionPool)
			_ = globals.chunkedConnectionPool.waiters.PushBack(cv)
			cv.Wait()
		} else {
			// Issue starvationCallback() (synchronously) before retrying
			globals.chunkedConnectionPool.Unlock()
			globals.starvationCallbackSerializer.Lock()
			globals.starvationCallback()
			globals.starvationCallbackSerializer.Unlock()
			globals.chunkedConnectionPool.Lock()
			swiftChunkedStarvationCallbacks++
		}

		globals.chunkedConnectionPool.numWaiters--
	}

	if 0 < swiftChunkedStarvationCallbacks {
		stats.IncrementOperationsBy(&stats.SwiftChunkedStarvationCallbacks, swiftChunkedStarvationCallbacks)
	}

	globals.chunkedConnectionPool.poolInUse++

	if 0 == globals.chunkedConnectionPool.lifoIndex {
		connectionToBeCreated = true
	} else {
		globals.chunkedConnectionPool.lifoIndex--
		connection = globals.chunkedConnectionPool.lifoOfAvailableConnections[globals.chunkedConnectionPool.lifoIndex]
		globals.chunkedConnectionPool.lifoOfAvailableConnections[globals.chunkedConnectionPool.lifoIndex] = nil
	}

	globals.chunkedConnectionPool.Unlock()

	if connectionToBeCreated {
		connection = &connectionStruct{connectionNonce: globals.connectionNonce, reserveForVolumeName: ""}
		err = openConnection("swiftclient.acquireChunkedConnection()", connection)
		if err != nil {
			connection = nil
			globals.chunkedConnectionPool.Lock()
			globals.chunkedConnectionPool.poolInUse--
			globals.chunkedConnectionPool.Unlock()

			// if openConnection() failed then the cached connections are
			// likely broken and using them will create further mischief
			drainConnections()
		}
		stats.IncrementOperations(&stats.SwiftChunkedConnsCreateOps)
	} else {
		stats.IncrementOperations(&stats.SwiftChunkedConnsReuseOps)
	}

	if wasStalled {
		stats.IncrementOperations(&stats.SwiftChunkedConnectionPoolStallOps)
	} else {
		stats.IncrementOperations(&stats.SwiftChunkedConnectionPoolNonStallOps)
	}

	return
}

func releaseChunkedConnection(connection *connectionStruct, keepAlive bool) {
	var (
		connectionToBeClosed bool
		cv                   *sync.Cond
		waiter               *list.Element
	)

	if "" != connection.reserveForVolumeName {
		if keepAlive &&
			(connection.connectionNonce == globals.connectionNonce) {
			// Re-insert connection in globals.reservedChunkedConnection map

			globals.reservedChunkedConnectionMutex.Lock()
			globals.reservedChunkedConnection[connection.reserveForVolumeName] = connection
			globals.reservedChunkedConnectionMutex.Unlock()
		} else {
			// Don't re-insert connection in globals.reservedChunkedConnection map... just Close() it

			_ = connection.tcpConn.Close()
		}

		return
	}

	connectionToBeClosed = false

	globals.chunkedConnectionPool.Lock()

	globals.chunkedConnectionPool.poolInUse--

	if keepAlive && (connection.connectionNonce == globals.connectionNonce) {
		globals.chunkedConnectionPool.lifoOfAvailableConnections[globals.chunkedConnectionPool.lifoIndex] = connection
		globals.chunkedConnectionPool.lifoIndex++
	} else {
		connectionToBeClosed = true
	}

	if 0 < globals.chunkedConnectionPool.waiters.Len() {
		// Note: If starvationCallback is armed, acquirers will be retrying (not cv.Wait()'ing)
		waiter = globals.chunkedConnectionPool.waiters.Front()
		cv = waiter.Value.(*sync.Cond)
		_ = globals.chunkedConnectionPool.waiters.Remove(waiter)
		cv.Signal()
	}

	globals.chunkedConnectionPool.Unlock()

	if connectionToBeClosed {
		_ = connection.tcpConn.Close()
	}
}

// Grow or shrink the chunked connection pool
//
func resizeChunkedConnectionPool(newPoolSize uint) {
	var (
		index                   uint16
		newAvailableConnections []*connectionStruct
		newSize                 uint16
	)

	newSize = uint16(newPoolSize)

	if newSize != globals.chunkedConnectionPool.poolCapacity {
		// Need to adjust the size of the pool

		globals.chunkedConnectionPool.Lock()

		if newSize < globals.chunkedConnectionPool.poolCapacity {
			// Shrinking the pool... must close any open connections beyond newSize

			for globals.chunkedConnectionPool.lifoIndex > newSize {
				globals.chunkedConnectionPool.lifoIndex--
				index = globals.chunkedConnectionPool.lifoIndex
				globals.chunkedConnectionPool.lifoOfAvailableConnections[index].tcpConn.Close()
				globals.chunkedConnectionPool.lifoOfAvailableConnections[index] = nil
			}
		} else {
			// Growing the pool... so clone and extend existing pool

			newAvailableConnections = make([]*connectionStruct, newPoolSize)
			for i := uint16(0); i < globals.chunkedConnectionPool.poolCapacity; i++ {
				newAvailableConnections[i] = globals.chunkedConnectionPool.lifoOfAvailableConnections[i]
			}
			for i := globals.chunkedConnectionPool.poolCapacity; i < newSize; i++ {
				newAvailableConnections[globals.chunkedConnectionPool.poolCapacity] = nil
			}

			globals.chunkedConnectionPool.lifoOfAvailableConnections = newAvailableConnections
		}

		globals.chunkedConnectionPool.poolCapacity = newSize

		globals.chunkedConnectionPool.Unlock()
	}
}

// Get a connection to the noauth server from the non-chunked connection pool.
//
// Return an error and a nil connection if it could not be opened.
//
func acquireNonChunkedConnection() (connection *connectionStruct, err error) {
	var (
		connectionToBeCreated bool
		cv                    *sync.Cond
		wasStalled            bool
	)

	connectionToBeCreated = false
	wasStalled = false

	globals.nonChunkedConnectionPool.Lock()

	// track this statistic once per call to acquireNonChunkedConnection()
	freeConnections := globals.nonChunkedConnectionPool.poolCapacity - globals.nonChunkedConnectionPool.poolInUse
	globals.ObjectNonChunkedFreeConnection.Add(uint64(freeConnections))

	for {
		if globals.nonChunkedConnectionPool.poolInUse < globals.nonChunkedConnectionPool.poolCapacity {
			break
		}
		wasStalled = true
		globals.nonChunkedConnectionPool.numWaiters++
		cv = sync.NewCond(&globals.nonChunkedConnectionPool)
		_ = globals.nonChunkedConnectionPool.waiters.PushBack(cv)
		cv.Wait()
		globals.nonChunkedConnectionPool.numWaiters--
	}

	globals.nonChunkedConnectionPool.poolInUse++

	if 0 == globals.nonChunkedConnectionPool.lifoIndex {
		connectionToBeCreated = true
	} else {
		globals.nonChunkedConnectionPool.lifoIndex--
		connection = globals.nonChunkedConnectionPool.lifoOfAvailableConnections[globals.nonChunkedConnectionPool.lifoIndex]
		globals.nonChunkedConnectionPool.lifoOfAvailableConnections[globals.nonChunkedConnectionPool.lifoIndex] = nil
	}

	globals.nonChunkedConnectionPool.Unlock()

	if connectionToBeCreated {
		connection = &connectionStruct{connectionNonce: globals.connectionNonce, reserveForVolumeName: ""}
		err = openConnection("swiftclient.acquireNonChunkedConnection()", connection)
		if err != nil {
			connection = nil
			globals.nonChunkedConnectionPool.Lock()
			globals.nonChunkedConnectionPool.poolInUse--
			globals.nonChunkedConnectionPool.Unlock()

			// if openConnection() failed then the cached connections are
			// likely broken and using them will create further mischief
			drainConnections()
		}
		stats.IncrementOperations(&stats.SwiftNonChunkedConnsCreateOps)
	} else {
		stats.IncrementOperations(&stats.SwiftNonChunkedConnsReuseOps)
	}

	if wasStalled {
		stats.IncrementOperations(&stats.SwiftNonChunkedConnectionPoolStallOps)
	} else {
		stats.IncrementOperations(&stats.SwiftNonChunkedConnectionPoolNonStallOps)
	}
	return
}

func releaseNonChunkedConnection(connection *connectionStruct, keepAlive bool) {
	var (
		connectionToBeClosed bool
		cv                   *sync.Cond
		waiter               *list.Element
	)

	connectionToBeClosed = false

	globals.nonChunkedConnectionPool.Lock()

	globals.nonChunkedConnectionPool.poolInUse--

	if keepAlive && (connection.connectionNonce == globals.connectionNonce) {
		globals.nonChunkedConnectionPool.lifoOfAvailableConnections[globals.nonChunkedConnectionPool.lifoIndex] = connection
		globals.nonChunkedConnectionPool.lifoIndex++
	} else {
		connectionToBeClosed = true
	}

	if 0 < globals.nonChunkedConnectionPool.waiters.Len() {
		waiter = globals.nonChunkedConnectionPool.waiters.Front()
		cv = waiter.Value.(*sync.Cond)
		_ = globals.nonChunkedConnectionPool.waiters.Remove(waiter)
		cv.Signal()
	}

	globals.nonChunkedConnectionPool.Unlock()

	if connectionToBeClosed {
		_ = connection.tcpConn.Close()
	}
}

// Grow or shrink the non-chunked connection pool
//
func resizeNonChunkedConnectionPool(newPoolSize uint) {
	var (
		index                   uint16
		newAvailableConnections []*connectionStruct
		newSize                 uint16
	)

	newSize = uint16(newPoolSize)

	if newSize != globals.nonChunkedConnectionPool.poolCapacity {
		// Need to adjust the size of the pool

		globals.nonChunkedConnectionPool.Lock()

		if newSize < globals.nonChunkedConnectionPool.poolCapacity {
			// Shrinking the pool... must close any open connections beyond newSize

			for globals.nonChunkedConnectionPool.lifoIndex > newSize {
				globals.nonChunkedConnectionPool.lifoIndex--
				index = globals.nonChunkedConnectionPool.lifoIndex
				globals.nonChunkedConnectionPool.lifoOfAvailableConnections[index].tcpConn.Close()
				globals.nonChunkedConnectionPool.lifoOfAvailableConnections[index] = nil
			}
		} else {
			// Growing the pool... so clone and extend existing pool

			newAvailableConnections = make([]*connectionStruct, newPoolSize)
			for i := uint16(0); i < globals.nonChunkedConnectionPool.poolCapacity; i++ {
				newAvailableConnections[i] = globals.nonChunkedConnectionPool.lifoOfAvailableConnections[i]
			}
			for i := globals.nonChunkedConnectionPool.poolCapacity; i < newSize; i++ {
				newAvailableConnections[globals.nonChunkedConnectionPool.poolCapacity] = nil
			}

			globals.nonChunkedConnectionPool.lifoOfAvailableConnections = newAvailableConnections
		}

		globals.nonChunkedConnectionPool.poolCapacity = newSize

		globals.nonChunkedConnectionPool.Unlock()
	}
}

func chunkedConnectionFreeCnt() (freeChunkedConnections int64) {
	globals.chunkedConnectionPool.Lock()
	freeChunkedConnections = int64(globals.chunkedConnectionPool.poolCapacity) - int64(globals.chunkedConnectionPool.poolInUse)
	globals.chunkedConnectionPool.Unlock()
	return
}

func nonChunkedConnectionFreeCnt() (freeNonChunkedConnections int64) {
	globals.nonChunkedConnectionPool.Lock()
	freeNonChunkedConnections = int64(globals.nonChunkedConnectionPool.poolCapacity) - int64(globals.nonChunkedConnectionPool.poolInUse)
	globals.nonChunkedConnectionPool.Unlock()
	return
}

// used during testing for error injection
var openConnectionCallCnt uint32

// Open a connection to the Swift NoAuth Proxy.
//
func openConnection(caller string, connection *connectionStruct) (err error) {
	if globals.chaosOpenConnectionFailureRate > 0 {
		// atomic add only used when testing
		if atomic.AddUint32(&openConnectionCallCnt, 1)%globals.chaosOpenConnectionFailureRate == 0 {
			err = fmt.Errorf("Simulated openConnection() error")
		}
	}

	if err == nil {
		connection.tcpConn, err = net.DialTCP("tcp4", nil, globals.noAuthTCPAddr)
	}
	if err != nil {
		logger.WarnfWithError(err, "%s cannot connect to Swift NoAuth Pipeline at %s",
			caller, globals.noAuthStringAddr)
	}
	return
}

func pathEscape(pathElements ...string) (pathEscaped string) {
	if 0 == len(pathElements) {
		pathEscaped = ""
	} else {
		pathEscaped = url.PathEscape(pathElements[0])
		for i := 1; i < len(pathElements); i++ {
			pathEscaped += "/" + url.PathEscape(pathElements[i])
		}
	}
	return
}

func writeBytesToTCPConn(tcpConn *net.TCPConn, buf []byte) (err error) {
	var (
		bufPos  = int(0)
		written int
	)

	for bufPos < len(buf) {
		written, err = tcpConn.Write(buf[bufPos:])
		if nil != err {
			return
		}

		bufPos += written
	}

	err = nil
	return
}

func writeHTTPRequestLineAndHeaders(tcpConn *net.TCPConn, method string, path string, headers map[string][]string) (err error) {
	var (
		bytesBuffer      bytes.Buffer
		headerName       string
		headerValue      string
		headerValueIndex int
		headerValues     []string
	)

	_, _ = bytesBuffer.WriteString(method + " " + path + " HTTP/1.1\r\n")

	_, _ = bytesBuffer.WriteString("Host: " + globals.noAuthStringAddr + "\r\n")
	_, _ = bytesBuffer.WriteString("User-Agent: ProxyFS\r\n")

	for headerName, headerValues = range headers {
		_, _ = bytesBuffer.WriteString(headerName + ": ")
		for headerValueIndex, headerValue = range headerValues {
			if 0 == headerValueIndex {
				_, _ = bytesBuffer.WriteString(headerValue)
			} else {
				_, _ = bytesBuffer.WriteString(", " + headerValue)
			}
		}
		_, _ = bytesBuffer.WriteString("\r\n")
	}

	_, _ = bytesBuffer.WriteString("\r\n")

	err = writeBytesToTCPConn(tcpConn, bytesBuffer.Bytes())

	return
}

func writeHTTPPutChunk(tcpConn *net.TCPConn, buf []byte) (err error) {
	err = writeBytesToTCPConn(tcpConn, []byte(fmt.Sprintf("%X\r\n", len(buf))))
	if nil != err {
		return
	}

	if 0 < len(buf) {
		err = writeBytesToTCPConn(tcpConn, buf)
		if nil != err {
			return
		}
	}

	err = writeBytesToTCPConn(tcpConn, []byte(fmt.Sprintf("\r\n")))

	return
}

func readByteFromTCPConn(tcpConn *net.TCPConn) (b byte, err error) {
	var (
		numBytesRead int
		oneByteBuf   = []byte{byte(0)}
	)

	for {
		numBytesRead, err = tcpConn.Read(oneByteBuf)
		if nil != err {
			return
		}

		if 1 == numBytesRead {
			b = oneByteBuf[0]
			err = nil
			return
		}
	}
}

func readBytesFromTCPConn(tcpConn *net.TCPConn, bufLen int) (buf []byte, err error) {
	var (
		bufPos       = int(0)
		numBytesRead int
	)

	buf = make([]byte, bufLen)

	for bufPos < bufLen {
		numBytesRead, err = tcpConn.Read(buf[bufPos:])
		if nil != err {
			return
		}

		bufPos += numBytesRead
	}

	err = nil
	return
}

func readBytesFromTCPConnIntoBuf(tcpConn *net.TCPConn, buf []byte) (err error) {
	var (
		bufLen       = cap(buf)
		bufPos       = int(0)
		numBytesRead int
	)

	for bufPos < bufLen {
		numBytesRead, err = tcpConn.Read(buf[bufPos:])
		if nil != err {
			return
		}

		bufPos += numBytesRead
	}

	err = nil
	return
}

func readHTTPEmptyLineCRLF(tcpConn *net.TCPConn) (err error) {
	var (
		b byte
	)

	b, err = readByteFromTCPConn(tcpConn)
	if nil != err {
		return
	}
	if '\r' != b {
		err = fmt.Errorf("readHTTPEmptyLineCRLF() didn't find the expected '\\r'")
		return
	}

	b, err = readByteFromTCPConn(tcpConn)
	if nil != err {
		return
	}
	if '\n' != b {
		err = fmt.Errorf("readHTTPEmptyLineCRLF() didn't find the expected '\\n'")
		return
	}

	err = nil
	return
}

func readHTTPLineCRLF(tcpConn *net.TCPConn) (line string, err error) {
	var (
		b           byte
		bytesBuffer bytes.Buffer
	)

	for {
		b, err = readByteFromTCPConn(tcpConn)
		if nil != err {
			return
		}

		if '\r' == b {
			b, err = readByteFromTCPConn(tcpConn)
			if nil != err {
				return
			}

			if '\n' != b {
				err = fmt.Errorf("readHTTPLine() expected '\\n' after '\\r' to terminate line")
				return
			}

			line = bytesBuffer.String()
			err = nil
			return
		}

		err = bytesBuffer.WriteByte(b)
		if nil != err {
			return
		}
	}
}

func readHTTPLineLF(tcpConn *net.TCPConn) (line string, err error) {
	var (
		b           byte
		bytesBuffer bytes.Buffer
	)

	for {
		b, err = readByteFromTCPConn(tcpConn)
		if nil != err {
			return
		}

		if '\n' == b {
			line = bytesBuffer.String()
			err = nil
			return
		}

		err = bytesBuffer.WriteByte(b)
		if nil != err {
			return
		}
	}
}

func readHTTPStatusAndHeaders(tcpConn *net.TCPConn) (httpStatus int, headers map[string][]string, err error) {
	var (
		colonSplit      []string
		commaSplit      []string
		commaSplitIndex int
		commaSplitValue string
		line            string
	)

	line, err = readHTTPLineCRLF(tcpConn)
	if nil != err {
		return
	}

	if len(line) < len("HTTP/1.1 XXX") {
		err = fmt.Errorf("readHTTPStatusAndHeaders() expected StatusLine beginning with \"HTTP/1.1 XXX\"")
		return
	}

	if !strings.HasPrefix(line, "HTTP/1.1 ") {
		err = fmt.Errorf("readHTTPStatusAndHeaders() expected StatusLine beginning with \"HTTP/1.1 XXX\"")
		return
	}

	httpStatus, err = strconv.Atoi(line[len("HTTP/1.1 ") : len("HTTP/1.1 ")+len("XXX")])
	if nil != err {
		return
	}

	headers = make(map[string][]string)

	for {
		line, err = readHTTPLineCRLF(tcpConn)
		if nil != err {
			return
		}

		if 0 == len(line) {
			return
		}

		colonSplit = strings.SplitN(line, ":", 2)
		if 2 != len(colonSplit) {
			err = fmt.Errorf("readHTTPStatusAndHeaders() expected HeaderLine")
			return
		}

		commaSplit = strings.Split(colonSplit[1], ",")

		for commaSplitIndex, commaSplitValue = range commaSplit {
			commaSplit[commaSplitIndex] = strings.TrimSpace(commaSplitValue)
		}

		headers[colonSplit[0]] = commaSplit
	}
}

func parseContentRange(headers map[string][]string) (firstByte int64, lastByte int64, objectSize int64, err error) {
	// A Content-Range header is of the form a-b/n, where a, b, and n
	// are all positive integers
	bytesPrefix := "bytes "

	values, ok := headers["Content-Range"]
	if !ok {
		err = fmt.Errorf("Content-Range header not present")
		return
	} else if ok && 1 != len(values) {
		err = fmt.Errorf("expected only one value for Content-Range header")
		return
	}

	if !strings.HasPrefix(values[0], bytesPrefix) {
		err = fmt.Errorf("malformed Content-Range header (doesn't start with %v)", bytesPrefix)
	}

	parts := strings.SplitN(values[0][len(bytesPrefix):], "/", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("malformed Content-Range header (no slash)")
		return
	}

	byteIndices := strings.SplitN(parts[0], "-", 2)
	if len(byteIndices) != 2 {
		err = fmt.Errorf("malformed Content-Range header (no dash)")
		return
	}

	firstByte, err = strconv.ParseInt(byteIndices[0], 10, 64)
	if err != nil {
		return
	}

	lastByte, err = strconv.ParseInt(byteIndices[1], 10, 64)
	if err != nil {
		return
	}

	objectSize, err = strconv.ParseInt(parts[1], 10, 64)
	return
}

func parseContentLength(headers map[string][]string) (contentLength int, err error) {
	var (
		contentLengthAsValues []string
		ok                    bool
	)

	contentLengthAsValues, ok = headers["Content-Length"]

	if ok {
		if 1 != len(contentLengthAsValues) {
			err = fmt.Errorf("parseContentLength() expected Content-Length HeaderLine with single value")
			return
		}

		contentLength, err = strconv.Atoi(contentLengthAsValues[0])
		if nil != err {
			err = fmt.Errorf("parseContentLength() could not parse Content-Length HeaderLine value: \"%s\"", contentLengthAsValues[0])
			return
		}

		if 0 > contentLength {
			err = fmt.Errorf("parseContentLength() could not parse Content-Length HeaderLine value: \"%s\"", contentLengthAsValues[0])
			return
		}
	} else {
		contentLength = 0
	}

	return
}

func parseTransferEncoding(headers map[string][]string) (chunkedTransfer bool) {
	var (
		transferEncodingAsValues []string
		ok                       bool
	)

	transferEncodingAsValues, ok = headers["Transfer-Encoding"]
	if !ok {
		chunkedTransfer = false
		return
	}

	if 1 != len(transferEncodingAsValues) {
		chunkedTransfer = false
		return
	}

	if "chunked" == transferEncodingAsValues[0] {
		chunkedTransfer = true
	} else {
		chunkedTransfer = false
	}

	return
}

func parseConnection(headers map[string][]string) (connectionStillOpen bool) {
	var (
		connectionAsValues []string
		ok                 bool
	)

	connectionAsValues, ok = headers["Connection"]
	if !ok {
		connectionStillOpen = true
		return
	}

	if 1 != len(connectionAsValues) {
		connectionStillOpen = true
		return
	}

	if "close" == connectionAsValues[0] {
		connectionStillOpen = false
	} else {
		connectionStillOpen = true
	}

	return
}

func readHTTPPayloadAsByteSlice(tcpConn *net.TCPConn, headers map[string][]string) (payloadAsByteSlice []byte, err error) {
	var (
		chunk         []byte
		contentLength int
	)

	if parseTransferEncoding(headers) {
		payloadAsByteSlice = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(tcpConn)
			if nil != err {
				return
			}

			if 0 == len(chunk) {
				break
			}

			payloadAsByteSlice = append(payloadAsByteSlice, chunk...)
		}
	} else {
		contentLength, err = parseContentLength(headers)
		if nil != err {
			return
		}

		if 0 == contentLength {
			payloadAsByteSlice = make([]byte, 0)
		} else {
			payloadAsByteSlice, err = readBytesFromTCPConn(tcpConn, contentLength)
			if nil != err {
				return
			}
		}
	}

	return
}

func readHTTPPayloadAsString(tcpConn *net.TCPConn, headers map[string][]string) (payloadString string, err error) {
	var (
		payloadByteSlice []byte
	)

	payloadByteSlice, err = readHTTPPayloadAsByteSlice(tcpConn, headers)
	if nil != err {
		return
	}

	payloadString = string(payloadByteSlice[:])

	return
}

func readHTTPPayloadLines(tcpConn *net.TCPConn, headers map[string][]string) (lines []string, err error) {
	var (
		buf                  []byte
		bufCurrentPosition   int
		bufLineStartPosition int
		contentLength        int
	)

	buf, err = readHTTPPayloadAsByteSlice(tcpConn, headers)
	if nil != err {
		return
	}

	contentLength = len(buf)

	lines = make([]string, 0)

	if 0 < len(buf) {
		bufLineStartPosition = 0
		bufCurrentPosition = 0

		for bufCurrentPosition < contentLength {
			if '\n' == buf[bufCurrentPosition] {
				if bufCurrentPosition == bufLineStartPosition {
					err = fmt.Errorf("readHTTPPayloadLines() unexpectedly found an empty line in Payload")
					return
				}

				lines = append(lines, string(buf[bufLineStartPosition:bufCurrentPosition]))

				bufLineStartPosition = bufCurrentPosition + 1
			}

			bufCurrentPosition++
		}

		if bufLineStartPosition != bufCurrentPosition {
			err = fmt.Errorf("readHTTPPayloadLines() unexpectedly found a non-terminated line in Payload")
			return
		}
	}

	err = nil
	return
}

func readHTTPChunk(tcpConn *net.TCPConn) (chunk []byte, err error) {
	var (
		chunkLen uint64
		line     string
	)

	line, err = readHTTPLineCRLF(tcpConn)
	if nil != err {
		return
	}

	chunkLen, err = strconv.ParseUint(line, 16, 64)
	if nil != err {
		return
	}

	if 0 == chunkLen {
		chunk = make([]byte, 0)
	} else {
		chunk, err = readBytesFromTCPConn(tcpConn, int(chunkLen))
		if nil != err {
			return
		}
	}

	err = readHTTPEmptyLineCRLF(tcpConn)

	return
}

func readHTTPChunkIntoBuf(tcpConn *net.TCPConn, buf []byte) (chunkLen uint64, err error) {
	var (
		line string
	)

	line, err = readHTTPLineCRLF(tcpConn)
	if nil != err {
		return
	}

	chunkLen, err = strconv.ParseUint(line, 16, 64)
	if nil != err {
		return
	}

	if 0 < chunkLen {
		err = readBytesFromTCPConnIntoBuf(tcpConn, buf[:chunkLen])
		if nil != err {
			return
		}
	}

	err = readHTTPEmptyLineCRLF(tcpConn)

	return
}

// mergeHeadersAndList performs a logical merge of headers and lists among successive Account or Container GETs.
//
// Content-Length header values are summed
// Other headers that are single valued and don't change don't change the header value
// Multi-valued headers or headers that change value are appended
func mergeHeadersAndList(masterHeaders map[string][]string, masterList *[]string, toAddHeaders map[string][]string, toAddList *[]string) {
	var (
		addedContentLength             uint64
		err                            error
		ok                             bool
		prevContentLength              uint64
		prevContentLengthAsStringSlice []string
		prevValues                     []string
	)

	for key, values := range toAddHeaders {
		if "Content-Length" == key {
			prevContentLengthAsStringSlice, ok = masterHeaders["Content-Length"]
			if !ok {
				prevContentLengthAsStringSlice = []string{"0"}
			}
			if 1 != len(prevContentLengthAsStringSlice) {
				err = fmt.Errorf("mergeHeadersAndList() passed masterHeaders with unexpected Content-Length header: %v", prevContentLengthAsStringSlice)
				panic(err)
			}
			prevContentLength, err = strconv.ParseUint(prevContentLengthAsStringSlice[0], 10, 64)
			if nil != err {
				err = fmt.Errorf("mergeHeadersAndList() passed masterHeaders with unexpected Content-Length header: %v", prevContentLengthAsStringSlice)
				panic(err)
			}
			if 1 != len(values) {
				err = fmt.Errorf("mergeHeadersAndList() passed toAddHeaders with unexpected Content-Length header: %v", values)
			}
			addedContentLength, err = strconv.ParseUint(values[0], 10, 64)
			if nil != err {
				err = fmt.Errorf("mergeHeadersAndList() passed toAddHeaders with unexpected Content-Length header: %v", values)
				panic(err)
			}
			masterHeaders["Content-Length"] = []string{strconv.FormatUint(prevContentLength+addedContentLength, 10)}
		} else {
			prevValues, ok = masterHeaders[key]
			if ok {
				if (1 != len(prevValues)) || (1 != len(values)) || (prevValues[0] != values[0]) {
					masterHeaders[key] = append(prevValues, values...)
				}
			} else {
				masterHeaders[key] = values
			}
		}
	}

	*masterList = append(*masterList, *toAddList...)
}
