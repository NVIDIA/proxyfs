// Swift Object-specific API access implementation

package swiftclient

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

func objectContentLengthWithRetry(accountName string, containerName string, objectName string) (uint64, error) {
	// request is a function that, through the miracle of closure, calls
	// objectContentLength() with the paramaters passed to this function,
	// stashes the relevant return values into the local variables of this
	// function, and then returns err and whether it is retriable to
	// RequestWithRetry()
	var (
		length uint64
		err    error
	)
	request := func() (bool, error) {
		var err error
		length, err = objectContentLength(accountName, containerName, objectName)
		return true, err
	}

	var (
		retryObj *RetryCtrl = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string     = fmt.Sprintf("swiftclient.objectContentLength(\"%v/%v/%v\")",
			accountName, containerName, objectName)
		statnm RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjContentLengthRetryOps,
			retrySuccessCnt: &stats.SwiftObjContentLengthRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return length, err
}

func objectContentLength(accountName string, containerName string, objectName string) (length uint64, err error) {
	var (
		connection         *connectionStruct
		contentLengthAsInt int
		fsErr              blunder.FsError
		headers            map[string][]string
		httpStatus         int
		isError            bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "HEAD", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "HEAD %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	contentLengthAsInt, err = parseContentLength(headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	length = uint64(contentLengthAsInt)

	stats.IncrementOperations(&stats.SwiftObjContentLengthOps)

	return
}

func objectCopy(srcAccountName string, srcContainerName string, srcObjectName string, dstAccountName string, dstContainerName string, dstObjectName string, chunkedCopyContext ChunkedCopyContext) (err error) {
	var (
		chunk                []byte
		chunkSize            uint64
		dstChunkedPutContext ChunkedPutContext
		srcObjectPosition    = uint64(0)
		srcObjectSize        uint64
	)

	srcObjectSize, err = objectContentLengthWithRetry(srcAccountName, srcContainerName, srcObjectName)
	if nil != err {
		return
	}

	dstChunkedPutContext, err = objectFetchChunkedPutContextWithRetry(dstAccountName, dstContainerName, dstObjectName)
	if nil != err {
		return
	}

	for srcObjectPosition < srcObjectSize {
		chunkSize = chunkedCopyContext.BytesRemaining(srcObjectSize - srcObjectPosition)
		if 0 == chunkSize {
			err = dstChunkedPutContext.Close()
			return
		}

		if (srcObjectPosition + chunkSize) > srcObjectSize {
			chunkSize = srcObjectSize - srcObjectPosition

			chunk, err = objectTailWithRetry(srcAccountName, srcContainerName, srcObjectName, chunkSize)
		} else {
			chunk, err = objectGetWithRetry(srcAccountName, srcContainerName, srcObjectName, srcObjectPosition, chunkSize)
		}

		srcObjectPosition += chunkSize

		err = dstChunkedPutContext.SendChunk(chunk)
		if nil != err {
			return
		}
	}

	err = dstChunkedPutContext.Close()

	stats.IncrementOperations(&stats.SwiftObjCopyOps)

	return
}

func objectDeleteAsync(accountName string, containerName string, objectName string, wgPreCondition *sync.WaitGroup, wgPostSignal *sync.WaitGroup) {
	pendingDelete := &pendingDeleteStruct{
		next:           nil,
		accountName:    accountName,
		containerName:  containerName,
		objectName:     objectName,
		wgPreCondition: wgPreCondition,
		wgPostSignal:   wgPostSignal,
	}

	pendingDeletes := globals.pendingDeletes

	pendingDeletes.Lock()

	if nil == pendingDeletes.tail {
		pendingDeletes.head = pendingDelete
		pendingDeletes.tail = pendingDelete
		pendingDeletes.cond.Signal()
	} else {
		pendingDeletes.tail.next = pendingDelete
		pendingDeletes.tail = pendingDelete
	}

	pendingDeletes.Unlock()
}

func objectDeleteAsyncDaemon() {
	pendingDeletes := globals.pendingDeletes

	pendingDeletes.Lock()

	pendingDeletes.armed = true

	for {
		pendingDeletes.cond.Wait()

		for {
			if pendingDeletes.shutdownInProgress {
				pendingDeletes.Unlock()
				pendingDeletes.shutdownWaitGroup.Done()
				return
			}

			pendingDelete := pendingDeletes.head

			if nil == pendingDelete {
				break // effectively a continue of the outer for loop
			}

			pendingDeletes.head = pendingDelete.next
			if nil == pendingDeletes.head {
				pendingDeletes.tail = nil
			}

			pendingDeletes.Unlock()

			if nil != pendingDelete.wgPreCondition {
				pendingDelete.wgPreCondition.Wait()
			}

			_ = objectDeleteSyncWithRetry(pendingDelete.accountName, pendingDelete.containerName, pendingDelete.objectName)

			if nil != pendingDelete.wgPostSignal {
				// TODO: what if the delete failed?
				pendingDelete.wgPostSignal.Done()
			}

			pendingDeletes.Lock()
		}
	}
}

func objectDeleteSyncWithRetry(accountName string, containerName string, objectName string) (err error) {
	// request is a function that, through the miracle of closure, calls
	// objectDeleteSync() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	request := func() (bool, error) {
		var err error
		err = objectDeleteSync(accountName, containerName, objectName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string      = fmt.Sprintf("swiftclient.objectDeleteSync(\"%v/%v/%v\")", accountName, containerName, objectName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjDeleteRetryOps,
			retrySuccessCnt: &stats.SwiftObjDeleteRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func objectDeleteSync(accountName string, containerName string, objectName string) (err error) {
	var (
		connection *connectionStruct
		fsErr      blunder.FsError
		headers    map[string][]string
		httpStatus int
		isError    bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "DELETE", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.objectDeleteSync(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.objectDeleteSync(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "DELETE %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectDeleteSync(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftObjDeleteOps)

	return
}

func objectGetWithRetry(accountName string, containerName string, objectName string,
	offset uint64, length uint64) ([]byte, error) {

	// request is a function that, through the miracle of closure, calls
	// objectGet() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	var (
		buf []byte
		err error
	)
	request := func() (bool, error) {
		var err error
		buf, err = objectGet(accountName, containerName, objectName, offset, length)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string      = fmt.Sprintf("swiftclient.objectGet(\"%v/%v/%v\")", accountName, containerName, objectName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjGetRetryOps,
			retrySuccessCnt: &stats.SwiftObjGetRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return buf, err
}

func objectGet(accountName string, containerName string, objectName string, offset uint64, length uint64) (buf []byte, err error) {
	var (
		connection    *connectionStruct
		chunk         []byte
		contentLength int
		fsErr         blunder.FsError
		headers       map[string][]string
		httpStatus    int
		isError       bool
	)

	headers = make(map[string][]string)
	headers["Range"] = []string{"bytes=" + strconv.FormatUint(offset, 10) + "-" + strconv.FormatUint((offset+length-1), 10)}

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(connection.tcpConn)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readHTTPChunk() error", accountName, containerName, objectName)
				return
			}

			if 0 == len(chunk) {
				break
			}

			buf = append(buf, chunk...)
		}
	} else {
		contentLength, err = parseContentLength(headers)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPGetError)
			logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjGet, uint64(len(buf)))

	return
}

func objectHeadWithRetry(accountName string, containerName string, objectName string) (map[string][]string, error) {
	// request is a function that, through the miracle of closure, calls
	// objectHead() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	var (
		headers map[string][]string
		err     error
	)
	request := func() (bool, error) {
		var err error
		headers, err = objectHead(accountName, containerName, objectName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string      = fmt.Sprintf("swiftclient.objectHead(\"%v/%v/%v\")", accountName, containerName, objectName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjHeadRetryOps,
			retrySuccessCnt: &stats.SwiftObjHeadRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return headers, err
}

func objectHead(accountName string, containerName string, objectName string) (headers map[string][]string, err error) {
	var (
		connection *connectionStruct
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "HEAD", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "HEAD %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftObjHeadOps)

	return
}

func objectLoadWithRetry(accountName string, containerName string, objectName string) ([]byte, error) {
	// request is a function that, through the miracle of closure, calls
	// objectLoad() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	var (
		buf []byte
		err error
	)
	request := func() (bool, error) {
		var err error
		buf, err = objectLoad(accountName, containerName, objectName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string      = fmt.Sprintf("swiftclient.objectLoad(\"%v/%v/%v\")", accountName, containerName, objectName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjLoadRetryOps,
			retrySuccessCnt: &stats.SwiftObjLoadRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return buf, err
}

func objectLoad(accountName string, containerName string, objectName string) (buf []byte, err error) {
	var (
		connection    *connectionStruct
		chunk         []byte
		contentLength int
		fsErr         blunder.FsError
		headers       map[string][]string
		httpStatus    int
		isError       bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(connection.tcpConn)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readHTTPChunk() error", accountName, containerName, objectName)
				return
			}

			if 0 == len(chunk) {
				break
			}

			buf = append(buf, chunk...)
		}
	} else {
		contentLength, err = parseContentLength(headers)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPGetError)
			logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjLoad, uint64(len(buf)))

	return
}

func objectReadWithRetry(accountName string, containerName string, objectName string, offset uint64, buf []byte) (uint64, error) {
	// request is a function that, through the miracle of closure, calls
	// objectRead() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	var (
		len uint64
		err error
	)
	request := func() (bool, error) {
		var err error
		len, err = objectRead(accountName, containerName, objectName, offset, buf)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string      = fmt.Sprintf("swiftclient.objectRead(\"%v/%v/%v\", offset=0x%016X, len(buf)=0x%016X)", accountName, containerName, objectName, offset, cap(buf))
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjReadRetryOps,
			retrySuccessCnt: &stats.SwiftObjReadRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return len, err
}

func objectRead(accountName string, containerName string, objectName string, offset uint64, buf []byte) (len uint64, err error) {
	var (
		capacity      uint64
		chunkLen      uint64
		chunkPos      uint64
		connection    *connectionStruct
		contentLength int
		fsErr         blunder.FsError
		headers       map[string][]string
		httpStatus    int
		isError       bool
	)

	capacity = uint64(cap(buf))

	headers = make(map[string][]string)
	headers["Range"] = []string{"bytes=" + strconv.FormatUint(offset, 10) + "-" + strconv.FormatUint((offset+capacity-1), 10)}

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		chunkPos = 0
		for {
			chunkLen, err = readHTTPChunkIntoBuf(connection.tcpConn, buf[chunkPos:])
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got readHTTPChunk() error", accountName, containerName, objectName)
				return
			}

			if 0 == chunkLen {
				len = chunkPos
				break
			}
		}
	} else {
		contentLength, err = parseContentLength(headers)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPGetError)
			logger.ErrorfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			len = 0
			err = nil
		} else {
			err = readBytesFromTCPConnIntoBuf(connection.tcpConn, buf)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
			len = capacity
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjRead, len)

	return
}

func objectTailWithRetry(accountName string, containerName string, objectName string,
	length uint64) ([]byte, error) {

	// request is a function that, through the miracle of closure, calls
	// objectTail() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	var (
		buf []byte
		err error
	)
	request := func() (bool, error) {
		var err error
		buf, err = objectTail(accountName, containerName, objectName, length)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string      = fmt.Sprintf("swiftclient.objectTail(\"%v/%v/%v\")", accountName, containerName, objectName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjTailRetryOps,
			retrySuccessCnt: &stats.SwiftObjTailRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return buf, err
}

func objectTail(accountName string, containerName string, objectName string, length uint64) (buf []byte, err error) {
	var (
		chunk         []byte
		connection    *connectionStruct
		contentLength int
		fsErr         blunder.FsError
		headers       map[string][]string
		httpStatus    int
		isError       bool
	)

	headers = make(map[string][]string)
	headers["Range"] = []string{"bytes=-" + strconv.FormatUint(length, 10)}

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(connection.tcpConn)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readHTTPChunk() error", accountName, containerName, objectName)
				return
			}

			if 0 == len(chunk) {
				break
			}

			buf = append(buf, chunk...)
		}
	} else {
		contentLength, err = parseContentLength(headers)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPGetError)
			logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperationsAndBytes(stats.SwiftObjTail, uint64(len(buf)))

	return
}

type chunkedPutContextStruct struct {
	sync.Mutex
	accountName   string
	containerName string
	objectName    string
	active        bool
	err           error
	fatal         bool
	connection    *connectionStruct
	bytesPut      uint64
	bytesPutTree  sortedmap.LLRBTree // Key   == objectOffset of start of chunk in object
	//                                  Value == []byte       of bytes sent to SendChunk()
}

func (chunkedPutContext *chunkedPutContextStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsUint64, ok := key.(uint64)
	if !ok {
		err = fmt.Errorf("swiftclient.chunkedPutContext.DumpKey() could not parse key as a uint64")
		return
	}

	keyAsString = fmt.Sprintf("0x%016X", keyAsUint64)

	err = nil
	return
}

func (chunkedPutContext *chunkedPutContextStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsByteSlice, ok := value.([]byte)
	if !ok {
		err = fmt.Errorf("swiftclient.chunkedPutContext.DumpValue() could not parse value as a []byte")
		return
	}

	valueAsString = string(valueAsByteSlice[:])

	err = nil
	return
}

func objectFetchChunkedPutContextWithRetry(accountName string, containerName string, objectName string) (*chunkedPutContextStruct, error) {
	// request is a function that, through the miracle of closure, calls
	// objectFetchChunkedPutContext() with the paramaters passed to this
	// function, stashes the relevant return values into the local variables of
	// this function, and then returns err and whether it is retriable to
	// RequestWithRetry()
	var (
		chunkedPutContext *chunkedPutContextStruct
	)
	request := func() (bool, error) {
		var err error
		chunkedPutContext, err = objectFetchChunkedPutContext(accountName, containerName, objectName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string      = fmt.Sprintf("swiftclient.objectFetchChunkedPutContext(\"%v/%v/%v\")", accountName, containerName, objectName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjFetchPutCtxtRetryOps,
			retrySuccessCnt: &stats.SwiftObjFetchPutCtxtRetrySuccessOps}
	)
	err := retryObj.RequestWithRetry(request, &opname, &statnm)
	return chunkedPutContext, err
}

// used during testing for error injection
var objectFetchChunkedPutContextCnt uint64

func objectFetchChunkedPutContext(accountName string, containerName string, objectName string) (chunkedPutContext *chunkedPutContextStruct, err error) {
	var (
		connection *connectionStruct
		headers    map[string][]string
	)
	objectFetchChunkedPutContextCnt += 1

	connection = acquireChunkedConnection()

	headers = make(map[string][]string)
	headers["Transfer-Encoding"] = []string{"chunked"}

	// check for chaos error generation (testing only)
	if globals.chaosFetchChunkedPutFailureRate > 0 &&
		objectFetchChunkedPutContextCnt%globals.chaosFetchChunkedPutFailureRate == 0 {
		err = fmt.Errorf("swiftclient.objectFetchChunkedPutContext returning simulated error")
	} else {
		err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "PUT", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, headers)
	}
	if nil != err {
		releaseChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.objectFetchChunkedPutContext(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	chunkedPutContext = &chunkedPutContextStruct{
		accountName:   accountName,
		containerName: containerName,
		objectName:    objectName,
		err:           nil,
		active:        true,
		connection:    connection,
		bytesPut:      0,
	}

	chunkedPutContext.bytesPutTree = sortedmap.NewLLRBTree(sortedmap.CompareUint64, chunkedPutContext)

	stats.IncrementOperations(&stats.SwiftObjPutCtxFetchOps)

	return
}

func (chunkedPutContext *chunkedPutContextStruct) BytesPut() (bytesPut uint64, err error) {
	chunkedPutContext.Lock()
	bytesPut = chunkedPutContext.bytesPut
	chunkedPutContext.Unlock()

	stats.IncrementOperations(&stats.SwiftObjPutCtxBytesPutOps)

	err = nil
	return
}

func (chunkedPutContext *chunkedPutContextStruct) Close() (err error) {

	err = chunkedPutContext.closeHelper()
	if nil == err {
		return
	}

	// fatal errors cannot be retried because we don't have the data that needs
	// to be resent available (it could not be stored)
	if chunkedPutContext.fatal {
		return chunkedPutContext.err
	}

	// There was a problem completing the ObjectPut.  Retry the operation.
	//
	// request is a function that, through the miracle of closure, calls
	// Retry() and Close() with the paramaters passed to this function and
	// stashes the return values into the local variables of this function
	// and then returns the error and whether it is retriable to its caller,
	// RequestWithRetry()
	request := func() (bool, error) {
		var err error

		err = chunkedPutContext.retry()
		if err != nil {
			// closeHelper() will shutdown the TCP connection and
			// clean up, but it needs to know there was an error
			chunkedPutContext.err = err
		}
		err = chunkedPutContext.closeHelper()
		return true, err
	}

	var (
		retryObj *RetryCtrl = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname   string     = fmt.Sprintf("swiftclient.chunkedPutContext.Close(\"%v/%v/%v\")",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		statnm RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftObjPutCtxtCloseRetryOps,
			retrySuccessCnt: &stats.SwiftObjPutCtxtCloseRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func (chunkedPutContext *chunkedPutContextStruct) closeHelper() (err error) {
	var (
		fsErr      blunder.FsError
		headers    map[string][]string
		httpStatus int
		isError    bool
	)

	chunkedPutContext.Lock()

	if !chunkedPutContext.active {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPPutError, "called while inactive")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.closeHelper(\"%v/%v/%v\") called while inactive", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkedPutContext.active = false

	// if an error occurred earlier there's no point in trying to send the closing chunk
	if chunkedPutContext.err != nil {
		if chunkedPutContext.connection != nil {
			releaseChunkedConnection(chunkedPutContext.connection, false)
			chunkedPutContext.connection = nil
		}
		err = chunkedPutContext.err
		chunkedPutContext.Unlock()
		return
	}

	err = writeHTTPPutChunk(chunkedPutContext.connection.tcpConn, []byte{})
	if nil != err {
		releaseChunkedConnection(chunkedPutContext.connection, false)
		chunkedPutContext.connection = nil
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.closeHelper(\"%v/%v/%v\") got writeHTTPPutChunk() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(chunkedPutContext.connection.tcpConn)
	if nil != err {
		releaseChunkedConnection(chunkedPutContext.connection, false)
		chunkedPutContext.connection = nil
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.closeHelper(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseChunkedConnection(chunkedPutContext.connection, false)
		chunkedPutContext.connection = nil
		chunkedPutContext.Unlock()
		err = blunder.NewError(fsErr, "PUT %s/%s/%s returned HTTP StatusCode %d", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.closeHelper(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	releaseChunkedConnection(chunkedPutContext.connection, parseConnection(headers))
	chunkedPutContext.connection = nil

	chunkedPutContext.Unlock()

	stats.IncrementOperations(&stats.SwiftObjPutCtxCloseOps)

	return
}

func (chunkedPutContext *chunkedPutContextStruct) Read(offset uint64, length uint64) (buf []byte, err error) {
	var (
		chunkBufAsByteSlice []byte
		chunkBufAsValue     sortedmap.Value
		chunkOffsetAsKey    sortedmap.Key
		chunkOffsetAsUint64 uint64
		found               bool
		chunkIndex          int
		ok                  bool
		readLimitOffset     uint64
	)

	readLimitOffset = offset + length

	chunkedPutContext.Lock()

	if readLimitOffset > chunkedPutContext.bytesPut {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() called for invalid range")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") called for invalid range", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkIndex, found, err = chunkedPutContext.bytesPutTree.BisectLeft(offset)
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.BisectLeft() failed: %v", err)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got bytesPutTree.BisectLeft() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}
	if !found && (0 > chunkIndex) {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.BisectLeft() returned unexpected index/found")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") attempt to read past end", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkOffsetAsKey, chunkBufAsValue, ok, err = chunkedPutContext.bytesPutTree.GetByIndex(chunkIndex)
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.GetByIndex() failed: %v", err)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got initial bytesPutTree.GetByIndex() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}
	if !ok {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.GetByIndex() returned ok == false")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got initial bytesPutTree.GetByIndex() !ok", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkOffsetAsUint64, ok = chunkOffsetAsKey.(uint64)
	if !ok {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.GetByIndex() returned non-uint64 Key")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got initial bytesPutTree.GetByIndex() malformed Key", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkBufAsByteSlice, ok = chunkBufAsValue.([]byte)
	if !ok {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.GetByIndex() returned non-[]byte Value")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got initial bytesPutTree.GetByIndex() malformed Value", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	if (readLimitOffset - chunkOffsetAsUint64) <= uint64(len(chunkBufAsByteSlice)) {
		// Trivial case: offset:readLimitOffset fits entirely within chunkBufAsByteSlice

		buf = chunkBufAsByteSlice[(offset - chunkOffsetAsUint64):(readLimitOffset - chunkOffsetAsUint64)]
	} else {
		// Complex case: offset:readLimit extends beyond end of chunkBufAsByteSlice

		buf = make([]byte, 0, length)
		buf = append(buf, chunkBufAsByteSlice[(offset-chunkOffsetAsUint64):]...)

		for uint64(len(buf)) < length {
			chunkIndex++

			chunkOffsetAsKey, chunkBufAsValue, ok, err = chunkedPutContext.bytesPutTree.GetByIndex(chunkIndex)
			if nil != err {
				chunkedPutContext.Unlock()
				err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.GetByIndex() failed: %v", err)
				logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got next bytesPutTree.GetByIndex() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
				return
			}
			if !ok {
				chunkedPutContext.Unlock()
				err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.GetByIndex() returned ok == false")
				logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got next bytesPutTree.GetByIndex() !ok", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
				return
			}

			chunkOffsetAsUint64, ok = chunkOffsetAsKey.(uint64)
			if !ok {
				chunkedPutContext.Unlock()
				err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.GetByIndex() returned non-uint64 Key")
				logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got next bytesPutTree.GetByIndex() malformed Key", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
				return
			}

			chunkBufAsByteSlice, ok = chunkBufAsValue.([]byte)
			if !ok {
				chunkedPutContext.Unlock()
				err = blunder.NewError(blunder.BadHTTPGetError, "swiftclient.chunkedPutContext.Read() bytesPutTree.GetByIndex() returned non-[]byte Value")
				logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Read(\"%v/%v/%v\") got next bytesPutTree.GetByIndex() malformed Key", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
				return
			}

			if (readLimitOffset - chunkOffsetAsUint64) < uint64(len(chunkBufAsByteSlice)) {
				buf = append(buf, chunkBufAsByteSlice[:(readLimitOffset-chunkOffsetAsUint64)]...)
			} else {
				buf = append(buf, chunkBufAsByteSlice...)
			}
		}
	}

	chunkedPutContext.Unlock()

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjPutCtxRead, length)

	err = nil
	return
}

func (chunkedPutContext *chunkedPutContextStruct) retry() (err error) {
	var (
		chunkBufAsByteSlice []byte
		chunkBufAsValue     sortedmap.Value
		chunkIndex          int
		headers             map[string][]string
		ok                  bool
	)

	chunkedPutContext.Lock()
	sendChunkRetryCnt += 1

	if chunkedPutContext.active {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPPutError, "called while active")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") called while active", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	// clear error from the previous attempt
	chunkedPutContext.err = nil

	chunkedPutContext.connection = acquireChunkedConnection()

	chunkedPutContext.active = true

	headers = make(map[string][]string)
	headers["Transfer-Encoding"] = []string{"chunked"}

	err = writeHTTPRequestLineAndHeaders(chunkedPutContext.connection.tcpConn, "PUT", "/"+swiftVersion+"/"+chunkedPutContext.accountName+"/"+chunkedPutContext.containerName+"/"+chunkedPutContext.objectName, headers)
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkIndex = 0

	for {
		_, chunkBufAsValue, ok, err = chunkedPutContext.bytesPutTree.GetByIndex(chunkIndex)
		if nil != err {
			chunkedPutContext.Unlock()
			err = blunder.NewError(blunder.BadHTTPPutError, "bytesPutTree.GetByIndex() failed: %v", err)
			logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") got bytesPutTree.GetByIndex() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
			return
		}

		if !ok {
			// We've reached the end of bytesPutTree... so we (presumably) have now sent bytesPut bytes in total
			break
		}

		// Simply (re)send the chunk (assuming it is a []byte)

		chunkBufAsByteSlice, ok = chunkBufAsValue.([]byte)
		if !ok {
			chunkedPutContext.Unlock()
			err = blunder.NewError(blunder.BadHTTPPutError, "bytesPutTree.GetByIndex() returned non-[]byte Value")
			logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") got bytesPutTree.GetByIndex(() malformed Value", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
			return
		}

		// check for chaos error generation (testing only)
		if globals.chaosSendChunkFailureRate > 0 &&
			sendChunkRetryCnt%globals.chaosSendChunkFailureRate == 0 {
			err = fmt.Errorf("writeHTTPPutChunk() simulated error")
		} else {
			err = writeHTTPPutChunk(chunkedPutContext.connection.tcpConn, chunkBufAsByteSlice)
		}
		if nil != err {
			chunkedPutContext.Unlock()
			err = blunder.NewError(blunder.BadHTTPPutError, "writeHTTPPutChunk() failed: %v", err)
			logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") got writeHTTPPutChunk() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
			return
		}

		// See if there is (was) another chunk

		chunkIndex++
	}

	chunkedPutContext.Unlock()

	stats.IncrementOperations(&stats.SwiftObjPutCtxRetryOps)

	return
}

// used during testing for error injection
var (
	sendChunkCnt      uint64
	sendChunkRetryCnt uint64
)

// SendChunk() tries to send the chunk of data to the Swift server and deal with
// any errors that occur.  There is a retry mechanism in Close() that will
// attempt to resend the data if the first attempt here was not successful.
//
// For "normal" error cases SendChunk() will returns nil (success) instead of
// the error and stash the data to be sent away so that retry can attempt to
// send it.  Whence Close() is called, it notices the pending error in
// ChunkedPutContext.err and retries the entire operation.  If the retry works,
// it returns success.  Otherwise, it returns the final error it encountered.

// There are some cases where SendChunk() cannot stash away the data to be sent
// due to logic errors in the program or corruption of in memory data
// structures.  These should probably just be dealt with by panic'ing, but
// instead ChunkedPutContext treats this as a "fatal" error, which it returns to
// the caller of SendChunk().  If Close() is called later, it does not attempt
// to retry but instead returns the same error.  Just in case Close() is not
// called (current code does not call Close() after SendChunk() returns an
// error, SendChunk() also cleans up the TCP connection.
//
func (chunkedPutContext *chunkedPutContextStruct) SendChunk(buf []byte) (err error) {

	chunkedPutContext.Lock()
	sendChunkCnt += 1

	if !chunkedPutContext.active {
		err = blunder.NewError(blunder.BadHTTPPutError, "called while inactive")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") logic error",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		chunkedPutContext.err = err
		chunkedPutContext.fatal = true

		// connection should already be nil
		chunkedPutContext.connection = nil
		chunkedPutContext.Unlock()
		return
	}

	if 0 == len(buf) {
		err = blunder.NewError(blunder.BadHTTPPutError, "called with zero-length buf")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") logic error",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		chunkedPutContext.err = err
		chunkedPutContext.fatal = true
		releaseChunkedConnection(chunkedPutContext.connection, false)
		chunkedPutContext.connection = nil
		chunkedPutContext.Unlock()
		return
	}

	ok, err := chunkedPutContext.bytesPutTree.Put(chunkedPutContext.bytesPut, buf)
	if nil != err {
		err = blunder.NewError(blunder.BadHTTPPutError, "attempt to append chunk to LLRB Tree failed: %v", err)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") got bytesPutTree.Put() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		chunkedPutContext.err = err
		chunkedPutContext.fatal = true
		releaseChunkedConnection(chunkedPutContext.connection, false)
		chunkedPutContext.connection = nil
		chunkedPutContext.Unlock()
		return
	}
	if !ok {
		err = blunder.NewError(blunder.BadHTTPPutError, "attempt to append chunk to LLRB Tree returned ok == false")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") got bytesPutTree.Put() !ok", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		chunkedPutContext.err = err
		chunkedPutContext.fatal = true
		releaseChunkedConnection(chunkedPutContext.connection, false)
		chunkedPutContext.connection = nil
		chunkedPutContext.Unlock()
		return
	}

	chunkedPutContext.bytesPut += uint64(len(buf))

	// The prior errors are logical/programmatic errors that cannot be fixed
	// by a retry so let them return with the error and let the caller abort
	// (Close() not called, so retry will not be tried).
	//
	// However, if writeHTTPPutChunk() fails that is probably due to a
	// problem with the TCP connection or storage which may be cured by a
	// retry.  Therefore, if the call fails stash the error in
	// chunkedPutContext and return success to the caller so it will feed us
	// the rest of the chunks.  We need those chunks for the retry!
	//
	// If an error has already been seen, stash the data for use by retry
	// and return success.
	if chunkedPutContext.err != nil {
		chunkedPutContext.Unlock()
		return nil
	}

	// check for chaos error generation (testing only)
	if globals.chaosSendChunkFailureRate > 0 &&
		sendChunkCnt%globals.chaosSendChunkFailureRate == 0 {
		err = fmt.Errorf("writeHTTPPutChunk() simulated error")
	} else {
		err = writeHTTPPutChunk(chunkedPutContext.connection.tcpConn, buf)
	}
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") got writeHTTPPutChunk() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		chunkedPutContext.err = err
		chunkedPutContext.Unlock()
		err = nil
		return
	}

	chunkedPutContext.Unlock()

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjPutCtxSendChunk, uint64(len(buf)))

	return
}
