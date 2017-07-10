// Swift Object-specific API access implementation

package swiftclient

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

func objectContentLength(accountName string, containerName string, objectName string) (length uint64, err error) {
	var (
		contentLengthAsInt int
		fsErr              blunder.FsError
		headers            map[string][]string
		httpStatus         int
		isError            bool
		tcpConn            *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName, objectName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "HEAD", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "HEAD %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	contentLengthAsInt, err = parseContentLength(headers)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

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

	srcObjectSize, err = objectContentLength(srcAccountName, srcContainerName, srcObjectName)
	if nil != err {
		return
	}

	dstChunkedPutContext, err = objectFetchChunkedPutContext(dstAccountName, dstContainerName, dstObjectName)
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

			chunk, _, err = objectTail(srcAccountName, srcContainerName, srcObjectName, chunkSize)
		} else {
			chunk, err = objectGet(srcAccountName, srcContainerName, srcObjectName, srcObjectPosition, chunkSize)
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

			_ = objectDeleteSync(pendingDelete.accountName, pendingDelete.containerName, pendingDelete.objectName)

			if nil != pendingDelete.wgPostSignal {
				pendingDelete.wgPostSignal.Done()
			}

			pendingDeletes.Lock()
		}
	}
}

func objectDeleteSync(accountName string, containerName string, objectName string) (err error) {
	var (
		fsErr      blunder.FsError
		headers    map[string][]string
		httpStatus int
		isError    bool
		tcpConn    *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.objectDeleteSync(\"%v/%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName, objectName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "DELETE", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.objectDeleteSync(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.objectDeleteSync(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "DELETE %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectDeleteSync(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftObjDeleteOps)

	return
}

func objectGetWithRetry_loop(accountName string, containerName string, objectName string, offset uint64, length uint64) (buf []byte, err error) {

	fmt.Printf("objectGetWithRetry_loop entered\n")

	var retryobj RetryCtrl = New(5, 1*time.Second)
	var lasterr error
	for {
		fmt.Printf("objectGetWithRetry_loop calling objectGet\n")
		buf, lasterr = objectGet(accountName, containerName, objectName, offset, length)
		if lasterr == nil {
			fmt.Printf("objectGetWithRetry_loop returned without error\n")
			return buf, lasterr
		}

		fmt.Printf("objectGetWithRetry_loop calling retryobj.RetryWait()\n")
		err = retryobj.RetryWait()
		if err != nil {
			// should lasterr be returned annotated with the timeout?
			return buf, err
		}
	}
}

func objectGetWithRetry_closure1(accountName string, containerName string, objectName string, offset uint64, length uint64) (buf []byte, err error) {

	// request is a function that, through the miracle of closure, calls
	// objectGet with the paramaters passed to this function and packages
	// the return value(s) into struct retval, returns that, the error, and
	// whether the error is retriable to RequestWithRetry().  the return
	// value(s) are retrieved from struct retval.
	type retval struct {
		buf []byte
	}
	request := func() (interface{}, bool, error) {
		var rval retval
		var err error
		rval.buf, err = objectGet(accountName, containerName, objectName, offset, length)
		return rval, true, err
	}

	var retryObj RetryCtrl = New(5, 1*time.Second)
	var rval retval
	var rv interface{}

	rv, err = retryObj.RequestWithRetry(request)
	rval = rv.(retval)
	return rval.buf, err
}

func objectGetWithRetry_closure2(accountName string, containerName string, objectName string, offset uint64, length uint64) (buf []byte, err error) {

	// request is a function that, through the miracle of closure, calls
	// objectGet with the paramaters passed to this function and stashes the
	// return values into the local variables of this function (buf, err)
	// and then returns any error and whether is retriable to the
	// RequestWithRetry()
	request := func() (interface{}, bool, error) {
		buf, err = objectGet(accountName, containerName, objectName, offset, length)
		return nil, true, err
	}

	var retryObj RetryCtrl = New(5, 1*time.Second)

	_, err = retryObj.RequestWithRetry(request)
	return buf, err
}

func objectGet(accountName string, containerName string, objectName string, offset uint64, length uint64) (buf []byte, err error) {
	var (
		chunk         []byte
		contentLength int
		fsErr         blunder.FsError
		headers       map[string][]string
		httpStatus    int
		isError       bool
		tcpConn       *net.TCPConn
	)

	headers = make(map[string][]string)
	headers["Range"] = []string{"bytes=" + strconv.FormatUint(offset, 10) + "-" + strconv.FormatUint((offset+length-1), 10)}

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName, objectName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, headers)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(tcpConn)
			if nil != err {
				releaseNonChunkedConnection(tcpConn, false)
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
			releaseNonChunkedConnection(tcpConn, false)
			err = blunder.AddError(err, blunder.BadHTTPGetError)
			logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(tcpConn, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjGet, uint64(contentLength))

	return
}

func objectHead(accountName string, containerName string, objectName string) (headers map[string][]string, err error) {
	var (
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
		tcpConn    *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName, objectName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "HEAD", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "HEAD %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftObjHeadOps)

	return
}

func objectLoad(accountName string, containerName string, objectName string) (buf []byte, err error) {
	var (
		chunk         []byte
		contentLength int
		fsErr         blunder.FsError
		headers       map[string][]string
		httpStatus    int
		isError       bool
		tcpConn       *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName, objectName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(tcpConn)
			if nil != err {
				releaseNonChunkedConnection(tcpConn, false)
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
			releaseNonChunkedConnection(tcpConn, false)
			err = blunder.AddError(err, blunder.BadHTTPGetError)
			logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(tcpConn, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjLoad, uint64(contentLength))

	return
}

func objectTail(accountName string, containerName string, objectName string, length uint64) (buf []byte, objectLength int64, err error) {
	var (
		chunk         []byte
		contentLength int
		fsErr         blunder.FsError
		headers       map[string][]string
		httpStatus    int
		isError       bool
		tcpConn       *net.TCPConn
	)

	headers = make(map[string][]string)
	headers["Range"] = []string{"bytes=-" + strconv.FormatUint(length, 10)}

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName, objectName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, headers)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d", accountName, containerName, objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	_, _, objectLength, err = parseContentRange(headers)
	if err != nil {
		releaseNonChunkedConnection(tcpConn, false)
		err = fmt.Errorf("GET %s/%s/%s had bad or missing Content-Range header (%s)", accountName, containerName, objectName, err.Error())
		logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got parseContentRange() error", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(tcpConn)
			if nil != err {
				releaseNonChunkedConnection(tcpConn, false)
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
			releaseNonChunkedConnection(tcpConn, false)
			err = blunder.AddError(err, blunder.BadHTTPGetError)
			logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(tcpConn, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.ErrorfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperationsAndBytes(stats.SwiftObjTail, uint64(len(buf)))

	return
}

type chunkedPutContextStruct struct {
	sync.Mutex
	accountName   string
	containerName string
	objectName    string
	active        bool
	tcpConn       *net.TCPConn
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

func objectFetchChunkedPutContext(accountName string, containerName string, objectName string) (chunkedPutContext *chunkedPutContextStruct, err error) {
	var (
		headers map[string][]string
		tcpConn *net.TCPConn
	)

	tcpConn, err = acquireChunkedConnection()
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.objectFetchChunkedPutContext(\"%v/%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName, objectName)
		return
	}

	headers = make(map[string][]string)
	headers["Transfer-Encoding"] = []string{"chunked"}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "PUT", "/"+swiftVersion+"/"+accountName+"/"+containerName+"/"+objectName, headers)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.objectFetchChunkedPutContext(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	chunkedPutContext = &chunkedPutContextStruct{
		accountName:   accountName,
		containerName: containerName,
		objectName:    objectName,
		active:        true,
		tcpConn:       tcpConn,
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
	var (
		tryIndex uint16
	)

	err = chunkedPutContext.closeHelper()
	if nil == err {
		return
	}

	for tryIndex = 0; tryIndex < globals.retryLimit; tryIndex++ {
		time.Sleep(globals.retryDelay)

		err = chunkedPutContext.Retry()
		if nil != err {
			return
		}

		err = chunkedPutContext.closeHelper()
		if nil == err {
			return
		}
	}

	return // err == last return from closeHelper()
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
		err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.Close() called while inactive")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Close(\"%v/%v/%v\") called while inactive", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkedPutContext.active = false

	err = writeHTTPPutChunk(chunkedPutContext.tcpConn, []byte{})
	if nil != err {
		releaseChunkedConnection(chunkedPutContext.tcpConn, false)
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Close(\"%v/%v/%v\") got writeHTTPPutChunk() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(chunkedPutContext.tcpConn)
	if nil != err {
		releaseChunkedConnection(chunkedPutContext.tcpConn, false)
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Close(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseChunkedConnection(chunkedPutContext.tcpConn, false)
		chunkedPutContext.Unlock()
		err = blunder.NewError(fsErr, "PUT %s/%s/%s returned HTTP StatusCode %d", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Close(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	releaseChunkedConnection(chunkedPutContext.tcpConn, parseConnection(headers))

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

func (chunkedPutContext *chunkedPutContextStruct) Retry() (err error) {
	var (
		chunkBufAsByteSlice []byte
		chunkBufAsValue     sortedmap.Value
		chunkIndex          int
		headers             map[string][]string
		ok                  bool
	)

	chunkedPutContext.Lock()

	if chunkedPutContext.active {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.Retry() called while active")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Retry(\"%v/%v/%v\") called while active", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkedPutContext.tcpConn, err = acquireChunkedConnection()
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Retry(\"%v/%v/%v\") got acquireNonChunkedConnection() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkedPutContext.active = true

	headers = make(map[string][]string)
	headers["Transfer-Encoding"] = []string{"chunked"}

	err = writeHTTPRequestLineAndHeaders(chunkedPutContext.tcpConn, "PUT", "/"+swiftVersion+"/"+chunkedPutContext.accountName+"/"+chunkedPutContext.containerName+"/"+chunkedPutContext.objectName, headers)
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Retry(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkIndex = 0

	for {
		_, chunkBufAsValue, ok, err = chunkedPutContext.bytesPutTree.GetByIndex(chunkIndex)
		if nil != err {
			chunkedPutContext.Unlock()
			err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.Retry() bytesPutTree.GetByIndex() failed: %v", err)
			logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Retry(\"%v/%v/%v\") got bytesPutTree.GetByIndex() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
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
			err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.Retry() bytesPutTree.GetByIndex() returned non-[]byte Value")
			logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Retry(\"%v/%v/%v\") got bytesPutTree.GetByIndex(() malformed Value", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
			return
		}

		err = writeHTTPPutChunk(chunkedPutContext.tcpConn, chunkBufAsByteSlice)
		if nil != err {
			chunkedPutContext.Unlock()
			err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.Retry() writeHTTPPutChunk() failed: %v", err)
			logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.Retry(\"%v/%v/%v\") got writeHTTPPutChunk() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
			return
		}

		// See if there is (was) another chunk

		chunkIndex++
	}

	chunkedPutContext.Unlock()

	stats.IncrementOperations(&stats.SwiftObjPutCtxRetryOps)

	return
}

func (chunkedPutContext *chunkedPutContextStruct) SendChunk(buf []byte) (err error) {
	chunkedPutContext.Lock()

	if !chunkedPutContext.active {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.SendChunk() called while inactive")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") called while active", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	if 0 == len(buf) {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.SendChunk() called with zero-length buf")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") called with zero-length buf", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	ok, err := chunkedPutContext.bytesPutTree.Put(chunkedPutContext.bytesPut, buf)
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.SendChunk()'s attempt to append chunk to LLRB Tree failed: %v", err)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") got bytesPutTree.Put() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}
	if !ok {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPPutError, "swiftclient.chunkedPutContext.SendChunk()'s attempt to append chunk to LLRB Tree returned ok == false")
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") got bytesPutTree.Put() !ok", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkedPutContext.bytesPut += uint64(len(buf))

	err = writeHTTPPutChunk(chunkedPutContext.tcpConn, buf)
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") got writeHTTPPutChunk() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkedPutContext.Unlock()

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjPutCtxSendChunk, uint64(len(buf)))

	return
}
