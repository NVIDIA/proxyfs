// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

// Swift Object-specific API access implementation

package swiftclient

import (
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/creachadair/cityhash"
	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/blunder"
	"github.com/NVIDIA/proxyfs/evtlog"
	"github.com/NVIDIA/proxyfs/logger"
	"github.com/NVIDIA/proxyfs/stats"
	"github.com/NVIDIA/proxyfs/trackedlock"
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
		retryObj *RetryCtrl = NewRetryCtrl(
			globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname string = fmt.Sprintf("swiftclient.objectContentLength(\"%v/%v/%v\")",
			accountName, containerName, objectName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjContentLengthRetryOps,
			retrySuccessCnt:   &stats.SwiftObjContentLengthRetrySuccessOps,
			clientRequestTime: &globals.ObjectContentLengthUsec,
			clientFailureCnt:  &globals.ObjectContentLengthFailure,
			swiftRequestTime:  &globals.SwiftObjectContentLengthUsec,
			swiftRetryOps:     &globals.SwiftObjectContentLengthRetryOps,
		}
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
		httpPayload        string
		httpStatus         int
		isError            bool
	)

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "HEAD", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	evtlog.Record(evtlog.FormatObjectHead, accountName, containerName, objectName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(connection.tcpConn, headers)
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "HEAD %s/%s/%s returned HTTP StatusCode %d Payload %s", accountName, containerName, objectName, httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	contentLengthAsInt, err = parseContentLength(headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.objectContentLength(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
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
		clientReqTime        time.Time
	)

	clientReqTime = time.Now()
	defer func() {
		elapsedUsec := time.Since(clientReqTime).Nanoseconds() / time.Microsecond.Nanoseconds()
		globals.ObjectCopyUsec.Add(uint64(elapsedUsec))
	}()

	srcObjectSize, err = objectContentLengthWithRetry(srcAccountName, srcContainerName, srcObjectName)
	if nil != err {
		return
	}

	dstChunkedPutContext, err = objectFetchChunkedPutContextWithRetry(dstAccountName, dstContainerName, dstObjectName, "")
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

func objectDelete(accountName string, containerName string, objectName string, operationOptions OperationOptions) (err error) {
	if (operationOptions & SkipRetry) == SkipRetry {
		err = objectDeleteOneTime(accountName, containerName, objectName)
	} else {
		// request is a function that, through the miracle of closure, calls
		// objectDelete() with the paramaters passed to this function, stashes
		// the relevant return values into the local variables of this function,
		// and then returns err and whether it is retriable to RequestWithRetry()
		request := func() (bool, error) {
			var err error
			err = objectDeleteOneTime(accountName, containerName, objectName)
			return true, err
		}

		var (
			retryObj *RetryCtrl = NewRetryCtrl(
				globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
			opname string = fmt.Sprintf(
				"swiftclient.objectDeleteOneTime(\"%v/%v/%v\")", accountName, containerName, objectName)

			statnm requestStatistics = requestStatistics{
				retryCnt:          &stats.SwiftObjDeleteRetryOps,
				retrySuccessCnt:   &stats.SwiftObjDeleteRetrySuccessOps,
				clientRequestTime: &globals.ObjectDeleteUsec,
				clientFailureCnt:  &globals.ObjectDeleteFailure,
				swiftRequestTime:  &globals.SwiftObjectDeleteUsec,
				swiftRetryOps:     &globals.SwiftObjectDeleteRetryOps,
			}
		)
		err = retryObj.RequestWithRetry(request, &opname, &statnm)
	}

	return
}

func objectDeleteOneTime(accountName string, containerName string, objectName string) (err error) {
	var (
		connection  *connectionStruct
		fsErr       blunder.FsError
		headers     map[string][]string
		httpPayload string
		httpStatus  int
		isError     bool
	)

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "DELETE", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.WarnfWithError(err, "swiftclient.objectDelete(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.WarnfWithError(err, "swiftclient.objectDelete(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	evtlog.Record(evtlog.FormatObjectDelete, accountName, containerName, objectName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(connection.tcpConn, headers)
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "DELETE %s/%s/%s returned HTTP StatusCode %d Payload %s", accountName, containerName, objectName, httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.objectDelete(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
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
		retryObj *RetryCtrl = NewRetryCtrl(
			globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname string = fmt.Sprintf(
			"swiftclient.objectGet(\"%v/%v/%v\")", accountName, containerName, objectName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjGetRetryOps,
			retrySuccessCnt:   &stats.SwiftObjGetRetrySuccessOps,
			clientRequestTime: &globals.ObjectGetUsec,
			clientFailureCnt:  &globals.ObjectGetFailure,
			swiftRequestTime:  &globals.SwiftObjectGetUsec,
			swiftRetryOps:     &globals.SwiftObjectGetRetryOps,
		}
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
		httpPayload   string
		httpStatus    int
		isError       bool
	)

	headers = make(map[string][]string)
	headers["Range"] = []string{"bytes=" + strconv.FormatUint(offset, 10) + "-" + strconv.FormatUint((offset+length-1), 10)}

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	evtlog.Record(evtlog.FormatObjectGet, accountName, containerName, objectName, offset, length, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(connection.tcpConn, headers)
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d Payload %s", accountName, containerName, objectName, httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(connection.tcpConn)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.WarnfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readHTTPChunk() error", accountName, containerName, objectName)
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
			logger.WarnfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.WarnfWithError(err, "swiftclient.objectGet(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjGet, uint64(len(buf)))
	globals.ObjectGetBytes.Add(uint64(len(buf)))
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
		retryObj *RetryCtrl = NewRetryCtrl(
			globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname string = fmt.Sprintf(
			"swiftclient.objectHead(\"%v/%v/%v\")", accountName, containerName, objectName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjHeadRetryOps,
			retrySuccessCnt:   &stats.SwiftObjHeadRetrySuccessOps,
			clientRequestTime: &globals.ObjectHeadUsec,
			clientFailureCnt:  &globals.ObjectHeadFailure,
			swiftRequestTime:  &globals.SwiftObjectHeadUsec,
			swiftRetryOps:     &globals.SwiftObjectHeadRetryOps,
		}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return headers, err
}

func objectHead(accountName string, containerName string, objectName string) (headers map[string][]string, err error) {
	var (
		connection  *connectionStruct
		fsErr       blunder.FsError
		httpPayload string
		httpStatus  int
		isError     bool
	)

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "HEAD", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	evtlog.Record(evtlog.FormatObjectHead, accountName, containerName, objectName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(connection.tcpConn, headers)
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "HEAD %s/%s/%s returned HTTP StatusCode %d Payload %s", accountName, containerName, objectName, httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.objectHead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
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
		retryObj *RetryCtrl = NewRetryCtrl(
			globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname string = fmt.Sprintf(
			"swiftclient.objectLoad(\"%v/%v/%v\")", accountName, containerName, objectName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjLoadRetryOps,
			retrySuccessCnt:   &stats.SwiftObjLoadRetrySuccessOps,
			clientRequestTime: &globals.ObjectLoadUsec,
			clientFailureCnt:  &globals.ObjectLoadFailure,
			swiftRequestTime:  &globals.SwiftObjectLoadUsec,
			swiftRetryOps:     &globals.SwiftObjectLoadRetryOps,
		}
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
		httpPayload   string
		httpStatus    int
		isError       bool
	)

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	evtlog.Record(evtlog.FormatObjectLoad, accountName, containerName, objectName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(connection.tcpConn, headers)
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d Payload %s", accountName, containerName, objectName, httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(connection.tcpConn)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.WarnfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readHTTPChunk() error", accountName, containerName, objectName)
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
			logger.WarnfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.WarnfWithError(err, "swiftclient.objectLoad(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjLoad, uint64(len(buf)))
	globals.ObjectLoadBytes.Add(uint64(len(buf)))
	return
}

func objectPostWithRetry(accountName string, containerName string, objectName string, requestHeaders map[string][]string) (err error) {
	// request is a function that, through the miracle of closure, calls
	// containerPost() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	request := func() (bool, error) {
		var err error
		err = objectPost(accountName, containerName, objectName, requestHeaders)
		return true, err
	}

	var (
		retryObj *RetryCtrl = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string     = fmt.Sprintf("swiftclient.objectPost(\"%v/%v/%v\")", accountName, containerName, objectName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjPostRetryOps,
			retrySuccessCnt:   &stats.SwiftObjPostRetrySuccessOps,
			clientRequestTime: &globals.ObjectPostUsec,
			clientFailureCnt:  &globals.ObjectPostFailure,
			swiftRequestTime:  &globals.SwiftObjectPostUsec,
			swiftRetryOps:     &globals.SwiftObjectPostRetryOps,
		}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func objectPost(accountName string, containerName string, objectName string, requestHeaders map[string][]string) (err error) {
	var (
		connection      *connectionStruct
		contentLength   int
		fsErr           blunder.FsError
		httpPayload     string
		httpStatus      int
		isError         bool
		responseHeaders map[string][]string
	)

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "POST", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.objectPost(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.objectPost(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	evtlog.Record(evtlog.FormatContainerPost, accountName, containerName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(connection.tcpConn, responseHeaders)
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "POST %s/%s/%s returned HTTP StatusCode %d Payload %s", accountName, containerName, objectName, httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.objectPost(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.objectPost(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.WarnfWithError(err, "swiftclient.objectPost(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
			return
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftObjPostOps)

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
		retryObj *RetryCtrl = NewRetryCtrl(
			globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname string = fmt.Sprintf(
			"swiftclient.objectRead(\"%v/%v/%v\", offset=0x%016X, len(buf)=0x%016X)", accountName, containerName, objectName, offset, cap(buf))

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjReadRetryOps,
			retrySuccessCnt:   &stats.SwiftObjReadRetrySuccessOps,
			clientRequestTime: &globals.ObjectReadUsec,
			clientFailureCnt:  &globals.ObjectReadFailure,
			swiftRequestTime:  &globals.SwiftObjectReadUsec,
			swiftRetryOps:     &globals.SwiftObjectReadRetryOps,
		}
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
		httpPayload   string
		httpStatus    int
		isError       bool
	)

	capacity = uint64(cap(buf))

	headers = make(map[string][]string)
	headers["Range"] = []string{"bytes=" + strconv.FormatUint(offset, 10) + "-" + strconv.FormatUint((offset+capacity-1), 10)}

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	evtlog.Record(evtlog.FormatObjectRead, accountName, containerName, objectName, offset, capacity, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(connection.tcpConn, headers)
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d Payload %s", accountName, containerName, objectName, httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		chunkPos = 0
		for {
			chunkLen, err = readHTTPChunkIntoBuf(connection.tcpConn, buf[chunkPos:])
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.WarnfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got readHTTPChunk() error", accountName, containerName, objectName)
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
			logger.WarnfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
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
				logger.WarnfWithError(err, "swiftclient.objectRead(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
			len = capacity
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjRead, len)
	globals.ObjectReadBytes.Add(len)
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
		retryObj *RetryCtrl = NewRetryCtrl(
			globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname string = fmt.Sprintf(
			"swiftclient.objectTail(\"%v/%v/%v\")", accountName, containerName, objectName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjTailRetryOps,
			retrySuccessCnt:   &stats.SwiftObjTailRetrySuccessOps,
			clientRequestTime: &globals.ObjectTailUsec,
			clientFailureCnt:  &globals.ObjectTailFailure,
			swiftRequestTime:  &globals.SwiftObjectTailUsec,
			swiftRetryOps:     &globals.SwiftObjectTailRetryOps,
		}
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
		httpPayload   string
		httpStatus    int
		isError       bool
	)

	headers = make(map[string][]string)
	headers["Range"] = []string{"bytes=-" + strconv.FormatUint(length, 10)}

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, objectName)
		return
	}
	evtlog.Record(evtlog.FormatObjectTail, accountName, containerName, objectName, length, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(connection.tcpConn, headers)
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s/%s/%s returned HTTP StatusCode %d Payload %s", accountName, containerName, objectName, httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, objectName)
		return
	}

	if parseTransferEncoding(headers) {
		buf = make([]byte, 0)
		for {
			chunk, err = readHTTPChunk(connection.tcpConn)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.WarnfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readHTTPChunk() error", accountName, containerName, objectName)
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
			logger.WarnfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got parseContentLength() error", accountName, containerName, objectName)
			return
		}

		if 0 == contentLength {
			buf = make([]byte, 0)
		} else {
			buf, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
			if nil != err {
				releaseNonChunkedConnection(connection, false)
				err = blunder.AddError(err, blunder.BadHTTPGetError)
				logger.WarnfWithError(err, "swiftclient.objectTail(\"%v/%v/%v\") got readBytesFromTCPConn() error", accountName, containerName, objectName)
				return
			}
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperationsAndBytes(stats.SwiftObjTail, uint64(len(buf)))
	globals.ObjectTailBytes.Add(uint64(len(buf)))
	return
}

// Checksum and related info for one chunked put chunk
type chunkedPutChunkInfo struct {
	chunkBuf   []byte
	chunkCksum uint64
}

type chunkedPutContextStruct struct {
	trackedlock.Mutex
	accountName             string
	containerName           string
	objectName              string
	fetchTime               time.Time
	sendTime                time.Time
	active                  bool
	err                     error
	fatal                   bool
	useReserveForVolumeName string
	connection              *connectionStruct
	stillOpen               bool
	bytesPut                uint64
	bytesPutTree            sortedmap.LLRBTree // Key   == objectOffset of start of chunk in object
	//                                  Value == []byte       of bytes sent to SendChunk()
	chunkInfoArray []chunkedPutChunkInfo
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

func objectFetchChunkedPutContextWithRetry(accountName string, containerName string, objectName string, useReserveForVolumeName string) (*chunkedPutContextStruct, error) {
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
		chunkedPutContext, err = objectFetchChunkedPutContext(accountName, containerName, objectName, useReserveForVolumeName)
		return true, err
	}

	var (
		retryObj *RetryCtrl = NewRetryCtrl(
			globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
		opname string = fmt.Sprintf(
			"swiftclient.objectFetchChunkedPutContext(\"%v/%v/%v\")", accountName, containerName, objectName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjFetchPutCtxtRetryOps,
			retrySuccessCnt:   &stats.SwiftObjFetchPutCtxtRetrySuccessOps,
			clientRequestTime: &globals.ObjectPutCtxtFetchUsec,
			clientFailureCnt:  &globals.ObjectPutCtxtFetchFailure,
			swiftRequestTime:  &globals.SwiftObjectPutCtxtFetchUsec,
			swiftRetryOps:     &globals.SwiftObjectPutCtxtFetchRetryOps,
		}
	)
	err := retryObj.RequestWithRetry(request, &opname, &statnm)
	return chunkedPutContext, err
}

// used during testing for error injection
var objectFetchChunkedPutContextCnt uint64

func objectFetchChunkedPutContext(accountName string, containerName string, objectName string, useReserveForVolumeName string) (chunkedPutContext *chunkedPutContextStruct, err error) {
	var (
		connection *connectionStruct
		headers    map[string][]string
	)

	evtlog.Record(evtlog.FormatObjectPutChunkedStart, accountName, containerName, objectName)

	connection, err = acquireChunkedConnection(useReserveForVolumeName)
	if err != nil {
		// acquireChunkedConnection()/openConnection() logged a warning
		return
	}

	headers = make(map[string][]string)
	headers["Transfer-Encoding"] = []string{"chunked"}

	// check for chaos error generation (testing only)
	if atomic.LoadUint64(&globals.chaosFetchChunkedPutFailureRate) > 0 &&
		(atomic.AddUint64(&objectFetchChunkedPutContextCnt, 1)%
			atomic.LoadUint64(&globals.chaosFetchChunkedPutFailureRate) == 0) {
		err = fmt.Errorf("swiftclient.objectFetchChunkedPutContext returning simulated error")
	} else {
		err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "PUT", "/"+swiftVersion+"/"+pathEscape(accountName, containerName, objectName), headers)
	}
	if nil != err {
		releaseChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.objectFetchChunkedPutContext(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, objectName)
		return
	}

	chunkedPutContext = &chunkedPutContextStruct{
		accountName:             accountName,
		containerName:           containerName,
		objectName:              objectName,
		err:                     nil,
		active:                  true,
		useReserveForVolumeName: useReserveForVolumeName,
		connection:              connection,
		bytesPut:                0,
	}

	chunkedPutContext.bytesPutTree = sortedmap.NewLLRBTree(sortedmap.CompareUint64, chunkedPutContext)
	chunkedPutContext.chunkInfoArray = make([]chunkedPutChunkInfo, 0)

	stats.IncrementOperations(&stats.SwiftObjPutCtxFetchOps)

	// record the time that ObjectFetchChunkedPutContext() returns
	chunkedPutContext.fetchTime = time.Now()
	return
}

func (chunkedPutContext *chunkedPutContextStruct) Active() (active bool) {
	active = chunkedPutContext.active

	stats.IncrementOperations(&stats.SwiftObjPutCtxActiveOps)

	return
}

func (chunkedPutContext *chunkedPutContextStruct) BytesPut() (bytesPut uint64, err error) {
	chunkedPutContext.Lock()
	bytesPut = chunkedPutContext.bytesPut
	chunkedPutContext.Unlock()

	stats.IncrementOperations(&stats.SwiftObjPutCtxBytesPutOps)
	globals.ObjectPutCtxtBytesPut.Add(1)

	err = nil
	return
}

func (chunkedPutContext *chunkedPutContextStruct) Close() (err error) {

	// track the amount of time the client spent holding the connection --
	// this includes time spent in SendChunk(), but not time spent in
	// ObjectFetchChunkedPutContext() and not time that will be spent here
	// in Close()
	fetchToCloseUsec := time.Since(chunkedPutContext.fetchTime).Nanoseconds() /
		time.Microsecond.Nanoseconds()
	globals.ObjectPutCtxtFetchToCloseUsec.Add(uint64(fetchToCloseUsec))

	chunkedPutContext.validateChunkChecksums()

	// request is a function that, through the miracle of closure, calls
	// retry() and Close() with the paramaters passed to this function and
	// stashes the return values into the local variables of this function
	// and then returns the error and whether it is retriable to its caller,
	// RequestWithRetry()
	var firstAttempt bool = true
	request := func() (bool, error) {
		var err error

		// if this not the first attempt, retry() will redo all of the
		// setup and SendChunk()s that were done before the first attempt
		if !firstAttempt {
			err = chunkedPutContext.retry()
			if err != nil {
				// closeHelper() will clean up the error, but
				// only if it needs to know there was one
				chunkedPutContext.err = err
			}
		}
		firstAttempt = false

		err = chunkedPutContext.closeHelper()

		// fatal errors cannot be retried because we don't have the data
		// that needs to be resent available (it could not be stored)
		retriable := !chunkedPutContext.fatal
		return retriable, err
	}

	var (
		retryObj *RetryCtrl
		opname   string
		statnm   requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftObjPutCtxtCloseRetryOps,
			retrySuccessCnt:   &stats.SwiftObjPutCtxtCloseRetrySuccessOps,
			clientRequestTime: &globals.ObjectPutCtxtCloseUsec,
			clientFailureCnt:  &globals.ObjectPutCtxtCloseFailure,
			swiftRequestTime:  &globals.SwiftObjectPutCtxtCloseUsec,
			swiftRetryOps:     &globals.SwiftObjectPutCtxtCloseRetryOps,
		}
	)
	retryObj = NewRetryCtrl(globals.retryLimitObject, globals.retryDelayObject, globals.retryExpBackoffObject)
	opname = fmt.Sprintf("swiftclient.chunkedPutContext.Close(\"%v/%v/%v\")",
		chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)

	// RequestWithRetry() tried the operation one or more times until it
	// either: succeeded; failed with a non-retriable (fatal) error; or hit
	// the retry limit. Regardless, its time to release the connection we
	// got at the very start or acquired during retry (if any).
	if err != nil {
		if nil != chunkedPutContext.connection {
			releaseChunkedConnection(chunkedPutContext.connection, false)
		}
	} else {
		releaseChunkedConnection(chunkedPutContext.connection, chunkedPutContext.stillOpen)
	}
	chunkedPutContext.connection = nil

	return err
}

func (chunkedPutContext *chunkedPutContextStruct) closeHelper() (err error) {
	var (
		fsErr       blunder.FsError
		headers     map[string][]string
		httpPayload string
		httpStatus  int
		isError     bool
	)

	chunkedPutContext.Lock()
	defer chunkedPutContext.Unlock()
	defer chunkedPutContext.validateChunkChecksums()

	if !chunkedPutContext.active {
		err = blunder.NewError(blunder.BadHTTPPutError, "called while inactive")
		logger.PanicfWithError(err, "swiftclient.chunkedPutContext.closeHelper(\"%v/%v/%v\") called while inactive",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkedPutContext.active = false

	// if an error occurred earlier there's no point in trying to send the closing chunk
	if chunkedPutContext.err != nil {
		err = chunkedPutContext.err
		return
	}

	err = writeHTTPPutChunk(chunkedPutContext.connection.tcpConn, []byte{})
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.chunkedPutContext.closeHelper(\"%v/%v/%v\") got writeHTTPPutChunk() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(chunkedPutContext.connection.tcpConn)

	atomic.AddUint64(&chunkedPutCloseCnt, 1)
	if atomic.LoadUint64(&globals.chaosCloseChunkFailureRate) > 0 &&
		atomic.LoadUint64(&chunkedPutCloseCnt)%atomic.LoadUint64(&globals.chaosCloseChunkFailureRate) == 0 {
		err = fmt.Errorf("chunkedPutContextStruct.closeHelper(): readHTTPStatusAndHeaders() simulated error")
	}
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.chunkedPutContext.closeHelper(\"%v/%v/%v\") got readHTTPStatusAndHeaders() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}
	evtlog.Record(evtlog.FormatObjectPutChunkedEnd, chunkedPutContext.accountName,
		chunkedPutContext.containerName, chunkedPutContext.objectName,
		chunkedPutContext.bytesPut, uint32(httpStatus))

	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		httpPayload, _ = readHTTPPayloadAsString(chunkedPutContext.connection.tcpConn, headers)
		err = blunder.NewError(fsErr,
			"chunkedPutContext.closeHelper(): PUT '%s/%s/%s' returned HTTP StatusCode %d Payload %s",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName,
			httpStatus, httpPayload)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "chunkedPutContext readHTTPStatusAndHeaders() returned error")
		return
	}
	// even though the PUT succeeded the server may have decided to close
	// the connection, something releaseChunkedConnection() needs to know
	chunkedPutContext.stillOpen = parseConnection(headers)

	stats.IncrementOperations(&stats.SwiftObjPutCtxCloseOps)
	globals.ObjectPutCtxtBytes.Add(chunkedPutContext.bytesPut)

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

	chunkedPutContext.validateChunkChecksums()

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
	globals.ObjectPutCtxtReadBytes.Add(length)

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

	if chunkedPutContext.active {
		chunkedPutContext.Unlock()
		err = blunder.NewError(blunder.BadHTTPPutError, "called while active")
		logger.PanicfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") called while active",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	// clear error from the previous attempt and make active
	chunkedPutContext.err = nil
	chunkedPutContext.active = true

	// try to re-open chunked put connection
	if nil != chunkedPutContext.connection {
		releaseChunkedConnection(chunkedPutContext.connection, false)
	}
	chunkedPutContext.connection, err = acquireChunkedConnection(chunkedPutContext.useReserveForVolumeName)
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") acquireChunkedConnection() failed",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	headers = make(map[string][]string)
	headers["Transfer-Encoding"] = []string{"chunked"}

	err = writeHTTPRequestLineAndHeaders(chunkedPutContext.connection.tcpConn, "PUT", "/"+swiftVersion+"/"+pathEscape(chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName), headers)
	if nil != err {
		chunkedPutContext.Unlock()
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") got writeHTTPRequestLineAndHeaders() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		return
	}

	chunkIndex = 0

	chunkedPutContext.validateChunkChecksums()
	for {
		_, chunkBufAsValue, ok, err = chunkedPutContext.bytesPutTree.GetByIndex(chunkIndex)
		if nil != err {
			chunkedPutContext.Unlock()
			err = blunder.NewError(blunder.BadHTTPPutError, "bytesPutTree.GetByIndex() failed: %v", err)
			logger.WarnfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") got bytesPutTree.GetByIndex() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
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
			logger.WarnfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") got bytesPutTree.GetByIndex(() malformed Value", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
			return
		}

		// check for chaos error generation (testing only)
		atomic.AddUint64(&chunkedPutSendRetryCnt, 1)
		if atomic.LoadUint64(&globals.chaosSendChunkFailureRate) > 0 &&
			atomic.LoadUint64(&chunkedPutSendRetryCnt)%atomic.LoadUint64(&globals.chaosSendChunkFailureRate) == 0 {

			nChunk, _ := chunkedPutContext.bytesPutTree.Len()
			err = fmt.Errorf("chunkedPutContextStruct.retry(): "+
				"writeHTTPPutChunk() simulated error with %d chunks", nChunk)
		} else {
			err = writeHTTPPutChunk(chunkedPutContext.connection.tcpConn, chunkBufAsByteSlice)
		}
		if nil != err {
			chunkedPutContext.Unlock()
			err = blunder.NewError(blunder.BadHTTPPutError, "writeHTTPPutChunk() failed: %v", err)
			logger.WarnfWithError(err, "swiftclient.chunkedPutContext.retry(\"%v/%v/%v\") got writeHTTPPutChunk() error", chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
			return
		}

		// See if there is (was) another chunk

		chunkIndex++
	}
	chunkedPutContext.Unlock()

	stats.IncrementOperations(&stats.SwiftObjPutCtxRetryOps)

	return
}

// If checksums are enabled validate the checksums for each chunk.
//
// The chunked put context is assumed to be locked (not actively changing), and
// is returned still locked.
//
func (chunkedPutContext *chunkedPutContextStruct) validateChunkChecksums() {
	if !globals.checksumChunkedPutChunks {
		return
	}
	if chunkedPutContext.bytesPut == 0 {
		return
	}

	var (
		chunkBufAsValue  sortedmap.Value
		chunkBuf         []byte
		chunkOffsetAsKey sortedmap.Key
		chunkOffset      uint64
		chunkIndex       int
		ok               bool
		panicErr         error
		offset           uint64
	)

	// print as much potentially useful information as we can before panic'ing
	panicErr = nil
	offset = 0
	for chunkIndex = 0; true; chunkIndex++ {
		var err error

		chunkOffsetAsKey, chunkBufAsValue, ok, err = chunkedPutContext.bytesPutTree.GetByIndex(chunkIndex)
		if err != nil {
			panicErr = fmt.Errorf("bytesPutTree.GetByIndex(%d) offset %d at failed: %v",
				chunkIndex, offset, err)
			break
		}
		if !ok {
			// We've reached the end of bytesPutTree... so we
			// (presumably) have now check all the chunks
			break
		}

		chunkOffset, ok = chunkOffsetAsKey.(uint64)
		if !ok {
			panicErr = fmt.Errorf(
				"bytesPutTree.GetByIndex(%d) offset %d returned incorrect key type: %T",
				chunkIndex, offset, chunkOffsetAsKey)
			break
		}

		chunkBuf, ok = chunkBufAsValue.([]byte)
		if !ok {
			panicErr = fmt.Errorf(
				"bytesPutTree.GetByIndex(%d) offset %d returned incorrect value type: %T",
				chunkIndex, offset, chunkOffsetAsKey)
			break
		}

		// verify slice still points to the same memory and has correct checksum
		if &chunkBuf[0] != &chunkedPutContext.chunkInfoArray[chunkIndex].chunkBuf[0] {
			err = fmt.Errorf("chunkInfoArray(%d) offset %d returned buf %p != saved buf %p",
				chunkIndex, offset, &chunkBuf[0],
				&chunkedPutContext.chunkInfoArray[chunkIndex].chunkBuf[0])

		} else if cityhash.Hash64(chunkBuf) != chunkedPutContext.chunkInfoArray[chunkIndex].chunkCksum {
			err = fmt.Errorf(
				"chunkInfoArray(%d) offset %d computed chunk checksum 0x%16x != saved checksum 0x%16x",
				chunkIndex, offset, cityhash.Hash64(chunkBuf),
				chunkedPutContext.chunkInfoArray[chunkIndex].chunkCksum)
		} else if offset != chunkOffset {
			err = fmt.Errorf("chunkInfoArray(%d) offset %d returned incorrect offset key %d",
				chunkIndex, offset, chunkOffset)
		}

		if err != nil {
			logger.ErrorfWithError(err,
				"validateChunkChecksums('%v/%v/%v') %d chunks %d bytes invalid chunk",
				chunkedPutContext.accountName, chunkedPutContext.containerName,
				chunkedPutContext.objectName,
				len(chunkedPutContext.chunkInfoArray), chunkedPutContext.bytesPut)
			if panicErr == nil {
				panicErr = err
			}
		}

		offset += uint64(len(chunkBuf))
	}

	if panicErr == nil {
		return
	}

	// log time stamps for chunked put (and last checksum validation)
	logger.Errorf(
		"validateChunkChecksums('%v/%v/%v') fetched %f sec ago last SendChunk() %f sec ago",
		chunkedPutContext.accountName, chunkedPutContext.containerName,
		chunkedPutContext.objectName,
		time.Since(chunkedPutContext.fetchTime).Seconds(),
		time.Since(chunkedPutContext.sendTime).Seconds())

	// there was an error; dump stack, other relevant information, and panic
	stackBuf := make([]byte, 64*1024)
	stackBuf = stackBuf[:runtime.Stack(stackBuf, false)]
	logger.Errorf(
		"validateChunkChecksums('%v/%v/%v') chunked buffer error: stack trace:\n%s",
		chunkedPutContext.accountName, chunkedPutContext.containerName,
		chunkedPutContext.objectName, stackBuf)

	logger.PanicfWithError(panicErr,
		"validateChunkChecksums('%v/%v/%v') %d chunks %d chunk bytes chunked buffer error",
		chunkedPutContext.accountName, chunkedPutContext.containerName,
		chunkedPutContext.objectName,
		len(chunkedPutContext.chunkInfoArray), chunkedPutContext.bytesPut)
	panic(panicErr)
}

// used during testing for error injection
var (
	chunkedPutSendCnt      uint64
	chunkedPutSendRetryCnt uint64
	chunkedPutCloseCnt     uint64
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

	clientReqTime := time.Now()

	chunkedPutContext.Lock()

	// record the start of the most recent (modulo lock scheduling) send request
	chunkedPutContext.sendTime = clientReqTime

	if !chunkedPutContext.active {
		err = blunder.NewError(blunder.BadHTTPPutError, "called while inactive")
		logger.PanicfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") logic error",
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
		logger.PanicfWithError(err, "swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") logic error",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		chunkedPutContext.err = err
		chunkedPutContext.fatal = true
		releaseChunkedConnection(chunkedPutContext.connection, false)
		chunkedPutContext.connection = nil
		chunkedPutContext.Unlock()
		return
	}

	// validate current checksums
	chunkedPutContext.validateChunkChecksums()

	// if checksum verification is enabled, compute the checksum
	if globals.checksumChunkedPutChunks {
		var chunkInfo chunkedPutChunkInfo

		chunkInfo.chunkBuf = buf
		chunkInfo.chunkCksum = cityhash.Hash64(buf)
		chunkedPutContext.chunkInfoArray = append(chunkedPutContext.chunkInfoArray, chunkInfo)
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
	// (Close() will not be called, so retry() will not be called).
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
	atomic.AddUint64(&chunkedPutSendCnt, 1)
	if atomic.LoadUint64(&globals.chaosSendChunkFailureRate) > 0 &&
		atomic.LoadUint64(&chunkedPutSendCnt)%atomic.LoadUint64(&globals.chaosSendChunkFailureRate) == 0 {
		err = fmt.Errorf("chunkedPutContextStruct.SendChunk(): writeHTTPPutChunk() simulated error")
	} else {
		err = writeHTTPPutChunk(chunkedPutContext.connection.tcpConn, buf)
	}
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err,
			"swiftclient.chunkedPutContext.SendChunk(\"%v/%v/%v\") got writeHTTPPutChunk() error",
			chunkedPutContext.accountName, chunkedPutContext.containerName, chunkedPutContext.objectName)
		chunkedPutContext.err = err
		chunkedPutContext.Unlock()
		err = nil
		return
	}

	chunkedPutContext.Unlock()

	stats.IncrementOperationsAndBucketedBytes(stats.SwiftObjPutCtxSendChunk, uint64(len(buf)))
	elapsedUsec := time.Since(clientReqTime).Nanoseconds() / time.Microsecond.Nanoseconds()
	globals.ObjectPutCtxtSendChunkUsec.Add(uint64(elapsedUsec))
	globals.ObjectPutCtxtSendChunkBytes.Add(uint64(len(buf)))

	return
}
