// Swift Container-specific API access implementation

package swiftclient

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

func containerDeleteWithRetry(accountName string, containerName string) (err error) {
	// request is a function that, through the miracle of closure, calls
	// containerDelete() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	request := func() (bool, error) {
		var err error
		err = containerDelete(accountName, containerName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.containerDelete(\"%v/%v\")", accountName, containerName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftContainerDeleteRetryOps,
			retrySuccessCnt: &stats.SwiftContainerDeleteRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func containerDelete(accountName string, containerName string) (err error) {
	var (
		connection *connectionStruct
		fsErr      blunder.FsError
		headers    map[string][]string
		httpStatus int
		isError    bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "DELETE", "/"+swiftVersion+"/"+accountName+"/"+containerName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.WarnfWithError(err, "swiftclient.containerDelete(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.WarnfWithError(err, "swiftclient.containerDelete(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	evtlog.Record(evtlog.FormatContainerDelete, accountName, containerName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "DELETE %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.containerDelete(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftContainerDeleteOps)

	return
}

func containerGetWithRetry(accountName string, containerName string) (headers map[string][]string, objectList []string, err error) {
	// request is a function that, through the miracle of closure, calls
	// containerGet() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	var (
		connection      *connectionStruct
		marker          string
		opname          string
		retryObj        *RetryCtrl
		statnm          RetryStatNm
		toAddHeaders    map[string][]string
		toAddObjectList []string
	)

	retryObj = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
	statnm = RetryStatNm{
		retryCnt:        &stats.SwiftContainerGetRetryOps,
		retrySuccessCnt: &stats.SwiftContainerGetRetrySuccessOps}

	request := func() (bool, error) {
		var err error
		toAddHeaders, toAddObjectList, err = containerGet(connection, accountName, containerName, marker)
		return true, err
	}

	headers = make(map[string][]string)
	objectList = make([]string, 0)

	connection = acquireNonChunkedConnection()

	marker = ""

	for {
		opname = fmt.Sprintf("swiftclient.containerGet(,\"%v\",\"%v\",\"%v\")", accountName, containerName, marker)

		err = retryObj.RequestWithRetry(request, &opname, &statnm)

		if nil == err {
			mergeHeadersAndList(headers, &objectList, toAddHeaders, &toAddObjectList)

			if 0 == len(toAddObjectList) {
				releaseNonChunkedConnection(connection, parseConnection(headers))

				break
			} else {
				marker = toAddObjectList[len(toAddObjectList)-1]
			}
		} else {
			releaseNonChunkedConnection(connection, false)

			break
		}
	}

	stats.IncrementOperations(&stats.SwiftContainerGetOps)

	return
}

func containerGet(connection *connectionStruct, accountName string, containerName string, marker string) (headers map[string][]string, objectList []string, err error) {
	var (
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
	)

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName+"?marker="+marker, nil)
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.containerGet(,\"%v\",\"%v\",\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName, marker)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.containerGet(,\"%v\",\"%v\",\"%v\") got readHTTPStatusAndHeaders() error", accountName, containerName, marker)
		return
	}
	evtlog.Record(evtlog.FormatContainerGet, accountName, containerName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		err = blunder.NewError(fsErr, "GET %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.containerGet(,\"%v\",\"%v\",\"%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName, marker)
		return
	}

	objectList, err = readHTTPPayloadLines(connection.tcpConn, headers)
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.containerGet(,\"%v\",\"%v\",\"%v\") got readHTTPPayloadLines() error", accountName, containerName, marker)
		return
	}

	return
}

func containerHeadWithRetry(accountName string, containerName string) (map[string][]string, error) {
	// request is a function that, through the miracle of closure, calls
	// containerHead() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	var (
		headers map[string][]string
		err     error
	)
	request := func() (bool, error) {
		var err error
		headers, err = containerHead(accountName, containerName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.containerHead(\"%v/%v\")", accountName, containerName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftContainerHeadRetryOps,
			retrySuccessCnt: &stats.SwiftContainerHeadRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return headers, err
}

func containerHead(accountName string, containerName string) (headers map[string][]string, err error) {
	var (
		connection *connectionStruct
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "HEAD", "/"+swiftVersion+"/"+accountName+"/"+containerName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.containerHead(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.containerHead(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	evtlog.Record(evtlog.FormatContainerHead, accountName, containerName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "HEAD %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.containerHead(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftContainerHeadOps)

	return
}

func containerPostWithRetry(accountName string, containerName string, requestHeaders map[string][]string) (err error) {
	// request is a function that, through the miracle of closure, calls
	// containerPost() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	request := func() (bool, error) {
		var err error
		err = containerPost(accountName, containerName, requestHeaders)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.containerPost(\"%v/%v\")", accountName, containerName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftContainerPostRetryOps,
			retrySuccessCnt: &stats.SwiftContainerPostRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func containerPost(accountName string, containerName string, requestHeaders map[string][]string) (err error) {
	var (
		connection      *connectionStruct
		contentLength   int
		fsErr           blunder.FsError
		httpStatus      int
		isError         bool
		responseHeaders map[string][]string
	)

	connection = acquireNonChunkedConnection()

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "POST", "/"+swiftVersion+"/"+accountName+"/"+containerName, requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.containerPost(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.containerPost(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	evtlog.Record(evtlog.FormatContainerPost, accountName, containerName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "POST %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.containerPost(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.containerPost(\"%v/%v\") got parseContentLength() error", accountName, containerName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.WarnfWithError(err, "swiftclient.containerPost(\"%v/%v\") got readBytesFromTCPConn() error", accountName, containerName)
			return
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftContainerPostOps)

	return
}

func containerPutWithRetry(accountName string, containerName string, requestHeaders map[string][]string) (err error) {
	// request is a function that, through the miracle of closure, calls
	// containerPut() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	request := func() (bool, error) {
		var err error
		err = containerPut(accountName, containerName, requestHeaders)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.containerPut(\"%v/%v\")", accountName, containerName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftContainerPutRetryOps,
			retrySuccessCnt: &stats.SwiftContainerPutRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func containerPut(accountName string, containerName string, requestHeaders map[string][]string) (err error) {
	var (
		connection      *connectionStruct
		contentLength   int
		fsErr           blunder.FsError
		httpStatus      int
		isError         bool
		responseHeaders map[string][]string
	)

	connection = acquireNonChunkedConnection()

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "PUT", "/"+swiftVersion+"/"+accountName+"/"+containerName, requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.containerPut(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.containerPut(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	evtlog.Record(evtlog.FormatContainerPut, accountName, containerName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "PUT %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.containerPut(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.containerPut(\"%v/%v\") got parseContentLength() error", accountName, containerName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.WarnfWithError(err, "swiftclient.containerPut(\"%v/%v\") got readBytesFromTCPConn() error", accountName, containerName)
			return
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftContainerPutOps)

	return
}
