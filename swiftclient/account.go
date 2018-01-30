// Swift Account-specific API access implementation

package swiftclient

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

func accountDeleteWithRetry(accountName string) (err error) {
	// request is a function that, through the miracle of closure, calls
	// accountDelete() with the paramaters passed to this function, stashes
	// the relevant return values into the local variables of this function,
	// and then returns err and whether it is retriable to RequestWithRetry()
	request := func() (bool, error) {
		var err error
		err = accountDelete(accountName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.accountDelete(\"%v\")", accountName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftAccountDeleteRetryOps,
			retrySuccessCnt: &stats.SwiftAccountDeleteRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func accountDelete(accountName string) (err error) {
	var (
		connection *connectionStruct
		fsErr      blunder.FsError
		headers    map[string][]string
		httpStatus int
		isError    bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "DELETE", "/"+swiftVersion+"/"+accountName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.accountDelete(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.accountDelete(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountDelete, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "DELETE %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountDelete(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftAccountDeleteOps)

	return
}

func accountGetWithRetry(accountName string) (map[string][]string, []string, error) {
	// request is a function that, through the miracle of closure, calls
	// accountGet() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	var (
		headers       map[string][]string
		containerList []string
	)
	request := func() (bool, error) {
		var err error
		headers, containerList, err = accountGet(accountName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.accountGet(\"%v\")", accountName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftAccountGetRetryOps,
			retrySuccessCnt: &stats.SwiftAccountGetRetrySuccessOps}
	)
	err := retryObj.RequestWithRetry(request, &opname, &statnm)
	return headers, containerList, err
}

func accountGet(accountName string) (headers map[string][]string, containerList []string, err error) {
	var (
		connection *connectionStruct
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+accountName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountGet, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "GET %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}

	// TODO: Possibly need to handle Transfer-Encoding: chunked for containerList

	containerList, err = readHTTPPayloadLines(connection.tcpConn, headers)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got readHTTPPayloadLines() error", accountName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftAccountGetOps)

	return
}

func accountHeadWithRetry(accountName string) (map[string][]string, error) {
	// request is a function that, through the miracle of closure, calls
	// accountHead() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	var (
		headers map[string][]string
	)
	request := func() (bool, error) {
		var err error
		headers, err = accountHead(accountName)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.accountHead(\"%v\")", accountName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftAccountHeadRetryOps,
			retrySuccessCnt: &stats.SwiftAccountHeadRetrySuccessOps}
	)
	err := retryObj.RequestWithRetry(request, &opname, &statnm)
	return headers, err
}

func accountHead(accountName string) (headers map[string][]string, err error) {
	var (
		connection *connectionStruct
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
	)

	connection = acquireNonChunkedConnection()

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "HEAD", "/"+swiftVersion+"/"+accountName, nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.accountHead(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.accountHead(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountHead, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "HEAD %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountHead(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftAccountHeadOps)

	return
}

func accountPostWithRetry(accountName string, requestHeaders map[string][]string) (err error) {
	// request is a function that, through the miracle of closure, calls
	// accountPost() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	request := func() (bool, error) {
		var err error
		err = accountPost(accountName, requestHeaders)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.accountPost(\"%v\")", accountName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftAccountPostRetryOps,
			retrySuccessCnt: &stats.SwiftAccountPostRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func accountPost(accountName string, requestHeaders map[string][]string) (err error) {
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

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "POST", "/"+swiftVersion+"/"+accountName, requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountPost, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "POST %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got parseContentLength() error", accountName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got readBytesFromTCPConn() error", accountName)
			return
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftAccountPostOps)

	return
}

func accountPutWithRetry(accountName string, requestHeaders map[string][]string) (err error) {
	// request is a function that, through the miracle of closure, calls
	// accountPut() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	request := func() (bool, error) {
		var err error
		err = accountPut(accountName, requestHeaders)
		return true, err
	}

	var (
		retryObj *RetryCtrl  = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string      = fmt.Sprintf("swiftclient.accountPut(\"%v\")", accountName)
		statnm   RetryStatNm = RetryStatNm{
			retryCnt:        &stats.SwiftAccountPutRetryOps,
			retrySuccessCnt: &stats.SwiftAccountPutRetrySuccessOps}
	)
	err = retryObj.RequestWithRetry(request, &opname, &statnm)
	return err
}

func accountPut(accountName string, requestHeaders map[string][]string) (err error) {
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

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "PUT", "/"+swiftVersion+"/"+accountName, requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountPut, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "PUT %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got parseContentLength() error", accountName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got readBytesFromTCPConn() error", accountName)
			return
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftAccountPutOps)

	return
}
