// Swift Account-specific API access implementation

package swiftclient

import (
	"fmt"
	"net/url"

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
		retryObj *RetryCtrl = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string     = fmt.Sprintf("swiftclient.accountDelete(\"%v\")", accountName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftAccountDeleteRetryOps,
			retrySuccessCnt:   &stats.SwiftAccountDeleteRetrySuccessOps,
			clientRequestTime: &globals.AccountDeleteUsec,
			clientFailureCnt:  &globals.AccountDeleteFailure,
			swiftRequestTime:  &globals.SwiftAccountDeleteUsec,
			swiftRetryOps:     &globals.SwiftAccountDeleteRetryOps,
		}
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

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "DELETE", "/"+swiftVersion+"/"+pathEscape(accountName), nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.WarnfWithError(err, "swiftclient.accountDelete(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.WarnfWithError(err, "swiftclient.accountDelete(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountDelete, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "DELETE %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.accountDelete(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}

	releaseNonChunkedConnection(connection, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftAccountDeleteOps)

	return
}

func accountGetWithRetry(accountName string) (headers map[string][]string, containerList []string, err error) {
	// request is a function that, through the miracle of closure, calls
	// accountGet() with the paramaters passed to this function, stashes the
	// relevant return values into the local variables of this function, and
	// then returns err and whether it is retriable to RequestWithRetry()
	var (
		connection         *connectionStruct
		marker             string
		opname             string
		retryObj           *RetryCtrl
		statnm             requestStatistics
		toAddContainerList []string
		toAddHeaders       map[string][]string
	)

	retryObj = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
	statnm = requestStatistics{
		retryCnt:          &stats.SwiftAccountGetRetryOps,
		retrySuccessCnt:   &stats.SwiftAccountGetRetrySuccessOps,
		clientRequestTime: &globals.AccountGetUsec,
		clientFailureCnt:  &globals.AccountGetFailure,
		swiftRequestTime:  &globals.SwiftAccountGetUsec,
		swiftRetryOps:     &globals.SwiftAccountGetRetryOps,
	}

	request := func() (bool, error) {
		var err error
		toAddHeaders, toAddContainerList, err = accountGet(connection, accountName, marker)
		return true, err
	}

	headers = make(map[string][]string)
	containerList = make([]string, 0)

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	marker = ""

	for {
		opname = fmt.Sprintf("swiftclient.accountGet(,\"%v\",\"%v\")", accountName, marker)

		err = retryObj.RequestWithRetry(request, &opname, &statnm)

		if nil == err {
			mergeHeadersAndList(headers, &containerList, toAddHeaders, &toAddContainerList)

			if 0 == len(toAddContainerList) {
				releaseNonChunkedConnection(connection, parseConnection(headers))

				break
			} else {
				marker = toAddContainerList[len(toAddContainerList)-1]
			}
		} else {
			releaseNonChunkedConnection(connection, false)

			break
		}
	}

	stats.IncrementOperations(&stats.SwiftAccountGetOps)

	return
}

func accountGet(connection *connectionStruct, accountName string, marker string) (headers map[string][]string, containerList []string, err error) {
	var (
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
	)

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "GET", "/"+swiftVersion+"/"+pathEscape(accountName)+"?marker="+url.QueryEscape(marker), nil)
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.accountGet(,\"%v\",\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName, marker)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.accountGet(,\"%v\",\"%v\") got readHTTPStatusAndHeaders() error", accountName, marker)
		return
	}
	evtlog.Record(evtlog.FormatAccountGet, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		err = blunder.NewError(fsErr, "GET %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.accountGet(,\"%v\",\"%v\") got readHTTPStatusAndHeaders() bad status", accountName, marker)
		return
	}

	containerList, err = readHTTPPayloadLines(connection.tcpConn, headers)
	if nil != err {
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.WarnfWithError(err, "swiftclient.accountGet(,\"%v\",\"%v\") got readHTTPPayloadLines() error", accountName, marker)
		return
	}

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
		retryObj *RetryCtrl = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string     = fmt.Sprintf("swiftclient.accountHead(\"%v\")", accountName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftAccountHeadRetryOps,
			retrySuccessCnt:   &stats.SwiftAccountHeadRetrySuccessOps,
			clientRequestTime: &globals.AccountHeadUsec,
			clientFailureCnt:  &globals.AccountHeadFailure,
			swiftRequestTime:  &globals.SwiftAccountHeadUsec,
			swiftRetryOps:     &globals.SwiftAccountHeadRetryOps,
		}
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

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "HEAD", "/"+swiftVersion+"/"+pathEscape(accountName), nil)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.accountHead(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.WarnfWithError(err, "swiftclient.accountHead(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountHead, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "HEAD %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.accountHead(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
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
		retryObj *RetryCtrl = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)

		opname string            = fmt.Sprintf("swiftclient.accountPost(\"%v\")", accountName)
		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftAccountPostRetryOps,
			retrySuccessCnt:   &stats.SwiftAccountPostRetrySuccessOps,
			clientRequestTime: &globals.AccountPostUsec,
			clientFailureCnt:  &globals.AccountPostFailure,
			swiftRequestTime:  &globals.SwiftAccountPostUsec,
			swiftRetryOps:     &globals.SwiftAccountPostRetryOps,
		}
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

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "POST", "/"+swiftVersion+"/"+pathEscape(accountName), requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.accountPost(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.accountPost(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountPost, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "POST %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.accountPost(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.accountPost(\"%v\") got parseContentLength() error", accountName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.WarnfWithError(err, "swiftclient.accountPost(\"%v\") got readBytesFromTCPConn() error", accountName)
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
		retryObj *RetryCtrl = NewRetryCtrl(globals.retryLimit, globals.retryDelay, globals.retryExpBackoff)
		opname   string     = fmt.Sprintf("swiftclient.accountPut(\"%v\")", accountName)

		statnm requestStatistics = requestStatistics{
			retryCnt:          &stats.SwiftAccountPutRetryOps,
			retrySuccessCnt:   &stats.SwiftAccountPutRetrySuccessOps,
			clientRequestTime: &globals.AccountPutUsec,
			clientFailureCnt:  &globals.AccountPutFailure,
			swiftRequestTime:  &globals.SwiftAccountPutUsec,
			swiftRetryOps:     &globals.SwiftAccountPutRetryOps,
		}
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

	connection, err = acquireNonChunkedConnection()
	if err != nil {
		// acquireNonChunkedConnection()/openConnection() logged a warning
		return
	}

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(connection.tcpConn, "PUT", "/"+swiftVersion+"/"+pathEscape(accountName), requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.accountPut(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(connection.tcpConn)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.accountPut(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	evtlog.Record(evtlog.FormatAccountPut, accountName, uint32(httpStatus))
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(connection, false)
		err = blunder.NewError(fsErr, "PUT %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.WarnfWithError(err, "swiftclient.accountPut(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(connection, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.WarnfWithError(err, "swiftclient.accountPut(\"%v\") got parseContentLength() error", accountName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(connection.tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(connection, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.WarnfWithError(err, "swiftclient.accountPut(\"%v\") got readBytesFromTCPConn() error", accountName)
			return
		}
	}

	releaseNonChunkedConnection(connection, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftAccountPutOps)

	return
}
