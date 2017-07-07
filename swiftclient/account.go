// Swift Account-specific API access implementation

package swiftclient

import (
	"net"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

func accountDelete(accountName string) (err error) {
	var (
		fsErr      blunder.FsError
		headers    map[string][]string
		httpStatus int
		isError    bool
		tcpConn    *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.accountDelete(\"%v\") got acquireNonChunkedConnection() error", accountName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "DELETE", "/"+swiftVersion+"/"+accountName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.accountDelete(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.accountDelete(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "DELETE %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountDelete(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftAccountDeleteOps)

	return
}

func accountGet(accountName string) (headers map[string][]string, containerList []string, err error) {
	var (
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
		tcpConn    *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got acquireNonChunkedConnection() error", accountName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "GET", "/"+swiftVersion+"/"+accountName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "GET %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}

	// TODO: Possibly need to handle Transfer-Encoding: chunked for containerList

	containerList, err = readHTTPPayloadLines(tcpConn, headers)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.accountGet(\"%v\") got readHTTPPayloadLines() error", accountName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftAccountGetOps)

	return
}

func accountHead(accountName string) (headers map[string][]string, err error) {
	var (
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
		tcpConn    *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.accountHead(\"%v\") got acquireNonChunkedConnection() error", accountName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "HEAD", "/"+swiftVersion+"/"+accountName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.accountHead(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.accountHead(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "HEAD %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountHead(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftAccountHeadOps)

	return
}

func accountPost(accountName string, requestHeaders map[string][]string) (err error) {
	var (
		contentLength   int
		fsErr           blunder.FsError
		httpStatus      int
		isError         bool
		responseHeaders map[string][]string
		tcpConn         *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got acquireNonChunkedConnection() error", accountName)
		return
	}

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "POST", "/"+swiftVersion+"/"+accountName, requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "POST %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got parseContentLength() error", accountName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(tcpConn, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.ErrorfWithError(err, "swiftclient.accountPost(\"%v\") got readBytesFromTCPConn() error", accountName)
			return
		}
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftAccountPutOps)

	return
}

func accountPut(accountName string, requestHeaders map[string][]string) (err error) {
	var (
		contentLength   int
		fsErr           blunder.FsError
		httpStatus      int
		isError         bool
		responseHeaders map[string][]string
		tcpConn         *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got acquireNonChunkedConnection() error", accountName)
		return
	}

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "PUT", "/"+swiftVersion+"/"+accountName, requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got writeHTTPRequestLineAndHeaders() error", accountName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got readHTTPStatusAndHeaders() error", accountName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "PUT %s returned HTTP StatusCode %d", accountName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got readHTTPStatusAndHeaders() bad status", accountName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got parseContentLength() error", accountName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(tcpConn, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.ErrorfWithError(err, "swiftclient.accountPut(\"%v\") got readBytesFromTCPConn() error", accountName)
			return
		}
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftAccountPutOps)

	return
}
