// Swift Container-specific API access implementation

package swiftclient

import (
	"net"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

func containerDelete(accountName string, containerName string) (err error) {
	var (
		fsErr      blunder.FsError
		headers    map[string][]string
		httpStatus int
		isError    bool
		tcpConn    *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.containerDelete(\"%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "DELETE", "/"+swiftVersion+"/"+accountName+"/"+containerName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.containerDelete(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPDeleteError)
		logger.ErrorfWithError(err, "swiftclient.containerDelete(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "DELETE %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.containerDelete(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftContainerDeleteOps)

	return
}

func containerGet(accountName string, containerName string) (headers map[string][]string, objectList []string, err error) {
	var (
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
		tcpConn    *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.containerGet(\"%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "GET", "/"+swiftVersion+"/"+accountName+"/"+containerName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.containerGet(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.containerGet(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "GET %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.containerGet(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}

	// TODO: Possibly need to handle Transfer-Encoding: chunked for objectList

	objectList, err = readHTTPPayloadLines(tcpConn, headers)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPGetError)
		logger.ErrorfWithError(err, "swiftclient.containerGet(\"%v/%v\") got readHTTPPayloadLines() error", accountName, containerName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftContainerGetOps)

	return
}

func containerHead(accountName string, containerName string) (headers map[string][]string, err error) {
	var (
		fsErr      blunder.FsError
		httpStatus int
		isError    bool
		tcpConn    *net.TCPConn
	)

	tcpConn, err = acquireNonChunkedConnection()
	if nil != err {
		logger.ErrorfWithError(err, "swiftclient.containerHead(\"%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName)
		return
	}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "HEAD", "/"+swiftVersion+"/"+accountName+"/"+containerName, nil)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.containerHead(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, headers, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPHeadError)
		logger.ErrorfWithError(err, "swiftclient.containerHead(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "HEAD %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.containerHead(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(headers))

	stats.IncrementOperations(&stats.SwiftContainerHeadOps)

	return
}

func containerPost(accountName string, containerName string, requestHeaders map[string][]string) (err error) {
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
		logger.ErrorfWithError(err, "swiftclient.containerPost(\"%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName)
		return
	}

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "POST", "/"+swiftVersion+"/"+accountName+"/"+containerName, requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.containerPost(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.containerPost(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "POST %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.containerPost(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.containerPost(\"%v/%v\") got parseContentLength() error", accountName, containerName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(tcpConn, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.ErrorfWithError(err, "swiftclient.containerPost(\"%v/%v\") got readBytesFromTCPConn() error", accountName, containerName)
			return
		}
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftContainerPutOps)

	return
}

func containerPut(accountName string, containerName string, requestHeaders map[string][]string) (err error) {
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
		logger.ErrorfWithError(err, "swiftclient.containerPut(\"%v/%v\") got acquireNonChunkedConnection() error", accountName, containerName)
		return
	}

	requestHeaders["Content-Length"] = []string{"0"}

	err = writeHTTPRequestLineAndHeaders(tcpConn, "PUT", "/"+swiftVersion+"/"+accountName+"/"+containerName, requestHeaders)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.containerPut(\"%v/%v\") got writeHTTPRequestLineAndHeaders() error", accountName, containerName)
		return
	}

	httpStatus, responseHeaders, err = readHTTPStatusAndHeaders(tcpConn)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.containerPut(\"%v/%v\") got readHTTPStatusAndHeaders() error", accountName, containerName)
		return
	}
	isError, fsErr = httpStatusIsError(httpStatus)
	if isError {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.NewError(fsErr, "PUT %s/%s returned HTTP StatusCode %d", accountName, containerName, httpStatus)
		err = blunder.AddHTTPCode(err, httpStatus)
		logger.ErrorfWithError(err, "swiftclient.containerPut(\"%v/%v\") got readHTTPStatusAndHeaders() bad status", accountName, containerName)
		return
	}
	contentLength, err = parseContentLength(responseHeaders)
	if nil != err {
		releaseNonChunkedConnection(tcpConn, false)
		err = blunder.AddError(err, blunder.BadHTTPPutError)
		logger.ErrorfWithError(err, "swiftclient.containerPut(\"%v/%v\") got parseContentLength() error", accountName, containerName)
		return
	}
	if 0 < contentLength {
		_, err = readBytesFromTCPConn(tcpConn, contentLength)
		if nil != err {
			releaseNonChunkedConnection(tcpConn, false)
			err = blunder.AddError(err, blunder.BadHTTPPutError)
			logger.ErrorfWithError(err, "swiftclient.containerPut(\"%v/%v\") got readBytesFromTCPConn() error", accountName, containerName)
			return
		}
	}

	releaseNonChunkedConnection(tcpConn, parseConnection(responseHeaders))

	stats.IncrementOperations(&stats.SwiftContainerPutOps)

	return
}
