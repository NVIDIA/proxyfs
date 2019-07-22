// Package swiftclient provides API access to the local Swift NoAuth Pipeline.
package swiftclient

type OperationOptions uint64

const (
	SkipRetry OperationOptions = 1 << iota
)

// ChunkedCopyContext provides a callback context to use for Object Copies.
//
// A returned chunkSize == 0 says to stop copying
type ChunkedCopyContext interface {
	BytesRemaining(bytesRemaining uint64) (chunkSize uint64)
}

// ChunkedPutContext provides a context to use for Object HTTP PUTs using "chunked" Transfer-Encoding.
//
// The slice(s) passed to SendChunk() must not be modified until Close() has
// been called and has returned success or failure.  Close() must always be
// called on a successfullly opened ChunkedPutContext, even if SendChunk()
// returns an error, or else we will leak open connections (although SendChunk()
// does its best).
type ChunkedPutContext interface {
	Active() (active bool)                                     // Report if currently active
	BytesPut() (bytesPut uint64, err error)                    // Report how many bytes have been sent via SendChunk() for this ChunkedPutContext
	Close() (err error)                                        // Finish the "chunked" HTTP PUT for this ChunkedPutContext (with possible retry)
	Read(offset uint64, length uint64) (buf []byte, err error) // Read back bytes previously sent via SendChunk()
	SendChunk(buf []byte) (err error)                          // Send the supplied "chunk" via this ChunkedPutContext
}

// StarvationParameters is used to report the current details pertaining to both
// the chunkedConnectionPool and nonChunkedConnectionPool. As this is a snapshot of
// a typically rapidly evolving state, it should only be use in well controlled test
// and debugging situations.
type StarvationParameters struct {
	ChunkedConnectionPoolCapacity      uint16
	ChunkedConnectionPoolInUse         uint16
	ChunkedConnectionPoolNumWaiters    uint64
	NonChunkedConnectionPoolCapacity   uint16
	NonChunkedConnectionPoolInUse      uint16
	NonChunkedConnectionPoolNumWaiters uint64
}

// GetStarvationParameters returns the the current details pertaining to both
// the chunkedConnectionPool and nonChunkedConnectionPool.
func GetStarvationParameters() (starvationParameters *StarvationParameters) {
	starvationParameters = getStarvationParameters()
	return
}

// StarvationCallbackFunc specifies the signature of a callback function to be invoked when
// the Chunked PUT Connection Pool is exhausted and would like the callback function to
// relieve this exhaustion.
type StarvationCallbackFunc func()

// SetStarvationCallbackFunc sets (or resets, if passed nil) the callback function to be
// invoked when the Chunked PUT Connection Pool is exhausted and would like the callback
// function to relieve this exhaustion.
func SetStarvationCallbackFunc(starvationCallback StarvationCallbackFunc) {
	globals.starvationCallback = starvationCallback
}

// AccountDelete invokes HTTP DELETE on the named Swift Account.
func AccountDelete(accountName string) (err error) {
	return accountDeleteWithRetry(accountName)
}

// AccountGet invokes HTTP GET on the named Swift Account.
func AccountGet(accountName string) (headers map[string][]string, containerList []string, err error) {
	return accountGetWithRetry(accountName)
}

// AccountHead invokes HTTP HEAD on the named Swift Account.
func AccountHead(accountName string) (headers map[string][]string, err error) {
	return accountHeadWithRetry(accountName)
}

// AccountPost invokes HTTP POST on the named Swift Account.
func AccountPost(accountName string, headers map[string][]string) (err error) {
	return accountPostWithRetry(accountName, headers)
}

// AccountPut invokes HTTP PUT on the named Swift Account.
func AccountPut(accountName string, headers map[string][]string) (err error) {
	return accountPutWithRetry(accountName, headers)
}

// ContainerDelete invokes HTTP DELETE on the named Swift Container.
func ContainerDelete(accountName string, containerName string) (err error) {
	return containerDeleteWithRetry(accountName, containerName)
}

// ContainerGet invokes HTTP GET on the named Swift Container.
func ContainerGet(accountName string, containerName string) (headers map[string][]string, objectList []string, err error) {
	return containerGetWithRetry(accountName, containerName)
}

// ContainerHead invokes HTTP HEAD on the named Swift Container.
func ContainerHead(accountName string, containerName string) (headers map[string][]string, err error) {
	return containerHeadWithRetry(accountName, containerName)
}

// ContainerPost invokes HTTP POST on the named Swift Container.
func ContainerPost(accountName string, containerName string, headers map[string][]string) (err error) {
	return containerPostWithRetry(accountName, containerName, headers)
}

// ContainerPut invokes HTTP PUT on the named Swift Container.
func ContainerPut(accountName string, containerName string, headers map[string][]string) (err error) {
	return containerPutWithRetry(accountName, containerName, headers)
}

// ObjectContentLength invokes HTTP HEAD on the named Swift Object and returns value of Content-Length Header.
func ObjectContentLength(accountName string, containerName string, objectName string) (length uint64, err error) {
	return objectContentLengthWithRetry(accountName, containerName, objectName)
}

// ObjectCopy asynchronously creates a copy of the named Swift Object Source called the named Swift Object Destination.
func ObjectCopy(srcAccountName string, srcContainerName string, srcObjectName string, dstAccountName string, dstContainerName string, dstObjectName string, chunkedCopyContext ChunkedCopyContext) (err error) {
	return objectCopy(srcAccountName, srcContainerName, srcObjectName, dstAccountName, dstContainerName, dstObjectName, chunkedCopyContext)
}

// ObjectDelete invokes HTTP DELETE on the named Swift Object.
func ObjectDelete(accountName string, containerName string, objectName string, operationOptions OperationOptions) (err error) {
	return objectDelete(accountName, containerName, objectName, operationOptions)
}

// ObjectFetchChunkedPutContext provisions a context to use for an HTTP PUT using "chunked" Transfer-Encoding on the named Swift Object.
// The useReserveForVolumeName string argument should be set to "" if normal chunked connection pool is to be used.
// If a reserved connection is to be used, note that there is only one per useReserveForVolumeName allowed at a time.
func ObjectFetchChunkedPutContext(accountName string, containerName string, objectName string, useReserveForVolumeName string) (chunkedPutContext ChunkedPutContext, err error) {
	return objectFetchChunkedPutContextWithRetry(accountName, containerName, objectName, useReserveForVolumeName)
}

// ObjectGet invokes HTTP GET on the named Swift Object for the specified byte range.
func ObjectGet(accountName string, containerName string, objectName string, offset uint64, length uint64) (buf []byte, err error) {
	return objectGetWithRetry(accountName, containerName, objectName, offset, length)
}

// ObjectHead invokes HTTP HEAD on the named Swift Object.
func ObjectHead(accountName string, containerName string, objectName string) (headers map[string][]string, err error) {
	return objectHeadWithRetry(accountName, containerName, objectName)
}

// ObjectLoad invokes HTTP GET on the named Swift Object for the entire object.
func ObjectLoad(accountName string, containerName string, objectName string) (buf []byte, err error) {
	return objectLoadWithRetry(accountName, containerName, objectName)
}

// ObjectPost invokes HTTP POST on the named Swift Object.
func ObjectPost(accountName string, containerName string, objectName string, headers map[string][]string) (err error) {
	return objectPostWithRetry(accountName, containerName, objectName, headers)
}

// ObjectRead invokes HTTP GET on the named Swift Object at the specified offset filling in the specified byte slice.
// Note that the byte slice must already have the desired length even though those bytes will be overwritten.
func ObjectRead(accountName string, containerName string, objectName string, offset uint64, buf []byte) (len uint64, err error) {
	return objectReadWithRetry(accountName, containerName, objectName, offset, buf)
}

// ObjectTail invokes HTTP GET on the named Swift Object with a byte range selecting the specified length of trailing bytes.
func ObjectTail(accountName string, containerName string, objectName string, length uint64) (buf []byte, err error) {
	return objectTailWithRetry(accountName, containerName, objectName, length)
}

// Number of chunked connections that are idle
func ChunkedConnectionFreeCnt() (freeChunkedConnections int64) {
	return chunkedConnectionFreeCnt()
}

// Number of non-chunked connections that are idle
func NonChunkedConnectionFreeCnt() (freeNonChunkedConnections int64) {
	return nonChunkedConnectionFreeCnt()
}
