// Package swiftclient provides API access to the local Swift NoAuth Pipeline.
package swiftclient

import (
	"sync"
)

// ChunkedCopyContext provides a callback context to use for Object Copies.
//
// A returned chunkSize == 0 says to stop copying
type ChunkedCopyContext interface {
	BytesRemaining(bytesRemaining uint64) (chunkSize uint64)
}

// ChunkedPutContext provides a context to use for Object HTTP PUTs using "chunked" Transfer-Encoding.
type ChunkedPutContext interface {
	BytesPut() (bytesPut uint64, err error)                    // Report how many bytes have been sent via SendChunk() for this ChunkedPutContext
	Close() (err error)                                        // Finish the "chunked" HTTP PUT for this ChunkedPutContext
	Read(offset uint64, length uint64) (buf []byte, err error) // Read back bytes previously sent via SendChunk()
	Retry() (err error)                                        // Retry all the preceeding steps for this ChunkedPutContext
	SendChunk(buf []byte) (err error)                          // Send the supplied "chunk" via this ChunkedPutContext
}

// AccountDelete invokes HTTP DELETE on the named Swift Account.
func AccountDelete(accountName string) (err error) {
	return accountDelete(accountName)
}

// AccountGet invokes HTTP GET on the named Swift Account.
func AccountGet(accountName string) (headers map[string][]string, containerList []string, err error) {
	return accountGet(accountName)
}

// AccountHead invokes HTTP HEAD on the named Swift Account.
func AccountHead(accountName string) (headers map[string][]string, err error) {
	return accountHead(accountName)
}

// AccountPost invokes HTTP PUT on the named Swift Account.
func AccountPost(accountName string, headers map[string][]string) (err error) {
	return accountPost(accountName, headers)
}

// AccountPut invokes HTTP PUT on the named Swift Account.
func AccountPut(accountName string, headers map[string][]string) (err error) {
	return accountPut(accountName, headers)
}

// ContainerDelete invokes HTTP DELETE on the named Swift Container.
func ContainerDelete(accountName string, containerName string) (err error) {
	return containerDelete(accountName, containerName)
}

// ContainerGet invokes HTTP GET on the named Swift Container.
func ContainerGet(accountName string, containerName string) (headers map[string][]string, objectList []string, err error) {
	return containerGet(accountName, containerName)
}

// ContainerHead invokes HTTP HEAD on the named Swift Container.
func ContainerHead(accountName string, containerName string) (headers map[string][]string, err error) {
	return containerHead(accountName, containerName)
}

// ContainerPost invokes HTTP PUT on the named Swift Container.
func ContainerPost(accountName string, containerName string, headers map[string][]string) (err error) {
	return containerPost(accountName, containerName, headers)
}

// ContainerPut invokes HTTP PUT on the named Swift Container.
func ContainerPut(accountName string, containerName string, headers map[string][]string) (err error) {
	return containerPut(accountName, containerName, headers)
}

// ObjectContentLength invokes HTTP HEAD on the named Swift Object and returns value of Content-Length Header.
func ObjectContentLength(accountName string, containerName string, objectName string) (length uint64, err error) {
	return objectContentLength(accountName, containerName, objectName)
}

// ObjectCopy asynchronously creates a copy of the named Swift Object Source called the named Swift Object Destination.
func ObjectCopy(srcAccountName string, srcContainerName string, srcObjectName string, dstAccountName string, dstContainerName string, dstObjectName string, chunkedCopyContext ChunkedCopyContext) (err error) {
	return objectCopy(srcAccountName, srcContainerName, srcObjectName, dstAccountName, dstContainerName, dstObjectName, chunkedCopyContext)
}

// ObjectDeleteAsync asynchronously invokes HTTP DELETE on the named Swift Object.
//
// If wgPreCondition is not nil, the HTTP DELETE will proceed only after wgPreCondition.Done() returns.
// If wgPostSignal is not nil, following the HTTP DELETE, wgPostSignal.Done() will be called.
func ObjectDeleteAsync(accountName string, containerName string, objectName string, wgPreCondition *sync.WaitGroup, wgPostSignal *sync.WaitGroup) {
	objectDeleteAsync(accountName, containerName, objectName, wgPreCondition, wgPostSignal)
}

// ObjectDeleteSync synchronously invokes HTTP DELETE on the named Swift Object.
func ObjectDeleteSync(accountName string, containerName string, objectName string) (err error) {
	return objectDeleteSync(accountName, containerName, objectName)
}

// ObjectFetchChunkedPutContext provisions a context to use for an HTTP PUT using "chunked" Transfer-Encoding on the named Swift Object.
func ObjectFetchChunkedPutContext(accountName string, containerName string, objectName string) (chunkedPutContext ChunkedPutContext, err error) {
	return objectFetchChunkedPutContext(accountName, containerName, objectName)
}

// ObjectGet invokes HTTP GET on the named Swift Object for the specified byte range.
func ObjectGet(accountName string, containerName string, objectName string, offset uint64, length uint64) (buf []byte, err error) {
	return objectGetWithRetry_closure2(accountName, containerName, objectName, offset, length)
}

// ObjectHead invokes HTTP HEAD on the named Swift Object.
func ObjectHead(accountName string, containerName string, objectName string) (headers map[string][]string, err error) {
	return objectHead(accountName, containerName, objectName)
}

// ObjectLoad invokes HTTP GET on the named Swift Object for the entire object.
func ObjectLoad(accountName string, containerName string, objectName string) (buf []byte, err error) {
	return objectLoad(accountName, containerName, objectName)
}

// ObjectTail invokes HTTP GET on the named Swift Object with a byte range selecting the specified length of trailing bytes.
func ObjectTail(accountName string, containerName string, objectName string, length uint64) (buf []byte, objectLength int64, err error) {
	return objectTail(accountName, containerName, objectName, length)
}
