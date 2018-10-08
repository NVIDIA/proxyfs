package swiftclient

import (
	"container/list"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/transitions"
)

type connectionStruct struct {
	connectionNonce      uint64 // globals.connectionNonce at time connection was established
	tcpConn              *net.TCPConn
	reserveForVolumeName string
}

type connectionPoolStruct struct {
	sync.Mutex
	poolCapacity            uint16              // Set to SwiftClient.{|Non}ChunkedConnectionPoolSize
	poolInUse               uint16              // Active (i.e. not in LIFO) *connectionStruct's
	lifoIndex               uint16              // Indicates where next released *connectionStruct will go
	lifoOfActiveConnections []*connectionStruct // LIFO of available active connections
	numWaiters              uint64              // Count of the number of blocked acquirers
	waiters                 *list.List          // Contains sync.Cond's of waiters
	//                                             At time of connection release:
	//                                               If poolInUse < poolCapacity,
	//                                                 If keepAlive: connectionStruct pushed to lifoOfActiveConnections
	//                                               If poolInUse == poolCapacity,
	//                                                 If keepAlive: connectionStruct pushed to lifoOfActiveConnections
	//                                                 sync.Cond at front of waitors is awakened
	//                                               If poolInUse > poolCapacity,
	//                                                 poolInUse is decremented and connection is discarded
	//                                             Note: waiters list is not used for when in starvation mode
	//                                                   for the chunkedConnectionPool if a starvationCallback
	//                                                   has been provided
}

// Used to track client request times (client's of swiftclient) and Swift server
// request times using bucketized statistics
//
type requestTimeStatistic struct {
	bucketstats.BucketLog2Round
}

// Used to track retries using an average, where the count increases by 1 for
// each retry and the total increases by 1 for each successful retry
//
type requestRetryStatistic struct {
	bucketstats.Average
}

// Used to track client request failures
//
type requestFailureStatistic struct {
	bucketstats.Total
}

type globalsStruct struct {
	noAuthStringAddr                string
	noAuthTCPAddr                   *net.TCPAddr
	timeout                         time.Duration // TODO: Currently not enforced
	retryLimit                      uint16        // maximum retries
	retryLimitObject                uint16        // maximum retries for object ops
	retryDelay                      time.Duration // delay before first retry
	retryDelayObject                time.Duration // delay before first retry for object ops
	retryExpBackoff                 float64       // increase delay by this factor each try (exponential backoff)
	retryExpBackoffObject           float64       // increase delay by this factor each try for object ops
	connectionNonce                 uint64        // incremented each SIGHUP... older connections always closed
	chunkedConnectionPool           connectionPoolStruct
	nonChunkedConnectionPool        connectionPoolStruct
	starvationCallback              StarvationCallbackFunc
	starvationCallbackSerializer    sync.Mutex
	reservedChunkedConnection       map[string]*connectionStruct // Key: VolumeName
	reservedChunkedConnectionMutex  sync.Mutex
	maxIntAsUint64                  uint64
	checksumChunkedPutChunks        bool   // compute and verify checksums (for testing)
	chaosFetchChunkedPutFailureRate uint64 // set only during testing
	chaosSendChunkFailureRate       uint64 // set only during testing
	chaosCloseChunkFailureRate      uint64 // set only during testing

	// statistics for requests to swiftclient
	AccountDeleteUsec              bucketstats.BucketLog2Round     // bucketized by time
	AccountGetUsec                 bucketstats.BucketLog2Round     // bucketized by time
	AccountHeadUsec                bucketstats.BucketLog2Round     // bucketized by time
	AccountPostUsec                bucketstats.BucketLog2Round     // bucketized by time
	AccountPutUsec                 bucketstats.BucketLog2Round     // bucketized by time
	ContainerDeleteUsec            bucketstats.BucketLog2Round     // bucketized by time
	ContainerGetUsec               bucketstats.BucketLog2Round     // bucketized by time
	ContainerHeadUsec              bucketstats.BucketLog2Round     // bucketized by time
	ContainerPostUsec              bucketstats.BucketLog2Round     // bucketized by time
	ContainerPutUsec               bucketstats.BucketLog2Round     // bucketized by time
	ObjectContentLengthUsec        bucketstats.BucketLog2Round     // bucketized by time
	ObjectCopyUsec                 bucketstats.BucketLog2Round     // bucketized by time
	ObjectDeleteUsec               bucketstats.BucketLog2Round     // bucketized by time
	ObjectGetUsec                  bucketstats.BucketLog2Round     // bucketized by time
	ObjectGetBytes                 bucketstats.BucketLog2Round     // bucketized by byte count
	ObjectHeadUsec                 bucketstats.BucketLog2Round     // bucketized by time
	ObjectLoadUsec                 bucketstats.BucketLog2Round     // bucketized by time
	ObjectLoadBytes                bucketstats.BucketLog2Round     // bucketized by byte count
	ObjectReadUsec                 bucketstats.BucketLog2Round     // bucketized by time
	ObjectReadBytes                bucketstats.BucketLog2Round     // bucketized by byte count
	ObjectTailUsec                 bucketstats.BucketLog2Round     // bucketized by time
	ObjectTailBytes                bucketstats.BucketLog2Round     // bucketized by byte count
	ObjectNonChunkedFreeConnection bucketstats.BucketLogRoot2Round // free non-chunked connections (at acquire time)
	ObjectPutCtxtFetchUsec         bucketstats.BucketLog2Round     // bucketized by time
	ObjectPutCtxtFreeConnection    bucketstats.BucketLogRoot2Round // free chunked put connections (at acquite time)
	ObjectPutCtxtBytesPut          bucketstats.Total               // number of calls to BytesPut() query
	ObjectPutCtxtCloseUsec         bucketstats.BucketLog2Round     // bucketized by time
	ObjectPutCtxtReadBytes         bucketstats.BucketLog2Round     // bucketized by bytes read
	ObjectPutCtxtSendChunkUsec     bucketstats.BucketLog2Round     // bucketized by time
	ObjectPutCtxtSendChunkBytes    bucketstats.BucketLog2Round     // bucketized by byte count
	ObjectPutCtxtBytes             bucketstats.BucketLog2Round     // bucketized by total bytes put
	ObjectPutCtxtFetchToCloseUsec  bucketstats.BucketLog2Round     // Fetch returns to Close called time

	// client request failures
	AccountDeleteFailure       bucketstats.Total
	AccountGetFailure          bucketstats.Total
	AccountHeadFailure         bucketstats.Total
	AccountPostFailure         bucketstats.Total
	AccountPutFailure          bucketstats.Total
	ContainerDeleteFailure     bucketstats.Total
	ContainerGetFailure        bucketstats.Total
	ContainerHeadFailure       bucketstats.Total
	ContainerPostFailure       bucketstats.Total
	ContainerPutFailure        bucketstats.Total
	ObjectContentLengthFailure bucketstats.Total
	ObjectDeleteFailure        bucketstats.Total
	ObjectGetFailure           bucketstats.Total
	ObjectHeadFailure          bucketstats.Total
	ObjectLoadFailure          bucketstats.Total
	ObjectReadFailure          bucketstats.Total
	ObjectTailFailure          bucketstats.Total
	ObjectPutCtxtFetchFailure  bucketstats.Total
	ObjectPutCtxtCloseFailure  bucketstats.Total

	// statistics for swiftclient requests to Swift
	SwiftAccountDeleteUsec       bucketstats.BucketLog2Round // bucketized by time
	SwiftAccountGetUsec          bucketstats.BucketLog2Round // bucketized by time
	SwiftAccountHeadUsec         bucketstats.BucketLog2Round // bucketized by time
	SwiftAccountPostUsec         bucketstats.BucketLog2Round // bucketized by time
	SwiftAccountPutUsec          bucketstats.BucketLog2Round // bucketized by time
	SwiftContainerDeleteUsec     bucketstats.BucketLog2Round // bucketized by time
	SwiftContainerGetUsec        bucketstats.BucketLog2Round // bucketized by time
	SwiftContainerHeadUsec       bucketstats.BucketLog2Round // bucketized by time
	SwiftContainerPostUsec       bucketstats.BucketLog2Round // bucketized by time
	SwiftContainerPutUsec        bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectContentLengthUsec bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectDeleteUsec        bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectGetUsec           bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectHeadUsec          bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectLoadUsec          bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectReadUsec          bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectTailUsec          bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectPutCtxtFetchUsec  bucketstats.BucketLog2Round // bucketized by time
	SwiftObjectPutCtxtCloseUsec  bucketstats.BucketLog2Round // bucketized by time

	// statistics for retries to Swift, Count() is the number of retries and
	// Total() is the number successful
	SwiftAccountDeleteRetryOps       bucketstats.Average
	SwiftAccountGetRetryOps          bucketstats.Average
	SwiftAccountHeadRetryOps         bucketstats.Average
	SwiftAccountPostRetryOps         bucketstats.Average
	SwiftAccountPutRetryOps          bucketstats.Average
	SwiftContainerDeleteRetryOps     bucketstats.Average
	SwiftContainerGetRetryOps        bucketstats.Average
	SwiftContainerHeadRetryOps       bucketstats.Average
	SwiftContainerPostRetryOps       bucketstats.Average
	SwiftContainerPutRetryOps        bucketstats.Average
	SwiftObjectContentLengthRetryOps bucketstats.Average
	SwiftObjectDeleteRetryOps        bucketstats.Average
	SwiftObjectGetRetryOps           bucketstats.Average
	SwiftObjectHeadRetryOps          bucketstats.Average
	SwiftObjectLoadRetryOps          bucketstats.Average
	SwiftObjectReadRetryOps          bucketstats.Average
	SwiftObjectTailRetryOps          bucketstats.Average
	SwiftObjectPutCtxtFetchRetryOps  bucketstats.Average
	SwiftObjectPutCtxtCloseRetryOps  bucketstats.Average
}

var globals globalsStruct

func init() {
	transitions.Register("swiftclient", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	var (
		chunkedConnectionPoolSize    uint16
		freeConnectionIndex          uint16
		noAuthIPAddr                 string
		noAuthTCPPort                uint16
		nonChunkedConnectionPoolSize uint16
	)

	// register the bucketstats statistics tracked here
	bucketstats.Register("proxyfs.swiftclient", "", &globals)

	noAuthIPAddr, err = confMap.FetchOptionValueString("SwiftClient", "NoAuthIPAddr")
	if nil != err {
		noAuthIPAddr = "127.0.0.1" // TODO: Eventually just return
	}

	noAuthTCPPort, err = confMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		return
	}
	if uint16(0) == noAuthTCPPort {
		err = fmt.Errorf("SwiftClient.NoAuthTCPPort must be a non-zero uint16")
		return
	}

	globals.noAuthStringAddr = noAuthIPAddr + ":" + strconv.Itoa(int(noAuthTCPPort))

	globals.noAuthTCPAddr, err = net.ResolveTCPAddr("tcp4", globals.noAuthStringAddr)
	if nil != err {
		return
	}

	globals.timeout, err = confMap.FetchOptionValueDuration("SwiftClient", "Timeout")
	if nil != err {
		return
	}

	globals.retryLimit, err = confMap.FetchOptionValueUint16("SwiftClient", "RetryLimit")
	if nil != err {
		return
	}
	globals.retryLimitObject, err = confMap.FetchOptionValueUint16("SwiftClient", "RetryLimitObject")
	if nil != err {
		return
	}

	globals.retryDelay, err = confMap.FetchOptionValueDuration("SwiftClient", "RetryDelay")
	if nil != err {
		return
	}
	globals.retryDelayObject, err = confMap.FetchOptionValueDuration("SwiftClient", "RetryDelayObject")
	if nil != err {
		return
	}

	globals.retryExpBackoff, err = confMap.FetchOptionValueFloat64("SwiftClient", "RetryExpBackoff")
	if nil != err {
		return
	}
	globals.retryExpBackoffObject, err = confMap.FetchOptionValueFloat64("SwiftClient", "RetryExpBackoffObject")
	if nil != err {
		return
	}
	globals.checksumChunkedPutChunks, err = confMap.FetchOptionValueBool("SwiftClient", "ChecksumChunkedPutChunks")
	if nil != err {
		globals.checksumChunkedPutChunks = false
	}

	logger.Infof("SwiftClient.RetryLimit %d, SwiftClient.RetryDelay %4.3f sec, SwiftClient.RetryExpBackoff %2.1f",
		globals.retryLimit, float64(globals.retryDelay)/float64(time.Second), globals.retryExpBackoff)
	logger.Infof("SwiftClient.RetryLimitObject %d, SwiftClient.RetryDelayObject %4.3f sec, SwiftClient.RetryExpBackoffObject %2.1f",
		globals.retryLimitObject, float64(globals.retryDelayObject)/float64(time.Second),
		globals.retryExpBackoffObject)

	checksums := "disabled"
	if globals.checksumChunkedPutChunks {
		checksums = "enabled"
	}
	logger.Infof("SwiftClient.ChecksumChunkedPutChunks %s\n", checksums)

	globals.connectionNonce = 0

	chunkedConnectionPoolSize, err = confMap.FetchOptionValueUint16("SwiftClient", "ChunkedConnectionPoolSize")
	if nil != err {
		return
	}
	if uint16(0) == chunkedConnectionPoolSize {
		err = fmt.Errorf("SwiftClient.ChunkedConnectionPoolSize must be a non-zero uint16")
		return
	}

	globals.chunkedConnectionPool.poolCapacity = chunkedConnectionPoolSize
	globals.chunkedConnectionPool.poolInUse = 0
	globals.chunkedConnectionPool.lifoIndex = 0
	globals.chunkedConnectionPool.lifoOfActiveConnections = make([]*connectionStruct, chunkedConnectionPoolSize)
	globals.chunkedConnectionPool.numWaiters = 0
	globals.chunkedConnectionPool.waiters = list.New()

	for freeConnectionIndex = uint16(0); freeConnectionIndex < chunkedConnectionPoolSize; freeConnectionIndex++ {
		globals.chunkedConnectionPool.lifoOfActiveConnections[freeConnectionIndex] = nil
	}

	nonChunkedConnectionPoolSize, err = confMap.FetchOptionValueUint16("SwiftClient", "NonChunkedConnectionPoolSize")
	if nil != err {
		return
	}
	if uint16(0) == nonChunkedConnectionPoolSize {
		err = fmt.Errorf("SwiftClient.NonChunkedConnectionPoolSize must be a non-zero uint16")
		return
	}

	globals.nonChunkedConnectionPool.poolCapacity = nonChunkedConnectionPoolSize
	globals.nonChunkedConnectionPool.poolInUse = 0
	globals.nonChunkedConnectionPool.lifoIndex = 0
	globals.nonChunkedConnectionPool.lifoOfActiveConnections = make([]*connectionStruct, nonChunkedConnectionPoolSize)
	globals.nonChunkedConnectionPool.numWaiters = 0
	globals.nonChunkedConnectionPool.waiters = list.New()

	for freeConnectionIndex = uint16(0); freeConnectionIndex < nonChunkedConnectionPoolSize; freeConnectionIndex++ {
		globals.nonChunkedConnectionPool.lifoOfActiveConnections[freeConnectionIndex] = nil
	}

	globals.starvationCallback = nil

	globals.reservedChunkedConnection = make(map[string]*connectionStruct)

	globals.maxIntAsUint64 = uint64(^uint(0) >> 1)

	return
}

func (dummy *globalsStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

func (dummy *globalsStruct) Signaled(confMap conf.ConfMap) (err error) {
	drainConnections()
	globals.connectionNonce++
	err = nil
	return
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	drainConnections()
	globals.connectionNonce++
	bucketstats.UnRegister("proxyfs.swiftclient", "")

	err = nil
	return
}
