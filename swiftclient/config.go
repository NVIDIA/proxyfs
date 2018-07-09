package swiftclient

import (
	"container/list"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
)

type connectionStruct struct {
	connectionNonce uint64 // globals.connectionNonce at time connection was established
	tcpConn         *net.TCPConn
}

type connectionPoolStruct struct {
	sync.Mutex
	poolCapacity            uint16              // Set to SwiftClient.{|Non}ChunkedConnectionPoolSize
	poolInUse               uint16              // Active (i.e. not in LIFO) *connectionStruct's
	lifoIndex               uint16              // Indicates where next released *connectionStruct will go
	lifoOfActiveConnections []*connectionStruct // LIFO of available active connections
	waiters                 *list.List          // Contains sync.Cond's of waiters
	//                                             At time of connection release:
	//                                               If poolInUse < poolCapacity,
	//                                                 If keepAlive: connectionStruct pushed to lifoOfActiveConnections
	//                                               If poolInUse == poolCapacity,
	//                                                 If keepAlive: connectionStruct pushed to lifoOfActiveConnections
	//                                                 sync.Cond at front of waitors is awakened
	//                                               If poolInUse > poolCapacity,
	//                                                 poolInUse is decremented and connection is discarded
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
	starvationCallbackFrequency     time.Duration
	starvationUnderway              bool
	stavationResolvedChan           chan bool // Signal this chan to halt calls to starvationCallback
	starvationCallback              StarvationCallbackFunc
	maxIntAsUint64                  uint64
	chaosSendChunkFailureRate       uint64 // set only during testing
	chaosFetchChunkedPutFailureRate uint64 // set only during testing
}

var globals globalsStruct

// Up reads the Swift configuration to enable subsequent communication
func Up(confMap conf.ConfMap) (err error) {
	var (
		chunkedConnectionPoolSize    uint16
		freeConnectionIndex          uint16
		noAuthTCPPort                uint16
		noAuthIPAddr                 string
		nonChunkedConnectionPoolSize uint16
	)

	noAuthTCPPort, err = confMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		return
	}
	if uint16(0) == noAuthTCPPort {
		err = fmt.Errorf("SwiftClient.NoAuthTCPPort must be a non-zero uint16")
		return
	}

	noAuthIPAddr, err = confMap.FetchOptionValueString("SwiftClient", "NoAuthIPAddr")
	if nil != err {
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

	logger.Infof("SwiftClient.RetryLimit %d, SwiftClient.RetryDelay %4.3f sec, SwiftClient.RetryExpBackoff %2.1f",
		globals.retryLimit, float64(globals.retryDelay)/float64(time.Second), globals.retryExpBackoff)
	logger.Infof("SwiftClient.RetryLimitObject %d, SwiftClient.RetryDelayObject %4.3f sec, SwiftClient.RetryExpBackoffObject %2.1f",
		globals.retryLimitObject, float64(globals.retryDelayObject)/float64(time.Second),
		globals.retryExpBackoffObject)

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
	globals.nonChunkedConnectionPool.waiters = list.New()

	for freeConnectionIndex = uint16(0); freeConnectionIndex < nonChunkedConnectionPoolSize; freeConnectionIndex++ {
		globals.nonChunkedConnectionPool.lifoOfActiveConnections[freeConnectionIndex] = nil
	}

	globals.starvationCallbackFrequency, err = confMap.FetchOptionValueDuration("SwiftClient", "StarvationCallbackFrequency")
	if nil != err {
		// TODO: eventually, just return
		globals.starvationCallbackFrequency, err = time.ParseDuration("100ms")
		if nil != err {
			return
		}
	}

	globals.starvationUnderway = false
	globals.stavationResolvedChan = make(chan bool, 1)
	globals.starvationCallback = nil

	globals.maxIntAsUint64 = uint64(^uint(0) >> 1)

	return
}

// PauseAndContract pauses the swiftclient package and applies any removals from the supplied confMap
func PauseAndContract(confMap conf.ConfMap) (err error) {
	drainConnectionPools()
	globals.connectionNonce++
	err = nil
	return
}

// ExpandAndResume applies any additions from the supplied confMap and resumes the swiftclient package
func ExpandAndResume(confMap conf.ConfMap) (err error) {
	drainConnectionPools()
	globals.connectionNonce++
	err = nil
	return
}

// Down terminates all outstanding communications as part of process shutdown
func Down() (err error) {
	drainConnectionPools()
	globals.connectionNonce++
	err = nil
	return
}
