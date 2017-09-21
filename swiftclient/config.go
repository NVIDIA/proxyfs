package swiftclient

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
)

type pendingDeleteStruct struct {
	next           *pendingDeleteStruct
	accountName    string
	containerName  string
	objectName     string
	wgPreCondition *sync.WaitGroup
	wgPostSignal   *sync.WaitGroup
}

type pendingDeletesStruct struct {
	sync.Mutex
	armed              bool
	cond               *sync.Cond // Signal if adding 1st pendingDeleteStruct or shutting down
	head               *pendingDeleteStruct
	tail               *pendingDeleteStruct
	shutdownWaitGroup  sync.WaitGroup
	shutdownInProgress bool
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
	nilTCPConn                      *net.TCPConn
	chunkedConnectionPool           chan *net.TCPConn
	nonChunkedConnectionPool        chan *net.TCPConn
	maxIntAsUint64                  uint64
	pendingDeletes                  *pendingDeletesStruct
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
		nonChunkedConnectionPoolSize uint16
		pendingDeletes               *pendingDeletesStruct
	)

	noAuthTCPPort, err = confMap.FetchOptionValueUint16("SwiftClient", "NoAuthTCPPort")
	if nil != err {
		return
	}
	if uint16(0) == noAuthTCPPort {
		err = fmt.Errorf("SwiftClient.NoAuthTCPPort must be a non-zero uint16")
		return
	}

	globals.noAuthStringAddr = "127.0.0.1:" + strconv.Itoa(int(noAuthTCPPort))

	globals.noAuthTCPAddr, err = net.ResolveTCPAddr("tcp4", globals.noAuthStringAddr)
	if nil != err {
		return
	}

	globals.timeout, err = confMap.FetchOptionValueDuration("SwiftClient", "Timeout")
	if nil != err {
		return
	}

	// take care of both RetryLimit and RetryLimitObject at same time
	var retryLimitMap = map[string]*uint16{
		"RetryLimit":       &globals.retryLimit,
		"RetryLimitObject": &globals.retryLimitObject,
	}
	for name, ptr := range retryLimitMap {
		*ptr, err = confMap.FetchOptionValueUint16("SwiftClient", name)
		if nil != err {
			// TODO: once controller understands RetryDelay paramater, return if undefined
			// return
			*ptr = 21
		}
		if *ptr > 20 {
			logger.Warnf("config variable 'SwiftClient.%s' at %v is too big; changing to 5",
				name, *ptr)
			*ptr = 5
		}
	}

	// take care of both RetryDelay and RetryDelayObject at same time
	var retryDelayMap = map[string]*time.Duration{
		"RetryDelay":       &globals.retryDelay,
		"RetryDelayObject": &globals.retryDelayObject,
	}
	for name, ptr := range retryDelayMap {
		*ptr, err = confMap.FetchOptionValueDuration("SwiftClient", name)
		if nil != err {
			// TODO: once controller understands RetryDelay*
			// paramater, return if not defined
			*ptr = 0
		}
		if *ptr < 50*time.Millisecond || *ptr > 30*time.Second {
			logger.Warnf("config variable 'SwiftClient.%s' at %d msec is too big or too small; changing to 1s",
				name, *ptr/time.Millisecond)
			*ptr, err = time.ParseDuration("1s")
		}
	}

	// take care of both RetryExpBackoff and RetryExpBackoffObject at same time
	var retryExpBackoffMap = map[string]*float64{
		"RetryExpBackoff":       &globals.retryExpBackoff,
		"RetryExpBackoffObject": &globals.retryExpBackoffObject,
	}
	for name, ptr := range retryExpBackoffMap {
		var expBackoff uint32
		expBackoff, err = confMap.FetchOptionValueFloatScaledToUint32("SwiftClient", name, 1000)
		if nil != err {
			// TODO: once controller understands RetryExpBackoff*
			// paramater, return if not defined
			expBackoff = 0
		}
		*ptr = float64(expBackoff) / float64(1000)
		if *ptr < 0.5 || *ptr > 3.0 {
			var ebo float64 = 2.0
			if name == "RetryExpBackoff" {
				ebo = 1.2
			}
			logger.Warnf("config variable 'SwiftClient.%s' at %2.1f is too big or too small; changing to %2.1f",
				name, *ptr, ebo)
			*ptr = ebo
		}
	}
	logger.Infof("SwiftClient.RetryLimit %d, SwiftClient.RetryDelay %4.3f sec, SwiftClient.RetryExpBackoff %2.1f",
		globals.retryLimit, float64(globals.retryDelay)/float64(time.Second), globals.retryExpBackoff)
	logger.Infof("SwiftClient.RetryLimitObject %d, SwiftClient.RetryDelayObject %4.3f sec, SwiftClient.RetryExpBackoffObject %2.1f",
		globals.retryLimitObject, float64(globals.retryDelayObject)/float64(time.Second),
		globals.retryExpBackoffObject)

	globals.nilTCPConn = nil

	chunkedConnectionPoolSize, err = confMap.FetchOptionValueUint16("SwiftClient", "ChunkedConnectionPoolSize")
	if nil != err {
		return
	}
	if uint16(0) == chunkedConnectionPoolSize {
		err = fmt.Errorf("SwiftClient.ChunkedConnectionPoolSize must be a non-zero uint16")
		return
	}

	globals.chunkedConnectionPool = make(chan *net.TCPConn, chunkedConnectionPoolSize)

	for freeConnectionIndex = uint16(0); freeConnectionIndex < chunkedConnectionPoolSize; freeConnectionIndex++ {
		globals.chunkedConnectionPool <- globals.nilTCPConn
	}

	nonChunkedConnectionPoolSize, err = confMap.FetchOptionValueUint16("SwiftClient", "NonChunkedConnectionPoolSize")
	if nil != err {
		return
	}
	if uint16(0) == nonChunkedConnectionPoolSize {
		err = fmt.Errorf("SwiftClient.NonChunkedConnectionPoolSize must be a non-zero uint16")
		return
	}

	globals.nonChunkedConnectionPool = make(chan *net.TCPConn, nonChunkedConnectionPoolSize)

	for freeConnectionIndex = uint16(0); freeConnectionIndex < nonChunkedConnectionPoolSize; freeConnectionIndex++ {
		globals.nonChunkedConnectionPool <- globals.nilTCPConn
	}

	globals.maxIntAsUint64 = uint64(^uint(0) >> 1)

	pendingDeletes = &pendingDeletesStruct{
		armed:              false,
		head:               nil,
		tail:               nil,
		shutdownInProgress: false,
	}

	pendingDeletes.cond = sync.NewCond(pendingDeletes)
	pendingDeletes.shutdownWaitGroup.Add(1)

	globals.pendingDeletes = pendingDeletes

	go objectDeleteAsyncDaemon()

	pendingDeletes.Lock()

	for !pendingDeletes.armed {
		pendingDeletes.Unlock()
		time.Sleep(100 * time.Millisecond)
		pendingDeletes.Lock()
	}

	pendingDeletes.Unlock()

	return
}

// PauseAndContract pauses the swiftclient package and applies any removals from the supplied confMap
func PauseAndContract(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

// ExpandAndResume applies any additions from the supplied confMap and resumes the swiftclient package
func ExpandAndResume(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

// Down terminates all outstanding communications as part of process shutdown
func Down() (err error) {
	globals.pendingDeletes.Lock()

	globals.pendingDeletes.shutdownInProgress = true

	globals.pendingDeletes.cond.Signal()

	globals.pendingDeletes.Unlock()

	globals.pendingDeletes.shutdownWaitGroup.Wait()

	err = nil

	return
}
