package swiftclient

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swiftstack/conf"
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
	noAuthStringAddr         string
	noAuthTCPAddr            *net.TCPAddr
	timeout                  time.Duration // TODO: Currently not enforced
	retryLimit               uint16        // maximum retries
	retryDelay               time.Duration // delay before first retry
	retryExpBackoff          float64       // increase delay by this factor each try (exponential backoff)
	nilTCPConn               *net.TCPConn
	chunkedConnectionPool    chan *net.TCPConn
	nonChunkedConnectionPool chan *net.TCPConn
	maxIntAsUint64           uint64
	pendingDeletes           *pendingDeletesStruct
}

var globals globalsStruct

// Up reads the Swift configuration to enable subsequent communication.
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

	globals.retryDelay, err = confMap.FetchOptionValueDuration("SwiftClient", "RetryDelay")
	if nil != err || globals.retryDelay < 50*time.Millisecond || globals.retryDelay > 20*time.Second {
		// TODO: eventually, just return
		globals.retryDelay, err = time.ParseDuration("50ms")
		if nil != err {
			return
		}
	}

	var expBackoff uint32
	expBackoff, err = confMap.FetchOptionValueFloatScaledToUint32("SwiftClient", "RetryExpBackoff", 1000)
	if nil != err || expBackoff < 1000 || expBackoff > 3000 {
		// TODO: eventually, just return
		globals.retryExpBackoff = float64(expBackoff) / float64(1000)
		if nil != err {
			return
		}
	}

	globals.retryLimit, err = confMap.FetchOptionValueUint16("SwiftClient", "RetryLimit")
	if nil != err || globals.retryLimit <= 2 {
		// TODO: eventually, just return
		globals.retryLimit = 10
	}

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

// Down terminates all outstanding communications as part of process shutdown.
func Down() (err error) {
	globals.pendingDeletes.Lock()

	globals.pendingDeletes.shutdownInProgress = true

	globals.pendingDeletes.cond.Signal()

	globals.pendingDeletes.Unlock()

	globals.pendingDeletes.shutdownWaitGroup.Wait()

	err = nil

	return
}
