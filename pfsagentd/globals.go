package main

import (
	"container/list"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/utils"
)

type configStruct struct {
	FUSEVolumeName          string
	FUSEMountPointPath      string // Unless starting with '/', relative to $CWD
	FUSEUnMountRetryDelay   time.Duration
	FUSEUnMountRetryCap     uint64
	SwiftAuthURL            string // If domain name is used, round-robin among all will be used
	SwiftAuthUser           string
	SwiftAuthKey            string
	SwiftAccountName        string // Must be a bi-modal account
	SwiftTimeout            time.Duration
	SwiftRetryLimit         uint64
	SwiftRetryDelay         time.Duration
	SwiftRetryExpBackoff    float64
	SwiftConnectionPoolSize uint64
	ReadCacheLineSize       uint64 // Aligned chunk of a LogSegment
	ReadCacheLineCount      uint64
	SharedFileLimit         uint64
	ExclusiveFileLimit      uint64
	MaxFlushSize            uint64
	MaxFlushTime            time.Duration
	ReadOnly                bool
	LogFilePath             string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole            bool
	TraceEnabled            bool
	AttrDuration            time.Duration
	AttrBlockSize           uint64
	LookupEntryDuration     time.Duration
	ReaddirMaxEntries       uint64
}

type fileInodeLockRequestStruct struct {
	sync.WaitGroup
	fileInode      *fileInodeStruct
	exclusive      bool
	holdersElement *list.Element // only maintained if exclusive == false
	waitersElement *list.Element // == nil if lock granted; != nil if waiting
}

type fileInodeLeaseStateType uint32

const (
	fileInodeLeaseStateNone fileInodeLeaseStateType = iota
	fileInodeLeaseStateSharedRequested
	fileInodeLeaseStateSharedGranted
	fileInodeLeaseStateSharedReleasing
	fileInodeLeaseStateSharedPromoting
	fileInodeLeaseStateExclusiveRequested
	fileInodeLeaseStateExclusiveGranted
	fileInodeLeaseStateExclusiveDemoting
	fileInodeLeaseStateExclusiveReleasing
)

type fileInodeLeaseRequestType uint32

const (
	fileInodeLeaseRequestShutdown fileInodeLeaseRequestType = iota
	fileInodeLeaseRequestShared
	fileInodeLeaseRequestExclusive
	fileInodeLeaseRequestDemote
	fileInodeLeaseRequestRelease
)

type fileInodeLeaseRequestStruct struct {
	sync.WaitGroup
	fileInode                      *fileInodeStruct
	requestType                    fileInodeLeaseRequestType
	delayedLeaseRequestListElement *list.Element
}

type fileInodeStruct struct {
	inode.InodeNumber
	references          uint64
	leaseState          fileInodeLeaseStateType
	sharedLockHolders   *list.List                  // Elements are fileInodeLockRequestStructs.holdersElement's
	exclusiveLockHolder *fileInodeLockRequestStruct // == nil if not exclusively held
	lockWaiters         *list.List                  // Front() is oldest fileInodeLockRequestStruct.waitersElement
	cacheLRUElement     *list.Element               // Element on one of globals.{unlocked|shared|exclusive}FileInodeCacheLRU
	//                                                   On globals.unleasedFileInodeCacheLRU       if leaseState one of:
	//                                                     fileInodeLeaseStateNone
	//                                                     fileInodeLeaseStateSharedReleasing
	//                                                     fileInodeLeaseStateExclusiveReleasing
	//                                                   On globals.sharedLeaseFileInodeCacheLRU    if leaseState one of:
	//                                                     fileInodeLeaseStateSharedRequested
	//                                                     fileInodeLeaseStateSharedGranted
	//                                                     fileInodeLeaseStateExclusiveDemoting
	//                                                   On globals.exclusiveLeaseFileInodeCacheLRU if leaseState one of:
	//                                                     fileInodeLeaseStateSharedPromoting
	//                                                     fileInodeLeaseStateExclusiveRequested
	//                                                     fileInodeLeaseStateExclusiveGranted
}

type handleStruct struct {
	inode.InodeNumber
	prevDirEntLocation int64 // -1 is used if fuse.ReadRequest on DirInode specifies Offset == 0
	//                          Otherwise, just assumes caller wants to keep going...
}

type globalsStruct struct {
	sync.Mutex
	config                          configStruct
	logFile                         *os.File // == nil if configStruct.LogFilePath == ""
	httpClient                      *http.Client
	retryDelay                      []time.Duration
	swiftAuthWaitGroup              *sync.WaitGroup
	swiftAuthToken                  string
	swiftAccountURL                 string // swiftStorageURL with AccountName forced to config.SwiftAccountName
	mountID                         jrpcfs.MountIDAsString
	rootDirInodeNumber              uint64
	fuseConn                        *fuse.Conn
	jrpcLastID                      uint64
	fileInodeMap                    map[inode.InodeNumber]*fileInodeStruct
	leaseRequestChan                chan *fileInodeLeaseRequestStruct
	unleasedFileInodeCacheLRU       *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
	sharedLeaseFileInodeCacheLRU    *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
	exclusiveLeaseFileInodeCacheLRU *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
	lastHandleID                    fuse.HandleID
	handleTable                     map[fuse.HandleID]*handleStruct
}

var globals globalsStruct

func initializeGlobals(confMap conf.ConfMap) {
	var (
		configJSONified  string
		customTransport  *http.Transport
		defaultTransport *http.Transport
		err              error
		nextRetryDelay   time.Duration
		ok               bool
		retryIndex       uint64
	)

	// Default logging related globals

	globals.config.LogFilePath = ""
	globals.config.LogToConsole = false
	globals.logFile = nil

	// Process resultant confMap

	globals.config.FUSEVolumeName, err = confMap.FetchOptionValueString("Agent", "FUSEVolumeName")
	if nil != err {
		logFatal(err)
	}

	globals.config.FUSEMountPointPath, err = confMap.FetchOptionValueString("Agent", "FUSEMountPointPath")
	if nil != err {
		logFatal(err)
	}

	globals.config.FUSEUnMountRetryDelay, err = confMap.FetchOptionValueDuration("Agent", "FUSEUnMountRetryDelay")
	if nil != err {
		logFatal(err)
	}

	globals.config.FUSEUnMountRetryCap, err = confMap.FetchOptionValueUint64("Agent", "FUSEUnMountRetryCap")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftAuthURL, err = confMap.FetchOptionValueString("Agent", "SwiftAuthURL")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftAuthUser, err = confMap.FetchOptionValueString("Agent", "SwiftAuthUser")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftAuthKey, err = confMap.FetchOptionValueString("Agent", "SwiftAuthKey")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftAccountName, err = confMap.FetchOptionValueString("Agent", "SwiftAccountName")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftTimeout, err = confMap.FetchOptionValueDuration("Agent", "SwiftTimeout")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftRetryLimit, err = confMap.FetchOptionValueUint64("Agent", "SwiftRetryLimit")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftRetryDelay, err = confMap.FetchOptionValueDuration("Agent", "SwiftRetryDelay")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftRetryExpBackoff, err = confMap.FetchOptionValueFloat64("Agent", "SwiftRetryExpBackoff")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftConnectionPoolSize, err = confMap.FetchOptionValueUint64("Agent", "SwiftConnectionPoolSize")
	if nil != err {
		logFatal(err)
	}

	globals.config.ReadCacheLineSize, err = confMap.FetchOptionValueUint64("Agent", "ReadCacheLineSize")
	if nil != err {
		logFatal(err)
	}

	globals.config.ReadCacheLineCount, err = confMap.FetchOptionValueUint64("Agent", "ReadCacheLineCount")
	if nil != err {
		logFatal(err)
	}

	globals.config.SharedFileLimit, err = confMap.FetchOptionValueUint64("Agent", "SharedFileLimit")
	if nil != err {
		logFatal(err)
	}

	globals.config.ExclusiveFileLimit, err = confMap.FetchOptionValueUint64("Agent", "ExclusiveFileLimit")
	if nil != err {
		logFatal(err)
	}

	globals.config.MaxFlushSize, err = confMap.FetchOptionValueUint64("Agent", "MaxFlushSize")
	if nil != err {
		logFatal(err)
	}

	globals.config.MaxFlushTime, err = confMap.FetchOptionValueDuration("Agent", "MaxFlushTime")
	if nil != err {
		logFatal(err)
	}

	globals.config.ReadOnly, err = confMap.FetchOptionValueBool("Agent", "ReadOnly")
	if nil != err {
		logFatal(err)
	}

	err = confMap.VerifyOptionValueIsEmpty("Agent", "LogFilePath")
	if nil == err {
		globals.config.LogFilePath = ""
	} else {
		globals.config.LogFilePath, err = confMap.FetchOptionValueString("Agent", "LogFilePath")
		if nil != err {
			logFatal(err)
		}
	}

	globals.config.LogToConsole, err = confMap.FetchOptionValueBool("Agent", "LogToConsole")
	if nil != err {
		logFatal(err)
	}

	globals.config.TraceEnabled, err = confMap.FetchOptionValueBool("Agent", "TraceEnabled")
	if nil != err {
		logFatal(err)
	}

	globals.config.AttrDuration, err = confMap.FetchOptionValueDuration("Agent", "AttrDuration")
	if nil != err {
		logFatal(err)
	}

	globals.config.AttrDuration, err = confMap.FetchOptionValueDuration("Agent", "AttrDuration")
	if nil != err {
		logFatal(err)
	}

	globals.config.AttrBlockSize, err = confMap.FetchOptionValueUint64("Agent", "AttrBlockSize")
	if nil != err {
		logFatal(err)
	}
	if (0 == globals.config.AttrBlockSize) || (math.MaxUint32 < globals.config.AttrBlockSize) {
		logFatalf("AttrBlockSize must be non-zero and fit in a uint32")
	}

	globals.config.LookupEntryDuration, err = confMap.FetchOptionValueDuration("Agent", "LookupEntryDuration")
	if nil != err {
		logFatal(err)
	}

	globals.config.ReaddirMaxEntries, err = confMap.FetchOptionValueUint64("Agent", "ReaddirMaxEntries")
	if nil != err {
		logFatal(err)
	}

	configJSONified = utils.JSONify(globals.config, true)

	logInfof("\n%s", configJSONified)

	defaultTransport, ok = http.DefaultTransport.(*http.Transport)
	if !ok {
		log.Fatalf("http.DefaultTransport not a *http.Transport")
	}

	customTransport = &http.Transport{ // Up-to-date as of Golang 1.11
		Proxy:                  defaultTransport.Proxy,
		DialContext:            defaultTransport.DialContext,
		Dial:                   defaultTransport.Dial,
		DialTLS:                defaultTransport.DialTLS,
		TLSClientConfig:        defaultTransport.TLSClientConfig,
		TLSHandshakeTimeout:    defaultTransport.TLSHandshakeTimeout,
		DisableKeepAlives:      false,
		DisableCompression:     defaultTransport.DisableCompression,
		MaxIdleConns:           int(globals.config.SwiftConnectionPoolSize),
		MaxIdleConnsPerHost:    int(globals.config.SwiftConnectionPoolSize),
		MaxConnsPerHost:        int(globals.config.SwiftConnectionPoolSize),
		IdleConnTimeout:        defaultTransport.IdleConnTimeout,
		ResponseHeaderTimeout:  defaultTransport.ResponseHeaderTimeout,
		ExpectContinueTimeout:  defaultTransport.ExpectContinueTimeout,
		TLSNextProto:           defaultTransport.TLSNextProto,
		ProxyConnectHeader:     defaultTransport.ProxyConnectHeader,
		MaxResponseHeaderBytes: defaultTransport.MaxResponseHeaderBytes,
	}

	globals.httpClient = &http.Client{
		Transport: customTransport,
		Timeout:   globals.config.SwiftTimeout,
	}

	globals.retryDelay = make([]time.Duration, globals.config.SwiftRetryLimit)

	nextRetryDelay = globals.config.SwiftRetryDelay

	for retryIndex = 0; retryIndex < globals.config.SwiftRetryLimit; retryIndex++ {
		globals.retryDelay[retryIndex] = nextRetryDelay
		nextRetryDelay = time.Duration(float64(nextRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	globals.swiftAuthWaitGroup = nil
	globals.swiftAuthToken = ""
	globals.swiftAccountURL = ""

	updateAuthTokenAndAccountURL()

	globals.jrpcLastID = 1

	globals.fileInodeMap = make(map[inode.InodeNumber]*fileInodeStruct)

	globals.leaseRequestChan = make(chan *fileInodeLeaseRequestStruct)

	go leaseDaemon()

	globals.unleasedFileInodeCacheLRU = list.New()
	globals.sharedLeaseFileInodeCacheLRU = list.New()
	globals.exclusiveLeaseFileInodeCacheLRU = list.New()

	globals.lastHandleID = 0
	globals.handleTable = make(map[fuse.HandleID]*handleStruct)
}

func uninitializeGlobals() {
	var (
		leaseRequest *fileInodeLeaseRequestStruct
	)

	leaseRequest = &fileInodeLeaseRequestStruct{
		fileInode:   nil,
		requestType: fileInodeLeaseRequestShutdown,
	}

	leaseRequest.Add(1)
	globals.leaseRequestChan <- leaseRequest
	leaseRequest.Wait()

	globals.logFile = nil
	globals.httpClient = nil
	globals.retryDelay = nil
	globals.swiftAuthWaitGroup = nil
	globals.swiftAuthToken = ""
	globals.swiftAccountURL = ""
	globals.fuseConn = nil
	globals.jrpcLastID = 0
	globals.fileInodeMap = nil
	globals.leaseRequestChan = nil
	globals.unleasedFileInodeCacheLRU = nil
	globals.sharedLeaseFileInodeCacheLRU = nil
	globals.exclusiveLeaseFileInodeCacheLRU = nil
	globals.lastHandleID = 0
	globals.handleTable = nil
}
