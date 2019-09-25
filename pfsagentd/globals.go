package main

import (
	"container/list"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"bazil.org/fuse"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/utils"
)

type configStruct struct {
	FUSEVolumeName               string
	FUSEMountPointPath           string // Unless starting with '/', relative to $CWD
	FUSEUnMountRetryDelay        time.Duration
	FUSEUnMountRetryCap          uint64
	SwiftAuthURL                 string // If domain name is used, round-robin among all will be used
	SwiftAuthUser                string
	SwiftAuthKey                 string
	SwiftAccountName             string // Must be a bi-modal account
	SwiftTimeout                 time.Duration
	SwiftRetryLimit              uint64
	SwiftRetryDelay              time.Duration
	SwiftRetryExpBackoff         float64
	SwiftConnectionPoolSize      uint64
	FetchExtentsFromFileOffset   uint64
	FetchExtentsBeforeFileOffset uint64
	ReadCacheLineSize            uint64 // Aligned chunk of a LogSegment
	ReadCacheLineCount           uint64
	SharedFileLimit              uint64
	ExclusiveFileLimit           uint64
	DirtyFileLimit               uint64
	MaxFlushSize                 uint64
	MaxFlushTime                 time.Duration
	ReadOnly                     bool
	LogFilePath                  string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole                 bool
	TraceEnabled                 bool
	HTTPServerIPAddr             string
	HTTPServerTCPPort            uint16
	AttrDuration                 time.Duration
	AttrBlockSize                uint64
	LookupEntryDuration          time.Duration
	ReaddirMaxEntries            uint64
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

// singleObjectExtentStruct is used for chunkedPutContextStruct.extentMap.
//
type singleObjectExtentStruct struct {
	fileOffset   uint64 // Key in chunkedPutContextStruct.extentMap
	objectOffset uint64
	length       uint64
}

// singleObjectExtentWithLinkStruct is used to represent a ReadPlanStep. A chunkedPutContext == nil
// indicates a zero-filled extent rather that a read from a LogSegment not yet persisted by Swift.
//
type singleObjectExtentWithLinkStruct struct {
	fileOffset        uint64
	objectOffset      uint64
	length            uint64
	chunkedPutContext *chunkedPutContextStruct // If == nil, implies a zero-filled extent/ReadPlanStep
}

// multiObjectExtentStruct is used for both the fileInodeStruct.extentMap as well
// as for representing a ReadPlanStep. In this latter case, an objectName == ""
// indicates a zero-filled extent rather than a read from a LogSegment already
// persisted by Swift.
//
type multiObjectExtentStruct struct {
	fileOffset    uint64 // Key in fileInodeStruct.extentMap
	containerName string
	objectName    string // If == "", implies a zero-filled extent/ReadPlanStep
	objectOffset  uint64
	length        uint64
}

const (
	chunkedPutContextStateOpen    uint8 = iota // Initial state indicating Chunked PUT is available to send a chunk
	chunkedPutContextStateClosing              // After a zero-length chunk is sent to close the Chunked PUT... awaiting http.StatusCreated
	chunkedPutContextStateClosed               // Chunked PUT received an http.StatusCreated...
	//                                              but we cannot yet merge it's ExtentMap updates because
	//                                              an as-yet un-closed Chunked PUT needs to do so first
)

type chunkedPutContextStruct struct {
	sync.WaitGroup                    //          Used to await completion of performChunkedPut goroutine
	containerName  string             //
	objectName     string             //
	extentMap      sortedmap.LLRBTree //          Key == singleObjectExtentStruct.fileOffset; Value == *singleObjectExtentStruct
	fileSize       uint64             //          Last (most recent) chunkedPutContextStruct on fileInode.chunkedPutList may have
	//                                              updated fileSize affecting reads while writing to read beyond fileInode.extentMapFileSize
	buf                   []byte           //
	chunkedPutListElement *list.Element    //     FIFO Element of fileInodeStruct.chunkedPutList
	fileInode             *fileInodeStruct //
	state                 uint8            //     One of chunkedPutContextState{Open|Closing|Closed}
	pos                   int              //     ObjectOffset just after last sent chunk
	sendChan              chan bool        //     Wake-up sendDaemon with a new chunk or to flush
	wakeChan              chan bool        //     Wake-up Read callback to respond with a chunk and/or return EOF
	flushRequested        bool             //     Set to remember that a flush has been requested of *chunkedPutContextStruct.Read()
}

const (
	chunkedPutContextSendChanBufferSize = 16
	chunkedPutContextWakeChanBufferSize = 16
)

type fileInodeStruct struct {
	sync.WaitGroup //                                  Used to await completion of all chunkedPutContext's
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
	extentMapFileSize         uint64             //        FileSize covered by .extentMap (.chunkedPutList may extend)
	extentMap                 sortedmap.LLRBTree //    Key == multiObjectExtentStruct.fileOffset; Value == *multiObjectExtentStruct
	chunkedPutList            *list.List         //    FIFO List of chunkedPutContextStruct's
	flushInProgress           bool               //    Serializes (& singularizes) explicit Flush requests
	chunkedPutFlushWaiterList *list.List         //    List of *sync.WaitGroup's for those awaiting an explicit Flush
	//                                                   Note: These waiters cannot be holding fileInodeStruct.Lock
	dirtyListElement *list.Element //                  Element on globals.fileInodeDirtyList (or nil)
}

type handleStruct struct {
	inode.InodeNumber
	prevDirEntLocation int64 // -1 is used if fuse.ReadRequest on DirInode specifies Offset == 0
	//                          Otherwise, just assumes caller wants to keep going...
}

type logSegmentCacheElementStateType uint8

const (
	logSegmentCacheElementStateGetIssued logSegmentCacheElementStateType = iota
	logSegmentCacheElementStateGetSuccessful
	logSegmentCacheElementStateGetFailed // In which case it must not be in LLRBTree nor LRU
)

type logSegmentCacheElementKeyStruct struct {
	logSegmentNumber uint64 // Converted from logSegmentCacheElementStruct.objectName
	cacheLineTag     uint64 // == logSegmentCacheElementStruct.offset / globals.config.ReadCacheLineSize
}

type logSegmentCacheElementStruct struct {
	sync.WaitGroup                                  // Used by those awaiting GET result
	state           logSegmentCacheElementStateType //  if logSegmentCacheElementStateGetIssued
	containerName   string
	objectName      string
	startingOffset  uint64
	cacheLRUElement *list.Element // Element on globals.logSegmentCacheLRU
	buf             []byte
}

// metricsStruct contains Prometheus-styled field names to be output by serveGetOfMetrics().
//
// In order to utilize Go Reflection, these field names must be capitalized (i.e. Global).

type metricsStruct struct {
	FUSE_AccessRequest_calls       uint64
	FUSE_CreateRequest_calls       uint64
	FUSE_DestroyRequest_calls      uint64
	FUSE_ExchangeDataRequest_calls uint64
	FUSE_FlushRequest_calls        uint64
	FUSE_ForgetRequest_calls       uint64
	FUSE_FsyncRequest_calls        uint64
	FUSE_GetattrRequest_calls      uint64
	FUSE_GetxattrRequest_calls     uint64
	FUSE_InitRequest_calls         uint64
	FUSE_InterruptRequest_calls    uint64
	FUSE_LinkRequest_calls         uint64
	FUSE_ListxattrRequest_calls    uint64
	FUSE_LookupRequest_calls       uint64
	FUSE_MkdirRequest_calls        uint64
	FUSE_MknodRequest_calls        uint64
	FUSE_OpenRequest_calls         uint64
	FUSE_ReadlinkRequest_calls     uint64
	FUSE_ReleaseRequest_calls      uint64
	FUSE_RemoveRequest_calls       uint64
	FUSE_RemovexattrRequest_calls  uint64
	FUSE_RenameRequest_calls       uint64
	FUSE_SetattrRequest_calls      uint64
	FUSE_SetxattrRequest_calls     uint64
	FUSE_StatfsRequest_calls       uint64
	FUSE_SymlinkRequest_calls      uint64

	FUSE_UnknownRequest_calls uint64

	FUSE_ReadRequestDirInodeCase_calls  uint64
	FUSE_ReadRequestFileInodeCase_calls uint64
	FUSE_WriteRequest_calls             uint64

	FUSE_ReadRequestFileInodeCase_bytes uint64
	FUSE_WriteRequest_bytes             uint64

	ReadCacheHits   uint64
	ReadCacheMisses uint64

	LogSegmentPUTs        uint64
	LogSegmentPUTReadHits uint64
}

type globalsStruct struct {
	sync.Mutex
	config                          configStruct
	logFile                         *os.File // == nil if configStruct.LogFilePath == ""
	httpServer                      *http.Server
	httpServerWG                    sync.WaitGroup
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
	fileInodeDirtyList              *list.List // LRU of fileInode's with non-empty chunkedPutList
	leaseRequestChan                chan *fileInodeLeaseRequestStruct
	unleasedFileInodeCacheLRU       *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
	sharedLeaseFileInodeCacheLRU    *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
	exclusiveLeaseFileInodeCacheLRU *list.List // Front() is oldest fileInodeStruct.cacheLRUElement
	lastHandleID                    fuse.HandleID
	handleTable                     map[fuse.HandleID]*handleStruct
	logSegmentCacheMap              map[logSegmentCacheElementKeyStruct]*logSegmentCacheElementStruct
	logSegmentCacheLRU              *list.List // Front() is oldest logSegmentCacheElementStruct.cacheLRUElement
	metrics                         *metricsStruct
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
	globals.config.LogToConsole = true
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
	if !strings.HasPrefix(strings.ToLower(globals.config.SwiftAuthURL), "http:") && !strings.HasPrefix(strings.ToLower(globals.config.SwiftAuthURL), "https:") {
		err = fmt.Errorf("[Agent]SwiftAuthURL (\"%s\") must start with either \"http:\" or \"https:\"", globals.config.SwiftAuthURL)
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

	globals.config.FetchExtentsFromFileOffset, err = confMap.FetchOptionValueUint64("Agent", "FetchExtentsFromFileOffset")
	if nil != err {
		logFatal(err)
	}

	globals.config.FetchExtentsBeforeFileOffset, err = confMap.FetchOptionValueUint64("Agent", "FetchExtentsBeforeFileOffset")
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

	globals.config.DirtyFileLimit, err = confMap.FetchOptionValueUint64("Agent", "DirtyFileLimit")
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

	globals.config.HTTPServerIPAddr, err = confMap.FetchOptionValueString("Agent", "HTTPServerIPAddr")
	if nil != err {
		logFatal(err)
	}
	globals.config.HTTPServerTCPPort, err = confMap.FetchOptionValueUint16("Agent", "HTTPServerTCPPort")
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
		TLSHandshakeTimeout:    globals.config.SwiftTimeout,
		DisableKeepAlives:      false,
		DisableCompression:     defaultTransport.DisableCompression,
		MaxIdleConns:           int(globals.config.SwiftConnectionPoolSize),
		MaxIdleConnsPerHost:    int(globals.config.SwiftConnectionPoolSize),
		MaxConnsPerHost:        int(globals.config.SwiftConnectionPoolSize),
		IdleConnTimeout:        globals.config.SwiftTimeout,
		ResponseHeaderTimeout:  globals.config.SwiftTimeout,
		ExpectContinueTimeout:  globals.config.SwiftTimeout,
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

	globals.fileInodeDirtyList = list.New()

	globals.leaseRequestChan = make(chan *fileInodeLeaseRequestStruct)

	go leaseDaemon()

	globals.unleasedFileInodeCacheLRU = list.New()
	globals.sharedLeaseFileInodeCacheLRU = list.New()
	globals.exclusiveLeaseFileInodeCacheLRU = list.New()

	globals.lastHandleID = 0
	globals.handleTable = make(map[fuse.HandleID]*handleStruct)

	globals.logSegmentCacheMap = make(map[logSegmentCacheElementKeyStruct]*logSegmentCacheElementStruct)
	globals.logSegmentCacheLRU = list.New()

	globals.metrics = &metricsStruct{}
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
	globals.httpServer = nil
	globals.httpClient = nil
	globals.retryDelay = nil
	globals.swiftAuthWaitGroup = nil
	globals.swiftAuthToken = ""
	globals.swiftAccountURL = ""
	globals.fuseConn = nil
	globals.jrpcLastID = 0
	globals.fileInodeMap = nil
	globals.fileInodeDirtyList = nil
	globals.leaseRequestChan = nil
	globals.unleasedFileInodeCacheLRU = nil
	globals.sharedLeaseFileInodeCacheLRU = nil
	globals.exclusiveLeaseFileInodeCacheLRU = nil
	globals.lastHandleID = 0
	globals.handleTable = nil
	globals.logSegmentCacheMap = nil
	globals.logSegmentCacheLRU = nil
	globals.metrics = nil
}
