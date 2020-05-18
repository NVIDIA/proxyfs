package main

import (
	"container/list"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"bazil.org/fuse"

	"github.com/swiftstack/fission"
	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/retryrpc"
	"github.com/swiftstack/ProxyFS/utils"
)

type configStruct struct {
	FUSEVolumeName               string
	FUSEMountPointPath           string // Unless starting with '/', relative to $CWD
	FUSEUnMountRetryDelay        time.Duration
	FUSEUnMountRetryCap          uint64
	PlugInPath                   string
	PlugInEnvName                string
	PlugInEnvValue               string // If "", assume it's already set as desired
	SwiftTimeout                 time.Duration
	SwiftRetryLimit              uint64
	SwiftRetryDelay              time.Duration
	SwiftRetryDelayVariance      uint8
	SwiftRetryExpBackoff         float64
	SwiftConnectionPoolSize      uint64
	FetchExtentsFromFileOffset   uint64
	FetchExtentsBeforeFileOffset uint64
	ReadCacheLineSize            uint64 // Aligned chunk of a LogSegment
	ReadCacheLineCount           uint64
	SharedFileLimit              uint64
	ExclusiveFileLimit           uint64
	DirtyFileLimit               uint64
	DirtyLogSegmentLimit         uint64
	MaxFlushSize                 uint64
	MaxFlushTime                 time.Duration
	LogFilePath                  string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole                 bool
	TraceEnabled                 bool
	HTTPServerIPAddr             string
	HTTPServerTCPPort            uint16
	ReadDirPlusEnabled           bool
	XAttrEnabled                 bool
	EntryDuration                time.Duration
	AttrDuration                 time.Duration
	AttrBlockSize                uint64
	ReaddirMaxEntries            uint64
	FUSEMaxBackground            uint16
	FUSECongestionThreshhold     uint16
	FUSEMaxWrite                 uint32
	RetryRPCDeadlineIO           time.Duration
	RetryRPCKEEPALIVEPeriod      time.Duration
}

type retryDelayElementStruct struct {
	nominal  time.Duration
	variance time.Duration
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
	chunkedPutContextExitReadPollingRate = time.Millisecond
)

type chunkedPutContextStruct struct {
	sync.WaitGroup                    //      Used to await completion of performChunkedPut goroutine
	containerName  string             //
	objectName     string             //
	extentMap      sortedmap.LLRBTree //      Key == singleObjectExtentStruct.fileOffset; Value == *singleObjectExtentStruct
	fileSize       uint64             //      Last (most recent) chunkedPutContextStruct on fileInode.chunkedPutList may have
	//                                          updated fileSize affecting reads while writing to read beyond fileInode.extentMapFileSize
	buf                   []byte           //
	chunkedPutListElement *list.Element    // FIFO Element of fileInodeStruct.chunkedPutList
	fileInode             *fileInodeStruct //
	state                 uint8            // One of chunkedPutContextState{Open|Closing|Closed}
	pos                   int              // ObjectOffset just after last sent chunk
	sendChan              chan struct{}    // Single element buffered chan to wake up *chunkedPutContextStruct.sendDaemon()
	//                                          will be closed to indicate a flush is requested
	wakeChan chan struct{} //                 Single element buffered chan to wake up *chunkedPutContextStruct.Read()
	//                                          will be closed to indicate a flush is requested
	inRead         bool //                    Set when in Read() as a hint to Close() to help Read() cleanly exit
	flushRequested bool //                    Set to remember that a flush has been requested of *chunkedPutContextStruct.Read()
}

type fileInodeStruct struct {
	sync.WaitGroup //                                  Used to await completion of all chunkedPutContext's
	inode.InodeNumber
	cachedStat          *jrpcfs.StatStruct
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

type fhSetType map[uint64]struct{}

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

type authPlugInControlStruct struct {
	cmd        *exec.Cmd
	stdinPipe  io.WriteCloser
	stdoutPipe io.ReadCloser
	stderrPipe io.ReadCloser
	stdoutChan chan []byte
	stderrChan chan []byte
	wg         sync.WaitGroup
}

// metricsStruct contains Prometheus-styled field names to be output by serveGetOfMetrics().
//
// In order to utilize Go Reflection, these field names must be capitalized (i.e. Global).

type metricsStruct struct {
	FUSE_DoLookup_calls      uint64
	FUSE_DoForget_calls      uint64
	FUSE_DoGetAttr_calls     uint64
	FUSE_DoSetAttr_calls     uint64
	FUSE_DoReadLink_calls    uint64
	FUSE_DoSymLink_calls     uint64
	FUSE_DoMkNod_calls       uint64
	FUSE_DoMkDir_calls       uint64
	FUSE_DoUnlink_calls      uint64
	FUSE_DoRmDir_calls       uint64
	FUSE_DoRename_calls      uint64
	FUSE_DoLink_calls        uint64
	FUSE_DoOpen_calls        uint64
	FUSE_DoRead_calls        uint64
	FUSE_DoWrite_calls       uint64
	FUSE_DoStatFS_calls      uint64
	FUSE_DoRelease_calls     uint64
	FUSE_DoFSync_calls       uint64
	FUSE_DoSetXAttr_calls    uint64
	FUSE_DoGetXAttr_calls    uint64
	FUSE_DoListXAttr_calls   uint64
	FUSE_DoRemoveXAttr_calls uint64
	FUSE_DoFlush_calls       uint64
	FUSE_DoInit_calls        uint64
	FUSE_DoOpenDir_calls     uint64
	FUSE_DoReadDir_calls     uint64
	FUSE_DoReleaseDir_calls  uint64
	FUSE_DoFSyncDir_calls    uint64
	FUSE_DoGetLK_calls       uint64
	FUSE_DoSetLK_calls       uint64
	FUSE_DoSetLKW_calls      uint64
	FUSE_DoAccess_calls      uint64
	FUSE_DoCreate_calls      uint64
	FUSE_DoInterrupt_calls   uint64
	FUSE_DoBMap_calls        uint64
	FUSE_DoDestroy_calls     uint64
	FUSE_DoPoll_calls        uint64
	FUSE_DoBatchForget_calls uint64
	FUSE_DoFAllocate_calls   uint64
	FUSE_DoReadDirPlus_calls uint64
	FUSE_DoRename2_calls     uint64
	FUSE_DoLSeek_calls       uint64

	FUSE_DoGetAttr_cache_hits   uint64
	FUSE_DoGetAttr_cache_misses uint64

	FUSE_DoRead_bytes  uint64
	FUSE_DoWrite_bytes uint64

	FUSE_DoReadDir_entries     uint64
	FUSE_DoReadDirPlus_entries uint64

	FUSE_DoSetXAttr_bytes  uint64
	FUSE_DoGetXAttr_bytes  uint64
	FUSE_DoListXAttr_names uint64

	FUSE_DoBatchForget_nodes uint64

	ReadCacheHits   uint64
	ReadCacheMisses uint64

	LogSegmentPUTs        uint64
	LogSegmentPUTReadHits uint64

	HTTPRequests                         uint64
	HTTPRequestSubmissionFailures        uint64
	HTTPRequestResponseBodyCorruptions   uint64
	HTTPRequestRetryLimitExceededCount   uint64
	HTTPRequestsRequiringReauthorization uint64
	HTTPRequestRetries                   uint64
	HTTPRequestsInFlight                 uint64
}

type statsStruct struct {
	FUSEDoReadBytes  bucketstats.BucketLog2Round
	FUSEDoWriteBytes bucketstats.BucketLog2Round

	LogSegmentGetUsec bucketstats.BucketLog2Round

	LogSegmentPutBytes bucketstats.BucketLog2Round
}

type globalsStruct struct {
	sync.Mutex
	config                          configStruct
	logFile                         *os.File // == nil if configStruct.LogFilePath == ""
	retryRPCPublicIPAddr            string
	retryRPCPort                    uint16
	retryRPCClient                  *retryrpc.Client
	rootCAx509CertificatePEM        []byte
	entryValidSec                   uint64
	entryValidNSec                  uint32
	attrValidSec                    uint64
	attrValidNSec                   uint32
	httpServer                      *http.Server
	httpServerWG                    sync.WaitGroup
	httpClient                      *http.Client
	retryDelay                      []retryDelayElementStruct
	authPlugInControl               *authPlugInControlStruct
	swiftAuthWaitGroup              *sync.WaitGroup // Protected by sync.Mutex of globalsStruct
	swiftAuthToken                  string          // Protected by swiftAuthWaitGroup
	swiftStorageURL                 string          // Protected by swiftAuthWaitGroup
	mountID                         jrpcfs.MountIDAsString
	rootDirInodeNumber              uint64
	fissionErrChan                  chan error
	fissionVolume                   fission.Volume
	fuseConn                        *fuse.Conn
	jrpcLastID                      uint64
	fileInodeMap                    map[inode.InodeNumber]*fileInodeStruct
	fileInodeDirtyList              *list.List    // LRU of fileInode's with non-empty chunkedPutList
	fileInodeDirtyLogSegmentChan    chan struct{} // Limits # of in-flight LogSegment Chunked PUTs
	leaseRequestChan                chan *fileInodeLeaseRequestStruct
	unleasedFileInodeCacheLRU       *list.List           // Front() is oldest fileInodeStruct.cacheLRUElement
	sharedLeaseFileInodeCacheLRU    *list.List           // Front() is oldest fileInodeStruct.cacheLRUElement
	exclusiveLeaseFileInodeCacheLRU *list.List           // Front() is oldest fileInodeStruct.cacheLRUElement
	fhToInodeNumberMap              map[uint64]uint64    // Key == FH; Value == InodeNumber
	inodeNumberToFHMap              map[uint64]fhSetType // Key == InodeNumber; Value == set of FH's
	lastFH                          uint64               // Valid FH's start at 1
	logSegmentCacheMap              map[logSegmentCacheElementKeyStruct]*logSegmentCacheElementStruct
	logSegmentCacheLRU              *list.List // Front() is oldest logSegmentCacheElementStruct.cacheLRUElement
	metrics                         *metricsStruct
	stats                           *statsStruct
}

var globals globalsStruct

func initializeGlobals(confMap conf.ConfMap) {
	var (
		configJSONified                   string
		customTransport                   *http.Transport
		defaultTransport                  *http.Transport
		err                               error
		fileInodeDirtyLogSegmentChanIndex uint64
		nextRetryDelay                    time.Duration
		ok                                bool
		plugInEnvValueSlice               []string
		retryIndex                        uint64
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

	globals.config.PlugInPath, err = confMap.FetchOptionValueString("Agent", "PlugInPath")
	if nil != err {
		logFatal(err)
	}

	globals.config.PlugInEnvName, err = confMap.FetchOptionValueString("Agent", "PlugInEnvName")
	if nil != err {
		logFatal(err)
	}

	err = confMap.VerifyOptionIsMissing("Agent", "PlugInEnvValue")
	if nil == err {
		globals.config.PlugInEnvValue = ""
	} else {
		plugInEnvValueSlice, err = confMap.FetchOptionValueStringSlice("Agent", "PlugInEnvValue")
		if nil != err {
			logFatal(err)
		} else {
			switch len(plugInEnvValueSlice) {
			case 0:
				globals.config.PlugInEnvValue = ""
			case 1:
				globals.config.PlugInEnvValue = plugInEnvValueSlice[0]
			default:
				log.Fatalf("[Agent]PlugInEnvValue must be missing, empty, or single-valued: %#v", plugInEnvValueSlice)
			}
		}
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

	globals.config.SwiftRetryDelayVariance, err = confMap.FetchOptionValueUint8("Agent", "SwiftRetryDelayVariance")
	if nil != err {
		globals.config.SwiftRetryDelayVariance = 25 // TODO: Eventually, just logFatal(err)
	}
	if 0 == globals.config.SwiftRetryDelayVariance {
		err = fmt.Errorf("[Agent]SwiftRetryDelayVariance must be > 0")
		logFatal(err)
	}
	if 100 < globals.config.SwiftRetryDelayVariance {
		err = fmt.Errorf("[Agent]SwiftRetryDelayVariance (%v) must be <= 100", globals.config.SwiftRetryDelayVariance)
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

	globals.config.DirtyLogSegmentLimit, err = confMap.FetchOptionValueUint64("Agent", "DirtyLogSegmentLimit")
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

	globals.config.ReadDirPlusEnabled, err = confMap.FetchOptionValueBool("Agent", "ReadDirPlusEnabled")
	if nil != err {
		logFatal(err)
	}

	globals.config.XAttrEnabled, err = confMap.FetchOptionValueBool("Agent", "XAttrEnabled")
	if nil != err {
		logFatal(err)
	}

	globals.config.EntryDuration, err = confMap.FetchOptionValueDuration("Agent", "EntryDuration")
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

	globals.config.ReaddirMaxEntries, err = confMap.FetchOptionValueUint64("Agent", "ReaddirMaxEntries")
	if nil != err {
		logFatal(err)
	}

	globals.config.FUSEMaxBackground, err = confMap.FetchOptionValueUint16("Agent", "FUSEMaxBackground")
	if nil != err {
		logFatal(err)
	}

	globals.config.FUSECongestionThreshhold, err = confMap.FetchOptionValueUint16("Agent", "FUSECongestionThreshhold")
	if nil != err {
		logFatal(err)
	}

	globals.config.FUSEMaxWrite, err = confMap.FetchOptionValueUint32("Agent", "FUSEMaxWrite")
	if nil != err {
		logFatal(err)
	}

	globals.config.RetryRPCDeadlineIO, err = confMap.FetchOptionValueDuration("Agent", "RetryRPCDeadlineIO")
	if nil != err {
		logFatal(err)
	}

	globals.config.RetryRPCKEEPALIVEPeriod, err = confMap.FetchOptionValueDuration("Agent", "RetryRPCKEEPALIVEPeriod")
	if nil != err {
		logFatal(err)
	}

	configJSONified = utils.JSONify(globals.config, true)

	logInfof("\n%s", configJSONified)

	globals.entryValidSec, globals.entryValidNSec = nsToUnixTime(uint64(globals.config.EntryDuration))
	globals.attrValidSec, globals.attrValidNSec = nsToUnixTime(uint64(globals.config.AttrDuration))

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

	globals.retryDelay = make([]retryDelayElementStruct, globals.config.SwiftRetryLimit)

	nextRetryDelay = globals.config.SwiftRetryDelay

	for retryIndex = 0; retryIndex < globals.config.SwiftRetryLimit; retryIndex++ {
		globals.retryDelay[retryIndex].nominal = nextRetryDelay
		globals.retryDelay[retryIndex].variance = nextRetryDelay * time.Duration(globals.config.SwiftRetryDelayVariance) / time.Duration(100)
		nextRetryDelay = time.Duration(float64(nextRetryDelay) * globals.config.SwiftRetryExpBackoff)
	}

	globals.authPlugInControl = nil

	globals.swiftAuthWaitGroup = nil
	globals.swiftAuthToken = ""
	globals.swiftStorageURL = ""

	globals.fissionErrChan = make(chan error)

	globals.jrpcLastID = 1

	globals.fileInodeMap = make(map[inode.InodeNumber]*fileInodeStruct)

	globals.fileInodeDirtyList = list.New()

	globals.fileInodeDirtyLogSegmentChan = make(chan struct{}, globals.config.DirtyLogSegmentLimit)

	for fileInodeDirtyLogSegmentChanIndex = 0; fileInodeDirtyLogSegmentChanIndex < globals.config.DirtyLogSegmentLimit; fileInodeDirtyLogSegmentChanIndex++ {
		globals.fileInodeDirtyLogSegmentChan <- struct{}{}
	}

	globals.leaseRequestChan = make(chan *fileInodeLeaseRequestStruct)

	go leaseDaemon()

	globals.unleasedFileInodeCacheLRU = list.New()
	globals.sharedLeaseFileInodeCacheLRU = list.New()
	globals.exclusiveLeaseFileInodeCacheLRU = list.New()

	globals.fhToInodeNumberMap = make(map[uint64]uint64)
	globals.inodeNumberToFHMap = make(map[uint64]fhSetType)

	globals.lastFH = 0

	globals.logSegmentCacheMap = make(map[logSegmentCacheElementKeyStruct]*logSegmentCacheElementStruct)
	globals.logSegmentCacheLRU = list.New()

	globals.metrics = &metricsStruct{}
	globals.stats = &statsStruct{}

	bucketstats.Register("PFSAgent", "", globals.stats)
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

	bucketstats.UnRegister("PFSAgent", "")

	// TODO: kill auth plug-in child (if alive)

	globals.logFile = nil
	globals.retryRPCPublicIPAddr = ""
	globals.retryRPCPort = 0
	globals.retryRPCClient = nil
	globals.rootCAx509CertificatePEM = []byte{}
	globals.entryValidSec = 0
	globals.entryValidNSec = 0
	globals.attrValidSec = 0
	globals.attrValidNSec = 0
	globals.httpServer = nil
	globals.httpClient = nil
	globals.retryDelay = nil
	globals.authPlugInControl = nil
	globals.swiftAuthWaitGroup = nil
	globals.swiftAuthToken = ""
	globals.swiftStorageURL = ""
	globals.fissionErrChan = nil
	globals.fissionVolume = nil
	globals.fuseConn = nil
	globals.jrpcLastID = 0
	globals.fileInodeMap = nil
	globals.fileInodeDirtyList = nil
	globals.fileInodeDirtyLogSegmentChan = nil
	globals.leaseRequestChan = nil
	globals.unleasedFileInodeCacheLRU = nil
	globals.sharedLeaseFileInodeCacheLRU = nil
	globals.exclusiveLeaseFileInodeCacheLRU = nil
	globals.fhToInodeNumberMap = nil
	globals.inodeNumberToFHMap = nil
	globals.lastFH = 0
	globals.logSegmentCacheMap = nil
	globals.logSegmentCacheLRU = nil
	globals.metrics = nil
	globals.stats = nil
}
