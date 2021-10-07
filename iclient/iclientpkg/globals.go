// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"container/list"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/fission"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/ilayout"
	"github.com/NVIDIA/proxyfs/retryrpc"
	"github.com/NVIDIA/proxyfs/utils"
)

type configStruct struct {
	VolumeName                       string
	MountPointDirPath                string
	FUSEAllowOther                   bool
	FUSEMaxBackground                uint16
	FUSECongestionThreshhold         uint16
	FUSEMaxWrite                     uint32
	FUSEEntryValidDuration           time.Duration
	FUSEAttrValidDuration            time.Duration
	AuthPlugInPath                   string
	AuthPlugInEnvName                string
	AuthPlugInEnvValue               string
	SwiftTimeout                     time.Duration
	SwiftRetryLimit                  uint64
	SwiftRetryDelay                  time.Duration
	SwiftRetryDelayVariance          uint8
	SwiftRetryExponentialBackoff     float64
	SwiftConnectionPoolSize          uint32
	RetryRPCPublicIPAddr             string
	RetryRPCPort                     uint16
	RetryRPCDeadlineIO               time.Duration
	RetryRPCKeepAlivePeriod          time.Duration
	RetryRPCCACertFilePath           string // Defaults to /dev/null
	MaxSharedLeases                  uint64
	MaxExclusiveLeases               uint64
	InodePayloadEvictLowLimit        uint64
	InodePayloadEvictHighLimit       uint64
	DirInodeMaxKeysPerBPlusTreePage  uint64
	FileInodeMaxKeysPerBPlusTreePage uint64
	ReadCacheLineSize                uint64
	ReadCacheLineCountMax            uint64
	FileFlushTriggerSize             uint64
	FileFlushTriggerDuration         time.Duration
	LogFilePath                      string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole                     bool
	TraceEnabled                     bool
	FUSELogEnabled                   bool
	HTTPServerIPAddr                 string
	HTTPServerPort                   uint16 // To be served on HTTPServerIPAddr via TCP
}

type swiftRetryDelayElementStruct struct {
	nominal  time.Duration
	variance time.Duration
}

type inodeLeaseStateType uint32

const (
	inodeLeaseStateNone inodeLeaseStateType = iota // Only used if (*inodeLeaseStruct).requestList is non-empty
	inodeLeaseStateSharedRequested
	inodeLeaseStateSharedGranted
	inodeLeaseStateSharedPromoting
	inodeLeaseStateSharedReleasing
	inodeLeaseStateSharedExpired
	inodeLeaseStateExclusiveRequested
	inodeLeaseStateExclusiveGranted
	inodeLeaseStateExclusiveDemoting
	inodeLeaseStateExclusiveReleasing
	inodeLeaseStateExclusiveExpired
)

type layoutMapEntryStruct struct {
	objectSize      uint64
	bytesReferenced uint64
}

type inodeLeaseStruct struct {
	inodeNumber uint64                     //
	leaseState  inodeLeaseStateType        //
	listElement *list.Element              //                                   Maintains position in globalsStruct.{shared|exclusive|LeaseLRU
	heldList    *list.List                 //                                   List of granted inodeHeldLockStruct's
	requestList *list.List                 //                                   List of pending inodeLockRequestStruct's
	inodeHeadV1 *ilayout.InodeHeadV1Struct //
	payload     sortedmap.BPlusTree        //                                   For DirInode:  Directory B+Tree from .inodeHeadV1.PayloadObjec{Number|Offset|Length}
	//                                                                          For FileInode: ExtentMap B+Tree from .inodeHeadV1.PayloadObjec{Number|Offset|Length}
	layoutMap                                map[uint64]layoutMapEntryStruct // For DirInode & FileInode: Map form of .inodeHeadV1.Layout
	superBlockInodeObjectCountAdjustment     int64                           //
	superBlockInodeObjectSizeAdjustment      int64                           //
	superBlockInodeBytesReferencedAdjustment int64                           //
	dereferencedObjectNumberArray            []uint64                        //
	putObjectNumber                          uint64                          // For DirInode & FileInode:
	putObjectBuffer                          []byte                          //   ObjectNumber and buffer to PUT during next flush
}

type inodeHeldLockStruct struct {
	inodeLease       *inodeLeaseStruct
	inodeLockRequest *inodeLockRequestStruct
	exclusive        bool
	listElement      *list.Element //Maintains position in .inodeNumber's indicated inodeLeaseStruct.heldList
}

type inodeLockRequestStruct struct {
	sync.WaitGroup                                 // Signaled when the state of the lock request has been served
	inodeNumber    uint64                          // The inodeNumber for which the latest lock request is being made
	exclusive      bool                            // Indicates if the latest lock request is exclusive
	listElement    *list.Element                   // Maintains position in .inodeNumber's indicated inodeLeaseStruct.requestList
	locksHeld      map[uint64]*inodeHeldLockStruct // At entry, contains the list of inodeLock's already held (shared or exclusively)
	//                                                At exit,  either contains the earlier list appended with the granted inodeLock
	//                                                          or     is empty indicating the caller should restart lock request sequence
}

type statsStruct struct {
	GetConfigUsecs  bucketstats.BucketLog2Round // GET /config
	GetLeasesUsecs  bucketstats.BucketLog2Round // GET /leases
	GetStatsUsecs   bucketstats.BucketLog2Round // GET /stats
	GetVersionUsecs bucketstats.BucketLog2Round // GET /version

	PostLeasesDemoteUsecs  bucketstats.BucketLog2Round // POST /leases/demote
	PostLeasesReleaseUsecs bucketstats.BucketLog2Round // POST /leases/release

	AdjustInodeTableEntryOpenCountUsecs bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)AdjustInodeTableEntryOpenCount()
	DeleteInodeTableEntryUsecs          bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)DeleteInodeTableEntry()
	FetchNonceRangeUsecs                bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)FetchNonceRange()
	FlushUsecs                          bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)Flush()
	GetInodeTableEntryUsecs             bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)GetInodeTableEntry()
	LeaseUsecs                          bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)Lease()
	MountUsecs                          bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)Mount()
	PutInodeTableEntriesUsecs           bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)PutInodeTableEntries()
	RenewMountUsecs                     bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)RenewMount()
	UnmountUsecs                        bucketstats.BucketLog2Round // (*imgrpkg.RetryRPCServerStruct)Unmount()

	DoLookupUsecs      bucketstats.BucketLog2Round // (*globalsStruct)DoLookup()
	DoForgetUsecs      bucketstats.BucketLog2Round // (*globalsStruct)DoForget()
	DoGetAttrUsecs     bucketstats.BucketLog2Round // (*globalsStruct)DoGetAttr()
	DoSetAttrUsecs     bucketstats.BucketLog2Round // (*globalsStruct)DoSetAttr()
	DoReadLinkUsecs    bucketstats.BucketLog2Round // (*globalsStruct)DoReadLink()
	DoSymLinkUsecs     bucketstats.BucketLog2Round // (*globalsStruct)DoSymLink()
	DoMkNodUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoMkNod()
	DoMkDirUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoMkDir()
	DoUnlinkUsecs      bucketstats.BucketLog2Round // (*globalsStruct)DoUnlink()
	DoRmDirUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoRmDir()
	DoRenameUsecs      bucketstats.BucketLog2Round // (*globalsStruct)DoRename()
	DoLinkUsecs        bucketstats.BucketLog2Round // (*globalsStruct)DoLink()
	DoOpenUsecs        bucketstats.BucketLog2Round // (*globalsStruct)DoOpen()
	DoReadUsecs        bucketstats.BucketLog2Round // (*globalsStruct)DoRead()
	DoWriteUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoWrite()
	DoStatFSUsecs      bucketstats.BucketLog2Round // (*globalsStruct)DoStatFS()
	DoReleaseUsecs     bucketstats.BucketLog2Round // (*globalsStruct)DoRelease()
	DoFSyncUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoFSync()
	DoSetXAttrUsecs    bucketstats.BucketLog2Round // (*globalsStruct)DoSetXAttr()
	DoGetXAttrUsecs    bucketstats.BucketLog2Round // (*globalsStruct)DoGetXAttr()
	DoListXAttrUsecs   bucketstats.BucketLog2Round // (*globalsStruct)DoListXAttr()
	DoRemoveXAttrUsecs bucketstats.BucketLog2Round // (*globalsStruct)DoRemoveXAttr()
	DoFlushUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoFlush()
	DoInitUsecs        bucketstats.BucketLog2Round // (*globalsStruct)DoInit()
	DoOpenDirUsecs     bucketstats.BucketLog2Round // (*globalsStruct)DoOpenDir()
	DoReadDirUsecs     bucketstats.BucketLog2Round // (*globalsStruct)DoReadDir()
	DoReleaseDirUsecs  bucketstats.BucketLog2Round // (*globalsStruct)DoReleaseDir()
	DoFSyncDirUsecs    bucketstats.BucketLog2Round // (*globalsStruct)DoFSyncDir()
	DoGetLKUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoGetLK()
	DoSetLKUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoSetLK()
	DoSetLKWUsecs      bucketstats.BucketLog2Round // (*globalsStruct)DoSetLKW()
	DoAccessUsecs      bucketstats.BucketLog2Round // (*globalsStruct)DoAccess()
	DoCreateUsecs      bucketstats.BucketLog2Round // (*globalsStruct)DoCreate()
	DoInterruptUsecs   bucketstats.BucketLog2Round // (*globalsStruct)DoInterrupt()
	DoBMapUsecs        bucketstats.BucketLog2Round // (*globalsStruct)DoBMap()
	DoDestroyUsecs     bucketstats.BucketLog2Round // (*globalsStruct)DoDestroy()
	DoPollUsecs        bucketstats.BucketLog2Round // (*globalsStruct)DoPoll()
	DoBatchForgetUsecs bucketstats.BucketLog2Round // (*globalsStruct)DoBatchForget()
	DoFAllocateUsecs   bucketstats.BucketLog2Round // (*globalsStruct)DoFAllocate()
	DoReadDirPlusUsecs bucketstats.BucketLog2Round // (*globalsStruct)DoReadDirPlus()
	DoRename2Usecs     bucketstats.BucketLog2Round // (*globalsStruct)DoRename2()
	DoLSeekUsecs       bucketstats.BucketLog2Round // (*globalsStruct)DoLSeek()
}

type globalsStruct struct {
	sync.Mutex                                                //
	config                     configStruct                   //
	fuseEntryValidDurationSec  uint64                         //
	fuseEntryValidDurationNSec uint32                         //
	fuseAttrValidDurationSec   uint64                         //
	fuseAttrValidDurationNSec  uint32                         //
	logFile                    *os.File                       // == nil if config.LogFilePath == ""
	retryRPCCACertPEM          []byte                         // == nil if config.RetryRPCCACertFilePath == ""
	httpClient                 *http.Client                   //
	swiftRetryDelay            []swiftRetryDelayElementStruct //
	swiftAuthInString          string                         //
	swiftAuthWaitGroup         *sync.WaitGroup                // != nil if updateAuthTokenAndStorageURL() is active
	swiftAuthToken             string                         //
	swiftStorageURL            string                         //
	retryRPCClientConfig       *retryrpc.ClientConfig         //
	retryRPCClient             *retryrpc.Client               //
	mountID                    string                         //
	fissionErrChan             chan error                     //
	inodeLeaseTable            map[uint64]*inodeLeaseStruct   //
	inodeLeasePayloadCache     sortedmap.BPlusTreeCache       //
	sharedLeaseLRU             *list.List                     // LRU-ordered list of inodeLeaseStruct.listElement's in or transitioning to inodeLeaseStateSharedGranted
	exclusiveLeaseLRU          *list.List                     // LRU-ordered list of inodeLeaseStruct.listElement's in or transitioning to inodeLeaseStateExclusiveGranted
	httpServer                 *http.Server                   //
	httpServerWG               sync.WaitGroup                 //
	stats                      *statsStruct                   //
	fissionVolume              fission.Volume                 //
}

var globals globalsStruct

func initializeGlobals(confMap conf.ConfMap, fissionErrChan chan error) (err error) {
	var (
		configJSONified string
	)

	// Default logging related globals

	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true
	globals.logFile = nil

	// Process resultant confMap

	globals.config.VolumeName, err = confMap.FetchOptionValueString("ICLIENT", "VolumeName")
	if nil != err {
		logFatal(err)
	}
	globals.config.MountPointDirPath, err = confMap.FetchOptionValueString("ICLIENT", "MountPointDirPath")
	if nil != err {
		logFatal(err)
	}
	globals.config.FUSEAllowOther, err = confMap.FetchOptionValueBool("ICLIENT", "FUSEAllowOther")
	if nil != err {
		logFatal(err)
	}
	globals.config.FUSEMaxBackground, err = confMap.FetchOptionValueUint16("ICLIENT", "FUSEMaxBackground")
	if nil != err {
		logFatal(err)
	}
	globals.config.FUSECongestionThreshhold, err = confMap.FetchOptionValueUint16("ICLIENT", "FUSECongestionThreshhold")
	if nil != err {
		logFatal(err)
	}
	globals.config.FUSEMaxWrite, err = confMap.FetchOptionValueUint32("ICLIENT", "FUSEMaxWrite")
	if nil != err {
		logFatal(err)
	}
	globals.config.FUSEEntryValidDuration, err = confMap.FetchOptionValueDuration("ICLIENT", "FUSEEntryValidDuration")
	if nil != err {
		logFatal(err)
	}
	globals.config.FUSEAttrValidDuration, err = confMap.FetchOptionValueDuration("ICLIENT", "FUSEAttrValidDuration")
	if nil != err {
		logFatal(err)
	}
	globals.config.AuthPlugInPath, err = confMap.FetchOptionValueString("ICLIENT", "AuthPlugInPath")
	if nil != err {
		logFatal(err)
	}
	err = confMap.VerifyOptionIsMissing("ICLIENT", "AuthPlugInEnvName")
	if nil == err {
		globals.config.AuthPlugInEnvName = ""
		globals.config.AuthPlugInEnvValue, err = confMap.FetchOptionValueString("ICLIENT", "AuthPlugInEnvValue")
		if nil != err {
			logFatal(err)
		}
	} else {
		err = confMap.VerifyOptionValueIsEmpty("ICLIENT", "AuthPlugInEnvName")
		if nil == err {
			globals.config.AuthPlugInEnvName = ""
			globals.config.AuthPlugInEnvValue, err = confMap.FetchOptionValueString("ICLIENT", "AuthPlugInEnvValue")
			if nil != err {
				logFatal(err)
			}
		} else {
			globals.config.AuthPlugInEnvName, err = confMap.FetchOptionValueString("ICLIENT", "AuthPlugInEnvName")
			if nil != err {
				logFatal(err)
			}
			err = confMap.VerifyOptionIsMissing("ICLIENT", "AuthPlugInEnvValue")
			if nil == err {
				globals.config.AuthPlugInEnvValue = ""
			} else {
				err = confMap.VerifyOptionValueIsEmpty("ICLIENT", "AuthPlugInEnvValue")
				if nil == err {
					globals.config.AuthPlugInEnvValue = ""
				} else {
					logFatalf("If [ICLIENT]AuthPlugInEnvName is present and non-empty, [ICLIENT]AuthPlugInEnvValue must be missing or empty")
				}
			}
		}
	}
	globals.config.SwiftTimeout, err = confMap.FetchOptionValueDuration("ICLIENT", "SwiftTimeout")
	if nil != err {
		logFatal(err)
	}
	globals.config.SwiftRetryLimit, err = confMap.FetchOptionValueUint64("ICLIENT", "SwiftRetryLimit")
	if nil != err {
		logFatal(err)
	}
	globals.config.SwiftRetryDelay, err = confMap.FetchOptionValueDuration("ICLIENT", "SwiftRetryDelay")
	if nil != err {
		logFatal(err)
	}
	globals.config.SwiftRetryDelayVariance, err = confMap.FetchOptionValueUint8("ICLIENT", "SwiftRetryDelayVariance")
	if nil == err {
		if 0 == globals.config.SwiftRetryDelayVariance {
			logFatalf("[Agent]SwiftRetryDelayVariance must be > 0")
		}
		if 100 < globals.config.SwiftRetryDelayVariance {
			logFatalf("[Agent]SwiftRetryDelayVariance (%v) must be <= 100", globals.config.SwiftRetryDelayVariance)
		}
	} else {
		logFatal(err)
	}
	globals.config.SwiftRetryExponentialBackoff, err = confMap.FetchOptionValueFloat64("ICLIENT", "SwiftRetryExponentialBackoff")
	if nil != err {
		logFatal(err)
	}
	globals.config.SwiftConnectionPoolSize, err = confMap.FetchOptionValueUint32("ICLIENT", "SwiftConnectionPoolSize")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCPublicIPAddr, err = confMap.FetchOptionValueString("ICLIENT", "RetryRPCPublicIPAddr")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCPort, err = confMap.FetchOptionValueUint16("ICLIENT", "RetryRPCPort")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCDeadlineIO, err = confMap.FetchOptionValueDuration("ICLIENT", "RetryRPCDeadlineIO")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCKeepAlivePeriod, err = confMap.FetchOptionValueDuration("ICLIENT", "RetryRPCKeepAlivePeriod")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCCACertFilePath, err = confMap.FetchOptionValueString("ICLIENT", "RetryRPCCACertFilePath")
	if nil != err {
		globals.config.RetryRPCCACertFilePath = ""
	}
	globals.config.MaxSharedLeases, err = confMap.FetchOptionValueUint64("ICLIENT", "MaxSharedLeases")
	if nil != err {
		logFatal(err)
	}
	globals.config.MaxExclusiveLeases, err = confMap.FetchOptionValueUint64("ICLIENT", "MaxExclusiveLeases")
	if nil != err {
		logFatal(err)
	}
	globals.config.InodePayloadEvictLowLimit, err = confMap.FetchOptionValueUint64("ICLIENT", "InodePayloadEvictLowLimit")
	if nil != err {
		logFatal(err)
	}
	globals.config.InodePayloadEvictHighLimit, err = confMap.FetchOptionValueUint64("ICLIENT", "InodePayloadEvictHighLimit")
	if nil != err {
		logFatal(err)
	}
	globals.config.DirInodeMaxKeysPerBPlusTreePage, err = confMap.FetchOptionValueUint64("ICLIENT", "DirInodeMaxKeysPerBPlusTreePage")
	if nil != err {
		logFatal(err)
	}
	globals.config.FileInodeMaxKeysPerBPlusTreePage, err = confMap.FetchOptionValueUint64("ICLIENT", "FileInodeMaxKeysPerBPlusTreePage")
	if nil != err {
		logFatal(err)
	}
	globals.config.ReadCacheLineSize, err = confMap.FetchOptionValueUint64("ICLIENT", "ReadCacheLineSize")
	if nil != err {
		logFatal(err)
	}
	globals.config.ReadCacheLineCountMax, err = confMap.FetchOptionValueUint64("ICLIENT", "ReadCacheLineCountMax")
	if nil != err {
		logFatal(err)
	}
	globals.config.FileFlushTriggerSize, err = confMap.FetchOptionValueUint64("ICLIENT", "FileFlushTriggerSize")
	if nil != err {
		logFatal(err)
	}
	globals.config.FileFlushTriggerDuration, err = confMap.FetchOptionValueDuration("ICLIENT", "FileFlushTriggerDuration")
	if nil != err {
		logFatal(err)
	}
	globals.config.LogFilePath, err = confMap.FetchOptionValueString("ICLIENT", "LogFilePath")
	if nil != err {
		err = confMap.VerifyOptionValueIsEmpty("ICLIENT", "LogFilePath")
		if nil == err {
			globals.config.LogFilePath = ""
		} else {
			logFatalf("[ICLIENT]LogFilePath must either be a valid string or empty]")
		}
	}
	globals.config.LogToConsole, err = confMap.FetchOptionValueBool("ICLIENT", "LogToConsole")
	if nil != err {
		logFatal(err)
	}
	globals.config.TraceEnabled, err = confMap.FetchOptionValueBool("ICLIENT", "TraceEnabled")
	if nil != err {
		logFatal(err)
	}
	globals.config.FUSELogEnabled, err = confMap.FetchOptionValueBool("ICLIENT", "FUSELogEnabled")
	if nil != err {
		logFatal(err)
	}
	globals.config.HTTPServerIPAddr, err = confMap.FetchOptionValueString("ICLIENT", "HTTPServerIPAddr")
	if nil != err {
		globals.config.HTTPServerIPAddr = "0.0.0.0"
	}
	globals.config.HTTPServerPort, err = confMap.FetchOptionValueUint16("ICLIENT", "HTTPServerPort")
	if nil != err {
		globals.config.HTTPServerPort = 0
	}

	configJSONified = utils.JSONify(globals.config, true)

	logInfof("globals.config:\n%s", configJSONified)

	globals.fuseEntryValidDurationSec, globals.fuseEntryValidDurationNSec = nsToUnixTime(uint64(globals.config.FUSEEntryValidDuration))
	globals.fuseAttrValidDurationSec, globals.fuseAttrValidDurationNSec = nsToUnixTime(uint64(globals.config.FUSEAttrValidDuration))

	if "" == globals.config.RetryRPCCACertFilePath {
		globals.retryRPCCACertPEM = nil
	} else {
		globals.retryRPCCACertPEM, err = ioutil.ReadFile(globals.config.RetryRPCCACertFilePath)
		if nil != err {
			logFatal(err)
		}
	}

	globals.fissionErrChan = fissionErrChan

	globals.inodeLeaseTable = make(map[uint64]*inodeLeaseStruct)
	globals.inodeLeasePayloadCache = sortedmap.NewBPlusTreeCache(globals.config.InodePayloadEvictLowLimit, globals.config.InodePayloadEvictHighLimit)
	globals.sharedLeaseLRU = list.New()
	globals.exclusiveLeaseLRU = list.New()

	globals.stats = &statsStruct{}

	bucketstats.Register("ICLIENT", "", globals.stats)

	err = nil
	return
}

func uninitializeGlobals() (err error) {
	globals.config.VolumeName = ""
	globals.config.MountPointDirPath = ""
	globals.config.FUSEAllowOther = false
	globals.config.FUSEMaxBackground = 0
	globals.config.FUSECongestionThreshhold = 0
	globals.config.FUSEMaxWrite = 0
	globals.config.FUSEEntryValidDuration = time.Duration(0)
	globals.config.FUSEAttrValidDuration = time.Duration(0)
	globals.config.AuthPlugInPath = ""
	globals.config.AuthPlugInEnvName = ""
	globals.config.AuthPlugInEnvValue = ""
	globals.config.SwiftTimeout = time.Duration(0)
	globals.config.SwiftRetryLimit = 0
	globals.config.SwiftRetryDelay = time.Duration(0)
	globals.config.SwiftRetryDelayVariance = 0
	globals.config.SwiftRetryExponentialBackoff = 0.0
	globals.config.SwiftConnectionPoolSize = 0
	globals.config.RetryRPCPublicIPAddr = ""
	globals.config.RetryRPCPort = 0
	globals.config.RetryRPCDeadlineIO = time.Duration(0)
	globals.config.RetryRPCKeepAlivePeriod = time.Duration(0)
	globals.config.RetryRPCCACertFilePath = ""
	globals.config.MaxSharedLeases = 0
	globals.config.MaxExclusiveLeases = 0
	globals.config.InodePayloadEvictLowLimit = 0
	globals.config.InodePayloadEvictHighLimit = 0
	globals.config.DirInodeMaxKeysPerBPlusTreePage = 0
	globals.config.FileInodeMaxKeysPerBPlusTreePage = 0
	globals.config.ReadCacheLineSize = 0
	globals.config.ReadCacheLineCountMax = 0
	globals.config.FileFlushTriggerSize = 0
	globals.config.FileFlushTriggerDuration = time.Duration(0)
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = false
	globals.config.TraceEnabled = false
	globals.config.FUSELogEnabled = false
	globals.config.HTTPServerIPAddr = ""
	globals.config.HTTPServerPort = 0

	globals.fuseEntryValidDurationSec = 0
	globals.fuseEntryValidDurationNSec = 0
	globals.fuseAttrValidDurationSec = 0
	globals.fuseAttrValidDurationNSec = 0

	globals.retryRPCCACertPEM = nil

	globals.inodeLeaseTable = nil
	globals.inodeLeasePayloadCache = nil
	globals.sharedLeaseLRU = nil
	globals.exclusiveLeaseLRU = nil

	globals.fissionErrChan = nil

	bucketstats.UnRegister("ICLIENT", "")

	err = nil
	return
}
