// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

import (
	"container/list"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/sortedmap"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/retryrpc"
	"github.com/NVIDIA/proxyfs/utils"
)

type configStruct struct {
	PrivateIPAddr  string
	PublicIPAddr   string
	RetryRPCPort   uint16 // To be served only on PublicIPAddr  via TLS
	HTTPServerPort uint16 // To be served only on PrivateIPAddr via TCP

	RetryRPCTTLCompleted    time.Duration
	RetryRPCAckTrim         time.Duration
	RetryRPCDeadlineIO      time.Duration
	RetryRPCKeepAlivePeriod time.Duration

	RetryRPCCertFilePath string
	RetryRPCKeyFilePath  string

	FetchNonceRangeToReturn uint64

	MinLeaseDuration       time.Duration
	LeaseInterruptInterval time.Duration
	LeaseInterruptLimit    uint32

	SwiftRetryDelay      time.Duration
	SwiftRetryExpBackoff float64
	SwiftRetryLimit      uint32

	SwiftTimeout            time.Duration
	SwiftConnectionPoolSize uint32

	InodeTableCacheEvictLowLimit  uint64
	InodeTableCacheEvictHighLimit uint64

	InodeTableMaxInodesPerBPlusTreePage  uint64
	RootDirMaxDirEntriesPerBPlusTreePage uint64

	LogFilePath  string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole bool
	TraceEnabled bool
}

type chunkedPutContextStruct struct {
	sync.WaitGroup                      // Used to await completion of performChunkedPut goroutine
	containerName         string        //
	objectName            string        //
	buf                   []byte        //
	chunkedPutListElement *list.Element // FIFO Element of fileInodeStruct.chunkedPutList
	state                 uint8         // One of chunkedPutContextState{Open|Closing|Closed}
	pos                   int           // ObjectOffset just after last sent chunk
	sendChan              chan struct{} // Single element buffered chan to wake up *chunkedPutContextStruct.sendDaemon()
	//                                            will be closed to indicate a flush is requested
	wakeChan chan struct{} //                   Single element buffered chan to wake up *chunkedPutContextStruct.Read()
	//                                            will be closed to indicate a flush is requested
	inRead         bool //                      Set when in Read() as a hint to Close() to help Read() cleanly exit
	flushRequested bool //                      Set to remember that a flush has been requested of *chunkedPutContextStruct.Read()
}

type statsStruct struct {
	DeleteVolumeUsecs  bucketstats.BucketLog2Round // DELETE /volume/<volumeName>
	GetConfigUsecs     bucketstats.BucketLog2Round // GET /config
	GetStatsUsecs      bucketstats.BucketLog2Round // GET /stats
	GetVolumeListUsecs bucketstats.BucketLog2Round // GET /volume
	GetVolumeUsecs     bucketstats.BucketLog2Round // GET /volume/<volumeName>
	PostVolumeUsecs    bucketstats.BucketLog2Round // POST /volume/<volumeName>
	PutVolumeUsecs     bucketstats.BucketLog2Round // PUT /volume/<volumeName>

	AdjustInodeTableEntryOpenCountUsecs bucketstats.BucketLog2Round // (*RetryRPCServerStruct).AdjustInodeTableEntryOpenCount()
	DeleteInodeTableEntryUsecs          bucketstats.BucketLog2Round // (*RetryRPCServerStruct).DeleteInodeTableEntry()
	FetchNonceRangeUsecs                bucketstats.BucketLog2Round // (*RetryRPCServerStruct).FetchNonceRange()
	FlushUsecs                          bucketstats.BucketLog2Round // (*RetryRPCServerStruct).Flush()
	GetInodeTableEntryUsecs             bucketstats.BucketLog2Round // (*RetryRPCServerStruct).GetInodeTableEntry()
	LeaseUsecs                          bucketstats.BucketLog2Round // (*RetryRPCServerStruct).Lease()
	MountUsecs                          bucketstats.BucketLog2Round // (*RetryRPCServerStruct).Mount()
	PutInodeTableEntriesUsecs           bucketstats.BucketLog2Round // (*RetryRPCServerStruct).PutInodeTableEntries()
	RenewMountUsecs                     bucketstats.BucketLog2Round // (*RetryRPCServerStruct).RenewMount()
	UnmountUsecs                        bucketstats.BucketLog2Round // (*RetryRPCServerStruct).Unmount()

	SharedLeaseRequestUsecs    bucketstats.BucketLog2Round
	PromoteLeaseRequestUsecs   bucketstats.BucketLog2Round
	ExclusiveLeaseRequestUsecs bucketstats.BucketLog2Round
	DemoteLeaseRequestUsecs    bucketstats.BucketLog2Round
	ReleaseLeaseRequestUsecs   bucketstats.BucketLog2Round

	UnmountInterrupts     bucketstats.Totaler
	DemoteLeaseInterrupts bucketstats.Totaler
	RevokeLeaseInterrupts bucketstats.Totaler

	VolumeCheckpointUsecs bucketstats.BucketLog2Round

	InodeTableCacheHits   bucketstats.Totaler
	InodeTableCacheMisses bucketstats.Totaler

	SwiftContainerHeaderGetUsecs bucketstats.BucketLog2Round
	SwiftContainerHeaderSetUsecs bucketstats.BucketLog2Round
	SwiftObjectGetRangeUsecs     bucketstats.BucketLog2Round
	SwiftObjectGetTailUsecs      bucketstats.BucketLog2Round
	SwiftObjectPutUsecs          bucketstats.BucketLog2Round
}

type mountStruct struct {
	//                              reentrancy covered by volumeStruct's sync.RWMutex
	volume         *volumeStruct // volume.{R|}Lock() also protects each mountStruct
	mountID        string        //
	authTokenValid bool          // if true, mount is "healthy"; if false, mount is "expired"
	authToken      string        //
	lastAuthTime   time.Time     // used to periodically check TTL of authToken
	listElement    *list.Element // LRU element on either volumeStruct.{healthy|expired}MountList
}

type volumeStruct struct {
	sync.RWMutex                             // must globals.{R|}Lock() before volume.{R|}Lock()
	name             string                  //
	storageURL       string                  //
	mountMap         map[string]*mountStruct // key == mountStruct.mountID
	healthyMountList *list.List              // LRU of mountStruct's with .authTokenValid == true
	expiredMountList *list.List              // LRU of mountStruct's with .authTokenValid == false
	deleting         bool                    //
	inodeTable       sortedmap.BPlusTree     //
}

type globalsStruct struct {
	sync.RWMutex                             //
	config          configStruct             //
	logFile         *os.File                 // == nil if config.LogFilePath == ""
	inodeTableCache sortedmap.BPlusTreeCache //
	volumeMap       sortedmap.LLRBTree       // key == volumeStruct.name; value == *volumeStruct
	mountMap        map[string]*mountStruct  // key == mountStruct.mountID
	httpClient      *http.Client             //
	retryrpcServer  *retryrpc.Server         //
	httpServer      *http.Server             //
	httpServerWG    sync.WaitGroup           //
	stats           *statsStruct             //
}

var globals globalsStruct

func initializeGlobals(confMap conf.ConfMap) (err error) {
	var (
		configJSONified string
	)

	// Default logging related globals

	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true
	globals.logFile = nil

	// Process resultant confMap

	globals.config.PublicIPAddr, err = confMap.FetchOptionValueString("IMGR", "PublicIPAddr")
	if nil != err {
		logFatal(err)
	}
	globals.config.PrivateIPAddr, err = confMap.FetchOptionValueString("IMGR", "PrivateIPAddr")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCPort, err = confMap.FetchOptionValueUint16("IMGR", "RetryRPCPort")
	if nil != err {
		logFatal(err)
	}
	globals.config.HTTPServerPort, err = confMap.FetchOptionValueUint16("IMGR", "HTTPServerPort")
	if nil != err {
		logFatal(err)
	}
	globals.config.HTTPServerPort, err = confMap.FetchOptionValueUint16("IMGR", "HTTPServerPort")
	if nil != err {
		logFatal(err)
	}

	globals.config.RetryRPCTTLCompleted, err = confMap.FetchOptionValueDuration("IMGR", "RetryRPCTTLCompleted")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCAckTrim, err = confMap.FetchOptionValueDuration("IMGR", "RetryRPCAckTrim")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCDeadlineIO, err = confMap.FetchOptionValueDuration("IMGR", "RetryRPCDeadlineIO")
	if nil != err {
		logFatal(err)
	}
	globals.config.RetryRPCKeepAlivePeriod, err = confMap.FetchOptionValueDuration("IMGR", "RetryRPCKeepAlivePeriod")
	if nil != err {
		logFatal(err)
	}

	err = confMap.VerifyOptionIsMissing("IMGR", "RetryRPCCertFilePath")
	if nil == err {
		err = confMap.VerifyOptionIsMissing("IMGR", "RetryRPCKeyFilePath")
		if nil == err {
			globals.config.RetryRPCCertFilePath = ""
			globals.config.RetryRPCKeyFilePath = ""
		} else {
			err = confMap.VerifyOptionValueIsEmpty("IMGR", "RetryRPCKeyFilePath")
			if nil == err {
				globals.config.RetryRPCCertFilePath = ""
				globals.config.RetryRPCKeyFilePath = ""
			} else {
				err = fmt.Errorf("[IMGR]RetryRPCCertFilePath is missing but [IMGR]RetryRPCKeyFilePath is present and non-empty")
				logFatal(err)
			}
		}
	} else {
		err = confMap.VerifyOptionValueIsEmpty("IMGR", "RetryRPCCertFilePath")
		if nil == err {
			err = confMap.VerifyOptionIsMissing("IMGR", "RetryRPCKeyFilePath")
			if nil == err {
				globals.config.RetryRPCCertFilePath = ""
				globals.config.RetryRPCKeyFilePath = ""
			} else {
				err = confMap.VerifyOptionValueIsEmpty("IMGR", "RetryRPCKeyFilePath")
				if nil == err {
					globals.config.RetryRPCCertFilePath = ""
					globals.config.RetryRPCKeyFilePath = ""
				} else {
					err = fmt.Errorf("[IMGR]RetryRPCCertFilePath is present and empty but [IMGR]RetryRPCKeyFilePath is present and non-empty")
					logFatal(err)
				}
			}
		} else {
			globals.config.RetryRPCCertFilePath, err = confMap.FetchOptionValueString("IMGR", "RetryRPCCertFilePath")
			if nil != err {
				logFatal(err)
			} else {
				globals.config.RetryRPCKeyFilePath, err = confMap.FetchOptionValueString("IMGR", "RetryRPCKeyFilePath")
				if nil != err {
					logFatal(err)
				}
			}
		}
	}

	globals.config.FetchNonceRangeToReturn, err = confMap.FetchOptionValueUint64("IMGR", "FetchNonceRangeToReturn")
	if nil != err {
		logFatal(err)
	}

	globals.config.MinLeaseDuration, err = confMap.FetchOptionValueDuration("IMGR", "MinLeaseDuration")
	if nil != err {
		logFatal(err)
	}
	globals.config.LeaseInterruptInterval, err = confMap.FetchOptionValueDuration("IMGR", "LeaseInterruptInterval")
	if nil != err {
		logFatal(err)
	}
	globals.config.LeaseInterruptLimit, err = confMap.FetchOptionValueUint32("IMGR", "LeaseInterruptLimit")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftRetryDelay, err = confMap.FetchOptionValueDuration("IMGR", "SwiftRetryDelay")
	if nil != err {
		logFatal(err)
	}
	globals.config.SwiftRetryExpBackoff, err = confMap.FetchOptionValueFloat64("IMGR", "SwiftRetryExpBackoff")
	if nil != err {
		logFatal(err)
	}
	globals.config.SwiftRetryLimit, err = confMap.FetchOptionValueUint32("IMGR", "SwiftRetryLimit")
	if nil != err {
		logFatal(err)
	}

	globals.config.SwiftTimeout, err = confMap.FetchOptionValueDuration("IMGR", "SwiftTimeout")
	if nil != err {
		logFatal(err)
	}
	globals.config.SwiftConnectionPoolSize, err = confMap.FetchOptionValueUint32("IMGR", "SwiftConnectionPoolSize")
	if nil != err {
		logFatal(err)
	}

	globals.config.InodeTableCacheEvictLowLimit, err = confMap.FetchOptionValueUint64("IMGR", "InodeTableCacheEvictLowLimit")
	if nil != err {
		logFatal(err)
	}
	globals.config.InodeTableCacheEvictHighLimit, err = confMap.FetchOptionValueUint64("IMGR", "InodeTableCacheEvictHighLimit")
	if nil != err {
		logFatal(err)
	}

	globals.config.InodeTableMaxInodesPerBPlusTreePage, err = confMap.FetchOptionValueUint64("IMGR", "InodeTableMaxInodesPerBPlusTreePage")
	if nil != err {
		logFatal(err)
	}
	globals.config.RootDirMaxDirEntriesPerBPlusTreePage, err = confMap.FetchOptionValueUint64("IMGR", "RootDirMaxDirEntriesPerBPlusTreePage")
	if nil != err {
		logFatal(err)
	}

	globals.config.LogFilePath, err = confMap.FetchOptionValueString("IMGR", "LogFilePath")
	if nil != err {
		err = confMap.VerifyOptionValueIsEmpty("IMGR", "LogFilePath")
		if nil == err {
			globals.config.LogFilePath = ""
		} else {
			logFatalf("[IMGR]LogFilePath must either be a valid string or empty]")
		}
	}
	globals.config.LogToConsole, err = confMap.FetchOptionValueBool("IMGR", "LogToConsole")
	if nil != err {
		logFatal(err)
	}
	globals.config.TraceEnabled, err = confMap.FetchOptionValueBool("IMGR", "TraceEnabled")
	if nil != err {
		logFatal(err)
	}

	configJSONified = utils.JSONify(globals.config, true)

	logInfof("globals.config:\n%s", configJSONified)

	globals.stats = &statsStruct{}

	bucketstats.Register("IMGR", "", globals.stats)

	err = nil
	return
}

func uninitializeGlobals() (err error) {
	globals.config.PrivateIPAddr = ""
	globals.config.PublicIPAddr = ""
	globals.config.RetryRPCPort = 0
	globals.config.HTTPServerPort = 0

	globals.config.RetryRPCTTLCompleted = time.Duration(0)
	globals.config.RetryRPCAckTrim = time.Duration(0)
	globals.config.RetryRPCDeadlineIO = time.Duration(0)
	globals.config.RetryRPCKeepAlivePeriod = time.Duration(0)

	globals.config.RetryRPCCertFilePath = ""
	globals.config.RetryRPCKeyFilePath = ""

	globals.config.FetchNonceRangeToReturn = 0

	globals.config.MinLeaseDuration = time.Duration(0)
	globals.config.LeaseInterruptInterval = time.Duration(0)
	globals.config.LeaseInterruptLimit = 0

	globals.config.SwiftRetryDelay = time.Duration(0)
	globals.config.SwiftRetryExpBackoff = 0.0
	globals.config.SwiftRetryLimit = 0

	globals.config.SwiftTimeout = time.Duration(0)
	globals.config.SwiftConnectionPoolSize = 0

	globals.config.InodeTableCacheEvictLowLimit = 0
	globals.config.InodeTableCacheEvictHighLimit = 0

	globals.config.InodeTableMaxInodesPerBPlusTreePage = 0
	globals.config.RootDirMaxDirEntriesPerBPlusTreePage = 0

	globals.config.LogFilePath = ""
	globals.config.LogToConsole = false
	globals.config.TraceEnabled = false

	bucketstats.UnRegister("IMGR", "")

	err = nil
	return
}
