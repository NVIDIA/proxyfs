// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/fission"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/utils"
)

type configStruct struct {
	VolumeName               string
	MountPointDirPath        string
	FUSEAllowOther           bool
	FUSEMaxBackground        uint16
	FUSECongestionThreshhold uint16
	FUSEMaxWrite             uint32
	PlugInPath               string
	PlugInEnvName            string
	PlugInEnvValue           string
	RetryRPCPublicIPAddr     string
	RetryRPCPort             uint16
	RetryRPCDeadlineIO       time.Duration
	RetryRPCKeepAlivePeriod  time.Duration
	RetryRPCCACertFilePath   string // Defaults to /dev/null
	LogFilePath              string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole             bool
	TraceEnabled             bool
	HTTPServerIPAddr         string
	HTTPServerPort           uint16 // To be served on HTTPServerIPAddr via TCP
}

type statsStruct struct {
	GetConfigUsecs  bucketstats.BucketLog2Round // GET /config
	GetStatsUsecs   bucketstats.BucketLog2Round // GET /stats
	GetVersionUsecs bucketstats.BucketLog2Round // GET /version

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
	config            configStruct   //
	logFile           *os.File       // == nil if config.LogFilePath == ""
	retryRPCCACertPEM []byte         // == nil if config.RetryRPCCACertFilePath == ""
	fissionErrChan    chan error     //
	httpServer        *http.Server   //
	httpServerWG      sync.WaitGroup //
	stats             *statsStruct   //
	fissionVolume     fission.Volume //
}

var globals globalsStruct

func initializeGlobals(confMap conf.ConfMap, fissionErrChan chan error) (err error) {
	var (
		configJSONified     string
		plugInEnvValueSlice []string
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
	globals.config.PlugInPath, err = confMap.FetchOptionValueString("ICLIENT", "PlugInPath")
	if nil != err {
		logFatal(err)
	}
	globals.config.PlugInEnvName, err = confMap.FetchOptionValueString("ICLIENT", "PlugInEnvName")
	if nil != err {
		logFatal(err)
	}
	globals.config.PlugInEnvValue, err = confMap.FetchOptionValueString("ICLIENT", "PlugInEnvValue")
	if nil != err {
		logFatal(err)
	}
	err = confMap.VerifyOptionIsMissing("ICLIENT", "PlugInEnvValue")
	if nil == err {
		globals.config.PlugInEnvValue = ""
	} else {
		plugInEnvValueSlice, err = confMap.FetchOptionValueStringSlice("ICLIENT", "PlugInEnvValue")
		if nil != err {
			logFatal(err)
		} else {
			switch len(plugInEnvValueSlice) {
			case 0:
				globals.config.PlugInEnvValue = ""
			case 1:
				globals.config.PlugInEnvValue = plugInEnvValueSlice[0]
			default:
				log.Fatalf("[ICLIENT]PlugInEnvValue must be missing, empty, or single-valued: %#v", plugInEnvValueSlice)
			}
		}
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

	if "" == globals.config.RetryRPCCACertFilePath {
		globals.retryRPCCACertPEM = nil
	} else {
		globals.retryRPCCACertPEM, err = ioutil.ReadFile(globals.config.RetryRPCCACertFilePath)
		if nil != err {
			logFatal(err)
		}
	}

	globals.fissionErrChan = fissionErrChan

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
	globals.config.PlugInPath = ""
	globals.config.PlugInEnvValue = ""
	globals.config.RetryRPCPublicIPAddr = ""
	globals.config.RetryRPCPort = 0
	globals.config.RetryRPCDeadlineIO = time.Duration(0)
	globals.config.RetryRPCKeepAlivePeriod = time.Duration(0)
	globals.config.RetryRPCCACertFilePath = ""
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = false
	globals.config.TraceEnabled = false
	globals.config.HTTPServerIPAddr = ""
	globals.config.HTTPServerPort = 0

	globals.retryRPCCACertPEM = nil

	globals.fissionErrChan = nil

	bucketstats.UnRegister("ICLIENT", "")

	err = nil
	return
}
