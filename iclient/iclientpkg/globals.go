// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package iclientpkg

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/utils"
)

type configStruct struct {
	VolumeName              string
	MountPointDirPath       string
	AllowOther              bool
	PlugInPath              string
	PlugInEnvName           string
	PlugInEnvValue          string
	RetryRPCPublicIPAddr    string
	RetryRPCPort            uint16
	RetryRPCDeadlineIO      time.Duration
	RetryRPCKeepAlivePeriod time.Duration
	RetryRPCCACertFilePath  string // Defaults to /dev/null
	LogFilePath             string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole            bool
	TraceEnabled            bool
}

type statsStruct struct {
	AdjustInodeTableEntryOpenCountUsecs bucketstats.BucketLog2Round // imgrpkg AdjustInodeTableEntryOpenCount
	DeleteInodeTableEntryUsecs          bucketstats.BucketLog2Round // imgrpkg DeleteInodeTableEntry
	FetchNonceRangeUsecs                bucketstats.BucketLog2Round // imgrpkg FetchNonceRange
	FlushUsecs                          bucketstats.BucketLog2Round // imgrpkg Flush
	GetInodeTableEntryUsecs             bucketstats.BucketLog2Round // imgrpkg GetInodeTableEntry
	LeaseUsecs                          bucketstats.BucketLog2Round // imgrpkg Lease
	MountUsecs                          bucketstats.BucketLog2Round // imgrpkg Mount
	PutInodeTableEntriesUsecs           bucketstats.BucketLog2Round // imgrpkg PutInodeTableEntries
	RenewMountUsecs                     bucketstats.BucketLog2Round // imgrpkg RenewMount
	UnmountUsecs                        bucketstats.BucketLog2Round // imgrpkg Unmount
}

type globalsStruct struct {
	config            configStruct //
	logFile           *os.File     // == nil if config.LogFilePath == ""
	retryRPCCACertPEM []byte       // == nil if config.RetryRPCCACertFilePath == ""
	stats             *statsStruct //
}

var globals globalsStruct

func initializeGlobals(confMap conf.ConfMap) (err error) {
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
	globals.config.AllowOther, err = confMap.FetchOptionValueBool("ICLIENT", "AllowOther")
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

	globals.stats = &statsStruct{}

	bucketstats.Register("ICLIENT", "", globals.stats)

	err = nil
	return
}

func uninitializeGlobals() (err error) {
	globals.config.VolumeName = ""
	globals.config.MountPointDirPath = ""
	globals.config.AllowOther = false
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

	globals.retryRPCCACertPEM = nil

	bucketstats.UnRegister("ICLIENT", "")

	err = nil
	return
}
