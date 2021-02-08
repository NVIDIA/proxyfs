// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"container/list"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/conf"
)

type configStruct struct {
	PrivateIPAddr  string
	PublicIPAddr   string
	JSONRPCPort    uint16 // To be served only on PrivateIPAddr via TCP
	RetryRPCPort   uint16 // To be served only on PublicIPAddr via TLS
	HTTPServerPort uint16 // To be served only on PrivateIPAddr via TCP

	RetryRPCTTLCompleted    time.Duration
	RetryRPCAckTrim         time.Duration
	RetryRPCDeadlineIO      time.Duration
	RetryRPCKeepAlivePeriod time.Duration

	MinLeaseDuration       time.Duration
	LeaseInterruptInterval time.Duration
	LeaseInterruptLimit    uint32

	SwiftNoAuthIPAddr  string
	SwiftNoAuthTCPPort uint16

	SwiftRetryDelay      time.Duration
	SwiftRetryExpBackoff float64
	SwiftRetryLimit      uint32

	SwiftConnectionPoolSize uint32

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
	SharedLeaseRequests    bucketstats.BucketLog2Round
	PromoteLeaseRequests   bucketstats.BucketLog2Round
	ExclusiveLeaseRequests bucketstats.BucketLog2Round
	DemoteLeaseRequests    bucketstats.BucketLog2Round
	ReleaseLeaseRequests   bucketstats.BucketLog2Round

	DemoteLeaseInterrupts bucketstats.Totaler
	RevokeLeaseInterrupts bucketstats.Totaler

	InodeGets bucketstats.BucketLog2Round
	InodePuts bucketstats.BucketLog2Round
}

type globalsStruct struct {
	sync.RWMutex
	config  configStruct
	logFile *os.File // == nil if config.LogFilePath == ""
	stats   *statsStruct
}

var globals globalsStruct

func initializeGlobals(confMap conf.ConfMap) {
	var (
		err error
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
	globals.config.JSONRPCPort, err = confMap.FetchOptionValueUint16("IMGR", "JSONRPCPort")
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

	globals.config.SwiftNoAuthIPAddr, err = confMap.FetchOptionValueString("IMGR", "SwiftNoAuthIPAddr")
	if nil != err {
		logFatal(err)
	}
	globals.config.SwiftNoAuthTCPPort, err = confMap.FetchOptionValueUint16("IMGR", "SwiftNoAuthTCPPort")
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

	globals.config.SwiftConnectionPoolSize, err = confMap.FetchOptionValueUint32("IMGR", "SwiftConnectionPoolSize")
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

	globals.stats = &statsStruct{}

	bucketstats.Register("IMGR", "", globals.stats)
}

func uninitializeGlobals() {
	globals.config.PrivateIPAddr = ""
	globals.config.PublicIPAddr = ""
	globals.config.JSONRPCPort = 0
	globals.config.RetryRPCPort = 0
	globals.config.HTTPServerPort = 0

	globals.config.RetryRPCTTLCompleted = time.Duration(0)
	globals.config.RetryRPCAckTrim = time.Duration(0)
	globals.config.RetryRPCDeadlineIO = time.Duration(0)
	globals.config.RetryRPCKeepAlivePeriod = time.Duration(0)

	globals.config.MinLeaseDuration = time.Duration(0)
	globals.config.LeaseInterruptInterval = time.Duration(0)
	globals.config.LeaseInterruptLimit = 0

	globals.config.SwiftNoAuthIPAddr = ""
	globals.config.SwiftNoAuthTCPPort = 0

	globals.config.SwiftRetryDelay = time.Duration(0)
	globals.config.SwiftRetryExpBackoff = 0.0
	globals.config.SwiftRetryLimit = 0

	globals.config.SwiftConnectionPoolSize = 0

	globals.config.LogFilePath = ""
	globals.config.LogToConsole = false
	globals.config.TraceEnabled = false

	bucketstats.UnRegister("IMGR", "")
}
