package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"bazil.org/fuse"

	"github.com/swiftstack/ProxyFS/conf"
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
	ReadPlanLineSize        uint64 // ReadPlan covering an aligned chunk of File Data
	ReadPlanLineCount       uint64
	LogFilePath             string // Unless starting with '/', relative to $CWD; == "" means disabled
	LogToConsole            bool
	TraceEnabled            bool
}

type globalsStruct struct {
	sync.Mutex
	config             configStruct
	logFile            *os.File // == nil if configStruct.LogFilePath == ""
	httpClient         *http.Client
	retryDelay         []time.Duration
	swiftAuthWaitGroup *sync.WaitGroup
	swiftAuthToken     string
	swiftAccountURL    string // swiftStorageURL with AccountName forced to config.SwiftAccountName
	fuseConn           *fuse.Conn
}

var globals globalsStruct

func main() {
	var (
		args       []string
		confMap    conf.ConfMap
		err        error
		signalChan chan os.Signal
	)

	// Setup signal catcher for clean shutdown

	signalChan = make(chan os.Signal, 1)

	signal.Notify(signalChan, unix.SIGHUP, unix.SIGINT, unix.SIGTERM)

	// Parse arguments (at this point, logging goes only to the console)

	globals.logFile = nil
	globals.config.LogFilePath = ""
	globals.config.LogToConsole = true

	args = os.Args[1:]

	if 0 == len(args) {
		logFatalf("no .conf file specified")
	}

	confMap, err = conf.MakeConfMapFromFile(args[0])
	if nil != err {
		logFatalf("failed to load config: %v", err)
	}

	err = confMap.UpdateFromStrings(args[1:])
	if nil != err {
		logFatalf("failed to load config overrides: %v", err)
	}

	// Initialize globals

	initializeGlobals(confMap)

	// Start serving FUSE mount point

	performMount()

	// Await SIGHUP, SIGINT, or SIGTERM

	_ = <-signalChan

	// Perform clean shutdown

	performUnmount()
}

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

	globals.config.ReadPlanLineSize, err = confMap.FetchOptionValueUint64("Agent", "ReadPlanLineSize")
	if nil != err {
		logFatal(err)
	}

	globals.config.ReadPlanLineCount, err = confMap.FetchOptionValueUint64("Agent", "ReadPlanLineCount")
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
}
