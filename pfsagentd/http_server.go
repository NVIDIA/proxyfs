package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/version"
)

func serveHTTP() {
	var (
		ipAddrTCPPort string
	)

	ipAddrTCPPort = net.JoinHostPort(globals.config.HTTPServerIPAddr, strconv.Itoa(int(globals.config.HTTPServerTCPPort)))

	globals.httpServer = &http.Server{
		Addr:    ipAddrTCPPort,
		Handler: &globals,
	}

	globals.httpServerWG.Add(1)

	go func() {
		var (
			err error
		)

		err = globals.httpServer.ListenAndServe()
		if http.ErrServerClosed != err {
			log.Fatalf("httpServer.ListenAndServe() exited unexpectedly: %v", err)
		}

		globals.httpServerWG.Done()
	}()
}

func unserveHTTP() {
	var (
		err error
	)

	err = globals.httpServer.Shutdown(context.TODO())
	if nil != err {
		log.Fatalf("httpServer.Shutdown() returned with an error: %v", err)
	}

	globals.httpServerWG.Wait()
}

func (dummy *globalsStruct) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		serveGet(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func serveGet(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		path string
	)

	path = strings.TrimRight(request.URL.Path, "/")

	switch path {
	case "/config":
		serveGetOfConfig(responseWriter, request)
	case "/debug/pprof":
		pprof.Index(responseWriter, request)
	case "/debug/pprof/cmdline":
		pprof.Cmdline(responseWriter, request)
	case "/debug/pprof/profile":
		pprof.Profile(responseWriter, request)
	case "/debug/pprof/symbol":
		pprof.Symbol(responseWriter, request)
	case "/debug/pprof/trace":
		pprof.Trace(responseWriter, request)
	case "/metrics":
		serveGetOfMetrics(responseWriter, request)
	case "/stats":
		serveGetOfStats(responseWriter, request)
	case "/version":
		serveGetOfVersion(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func serveGetOfConfig(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		confMapJSON       bytes.Buffer
		confMapJSONPacked []byte
		ok                bool
		paramList         []string
		sendPackedConfig  bool
	)

	paramList, ok = request.URL.Query()["compact"]
	if ok {
		if 0 == len(paramList) {
			sendPackedConfig = false
		} else {
			sendPackedConfig = !((paramList[0] == "") || (paramList[0] == "0") || (paramList[0] == "false"))
		}
	} else {
		sendPackedConfig = false
	}

	confMapJSONPacked, _ = json.Marshal(globals.config)

	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)

	if sendPackedConfig {
		_, _ = responseWriter.Write(confMapJSONPacked)
	} else {
		json.Indent(&confMapJSON, confMapJSONPacked, "", "\t")
		_, _ = responseWriter.Write(confMapJSON.Bytes())
		_, _ = responseWriter.Write([]byte("\n"))
	}
}

func serveGetOfMetrics(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err                  error
		format               string
		i                    int
		keyAsKey             sortedmap.Key
		keyAsString          string
		line                 string
		longestKeyAsString   int
		longestValueAsString int
		memStats             runtime.MemStats
		metricsFieldName     string
		metricsFieldValuePtr *uint64
		metricsLLRB          sortedmap.LLRBTree
		metricsLLRBLen       int
		metricsStructValue   reflect.Value
		metricsValue         reflect.Value
		ok                   bool
		pauseNsAccumulator   uint64
		valueAsString        string
		valueAsValue         sortedmap.Value
	)

	runtime.ReadMemStats(&memStats)

	metricsLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

	// General statistics.
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_Alloc", memStats.Alloc)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_TotalAlloc", memStats.TotalAlloc)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_Sys", memStats.Sys)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_Lookups", memStats.Lookups)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_Mallocs", memStats.Mallocs)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_Frees", memStats.Frees)

	// Main allocation heap statistics.
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_HeapAlloc", memStats.HeapAlloc)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_HeapSys", memStats.HeapSys)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_HeapIdle", memStats.HeapIdle)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_HeapInuse", memStats.HeapInuse)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_HeapReleased", memStats.HeapReleased)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_HeapObjects", memStats.HeapObjects)

	// Low-level fixed-size structure allocator statistics.
	//	Inuse is bytes used now.
	//	Sys is bytes obtained from system.
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_StackInuse", memStats.StackInuse)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_StackSys", memStats.StackSys)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_MSpanInuse", memStats.MSpanInuse)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_MSpanSys", memStats.MSpanSys)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_MCacheInuse", memStats.MCacheInuse)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_MCacheSys", memStats.MCacheSys)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_BuckHashSys", memStats.BuckHashSys)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_GCSys", memStats.GCSys)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_OtherSys", memStats.OtherSys)

	// Garbage collector statistics (fixed portion).
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_LastGC", memStats.LastGC)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_PauseTotalNs", memStats.PauseTotalNs)
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_NumGC", uint64(memStats.NumGC))
	insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_GCCPUPercentage", uint64(100.0*memStats.GCCPUFraction))

	// Garbage collector statistics (go_runtime_MemStats_PauseAverageNs).
	if 0 == memStats.NumGC {
		insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_PauseAverageNs", 0)
	} else {
		pauseNsAccumulator = 0
		if memStats.NumGC < 255 {
			for i = 0; i < int(memStats.NumGC); i++ {
				pauseNsAccumulator += memStats.PauseNs[i]
			}
			insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_PauseAverageNs", pauseNsAccumulator/uint64(memStats.NumGC))
		} else {
			for i = 0; i < 256; i++ {
				pauseNsAccumulator += memStats.PauseNs[i]
			}
			insertInMetricsLLRB(metricsLLRB, "go_runtime_MemStats_PauseAverageNs", pauseNsAccumulator/256)
		}
	}

	// Add in locally generated metrics

	metricsStructValue = reflect.Indirect(reflect.ValueOf(globals.metrics))
	metricsValue = reflect.ValueOf(globals.metrics).Elem()

	for i = 0; i < metricsStructValue.NumField(); i++ {
		metricsFieldName = metricsStructValue.Type().Field(i).Name
		metricsFieldValuePtr = metricsValue.Field(i).Addr().Interface().(*uint64)
		insertInMetricsLLRB(metricsLLRB, metricsFieldName, atomic.LoadUint64(metricsFieldValuePtr))
	}

	// Produce sorted and column-aligned response

	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)

	metricsLLRBLen, err = metricsLLRB.Len()
	if nil != err {
		logFatalf("metricsLLRB.Len() failed: %v", err)
	}

	longestKeyAsString = 0
	longestValueAsString = 0

	for i = 0; i < metricsLLRBLen; i++ {
		keyAsKey, valueAsValue, ok, err = metricsLLRB.GetByIndex(i)
		if nil != err {
			logFatalf("llrb.GetByIndex(%v) failed: %v", i, err)
		}
		if !ok {
			logFatalf("llrb.GetByIndex(%v) returned ok == false", i)
		}
		keyAsString = keyAsKey.(string)
		valueAsString = valueAsValue.(string)
		if len(keyAsString) > longestKeyAsString {
			longestKeyAsString = len(keyAsString)
		}
		if len(valueAsString) > longestValueAsString {
			longestValueAsString = len(valueAsString)
		}
	}

	format = fmt.Sprintf("%%-%vs %%%vs\n", longestKeyAsString, longestValueAsString)

	for i = 0; i < metricsLLRBLen; i++ {
		keyAsKey, valueAsValue, ok, err = metricsLLRB.GetByIndex(i)
		if nil != err {
			logFatalf("llrb.GetByIndex(%v) failed: %v", i, err)
		}
		if !ok {
			logFatalf("llrb.GetByIndex(%v) returned ok == false", i)
		}
		keyAsString = keyAsKey.(string)
		valueAsString = valueAsValue.(string)
		line = fmt.Sprintf(format, keyAsString, valueAsString)
		_, _ = responseWriter.Write([]byte(line))
	}
}

func serveGetOfStats(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write([]byte(bucketstats.SprintStats(bucketstats.StatFormatParsable1, "PFSAgent", "")))
}

func insertInMetricsLLRB(metricsLLRB sortedmap.LLRBTree, metricKey string, metricValueAsUint64 uint64) {
	var (
		err                 error
		metricValueAsString string
		ok                  bool
	)

	metricValueAsString = fmt.Sprintf("%v", metricValueAsUint64)

	ok, err = metricsLLRB.Put(metricKey, metricValueAsString)
	if nil != err {
		logFatalf("metricsLLRB.Put(%v, %v) failed: %v", metricKey, metricValueAsString, err)
	}
	if !ok {
		logFatalf("metricsLLRB.Put(%v, %v) returned ok == false", metricKey, metricValueAsString)
	}
}

func serveGetOfVersion(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write([]byte(version.ProxyFSVersion))
}
