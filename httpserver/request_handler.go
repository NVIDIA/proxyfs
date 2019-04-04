package httpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/pprof"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/bucketstats"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/halter"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/liveness"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/utils"
	"github.com/swiftstack/ProxyFS/version"
)

type httpRequestHandler struct{}

func serveHTTP() {
	_ = http.Serve(globals.netListener, httpRequestHandler{})
	globals.wg.Done()
}

func (h httpRequestHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	globals.Lock()
	if globals.active {
		switch request.Method {
		case http.MethodDelete:
			doDelete(responseWriter, request)
		case http.MethodGet:
			doGet(responseWriter, request)
		case http.MethodPost:
			doPost(responseWriter, request)
		default:
			responseWriter.WriteHeader(http.StatusMethodNotAllowed)
		}
	} else {
		responseWriter.WriteHeader(http.StatusServiceUnavailable)
	}
	globals.Unlock()
}

func doDelete(responseWriter http.ResponseWriter, request *http.Request) {
	switch {
	case strings.HasPrefix(request.URL.Path, "/volume"):
		doDeleteOfVolume(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doDeleteOfVolume(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err           error
		numPathParts  int
		ok            bool
		pathSplit     []string
		snapShotID    uint64
		volume        *volumeStruct
		volumeAsValue sortedmap.Value
		volumeName    string
	)

	pathSplit = strings.Split(request.URL.Path, "/") // leading  "/" places "" in pathSplit[0]
	//                                                  pathSplit[1] should be "volume" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]
	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
	}

	if "volume" != pathSplit[1] {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	if 4 != numPathParts {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	volumeName = pathSplit[2]

	volumeAsValue, ok, err = globals.volumeLLRB.GetByKey(volumeName)
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}
	if !ok {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	volume = volumeAsValue.(*volumeStruct)

	if "snapshot" != pathSplit[3] {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	// Form: /volume/<volume-name>/snapshot/<snapshot-id>

	snapShotID, err = strconv.ParseUint(pathSplit[4], 10, 64)
	if nil != err {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	err = volume.inodeVolumeHandle.SnapShotDelete(snapShotID)
	if nil == err {
		responseWriter.WriteHeader(http.StatusNoContent)
	} else {
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doGet(responseWriter http.ResponseWriter, request *http.Request) {
	path := strings.TrimRight(request.URL.Path, "/")

	switch {
	case "" == path:
		doGetOfIndexDotHTML(responseWriter, request)
	case "/bootstrap.min.css" == path:
		responseWriter.Header().Set("Content-Type", bootstrapDotCSSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(bootstrapDotCSSContent))
	case "/bootstrap.min.js" == path:
		responseWriter.Header().Set("Content-Type", bootstrapDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(bootstrapDotJSContent))
	case "/config" == path:
		doGetOfConfig(responseWriter, request)
	case "/index.html" == path:
		doGetOfIndexDotHTML(responseWriter, request)
	case "/jquery-3.2.1.min.js" == path:
		responseWriter.Header().Set("Content-Type", jqueryDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(jqueryDotJSContent))
	case "/jsontree.js" == path:
		responseWriter.Header().Set("Content-Type", jsontreeDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(jsontreeDotJSContent))
	case "/liveness" == path:
		doGetOfLiveness(responseWriter, request)
	case "/metrics" == path:
		doGetOfMetrics(responseWriter, request)
	case "/stats" == path:
		doGetOfStats(responseWriter, request)
	case "/debug/pprof" == path:
		pprof.Index(responseWriter, request)
	case "/debug/pprof/cmdline" == path:
		pprof.Cmdline(responseWriter, request)
	case "/debug/pprof/profile" == path:
		pprof.Profile(responseWriter, request)
	case "/debug/pprof/symbol" == path:
		pprof.Symbol(responseWriter, request)
	case "/debug/pprof/trace" == path:
		pprof.Trace(responseWriter, request)
	case "/open-iconic/font/css/open-iconic-bootstrap.min.css" == path:
		responseWriter.Header().Set("Content-Type", openIconicBootstrapDotCSSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(openIconicBootstrapDotCSSContent))
	case "/open-iconic/font/fonts/open-iconic.eot" == path:
		responseWriter.Header().Set("Content-Type", openIconicDotEOTContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(openIconicDotEOTContent)
	case "/open-iconic/font/fonts/open-iconic.otf" == path:
		responseWriter.Header().Set("Content-Type", openIconicDotOTFContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(openIconicDotOTFContent)
	case "/open-iconic/font/fonts/open-iconic.svg" == path:
		responseWriter.Header().Set("Content-Type", openIconicDotSVGContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(openIconicDotSVGContent))
	case "/open-iconic/font/fonts/open-iconic.ttf" == path:
		responseWriter.Header().Set("Content-Type", openIconicDotTTFContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(openIconicDotTTFContent)
	case "/open-iconic/font/fonts/open-iconic.woff" == path:
		responseWriter.Header().Set("Content-Type", openIconicDotWOFFContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(openIconicDotWOFFContent)
	case "/popper.min.js" == path:
		responseWriter.Header().Set("Content-Type", popperDotJSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write(popperDotJSContent)
	case "/styles.css" == path:
		responseWriter.Header().Set("Content-Type", stylesDotCSSContentType)
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(stylesDotCSSContent))
	case "/version" == path:
		responseWriter.Header().Set("Content-Type", "text/plain")
		responseWriter.WriteHeader(http.StatusOK)
		_, _ = responseWriter.Write([]byte(version.ProxyFSVersion))
	case strings.HasPrefix(request.URL.Path, "/trigger"):
		doGetOfTrigger(responseWriter, request)
	case strings.HasPrefix(request.URL.Path, "/volume"):
		doGetOfVolume(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doGetOfIndexDotHTML(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/html")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write([]byte(fmt.Sprintf(indexDotHTMLTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort)))
}

func doGetOfConfig(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		acceptHeader         string
		confMapJSON          bytes.Buffer
		confMapJSONPacked    []byte
		formatResponseAsJSON bool
		ok                   bool
		paramList            []string
		sendPackedConfig     bool
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

	acceptHeader = request.Header.Get("Accept")

	if strings.Contains(acceptHeader, "application/json") {
		formatResponseAsJSON = true
	} else if strings.Contains(acceptHeader, "text/html") {
		formatResponseAsJSON = false
	} else if strings.Contains(acceptHeader, "*/*") {
		formatResponseAsJSON = true
	} else if strings.Contains(acceptHeader, "") {
		formatResponseAsJSON = true
	} else {
		responseWriter.WriteHeader(http.StatusNotAcceptable)
		return
	}

	confMapJSONPacked, _ = json.Marshal(globals.confMap)

	if formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		if sendPackedConfig {
			_, _ = responseWriter.Write(confMapJSONPacked)
		} else {
			json.Indent(&confMapJSON, confMapJSONPacked, "", "\t")
			_, _ = responseWriter.Write(confMapJSON.Bytes())
			_, _ = responseWriter.Write([]byte("\n"))
		}
	} else {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write([]byte(fmt.Sprintf(configTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, utils.ByteSliceToString(confMapJSONPacked))))
	}
}

func doGetOfLiveness(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		livenessReportAsJSON       bytes.Buffer
		livenessReportAsJSONPacked []byte
		livenessReportAsStruct     *liveness.LivenessReportStruct
		ok                         bool
		paramList                  []string
		sendPackedReport           bool
	)

	// TODO: For now, assume JSON reponse requested

	livenessReportAsStruct = liveness.FetchLivenessReport()

	if nil == livenessReportAsStruct {
		responseWriter.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	livenessReportAsJSONPacked, _ = json.Marshal(livenessReportAsStruct)

	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)

	paramList, ok = request.URL.Query()["compact"]
	if ok {
		if 0 == len(paramList) {
			sendPackedReport = false
		} else {
			sendPackedReport = !((paramList[0] == "") || (paramList[0] == "0") || (paramList[0] == "false"))
		}
	} else {
		sendPackedReport = false
	}

	if sendPackedReport {
		_, _ = responseWriter.Write(livenessReportAsJSONPacked)
	} else {
		json.Indent(&livenessReportAsJSON, livenessReportAsJSONPacked, "", "\t")
		_, _ = responseWriter.Write(livenessReportAsJSON.Bytes())
		_, _ = responseWriter.Write([]byte("\n"))
	}
}

func doGetOfMetrics(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		acceptHeader         string
		err                  error
		formatResponseAsHTML bool
		formatResponseAsJSON bool
		i                    int
		memStats             runtime.MemStats
		metricKey            string
		metricValueAsString  string
		metricValueAsUint64  uint64
		metricsJSON          bytes.Buffer
		metricsJSONPacked    []byte
		metricsLLRB          sortedmap.LLRBTree
		metricsMap           map[string]uint64
		ok                   bool
		paramList            []string
		pauseNsAccumulator   uint64
		sendPackedMetrics    bool
		statKey              string
		statValue            uint64
		statsMap             map[string]uint64
	)

	runtime.ReadMemStats(&memStats)

	metricsMap = make(map[string]uint64)

	// General statistics.
	metricsMap["go_runtime_MemStats_Alloc"] = memStats.Alloc
	metricsMap["go_runtime_MemStats_TotalAlloc"] = memStats.TotalAlloc
	metricsMap["go_runtime_MemStats_Sys"] = memStats.Sys
	metricsMap["go_runtime_MemStats_Lookups"] = memStats.Lookups
	metricsMap["go_runtime_MemStats_Mallocs"] = memStats.Mallocs
	metricsMap["go_runtime_MemStats_Frees"] = memStats.Frees

	// Main allocation heap statistics.
	metricsMap["go_runtime_MemStats_HeapAlloc"] = memStats.HeapAlloc
	metricsMap["go_runtime_MemStats_HeapSys"] = memStats.HeapSys
	metricsMap["go_runtime_MemStats_HeapIdle"] = memStats.HeapIdle
	metricsMap["go_runtime_MemStats_HeapInuse"] = memStats.HeapInuse
	metricsMap["go_runtime_MemStats_HeapReleased"] = memStats.HeapReleased
	metricsMap["go_runtime_MemStats_HeapObjects"] = memStats.HeapObjects

	// Low-level fixed-size structure allocator statistics.
	//	Inuse is bytes used now.
	//	Sys is bytes obtained from system.
	metricsMap["go_runtime_MemStats_StackInuse"] = memStats.StackInuse
	metricsMap["go_runtime_MemStats_StackSys"] = memStats.StackSys
	metricsMap["go_runtime_MemStats_MSpanInuse"] = memStats.MSpanInuse
	metricsMap["go_runtime_MemStats_MSpanSys"] = memStats.MSpanSys
	metricsMap["go_runtime_MemStats_MCacheInuse"] = memStats.MCacheInuse
	metricsMap["go_runtime_MemStats_MCacheSys"] = memStats.MCacheSys
	metricsMap["go_runtime_MemStats_BuckHashSys"] = memStats.BuckHashSys
	metricsMap["go_runtime_MemStats_GCSys"] = memStats.GCSys
	metricsMap["go_runtime_MemStats_OtherSys"] = memStats.OtherSys

	// Garbage collector statistics (fixed portion).
	metricsMap["go_runtime_MemStats_LastGC"] = memStats.LastGC
	metricsMap["go_runtime_MemStats_PauseTotalNs"] = memStats.PauseTotalNs
	metricsMap["go_runtime_MemStats_NumGC"] = uint64(memStats.NumGC)
	metricsMap["go_runtime_MemStats_GCCPUPercentage"] = uint64(100.0 * memStats.GCCPUFraction)

	// Garbage collector statistics (go_runtime_MemStats_PauseAverageNs).
	if 0 == memStats.NumGC {
		metricsMap["go_runtime_MemStats_PauseAverageNs"] = 0
	} else {
		pauseNsAccumulator = 0
		if memStats.NumGC < 255 {
			for i = 0; i < int(memStats.NumGC); i++ {
				pauseNsAccumulator += memStats.PauseNs[i]
			}
			metricsMap["go_runtime_MemStats_PauseAverageNs"] = pauseNsAccumulator / uint64(memStats.NumGC)
		} else {
			for i = 0; i < 256; i++ {
				pauseNsAccumulator += memStats.PauseNs[i]
			}
			metricsMap["go_runtime_MemStats_PauseAverageNs"] = pauseNsAccumulator / 256
		}
	}

	statsMap = stats.Dump()

	for statKey, statValue = range statsMap {
		metricKey = strings.Replace(statKey, ".", "_", -1)
		metricKey = strings.Replace(metricKey, "-", "_", -1)
		metricsMap[metricKey] = statValue
	}

	acceptHeader = request.Header.Get("Accept")

	if strings.Contains(acceptHeader, "application/json") {
		formatResponseAsHTML = false
		formatResponseAsJSON = true
	} else if strings.Contains(acceptHeader, "text/html") {
		formatResponseAsHTML = true
		formatResponseAsJSON = true
	} else if strings.Contains(acceptHeader, "text/plain") {
		formatResponseAsHTML = false
		formatResponseAsJSON = false
	} else if strings.Contains(acceptHeader, "*/*") {
		formatResponseAsHTML = false
		formatResponseAsJSON = true
	} else if strings.Contains(acceptHeader, "") {
		formatResponseAsHTML = false
		formatResponseAsJSON = true
	} else {
		responseWriter.WriteHeader(http.StatusNotAcceptable)
		return
	}

	if formatResponseAsJSON {
		metricsJSONPacked, _ = json.Marshal(metricsMap)
		if formatResponseAsHTML {
			responseWriter.Header().Set("Content-Type", "text/html")
			responseWriter.WriteHeader(http.StatusOK)

			_, _ = responseWriter.Write([]byte(fmt.Sprintf(metricsTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, utils.ByteSliceToString(metricsJSONPacked))))
		} else {
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)

			paramList, ok = request.URL.Query()["compact"]
			if ok {
				if 0 == len(paramList) {
					sendPackedMetrics = false
				} else {
					sendPackedMetrics = !((paramList[0] == "") || (paramList[0] == "0") || (paramList[0] == "false"))
				}
			} else {
				sendPackedMetrics = false
			}

			if sendPackedMetrics {
				_, _ = responseWriter.Write(metricsJSONPacked)
			} else {
				json.Indent(&metricsJSON, metricsJSONPacked, "", "\t")
				_, _ = responseWriter.Write(metricsJSON.Bytes())
				_, _ = responseWriter.Write([]byte("\n"))
			}
		}
	} else {
		metricsLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

		for metricKey, metricValueAsUint64 = range metricsMap {
			metricValueAsString = fmt.Sprintf("%v", metricValueAsUint64)
			ok, err = metricsLLRB.Put(metricKey, metricValueAsString)
			if nil != err {
				err = fmt.Errorf("metricsLLRB.Put(%v, %v) failed: %v", metricKey, metricValueAsString, err)
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
			if !ok {
				err = fmt.Errorf("metricsLLRB.Put(%v, %v) returned ok == false", metricKey, metricValueAsString)
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
		}

		sortedTwoColumnResponseWriter(metricsLLRB, responseWriter)
	}
}

func doGetOfStats(responseWriter http.ResponseWriter, request *http.Request) {

	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)

	_, _ = responseWriter.Write([]byte(bucketstats.SprintStats(bucketstats.StatFormatParsable1, "*", "*")))
}

func doGetOfArmDisarmTrigger(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		availableTriggers []string
		err               error
		haltTriggerString string
		ok                bool
		triggersLLRB      sortedmap.LLRBTree
	)

	responseWriter.Header().Set("Content-Type", "text/html")
	responseWriter.WriteHeader(http.StatusOK)

	_, _ = responseWriter.Write([]byte("<!DOCTYPE html>\n"))
	_, _ = responseWriter.Write([]byte("<html lang=\"en\">\n"))
	_, _ = responseWriter.Write([]byte("  <head>\n"))
	_, _ = responseWriter.Write([]byte(fmt.Sprintf("    <title>Trigger Arm/Disarm Page</title>\n")))
	_, _ = responseWriter.Write([]byte("  </head>\n"))
	_, _ = responseWriter.Write([]byte("  <body>\n"))
	_, _ = responseWriter.Write([]byte("    <form method=\"post\" action=\"/arm-disarm-trigger\">\n"))
	_, _ = responseWriter.Write([]byte("      <select name=\"haltLabelString\">\n"))
	_, _ = responseWriter.Write([]byte("        <option value=\"\">-- select one --</option>\n"))

	availableTriggers = halter.List()

	triggersLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

	for _, haltTriggerString = range availableTriggers {
		ok, err = triggersLLRB.Put(haltTriggerString, true)
		if nil != err {
			err = fmt.Errorf("triggersLLRB.Put(%v, true) failed: %v", haltTriggerString, err)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		if !ok {
			err = fmt.Errorf("triggersLLRB.Put(%v, true) returned ok == false", haltTriggerString)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		_, _ = responseWriter.Write([]byte(fmt.Sprintf("        <option value=\"%v\">%v</option>\n", haltTriggerString, haltTriggerString)))
	}

	_, _ = responseWriter.Write([]byte("      </select>\n"))
	_, _ = responseWriter.Write([]byte("      <input type=\"number\" name=\"haltAfterCount\" min=\"0\" max=\"4294967295\" required>\n"))
	_, _ = responseWriter.Write([]byte("      <input type=\"submit\">\n"))
	_, _ = responseWriter.Write([]byte("    </form>\n"))
	_, _ = responseWriter.Write([]byte("  </body>\n"))
	_, _ = responseWriter.Write([]byte("</html>\n"))
}

func doGetOfTrigger(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		triggerAllArmedOrDisarmedActiveString string
		armedTriggers                         map[string]uint32
		availableTriggers                     []string
		err                                   error
		haltTriggerArmedStateAsBool           bool
		haltTriggerArmedStateAsString         string
		haltTriggerCount                      uint32
		haltTriggerString                     string
		i                                     int
		lenTriggersLLRB                       int
		numPathParts                          int
		key                                   sortedmap.Key
		ok                                    bool
		pathSplit                             []string
		triggersLLRB                          sortedmap.LLRBTree
		value                                 sortedmap.Value
	)

	pathSplit = strings.Split(request.URL.Path, "/") // leading  "/" places "" in pathSplit[0]
	//                                                  pathSplit[1] should be "trigger" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]

	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
	}

	if "trigger" != pathSplit[1] {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	switch numPathParts {
	case 1:
		// Form: /trigger[?armed={true|false}]

		haltTriggerArmedStateAsString = request.FormValue("armed")

		if "" == haltTriggerArmedStateAsString {
			triggerAllArmedOrDisarmedActiveString = triggerAllActive
			armedTriggers = halter.Dump()
			availableTriggers = halter.List()

			triggersLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

			for _, haltTriggerString = range availableTriggers {
				haltTriggerCount, ok = armedTriggers[haltTriggerString]
				if !ok {
					haltTriggerCount = 0
				}
				ok, err = triggersLLRB.Put(haltTriggerString, haltTriggerCount)
				if nil != err {
					err = fmt.Errorf("triggersLLRB.Put(%v, %v) failed: %v", haltTriggerString, haltTriggerCount, err)
					logger.Fatalf("HTTP Server Logic Error: %v", err)
				}
				if !ok {
					err = fmt.Errorf("triggersLLRB.Put(%v, %v) returned ok == false", haltTriggerString, haltTriggerCount)
					logger.Fatalf("HTTP Server Logic Error: %v", err)
				}
			}
		} else {
			haltTriggerArmedStateAsBool, err = strconv.ParseBool(haltTriggerArmedStateAsString)
			if nil == err {
				triggersLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

				if haltTriggerArmedStateAsBool {
					triggerAllArmedOrDisarmedActiveString = triggerArmedActive
					armedTriggers = halter.Dump()
					for haltTriggerString, haltTriggerCount = range armedTriggers {
						ok, err = triggersLLRB.Put(haltTriggerString, haltTriggerCount)
						if nil != err {
							err = fmt.Errorf("triggersLLRB.Put(%v, %v) failed: %v", haltTriggerString, haltTriggerCount, err)
							logger.Fatalf("HTTP Server Logic Error: %v", err)
						}
						if !ok {
							err = fmt.Errorf("triggersLLRB.Put(%v, %v) returned ok == false", haltTriggerString, haltTriggerCount)
							logger.Fatalf("HTTP Server Logic Error: %v", err)
						}
					}
				} else {
					triggerAllArmedOrDisarmedActiveString = triggerDisarmedActive
					armedTriggers = halter.Dump()
					availableTriggers = halter.List()

					for _, haltTriggerString = range availableTriggers {
						_, ok = armedTriggers[haltTriggerString]
						if !ok {
							ok, err = triggersLLRB.Put(haltTriggerString, uint32(0))
							if nil != err {
								err = fmt.Errorf("triggersLLRB.Put(%v, %v) failed: %v", haltTriggerString, 0, err)
								logger.Fatalf("HTTP Server Logic Error: %v", err)
							}
							if !ok {
								err = fmt.Errorf("triggersLLRB.Put(%v, %v) returned ok == false", haltTriggerString, 0)
								logger.Fatalf("HTTP Server Logic Error: %v", err)
							}
						}
					}
				}
			} else {
				responseWriter.WriteHeader(http.StatusBadRequest)
			}
		}

		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write([]byte(fmt.Sprintf(triggerTopTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort)))
		_, _ = responseWriter.Write([]byte(triggerAllArmedOrDisarmedActiveString))
		_, _ = responseWriter.Write([]byte(triggerTableTop))

		lenTriggersLLRB, err = triggersLLRB.Len()
		if nil != err {
			err = fmt.Errorf("triggersLLRB.Len()) failed: %v", err)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		for i = 0; i < lenTriggersLLRB; i++ {
			key, value, ok, err = triggersLLRB.GetByIndex(i)
			if nil != err {
				err = fmt.Errorf("triggersLLRB.GetByIndex(%v) failed: %v", i, err)
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
			if !ok {
				err = fmt.Errorf("triggersLLRB.GetByIndex(%v) returned ok == false", i)
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}

			haltTriggerString = key.(string)
			haltTriggerCount = value.(uint32)

			_, _ = responseWriter.Write([]byte(fmt.Sprintf(triggerTableRowTemplate, haltTriggerString, haltTriggerCount)))
		}

		_, _ = responseWriter.Write([]byte(triggerBottom))
	case 2:
		// Form: /trigger/<trigger-name>

		haltTriggerString = pathSplit[2]

		haltTriggerCount, err = halter.Stat(haltTriggerString)
		if nil == err {
			responseWriter.Header().Set("Content-Type", "text/plain")
			responseWriter.WriteHeader(http.StatusOK)

			_, _ = responseWriter.Write([]byte(fmt.Sprintf("%v\n", haltTriggerCount)))
		} else {
			responseWriter.WriteHeader(http.StatusNotFound)
		}
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

type requestState struct {
	pathSplit               []string
	numPathParts            int
	formatResponseAsJSON    bool
	formatResponseCompactly bool
	volume                  *volumeStruct
}

func doGetOfVolume(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		requestState            requestState
		acceptHeader            string
		err                     error
		formatResponseAsJSON    bool
		formatResponseCompactly bool
		numPathParts            int
		ok                      bool
		paramList               []string
		pathSplit               []string
		volumeAsValue           sortedmap.Value
		volumeList              []string
		volumeListIndex         int
		volumeListJSON          bytes.Buffer
		volumeListJSONPacked    []byte
		volumeListLen           int
		volumeName              string
		volumeNameAsKey         sortedmap.Key
	)

	pathSplit = strings.Split(request.URL.Path, "/") // leading  "/" places "" in pathSplit[0]
	//                                                  pathSplit[1] should be "volume" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]

	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
	}

	if "volume" != pathSplit[1] {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	switch numPathParts {
	case 1:
		// Form: /volume
	case 2:
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	case 3:
		// Form: /volume/<volume-name>/extent-map
		// Form: /volume/<volume-name>/fsck-job
		// Form: /volume/<volume-name>/layout-report
		// Form: /volume/<volume-name>/scrub-job
		// Form: /volume/<volume-name>/snapshot
	case 4:
		// Form: /volume/<volume-name>/extent-map/<basename>
		// Form: /volume/<volume-name>/fsck-job/<job-id>
		// Form: /volume/<volume-name>/scrub-job/<job-id>
	default:
		// Form: /volume/<volume-name>/extent-map/<dir>/.../<basename>
	}

	acceptHeader = request.Header.Get("Accept")

	if strings.Contains(acceptHeader, "application/json") {
		formatResponseAsJSON = true
	} else if strings.Contains(acceptHeader, "text/html") {
		formatResponseAsJSON = false
	} else if strings.Contains(acceptHeader, "*/*") {
		formatResponseAsJSON = true
	} else if strings.Contains(acceptHeader, "") {
		formatResponseAsJSON = true
	} else {
		responseWriter.WriteHeader(http.StatusNotAcceptable)
		return
	}

	if formatResponseAsJSON {
		paramList, ok = request.URL.Query()["compact"]
		formatResponseCompactly = (ok && (0 < len(paramList)) && (("true" == paramList[0]) || ("1" == paramList[0])))
	}

	if 1 == numPathParts {
		volumeListLen, err = globals.volumeLLRB.Len()
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		volumeList = make([]string, 0, volumeListLen)
		for volumeListIndex = 0; volumeListIndex < volumeListLen; volumeListIndex++ {
			// GetByIndex(index int) (key Key, value Value, ok bool, err error)
			volumeNameAsKey, _, ok, err = globals.volumeLLRB.GetByIndex(volumeListIndex)
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
			if !ok {
				err = fmt.Errorf("httpserver.doGetOfVolume() indexing globals.volumeLLRB failed")
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}

			volumeName = volumeNameAsKey.(string)
			volumeList = append(volumeList, volumeName)
		}

		if formatResponseAsJSON {
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)

			volumeListJSONPacked, err = json.Marshal(volumeList)
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}

			if formatResponseCompactly {
				_, _ = responseWriter.Write(volumeListJSONPacked)
			} else {
				json.Indent(&volumeListJSON, volumeListJSONPacked, "", "\t")
				_, _ = responseWriter.Write(volumeListJSON.Bytes())
				_, _ = responseWriter.Write([]byte("\n"))
			}
		} else {
			responseWriter.Header().Set("Content-Type", "text/html")
			responseWriter.WriteHeader(http.StatusOK)

			_, _ = responseWriter.Write([]byte(fmt.Sprintf(volumeListTopTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort)))

			for volumeListIndex, volumeName = range volumeList {
				_, _ = responseWriter.Write([]byte(fmt.Sprintf(volumeListPerVolumeTemplate, volumeName)))
			}

			_, _ = responseWriter.Write([]byte(volumeListBottom))
		}

		return
	}

	// If we reach here, numPathParts is at least 3

	volumeName = pathSplit[2]

	volumeAsValue, ok, err = globals.volumeLLRB.GetByKey(volumeName)
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}
	if !ok {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	requestState.volume = volumeAsValue.(*volumeStruct)

	requestState.pathSplit = pathSplit
	requestState.numPathParts = numPathParts
	requestState.formatResponseAsJSON = formatResponseAsJSON
	requestState.formatResponseCompactly = formatResponseCompactly

	switch pathSplit[3] {
	case "extent-map":
		doExtentMap(responseWriter, request, requestState)

	case "fsck-job":
		doJob(fsckJobType, responseWriter, request, requestState)

	case "layout-report":
		doLayoutReport(responseWriter, request, requestState)

	case "scrub-job":
		doJob(scrubJobType, responseWriter, request, requestState)

	case "snapshot":
		doGetOfSnapShot(responseWriter, request, requestState)

	default:
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	return
}

func doExtentMap(responseWriter http.ResponseWriter, request *http.Request, requestState requestState) {
	var (
		dirEntryInodeNumber inode.InodeNumber
		dirInodeNumber      inode.InodeNumber
		err                 error
		extentMap           []ExtentMapElementStruct
		extentMapChunk      *inode.ExtentMapChunkStruct
		extentMapEntry      inode.ExtentMapEntryStruct
		extentMapJSONBuffer bytes.Buffer
		extentMapJSONPacked []byte
		path                string
		pathDoubleQuoted    string
		pathPartIndex       int
	)

	if 3 > requestState.numPathParts {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}

	if (3 == requestState.numPathParts) && requestState.formatResponseAsJSON {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	if 3 == requestState.numPathParts {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write([]byte(fmt.Sprintf(extentMapTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, requestState.volume.name, "null", "null", "false")))
		return
	}

	dirEntryInodeNumber = inode.RootDirInodeNumber
	pathPartIndex = 3

	path = strings.Join(requestState.pathSplit[4:], "/")
	pathDoubleQuoted = "\"" + path + "\""

	for ; pathPartIndex < requestState.numPathParts; pathPartIndex++ {
		dirInodeNumber = dirEntryInodeNumber
		dirEntryInodeNumber, err = requestState.volume.fsMountHandle.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirInodeNumber, requestState.pathSplit[pathPartIndex+1])
		if nil != err {
			if requestState.formatResponseAsJSON {
				responseWriter.WriteHeader(http.StatusNotFound)
				return
			} else {
				responseWriter.Header().Set("Content-Type", "text/html")
				responseWriter.WriteHeader(http.StatusOK)

				_, _ = responseWriter.Write([]byte(fmt.Sprintf(extentMapTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, requestState.volume.name, "null", pathDoubleQuoted, "true")))
				return
			}
		}
	}

	extentMapChunk, err = requestState.volume.fsMountHandle.FetchExtentMapChunk(inode.InodeRootUserID, inode.InodeGroupID(0), nil, dirEntryInodeNumber, uint64(0), math.MaxInt64, int64(0))
	if nil != err {
		if requestState.formatResponseAsJSON {
			responseWriter.WriteHeader(http.StatusNotFound)
			return
		} else {
			responseWriter.Header().Set("Content-Type", "text/html")
			responseWriter.WriteHeader(http.StatusOK)

			_, _ = responseWriter.Write([]byte(fmt.Sprintf(extentMapTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, requestState.volume.name, "null", pathDoubleQuoted, "true")))
			return
		}
	}

	extentMap = make([]ExtentMapElementStruct, 0, len(extentMapChunk.ExtentMapEntry))

	for _, extentMapEntry = range extentMapChunk.ExtentMapEntry {
		extentMap = append(extentMap, ExtentMapElementStruct{
			FileOffset:    extentMapEntry.FileOffset,
			ContainerName: extentMapEntry.ContainerName,
			ObjectName:    extentMapEntry.ObjectName,
			ObjectOffset:  extentMapEntry.LogSegmentOffset,
			Length:        extentMapEntry.Length,
		})
	}

	extentMapJSONPacked, err = json.Marshal(extentMap)
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}

	if requestState.formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		if requestState.formatResponseCompactly {
			_, _ = responseWriter.Write(extentMapJSONPacked)
		} else {
			json.Indent(&extentMapJSONBuffer, extentMapJSONPacked, "", "\t")
			_, _ = responseWriter.Write(extentMapJSONBuffer.Bytes())
			_, _ = responseWriter.Write([]byte("\n"))
		}
	} else {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write([]byte(fmt.Sprintf(extentMapTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, requestState.volume.name, utils.ByteSliceToString(extentMapJSONPacked), pathDoubleQuoted, "false")))
	}
}

func doJob(jobType jobTypeType, responseWriter http.ResponseWriter, request *http.Request, requestState requestState) {
	var (
		err                     error
		formatResponseAsJSON    bool
		formatResponseCompactly bool
		inactive                bool
		job                     *jobStruct
		jobAsValue              sortedmap.Value
		jobErrorList            []string
		jobID                   uint64
		jobIDAsKey              sortedmap.Key
		jobPerJobTemplate       string
		jobsIDListJSONBuffer    bytes.Buffer
		jobsIDListJSONPacked    []byte
		jobStatusJSONBuffer     bytes.Buffer
		jobStatusJSONPacked     []byte
		jobStatusJSONStruct     *JobStatusJSONPackedStruct
		jobsCount               int
		jobsIDList              []uint64
		jobsIndex               int
		ok                      bool
		numPathParts            int
		pathSplit               []string
		volume                  *volumeStruct
		volumeName              string
	)

	if limitJobType <= jobType {
		err = fmt.Errorf("httpserver.doJob(jobtype==%v,,,) called for invalid jobType", jobType)
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}

	volume = requestState.volume
	pathSplit = requestState.pathSplit
	numPathParts = requestState.numPathParts
	formatResponseAsJSON = requestState.formatResponseAsJSON
	formatResponseCompactly = requestState.formatResponseCompactly

	volumeName = volume.name
	volume.Lock()

	markJobsCompletedIfNoLongerActiveWhileLocked(volume)

	if 3 == numPathParts {
		switch jobType {
		case fsckJobType:
			jobsCount, err = volume.fsckJobs.Len()
		case scrubJobType:
			jobsCount, err = volume.scrubJobs.Len()
		}
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		inactive = (nil == volume.fsckActiveJob) && (nil == volume.scrubActiveJob)

		volume.Unlock()

		if formatResponseAsJSON {
			jobsIDList = make([]uint64, 0, jobsCount)

			for jobsIndex = jobsCount - 1; jobsIndex >= 0; jobsIndex-- {
				switch jobType {
				case fsckJobType:
					jobIDAsKey, _, ok, err = volume.fsckJobs.GetByIndex(jobsIndex)
				case scrubJobType:
					jobIDAsKey, _, ok, err = volume.scrubJobs.GetByIndex(jobsIndex)
				}
				if nil != err {
					logger.Fatalf("HTTP Server Logic Error: %v", err)
				}
				if !ok {
					switch jobType {
					case fsckJobType:
						err = fmt.Errorf("httpserver.doGetOfVolume() indexing volume.fsckJobs failed")
					case scrubJobType:
						err = fmt.Errorf("httpserver.doGetOfVolume() indexing volume.scrubJobs failed")
					}
					logger.Fatalf("HTTP Server Logic Error: %v", err)
				}
				jobID = jobIDAsKey.(uint64)

				jobsIDList = append(jobsIDList, jobID)
			}

			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)

			jobsIDListJSONPacked, err = json.Marshal(jobsIDList)
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}

			if formatResponseCompactly {
				_, _ = responseWriter.Write(jobsIDListJSONPacked)
			} else {
				json.Indent(&jobsIDListJSONBuffer, jobsIDListJSONPacked, "", "\t")
				_, _ = responseWriter.Write(jobsIDListJSONBuffer.Bytes())
				_, _ = responseWriter.Write([]byte("\n"))
			}
		} else {
			responseWriter.Header().Set("Content-Type", "text/html")
			responseWriter.WriteHeader(http.StatusOK)

			switch jobType {
			case fsckJobType:
				_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobsTopTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, volumeName, "FSCK")))
			case scrubJobType:
				_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobsTopTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, volumeName, "SCRUB")))
			}

			for jobsIndex = jobsCount - 1; jobsIndex >= 0; jobsIndex-- {
				switch jobType {
				case fsckJobType:
					jobIDAsKey, jobAsValue, ok, err = volume.fsckJobs.GetByIndex(jobsIndex)
				case scrubJobType:
					jobIDAsKey, jobAsValue, ok, err = volume.scrubJobs.GetByIndex(jobsIndex)
				}
				if nil != err {
					logger.Fatalf("HTTP Server Logic Error: %v", err)
				}
				if !ok {
					switch jobType {
					case fsckJobType:
						err = fmt.Errorf("httpserver.doGetOfVolume() indexing volume.fsckJobs failed")
					case scrubJobType:
						err = fmt.Errorf("httpserver.doGetOfVolume() indexing volume.scrubJobs failed")
					}
					logger.Fatalf("HTTP Server Logic Error: %v", err)
				}

				jobID = jobIDAsKey.(uint64)
				job = jobAsValue.(*jobStruct)

				if jobRunning == job.state {
					switch jobType {
					case fsckJobType:
						_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobsPerRunningJobTemplate, jobID, job.startTime.Format(time.RFC3339), volumeName, "fsck")))
					case scrubJobType:
						_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobsPerRunningJobTemplate, jobID, job.startTime.Format(time.RFC3339), volumeName, "scrub")))
					}
				} else {
					switch job.state {
					case jobHalted:
						jobPerJobTemplate = jobsPerHaltedJobTemplate
					case jobCompleted:
						jobErrorList = job.jobHandle.Error()
						if 0 == len(jobErrorList) {
							jobPerJobTemplate = jobsPerSuccessfulJobTemplate
						} else {
							jobPerJobTemplate = jobsPerFailedJobTemplate
						}
					}

					switch jobType {
					case fsckJobType:
						_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobPerJobTemplate, jobID, job.startTime.Format(time.RFC3339), job.endTime.Format(time.RFC3339), volumeName, "fsck")))
					case scrubJobType:
						_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobPerJobTemplate, jobID, job.startTime.Format(time.RFC3339), job.endTime.Format(time.RFC3339), volumeName, "scrub")))
					}
				}
			}

			_, _ = responseWriter.Write([]byte(jobsListBottom))

			if inactive {
				switch jobType {
				case fsckJobType:
					_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobsStartJobButtonTemplate, volumeName, "fsck")))
				case scrubJobType:
					_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobsStartJobButtonTemplate, volumeName, "scrub")))
				}
			}

			_, _ = responseWriter.Write([]byte(jobsBottom))
		}

		return
	}

	// If we reach here, numPathParts is 4

	jobID, err = strconv.ParseUint(pathSplit[4], 10, 64)
	if nil != err {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	switch jobType {
	case fsckJobType:
		jobAsValue, ok, err = volume.fsckJobs.GetByKey(jobID)
	case scrubJobType:
		jobAsValue, ok, err = volume.scrubJobs.GetByKey(jobID)
	}
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}
	if !ok {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	job = jobAsValue.(*jobStruct)

	jobStatusJSONStruct = &JobStatusJSONPackedStruct{
		StartTime: job.startTime.Format(time.RFC3339),
		ErrorList: job.jobHandle.Error(),
		InfoList:  job.jobHandle.Info(),
	}

	switch job.state {
	case jobRunning:
		// Nothing to add here
	case jobHalted:
		jobStatusJSONStruct.HaltTime = job.endTime.Format(time.RFC3339)
	case jobCompleted:
		jobStatusJSONStruct.DoneTime = job.endTime.Format(time.RFC3339)
	}

	jobStatusJSONPacked, err = json.Marshal(jobStatusJSONStruct)
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}

	if formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		if formatResponseCompactly {
			_, _ = responseWriter.Write(jobStatusJSONPacked)
		} else {
			json.Indent(&jobStatusJSONBuffer, jobStatusJSONPacked, "", "\t")
			_, _ = responseWriter.Write(jobStatusJSONBuffer.Bytes())
			_, _ = responseWriter.Write([]byte("\n"))
		}
	} else {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		switch jobType {
		case fsckJobType:
			_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, volumeName, "FSCK", "fsck", job.id, utils.ByteSliceToString(jobStatusJSONPacked))))
		case scrubJobType:
			_, _ = responseWriter.Write([]byte(fmt.Sprintf(jobTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, volumeName, "SCRUB", "scrub", job.id, utils.ByteSliceToString(jobStatusJSONPacked))))
		}
	}

	volume.Unlock()
}

type layoutReportElementLayoutReportElementStruct struct {
	ObjectNumber uint64
	ObjectBytes  uint64
}

type layoutReportSetElementStruct struct {
	TreeName     string
	LayoutReport []layoutReportElementLayoutReportElementStruct
}

func doLayoutReport(responseWriter http.ResponseWriter, request *http.Request, requestState requestState) {
	var (
		err                       error
		layoutReportIndex         int
		layoutReportMap           sortedmap.LayoutReport
		layoutReportSet           [6]*layoutReportSetElementStruct
		layoutReportSetElement    *layoutReportSetElementStruct
		layoutReportSetJSON       bytes.Buffer
		layoutReportSetJSONPacked []byte
		objectBytes               uint64
		objectNumber              uint64
		treeTypeIndex             int
	)

	layoutReportSet[headhunter.MergedBPlusTree] = &layoutReportSetElementStruct{
		TreeName: "Checkpoint (Trailer + B+Trees)",
	}
	layoutReportSet[headhunter.InodeRecBPlusTree] = &layoutReportSetElementStruct{
		TreeName: "Inode Record B+Tree",
	}
	layoutReportSet[headhunter.LogSegmentRecBPlusTree] = &layoutReportSetElementStruct{
		TreeName: "Log Segment Record B+Tree",
	}
	layoutReportSet[headhunter.BPlusTreeObjectBPlusTree] = &layoutReportSetElementStruct{
		TreeName: "B+Plus Tree Objects B+Tree",
	}
	layoutReportSet[headhunter.CreatedObjectsBPlusTree] = &layoutReportSetElementStruct{
		TreeName: "Created Objects B+Tree",
	}
	layoutReportSet[headhunter.DeletedObjectsBPlusTree] = &layoutReportSetElementStruct{
		TreeName: "Deleted Objects B+Tree",
	}

	for treeTypeIndex, layoutReportSetElement = range layoutReportSet {
		layoutReportMap, err = requestState.volume.headhunterVolumeHandle.FetchLayoutReport(headhunter.BPlusTreeType(treeTypeIndex))
		if nil != err {
			responseWriter.WriteHeader(http.StatusInternalServerError)
			return
		}

		layoutReportSetElement.LayoutReport = make([]layoutReportElementLayoutReportElementStruct, len(layoutReportMap))

		layoutReportIndex = 0

		for objectNumber, objectBytes = range layoutReportMap {
			layoutReportSetElement.LayoutReport[layoutReportIndex] = layoutReportElementLayoutReportElementStruct{objectNumber, objectBytes}
			layoutReportIndex++
		}
	}

	if requestState.formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		layoutReportSetJSONPacked, err = json.Marshal(layoutReportSet)
		if nil != err {
			responseWriter.WriteHeader(http.StatusInternalServerError)
			return
		}

		if requestState.formatResponseCompactly {
			_, _ = responseWriter.Write(layoutReportSetJSONPacked)
		} else {
			json.Indent(&layoutReportSetJSON, layoutReportSetJSONPacked, "", "\t")
			_, _ = responseWriter.Write(layoutReportSetJSON.Bytes())
			_, _ = responseWriter.Write([]byte("\n"))
		}
	} else {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write([]byte(fmt.Sprintf(layoutReportTopTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, requestState.volume.name)))

		for _, layoutReportSetElement = range layoutReportSet {
			_, _ = responseWriter.Write([]byte(fmt.Sprintf(layoutReportTableTopTemplate, layoutReportSetElement.TreeName)))

			for layoutReportIndex = 0; layoutReportIndex < len(layoutReportSetElement.LayoutReport); layoutReportIndex++ {
				_, _ = responseWriter.Write([]byte(fmt.Sprintf(layoutReportTableRowTemplate, layoutReportSetElement.LayoutReport[layoutReportIndex].ObjectNumber, layoutReportSetElement.LayoutReport[layoutReportIndex].ObjectBytes)))
			}

			_, _ = responseWriter.Write([]byte(layoutReportTableBottom))
		}

		_, _ = responseWriter.Write([]byte(layoutReportBottom))
	}
}

func doGetOfSnapShot(responseWriter http.ResponseWriter, request *http.Request, requestState requestState) {
	var (
		directionStringCanonicalized string
		directionStringSlice         []string
		err                          error
		list                         []headhunter.SnapShotStruct
		listJSON                     bytes.Buffer
		listJSONPacked               []byte
		orderByStringCanonicalized   string
		orderByStringSlice           []string
		queryValues                  url.Values
		reversed                     bool
		snapShot                     headhunter.SnapShotStruct
	)

	queryValues = request.URL.Query()

	directionStringSlice = queryValues["direction"]

	if 0 == len(directionStringSlice) {
		reversed = false
	} else {
		directionStringCanonicalized = strings.ToLower(directionStringSlice[0])
		if "desc" == directionStringCanonicalized {
			reversed = true
		} else {
			reversed = false
		}
	}

	orderByStringSlice = queryValues["orderby"]

	if 0 == len(orderByStringSlice) {
		list = requestState.volume.headhunterVolumeHandle.SnapShotListByTime(reversed)
	} else {
		orderByStringCanonicalized = strings.ToLower(orderByStringSlice[0])
		switch orderByStringCanonicalized {
		case "id":
			list = requestState.volume.headhunterVolumeHandle.SnapShotListByID(reversed)
		case "name":
			list = requestState.volume.headhunterVolumeHandle.SnapShotListByName(reversed)
		default: // assume "time"
			list = requestState.volume.headhunterVolumeHandle.SnapShotListByTime(reversed)
		}
	}

	if requestState.formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		listJSONPacked, err = json.Marshal(list)
		if nil != err {
			responseWriter.WriteHeader(http.StatusInternalServerError)
			return
		}

		if requestState.formatResponseCompactly {
			_, _ = responseWriter.Write(listJSONPacked)
		} else {
			json.Indent(&listJSON, listJSONPacked, "", "\t")
			_, _ = responseWriter.Write(listJSON.Bytes())
			_, _ = responseWriter.Write([]byte("\n"))
		}
	} else {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write([]byte(fmt.Sprintf(snapShotsTopTemplate, version.ProxyFSVersion, globals.ipAddrTCPPort, requestState.volume.name)))

		for _, snapShot = range list {
			_, _ = responseWriter.Write([]byte(fmt.Sprintf(snapShotsPerSnapShotTemplate, snapShot.ID, snapShot.Time.Format(time.RFC3339), snapShot.Name)))
		}

		_, _ = responseWriter.Write([]byte(fmt.Sprintf(snapShotsBottomTemplate, requestState.volume.name)))
	}
}

func doPost(responseWriter http.ResponseWriter, request *http.Request) {
	switch {
	case strings.HasPrefix(request.URL.Path, "/trigger"):
		doPostOfTrigger(responseWriter, request)
	case strings.HasPrefix(request.URL.Path, "/volume"):
		doPostOfVolume(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doPostOfTrigger(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err                      error
		haltTriggerCountAsString string
		haltTriggerCountAsU32    uint32
		haltTriggerCountAsU64    uint64
		haltTriggerString        string
		numPathParts             int
		pathSplit                []string
	)

	pathSplit = strings.Split(request.URL.Path, "/") // leading  "/" places "" in pathSplit[0]
	//                                                  pathSplit[1] should be "trigger" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]

	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
	}

	if "trigger" != pathSplit[1] {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	switch numPathParts {
	case 2:
		// Form: /trigger/<trigger-name>

		haltTriggerString = pathSplit[2]

		_, err = halter.Stat(haltTriggerString)
		if nil != err {
			responseWriter.WriteHeader(http.StatusNotFound)
			return
		}

		haltTriggerCountAsString = request.FormValue("count")

		haltTriggerCountAsU64, err = strconv.ParseUint(haltTriggerCountAsString, 10, 32)
		if nil != err {
			responseWriter.WriteHeader(http.StatusBadRequest)
			return
		}
		haltTriggerCountAsU32 = uint32(haltTriggerCountAsU64)

		if 0 == haltTriggerCountAsU32 {
			halter.Disarm(haltTriggerString)
		} else {
			halter.Arm(haltTriggerString, haltTriggerCountAsU32)
		}

		responseWriter.WriteHeader(http.StatusNoContent)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doPostOfVolume(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		acceptHeader  string
		err           error
		job           *jobStruct
		jobAsValue    sortedmap.Value
		jobID         uint64
		jobType       jobTypeType
		jobsCount     int
		numPathParts  int
		ok            bool
		pathSplit     []string
		volume        *volumeStruct
		volumeAsValue sortedmap.Value
		volumeName    string
	)

	pathSplit = strings.Split(request.URL.Path, "/") // leading  "/" places "" in pathSplit[0]
	//                                                  pathSplit[1] should be "volume" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]

	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
	}

	if "volume" != pathSplit[1] {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	switch numPathParts {
	case 3:
		// Form: /volume/<volume-name>/fsck-job
		// Form: /volume/<volume-name>/scrub-job
		// Form: /volume/<volume-name>/snapshot
	case 4:
		// Form: /volume/<volume-name>/fsck-job/<job-id>
		// Form: /volume/<volume-name>/scrub-job/<job-id>
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	volumeName = pathSplit[2]

	volumeAsValue, ok, err = globals.volumeLLRB.GetByKey(volumeName)
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}
	if !ok {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	volume = volumeAsValue.(*volumeStruct)

	switch pathSplit[3] {
	case "fsck-job":
		jobType = fsckJobType
	case "scrub-job":
		jobType = scrubJobType
	case "snapshot":
		if 3 != numPathParts {
			responseWriter.WriteHeader(http.StatusNotFound)
			return
		}
		doPostOfSnapShot(responseWriter, request, volume)
		return
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	volume.Lock()

	if 3 == numPathParts {
		markJobsCompletedIfNoLongerActiveWhileLocked(volume)

		if (nil != volume.fsckActiveJob) || (nil != volume.scrubActiveJob) {
			// Cannot start an FSCK or SCRUB job while either is active

			volume.Unlock()
			responseWriter.WriteHeader(http.StatusPreconditionFailed)
			return
		}

		for {
			switch jobType {
			case fsckJobType:
				jobsCount, err = volume.fsckJobs.Len()
			case scrubJobType:
				jobsCount, err = volume.scrubJobs.Len()
			}
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}

			if jobsCount < int(globals.jobHistoryMaxSize) {
				break
			}

			switch jobType {
			case fsckJobType:
				ok, err = volume.fsckJobs.DeleteByIndex(0)
			case scrubJobType:
				ok, err = volume.scrubJobs.DeleteByIndex(0)
			}
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
			if !ok {
				switch jobType {
				case fsckJobType:
					err = fmt.Errorf("httpserver.doPostOfVolume() delete of oldest element of volume.fsckJobs failed")
				case scrubJobType:
					err = fmt.Errorf("httpserver.doPostOfVolume() delete of oldest element of volume.scrubJobs failed")
				}
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
		}

		job = &jobStruct{
			volume:    volume,
			state:     jobRunning,
			startTime: time.Now(),
		}

		job.id, err = volume.headhunterVolumeHandle.FetchNonce()
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		switch jobType {
		case fsckJobType:
			ok, err = volume.fsckJobs.Put(job.id, job)
		case scrubJobType:
			ok, err = volume.scrubJobs.Put(job.id, job)
		}
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		if !ok {
			switch jobType {
			case fsckJobType:
				err = fmt.Errorf("httpserver.doPostOfVolume() PUT to volume.fsckJobs failed")
			case scrubJobType:
				err = fmt.Errorf("httpserver.doPostOfVolume() PUT to volume.scrubJobs failed")
			}
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		switch jobType {
		case fsckJobType:
			volume.fsckActiveJob = job

			job.jobHandle = fs.ValidateVolume(volumeName)
		case scrubJobType:
			volume.scrubActiveJob = job

			job.jobHandle = fs.ScrubVolume(volumeName)
		}

		volume.Unlock()

		switch jobType {
		case fsckJobType:
			responseWriter.Header().Set("Location", fmt.Sprintf("/volume/%v/fsck-job/%v", volumeName, job.id))
		case scrubJobType:
			responseWriter.Header().Set("Location", fmt.Sprintf("/volume/%v/scrub-job/%v", volumeName, job.id))
		}

		acceptHeader = request.Header.Get("Accept")

		if strings.Contains(acceptHeader, "text/html") {
			responseWriter.WriteHeader(http.StatusSeeOther)
		} else {
			responseWriter.WriteHeader(http.StatusCreated)
		}

		return
	}

	// If we reach here, numPathParts is 4

	jobID, err = strconv.ParseUint(pathSplit[4], 10, 64)
	if nil != err {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	switch jobType {
	case fsckJobType:
		jobAsValue, ok, err = volume.fsckJobs.GetByKey(jobID)
	case scrubJobType:
		jobAsValue, ok, err = volume.scrubJobs.GetByKey(jobID)
	}
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}
	if !ok {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	job = jobAsValue.(*jobStruct)

	switch jobType {
	case fsckJobType:
		if volume.fsckActiveJob != job {
			volume.Unlock()
			responseWriter.WriteHeader(http.StatusPreconditionFailed)
			return
		}
	case scrubJobType:
		if volume.scrubActiveJob != job {
			volume.Unlock()
			responseWriter.WriteHeader(http.StatusPreconditionFailed)
			return
		}
	}

	job.jobHandle.Cancel()

	job.state = jobHalted
	job.endTime = time.Now()

	switch jobType {
	case fsckJobType:
		volume.fsckActiveJob = nil
	case scrubJobType:
		volume.scrubActiveJob = nil
	}

	volume.Unlock()

	responseWriter.WriteHeader(http.StatusNoContent)
}

func doPostOfSnapShot(responseWriter http.ResponseWriter, request *http.Request, volume *volumeStruct) {
	var (
		err        error
		snapShotID uint64
	)

	snapShotID, err = volume.inodeVolumeHandle.SnapShotCreate(request.FormValue("name"))
	if nil == err {
		responseWriter.Header().Set("Location", fmt.Sprintf("/volume/%v/snapshot/%v", volume.name, snapShotID))

		responseWriter.WriteHeader(http.StatusCreated)
	} else {
		responseWriter.WriteHeader(http.StatusConflict)
	}
}

func sortedTwoColumnResponseWriter(llrb sortedmap.LLRBTree, responseWriter http.ResponseWriter) {
	var (
		err                  error
		format               string
		i                    int
		keyAsKey             sortedmap.Key
		keyAsString          string
		lenLLRB              int
		line                 string
		longestKeyAsString   int
		longestValueAsString int
		ok                   bool
		valueAsString        string
		valueAsValue         sortedmap.Value
	)

	lenLLRB, err = llrb.Len()
	if nil != err {
		err = fmt.Errorf("llrb.Len()) failed: %v", err)
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}

	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)

	longestKeyAsString = 0
	longestValueAsString = 0

	for i = 0; i < lenLLRB; i++ {
		keyAsKey, valueAsValue, ok, err = llrb.GetByIndex(i)
		if nil != err {
			err = fmt.Errorf("llrb.GetByIndex(%v) failed: %v", i, err)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		if !ok {
			err = fmt.Errorf("llrb.GetByIndex(%v) returned ok == false", i)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
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

	for i = 0; i < lenLLRB; i++ {
		keyAsKey, valueAsValue, ok, err = llrb.GetByIndex(i)
		if nil != err {
			err = fmt.Errorf("llrb.GetByIndex(%v) failed: %v", i, err)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		if !ok {
			err = fmt.Errorf("llrb.GetByIndex(%v) returned ok == false", i)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		keyAsString = keyAsKey.(string)
		valueAsString = valueAsValue.(string)
		line = fmt.Sprintf(format, keyAsString, valueAsString)
		_, _ = responseWriter.Write([]byte(line))
	}
}

func markJobsCompletedIfNoLongerActiveWhileLocked(volume *volumeStruct) {
	// First, mark as finished now any FSCK/SCRUB job

	if (nil != volume.fsckActiveJob) && !volume.fsckActiveJob.jobHandle.Active() {
		// FSCK job finished at some point... make it look like it just finished now

		volume.fsckActiveJob.state = jobCompleted
		volume.fsckActiveJob.endTime = time.Now()
		volume.fsckActiveJob = nil
	}

	if (nil != volume.scrubActiveJob) && !volume.scrubActiveJob.jobHandle.Active() {
		// SCRUB job finished at some point... make it look like it just finished now

		volume.scrubActiveJob.state = jobCompleted
		volume.scrubActiveJob.endTime = time.Now()
		volume.scrubActiveJob = nil
	}
}
