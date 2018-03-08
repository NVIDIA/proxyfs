package httpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/halter"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/utils"
	"github.com/swiftstack/sortedmap"
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

func doGet(responseWriter http.ResponseWriter, request *http.Request) {
	path := strings.TrimRight(request.URL.Path, "/")

	switch {
	case "" == path:
		doGetOfIndexDotHTML(responseWriter, request)
	case "/styles.css" == path:
		doGetOfStylesDotCSS(responseWriter, request)
	case "/index.html" == path:
		doGetOfIndexDotHTML(responseWriter, request)
	case "/config" == path:
		doGetOfConfig(responseWriter, request)
	case "/metrics" == path:
		doGetOfMetrics(responseWriter, request)
	case "/arm-disarm-trigger" == path:
		doGetOfArmDisarmTrigger(responseWriter, request)
	case strings.HasPrefix(request.URL.Path, "/trigger"):
		doGetOfTrigger(responseWriter, request)
	case strings.HasPrefix(request.URL.Path, "/volume"):
		doGetOfVolume(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doGetOfStylesDotCSS(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/css")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write(utils.StringToByteSlice(".table td.fit,\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice(".table th.fit {\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  white-space: nowrap;\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  width: 1%;\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("}\n"))
}

func doGetOfIndexDotHTML(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/html")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<html lang=\"en\">\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    <title>ProxyFS</title>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    <a href=\"/config\">Configuration Parameters</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    <a href=\"/metrics\">StatsD/Prometheus Page</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    Trigger Pages:\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("      <a href=\"/arm-disarm-trigger\">Arm/Disarm</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("      <a href=\"/trigger\">All</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("      <a href=\"/trigger?armed=true\">Armed</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("      <a href=\"/trigger?armed=false\">Disarmed</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    <a href=\"/volume\">Volume Page</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  </body>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("</html>\n"))
}

func doGetOfConfig(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		confMapJSON       bytes.Buffer
		confMapJSONPacked []byte
	)

	// NOTE:  Some day, perhaps, we'll use utility functions for semantic extraction
	// of query strings, and we can get rid of this.  Or we'll use an off-the-shelf router.
	sendPackedConfig := false
	if paramList, ok := request.URL.Query()["compact"]; ok {
		if len(paramList) > 0 {
			param := paramList[0]
			sendPackedConfig = (param == "true" || param == "1")
		}
	}

	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)
	confMapJSONPacked, _ = json.Marshal(globals.confMap)

	if sendPackedConfig {
		_, _ = responseWriter.Write(confMapJSONPacked)
	} else {
		json.Indent(&confMapJSON, confMapJSONPacked, "", "\t")
		_, _ = responseWriter.Write(confMapJSON.Bytes())
		_, _ = responseWriter.Write(utils.StringToByteSlice("\n"))
	}
}

func doGetOfMetrics(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err                error
		i                  int
		memStats           runtime.MemStats
		ok                 bool
		pauseNsAccumulator uint64
		statKey            string
		statsLLRB          sortedmap.LLRBTree
		statsMap           map[string]uint64
		statValueAsString  string
		statValueAsUint64  uint64
	)

	statsMap = stats.Dump()

	runtime.ReadMemStats(&memStats)

	// General statistics.
	statsMap["go_runtime_MemStats_Alloc"] = memStats.Alloc
	statsMap["go_runtime_MemStats_TotalAlloc"] = memStats.TotalAlloc
	statsMap["go_runtime_MemStats_Sys"] = memStats.Sys
	statsMap["go_runtime_MemStats_Lookups"] = memStats.Lookups
	statsMap["go_runtime_MemStats_Mallocs"] = memStats.Mallocs
	statsMap["go_runtime_MemStats_Frees"] = memStats.Frees

	// Main allocation heap statistics.
	statsMap["go_runtime_MemStats_HeapAlloc"] = memStats.HeapAlloc
	statsMap["go_runtime_MemStats_HeapSys"] = memStats.HeapSys
	statsMap["go_runtime_MemStats_HeapIdle"] = memStats.HeapIdle
	statsMap["go_runtime_MemStats_HeapInuse"] = memStats.HeapInuse
	statsMap["go_runtime_MemStats_HeapReleased"] = memStats.HeapReleased
	statsMap["go_runtime_MemStats_HeapObjects"] = memStats.HeapObjects

	// Low-level fixed-size structure allocator statistics.
	//	Inuse is bytes used now.
	//	Sys is bytes obtained from system.
	statsMap["go_runtime_MemStats_StackInuse"] = memStats.StackInuse
	statsMap["go_runtime_MemStats_StackSys"] = memStats.StackSys
	statsMap["go_runtime_MemStats_MSpanInuse"] = memStats.MSpanInuse
	statsMap["go_runtime_MemStats_MSpanSys"] = memStats.MSpanSys
	statsMap["go_runtime_MemStats_MCacheInuse"] = memStats.MCacheInuse
	statsMap["go_runtime_MemStats_MCacheSys"] = memStats.MCacheSys
	statsMap["go_runtime_MemStats_BuckHashSys"] = memStats.BuckHashSys
	statsMap["go_runtime_MemStats_GCSys"] = memStats.GCSys
	statsMap["go_runtime_MemStats_OtherSys"] = memStats.OtherSys

	// Garbage collector statistics (fixed portion).
	statsMap["go_runtime_MemStats_LastGC"] = memStats.LastGC
	statsMap["go_runtime_MemStats_PauseTotalNs"] = memStats.PauseTotalNs
	statsMap["go_runtime_MemStats_NumGC"] = uint64(memStats.NumGC)
	statsMap["go_runtime_MemStats_GCCPUPercentage"] = uint64(100.0 * memStats.GCCPUFraction)

	// Garbage collector statistics (go_runtime_MemStats_PauseAverageNs).
	if 0 == memStats.NumGC {
		statsMap["go_runtime_MemStats_PauseAverageNs"] = 0
	} else {
		pauseNsAccumulator = 0
		if memStats.NumGC < 255 {
			for i = 0; i < int(memStats.NumGC); i++ {
				pauseNsAccumulator += memStats.PauseNs[i]
			}
			statsMap["go_runtime_MemStats_PauseAverageNs"] = pauseNsAccumulator / uint64(memStats.NumGC)
		} else {
			for i = 0; i < 256; i++ {
				pauseNsAccumulator += memStats.PauseNs[i]
			}
			statsMap["go_runtime_MemStats_PauseAverageNs"] = pauseNsAccumulator / 256
		}
	}

	statsLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

	for statKey, statValueAsUint64 = range statsMap {
		statKey = strings.Replace(statKey, ".", "_", -1)
		statKey = strings.Replace(statKey, "-", "_", -1)
		statValueAsString = fmt.Sprintf("%v", statValueAsUint64)
		ok, err = statsLLRB.Put(statKey, statValueAsString)
		if nil != err {
			err = fmt.Errorf("statsLLRB.Put(%v, %v) failed: %v", statKey, statValueAsString, err)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		if !ok {
			err = fmt.Errorf("statsLLRB.Put(%v, %v) returned ok == false", statKey, statValueAsString)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
	}

	sortedTwoColumnResponseWriter(statsLLRB, responseWriter)
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

	_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<html lang=\"en\">\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <title>Trigger Arm/Disarm Page</title>\n")))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    <form method=\"post\" action=\"/arm-disarm-trigger\">\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("      <select name=\"haltLabelString\">\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("        <option value=\"\">-- select one --</option>\n"))

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
		_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <option value=\"%v\">%v</option>\n", haltTriggerString, haltTriggerString)))
	}

	_, _ = responseWriter.Write(utils.StringToByteSlice("      </select>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("      <input type=\"number\" name=\"haltAfterCount\" min=\"0\" max=\"4294967295\" required>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("      <input type=\"submit\">\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("    </form>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  </body>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("</html>\n"))
}

func doGetOfTrigger(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		armedTriggers                 map[string]uint32
		availableTriggers             []string
		err                           error
		haltTriggerArmedStateAsBool   bool
		haltTriggerArmedStateAsString string
		haltTriggerCount              uint32
		haltTriggerString             string
		numPathParts                  int
		ok                            bool
		pathSplit                     []string
		triggersLLRB                  sortedmap.LLRBTree
	)

	pathSplit = strings.Split(request.URL.Path, "/") // leading  "/" places "" in pathSplit[0]
	//                                                  pathSplit[1] must be "trigger" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]
	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
	}

	switch numPathParts {
	case 1:
		// Form: /trigger[?armed={true|false}]

		haltTriggerArmedStateAsString = request.FormValue("armed")

		if "" == haltTriggerArmedStateAsString {
			armedTriggers = halter.Dump()
			availableTriggers = halter.List()

			triggersLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

			for _, haltTriggerString = range availableTriggers {
				haltTriggerCount, ok = armedTriggers[haltTriggerString]
				if ok {
					ok, err = triggersLLRB.Put(haltTriggerString, strconv.FormatUint(uint64(haltTriggerCount), 10))
					if nil != err {
						err = fmt.Errorf("triggersLLRB.Put(%v, %v) failed: %v", haltTriggerString, haltTriggerCount, err)
						logger.Fatalf("HTTP Server Logic Error: %v", err)
					}
					if !ok {
						err = fmt.Errorf("triggersLLRB.Put(%v, %v) returned ok == false", haltTriggerString, haltTriggerCount)
						logger.Fatalf("HTTP Server Logic Error: %v", err)
					}
				} else {
					ok, err = triggersLLRB.Put(haltTriggerString, "0")
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

			sortedTwoColumnResponseWriter(triggersLLRB, responseWriter)
		} else {
			haltTriggerArmedStateAsBool, err = strconv.ParseBool(haltTriggerArmedStateAsString)
			if nil == err {
				triggersLLRB = sortedmap.NewLLRBTree(sortedmap.CompareString, nil)

				if haltTriggerArmedStateAsBool {
					armedTriggers = halter.Dump()
					for haltTriggerString, haltTriggerCount = range armedTriggers {
						ok, err = triggersLLRB.Put(haltTriggerString, strconv.FormatUint(uint64(haltTriggerCount), 10))
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
					armedTriggers = halter.Dump()
					availableTriggers = halter.List()

					for _, haltTriggerString = range availableTriggers {
						_, ok = armedTriggers[haltTriggerString]
						if !ok {
							ok, err = triggersLLRB.Put(haltTriggerString, "0")
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

				sortedTwoColumnResponseWriter(triggersLLRB, responseWriter)
			} else {
				responseWriter.WriteHeader(http.StatusBadRequest)
			}
		}
	case 2:
		// Form: /trigger/<trigger-name>

		haltTriggerString = pathSplit[2]

		haltTriggerCount, err = halter.Stat(haltTriggerString)
		if nil == err {
			responseWriter.Header().Set("Content-Type", "text/plain")
			responseWriter.WriteHeader(http.StatusOK)

			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("%v\n", haltTriggerCount)))
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
	//                                                  pathSplit[1] must be "volume" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]
	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
	}

	switch numPathParts {
	case 1:
		// Form: /volume
	case 3:
		// Form: /volume/<volume-name/fsck-job
		// Form: /volume/<volume-name/layout-report
		// Form: /volume/<volume-name/scrub-job
	case 4:
		// Form: /volume/<volume-name/fsck-job/<job-id>
		// Form: /volume/<volume-name/scrub-job/<job-id>
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	acceptHeader = request.Header.Get("Accept")

	if strings.Contains(acceptHeader, "application/json") {
		formatResponseAsJSON = true
	} else if strings.Contains(acceptHeader, "text/html") {
		formatResponseAsJSON = false
	} else if strings.Contains(acceptHeader, "*/*") {
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
				_, _ = responseWriter.Write(utils.StringToByteSlice("\n"))
			}
		} else {
			responseWriter.Header().Set("Content-Type", "text/html")
			responseWriter.WriteHeader(http.StatusOK)

			_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("<html lang=\"en\">\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <title>Volumes</title>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <table>\n"))
			for volumeListIndex, volumeName = range volumeList {
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%v</td>\n", volumeName)))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td><a href=\"/volume/%v/fsck-job\">FSCK</a></td>\n", volumeName)))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td><a href=\"/volume/%v/scrub-job\">SCRUB</a></td>\n", volumeName)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			}
			_, _ = responseWriter.Write(utils.StringToByteSlice("    </table>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </body>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("</html>\n"))
		}

		return
	}

	// If we reach here, numPathParts is either 3 or 4
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
	case "fsck-job":
		doJob(fsckJobType, responseWriter, request, requestState)

	case "layout-report":
		doLayoutReport(responseWriter, request, requestState)

	case "scrub-job":
		doJob(scrubJobType, responseWriter, request, requestState)

	default:
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	return
}

func doJob(jobType jobTypeType, responseWriter http.ResponseWriter, request *http.Request, requestState requestState) {
	var (
		err                     error
		formatResponseAsJSON    bool
		formatResponseCompactly bool
		inactive                bool
		job                     *jobStruct
		jobAsValue              sortedmap.Value
		jobError                string
		jobErrorList            []string
		jobID                   uint64
		jobIDAsKey              sortedmap.Key
		jobInfo                 string
		jobInfoList             []string
		jobsIDListJSON          bytes.Buffer
		jobsIDListJSONPacked    []byte
		jobStatusJSONBuffer     bytes.Buffer
		jobStatusJSONPacked     []byte
		jobStatusJSONStruct     *JobStatusJSONPackedStruct
		jobsCount               int
		jobsIDList              []uint64
		jobsIDListIndex         int
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

		inactive = (nil == volume.fsckActiveJob) && (nil == volume.scrubActiveJob)

		volume.Unlock()

		if formatResponseAsJSON {
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)

			jobsIDListJSONPacked, err = json.Marshal(jobsIDList)
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}

			if formatResponseCompactly {
				_, _ = responseWriter.Write(jobsIDListJSONPacked)
			} else {
				json.Indent(&jobsIDListJSON, jobsIDListJSONPacked, "", "\t")
				_, _ = responseWriter.Write(jobsIDListJSON.Bytes())
				_, _ = responseWriter.Write(utils.StringToByteSlice("\n"))
			}
		} else {
			responseWriter.Header().Set("Content-Type", "text/html")
			responseWriter.WriteHeader(http.StatusOK)

			_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("<html lang=\"en\">\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
			switch jobType {
			case fsckJobType:
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <title>%v FSCK Jobs</title>\n", volumeName)))
			case scrubJobType:
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <title>%v SCRUB Jobs</title>\n", volumeName)))
			}
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))
			for jobsIDListIndex, jobID = range jobsIDList {
				if 0 < jobsIDListIndex {
					_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
				}
				switch jobType {
				case fsckJobType:
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <a href=\"/volume/%v/fsck-job/%v\">\n", volumeName, jobID)))
				case scrubJobType:
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <a href=\"/volume/%v/scrub-job/%v\">\n", volumeName, jobID)))
				}
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("      %v\n", jobID)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </a>\n"))
			}
			if inactive {
				if 0 < jobsCount {
					_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
				}
				switch jobType {
				case fsckJobType:
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <form method=\"post\" action=\"/volume/%v/fsck-job\">\n", volumeName)))
				case scrubJobType:
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <form method=\"post\" action=\"/volume/%v/scrub-job\">\n", volumeName)))
				}
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <input type=\"submit\" value=\"Start\">\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </form>\n"))
			}
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </body>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("</html>\n"))
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

	if formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

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

		if formatResponseCompactly {
			_, _ = responseWriter.Write(jobStatusJSONPacked)
		} else {
			json.Indent(&jobStatusJSONBuffer, jobStatusJSONPacked, "", "\t")
			_, _ = responseWriter.Write(jobStatusJSONBuffer.Bytes())
			_, _ = responseWriter.Write(utils.StringToByteSlice("\n"))
		}
	} else {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("<html lang=\"en\">\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
		switch jobType {
		case fsckJobType:
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <title>%v FSCK Job %v</title>\n", volumeName, job.id)))
		case scrubJobType:
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <title>%v SCRUB Job %v</title>\n", volumeName, job.id)))
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("    <table>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>State</td>\n"))
		switch job.state {
		case jobRunning:
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Running</td>\n"))
		case jobHalted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Halted</td>\n"))
		case jobCompleted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Completed</td>\n"))
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Start Time</td>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", job.startTime.Format(time.RFC3339))))
		_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		switch job.state {
		case jobRunning:
			// Nothing to add here
		case jobHalted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Halt Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", job.endTime.Format(time.RFC3339))))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		case jobCompleted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Done Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", job.endTime.Format(time.RFC3339))))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		}
		jobErrorList = job.jobHandle.Error()
		if 0 == len(jobErrorList) {
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>No Errors</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>&nbsp;</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		} else {
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Errors:</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			for _, jobError = range jobErrorList {
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>&nbsp;</td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%v</td>\n", html.EscapeString(jobError))))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			}
		}
		jobInfoList = job.jobHandle.Info()
		if 0 < len(jobInfoList) {
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Info:</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>&nbsp;</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			for _, jobInfo = range jobInfoList {
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>&nbsp;</td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%v</td>\n", html.EscapeString(jobInfo))))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			}
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("    </table>\n"))
		if jobRunning == job.state {
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
			switch jobType {
			case fsckJobType:
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <form method=\"post\" action=\"/volume/%v/fsck-job/%v\">\n", volumeName, job.id)))
			case scrubJobType:
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <form method=\"post\" action=\"/volume/%v/scrub-job/%v\">\n", volumeName, job.id)))
			}
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <input type=\"submit\" value=\"Stop\">\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("    </form>\n"))
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("  </body>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("</html>\n"))
	}

	volume.Unlock()
}

func doLayoutReport(responseWriter http.ResponseWriter, request *http.Request, requestState requestState) {
	var (
		err                     error
		formatResponseAsJSON    bool
		formatResponseCompactly bool
		volume                  *volumeStruct
		volumeName              string
		layoutReport            sortedmap.LayoutReport
	)

	volume = requestState.volume
	// pathSplit = requestState.pathSplit
	// numPathParts = requestState.numPathParts
	formatResponseAsJSON = requestState.formatResponseAsJSON
	formatResponseCompactly = requestState.formatResponseCompactly
	_ = formatResponseCompactly

	volumeName = volume.name

	treeTypes := map[headhunter.BPlusTreeType]string{
		headhunter.InodeRecBPlusTree:        "Inode Record B+Tree",
		headhunter.LogSegmentRecBPlusTree:   "Log Segment Record B+Tree",
		headhunter.BPlusTreeObjectBPlusTree: "B+Plus Tree Objects B+Tree",
	}

	if formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		for treeType, treeName := range treeTypes {
			_ = treeType
			_ = treeName
		}
	} else {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("<html lang=\"en\">\n"))

		for treeType, treeName := range treeTypes {

			layoutReport, err = volume.headhunterHandle.FetchLayoutReport(treeType)
			if err != nil {
				logger.ErrorfWithError(err, "doLayoutReport(): failed for %s tree", treeName)
				responseWriter.WriteHeader(http.StatusInternalServerError)
				return
			}

			_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <title>Volume %s %v Tree</title>\n",
				volumeName, treeName)))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))

			for objNum, objBytes := range layoutReport {
				_, _ = responseWriter.Write(utils.StringToByteSlice(
					fmt.Sprintf("%016X %d<br>\n", objNum, objBytes)))
			}
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </body>\n"))
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("</html>\n"))
	}

	return
}

func doPost(responseWriter http.ResponseWriter, request *http.Request) {
	path := strings.TrimRight(request.URL.Path, "/")

	switch {
	case "/arm-disarm-trigger" == path:
		doPostOfArmDisarmTrigger(responseWriter, request)
	case strings.HasPrefix(request.URL.Path, "/trigger"):
		doPostOfTrigger(responseWriter, request)
	case strings.HasPrefix(request.URL.Path, "/volume"):
		doPostOfVolume(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doPostOfArmDisarmTrigger(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		err                    error
		haltAfterCountAsString string
		haltAfterCountAsU32    uint32
		haltAfterCountAsU64    uint64
		haltLabelString        string
	)

	haltLabelString = request.PostFormValue("haltLabelString")
	haltAfterCountAsString = request.PostFormValue("haltAfterCount")

	_, err = halter.Stat(haltLabelString)
	if nil != err {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	haltAfterCountAsU64, err = strconv.ParseUint(haltAfterCountAsString, 10, 32)
	if nil != err {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}
	haltAfterCountAsU32 = uint32(haltAfterCountAsU64)

	if 0 == haltAfterCountAsU32 {
		halter.Disarm(haltLabelString)
	} else {
		halter.Arm(haltLabelString, haltAfterCountAsU32)
	}

	responseWriter.Header().Set("Location", "/trigger")
	responseWriter.WriteHeader(http.StatusSeeOther)
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
	//                                                  pathSplit[1] must be "trigger" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]
	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
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
	//                                                  pathSplit[1] must be "volume" based on how we got here
	//                                                  trailing "/" places "" in pathSplit[len(pathSplit)-1]
	numPathParts = len(pathSplit) - 1
	if "" == pathSplit[numPathParts] {
		numPathParts--
	}

	switch numPathParts {
	case 3:
		// Form: /volume/<volume-name/fsck-job
		// Form: /volume/<volume-name/scrub-job
	case 4:
		// Form: /volume/<volume-name/fsck-job/<job-id>
		// Form: /volume/<volume-name/scrub-job/<job-id>
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

	volume.Lock()

	switch pathSplit[3] {
	case "fsck-job":
		jobType = fsckJobType
	case "scrub-job":
		jobType = scrubJobType
	default:
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

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

		job.id, err = volume.headhunterHandle.FetchNonce()
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
		responseWriter.WriteHeader(http.StatusCreated)

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
		_, _ = responseWriter.Write(utils.StringToByteSlice(line))
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
