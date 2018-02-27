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

func doGetOfIndexDotHTML(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Set("Content-Type", "text/html")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<html>\n"))
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
	_, _ = responseWriter.Write(utils.StringToByteSlice("<html>\n"))
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
	case 4:
		// Form: /volume/<volume-name/fsck-job/<job-id>
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	acceptHeader = request.Header.Get("Accept")

	switch acceptHeader {
	case "":
		// Default assumes Accept: text/html
		formatResponseAsJSON = false
	case "application/json":
		formatResponseAsJSON = true
	case "text/html":
		formatResponseAsJSON = false
	default:
		// TODO: Decide if returning text/html is not allowed here, but for now assume it is
		formatResponseAsJSON = false
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
			_, _ = responseWriter.Write(utils.StringToByteSlice("<html>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <title>Volumes</title>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))

			for volumeListIndex, volumeName = range volumeList {
				if 0 < volumeListIndex {
					_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
				}
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <a href=\"/volume/%v/fsck-job\">\n", volumeName)))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("      %v\n", volumeName)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </a>\n"))
			}
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
		doFsckJob(responseWriter, request, requestState)

	case "layout-report":
		doLayoutReport(responseWriter, request, requestState)

	default:
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	return
}

func doFsckJob(responseWriter http.ResponseWriter, request *http.Request, requestState requestState) {
	var (
		err                      error
		pathSplit                []string
		numPathParts             int
		formatResponseAsJSON     bool
		formatResponseCompactly  bool
		fsckInactive             bool
		fsckJob                  *fsckJobStruct
		fsckJobAsValue           sortedmap.Value
		fsckJobError             string
		fsckJobErrorList         []string
		fsckJobID                uint64
		fsckJobIDAsKey           sortedmap.Key
		fsckJobInfo              string
		fsckJobInfoList          []string
		fsckJobsIDListJSON       bytes.Buffer
		fsckJobsIDListJSONPacked []byte
		fsckJobStatusJSONBuffer  bytes.Buffer
		fsckJobStatusJSONPacked  []byte
		fsckJobStatusJSONStruct  *FSCKJobStatusJSONPackedStruct
		fsckJobsCount            int
		fsckJobsIDList           []uint64
		fsckJobsIDListIndex      int
		fsckJobsIndex            int
		ok                       bool
		volume                   *volumeStruct
		volumeName               string
	)

	volume = requestState.volume
	pathSplit = requestState.pathSplit
	numPathParts = requestState.numPathParts
	formatResponseAsJSON = requestState.formatResponseAsJSON
	formatResponseCompactly = requestState.formatResponseCompactly

	volumeName = volume.name
	volume.Lock()

	if 3 == numPathParts {
		fsckJobsCount, err = volume.fsckJobs.Len()
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		fsckJobsIDList = make([]uint64, 0, fsckJobsCount)
		for fsckJobsIndex = fsckJobsCount - 1; fsckJobsIndex >= 0; fsckJobsIndex-- {
			fsckJobIDAsKey, _, ok, err = volume.fsckJobs.GetByIndex(fsckJobsIndex)
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
			if !ok {
				err = fmt.Errorf("httpserver.doGetOfVolume() indexing volume.fsckJobs failed")
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
			fsckJobID = fsckJobIDAsKey.(uint64)

			fsckJobsIDList = append(fsckJobsIDList, fsckJobID)
		}

		if nil == volume.fsckActiveJob {
			fsckInactive = true
		} else {
			// We know fsckJobRunning == volume.fsckActiveJob.state

			if volume.fsckActiveJob.validateVolumeHandle.Active() {
				// FSCK must still be running

				fsckInactive = false
			} else {
				// FSCK finished at some point... make it look like it just finished now

				volume.fsckActiveJob.state = fsckJobCompleted
				volume.fsckActiveJob.endTime = time.Now()
				volume.fsckActiveJob = nil

				fsckInactive = true
			}
		}

		volume.Unlock()

		if formatResponseAsJSON {
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)

			fsckJobsIDListJSONPacked, err = json.Marshal(fsckJobsIDList)
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}

			if formatResponseCompactly {
				_, _ = responseWriter.Write(fsckJobsIDListJSONPacked)
			} else {
				json.Indent(&fsckJobsIDListJSON, fsckJobsIDListJSONPacked, "", "\t")
				_, _ = responseWriter.Write(fsckJobsIDListJSON.Bytes())
				_, _ = responseWriter.Write(utils.StringToByteSlice("\n"))
			}
		} else {
			responseWriter.Header().Set("Content-Type", "text/html")
			responseWriter.WriteHeader(http.StatusOK)

			_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("<html>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <title>%v FSCK Jobs</title>\n", volumeName)))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))
			for fsckJobsIDListIndex, fsckJobID = range fsckJobsIDList {
				if 0 < fsckJobsIDListIndex {
					_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
				}
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <a href=\"/volume/%v/fsck-job/%v\">\n", volumeName, fsckJobID)))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("      %v\n", fsckJobID)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </a>\n"))
			}
			if fsckInactive {
				if 0 < fsckJobsCount {
					_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
				}
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <form method=\"post\" action=\"/volume/%v/fsck-job\">\n", volumeName)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <input type=\"submit\" value=\"Start\">\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </form>\n"))
			}
			_, _ = responseWriter.Write(utils.StringToByteSlice("  </body>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("</html>\n"))
		}

		return
	}

	// If we reach here, numPathParts is 4

	fsckJobID, err = strconv.ParseUint(pathSplit[4], 10, 64)
	if nil != err {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	fsckJobAsValue, ok, err = volume.fsckJobs.GetByKey(fsckJobID)
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}
	if !ok {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	fsckJob = fsckJobAsValue.(*fsckJobStruct)

	if fsckJobRunning == fsckJob.state {
		if !fsckJob.validateVolumeHandle.Active() {
			// FSCK finished at some point... make it look like it just finished now

			fsckJob.state = fsckJobCompleted
			fsckJob.endTime = time.Now()
			volume.fsckActiveJob = nil
		}
	}

	if formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		fsckJobStatusJSONStruct = &FSCKJobStatusJSONPackedStruct{
			StartTime: fsckJob.startTime.Format(time.RFC3339),
			ErrorList: fsckJob.validateVolumeHandle.Error(),
			InfoList:  fsckJob.validateVolumeHandle.Info(),
		}

		switch fsckJob.state {
		case fsckJobRunning:
			// Nothing to add here
		case fsckJobHalted:
			fsckJobStatusJSONStruct.HaltTime = fsckJob.endTime.Format(time.RFC3339)
		case fsckJobCompleted:
			fsckJobStatusJSONStruct.DoneTime = fsckJob.endTime.Format(time.RFC3339)
		}

		fsckJobStatusJSONPacked, err = json.Marshal(fsckJobStatusJSONStruct)
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		if formatResponseCompactly {
			_, _ = responseWriter.Write(fsckJobStatusJSONPacked)
		} else {
			json.Indent(&fsckJobStatusJSONBuffer, fsckJobStatusJSONPacked, "", "\t")
			_, _ = responseWriter.Write(fsckJobStatusJSONBuffer.Bytes())
			_, _ = responseWriter.Write(utils.StringToByteSlice("\n"))
		}
	} else {
		responseWriter.Header().Set("Content-Type", "text/html")
		responseWriter.WriteHeader(http.StatusOK)

		_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("<html>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("  <head>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <title>%v FSCK Job %v</title>\n", volumeName, fsckJob.id)))
		_, _ = responseWriter.Write(utils.StringToByteSlice("  </head>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("  <body>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("    <table>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>State</td>\n"))
		switch fsckJob.state {
		case fsckJobRunning:
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Running</td>\n"))
		case fsckJobHalted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Halted</td>\n"))
		case fsckJobCompleted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Completed</td>\n"))
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Start Time</td>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", fsckJob.startTime.Format(time.RFC3339))))
		_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		switch fsckJob.state {
		case fsckJobRunning:
			// Nothing to add here
		case fsckJobHalted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Halt Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", fsckJob.endTime.Format(time.RFC3339))))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		case fsckJobCompleted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Done Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", fsckJob.endTime.Format(time.RFC3339))))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		}
		fsckJobErrorList = fsckJob.validateVolumeHandle.Error()
		if 0 == len(fsckJobErrorList) {
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>No Errors</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>&nbsp;</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
		} else {
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Errors:</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			for _, fsckJobError = range fsckJobErrorList {
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>&nbsp;</td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%v</td>\n", html.EscapeString(fsckJobError))))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			}
		}
		fsckJobInfoList = fsckJob.validateVolumeHandle.Info()
		if 0 < len(fsckJobInfoList) {
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Info:</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>&nbsp;</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			for _, fsckJobInfo = range fsckJobInfoList {
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>&nbsp;</td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%v</td>\n", html.EscapeString(fsckJobInfo))))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			}
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("    </table>\n"))
		if fsckJobRunning == fsckJob.state {
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <form method=\"post\" action=\"/volume/%v/fsck-job/%v\">\n", volumeName, fsckJob.id)))
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
		_, _ = responseWriter.Write(utils.StringToByteSlice("<html>\n"))

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
		err            error
		fsckJob        *fsckJobStruct
		fsckJobAsValue sortedmap.Value
		fsckJobID      uint64
		fsckJobsCount  int
		numPathParts   int
		ok             bool
		pathSplit      []string
		volume         *volumeStruct
		volumeAsValue  sortedmap.Value
		volumeName     string
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
	case 4:
		// Form: /volume/<volume-name/fsck-job/<job-id>
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

	if "fsck-job" != pathSplit[3] {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	if 3 == numPathParts {
		if nil != volume.fsckActiveJob {
			// We know fsckJobRunning == volume.fsckActiveJob.state

			if volume.fsckActiveJob.validateVolumeHandle.Active() {
				// FSCK must still be running

				volume.Unlock()
				responseWriter.WriteHeader(http.StatusPreconditionFailed)
				return
			}

			// FSCK finished at some point... make it look like it just finished now

			volume.fsckActiveJob.state = fsckJobCompleted
			volume.fsckActiveJob.endTime = time.Now()
			volume.fsckActiveJob = nil
		}

		for {
			fsckJobsCount, err = volume.fsckJobs.Len()
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}

			if fsckJobsCount < fsckJobsHistoryMaxSize {
				break
			}

			ok, err = volume.fsckJobs.DeleteByIndex(0)
			if nil != err {
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
			if !ok {
				err = fmt.Errorf("httpserver.doPostOfVolume() delete of oldest element of volume.fsckJobs failed")
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
		}

		fsckJob = &fsckJobStruct{
			volume:    volume,
			state:     fsckJobRunning,
			startTime: time.Now(),
		}

		fsckJob.id, err = volume.headhunterHandle.FetchNonce()
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		ok, err = volume.fsckJobs.Put(fsckJob.id, fsckJob)
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		if !ok {
			err = fmt.Errorf("httpserver.doPostOfVolume() PUT to volume.fsckJobs failed")
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		volume.fsckActiveJob = fsckJob

		fsckJob.validateVolumeHandle = fs.ValidateVolume(volumeName)

		volume.Unlock()

		responseWriter.Header().Set("Location", fmt.Sprintf("/volume/%v/fsck-job/%v", volumeName, fsckJob.id))
		responseWriter.WriteHeader(http.StatusCreated)

		return
	}

	// If we reach here, numPathParts is 4

	fsckJobID, err = strconv.ParseUint(pathSplit[4], 10, 64)
	if nil != err {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	fsckJobAsValue, ok, err = volume.fsckJobs.GetByKey(fsckJobID)
	if nil != err {
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}
	if !ok {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	fsckJob = fsckJobAsValue.(*fsckJobStruct)

	if volume.fsckActiveJob != fsckJob {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	volume.fsckActiveJob.validateVolumeHandle.Cancel()

	volume.fsckActiveJob.state = fsckJobHalted
	volume.fsckActiveJob.endTime = time.Now()
	volume.fsckActiveJob = nil

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
