package httpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/swiftstack/sortedmap"

	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/utils"
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
		err                      error
		format                   string
		i                        int
		keyAsKey                 sortedmap.Key
		keyAsString              string
		lenStatsLLRB             int
		line                     string
		longestStatKey           int
		longestStatValueAsString int
		memStats                 runtime.MemStats
		ok                       bool
		pauseNsAccumulator       uint64
		statKey                  string
		statsLLRB                sortedmap.LLRBTree
		statsMap                 map[string]uint64
		statValueAsString        string
		statValueAsUint64        uint64
		valueAsString            string
		valueAsValue             sortedmap.Value
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
		if len(statKey) > longestStatKey {
			longestStatKey = len(statKey)
		}
		statValueAsString = fmt.Sprintf("%v", statValueAsUint64)
		if len(statValueAsString) > longestStatValueAsString {
			longestStatValueAsString = len(statValueAsString)
		}
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

	format = fmt.Sprintf("%%-%vs %%%vs\n", longestStatKey, longestStatValueAsString)

	lenStatsLLRB, err = statsLLRB.Len()
	if nil != err {
		err = fmt.Errorf("statsLLRB.Len()) failed: %v", err)
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}
	if len(statsMap) != lenStatsLLRB {
		err = fmt.Errorf("len(statsMap) != lenStatsLLRB")
		logger.Fatalf("HTTP Server Logic Error: %v", err)
	}

	responseWriter.Header().Set("Content-Type", "text/plain")
	responseWriter.WriteHeader(http.StatusOK)

	for i = 0; i < lenStatsLLRB; i++ {
		keyAsKey, valueAsValue, ok, err = statsLLRB.GetByIndex(i)
		if nil != err {
			err = fmt.Errorf("statsLLRB.GetByIndex(%v) failed: %v", i, err)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		if !ok {
			err = fmt.Errorf("statsLLRB.GetByIndex(%v) returned ok == false", i)
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}
		keyAsString = keyAsKey.(string)
		valueAsString = valueAsValue.(string)
		line = fmt.Sprintf(format, keyAsString, valueAsString)
		_, _ = responseWriter.Write(utils.StringToByteSlice(line))
	}
}

func doGetOfVolume(responseWriter http.ResponseWriter, request *http.Request) {
	var (
		acceptHeader              string
		err                       error
		formatResponseAsJSON      bool
		formatResponseCompactly   bool
		fsckCompletedJobNoError   fsckCompletedJobNoErrorStruct
		fsckCompletedJobWithError fsckCompletedJobWithErrorStruct
		fsckHaltedJob             fsckHaltedJobStruct
		fsckInactive              bool
		fsckJob                   *fsckJobStruct
		fsckJobAsValue            sortedmap.Value
		fsckJobID                 uint64
		fsckJobIDAsKey            sortedmap.Key
		fsckJobsIDListJSON        bytes.Buffer
		fsckJobsIDListJSONPacked  []byte
		fsckJobStatusJSON         bytes.Buffer
		fsckJobStatusJSONPacked   []byte
		fsckJobsCount             int
		fsckJobsIDList            []uint64
		fsckJobsIDListIndex       int
		fsckJobsIndex             int
		fsckRunningJob            fsckRunningJobStruct
		numPathParts              int
		ok                        bool
		paramList                 []string
		pathSplit                 []string
		volume                    *volumeStruct
		volumeAsValue             sortedmap.Value
		volumeList                []string
		volumeListIndex           int
		volumeListJSON            bytes.Buffer
		volumeListJSONPacked      []byte
		volumeListLen             int
		volumeName                string
		volumeNameAsKey           sortedmap.Key
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
	volume = volumeAsValue.(*volumeStruct)

	volume.Lock()

	if "fsck-job" != pathSplit[3] {
		volume.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

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
			select {
			case volume.fsckActiveJob.err = <-volume.fsckActiveJob.errChan:
				// FSCK finished at some point... make it look like it just finished now

				volume.fsckActiveJob.state = fsckJobCompleted
				volume.fsckActiveJob.endTime = time.Now()
				volume.fsckActiveJob = nil

				fsckInactive = true
			default:
				// FSCK must still be running

				fsckInactive = false
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
		select {
		case fsckJob.err = <-fsckJob.errChan:
			// FSCK finished at some point... make it look like it just finished now

			fsckJob.state = fsckJobCompleted
			fsckJob.endTime = time.Now()
			volume.fsckActiveJob = nil
		default:
			// FSCK must still be running
		}
	}

	if formatResponseAsJSON {
		responseWriter.Header().Set("Content-Type", "application/json")
		responseWriter.WriteHeader(http.StatusOK)

		switch fsckJob.state {
		case fsckJobRunning:
			fsckRunningJob.StartTime = fsckJob.startTime.String()
			fsckJobStatusJSONPacked, err = json.Marshal(fsckRunningJob)
		case fsckJobHalted:
			fsckHaltedJob.StartTime = fsckJob.startTime.String()
			fsckHaltedJob.HaltTime = fsckJob.endTime.String()
			fsckJobStatusJSONPacked, err = json.Marshal(fsckHaltedJob)
		case fsckJobCompleted:
			if nil == fsckJob.err {
				fsckCompletedJobNoError.StartTime = fsckJob.startTime.String()
				fsckCompletedJobNoError.DoneTime = fsckJob.endTime.String()
				fsckJobStatusJSONPacked, err = json.Marshal(fsckCompletedJobNoError)
			} else {
				fsckCompletedJobWithError.StartTime = fsckJob.startTime.String()
				fsckCompletedJobWithError.DoneTime = fsckJob.endTime.String()
				fsckCompletedJobWithError.Error = fsckJob.err.Error()
				fsckJobStatusJSONPacked, err = json.Marshal(fsckCompletedJobWithError)
			}
		}
		if nil != err {
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		if formatResponseCompactly {
			_, _ = responseWriter.Write(fsckJobStatusJSONPacked)
		} else {
			json.Indent(&fsckJobStatusJSON, fsckJobStatusJSONPacked, "", "\t")
			_, _ = responseWriter.Write(fsckJobStatusJSON.Bytes())
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
		switch fsckJob.state {
		case fsckJobRunning:
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <table>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>State</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Running</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Start Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", fsckJob.startTime.String())))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("    </table>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <br />\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <form method=\"post\" action=\"/volume/%v/fsck-job/%v\">\n", volumeName, fsckJob.id)))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <input type=\"submit\" value=\"Stop\">\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("    </form>\n"))
		case fsckJobHalted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <table>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>State</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Halted</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Start Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", fsckJob.startTime.String())))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Halt Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", fsckJob.endTime.String())))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("    </table>\n"))
		case fsckJobCompleted:
			_, _ = responseWriter.Write(utils.StringToByteSlice("    <table>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>State</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Completed</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Start Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", fsckJob.startTime.String())))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Done Time</td>\n"))
			_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>%s</td>\n", fsckJob.endTime.String())))
			_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			if nil == fsckJob.err {
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Errors</td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>None</td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			} else {
				_, _ = responseWriter.Write(utils.StringToByteSlice("      <tr>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <td>Errors</td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("        <td>No%vne</td>\n", fsckJob.err.Error())))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </tr>\n"))
			}
			_, _ = responseWriter.Write(utils.StringToByteSlice("    </table>\n"))
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("  </body>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice("</html>\n"))
	}

	volume.Unlock()
}

func doPost(responseWriter http.ResponseWriter, request *http.Request) {
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
			select {
			case volume.fsckActiveJob.err = <-volume.fsckActiveJob.errChan:
				// FSCK finished at some point... make it look like it just finished now

				volume.fsckActiveJob.state = fsckJobCompleted
				volume.fsckActiveJob.endTime = time.Now()
				volume.fsckActiveJob = nil
			default:
				// FSCK must still be running

				volume.Unlock()
				responseWriter.WriteHeader(http.StatusPreconditionFailed)
				return
			}
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
				err = fmt.Errorf("httpserver.doPost() delete of oldest element of volume.fsckJobs failed")
				logger.Fatalf("HTTP Server Logic Error: %v", err)
			}
		}

		fsckJob = &fsckJobStruct{
			volume:    volume,
			stopChan:  make(chan bool, 1),
			errChan:   make(chan error, 1),
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
			err = fmt.Errorf("httpserver.doPost() PUT to volume.fsckJobs failed")
			logger.Fatalf("HTTP Server Logic Error: %v", err)
		}

		volume.fsckActiveJob = fsckJob

		go fs.ValidateVolume(volumeName, fsckJob.stopChan, fsckJob.errChan)

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

	volume.fsckActiveJob.stopChan <- true
	volume.fsckActiveJob.err = <-volume.fsckActiveJob.errChan
	select {
	case _, _ = <-volume.fsckActiveJob.stopChan:
		// Swallow our stopChan write from above if fs.ValidateVolume() finished before reading it
	default:
		// fs.ValidateVolume() must have read and honored our stopChan write
	}
	volume.fsckActiveJob.state = fsckJobHalted
	volume.fsckActiveJob.endTime = time.Now()
	volume.fsckActiveJob = nil

	volume.Unlock()

	responseWriter.WriteHeader(http.StatusNoContent)
}
