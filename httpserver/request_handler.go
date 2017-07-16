package httpserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
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
	switch request.Method {
	case http.MethodGet:
		doGet(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}
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
	case "/fsck" == path:
		doGetOfFSCK(responseWriter, request)
	case strings.HasPrefix(request.URL.Path, "/fsck-start/"):
		doGetOfFSCKStart(responseWriter, request, strings.TrimPrefix(request.URL.Path, "/fsck-start/"), true)
	case strings.HasPrefix(request.URL.Path, "/fsck-start-non-interactive/"):
		doGetOfFSCKStart(responseWriter, request, strings.TrimPrefix(request.URL.Path, "/fsck-start-non-interactive/"), false)
	case strings.HasPrefix(request.URL.Path, "/fsck-status/"):
		doGetOfFSCKStatus(responseWriter, request, strings.TrimPrefix(request.URL.Path, "/fsck-status/"))
	case strings.HasPrefix(request.URL.Path, "/fsck-stop/"):
		doGetOfFSCKStop(responseWriter, request, strings.TrimPrefix(request.URL.Path, "/fsck-stop/"), true)
	case strings.HasPrefix(request.URL.Path, "/fsck-stop-non-interactive/"):
		doGetOfFSCKStop(responseWriter, request, strings.TrimPrefix(request.URL.Path, "/fsck-stop-non-interactive/"), false)
	case "/metrics" == path:
		doGetOfMetrics(responseWriter, request)
	default:
		responseWriter.WriteHeader(http.StatusNotFound)
	}
}

func doGetOfIndexDotHTML(responseWriter http.ResponseWriter, request *http.Request) {
	responseWriter.Header().Add("Content-Type", "text/html")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<title>ProxyFS</title>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<p>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  <a href=\"/config\">Configuration Parameters</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("</p>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<p>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  <a href=\"/fsck\">FSCK Control Page</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("</p>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<p>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("  <a href=\"/metrics\">StatsD/Prometheus Page</a>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("</p>\n"))
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

	responseWriter.Header().Add("Content-Type", "application/json")
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

func doGetOfFSCK(responseWriter http.ResponseWriter, request *http.Request) {
	globals.Lock()
	responseWriter.Header().Add("Content-Type", "text/html")
	responseWriter.WriteHeader(http.StatusOK)
	_, _ = responseWriter.Write(utils.StringToByteSlice("<!DOCTYPE html>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<title>FSCK</title>\n"))
	_, _ = responseWriter.Write(utils.StringToByteSlice("<table>\n"))
	numVolumes, err := globals.volumeLLRB.Len()
	if nil != err {
		panic(fmt.Errorf("globals.volumeLLRB.Len() failed: %v", err))
	}
	for i := 0; i < numVolumes; i++ {
		_, volumeAsValue, ok, nonShadowingErr := globals.volumeLLRB.GetByIndex(i)
		if nil != nonShadowingErr {
			panic(fmt.Errorf("globals.volumeLLRB.GetByIndex(%v) failed: %v", i, err))
		}
		if !ok {
			panic(fmt.Errorf("globals.volumeLLRB.GetByIndex(%v) returned ok == false", i))
		}
		volume, ok := volumeAsValue.(*volumeStruct)
		if !ok {
			panic(fmt.Errorf("volumeAsValue.(*volumeStruct) for index %v returned ok == false", i))
		}
		volume.Lock()
		_, _ = responseWriter.Write(utils.StringToByteSlice("  <tr>\n"))
		_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>%v</td>\n", volume.name)))
		if volume.fsckRunning {
			select {
			case volume.lastRunErr, ok = <-volume.errChan:
				// Apparently FSCK has finished
				volume.fsckRunning = false
				lastFinishTime := time.Now()
				volume.lastFinishTime = &lastFinishTime
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>last finished at %v</td>\n", volume.lastFinishTime)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    <td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("      <form action=\"/fsck-start/%v\" method=\"get\">\n", volume.name)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <button>RESTART</button>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </form>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </td>\n"))
				if nil == volume.lastRunErr {
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>No errors found</td>\n")))
				} else {
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>%v</td>\n", volume.lastRunErr)))
				}
			default:
				// Apparently FSCK is still running
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>started at %v</td>\n", volume.lastStartTime)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    <td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("      <form action=\"/fsck-stop/%v\" method=\"get\">\n", volume.name)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <button>STOP</button>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </form>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    <td></td>\n"))
			}
		} else {
			if (nil == volume.lastStopTime) && (nil == volume.lastFinishTime) {
				_, _ = responseWriter.Write(utils.StringToByteSlice("    <td></td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    <td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("      <form action=\"/fsck-start/%v\" method=\"get\">\n", volume.name)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <button>START</button>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </form>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td></td>\n")))
			} else {
				if nil == volume.lastFinishTime {
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>stopped at %v</td>\n", volume.lastStopTime)))
				} else {
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>last finished at %v</td>\n", volume.lastFinishTime)))
				}
				_, _ = responseWriter.Write(utils.StringToByteSlice("    <td>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("      <form action=\"/fsck-start/%v\" method=\"get\">\n", volume.name)))
				_, _ = responseWriter.Write(utils.StringToByteSlice("        <button>RESTART</button>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("      </form>\n"))
				_, _ = responseWriter.Write(utils.StringToByteSlice("    </td>\n"))
				if nil == volume.lastRunErr {
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>No errors found</td>\n")))
				} else {
					_, _ = responseWriter.Write(utils.StringToByteSlice(fmt.Sprintf("    <td>%v</td>\n", volume.lastRunErr)))
				}
			}
		}
		_, _ = responseWriter.Write(utils.StringToByteSlice("  </tr>\n"))
		volume.Unlock()
	}
	_, _ = responseWriter.Write(utils.StringToByteSlice("</table>\n"))
	globals.Unlock()
}

func doGetOfFSCKStart(responseWriter http.ResponseWriter, request *http.Request, volumeName string, interactive bool) {
	globals.Lock()
	volumeAsValue, ok, err := globals.volumeLLRB.GetByKey(volumeName)
	if nil != err {
		panic(fmt.Errorf("globals.volumeLLRB.GetByKey(%v)) failed: %v", volumeName, err))
	}
	if !ok {
		globals.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	volume, ok := volumeAsValue.(*volumeStruct)
	if !ok {
		panic(fmt.Errorf("volumeAsValue.(*volumeStruct) for key %v returned ok == false", volumeName))
	}
	volume.Lock()
	if volume.fsckRunning {
		if interactive {
			responseWriter.Header().Add("Location", "/fsck")
			responseWriter.WriteHeader(http.StatusTemporaryRedirect)
		} else {
			responseWriter.WriteHeader(http.StatusConflict)
		}
	} else {
		volume.fsckRunning = true
		lastStartTime := time.Now()
		volume.lastStartTime = &lastStartTime
		volume.lastStopTime = nil
		volume.lastFinishTime = nil
		go fs.ValidateVolume(volume.name, volume.stopChan, volume.errChan)
		if interactive {
			responseWriter.Header().Add("Location", "/fsck")
			responseWriter.WriteHeader(http.StatusTemporaryRedirect)
		} else {
			responseWriter.WriteHeader(http.StatusOK)
		}
	}
	volume.Unlock()
	globals.Unlock()
}

func doGetOfFSCKStatus(responseWriter http.ResponseWriter, request *http.Request, volumeName string) {
	globals.Lock()
	volumeAsValue, ok, err := globals.volumeLLRB.GetByKey(volumeName)
	if nil != err {
		panic(fmt.Errorf("globals.volumeLLRB.GetByKey(%v)) failed: %v", volumeName, err))
	}
	if !ok {
		globals.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	volume, ok := volumeAsValue.(*volumeStruct)
	if !ok {
		panic(fmt.Errorf("volumeAsValue.(*volumeStruct) for key %v returned ok == false", volumeName))
	}
	volume.Lock()
	if volume.fsckRunning {
		select {
		case volume.lastRunErr, ok = <-volume.errChan:
			// Apparently FSCK has finished
			volume.fsckRunning = false
			lastFinishTime := time.Now()
			volume.lastFinishTime = &lastFinishTime
			responseWriter.WriteHeader(http.StatusNotFound)
		default:
			// Apparently FSCK is still running
			responseWriter.WriteHeader(http.StatusFound)
		}
	} else {
		responseWriter.WriteHeader(http.StatusNotFound)
	}
	volume.Unlock()
	globals.Unlock()
}

func doGetOfFSCKStop(responseWriter http.ResponseWriter, request *http.Request, volumeName string, interactive bool) {
	globals.Lock()
	volumeAsValue, ok, err := globals.volumeLLRB.GetByKey(volumeName)
	if nil != err {
		panic(fmt.Errorf("globals.volumeLLRB.GetByKey(%v)) failed: %v", volumeName, err))
	}
	if !ok {
		globals.Unlock()
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}
	volume, ok := volumeAsValue.(*volumeStruct)
	if !ok {
		panic(fmt.Errorf("volumeAsValue.(*volumeStruct) for key %v returned ok == false", volumeName))
	}
	volume.Lock()
	if volume.fsckRunning {
		volume.fsckRunning = false
		lastStopTime := time.Now()
		volume.lastStopTime = &lastStopTime
		volume.stopChan <- true
		volume.lastRunErr = <-volume.errChan
		if nil != volume.lastRunErr {
			err := fmt.Errorf("FSCK of %v returned error: %v", volume.name, volume.lastRunErr)
			logger.ErrorWithError(err)
		}
		select {
		case _, _ = <-volume.stopChan:
			// Swallow our stopChan write that wasn't read before FSCK exited
		default:
			// Apparently FSCK read our stopChan write
		}
		if interactive {
			responseWriter.Header().Add("Location", "/fsck")
			responseWriter.WriteHeader(http.StatusTemporaryRedirect)
		} else {
			responseWriter.WriteHeader(http.StatusOK)
		}
	} else {
		if interactive {
			responseWriter.Header().Add("Location", "/fsck")
			responseWriter.WriteHeader(http.StatusTemporaryRedirect)
		} else {
			responseWriter.WriteHeader(http.StatusConflict)
		}
	}
	volume.Unlock()
	globals.Unlock()
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

	responseWriter.WriteHeader(http.StatusOK)

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
			panic(fmt.Errorf("statsLLRB.Put(%v, %v) failed: %v", statKey, statValueAsString, err))
		}
		if !ok {
			panic(fmt.Errorf("statsLLRB.Put(%v, %v) returned ok == false", statKey, statValueAsString))
		}
	}

	format = fmt.Sprintf("%%-%vs %%%vs\n", longestStatKey, longestStatValueAsString)

	lenStatsLLRB, err = statsLLRB.Len()
	if nil != err {
		panic(fmt.Errorf("statsLLRB.Len()) failed: %v", err))
	}
	if len(statsMap) != lenStatsLLRB {
		panic(fmt.Errorf("len(statsMap) != lenStatsLLRB"))
	}

	for i = 0; i < lenStatsLLRB; i++ {
		keyAsKey, valueAsValue, ok, err = statsLLRB.GetByIndex(i)
		if nil != err {
			panic(fmt.Errorf("statsLLRB.GetByIndex(%v) failed: %v", i, err))
		}
		if !ok {
			panic(fmt.Errorf("statsLLRB.GetByIndex(%v) returned ok == false", i))
		}
		keyAsString = keyAsKey.(string)
		valueAsString = valueAsValue.(string)
		line = fmt.Sprintf(format, keyAsString, valueAsString)
		_, _ = responseWriter.Write(utils.StringToByteSlice(line))
	}
}
