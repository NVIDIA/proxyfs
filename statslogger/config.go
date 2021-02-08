// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package statslogger

import (
	"runtime"
	"strings"
	"time"

	"github.com/NVIDIA/proxyfs/bucketstats"
	"github.com/NVIDIA/proxyfs/conf"
	"github.com/NVIDIA/proxyfs/logger"
	"github.com/NVIDIA/proxyfs/stats"
	"github.com/NVIDIA/proxyfs/swiftclient"
	"github.com/NVIDIA/proxyfs/transitions"
)

type globalsStruct struct {
	collectChan    <-chan time.Time // time to collect swiftclient stats
	logChan        <-chan time.Time // time to log statistics
	stopChan       chan bool        // time to shutdown and go home
	doneChan       chan bool        // shutdown complete
	statsLogPeriod time.Duration    // time between statistics logging
	verbose        bool             // verbosity of logging
	collectTicker  *time.Ticker     // ticker for collectChan (if any)
	logTicker      *time.Ticker     // ticker for logChan (if any)
}

var globals globalsStruct

func init() {
	transitions.Register("statslogger", &globals)
}

func parseConfMap(confMap conf.ConfMap) (err error) {
	globals.statsLogPeriod, err = confMap.FetchOptionValueDuration("StatsLogger", "Period")
	if err != nil {
		logger.Warnf("config variable 'StatsLogger.Period' defaulting to '10m': %v", err)
		globals.statsLogPeriod = time.Duration(10 * time.Minute)
	}

	// statsLogPeriod must be >= 1 sec, except 0 means disabled
	if globals.statsLogPeriod < time.Second && globals.statsLogPeriod != 0 {
		logger.Warnf("config variable 'StatsLogger.Period' value is non-zero and less then 1 min; defaulting to '10m'")
		globals.statsLogPeriod = time.Duration(10 * time.Minute)
	}

	globals.verbose, err = confMap.FetchOptionValueBool("StatsLogger", "Verbose")
	if err != nil {
		logger.Warnf("config variable 'StatsLogger.Verbose' defaulting to 'true': %v", err)
		globals.verbose = true
	}

	err = nil
	return
}

// Up initializes the package and must successfully return before any API
// functions are invoked
func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	err = parseConfMap(confMap)
	if err != nil {
		// parseConfMap() has logged an error
		return
	}

	if globals.statsLogPeriod == 0 {
		return
	}

	// collect info about free connections from SwiftClient once per second
	globals.collectTicker = time.NewTicker(1 * time.Second)
	globals.collectChan = globals.collectTicker.C

	// record statistics in the log periodically
	globals.logTicker = time.NewTicker(globals.statsLogPeriod)
	globals.logChan = globals.logTicker.C

	globals.stopChan = make(chan bool)
	globals.doneChan = make(chan bool)

	go statsLogger()
	return
}

func (dummy *globalsStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeToBeUnserved(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}
func (dummy *globalsStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	return nil
}

func (dummy *globalsStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	// read the new confmap; if the log period has changed or there was an
	// error shutdown the old logger prior to starting a new one
	oldLogPeriod := globals.statsLogPeriod
	err = parseConfMap(confMap)
	if err != nil {
		logger.ErrorWithError(err, "cannot parse confMap")
		if oldLogPeriod != 0 {
			globals.logTicker.Stop()
			globals.logTicker = nil

			globals.stopChan <- true
			_ = <-globals.doneChan
		}
		return
	}

	// if no change required, just return
	if globals.statsLogPeriod == oldLogPeriod {
		return
	}

	logger.Infof("statslogger log period changing from %d sec to %d sec",
		oldLogPeriod/time.Second, globals.statsLogPeriod/time.Second)
	// shutdown the old logger (if any) and start a new one (if any)
	if oldLogPeriod != 0 {
		globals.logTicker.Stop()
		globals.logTicker = nil

		globals.stopChan <- true
		_ = <-globals.doneChan
	}

	err = dummy.Up(confMap)
	return
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	// shutdown the stats logger (if any)
	logger.Infof("statslogger.Down() called")
	if globals.statsLogPeriod != 0 {
		globals.logTicker.Stop()
		globals.logTicker = nil

		globals.stopChan <- true
		_ = <-globals.doneChan
	}

	// err is already nil
	return
}

// the statsLogger collects the free connection statistics every collectChan tick
// and then logs a batch of statistics, including free connection statistics,
// every logChan tick ("statslogger.period" in the conf file.
//
func statsLogger() {
	var (
		chunkedConnectionStats    SimpleStats
		nonChunkedConnectionStats SimpleStats
		oldStatsMap               map[string]uint64
		newStatsMap               map[string]uint64
		oldMemStats               runtime.MemStats
		newMemStats               runtime.MemStats
	)

	chunkedConnectionStats.Clear()
	nonChunkedConnectionStats.Clear()
	chunkedConnectionStats.Sample(swiftclient.ChunkedConnectionFreeCnt())
	nonChunkedConnectionStats.Sample(swiftclient.NonChunkedConnectionFreeCnt())

	// memstats "stops the world"
	oldStatsMap = stats.Dump()
	runtime.ReadMemStats(&oldMemStats)

	// print an initial round of absolute stats
	logStats("total", &chunkedConnectionStats, &nonChunkedConnectionStats, &oldMemStats, oldStatsMap)

mainloop:
	for stopRequest := false; !stopRequest; {
		select {
		case <-globals.stopChan:
			// print final stats and then exit
			stopRequest = true

		case <-globals.collectChan:
			chunkedConnectionStats.Sample(swiftclient.ChunkedConnectionFreeCnt())
			nonChunkedConnectionStats.Sample(swiftclient.NonChunkedConnectionFreeCnt())
			continue mainloop

		case <-globals.logChan:
			// fall through to do the logging
		}

		runtime.ReadMemStats(&newMemStats)
		newStatsMap = stats.Dump()

		// collect an extra connection stats sample to ensure we have at least one
		chunkedConnectionStats.Sample(swiftclient.ChunkedConnectionFreeCnt())
		nonChunkedConnectionStats.Sample(swiftclient.NonChunkedConnectionFreeCnt())

		// print absolute stats and then deltas
		logStats("total", &chunkedConnectionStats, &nonChunkedConnectionStats, &newMemStats, newStatsMap)

		oldMemStats.Sys = newMemStats.Sys - oldMemStats.Sys
		oldMemStats.TotalAlloc = newMemStats.TotalAlloc - oldMemStats.TotalAlloc
		oldMemStats.HeapInuse = newMemStats.HeapInuse - oldMemStats.HeapInuse
		oldMemStats.HeapIdle = newMemStats.HeapIdle - oldMemStats.HeapIdle
		oldMemStats.HeapReleased = newMemStats.HeapReleased - oldMemStats.HeapReleased
		oldMemStats.StackSys = newMemStats.StackSys - oldMemStats.StackSys
		oldMemStats.MSpanSys = newMemStats.MSpanSys - oldMemStats.MSpanSys
		oldMemStats.MCacheSys = newMemStats.MCacheSys - oldMemStats.MCacheSys
		oldMemStats.BuckHashSys = newMemStats.BuckHashSys - oldMemStats.BuckHashSys
		oldMemStats.GCSys = newMemStats.GCSys - oldMemStats.GCSys
		oldMemStats.OtherSys = newMemStats.OtherSys - oldMemStats.OtherSys

		oldMemStats.NextGC = newMemStats.NextGC - oldMemStats.NextGC
		oldMemStats.NumGC = newMemStats.NumGC - oldMemStats.NumGC
		oldMemStats.NumForcedGC = newMemStats.NumForcedGC - oldMemStats.NumForcedGC
		oldMemStats.PauseTotalNs = newMemStats.PauseTotalNs - oldMemStats.PauseTotalNs
		oldMemStats.GCCPUFraction = newMemStats.GCCPUFraction - oldMemStats.GCCPUFraction

		for key := range newStatsMap {
			oldStatsMap[key] = newStatsMap[key] - oldStatsMap[key]
		}
		logStats("delta", nil, nil, &oldMemStats, oldStatsMap)

		if globals.verbose {
			logVerboseStats(&newMemStats, newStatsMap)
		}

		oldMemStats = newMemStats
		oldStatsMap = newStatsMap

		// clear the connection stats
		chunkedConnectionStats.Clear()
		nonChunkedConnectionStats.Clear()
	}

	globals.doneChan <- true
	return
}

// Write interesting statistics to the log in a semi-human readable format
//
// statsType is "total" or "delta" indicating whether statsMap and memStats are
// absolute or relative to the previous sample (doesn't apply to chunkedStats
// and nonChunkedStats, though they can be nil).
//
func logStats(statsType string, chunkedStats *SimpleStats, nonChunkedStats *SimpleStats,
	memStats *runtime.MemStats, statsMap map[string]uint64) {

	// if we have connection statistics, log them
	if chunkedStats != nil || nonChunkedStats != nil {
		logger.Infof("ChunkedFreeConnections: min=%d mean=%d max=%d  NonChunkedFreeConnections: min=%d mean=%d max=%d",
			chunkedStats.Min(), chunkedStats.Mean(), chunkedStats.Max(),
			nonChunkedStats.Min(), nonChunkedStats.Mean(), nonChunkedStats.Max())
	}

	// memory allocation info (see runtime.MemStats for definitions)
	// no GC stats logged at this point
	logger.Infof("Memory in Kibyte (%s): Sys=%d StackSys=%d MSpanSys=%d MCacheSys=%d BuckHashSys=%d GCSys=%d OtherSys=%d",
		statsType,
		int64(memStats.Sys)/1024, int64(memStats.StackSys)/1024,
		int64(memStats.MSpanSys)/1024, int64(memStats.MCacheSys)/1024,
		int64(memStats.BuckHashSys)/1024, int64(memStats.GCSys)/1024, int64(memStats.OtherSys)/1024)
	logger.Infof("Memory in Kibyte (%s): HeapInuse=%d HeapIdle=%d HeapReleased=%d Cumulative TotalAlloc=%d",
		statsType,
		int64(memStats.HeapInuse)/1024, int64(memStats.HeapIdle)/1024,
		int64(memStats.HeapReleased)/1024, int64(memStats.TotalAlloc)/1024)
	logger.Infof("GC Stats (%s): NumGC=%d  NumForcedGC=%d  NextGC=%d KiB  PauseTotalMsec=%d  GC_CPU=%4.2f%%",
		statsType,
		memStats.NumGC, memStats.NumForcedGC, int64(memStats.NextGC)/1024,
		memStats.PauseTotalNs/1000000, memStats.GCCPUFraction*100)

	// selected proxyfs statistics that show filesystem or swift activity; consolidate all
	// opps that query an account as SwiftAccountQueryOps, all opps that modify an account as
	// accountModifyOps, etc. A chunked put counts as 1 SwiftObjModifyOps but is also counted
	// separated in chunked put statistics.
	accountQueryOps := statsMap[stats.SwiftAccountGetOps] + statsMap[stats.SwiftAccountHeadOps]
	accountModifyOps := (statsMap[stats.SwiftAccountDeleteOps] + statsMap[stats.SwiftAccountPostOps] +
		statsMap[stats.SwiftAccountPutOps])

	containerQueryOps := statsMap[stats.SwiftContainerGetOps] + statsMap[stats.SwiftContainerHeadOps]
	containerModifyOps := (statsMap[stats.SwiftContainerDeleteOps] + statsMap[stats.SwiftContainerPostOps] +
		statsMap[stats.SwiftContainerPutOps])

	objectQueryOps := (statsMap[stats.SwiftObjGetOps] + statsMap[stats.SwiftObjHeadOps] +
		statsMap[stats.SwiftObjContentLengthOps] + statsMap[stats.SwiftObjLoadOps] +
		statsMap[stats.SwiftObjTailOps])
	objectModifyOps := (statsMap[stats.SwiftObjDeleteOps] + statsMap[stats.SwiftObjCopyOps] +
		statsMap[stats.SwiftObjPutCtxFetchOps])

	chunkedPutFetchOps := statsMap[stats.SwiftObjPutCtxFetchOps]
	chunkedPutQueryOps := statsMap[stats.SwiftObjPutCtxReadOps]
	chunkedPutModifyOps := statsMap[stats.SwiftObjPutCtxSendChunkOps]
	chunkedPutCloseOPs := statsMap[stats.SwiftObjPutCtxCloseOps]

	logger.Infof("Swift Client Ops (%s): Account QueryOps=%d ModifyOps=%d Container QueryOps=%d ModifyOps=%d Object QueryOps=%d ModifyOps=%d",
		statsType, accountQueryOps, accountModifyOps,
		containerQueryOps, containerModifyOps, objectQueryOps, objectModifyOps)
	logger.Infof("Swift Client ChunkedPut Ops (%s): FetchOps=%d ReadOps=%d SendOps=%d CloseOps=%d",
		statsType, chunkedPutFetchOps, chunkedPutQueryOps, chunkedPutModifyOps, chunkedPutCloseOPs)
}

func logVerboseStats(memStats *runtime.MemStats, statsMap map[string]uint64) {
	var (
		bucketstatsValue       string
		bucketstatsValues      string
		bucketstatsValuesSlice []string
		i                      int
		pauseNsAccumulator     uint64
		statKey                string
		statValue              uint64
		varboseStatKey         string
		varboseStatValue       uint64
		varboseStatsMap        map[string]uint64
	)

	varboseStatsMap = make(map[string]uint64)

	// General statistics.
	varboseStatsMap["go_runtime_MemStats_Alloc"] = memStats.Alloc
	varboseStatsMap["go_runtime_MemStats_TotalAlloc"] = memStats.TotalAlloc
	varboseStatsMap["go_runtime_MemStats_Sys"] = memStats.Sys
	varboseStatsMap["go_runtime_MemStats_Lookups"] = memStats.Lookups
	varboseStatsMap["go_runtime_MemStats_Mallocs"] = memStats.Mallocs
	varboseStatsMap["go_runtime_MemStats_Frees"] = memStats.Frees

	// Main allocation heap statistics.
	varboseStatsMap["go_runtime_MemStats_HeapAlloc"] = memStats.HeapAlloc
	varboseStatsMap["go_runtime_MemStats_HeapSys"] = memStats.HeapSys
	varboseStatsMap["go_runtime_MemStats_HeapIdle"] = memStats.HeapIdle
	varboseStatsMap["go_runtime_MemStats_HeapInuse"] = memStats.HeapInuse
	varboseStatsMap["go_runtime_MemStats_HeapReleased"] = memStats.HeapReleased
	varboseStatsMap["go_runtime_MemStats_HeapObjects"] = memStats.HeapObjects

	// Low-level fixed-size structure allocator statistics.
	//	Inuse is bytes used now.
	//	Sys is bytes obtained from system.
	varboseStatsMap["go_runtime_MemStats_StackInuse"] = memStats.StackInuse
	varboseStatsMap["go_runtime_MemStats_StackSys"] = memStats.StackSys
	varboseStatsMap["go_runtime_MemStats_MSpanInuse"] = memStats.MSpanInuse
	varboseStatsMap["go_runtime_MemStats_MSpanSys"] = memStats.MSpanSys
	varboseStatsMap["go_runtime_MemStats_MCacheInuse"] = memStats.MCacheInuse
	varboseStatsMap["go_runtime_MemStats_MCacheSys"] = memStats.MCacheSys
	varboseStatsMap["go_runtime_MemStats_BuckHashSys"] = memStats.BuckHashSys
	varboseStatsMap["go_runtime_MemStats_GCSys"] = memStats.GCSys
	varboseStatsMap["go_runtime_MemStats_OtherSys"] = memStats.OtherSys

	// Garbage collector statistics (fixed portion).
	varboseStatsMap["go_runtime_MemStats_LastGC"] = memStats.LastGC
	varboseStatsMap["go_runtime_MemStats_PauseTotalNs"] = memStats.PauseTotalNs
	varboseStatsMap["go_runtime_MemStats_NumGC"] = uint64(memStats.NumGC)
	varboseStatsMap["go_runtime_MemStats_GCCPUPercentage"] = uint64(100.0 * memStats.GCCPUFraction)

	// Garbage collector statistics (go_runtime_MemStats_PauseAverageNs).
	if 0 == memStats.NumGC {
		varboseStatsMap["go_runtime_MemStats_PauseAverageNs"] = 0
	} else {
		pauseNsAccumulator = 0
		if memStats.NumGC < 255 {
			for i = 0; i < int(memStats.NumGC); i++ {
				pauseNsAccumulator += memStats.PauseNs[i]
			}
			varboseStatsMap["go_runtime_MemStats_PauseAverageNs"] = pauseNsAccumulator / uint64(memStats.NumGC)
		} else {
			for i = 0; i < 256; i++ {
				pauseNsAccumulator += memStats.PauseNs[i]
			}
			varboseStatsMap["go_runtime_MemStats_PauseAverageNs"] = pauseNsAccumulator / 256
		}
	}

	for statKey, statValue = range statsMap {
		varboseStatKey = strings.Replace(statKey, ".", "_", -1)
		varboseStatKey = strings.Replace(varboseStatKey, "-", "_", -1)
		varboseStatsMap[varboseStatKey] = statValue
	}

	for varboseStatKey, varboseStatValue = range varboseStatsMap {
		logger.Infof("metrics %s: %d", varboseStatKey, varboseStatValue)
	}

	bucketstatsValues = bucketstats.SprintStats(bucketstats.StatFormatParsable1, "*", "*")

	bucketstatsValuesSlice = strings.Split(bucketstatsValues, "\n")

	for _, bucketstatsValue = range bucketstatsValuesSlice {
		logger.Infof("stats %s", bucketstatsValue)
	}
}
