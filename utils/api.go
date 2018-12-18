// Package utils provides miscellaneous utilities for ProxyFS.
package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TryLockMutex is used to support a timeout a the lock request
type TryLockMutex struct {
	c chan struct{} // a  lock()    request             writes a struct{} to   c
	//                 a  tryLock() request attempts to write  a struct{} to   c but will give up after a time.Duration
	//                 an unlock()  request             reads  a struct{} from c
}

func NewTryLockMutex() (tryLockMutex *TryLockMutex) {
	return &TryLockMutex{c: make(chan struct{}, 1)} // since there is space for one struct{}, lock is initially available
}

func (tryLockMutex *TryLockMutex) Lock() {
	tryLockMutex.c <- struct{}{}
}

func (tryLockMutex *TryLockMutex) TryLock(timeout time.Duration) (gotIt bool) {
	timer := time.NewTimer(timeout)
	select {
	case tryLockMutex.c <- struct{}{}:
		timer.Stop()
		gotIt = true
	case <-time.After(time.Duration(timeout)):
		gotIt = false
	}
	return
}

func (tryLockMutex *TryLockMutex) Unlock() {
	<-tryLockMutex.c
}

// MultiWaiterWaitGroup emulates the behavior of sync.WaitGroup while enabling multiple waiters.
//
// Unline sync.WaitGroup, however, you must allocate a MultiWaiterWaitGroup with a
// call to FetchMultiWaiterWaitGroup().
type MultiWaiterWaitGroup struct {
	sync.Mutex
	cv           *sync.Cond
	numWaiters   uint64
	numSignalers int
}

func FetchMultiWaiterWaitGroup() (mwwg *MultiWaiterWaitGroup) {
	mwwg = &MultiWaiterWaitGroup{numWaiters: 0, numSignalers: 0}
	mwwg.cv = sync.NewCond(mwwg)
	return
}

func (mwwg *MultiWaiterWaitGroup) Add(delta int) {
	mwwg.Lock()
	mwwg.numSignalers += delta
	if 0 > mwwg.numSignalers {
		err := fmt.Errorf("*MultiWaiterWaitGroup.Add(%v) has taken numSignalers < 0", delta)
		panic(err)
	}
	if (0 == mwwg.numSignalers) && (0 < mwwg.numWaiters) {
		mwwg.numWaiters = 0
		mwwg.cv.Broadcast()
	}
	mwwg.Unlock()
}

func (mwwg *MultiWaiterWaitGroup) Done() {
	mwwg.Add(-1)
}

func (mwwg *MultiWaiterWaitGroup) Wait() {
	mwwg.Lock()
	for 0 != mwwg.numSignalers {
		mwwg.numWaiters++
		mwwg.cv.Wait()
	}
	mwwg.Unlock()
}

func ByteSliceToUint32(byteSlice []byte) (u32 uint32, ok bool) {
	if 4 != len(byteSlice) {
		ok = false
		return
	}

	u32 = binary.LittleEndian.Uint32(byteSlice)
	ok = true

	return
}

func Uint32ToByteSlice(u32 uint32) (byteSlice []byte) {
	byteSlice = make([]byte, 4)

	binary.LittleEndian.PutUint32(byteSlice, u32)

	return
}

func ByteSliceToUint64(byteSlice []byte) (u64 uint64, ok bool) {
	if 8 != len(byteSlice) {
		ok = false
		return
	}

	u64 = binary.LittleEndian.Uint64(byteSlice)
	ok = true

	return
}

func Uint64ToByteSlice(u64 uint64) (byteSlice []byte) {
	byteSlice = make([]byte, 8)

	binary.LittleEndian.PutUint64(byteSlice, u64)

	return
}

func ByteSliceToString(byteSlice []byte) (str string) {
	str = string(byteSlice[:])
	return
}

func StringToByteSlice(str string) (byteSlice []byte) {
	byteSlice = []byte(str)
	return
}

func PathToAcctContObj(path string) (accountName string, containerName string, objectName string, err error) {
	//  0  1->          2->       3->
	//  v1/AUTH_account/container/object/possibly/including/slashes
	pathSplit := strings.SplitN(path[1:], "/", 4)
	if len(pathSplit) < 2 {
		return "", "", "", fmt.Errorf("%q is not a valid Swift path", path)
	}
	accountName = pathSplit[1]
	if len(pathSplit) == 2 {
		containerName = ""
		objectName = ""
	} else {
		containerName = pathSplit[2]
		if len(pathSplit) == 3 {
			objectName = ""
		} else {
			objectName = pathSplit[3]
		}
	}
	return accountName, containerName, objectName, nil
}

// XXX TODO TEMPORARY:
//
// I know our go-overlords would prefer that we knew nothing about goroutines,
// but logging the goroutine context can be useful when trying to debug things
// like locking.
//
// Intent is to have this now and hopefully remove it once we've gotten debugged.
//
func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

// Return a string containing calling function and package
func GetAFnName(level int) string {
	// Get the PC and file for the level requested, adding one level to skip this function
	pc, _, _, _ := runtime.Caller(level + 1)
	// Retrieve a Function object this functions parent
	functionObject := runtime.FuncForPC(pc)
	// Regex to extract just the package and function name (and not the module path)
	extractFnName := regexp.MustCompile(`[^\/]*$`)
	return extractFnName.FindString(functionObject.Name())
}

// Return separage strings containing calling function and package
//
// XXX TODO TEMPORARY: also return goroutine id
//
func GetFuncPackage(level int) (fn string, pkg string, gid uint64) {
	// Get the combined function and package names of our caller
	funcPkg := GetAFnName(level + 1)

	// Regex to extract the package name (beginning of string to first ".")
	extractPkgName := regexp.MustCompile(`^[^.]*`)
	pkg = extractPkgName.FindString(funcPkg)

	// Regex to extract the function name (end of string to last ".")
	extractFnName := regexp.MustCompile(`[^.]*$`)
	fn = extractFnName.FindString(funcPkg)

	// TEMPORARY: get goroutine id
	gid = GetGID()

	return fn, pkg, gid
}

// GetFnName returns a string containing the name of the running function and its package.
// This can be useful for debug prints.
func GetFnName() string {
	// Skip this function, and fetch the PC and file for its parent
	return GetAFnName(1)
}

// GetCallerFnName returns a string containing the name of the calling function.
// This can be useful for debug prints.
func GetCallerFnName() string {
	// Skip this function and its caller, and fetch the PC and file for its (grand)parent
	return GetAFnName(2)
}

// ByteToHexDigit returns the (uppercase) hex character representation of the low order nibble of the byte supplied.
func ByteToHexDigit(u8 byte) (digit byte) {
	u8 = u8 & 0x0F
	if 0x0A > u8 {
		digit = '0' + u8
	} else {
		digit = 'A' + (u8 - 0x0A)
	}

	return
}

func Uint64ToHexStr(value uint64) string {
	return fmt.Sprintf("%016X", value)
}

func HexStrToUint64(value string) (uint64, error) {
	return strconv.ParseUint(value, 16, 64)
}

type Stopwatch struct {
	StartTime   time.Time
	StopTime    time.Time
	ElapsedTime time.Duration
	IsRunning   bool
}

func NewStopwatch() *Stopwatch {
	return &Stopwatch{StartTime: time.Now(), IsRunning: true}
}

func (sw *Stopwatch) Stop() time.Duration {
	sw.StopTime = time.Now()

	// Stopwatch should have been running when stopped, but
	// to avoid making callers do error checking we just
	// don't do calculations if it wasn't.
	if sw.IsRunning {
		sw.ElapsedTime = sw.StopTime.Sub(sw.StartTime)
		sw.IsRunning = false
	}
	return sw.ElapsedTime
}

func (sw *Stopwatch) Restart() {
	// Stopwatch should not be running when restarted, but
	// to avoid making callers do error checking we just
	// don't do anything if it wasn't.
	if !sw.IsRunning {
		sw.ElapsedTime = 0
		sw.StartTime = time.Now()
		sw.StopTime = time.Time{}
		sw.IsRunning = true
	}
}

func (sw *Stopwatch) Elapsed() time.Duration {
	if !sw.IsRunning {
		// Not running, return elapsed time when stopped
		return sw.ElapsedTime
	}

	// Otherwise still running, return time so far
	return time.Since(sw.StartTime)
}

func (sw *Stopwatch) ElapsedSec() int64 {
	return int64(sw.Elapsed() / time.Second)
}

func (sw *Stopwatch) ElapsedMs() int64 {
	return int64(sw.Elapsed() / time.Millisecond)
}

func (sw *Stopwatch) ElapsedUs() int64 {
	return int64(sw.Elapsed() / time.Microsecond)
}

func (sw *Stopwatch) ElapsedNs() int64 {
	return int64(sw.Elapsed() / time.Nanosecond)
}

func (sw *Stopwatch) ElapsedSecString() string {
	return strconv.FormatInt(sw.ElapsedSec(), 10) + "s"
}

func (sw *Stopwatch) ElapsedMsString() string {
	return strconv.FormatInt(sw.ElapsedMs(), 10) + "ms"
}

func (sw *Stopwatch) ElapsedUsString() string {
	return strconv.FormatInt(sw.ElapsedUs(), 10) + "us"
}

func (sw *Stopwatch) ElapsedNsString() string {
	return strconv.FormatInt(sw.ElapsedNs(), 10) + "ns"
}

func (sw *Stopwatch) ElapsedString() string {
	return sw.Elapsed().String()
}

type EventTime struct {
	event     string
	timestamp time.Time
}

type EventDuration struct {
	event     string
	numEvents int
	duration  time.Duration
}

func (e *EventDuration) durationMs() int64 {
	return e.duration.Nanoseconds() / int64(time.Millisecond)
}

func (t *EventTime) SubUs(prevTime time.Time) (durationUs int64) {
	return t.timestamp.Sub(prevTime).Nanoseconds() / int64(time.Microsecond)
}

func (t *EventTime) Sub(prevTime time.Time) (duration time.Duration) {
	return t.timestamp.Sub(prevTime)
}

func isSmallerThan(newDur time.Duration, oldDur time.Duration) bool {
	oldNs := oldDur.Nanoseconds()
	newNs := newDur.Nanoseconds()
	return newNs < oldNs
}

func isGreaterThan(newDur time.Duration, oldDur time.Duration) bool {
	oldNs := oldDur.Nanoseconds()
	newNs := newDur.Nanoseconds()
	return newNs > oldNs
}

type Profiler struct {
	op     string
	Timer  *Stopwatch
	events map[int]EventTime
}

type EventStats struct {
	op          string
	numOps      int
	opDuration  time.Duration
	lwmDuration time.Duration
	hwmDuration time.Duration
	events      map[int]EventDuration
}

func (s *EventStats) incOpDuration(duration time.Duration) {
	s.opDuration += duration

	// Set high/low duration, if applicable
	s.setHighLowValue(duration)
}

func (s *EventStats) opDurationMs() int64 {
	return s.opDuration.Nanoseconds() / int64(time.Millisecond)
}

var zeroDuration time.Duration

func durationIsZero(duration time.Duration) bool {
	// Go should have an API for this, but it doesn't
	return duration.Nanoseconds() == zeroDuration.Nanoseconds()
}

func (s *EventStats) setHighLowValue(duration time.Duration) {
	if isSmallerThan(duration, s.lwmDuration) || durationIsZero(s.lwmDuration) {
		s.lwmDuration = duration
	} else {
		// Value is either higher or the same
		s.hwmDuration = duration
	}
}

func (s *EventStats) lowValueMs() int64 {
	return s.lwmDuration.Nanoseconds() / int64(time.Millisecond)
}

func (s *EventStats) lowValueUs() int64 {
	return s.lwmDuration.Nanoseconds() / int64(time.Microsecond)
}

func (s *EventStats) highValueMs() int64 {
	return s.hwmDuration.Nanoseconds() / int64(time.Millisecond)
}

func (s *EventStats) highValueUs() int64 {
	return s.hwmDuration.Nanoseconds() / int64(time.Microsecond)
}
func (s *EventStats) DumpToStdout() {
	outStr := s.DumpToString(false)
	fmt.Printf("%s", outStr)
}

func (s *EventStats) DumpToString(condensed bool) (out string) {

	var os string           // Output separator; ";" if condensed, "\n" if not
	var evNameFormat string // Print format for event name
	if condensed {
		os = " ; "
		evNameFormat = " %s "
	} else {
		os = "\n"
		evNameFormat = "%40s   "
	}

	out = fmt.Sprintf("Duration stats for %v, number of ops %v:    [single op low %d us, high %d us]%s", s.op, s.numOps, s.lowValueUs(), s.highValueUs(), os)

	var totalEventTimeMs int64

	totalDurationMs := s.opDurationMs()
	if totalDurationMs == 0 {
		// We got here by avoiding a divide-by-zero situation, but let's print out the
		// total duration in case Ms is zero but Us isn't
		out += fmt.Sprintf(evNameFormat+"     %v  %s", "total duration", s.opDuration, os)
		return out
	}

	for evNum := 0; evNum < len(s.events); evNum++ {
		thisEvent := s.events[evNum]

		eventTimeMs := thisEvent.durationMs()
		totalEventTimeMs += eventTimeMs

		out += fmt.Sprintf(evNameFormat+"+ %6d ms  (%3v %%)  [numEvents %v] %s",
			thisEvent.event, eventTimeMs, eventTimeMs*100/totalDurationMs, thisEvent.numEvents, os)
	}
	remainingTime := totalDurationMs - totalEventTimeMs
	out += fmt.Sprintf(evNameFormat+"+ %6d ms  (%3v %%)%s", "(remaining time)", remainingTime, remainingTime*100/totalDurationMs, os)
	out += fmt.Sprintf(evNameFormat+"+ %6d ms  (%3v %%)%s", "total duration", totalDurationMs, totalDurationMs*100/totalDurationMs, os)
	return out
}

func GetStats(profiles map[int]*Profiler) *EventStats {
	stats := &EventStats{events: make(map[int]EventDuration, 10)}

	// If events are not homogeneous, pick the profile with the most events.
	// NOTE: We're still assuming here that the only difference is that some profiles
	//       have more events than others, but the additional events are homogeneous.
	numEvents := len(profiles[0].events)
	pNumForEvents := 0
	for pNum, p := range profiles {
		numProfileEvents := len(p.events)

		if numProfileEvents > numEvents {
			numEvents = numProfileEvents
			pNumForEvents = pNum
		}
	}
	//fmt.Printf("Should get events from profile %v; number of events (%v)\n", pNumForEvents, numEvents)

	masterProfile := profiles[pNumForEvents]

	if len(profiles) > 0 {
		profiler := masterProfile

		// Init op from first profile
		stats.op = profiler.op

		// Set number of operations
		stats.numOps = len(profiles)

		// Add events from the profile with the most events
		for evNum := 0; evNum < len(profiler.events); evNum++ {
			// add events
			durEvent := EventDuration{event: profiler.events[evNum].event}
			stats.events[evNum] = durEvent
		}
	}

	// Go through the profilers and add the durations for each event across
	// all profilers. Assumption here is that events are homogeneous.
	for _, p := range profiles {
		numProfileEvents := len(p.events)

		prevTime := p.Timer.StartTime
		for evNum := 0; evNum < numProfileEvents; evNum++ {

			event := p.events[evNum]
			eventIndex := evNum

			// For profiles with a different number of events than the max,
			// adjust event numbers
			if numProfileEvents != numEvents {

				// Find the matching event from the master profile.
				// When looping over the events, go makes sure that they don't
				// appear in numerical or consistent order.
				foundMatchingEvent := false

				for masterEvNum := 0; masterEvNum < numEvents; masterEvNum++ {

					if p.events[evNum].event == masterProfile.events[masterEvNum].event {
						// Found a match
						foundMatchingEvent = true
						eventIndex = masterEvNum
						break
					}
				}

				if !foundMatchingEvent {
					// Uh oh, we can't find a match!
					fmt.Printf("Error, couldn't find a match for event %v! This profile's events: %v, master event list %v\n",
						p.events[evNum].event, p.events, masterProfile.events)

					// Skip the rest of this profile and go to the next one; this one is broken.
					break
				} else {
					//fmt.Printf("profile %v evNum %v (%v) maps to master event %v (%v).\n",
					//           pNum, evNum, p.events[evNum].event, eventIndex, masterProfile.events[eventIndex].event)
				}
			}

			// NOTE: go won't let you assign directly to a map struct like this:
			//stats.events[eventIndex].duration += event.Sub(prevTime)

			statsEvent := stats.events[eventIndex]
			statsEvent.duration += event.Sub(prevTime)
			statsEvent.numEvents++
			stats.events[eventIndex] = statsEvent

			prevTime = event.timestamp
		}

		// Keep track of overall total time for all events
		stats.incOpDuration(p.Timer.Elapsed())
	}

	return stats
}

var defaultEventsToAlloc int = 40

func NewProfiler(timedOp string) *Profiler {
	return &Profiler{op: timedOp,
		Timer:  NewStopwatch(),
		events: make(map[int]EventTime, defaultEventsToAlloc)}
}

// NewProfilerIf allocates and returns a new Profiler, if condition is true.
func NewProfilerIf(condition bool, timedOp string) *Profiler {
	if condition {
		return &Profiler{op: timedOp,
			Timer:  NewStopwatch(),
			events: make(map[int]EventTime, defaultEventsToAlloc)}
	} else {
		return nil
	}
}

func (p *Profiler) AddEventNow(eventName string) {
	if p == nil {
		return
	}

	// Use numeric index so that they stay in the order in which they occurred
	eventIndex := len(p.events)
	p.events[eventIndex] = EventTime{event: eventName, timestamp: time.Now()}
}

func (p *Profiler) Close() {
	if p == nil {
		return
	}

	p.Timer.Stop()
}

func (p *Profiler) SetName(opName string) {
	if p == nil {
		return
	}

	p.op = opName
}

func (p *Profiler) Name() string {
	if p == nil {
		return ""
	}

	return p.op
}

func (p *Profiler) DumpRaw() {
	fmt.Printf("Profiler is %+v\n", p)
}

func (p *Profiler) Dump() {
	if p == nil {
		fmt.Printf("Profiler is nil!\n")
		return
	}

	fmt.Printf("Profiler for op %v is:\n", p.op)

	prevTime := p.Timer.StartTime
	totalDuration := p.Timer.ElapsedUs()
	totalEventTime := int64(0)

	for evNum := 0; evNum < len(p.events); evNum++ {
		event := p.events[evNum]
		eventTime := event.SubUs(prevTime)
		totalEventTime += eventTime
		fmt.Printf("%30s   + %6d us  (%3v %%)\n", event.event, eventTime, eventTime*100/totalDuration)
		prevTime = event.timestamp
	}
	remainingTime := totalDuration - totalEventTime
	fmt.Printf("%30s   + %6d us  (%3v %%)\n", "(remaining time)", remainingTime, remainingTime*100/totalDuration)
	fmt.Printf("%30s   + %6d us  (%3v %%)\n", "total duration", totalDuration, totalDuration*100/totalDuration)
}

func JSONify(input interface{}, indentify bool) (output string) {
	var (
		err             error
		inputJSON       bytes.Buffer
		inputJSONPacked []byte
	)

	inputJSONPacked, err = json.Marshal(input)
	if nil == err {
		if indentify {
			err = json.Indent(&inputJSON, inputJSONPacked, "", "\t")
			if nil == err {
				output = inputJSON.String()
			} else {
				output = fmt.Sprintf("<<<json.Indent failed: %v>>>", err)
			}
		} else {
			output = string(inputJSONPacked)
		}
	} else {
		output = fmt.Sprintf("<<<json.Marshall failed: %v>>>", err)
	}

	return
}
