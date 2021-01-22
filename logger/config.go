// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/swiftstack/ProxyFS/conf"
)

// RFC3339 format with microsecond precision
//
const timeFormat = "2006-01-02T15:04:05.000000Z07:00"

var logFile *os.File = nil

// multiWriter groups multiple io.Writers into a single io.Writer. (Our
// immediate motivation for this is that logrus's SetOutput expects an
// io.Writer, but we might want to log to both the console and a file in
// development, and this seems potentially simpler in some aspects than
// defining a Hook. We may want to revisit this judgement—again—later.
//
// Supporting multiple writers is now a full-fledged feature of logger.
//
type multiWriter struct {
	writers []io.Writer
}

// The global list of log targets to write to.
//
// logTargets should probably be protected by a lock or use some clever RCU
// update technique, but its really only changed for test cases.
//
var logTargets multiWriter

func (mw *multiWriter) addWriter(writer io.Writer) {
	mw.writers = append(mw.writers, writer)
}

func (mw *multiWriter) Write(p []byte) (n int, err error) {
	for _, writer := range mw.writers {
		n, err = writer.Write(p)
		// regrettably, the first thing that goes wrong determines our return
		// values
		if err != nil {
			return
		}
	}
	return
}

func (mw *multiWriter) Clear() {
	mw.writers = []io.Writer{}
}

func addLogTarget(writer io.Writer) {
	logTargets.addWriter(writer)
}

// This is used by LogTarget, which is a logging target that is useful for
// capturing the output logged by other packages for testing in test cases.
//
func (log LogTarget) write(p []byte) (n int, err error) {
	log.LogBuf.Lock()
	defer log.LogBuf.Unlock()

	for i := len(log.LogBuf.LogEntries) - 1; i > 0; i-- {
		log.LogBuf.LogEntries[i] = log.LogBuf.LogEntries[i-1]
	}
	log.LogBuf.LogEntries[0] = strings.TrimRight(string(p), " \t\n")

	log.LogBuf.TotalEntries++
	return 0, nil
}

// openLogFile is called to open the log file and (re-) int the logger.  This
// really should be done before using it, but you can log things before calling.
// However, they will not appear in the logfile and will not be in the new text
// format.
//
// Config variables that affect logging include:
//     Logging.LogFilePath        string       if present, pathname to log file
//     Logging.LogToConsole       bool         if present and true, log to console as well as file
//     Logging.TraceLevelLogging  stringslice  list of packages where tracing is enabled (name must
//                                             also appear in packageTraceSettings)
//     Logging.DebugLevelLogging  stringslice
//
func openLogFile(confMap conf.ConfMap) (err error) {
	log.SetFormatter(&log.TextFormatter{DisableColors: true, TimestampFormat: timeFormat})

	// Fetch log file info, if provided
	logFilePath, _ := confMap.FetchOptionValueString("Logging", "LogFilePath")
	if logFilePath != "" {
		logFile, err = os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Errorf("couldn't open log file '%s': %v", logFilePath, err)
			return err
		}
	}

	// Determine whether we should log to console. Default is false.
	logToConsole, err := confMap.FetchOptionValueBool("Logging", "LogToConsole")
	if err != nil {
		logToConsole = false
		err = nil
	}

	log.SetOutput(&logTargets)
	if logFilePath == "" {
		addLogTarget(os.Stderr)
	} else {
		addLogTarget(logFile)
		if logToConsole {
			addLogTarget(os.Stderr)
		}
	}

	// NOTE: We always enable max logging in logrus, and either decide in
	//       this package whether to log OR log everything and parse it out of
	//       the logs after the fact
	log.SetLevel(log.DebugLevel)

	// Fetch trace and debug log settings, if provided
	traceConfSlice, _ := confMap.FetchOptionValueStringSlice("Logging", "TraceLevelLogging")
	setTraceLoggingLevel(traceConfSlice)

	debugConfSlice, _ := confMap.FetchOptionValueStringSlice("Logging", "DebugLevelLogging")
	setDebugLoggingLevel(debugConfSlice)

	Infof("logger opened logfile '%s' (PID %d)", logFilePath, os.Getpid())
	return
}

// closeLogFile closes the output log file as part of shutdown or as part of
// rotating to a new log file.
//
func closeLogFile(confMap conf.ConfMap) (err error) {

	Infof("logger closing logfile (PID %d)", os.Getpid())

	// We open and close our own logfile
	if logFile != nil {
		// Sync() flushes data cached in the kernel to disk, which is
		// really only useful if the OS were to crash soon
		logFile.Sync()
		logFile.Close()
	}
	logTargets.Clear()
	err = nil
	return
}

// Up opens the logfile.  This really should be done before using the logfile,
// but you can log things before calling.  However, they will not appear in
// the logfile and will not be in the new text format.
//
// Config variables that affect logging include:
//     Logging.LogFilePath        string       if present, pathname to log file
//     Logging.LogToConsole       bool         if present and true, log to console as well as file
//     Logging.TraceLevelLogging  stringslice  list of packages where tracing is enabled (name must
//                                             also appear in packageTraceSettings)
//     Logging.DebugLevelLogging  stringslice
//

func Up(confMap conf.ConfMap) (err error) {

	err = openLogFile(confMap)
	if err != nil {
		return
	}

	Infof("logger is starting up (PID %d)", os.Getpid())
	return
}

func SignaledStart(confMap conf.ConfMap) (err error) {
	err = nil
	return
}

func SignaledFinish(confMap conf.ConfMap) (err error) {

	Infof("logger is closing logfile (PID %d)", os.Getpid())
	err = closeLogFile(confMap)
	if nil == err {
		err = openLogFile(confMap)
		Infof("logger opened logfile (PID %d)", os.Getpid())
	}
	return
}

func Down(confMap conf.ConfMap) (err error) {

	log.Infof("logger is shutting down (PID %d)", os.Getpid())

	err = closeLogFile(confMap)
	return
}

// Parse a log entry captured via LogTarget return the fields as key value pairs
// in a map.
//
// Match log entries look like:
//
// time="2017-07-27T01:30:46.060080Z" level=info msg="retry.RequestWithRetry(): swiftclient.testRetry.request(1) succeeded after 4 attempts in 0.031 sec" function=RequestWithRetry goroutine=6 package=swiftclient
//
// time="2017-07-27T02:18:19.214012Z" level=error msg="retry.RequestWithRetry(): swiftclient.testRetry.request(1) failed after 7 attempts in 0.053 sec with retriable error" error="Simulate a retriable errror" function=RequestWithRetry goroutine=6 package=swiftclient
//
// time="2017-07-27T02:09:32.259383Z" level=error msg="retry.RequestWithRetry(): swiftclient.testRetry.request(1) failed after 6 attempts in 0.054 sec with unretriable error" error="Simulate an unretriable error" function=RequestWithRetry goroutine=20 package=swiftclient
//
func parseLogEntry(entry string) (fields map[string]string, err error) {
	var (
		matches []string
	)

	var fieldRE = regexp.MustCompile(
		`^time="(?P<time>[-:0-9.ZTt_]+)" level=(?P<level>[a-zA-Z]+) msg="(?P<msg>([^"]|\\")+)" (error="(?P<error>([^"]|\\")+)" )?function=(?P<function>\w+) goroutine=(?P<goroutine>\d+) package=(?P<package>\w+)`)

	matches = fieldRE.FindStringSubmatch(entry)
	if matches == nil {
		fmt.Printf("parseLogEntry: log entry not matched by regular expression fieldRE: '%s'\n", entry)
		err = fmt.Errorf("log entry not matched by regular expression fieldRE: '%s'", entry)
		return
	}

	fields = make(map[string]string)
	for idx, name := range fieldRE.SubexpNames() {
		if name != "" && matches[idx] != "" {
			fields[name] = matches[idx]
		}
	}

	return
}

// Parse the log entries, starting with the most recent, looking for a message
// generated by the function funcName that matches the regular expression
// logEntryRE within the most recent maxEntries lines of the log.
//
// If found, return the parsed fields from the log message, which are a
// combination of the fields returned by ParseLogEntry() and the fields in the
// passed regular expression (which must use names for the matching parts).
// funcName must match the contents of the field "function" returned by
// ParseLogEntry().  entryIdx is the index of the entry in the log with 0 being
// the most recent.
//
// If not found, return an error.
//
// Example regexp from trackedlock/api_test.go that matches:
//
// msg="trackedlock watcher: *trackedlock.Mutex at 0xc420110000 locked for 2 sec; stack at Lock() call:\ngoroutine 19 [running]:..."
//
// watcherLogMatch = `^trackedlock watcher: (?P<type>[*a-zA-Z0-9_.]+) at (?P<ptr>0x[0-9a-f]+) locked for (?P<time>\d+) sec; stack at (?P<locker>[a-zA-Z0-9_()]+) call:\\n(?P<lockStack>.*)$`
//
func parseLogForFunc(logcopy LogTarget, funcName string, logEntryRE *regexp.Regexp, maxEntries int) (fields map[string]string, entryIdx int, err error) {
	logcopy.LogBuf.Lock()
	defer logcopy.LogBuf.Unlock()

	if logcopy.LogBuf.TotalEntries < 1 {
		err = fmt.Errorf("parseLogForFunc(): no log entries")
		return
	}

	var logEntry string
	for entryIdx, logEntry = range logcopy.LogBuf.LogEntries {

		if entryIdx >= maxEntries {
			err = fmt.Errorf(
				"parseLogForFunc(): no matching log entry for function '%s' found in %d log entries",
				funcName, maxEntries)
			return
		}
		if logEntry == "" {
			err = fmt.Errorf("parseLogForFunc(): exhausted all log entries without finding a match")
			return
		}

		fields, err = ParseLogEntry(logEntry)
		if err != nil {
			err = fmt.Errorf("parseLogForFunc(): log entry '%s' unparsable by ParseLogEntry(): %v",
				logEntry, err)
			return
		}

		if fields["msg"] == "" {
			err = fmt.Errorf("parseLogForFunc(): log entry does not contain a 'msg=' string: '%s'",
				logEntry)
			return
		}
		if fields["function"] != funcName {
			continue
		}

		matches := logEntryRE.FindStringSubmatch(fields["msg"])
		if matches == nil {
			continue
		}

		// we found a matching log entry; copy the fields and we're done!
		for idx, name := range logEntryRE.SubexpNames() {
			if name != "" {
				fields[name] = matches[idx]
			}
		}
		return
	}

	err = fmt.Errorf("parseLogForFunc(): no matching log entry found: function '%s'", funcName)
	return
}
