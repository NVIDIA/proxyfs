package logger

import (
	"io"
	"os"
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
	for i := len(log.LogBuf.LogEntries) - 1; i > 0; i-- {
		log.LogBuf.LogEntries[i] = log.LogBuf.LogEntries[i-1]
	}
	log.LogBuf.LogEntries[0] = strings.TrimRight(string(p), " \t\n")

	log.LogBuf.TotalEntries++
	return 0, nil
}

// Call to configure the logger.  This really should be done before using it,
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
	log.SetFormatter(&log.TextFormatter{DisableColors: true, TimestampFormat: timeFormat})

	// Fetch log file info, if provided
	logFilePath, _ := confMap.FetchOptionValueString("Logging", "LogFilePath")
	if logFilePath != "" {
		logFile, err = os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Errorf("couldn't open log file: %v", err)
			return err
		}
	}

	// Determine whether we should log to console. Default is false.
	logToConsole, err := confMap.FetchOptionValueBool("Logging", "LogToConsole")
	if err != nil {
		logToConsole = false
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

	return nil
}

func Down(confMap conf.ConfMap) (err error) {
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

func Signaled(confMap conf.ConfMap) (err error) {
	err = Down(confMap)
	if nil == err {
		err = Up(confMap)
	}
	return
}
