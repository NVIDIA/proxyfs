package logger

import (
	"os"

	log "github.com/Sirupsen/logrus"

	"github.com/swiftstack/conf"
)

var logFile *os.File = nil

func Up(confMap conf.ConfMap) (err error) {
	log.SetFormatter(&log.TextFormatter{DisableColors: true})

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

	if logFilePath != "" {
		if logToConsole {
			// use multiWriter to log to both file and console
			output := &multiWriter{}
			output.addWriter(logFile)
			output.addWriter(os.Stderr)
			log.SetOutput(output)
		} else {
			log.SetOutput(logFile)
		}
	}
	// else: accept default destination of stderr

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

func PauseAndContract(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	// Nothing to do here
	err = nil
	return
}

func Down() (err error) {
	// We open and close our own logfile
	if logFile != nil {
		logFile.Close()
	}
	return
}
