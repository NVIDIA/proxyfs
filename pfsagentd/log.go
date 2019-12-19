package main

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func logFatal(err error) {
	logf("FATAL", "%v", err)
	os.Exit(1)
}

func logFatalf(format string, args ...interface{}) {
	logf("FATAL", format, args...)
	os.Exit(1)
}

func logErrorf(format string, args ...interface{}) {
	logf("ERROR", format, args...)
}

func logWarnf(format string, args ...interface{}) {
	logf("WARN", format, args...)
}

func logInfof(format string, args ...interface{}) {
	logf("INFO", format, args...)
}

func logTracef(format string, args ...interface{}) {
	if globals.config.TraceEnabled {
		logf("TRACE", format, args...)
	}
}

func logf(level string, format string, args ...interface{}) {
	var (
		enhancedArgs   []interface{}
		enhancedFormat string
		err            error
		logMsg         string
	)

	enhancedFormat = "[%s][%s][%d][%s] " + format
	enhancedArgs = append([]interface{}{time.Now().Format(time.RFC3339Nano), globals.config.FUSEVolumeName, unix.Getpid(), level}, args...)

	logMsg = fmt.Sprintf(enhancedFormat, enhancedArgs[:]...)

	if nil == globals.logFile {
		if "" != globals.config.LogFilePath {
			globals.logFile, err = os.OpenFile(globals.config.LogFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
			if nil == err {
				globals.logFile.WriteString(logMsg + "\n")
			} else {
				globals.logFile = nil
			}
		}
	} else {
		globals.logFile.WriteString(logMsg + "\n")
	}
	if globals.config.LogToConsole {
		fmt.Fprintln(os.Stderr, logMsg)
	}
}
