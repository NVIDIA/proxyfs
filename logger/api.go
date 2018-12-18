// Package logger provides logging wrappers
//
// These wrappers allow us to standardize logging while still using a third-party
// logging package.
//
// This package is currently implemented on top of the sirupsen/logrus package:
//   https://github.com/sirupsen/logrus
//
// The APIs here add package and calling function to all logs.
//
// Where possible, fields passed to logs are parameterized, to standardize log
// formatting to make log searches based on these fields easier.
//
// Logging of trace and debug logs are enabled/disabled on a per package basis.
package logger

import (
	"fmt"
	"io"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/swiftstack/ProxyFS/utils"
)

type Level int

// Our logging levels - These are the different logging levels supported by this package.
//
// We have more detailed logging levels than the logrus log package.
// As a result, when we do our logging we need to map from our levels
// to the logrus ones before calling logrus APIs.
const (
	// PanicLevel corresponds to logrus.PanicLevel; Logrus will log and then call panic with the log message
	PanicLevel Level = iota
	// FatalLevel corresponds to logrus.FatalLevel; Logrus will log and then calls `os.Exit(1)`.
	// It will exit even if the logging level is set to Panic.
	FatalLevel
	// ErrorLevel corresponds to logrus.ErrorLevel
	ErrorLevel
	// WarnLevel corresponds to logrus.WarnLevel
	WarnLevel
	// InfoLevel corresponds to logrus.InfoLevel; These are general operational entries about what's going on inside the application.
	InfoLevel

	// TraceLevel is used for operational logs that trace success path through the application.
	// Whether these are logged is controlled on a per-package basis by settings in this file.
	// When enabled, these are logged at logrus.InfoLevel.
	TraceLevel

	// DebugLevel is used for very verbose logging, intended to debug internal operations of a
	// particular area. Whether these are logged is controlled on a per-package basis by settings
	// in this file.
	// When enabled, these are logged at logrus.DebugLevel.
	DebugLevel
)

// Stats-related constants
var (
	panicStat = "logging.level.panic"
	fatalStat = "logging.level.fatal"
	errorStat = "logging.level.error"
	warnStat  = "logging.level.warn"
	infoStat  = "logging.level.info"
	traceStat = "logging.level.trace"
	debugStat = "logging.level.debug"
	noStat    = ""
)

// Level to stat string conversion
func (level Level) statString() *string {
	switch level {
	case PanicLevel:
		return &panicStat
	case FatalLevel:
		return &fatalStat
	case ErrorLevel:
		return &errorStat
	case WarnLevel:
		return &warnStat
	case TraceLevel:
		return &traceStat
	case InfoLevel:
		return &infoStat
	case DebugLevel:
		return &debugStat
	}
	return &noStat
}

// Flag to disable all logging, for performance testing.
var disableLoggingForPerfTesting = false

// Enable/disable for trace and debug levels.
// These are defaulted to disabled unless otherwise specified in .conf file
var traceLevelEnabled = false
var debugLevelEnabled = false

// packageTraceSettings controls whether tracing is enabled for particular packages.
// If a package is in this map and is set to "true", then tracing for that package is
// considered to be enabled and trace logs for that package will be emitted. If the
// package is in this list and is set to "false", OR if the package is not in this list,
// trace logs for that package will NOT be emitted.
//
// Note: In order to enable tracing for a package using the "Logging.TraceLevelLogging"
// config variable, the package must be in this map with a value of false (or true).
//
var packageTraceSettings = map[string]bool{
	"dlm":         false,
	"fs":          false,
	"fuse":        false,
	"headhunter":  false,
	"inode":       false,
	"jrpcfs":      false,
	"logger":      false,
	"proxyfsd":    false,
	"sortedmap":   false,
	"swiftclient": false,
}

func setTraceLoggingLevel(confStrSlice []string) {
	if len(confStrSlice) == 0 {
		traceLevelEnabled = false
	}

HandlePkgs:
	for _, pkg := range confStrSlice {
		switch pkg {
		case "none":
			traceLevelEnabled = false
			break HandlePkgs
		default:
			if _, ok := packageTraceSettings[pkg]; ok {
				// Package exists in the map
				packageTraceSettings[pkg] = true

				// If any trace level is enabled, need to enable trace level in general.
				// This flag lets us avoid the performance hit of trace-level API calls
				// if the trace level is disabled.
				traceLevelEnabled = true
			}
		}
	}

	// Log the packages that are enabled, if any
	if traceLevelEnabled {
		for pkg, isEnabled := range packageTraceSettings {
			if isEnabled {
				Infof("Package %v trace logging is enabled.", pkg)
			}
		}
	}
}

func traceEnabled(pkg string) bool {
	// Return whether tracing is enabled for the specified package.
	// If not found in the package trace map, traces are considered to be turned off.
	if isEnabled, ok := packageTraceSettings[pkg]; ok {
		return isEnabled
	} else {
		return false
	}
}

// traceEnabledForPackage returns whether tracing is enabled for the package stored in the context.
// If not found in the package trace map, traces are considered to be turned off.
func (ctx *FuncCtx) traceEnabledForPackage() bool {
	pkg := ctx.getPackage()
	return traceEnabled(pkg)
}

// packageDebugSettings controls which debug logs are enabled for particular packages.
//
// If a package is in this map, then debug logging for that package is
// considered to be enabled for debug tags in the package's map entry.
// If a debug log's tag is NOT in this list, OR if the package is not in the map,
// debug logs for that package will NOT be emitted.
//
// Unlike trace settings, debug settings are stored as a list of enabled debug tags.
// Bear in mind that these tags are evaluated on a package + tag basis, so the same tag
// can be used on different packages without conflict.
const DbgInodeInternal string = "debug_inode_internal"
const DbgInternal string = "debug_internal"
const DbgTesting string = "debug_test"

var packageDebugSettings = map[string][]string{
	"ldlm": []string{
		//DbgInternal,
		//DbgTesting,
	},
	"fs": []string{
		//DbgInternal,
	},
	"jrpcfs": []string{
		//DbgInternal,
		//DbgTesting,
	},
	"inode": []string{
		//DbgInodeInternal,
	},
}

func setDebugLoggingLevel(confStrSlice []string) {
	if len(confStrSlice) == 0 {
		debugLevelEnabled = false
	}

HandlePkgs:
	for _, pkg := range confStrSlice {
		switch pkg {
		case "none":
			debugLevelEnabled = false
			break HandlePkgs
		default:
			if _, ok := packageDebugSettings[pkg]; ok {
				// Package exists in the map

				// XXX TODO: Add .conf support for specifying which debug IDs to enable.
				//           For now, enabling internal and testing by default.
				packageDebugSettings[pkg] = []string{DbgInternal, DbgTesting}

				// If any debug level is enabled, need to enable debug level in general.
				// This flag lets us avoid the performance hit of debug-level API calls
				// if the debug level is disabled.
				debugLevelEnabled = true
			}
		}
	}

	// Log the packages that are enabled, if any
	if debugLevelEnabled {
		for pkg, ids := range packageDebugSettings {
			if len(ids) > 0 {
				Infof("Package %v debug logging is enabled.", pkg)
			}
		}
	}
}

// debugEnabledForPackage returns whether debug logs are enabled for the package stored in the context.
func (ctx *FuncCtx) debugEnabledForPackage(debugID string) bool {
	pkg := ctx.getPackage()

	// Return whether debug is enabled for the package and id.
	// If not found in the package debug map, debug logging is considered to be turned off.
	if idList, ok := packageDebugSettings[pkg]; ok {
		for _, id := range idList {
			if id == debugID {
				return true
			}
		}
	}
	return false
}

// Log fields supported by logger:
const packageKey string = "package"
const functionKey string = "function"
const errorKey string = "error"
const gidKey string = "goroutine"
const pidKey string = "pid"

var ssTransIDKey string = "ss_transid" // transaction ID for sock_swift stuff

// Potential fields we could add to our logger:
// - IO CONTEXT id, use to track through system, multinode eventually.
//    => will need to add this to all JSON RPCs and all the way through system
//       some sort of context struct that is easily passed around?
//    => this could even help now, to match writes to flushes...
const contextKey string = "context"
const nodeidKey string = "nodeid"     // Node ID of node that generated the log
const inodeNumKey string = "inode"    // Inode number, for package inode logic
const filenameKey string = "filename" // Filename, for package fs logic

// This struct is an optimization so that package and function are only
// extracted once per function.
type FuncCtx struct {
	funcContext *log.Entry // Struct allows us to save fields common between log calls within a function
}

// getPackage extracts the package name from the FuncCtx
func (ctx *FuncCtx) getPackage() string {
	pkg, ok := ctx.funcContext.Data[packageKey].(string)
	if ok {
		return pkg
	}
	return ""
}

// getFunc extracts the function name from the FuncCtx
func (ctx *FuncCtx) getFunc() string {
	fn, ok := ctx.funcContext.Data[functionKey].(string)
	if ok {
		return fn
	}
	return ""
}

var nullCtx = FuncCtx{funcContext: nil}

// newFuncCtx creates a new function logging context, extracting the calling
// function from the call stack.
func newFuncCtx(level int) (ctx *FuncCtx) {

	if disableLoggingForPerfTesting {
		return &nullCtx
	}

	// Extract package and function from the call stack
	fn, pkg, gid := utils.GetFuncPackage(level + 1)

	// Get PID as a string (since our PID only changes in fork(2) and
	// clone(2) this could be optimized)
	pid := fmt.Sprint(os.Getpid())

	// Save fields
	fields := make(log.Fields)
	fields[functionKey] = fn
	fields[packageKey] = pkg
	fields[gidKey] = gid
	fields[pidKey] = pid

	ctx = &FuncCtx{funcContext: log.WithFields(fields)}
	return ctx
}

// newFuncCtxWithField creates a new function logging context including a field,
// extracting the calling function from the call stack.
func newFuncCtxWithField(level int, key string, value interface{}) (ctx *FuncCtx) {

	if disableLoggingForPerfTesting {
		return &nullCtx
	}

	// Extract package and function from the call stack
	fn, pkg, gid := utils.GetFuncPackage(level + 1)

	// Save fields
	fields := make(log.Fields)
	fields[key] = value
	fields[functionKey] = fn
	fields[packageKey] = pkg
	fields[gidKey] = gid

	ctx = &FuncCtx{funcContext: log.WithFields(fields)}
	return ctx
}

// newFuncCtxWithFields creates a new function logging context including multiple fields,
// extracting the calling function from the call stack.
func newFuncCtxWithFields(level int, fields log.Fields) (ctx *FuncCtx) {

	if disableLoggingForPerfTesting {
		return &nullCtx
	}

	// Extract package and function from the call stack
	fn, pkg, gid := utils.GetFuncPackage(level + 1)

	// add the function and package fields to the ones passed in
	fields[functionKey] = fn
	fields[packageKey] = pkg
	fields[gidKey] = gid

	ctx = &FuncCtx{funcContext: log.WithFields(fields)}
	return ctx
}

func newLogEntry(level int) *log.Entry {

	if disableLoggingForPerfTesting {
		return nil
	}

	// Extract package and function from the call stack
	fn, pkg, gid := utils.GetFuncPackage(level + 1)

	// XXX TODO: add setting of nodeid here as well. Where would we get this info from?

	// Save fields
	fields := make(log.Fields)
	fields[functionKey] = fn
	fields[packageKey] = pkg
	fields[gidKey] = gid

	return log.WithFields(fields)
}

var backtraceOneLevel int = 1

// EXTERNAL logging APIs
// These APIs are in the style of those provided by the logrus package.

// Logger intentionally does not provide a Debug()  API; use DebugID()  instead.
// Logger intentionally does not provide a Debugf() API; use DebugfID() instead.
// Logger intentionally does not provide a DebugWithError() API; use DebugfWithError() instead.
// Logger intentionally does not provide a DebugfWithError() API; use DebugfIDWithError() instead.

func logEnabled(level Level) bool {
	if disableLoggingForPerfTesting {
		return false
	}
	if (level == TraceLevel) && !traceLevelEnabled {
		return false
	}
	if (level == DebugLevel) && !debugLevelEnabled {
		return false
	}
	return true
}

func DebugID(id string, args ...interface{}) {
	level := DebugLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprint(args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.logWithID(level, id, logString)
}

func Error(args ...interface{}) {
	level := ErrorLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprint(args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.log(level, logString)
}

func Info(args ...interface{}) {
	level := InfoLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprint(args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.log(level, logString)
}

func DebugfID(id string, format string, args ...interface{}) {
	level := DebugLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.logWithID(level, id, logString)
}

func DebugfIDWithTxnID(id string, transIDStr string, format string, args ...interface{}) {
	level := DebugLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, ssTransIDKey, transIDStr)
	ctx.logWithID(level, id, logString)
}

func Errorf(format string, args ...interface{}) {
	level := ErrorLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.log(level, logString)
}

func Fatalf(format string, args ...interface{}) {
	level := FatalLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.log(level, logString)
}

func Infof(format string, args ...interface{}) {
	level := InfoLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.log(level, logString)
}

func Tracef(format string, args ...interface{}) {
	level := TraceLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.log(level, logString)
}

func Warnf(format string, args ...interface{}) {
	level := WarnLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtx(backtraceOneLevel)
	ctx.log(level, logString)
}

func ErrorWithError(err error, args ...interface{}) {
	level := ErrorLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprint(args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func FatalWithError(err error, args ...interface{}) {
	level := FatalLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprint(args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func InfoWithError(err error, args ...interface{}) {
	level := InfoLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprint(args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func TraceWithError(err error, args ...interface{}) {
	level := TraceLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprint(args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func WarnWithError(err error, args ...interface{}) {
	level := WarnLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprint(args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func DebugfIDWithError(id string, err error, format string, args ...interface{}) {
	level := DebugLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.logWithID(level, id, logString)
}

func ErrorfWithError(err error, format string, args ...interface{}) {
	level := ErrorLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func ErrorfWithErrorTxnID(err error, transIDStr string, format string, args ...interface{}) {
	level := ErrorLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithFields(backtraceOneLevel, log.Fields{errorKey: err, ssTransIDKey: transIDStr})
	ctx.log(level, logString)
}

func FatalfWithError(err error, format string, args ...interface{}) {
	level := FatalLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func InfofWithError(err error, format string, args ...interface{}) {
	level := InfoLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func InfofWithErrorTxnID(err error, transIDStr string, format string, args ...interface{}) {
	level := InfoLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithFields(backtraceOneLevel, log.Fields{errorKey: err, ssTransIDKey: transIDStr})
	ctx.log(level, logString)
}

func PanicfWithError(err error, format string, args ...interface{}) {
	level := PanicLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func TracefWithError(err error, format string, args ...interface{}) {
	level := TraceLevel
	if !logEnabled(level) {
		return
	}
	if disableLoggingForPerfTesting || !traceLevelEnabled {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func WarnfWithError(err error, format string, args ...interface{}) {
	level := WarnLevel
	if !logEnabled(level) {
		return
	}
	logString := fmt.Sprintf(format, args...)
	ctx := newFuncCtxWithField(backtraceOneLevel, errorKey, err)
	ctx.log(level, logString)
}

func TraceEnter(argsPrefix string, args ...interface{}) (ctx FuncCtx) {
	level := TraceLevel
	if !logEnabled(level) {
		return
	}

	// We are the first to use the function context, so fill it in.
	ctx.funcContext = newLogEntry(backtraceOneLevel)

	// Do logging
	ctx.traceEnterInternal(argsPrefix, args...)

	return ctx
}

// TraceExit generates a function exit trace with the provided parameters, and using
// the package and function set in FuncCtx (if set).
//
// The implementation of this function assumes that it will be called deferred.
// This assumption only matters in the case where this API is called without having
// called TraceEnter* first, since that is the only case where this function will
// attempt to determine the calling function.
func (ctx *FuncCtx) TraceExit(argsPrefix string, args ...interface{}) {
	level := TraceLevel
	if !logEnabled(level) {
		return
	}

	// This should really be set already, assuming that one would call TraceEnter before TraceExit.
	// But I don't really want to trigger a panic for just a log call, so we'll check just in case.
	if ctx.funcContext == nil {
		// We are the first to use the function context, so fill it in.
		ctx.funcContext = newLogEntry(2)
	}

	// Do logging
	ctx.traceExitInternal(argsPrefix, args...)
}

func (ctx *FuncCtx) TraceExitErr(argsPrefix string, err error, args ...interface{}) {
	level := TraceLevel
	if !logEnabled(level) {
		return
	}

	// This should really be set already, assuming that one would call TraceEnter before TraceExit.
	// But I don't really want to trigger a panic for just a log call, so we'll check just in case.
	if ctx.funcContext == nil {
		// We are the first to use the function context, so fill it in.
		ctx.funcContext = newLogEntry(2)

	}

	// Add error to the fields
	// We throw away the temporary context with the error, in case the
	// context we got called with gets reused.
	newCtx := FuncCtx{funcContext: ctx.funcContext.WithField(errorKey, err)}

	// Do logging
	newCtx.traceExitInternal(argsPrefix, args...)
}

func (ctx *FuncCtx) traceInternal(formatPrefix string, argsPrefix string, args ...interface{}) {

	numParams := len(args)

	// Build format string
	format := formatPrefix

	// prepend argsPrefix to the args that were passed in
	newArgs := append([]interface{}{argsPrefix}, args...)

	// XXX TODO: make more generic, perhaps a for loop to add "%v" based on numParams
	switch numParams {
	case 0:
		format += " %s"
	case 1:
		format += " %s %+v"
	case 2:
		format += " %s %+v %+v"
	case 3:
		format += " %s %+v %+v %+v"
	case 4:
		format += " %s %+v %+v %+v %+v"
	case 5:
		format += " %s %+v %+v %+v %+v %+v"
	case 6:
		format += " %s %+v %+v %+v %+v %+v %+v"
	case 7:
		format += " %s %+v %+v %+v %+v %+v %+v %+v"
	case 8:
		format += " %s %+v %+v %+v %+v %+v %+v %+v %+v"
	case 9:
		format += " %s %+v %+v %+v %+v %+v %+v %+v %+v %+v"
	case 10:
		format += " %s %+v %+v %+v %+v %+v %+v %+v %+v %+v %+v"
	default:
		format += " %s %+v %+v %+v %+v %+v %+v %+v %+v %+v %+v"
	}
	ctx.log(TraceLevel, fmt.Sprintf(format, newArgs...))
}

func (ctx *FuncCtx) traceEnterInternal(argsPrefix string, args ...interface{}) {

	ctx.traceInternal(">> called", argsPrefix, args...)
}

func (ctx *FuncCtx) traceExitInternal(argsPrefix string, args ...interface{}) {

	ctx.traceInternal("<< returning", argsPrefix, args...)
}

// log is our equivalent to logrus.entry.go's log function, and is intended to
// be the common low-level logging function used internal to this package.
//
// Following the example of logrus.entry.go's equivalent function, "this function
// is not declared with a pointer value because otherwise race conditions will
// occur when using multiple goroutines"
//
// XXX TODO: It could be convenient to export a non-level-specific interface
//           like this, though logrus specifically does not.
//           Until we understand better why that is, let's leave this as an
//           internal function.
//
func (ctx FuncCtx) log(level Level, args ...interface{}) {

	// Note that certain levels will always be on, like panic/fatal/error/warn.

	// Return if trace level not enabled for this package
	if (level == TraceLevel) && !ctx.traceEnabledForPackage() {
		return
	}
	// NOTE: Debug level checking is done in logWithID; all debug logging should
	//       come through that API and not directly to this one.

	switch level {
	case PanicLevel:
		ctx.funcContext.Panic(args...)
	case FatalLevel:
		ctx.funcContext.Fatal(args...)
	case ErrorLevel:
		ctx.funcContext.Error(args...)
	case WarnLevel:
		ctx.funcContext.Warn(args...)
	case TraceLevel:
		ctx.funcContext.Info(args...)
	case InfoLevel:
		ctx.funcContext.Info(args...)
	case DebugLevel:
		ctx.funcContext.Debug(args...)
	}
}

func (ctx FuncCtx) logWithID(level Level, id string, args ...interface{}) {

	if (level == DebugLevel) && !ctx.debugEnabledForPackage(id) {
		//log.Infof("logWithID: NOT logging debug log with id=%v.", id)
		return
	}

	// Otherwise just call the log API
	ctx.log(level, args...)
}

// Add another target for log messages to be written to.  writer is an object
// with an io.Writer interface that's called once for each log message.
//
// Logger.Up() must be called before this function is used.
//
func AddLogTarget(writer io.Writer) {
	addLogTarget(writer)
}

// An example of a log target that captures the most recent n lines of log into
// an array.  Useful for writing test cases.
//
// There should really be a lock or clever RCU mechanism to coordinate
// access/updates to the array, but its not really necessary for test case code
// (and adds overhead).
//
type LogBuffer struct {
	LogEntries   []string // most recent log entry is [0]
	TotalEntries int      // count of all entries see
}

type LogTarget struct {
	LogBuf *LogBuffer
}

// Initialize a LogTarget to hold upto nEntry log entries.
//
func (log *LogTarget) Init(nEntry int) {
	log.LogBuf = &LogBuffer{TotalEntries: 0}
	log.LogBuf.LogEntries = make([]string, nEntry)
}

// Called by logger for each log entry
//
func (log LogTarget) Write(p []byte) (n int, err error) {
	return log.write(p)
}
