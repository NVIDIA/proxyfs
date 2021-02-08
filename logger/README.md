# logger
Logging package for ProxyFS


## About

This package is implemented on top of the [logrus](https://github.com/sirupsen/logrus) package to provide structured logging functionality for ProxyFS. It uses logrus's log fields functionality to generate logs that are easily parsed by log tools.

The purpose of this package is to provide a front-end to the Logrus package to provide standard field-based logging for ProxyFS.
	


## Features
- The package and function name of the caller are automatically added to the log's fields. See the [Log fields section](#log-fields) for more detailed information about how log fields are used internal to this package.
- Logging of commonly-used fields is standardized to make log searches easier.
- Provides per-calling-package control (in one place) of whether Trace and Debug logs are emitted.
- Logger is API-compatible with many **logrus** APIs.




## Logging Levels

Logger has seven logging levels, one more (Trace level) than Logrus.

Each Logger level maps to a particular Logrus logging level. Trace logs are sent to Logrus at the Info level.

The intent is to have the ability to have more logging levels than Logrus, with the verbosity controlled by the Logger package. While Logrus has log level controls, we do not use them. Instead we have our own logging controls.

| Level     | Description |
| -----     | ----------- |
| Debug     | Logs intended to debug internal package behaviour. These are not normally enabled by default.  |
| **Trace** | Logs intended to trace the success path through a package. These may or may not be enabled in production code. |
| Info      | Logs intended record something that might be useful. |
| Warning   | Logs intended to call attention to something. |
| Error     | Logs intended to capture an error. This level should only be used for things that the top-level caller would consider an error. |
| Fatal     | Log a fatal condition. This will cause the calling process to exit, without a clean shutdown. Should be used very sparingly. |
| Panic     | Log a panic condition. This will cause the calling process to panic. Note that golang panics do not generate a core, so put as much information as possible into the log text. |


## Logging verbosity control

Logs at Info, Warning, Error, Fatal and Panic levels are always on.

Logs at Trace and Debug levels have per-package controls.

The emission of trace logs can be set to true or false on a per-calling-package basis. If a trace setting for the logged package cannot be found, trace logging for that package is considered to be **turned off**.

**NOTE:**

**TODO: Add support for setting trace and debug logging configuration via .conf files**


### Trace logs

The `packageTraceSettings` map controls whether tracing is enabled for particular packages. If a package is in this map and is set to "true", then tracing for that package is considered to be enabled and trace logs for that package will be emitted. If the package is in this list and is set to "false", OR if the package is not in this list, trace logs for that package will NOT be emitted.


### Debug logs

The `packageDebugSettings` map controls which debug logs are enabled for particular packages. Unlike trace settings, debug settings are stored as a list of enabled debug tags.

The intent of using a map of tags like this is to be able to turn on/off debug logs for subsets of functionality within a package so that one doesn't have to turn on traces that aren't needed.

Bear in mind that these tags are evaluated on a package + tag basis, so the same tag can be used on different packages without conflict.



## Example

The simplest way to use Logger is as a replacement for the stdlib logger:

```
package main

import (
	"github.com/NVIDIA/proxyfs/logger"
)

func main() {
  receivedSignal := <-signalChan
  logger.Infof("Received signal %v", receivedSignal)
}
```

## Caveats
- **Logger APIs should not be called if one has not started up the logger package by calling logger.Up().**




## APIs


| API | Description |
| --- | ----------- |
| `Error`,`Info` | Same as the Logrus version of these APIs, except function and package fields are added |
| `Errorf`, `Fatalf`, `Infof`, `Warnf` | Same as the Logrus version of these APIs, except function and package fields are added |
|`Tracef` | Behaves like `*f` APIs, at Trace level. Function and package fields are added.|
| `ErrorWithError`, `FatalWithError`, `InfoWithError`, `WarnWithError` |  Adds support to error field. Otherwise functionally equivalent to the non-`WithError` versions of these APIs  |
| `TraceWithError` | Behaves like `*WithError` APIs, at Trace level.  |
| `ErrorfWithError`, `FatalfWithError`, `InfofWithError`, `PanicfWithError`, `WarnfWithError` | A combination of the `*f`  and `*WithError` APIs |
|`TracefWithError` | `*fWithError` API at Trace level. |
|`ErrorfWithErrorTxnID`, `InfofWithErrorTxnID` | Adds support for a transaction ID field to `*fWithError` APIs. |
|`DebugID` | Adds support for a debug ID field to the `Debug` API. Debug IDs are how we control whether debug logs are emitted. |
|`DebugfID` | Adds support for a debug ID field to the `Debugf` API. Debug IDs are how we control whether debug logs are emitted. |
|`DebugfIDWithTxnID` | Adds support for a transaction ID field to `DebugfID` API. |
|`DebugfIDWithError` | Adds support for the error field to `DebugfID` API.|
|`TraceEnter`, `TraceEnterDeferred` | APIs for tracing function entry. |
|`TraceExit`, `TraceExitErr` | APIs for tracing function exit. |


** The following debug-related APIs are intentionally not supported **

The following plain Debug* APIs are not supported, since all Debug logging must be done using a debug ID.


| Not supported API | Suggested Alternative API |
| ----------------- | ------------------------- |
| `Debug`           | `DebugID` |
| `Debugf`          | `DebugfID` |
| `DebugWithError`  | `DebugIDWithError` |
| `DebugfWithError` | `DebugfIDWithError` |


# Internal design notes

## [Log Fields](log-fields)

As described in 
[logrus fields documentation](https://github.com/sirupsen/logrus/blob/master/README.md#fields), the use of log fields allows careful, structured logging instead of long, hard-to-parse error messages. 

For example, instead of:

	log.Infof("%s: (txn = %v) error = %v", utils.GetFnName(), scc.hc.transID, err)

	(with the corresponding log)

	time="2016-08-16T22:41:49Z" level=info msg="inode.Close: (txn = 45362) error = hc.setupPutChunkedBegin error"

we now do

	logger.InfofWithErrorTxnID(err, scc.hc.transIDStr(), "error")

	(with the corresponding log)
	
	time="2016-08-16T22:41:49Z" level=info msg="error" error="hc.setupPutChunkedBegin error" function=Close package=inode ss_transid=45362



The use of log fields make it much easier to parse log files, either manually or with log parsing tools.


The log fields that are currently supported by this package are:

* **package**: the package of the calling function
* **function**: the calling function
* **error**
* **ss_transid**: swift transaction ID for inode/sock_swift logic


## Function context
`FuncCtx` is a context structure that is used by some of the Logger APIs. This struct is an optimization so that package and function are only extracted once per function.

The package, function and other fields are stored in the `FuncCtx` in a `logrus.Entry*`.

This construct is mainly used inside the logger package, but is used by `TraceExit`/`TraceExitErr` as well.
