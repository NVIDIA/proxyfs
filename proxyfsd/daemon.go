package proxyfsd

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/pkg/profile"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/transitions"

	// Force importing of the following "top-most" packages
	_ "github.com/swiftstack/ProxyFS/fuse"
	_ "github.com/swiftstack/ProxyFS/httpserver"
	_ "github.com/swiftstack/ProxyFS/jrpcfs"
)

// Daemon is launched as a GoRoutine that launches ProxyFS. During startup, the parent should read errChan
// to await Daemon getting to the point where it is ready to handle the specified signal set. Any errors
// encountered before or after this point will be sent to errChan (and be non-nil of course).
func Daemon(confFile string, confStrings []string, errChan chan error, wg *sync.WaitGroup, execArgs []string, signals ...os.Signal) {
	var (
		confMap        conf.ConfMap
		err            error
		signalReceived os.Signal
	)

	// Compute confMap

	confMap, err = conf.MakeConfMapFromFile(confFile)
	if nil != err {
		errChan <- err

		return
	}

	err = confMap.UpdateFromStrings(confStrings)
	if nil != err {
		errChan <- err

		return
	}

	// Start profiling using pkg/profile, if requested. Profiling stops when proxyfsd exits.
	//
	// With the settings used below, output goes to a generated directory in /tmp.
	//  (example: /tmp/profile083387279/cpu.pprof)
	//
	// go tool pprof can then be used to analyze the file.
	//
	profileType, confErr := confMap.FetchOptionValueString("ProxyfsDebug", "ProfileType")
	if confErr == nil {
		switch profileType {
		case "Block":
			defer profile.Start(profile.BlockProfile).Stop()
		case "CPU":
			defer profile.Start(profile.CPUProfile).Stop()
		case "Memory":
			defer profile.Start(profile.MemProfile).Stop()
		case "Mutex":
			defer profile.Start(profile.MutexProfile).Stop()
		}
	}
	// If not specified in conf map or type doesn't match one of the above, don't do profiling.

	// Start up dæmon packages

	err = transitions.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	logger.Infof("proxyfsd is starting up (PID %d); invoked as '%s'",
		os.Getpid(), strings.Join(execArgs, "' '"))
	defer func() {
		logger.Infof("proxyfsd logger is shutting down (PID %d)", os.Getpid())
		err = transitions.Down(confMap)
		if nil != err {
			logger.Errorf("logger.Down() failed: %v", err) // Oddly, if logger.Down() fails, will this work?
		}
		wg.Done()
	}()

	// Arm signal handler used to indicate termination and wait on it
	//
	// Note: signalled chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan := make(chan os.Signal, 8)

	// if signals is empty it means "catch all signals" its possible to catch
	signal.Notify(signalChan, signals...)

	// indicate signal handlers have been armed successfully
	errChan <- nil

	// Await a signal - reloading confFile each SIGHUP - exiting otherwise
	for {
		signalReceived = <-signalChan
		logger.Infof("Received signal: '%v'", signalReceived)

		// these signals are normally ignored, but if "signals..." above is empty
		// they are delivered via the channel.  we should simply ignore them.
		if signalReceived == unix.SIGCHLD || signalReceived == unix.SIGURG ||
			signalReceived == unix.SIGWINCH || signalReceived == unix.SIGCONT {
			logger.Infof("Ignored signal: '%v'", signalReceived)
			continue
		}

		// we can get SIGPIPE whenever an HTTP or other client closes a
		// socket on us, so ignore it
		if signalReceived == unix.SIGPIPE {
			logger.Infof("Ignored signal: '%v'", signalReceived)
			continue
		}

		// SIGHUP means reconfig but any other signal means time to exit
		if unix.SIGHUP != signalReceived {
			logger.Infof("signal catcher is shutting down proxyfsd (PID %d)", os.Getpid())

			if signalReceived != unix.SIGTERM && signalReceived != unix.SIGINT {
				logger.Errorf("proxyfsd received unexpected signal: %v", signalReceived)
				err = fmt.Errorf("proxyfsd received unexpected signal: %v", signalReceived)
			}

			err = transitions.Down(confMap)

			errChan <- err
			return
		}

		// caught SIGHUP -- recompute confMap and re-apply

		confMap, err = conf.MakeConfMapFromFile(confFile)
		if nil != err {
			err = fmt.Errorf("failed to load updated config: %v", err)
			errChan <- err
			return
		}

		err = confMap.UpdateFromStrings(confStrings)
		if nil != err {
			err = fmt.Errorf("failed to reapply config overrides: %v", err)
			errChan <- err
			return
		}

		err = transitions.Signaled(confMap)
		if nil != err {
			err = fmt.Errorf("transitions.Signaled() failed: %v", err)
			errChan <- err
			return
		}
	}
}
