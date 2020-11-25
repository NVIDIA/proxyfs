package proxyfsd

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/transitions"
	"github.com/swiftstack/ProxyFS/version"

	// Force importing of the following "top-most" package
	_ "github.com/swiftstack/ProxyFS/httpserver"
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

	// Optionally launch an embedded HTTP Server for Golang runtime access;
	// this should be done before transitions.Up() is called so it is
	// available if transitions.Up() hangs (the embedded http server will
	// return http.StatusServiceUnavailable (503) during the transition.
	debugServerPortAsUint16, err := confMap.FetchOptionValueUint16("ProxyfsDebug", "DebugServerPort")
	if nil != err {
		debugServerPortAsUint16 = 6058 // TODO: Eventually set it to zero
	}
	if uint16(0) != debugServerPortAsUint16 {
		debugServerPortAsString := fmt.Sprintf("%d", debugServerPortAsUint16)
		logger.Infof("proxyfsd.Daemon() starting debug HTTP Server on localhost:%s", debugServerPortAsString)
		go http.ListenAndServe("localhost:"+debugServerPortAsString, nil)
	}

	// Arm signal handler used to catch signals
	//
	// Note: signalChan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read when signals might
	// otherwise be lost.  No signals will be processed until
	// transitions.Up() finishes, but an incoming SIGHUP will not cause the
	// process to immediately exit.
	signalChan := make(chan os.Signal, 16)

	// if signals is empty it means "catch all signals" it is possible to catch
	signal.Notify(signalChan, signals...)

	// Start up dæmon packages

	err = transitions.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	logger.Infof("proxyfsd is starting up (version %s) (PID %d); invoked as '%s'",
		version.ProxyFSVersion, os.Getpid(), strings.Join(execArgs, "' '"))
	defer func() {
		logger.Infof("proxyfsd logger is shutting down (PID %d)", os.Getpid())
		err = transitions.Down(confMap)
		if nil != err {
			logger.Errorf("transitions.Down() failed: %v", err) // Oddly, if logger.Down() fails, will this work?
		}
		errChan <- err
		wg.Done()
	}()

	// indicate transitions finished and signal handlers have been armed successfully
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
			}

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
