package proxyfsd

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/pkg/profile"
	"github.com/swiftstack/conf"

	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/fuse"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/httpserver"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

func Daemon(confFile string, confStrings []string, signalHandlerIsArmed *bool, errChan chan error, wg *sync.WaitGroup, signals ...os.Signal) {
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
		case "Memory":
			defer profile.Start(profile.MemProfile).Stop()
		case "Block":
			defer profile.Start(profile.BlockProfile).Stop()
		case "CPU":
			defer profile.Start(profile.CPUProfile).Stop()
		}
	}
	// If not specified in conf map or type doesn't match one of the above, don't do profiling.

	// Start up dæmon packages (logger starts first and finishes last)

	err = logger.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	logger.Infof("proxyfsd is starting up (PID %d)", os.Getpid())
	defer func() {
		logger.Infof("proxyfsd logger is shutting down (PID %d)", os.Getpid())
		logger.Down()
		wg.Done()
	}()

	err = stats.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		stats.Down()
		wg.Done()
	}()

	err = swiftclient.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		swiftclient.Down()
		wg.Done()
	}()

	err = headhunter.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		headhunter.Down()
		wg.Done()
	}()

	err = inode.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		inode.Down()
		wg.Done()
	}()

	err = dlm.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		dlm.Down()
		wg.Done()
	}()

	err = fs.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		fs.Down()
		wg.Done()
	}()

	err = fuse.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		fuse.Down()
		wg.Done()
	}()

	err = jrpcfs.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		jrpcfs.Down()
		wg.Done()
	}()

	err = httpserver.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		httpserver.Down()
		wg.Done()
	}()

	// Arm signal handler used to indicate termination and wait on it
	//
	// Note: signalled chan must be buffered to avoid race with window between
	// arming handler and blocking on the chan read

	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, signals...)

	if nil != signalHandlerIsArmed {
		*signalHandlerIsArmed = true
	}

	// Await a signal - reloading confFile each SIGHUP - exiting otherwise

	for {
		signalReceived = <-signalChan
		logger.Infof("Received signal: '%v'", signalReceived)

		if unix.SIGHUP != signalReceived { // signalReceived either SIGINT or SIGTERM... so just exit

			errChan <- nil

			// if the following message doesn't appear in the log,
			// it may be because the caller has not called wg.Wait()
			// to wait for all of the defer'ed routines (above) to
			// finish before it exits
			logger.Infof("signal catcher is shutting down proxyfsd (PID %d)", os.Getpid())
			return
		}

		// caught SIGHUP -- recompute confMap and re-apply
		// break out of loop at first error and handle it
		for {
			logger.Infof("Reconfig started; pausing daemons")

			confMap, err = conf.MakeConfMapFromFile(confFile)
			if nil != err {
				err = fmt.Errorf("failed to load updated config: %v", err)
				break
			}

			err = confMap.UpdateFromStrings(confStrings)
			if nil != err {
				err = fmt.Errorf("failed to reapply config overrides: %v", err)
				break
			}

			// tell each daemon to pause and apply "contracting" confMap changes
			err = httpserver.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = jrpcfs.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = fuse.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = fs.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = dlm.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = inode.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = headhunter.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = swiftclient.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = stats.PauseAndContract(confMap)
			if nil != err {
				break
			}

			err = logger.PauseAndContract(confMap)
			if nil != err {
				break
			}

			// tell each daemon to apply "expanding" confMap changes and result
			logger.Infof("Reconfig starting; resuming daemons")

			err = logger.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = stats.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = swiftclient.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = headhunter.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = inode.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = dlm.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = fs.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = fuse.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = jrpcfs.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			err = httpserver.ExpandAndResume(confMap)
			if nil != err {
				break
			}

			logger.Infof("Reconfig finished successfully")
			break
		}

		// if one of the daemons didn't make it, log the error and shutdown
		// (calls to logger.Fatalf() don't return)
		if nil != err {
			logger.Errorf("Reconfig failed: %v", err)
			errChan <- err
			return
		}
	}
}
