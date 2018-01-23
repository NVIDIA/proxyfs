package proxyfsd

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/pkg/profile"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/fuse"
	"github.com/swiftstack/ProxyFS/halter"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/httpserver"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/statslogger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/utils"
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

	// TODO: Remove call to utils.AdjustConfSectionNamespacingAsNecessary() when appropriate
	err = utils.AdjustConfSectionNamespacingAsNecessary(confMap)
	if nil != err {
		err = fmt.Errorf("utils.AdjustConfSectionNamespacingAsNecessary() failed: %v", err)

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

	// Start up dæmon packages (logging starts first (followed by event logging) and finishes last)

	err = logger.Up(confMap)
	if nil != err {
		errChan <- err
		return
	}
	wg.Add(1)
	logger.Infof("proxyfsd is starting up (PID %d)", os.Getpid())
	defer func() {
		logger.Infof("proxyfsd logger is shutting down (PID %d)", os.Getpid())
		err = logger.Down()
		if nil != err {
			logger.Errorf("logger.Down() failed: %v", err) // Oddly, if logger.Down() fails, will this work?
		}
		wg.Done()
	}()

	err = evtlog.Up(confMap)
	if nil != err {
		logger.Errorf("evtlog.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = evtlog.Down()
		if nil != err {
			logger.Errorf("evtlog.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = halter.Up(confMap)
	if nil != err {
		logger.Errorf("halter.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = halter.Down()
		if nil != err {
			logger.Errorf("halter.Down() failed: %v", err)
		}
		wg.Done()
	}()

	evtlog.Record(evtlog.FormatUpSequenceStart)

	err = stats.Up(confMap)
	if nil != err {
		logger.Errorf("stats.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = stats.Down()
		if nil != err {
			logger.Errorf("stats.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = dlm.Up(confMap)
	if nil != err {
		logger.Errorf("dlm.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = dlm.Down()
		if nil != err {
			logger.Errorf("dlm.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = swiftclient.Up(confMap)
	if nil != err {
		logger.Errorf("swiftclient.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = swiftclient.Down()
		if nil != err {
			logger.Errorf("swiftclient.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = statslogger.Up(confMap)
	if nil != err {
		logger.Errorf("statslogger.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = statslogger.Down()
		if nil != err {
			logger.Errorf("statslogger.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = headhunter.Up(confMap)
	if nil != err {
		logger.Errorf("headhunter.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = headhunter.Down()
		if nil != err {
			logger.Errorf("headhunter.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = inode.Up(confMap)
	if nil != err {
		logger.Errorf("inode.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = inode.Down()
		if nil != err {
			logger.Errorf("inode.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = fs.Up(confMap)
	if nil != err {
		logger.Errorf("fs.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = fs.Down()
		if nil != err {
			logger.Errorf("fs.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = fuse.Up(confMap)
	if nil != err {
		logger.Errorf("fuse.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = fuse.Down()
		if nil != err {
			logger.Errorf("fuse.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = jrpcfs.Up(confMap)
	if nil != err {
		logger.Errorf("jrpcfs.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = jrpcfs.Down()
		if nil != err {
			logger.Errorf("jrpcfs.Down() failed: %v", err)
		}
		wg.Done()
	}()

	err = httpserver.Up(confMap) // Note: Must be the last .Up() step as it is used to indicate "up" status via HTTP
	if nil != err {
		logger.Errorf("httpserver.Up() failed: %v", err)
		errChan <- err
		return
	}
	wg.Add(1)
	defer func() {
		err = httpserver.Down()
		if nil != err {
			logger.Errorf("httpserver.Down() failed: %v", err)
		}
		wg.Done()
	}()

	evtlog.Record(evtlog.FormatUpSequenceEnd)

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
			evtlog.Record(evtlog.FormatDownSequenceEnd)
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

			// TODO: Remove call to utils.AdjustConfSectionNamespacingAsNecessary() when appropriate
			err = utils.AdjustConfSectionNamespacingAsNecessary(confMap)
			if nil != err {
				err = fmt.Errorf("utils.AdjustConfSectionNamespacingAsNecessary() failed: %v", err)
				break
			}

			// tell each daemon to pause and apply "contracting" confMap changes

			evtlog.Record(evtlog.FormatPauseAndContractSequenceStart)

			err = httpserver.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("httpserver.PauseAndContract(): %v", err)
				break
			}

			err = jrpcfs.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("jrpcfs.PauseAndContract(): %v", err)
				break
			}

			err = fuse.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("fuse.PauseAndContract(): %v", err)
				break
			}

			err = fs.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("fs.PauseAndContract(): %v", err)
				break
			}

			err = inode.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("inode.PauseAndContract(): %v", err)
				break
			}

			err = headhunter.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("headhunter.PauseAndContract(): %v", err)
				break
			}

			err = statslogger.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("statslogger.PauseAndContract(): %v", err)
				break
			}

			err = swiftclient.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("swiftclient.PauseAndContract(): %v", err)
				break
			}

			err = dlm.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("dlm.PauseAndContract(): %v", err)
				break
			}

			err = stats.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("stats.PauseAndContract(): %v", err)
				break
			}

			evtlog.Record(evtlog.FormatPauseAndContractSequenceEnd)

			err = evtlog.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("evtlog.PauseAndContract(): %v", err)
				break
			}

			err = halter.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("halter.PauseAndContract(): %v", err)
				break
			}

			err = logger.PauseAndContract(confMap)
			if nil != err {
				err = fmt.Errorf("logger.PauseAndContract(): %v", err)
				break
			}

			// tell each daemon to apply "expanding" confMap changes and result

			logger.Infof("Reconfig starting; resuming daemons")

			err = logger.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("logger.ExpandAndResume(): %v", err)
				break
			}

			err = halter.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("halter.ExpandAndResume(): %v", err)
				break
			}

			err = evtlog.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("evtlog.ExpandAndResume(): %v", err)
				break
			}

			evtlog.Record(evtlog.FormatExpandAndResumeSequenceStart)

			err = stats.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("stats.ExpandAndResume(): %v", err)
				break
			}

			err = dlm.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("dlm.ExpandAndResume(): %v", err)
				break
			}

			err = swiftclient.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("swiftclient.ExpandAndResume(): %v", err)
				break
			}

			err = statslogger.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("statslogger.ExpandAndResume(): %v", err)
				break
			}

			err = headhunter.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("headhunter.ExpandAndResume(): %v", err)
				break
			}

			err = inode.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("inode.ExpandAndResume(): %v", err)
				break
			}

			err = fs.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("fs.ExpandAndResume(): %v", err)
				break
			}

			err = fuse.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("fuse.ExpandAndResume(): %v", err)
				break
			}

			err = jrpcfs.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("jrpcfs.ExpandAndResume(): %v", err)
				break
			}

			err = httpserver.ExpandAndResume(confMap)
			if nil != err {
				err = fmt.Errorf("httpserver.ExpandAndResume(): %v", err)
				break
			}

			logger.Infof("Reconfig finished successfully")

			evtlog.Record(evtlog.FormatExpandAndResumeSequenceEnd)

			break
		}

		// if one of the daemons didn't make it, log the error
		if nil != err {
			logger.Errorf("Reconfig failed: %v", err)
			errChan <- err
			return
		}
	}
}
