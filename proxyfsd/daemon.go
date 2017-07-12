package proxyfsd

import (
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

func Daemon(confFile string, confStrings []string, signalHandlerIsArmed *bool, errChan chan error, wg *sync.WaitGroup) {
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

	// Start up dæmon packages

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

	err = logger.Up(confMap)
	if nil != err {
		errChan <- err

		return
	}
	wg.Add(1)
	defer func() {
		logger.Down()
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

	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM, unix.SIGHUP)

	if nil != signalHandlerIsArmed {
		*signalHandlerIsArmed = true
	}

	signalReceived = <-signalChan
	// TODO: Handle SIGHUP
	logger.Infof("Received signal %v", signalReceived)

	errChan <- nil
}
