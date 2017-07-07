package proxyfsd

import (
	"os"
	"os/signal"
	"sync"

	"golang.org/x/sys/unix"

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

func Daemon(confMap conf.ConfMap, signalHandlerIsArmed *bool, errChan chan error, wg *sync.WaitGroup) {
	var err error

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

	signal.Notify(signalChan, unix.SIGINT, unix.SIGTERM)

	if nil != signalHandlerIsArmed {
		*signalHandlerIsArmed = true
	}

	receivedSignal := <-signalChan
	logger.Infof("Received signal %v", receivedSignal)

	errChan <- nil
}
