package mkproxyfs

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/dlm"
	"github.com/swiftstack/ProxyFS/evtlog"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
	"github.com/swiftstack/ProxyFS/swiftclient"
)

type Mode int

const (
	ModeNew Mode = iota
	ModeOnlyIfNeeded
	ModeReformat
)

func Format(mode Mode, volumeNameToFormat string, confFile string, confStrings []string, execArgs []string) (err error) {
	var (
		accountName       string
		confMap           conf.ConfMap
		containerList     []string
		containerName     string
		isEmpty           bool
		objectList        []string
		objectName        string
		replayLogFileName string
	)

	// Valid mode?

	switch mode {
	case ModeNew:
	case ModeOnlyIfNeeded:
	case ModeReformat:
	default:
		err = fmt.Errorf("mode (%v) must be one of ModeNew (%v), ModeOnlyIfNeeded (%v), or ModeReformat (%v)", mode, ModeNew, ModeOnlyIfNeeded, ModeReformat)
		return
	}

	// Load confFile & confStrings (overrides)

	confMap, err = conf.MakeConfMapFromFile(confFile)
	if nil != err {
		err = fmt.Errorf("failed to load config: %v", err)
		return
	}

	err = confMap.UpdateFromStrings(confStrings)
	if nil != err {
		err = fmt.Errorf("failed to apply config overrides: %v", err)
		return
	}

	// Fetch confMap particulars needed below

	accountName, err = confMap.FetchOptionValueString("Volume:"+volumeNameToFormat, "AccountName")
	if nil != err {
		return
	}

	// Call Up() for required packages (deferring their Down() calls until function return)

	err = logger.Up(confMap)
	if nil != err {
		return
	}
	defer func() {
		_ = logger.Down()
	}()
	logger.Infof("mkproxyfs is starting up (PID %d); invoked as '%s'",
		os.Getpid(), strings.Join(execArgs, "' '"))

	err = evtlog.Up(confMap)
	if nil != err {
		return
	}
	defer func() {
		_ = evtlog.Down()
	}()

	err = stats.Up(confMap)
	if nil != err {
		return
	}
	defer func() {
		_ = stats.Down()
	}()

	err = dlm.Up(confMap)
	if nil != err {
		return
	}
	defer func() {
		_ = dlm.Down()
	}()

	err = swiftclient.Up(confMap)
	if nil != err {
		return
	}
	defer func() {
		_ = swiftclient.Down()
	}()

	// Determine if underlying accountName is empty

	_, containerList, err = swiftclient.AccountGet(accountName)
	if nil == err {
		// accountName exists (possibly auto-created)... consider it empty only if no containers therein
		isEmpty = (0 == len(containerList))
	} else {
		if http.StatusNotFound == blunder.HTTPCode(err) {
			// accountName does not exist, so accountName is empty
			isEmpty = true
		} else {
			err = fmt.Errorf("failed to GET %v: %v", accountName, err)
			return
		}
	}

	if !isEmpty {
		switch mode {
		case ModeNew:
			// If Swift Account is not empty && ModeNew, exit with failure

			err = fmt.Errorf("%v found to be non-empty with mode == ModeNew (%v)", accountName, ModeNew)
			return
		case ModeOnlyIfNeeded:
			// If Swift Account is not empty && ModeOnlyIfNeeded, exit successfully

			err = nil
			return
		case ModeReformat:
			// If Swift Account is not empty && ModeReformat, clear out accountName

			for !isEmpty {
				for _, containerName = range containerList {
					_, objectList, err = swiftclient.ContainerGet(accountName, containerName)
					if nil != err {
						err = fmt.Errorf("failed to GET %v/%v: %v", accountName, containerName, err)
						return
					}

					isEmpty = (0 == len(objectList))

					for !isEmpty {
						for _, objectName = range objectList {
							err = swiftclient.ObjectDelete(accountName, containerName, objectName, 0)
							if nil != err {
								err = fmt.Errorf("failed to DELETE %v/%v/%v: %v", accountName, containerName, objectName, err)
								return
							}
						}

						_, objectList, err = swiftclient.ContainerGet(accountName, containerName)
						if nil != err {
							err = fmt.Errorf("failed to GET %v/%v: %v", accountName, containerName, err)
							return
						}

						isEmpty = (0 == len(objectList))
					}

					err = swiftclient.ContainerDelete(accountName, containerName)
					if nil != err {
						err = fmt.Errorf("failed to DELETE %v/%v: %v", accountName, containerName, err)
						return
					}
				}

				_, containerList, err = swiftclient.AccountGet(accountName)
				if nil != err {
					err = fmt.Errorf("failed to GET %v: %v", accountName, err)
					return
				}

				isEmpty = (0 == len(containerList))
			}

			replayLogFileName, err = confMap.FetchOptionValueString("Volume:"+volumeNameToFormat, "ReplayLogFileName")
			if nil == err {
				if "" != replayLogFileName {
					removeReplayLogFileErr := os.Remove(replayLogFileName)
					if nil != removeReplayLogFileErr {
						if !os.IsNotExist(removeReplayLogFileErr) {
							err = fmt.Errorf("os.Remove(replayLogFileName == \"%v\") returned unexpected error: %v", replayLogFileName, removeReplayLogFileErr)
							return
						}
					}
				}
			}
		}
	}

	// Format Swift Account (who's error return will suffice for this function's error return)

	err = headhunter.Format(confMap, volumeNameToFormat)

	return
}
