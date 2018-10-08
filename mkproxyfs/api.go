package mkproxyfs

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/transitions"
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
		whoAmI            string
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

	// Upgrade confMap if necessary
	// [should be removed once backwards compatibility is no longer required...]

	err = transitions.UpgradeConfMapIfNeeded(confMap)
	if nil != err {
		err = fmt.Errorf("failed to upgrade config: %v", err)
		return
	}

	// Update confMap to specify an empty FSGlobals.VolumeGroupList

	err = confMap.UpdateFromString("FSGlobals.VolumeGroupList=")
	if nil != err {
		err = fmt.Errorf("failed to empty config VolumeGroupList: %v", err)
		return
	}

	// Fetch confMap particulars needed below

	accountName, err = confMap.FetchOptionValueString("Volume:"+volumeNameToFormat, "AccountName")
	if nil != err {
		return
	}

	// Call transitions.Up() with empty FSGlobals.VolumeGroupList

	err = transitions.Up(confMap)
	if nil != err {
		return
	}

	logger.Infof("mkproxyfs is starting up (PID %d); invoked as '%s'",
		os.Getpid(), strings.Join(execArgs, "' '"))

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
			_ = transitions.Down(confMap)
			err = fmt.Errorf("failed to GET %v: %v", accountName, err)
			return
		}
	}

	if !isEmpty {
		switch mode {
		case ModeNew:
			// If Swift Account is not empty && ModeNew, exit with failure

			_ = transitions.Down(confMap)
			err = fmt.Errorf("%v found to be non-empty with mode == ModeNew (%v)", accountName, ModeNew)
			return
		case ModeOnlyIfNeeded:
			// If Swift Account is not empty && ModeOnlyIfNeeded, exit successfully

			_ = transitions.Down(confMap)
			err = nil
			return
		case ModeReformat:
			// If Swift Account is not empty && ModeReformat, clear out accountName

			for !isEmpty {
				for _, containerName = range containerList {
					_, objectList, err = swiftclient.ContainerGet(accountName, containerName)
					if nil != err {
						_ = transitions.Down(confMap)
						err = fmt.Errorf("failed to GET %v/%v: %v", accountName, containerName, err)
						return
					}

					isEmpty = (0 == len(objectList))

					for !isEmpty {
						for _, objectName = range objectList {
							err = swiftclient.ObjectDelete(accountName, containerName, objectName, 0)
							if nil != err {
								_ = transitions.Down(confMap)
								err = fmt.Errorf("failed to DELETE %v/%v/%v: %v", accountName, containerName, objectName, err)
								return
							}
						}

						_, objectList, err = swiftclient.ContainerGet(accountName, containerName)
						if nil != err {
							_ = transitions.Down(confMap)
							err = fmt.Errorf("failed to GET %v/%v: %v", accountName, containerName, err)
							return
						}

						isEmpty = (0 == len(objectList))
					}

					err = swiftclient.ContainerDelete(accountName, containerName)
					if nil != err {
						_ = transitions.Down(confMap)
						err = fmt.Errorf("failed to DELETE %v/%v: %v", accountName, containerName, err)
						return
					}
				}

				_, containerList, err = swiftclient.AccountGet(accountName)
				if nil != err {
					_ = transitions.Down(confMap)
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
							_ = transitions.Down(confMap)
							err = fmt.Errorf("os.Remove(replayLogFileName == \"%v\") returned unexpected error: %v", replayLogFileName, removeReplayLogFileErr)
							return
						}
					}
				}
			}
		}
	}

	// Call transitions.Down() before restarting to do format

	err = transitions.Down(confMap)
	if nil != err {
		return
	}

	// Update confMap to specify only volumeNameToFormat with AuthFormat set to true

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	err = confMap.UpdateFromStrings([]string{
		"FSGlobals.VolumeGroupList=MKPROXYFS",
		"VolumeGroup:MKPROXYFS.VolumeList=" + volumeNameToFormat,
		"VolumeGroup:MKPROXYFS.VirtualIPAddr=",
		"VolumeGroup:MKPROXYFS.PrimaryPeer=" + whoAmI,
		"Volume:" + volumeNameToFormat + ".AutoFormat=true"})
	if nil != err {
		err = fmt.Errorf("failed to retarget config at only %s: %v", volumeNameToFormat, err)
		return
	}

	// Restart... this time AutoFormat of volumeNameToFormat will be applied

	err = transitions.Up(confMap)
	if nil != err {
		return
	}

	// Make reference to package headhunter to ensure it gets registered with package transitions

	_, err = headhunter.FetchVolumeHandle(volumeNameToFormat)
	if nil != err {
		return
	}

	// With format complete, we can shutdown for good... return code set appropriately by Down()

	err = transitions.Down(confMap)

	return
}
