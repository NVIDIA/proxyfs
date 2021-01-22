// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package mkproxyfs

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	etcd "go.etcd.io/etcd/clientv3"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/etcdclient"
	"github.com/swiftstack/ProxyFS/headhunter"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/swiftclient"
	"github.com/swiftstack/ProxyFS/transitions"
	"github.com/swiftstack/ProxyFS/version"
)

type Mode int

const (
	ModeNew Mode = iota
	ModeOnlyIfNeeded
	ModeReformat
)

func Format(mode Mode, volumeNameToFormat string, confFile string, confStrings []string, execArgs []string) (err error) {
	var (
		accountName           string
		cancel                context.CancelFunc
		checkpointEtcdKeyName string
		confMap               conf.ConfMap
		containerList         []string
		containerName         string
		ctx                   context.Context
		etcdAutoSyncInterval  time.Duration
		etcdClient            *etcd.Client
		etcdDialTimeout       time.Duration
		etcdEnabled           bool
		etcdEndpoints         []string
		etcdKV                etcd.KV
		etcdOpTimeout         time.Duration
		getResponse           *etcd.GetResponse
		isEmpty               bool
		objectList            []string
		objectName            string
		replayLogFileName     string
		whoAmI                string
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

	etcdEnabled, err = confMap.FetchOptionValueBool("FSGlobals", "EtcdEnabled")
	if nil != err {
		etcdEnabled = false // Current default
	}

	if etcdEnabled {
		etcdEndpoints, err = confMap.FetchOptionValueStringSlice("FSGlobals", "EtcdEndpoints")
		if nil != err {
			return
		}
		etcdAutoSyncInterval, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdAutoSyncInterval")
		if nil != err {
			return
		}
		etcdDialTimeout, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdDialTimeout")
		if nil != err {
			return
		}
		etcdOpTimeout, err = confMap.FetchOptionValueDuration("FSGlobals", "EtcdOpTimeout")
		if nil != err {
			return
		}

		checkpointEtcdKeyName, err = confMap.FetchOptionValueString("Volume:"+volumeNameToFormat, "CheckpointEtcdKeyName")
		if nil != err {
			return
		}

		// Initialize etcd Client & KV objects
		etcdClient, err = etcdclient.New(etcdEndpoints,
			etcdAutoSyncInterval, etcdDialTimeout)
		if nil != err {
			return
		}

		etcdKV = etcd.NewKV(etcdClient)
	}

	// Call transitions.Up() with empty FSGlobals.VolumeGroupList

	err = transitions.Up(confMap)
	if nil != err {
		return
	}

	logger.Infof("mkproxyfs is starting up (version %s) (PID %d); invoked as '%s'",
		version.ProxyFSVersion, os.Getpid(), strings.Join(execArgs, "' '"))

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

	// Adjust isEmpty based on etcd

	if isEmpty && etcdEnabled {
		ctx, cancel = context.WithTimeout(context.Background(), etcdOpTimeout)
		getResponse, err = etcdKV.Get(ctx, checkpointEtcdKeyName)
		cancel()
		if nil != err {
			err = fmt.Errorf("Error contacting etcd [Case 1]: %v", err)
			return
		}

		if 0 < getResponse.Count {
			isEmpty = false
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

			// Clear etcd if it exists...

			if etcdEnabled {
				ctx, cancel = context.WithTimeout(context.Background(), etcdOpTimeout)
				_, err = etcdKV.Delete(ctx, checkpointEtcdKeyName)
				cancel()
				if nil != err {
					err = fmt.Errorf("Error contacting etcd [Case 2]: %v", err)
					return
				}
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

	// With format complete, we can shutdown for good

	err = transitions.Down(confMap)
	if nil != err {
		return
	}

	// Close down etcd

	if etcdEnabled {
		etcdKV = nil

		err = etcdClient.Close()
		if nil != err {
			return
		}
	}

	return
}
