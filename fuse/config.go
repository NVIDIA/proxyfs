// Package fuse is a FUSE filesystem for ProxyFS (an alternative to the Samba-VFS frontend).
package fuse

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"syscall"
	"time"

	fuselib "bazil.org/fuse"
	fusefslib "bazil.org/fuse/fs"

	"github.com/swiftstack/ProxyFS/conf"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/utils"
)

const (
	maxRetryCount uint32 = 100
	retryGap             = 100 * time.Millisecond
)

type mountPointStruct struct {
	mountPointName string
	volumeName     string
	mounted        bool
}

type globalsStruct struct {
	whoAmI        string
	mountPointMap map[string]*mountPointStruct // key == mountPointStruct.mountPointName
}

var globals globalsStruct

func Up(confMap conf.ConfMap) (err error) {
	var (
		alreadyInMountPointMap bool
		mountPoint             *mountPointStruct
		mountPointName         string
		primaryPeerNameList    []string
		volumeList             []string
		volumeName             string
		volumeSectionName      string
	)

	globals.mountPointMap = make(map[string]*mountPointStruct)

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	// Look thru list of volumes and generate map of volumes to be mounted on
	// the local node.
	for _, volumeName = range volumeList {
		volumeSectionName = utils.VolumeNameConfSection(volumeName)

		primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(volumeSectionName, "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerNameList) {
			continue
		} else if 1 == len(primaryPeerNameList) {
			if globals.whoAmI == primaryPeerNameList[0] {
				mountPointName, err = confMap.FetchOptionValueString(volumeSectionName, "FUSEMountPointName")
				if nil != err {
					return
				}

				_, alreadyInMountPointMap = globals.mountPointMap[mountPointName]
				if alreadyInMountPointMap {
					err = fmt.Errorf("MountPoint \"%v\" only allowed to be used by a single Volume", mountPointName)
					return
				}

				mountPoint = &mountPointStruct{mountPointName: mountPointName, volumeName: volumeName, mounted: false}

				globals.mountPointMap[mountPointName] = mountPoint
			}
		} else {
			err = fmt.Errorf("Volume \"%v\" only allowed one PrimaryPeer", volumeName)
			return
		}
	}

	// If we reach here, we succeeded importing confMap.
	// Now go and mount the volumes.
	for _, mountPoint = range globals.mountPointMap {
		err = performMount(mountPoint)
		if nil != err {
			return
		}
	}

	err = nil
	return
}

func PauseAndContract(confMap conf.ConfMap) (err error) {
	var (
		mountPoint          *mountPointStruct
		ok                  bool
		primaryPeerNameList []string
		removedVolumeMap    map[string]*mountPointStruct // key == volumeName
		volumeList          []string
		volumeName          string
		whoAmI              string
	)

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueString(\"Cluster\", \"WhoAmI\") failed: %v", err)
		return
	}
	if whoAmI != globals.whoAmI {
		err = fmt.Errorf("confMap change not allowed to alter [Cluster]WhoAmI")
		return
	}

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	removedVolumeMap = make(map[string]*mountPointStruct)

	for _, mountPoint = range globals.mountPointMap {
		removedVolumeMap[mountPoint.volumeName] = mountPoint
	}

	for _, volumeName = range volumeList {
		_, ok = removedVolumeMap[volumeName]
		if ok {
			primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(utils.VolumeNameConfSection(volumeName), "PrimaryPeer")
			if nil != err {
				return
			}

			if 0 == len(primaryPeerNameList) {
				continue
			} else if 1 == len(primaryPeerNameList) {
				if globals.whoAmI == primaryPeerNameList[0] {
					delete(removedVolumeMap, volumeName)
				}
			} else {
				err = fmt.Errorf("Volume \"%v\" only allowed one PrimaryPeer", volumeName)
				return
			}
		}
	}

	for volumeName, mountPoint = range removedVolumeMap {
		if mountPoint.mounted {
			err = fuselib.Unmount(mountPoint.mountPointName)
			if nil == err {
				logger.Infof("Unmounted %v", mountPoint.mountPointName)
			} else {
				lazyUnmountCmd := exec.Command("fusermount", "-uz", mountPoint.mountPointName)
				err = lazyUnmountCmd.Run()
				if nil == err {
					logger.Infof("Lazily unmounted %v", mountPoint.mountPointName)
				} else {
					logger.Infof("Unable to lazily unmount %v - got err == %v", mountPoint.mountPointName, err)
				}
			}
		}

		delete(globals.mountPointMap, mountPoint.mountPointName)
	}

	err = nil
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	var (
		mountPoint          *mountPointStruct
		mountPointName      string
		ok                  bool
		primaryPeerNameList []string
		volumeList          []string
		volumeName          string
		volumeSectionName   string
	)

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	for _, volumeName = range volumeList {
		volumeSectionName = utils.VolumeNameConfSection(volumeName)

		primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(volumeSectionName, "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerNameList) {
			continue
		} else if 1 == len(primaryPeerNameList) {
			if globals.whoAmI == primaryPeerNameList[0] {
				mountPointName, err = confMap.FetchOptionValueString(volumeSectionName, "FUSEMountPointName")
				if nil != err {
					return
				}

				_, ok = globals.mountPointMap[mountPointName]
				if !ok {
					mountPoint = &mountPointStruct{mountPointName: mountPointName, volumeName: volumeName, mounted: false}

					globals.mountPointMap[mountPointName] = mountPoint

					err = performMount(mountPoint)
					if nil != err {
						return
					}
				}
			}
		} else {
			err = fmt.Errorf("Volume \"%v\" only allowed one PrimaryPeer", volumeName)
			return
		}
	}

	err = nil
	return
}

func Down() (err error) {
	var (
		mountPoint     *mountPointStruct
		mountPointName string
	)

	for mountPointName, mountPoint = range globals.mountPointMap {
		if mountPoint.mounted {
			err = fuselib.Unmount(mountPointName)
			if nil == err {
				logger.Infof("Unmounted %v", mountPointName)
				break
			}
			lazyUnmountCmd := exec.Command("fusermount", "-uz", mountPointName)
			err = lazyUnmountCmd.Run()
			if nil == err {
				logger.Infof("Lazily unmounted %v", mountPointName)
			} else {
				logger.Infof("Unable to lazily unmount %v - got err == %v", mountPointName, err)
			}
		}
	}

	return
}

func fetchInodeDevice(path string) (missing bool, inodeDevice int64, err error) {
	fi, err := os.Stat(path)
	if nil != err {
		if os.IsNotExist(err) {
			missing = true
			err = nil
		} else {
			err = fmt.Errorf("fetchInodeDevice(%v): os.Stat() failed: %v", path, err)
		}
		return
	}
	if nil == fi.Sys() {
		err = fmt.Errorf("fetchInodeDevice(%v): fi.Sys() was nil", path)
		return
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		err = fmt.Errorf("fetchInodeDevice(%v): fi.Sys().(*syscall.Stat_t) returned ok == false", path)
		return
	}
	missing = false
	inodeDevice = int64(stat.Dev)
	return
}

func performMount(mountPoint *mountPointStruct) (err error) {
	var (
		conn                          *fuselib.Conn
		curRetryCount                 uint32
		lazyUnmountCmd                *exec.Cmd
		missing                       bool
		mountHandle                   fs.MountHandle
		mountPointContainingDirDevice int64
		mountPointDevice              int64
	)

	mountPoint.mounted = false

	missing, mountPointContainingDirDevice, err = fetchInodeDevice(path.Dir(mountPoint.mountPointName))
	if nil != err {
		return
	}
	if missing {
		logger.Infof("Unable to serve %s.FUSEMountPoint == %s (mount point dir's parent does not exist)", mountPoint.volumeName, mountPoint.mountPointName)
		return
	}

	missing, mountPointDevice, err = fetchInodeDevice(mountPoint.mountPointName)
	if nil == err {
		if missing {
			logger.Infof("Unable to serve %s.FUSEMountPoint == %s (mount point dir does not exist)", mountPoint.volumeName, mountPoint.mountPointName)
			return
		}
	}

	if (nil != err) || (mountPointDevice != mountPointContainingDirDevice) {
		// Presumably, the mount point is (still) currently mounted, so attempt to unmount it first

		lazyUnmountCmd = exec.Command("fusermount", "-uz", mountPoint.mountPointName)
		err = lazyUnmountCmd.Run()
		if nil != err {
			return
		}

		curRetryCount = 0

		for {
			time.Sleep(retryGap) // Try again in a bit
			missing, mountPointDevice, err = fetchInodeDevice(mountPoint.mountPointName)
			if nil == err {
				if missing {
					err = fmt.Errorf("Race condition: %s.FUSEMountPoint == %s disappeared [case 1]", mountPoint.volumeName, mountPoint.mountPointName)
					return
				}
				if mountPointDevice == mountPointContainingDirDevice {
					break
				}
			}
			curRetryCount++
			if curRetryCount >= maxRetryCount {
				err = fmt.Errorf("MaxRetryCount exceeded for %s.FUSEMountPoint == %s [case 1]", mountPoint.volumeName, mountPoint.mountPointName)
				return
			}
		}
	}

	conn, err = fuselib.Mount(
		mountPoint.mountPointName,
		fuselib.FSName(mountPoint.mountPointName),
		fuselib.AllowOther(),
		// OS X specific—
		fuselib.LocalVolume(),
		fuselib.VolumeName(mountPoint.mountPointName),
	)

	if nil != err {
		logger.WarnfWithError(err, "Couldn't mount %s.FUSEMountPoint == %s", mountPoint.volumeName, mountPoint.mountPointName)
		err = nil
		return
	}

	mountHandle, err = fs.Mount(mountPoint.volumeName, fs.MountOptions(0))
	if nil != err {
		return
	}

	fs := &ProxyFUSE{mountHandle: mountHandle}

	// We synchronize the mounting of the mount point to make sure our FUSE goroutine
	// has reached the point that it can service requests.
	//
	// Otherwise, if proxyfsd is killed after we block on a FUSE request but before our
	// FUSE goroutine has had a chance to run we end up with an unkillable proxyfsd process.
	//
	// This would result in a "proxyfsd <defunct>" process that is only cleared by rebooting
	// the system.
	fs.wg.Add(1)

	go func(mountPointName string, conn *fuselib.Conn) {
		defer conn.Close()
		fusefslib.Serve(conn, fs)
	}(mountPoint.mountPointName, conn)

	// Wait for FUSE to mount the file system.   The "fs.wg.Done()" is in the
	// Root() routine.
	fs.wg.Wait()

	// If we made it to here, all was ok

	logger.Infof("Now serving %s.FUSEMountPoint == %s", mountPoint.volumeName, mountPoint.mountPointName)

	mountPoint.mounted = true

	err = nil
	return
}
