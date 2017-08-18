// Package fuse is a FUSE filesystem for ProxyFS (an alternative to the Samba-VFS frontend).
package fuse

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	fuselib "bazil.org/fuse"
	fusefslib "bazil.org/fuse/fs"

	"github.com/swiftstack/conf"

	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/platform"
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
		volumeName             string
		volumeNameSlice        []string
	)

	globals.mountPointMap = make(map[string]*mountPointStruct)

	volumeNameSlice, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	globals.whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	for _, volumeName = range volumeNameSlice {
		primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(volumeName, "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerNameList) {
			continue
		} else if 1 == len(primaryPeerNameList) {
			if globals.whoAmI == primaryPeerNameList[0] {
				mountPointName, err = confMap.FetchOptionValueString(volumeName, "FUSEMountPointName")
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

	// If we reach here, we succeeded importing confMap

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
			primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(volumeName, "PrimaryPeer")
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
	)

	volumeList, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		err = fmt.Errorf("confMap.FetchOptionValueStringSlice(\"FSGlobals\", \"VolumeList\") failed: %v", err)
		return
	}

	for _, volumeName = range volumeList {
		primaryPeerNameList, err = confMap.FetchOptionValueStringSlice(volumeName, "PrimaryPeer")
		if nil != err {
			return
		}

		if 0 == len(primaryPeerNameList) {
			continue
		} else if 1 == len(primaryPeerNameList) {
			if globals.whoAmI == primaryPeerNameList[0] {
				mountPointName, err = confMap.FetchOptionValueString(volumeName, "FUSEMountPointName")
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

func isMountPoint(path string) (toReturn bool) {
	cmd := exec.Command("mountpoint", "-q", path)
	err := cmd.Run()
	toReturn = (nil == err)
	return
}

func fetchInodeNumber(path string) (inodeNumber uint64, err error) {
	fi, err := os.Stat(path)
	if nil != err {
		err = fmt.Errorf("fetchInodeNumber(): os.Stat() failed: %v\n", err)
		return
	}
	if nil == fi.Sys() {
		err = fmt.Errorf("fetchInodeNumber(): fi.Sys() was nil\n")
		return
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		err = fmt.Errorf("fetchInodeNumber(): fi.Sys().(*syscall.Stat_t) returned ok == false\n")
		return
	}
	inodeNumber = uint64(stat.Ino)
	return
}

func performMount(mountPoint *mountPointStruct) (err error) {
	var (
		conn                *fuselib.Conn
		endingInodeNumber   uint64
		mountHandle         fs.MountHandle
		startingInodeNumber uint64
	)

	lazyUnmountCmd := exec.Command("fusermount", "-uz", mountPoint.mountPointName)
	err = lazyUnmountCmd.Run()
	if nil != err {
		logger.Infof("Unable to lazily unmount %v - got err == %v", mountPoint.mountPointName, err)
	}

	if platform.IsDarwin {
		startingInodeNumber, err = fetchInodeNumber(mountPoint.mountPointName)
		if nil != err {
			logger.WarnfWithError(err, "Couldn't mount %v; (continuing with remaining volumes ...) [Case One]", mountPoint.mountPointName)
			err = nil
			return
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
		logger.WarnfWithError(err, "Couldn't mount %v; (continuing with remaining volumes ...) [Case Two]", mountPoint.mountPointName)
		err = nil
		return
	}

	mountHandle, err = fs.Mount(mountPoint.volumeName, fs.MountOptions(0))
	if nil != err {
		return
	}

	fs := &ProxyFUSE{mountHandle: mountHandle}

	go func(mountPointName string, conn *fuselib.Conn) {
		logger.Infof("Serving %v", mountPointName)
		defer conn.Close()
		fusefslib.Serve(conn, fs)
	}(mountPoint.mountPointName, conn)

	if platform.IsDarwin {
		for {
			endingInodeNumber, err = fetchInodeNumber(mountPoint.mountPointName)
			if nil != err {
				logger.WarnfWithError(err, "Couldn't mount %v; (continuing with remaining volumes ...) [Case Three]", mountPoint.mountPointName)
				err = nil
				return
			}

			if startingInodeNumber != endingInodeNumber { // Indicates the FUSE Mount has completed
				mountPoint.mounted = true
				err = nil
				return
			}

			time.Sleep(10 * time.Millisecond) // Try again in a bit
		}
	} else { // platform.IsLinux
		for {
			if isMountPoint(mountPoint.mountPointName) {
				mountPoint.mounted = true
				err = nil
				return
			}

			time.Sleep(10 * time.Millisecond) // Try again in a bit
		}
	}
}
