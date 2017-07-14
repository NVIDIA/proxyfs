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
	mountPointMap map[string]*mountPointStruct
}

var globals globalsStruct

func Up(confMap conf.ConfMap) (err error) {
	var (
		alreadyInMountPointMap bool
		conn                   *fuselib.Conn
		endingInodeNumber      uint64
		mountHandle            fs.MountHandle
		mountPoint             *mountPointStruct
		mountPointName         string
		primaryPeerName        string
		startingInodeNumber    uint64
		volumeName             string
		volumeNameSlice        []string
		whoAmI                 string
	)

	globals.mountPointMap = make(map[string]*mountPointStruct)

	volumeNameSlice, err = confMap.FetchOptionValueStringSlice("FSGlobals", "VolumeList")
	if nil != err {
		return
	}

	whoAmI, err = confMap.FetchOptionValueString("Cluster", "WhoAmI")
	if nil != err {
		return
	}

	for _, volumeName = range volumeNameSlice {
		primaryPeerName, err = confMap.FetchOptionValueString(volumeName, "PrimaryPeer")
		if nil != err {
			return
		}

		if primaryPeerName == whoAmI {
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
	}

	// If we reach here, we succeeded importing confMap

	for mountPointName, mountPoint = range globals.mountPointMap {
		if platform.IsDarwin {
			startingInodeNumber, err = fetchInodeNumber(mountPointName)
			if nil != err {
				logger.WarnfWithError(err, "Couldn't mount %v; (continuing with remaining volumes ...) [Case One]", mountPointName)
				continue
			}
		}

		conn, err = fuselib.Mount(
			mountPointName,
			fuselib.FSName(mountPointName),
			fuselib.AllowOther(),
			// OS X specific—
			fuselib.LocalVolume(),
			fuselib.VolumeName(mountPointName),
		)

		if nil != err {
			logger.WarnfWithError(err, "Couldn't mount %v; (continuing with remaining volumes ...) [Case Two]", mountPointName)
			continue
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
		}(mountPointName, conn)

		if platform.IsDarwin {
			for {
				endingInodeNumber, err = fetchInodeNumber(mountPointName)
				if nil != err {
					logger.WarnfWithError(err, "Couldn't mount %v; (continuing with remaining volumes ...) [Case Three]", mountPointName)
					continue
				}

				if startingInodeNumber != endingInodeNumber { // Indicates the FUSE Mount has completed
					mountPoint.mounted = true
					break
				}

				time.Sleep(10 * time.Millisecond) // Try again in a bit
			}
		} else { // platform.IsLinux
			for {
				if isMountPoint(mountPointName) {
					mountPoint.mounted = true
					break
				}

				time.Sleep(10 * time.Millisecond) // Try again in a bit
			}
		}
	}

	err = nil
	return
}

func PauseAndContract(confMap conf.ConfMap) (err error) {
	err = nil // TODO
	return
}

func ExpandAndResume(confMap conf.ConfMap) (err error) {
	err = nil // TODO
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
