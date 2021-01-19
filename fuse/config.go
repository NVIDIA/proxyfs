// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/swiftstack/ProxyFS/trackedlock"
	"github.com/swiftstack/ProxyFS/transitions"
)

const (
	maxRetryCount uint32 = 100
	retryGap             = 100 * time.Millisecond
)

type volumeStruct struct {
	volumeName     string
	mountPointName string
	mounted        bool
}

type globalsStruct struct {
	gate trackedlock.RWMutex //               API Requests RLock()/RUnlock()
	//                                        confMap changes Lock()/Unlock()
	//                                        Note: fuselib.Unmount() results in an Fsync() call on RootDir
	//                                        Hence, no current confMap changes currently call Lock()
	volumeMap     map[string]*volumeStruct // key == volumeStruct.volumeName
	mountPointMap map[string]*volumeStruct // key == volumeStruct.mountPointName
}

var globals globalsStruct

func init() {
	transitions.Register("fuse", &globals)
}

func (dummy *globalsStruct) Up(confMap conf.ConfMap) (err error) {
	globals.volumeMap = make(map[string]*volumeStruct)
	globals.mountPointMap = make(map[string]*volumeStruct)

	closeGate() // Ensure gate starts out in the Exclusively Locked state

	err = nil
	return
}

func (dummy *globalsStruct) VolumeGroupCreated(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupMoved(confMap conf.ConfMap, volumeGroupName string, activePeer string, virtualIPAddr string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeGroupDestroyed(confMap conf.ConfMap, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeCreated(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeMoved(confMap conf.ConfMap, volumeName string, volumeGroupName string) (err error) {
	return nil
}
func (dummy *globalsStruct) VolumeDestroyed(confMap conf.ConfMap, volumeName string) (err error) {
	return nil
}

func (dummy *globalsStruct) ServeVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		volume *volumeStruct
	)

	volume = &volumeStruct{
		volumeName: volumeName,
		mounted:    false,
	}

	volume.mountPointName, err = confMap.FetchOptionValueString("Volume:"+volumeName, "FUSEMountPointName")
	if nil != err {
		return
	}

	globals.volumeMap[volume.volumeName] = volume
	globals.mountPointMap[volume.mountPointName] = volume

	err = performMount(volume)

	return // return err from performMount() sufficient
}

func (dummy *globalsStruct) UnserveVolume(confMap conf.ConfMap, volumeName string) (err error) {
	var (
		lazyUnmountCmd *exec.Cmd
		ok             bool
		volume         *volumeStruct
	)

	volume, ok = globals.volumeMap[volumeName]

	if ok {
		if volume.mounted {
			err = fuselib.Unmount(volume.mountPointName)
			if nil == err {
				logger.Infof("Unmounted %v", volume.mountPointName)
			} else {
				lazyUnmountCmd = exec.Command("fusermount", "-uz", volume.mountPointName)
				err = lazyUnmountCmd.Run()
				if nil == err {
					logger.Infof("Lazily unmounted %v", volume.mountPointName)
				} else {
					logger.Infof("Unable to lazily unmount %v - got err == %v", volume.mountPointName, err)
				}
			}
		}

		delete(globals.volumeMap, volume.volumeName)
		delete(globals.mountPointMap, volume.mountPointName)
	}

	err = nil
	return
}

func (dummy *globalsStruct) VolumeToBeUnserved(confMap conf.ConfMap, volumeName string) (err error) {
	// TODO: Might want to actually FUSE Unmount right here

	err = nil
	return
}

func (dummy *globalsStruct) SignaledStart(confMap conf.ConfMap) (err error) {
	closeGate()

	err = nil
	return
}

func (dummy *globalsStruct) SignaledFinish(confMap conf.ConfMap) (err error) {
	openGate()

	err = nil
	return
}

func (dummy *globalsStruct) Down(confMap conf.ConfMap) (err error) {
	if 0 != len(globals.volumeMap) {
		err = fmt.Errorf("fuse.Down() called with 0 != len(globals.volumeMap")
		return
	}
	if 0 != len(globals.mountPointMap) {
		err = fmt.Errorf("fuse.Down() called with 0 != len(globals.mountPointMap")
		return
	}

	openGate() // In case we are restarted... Up() expects Gate to initially be open

	err = nil
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

func performMount(volume *volumeStruct) (err error) {
	var (
		conn                          *fuselib.Conn
		curRetryCount                 uint32
		lazyUnmountCmd                *exec.Cmd
		missing                       bool
		mountPointContainingDirDevice int64
		mountPointDevice              int64
		mountPointNameBase            string
		volumeHandle                  fs.VolumeHandle
	)

	volume.mounted = false

	missing, mountPointContainingDirDevice, err = fetchInodeDevice(path.Dir(volume.mountPointName))
	if nil != err {
		return
	}
	if missing {
		logger.Infof("Unable to serve %s.FUSEMountPoint == %s (mount point dir's parent does not exist)", volume.volumeName, volume.mountPointName)
		return
	}

	missing, mountPointDevice, err = fetchInodeDevice(volume.mountPointName)
	if nil == err {
		if missing {
			logger.Infof("Unable to serve %s.FUSEMountPoint == %s (mount point dir does not exist)", volume.volumeName, volume.mountPointName)
			return
		}
	}

	if (nil != err) || (mountPointDevice != mountPointContainingDirDevice) {
		// Presumably, the mount point is (still) currently mounted, so attempt to unmount it first

		lazyUnmountCmd = exec.Command("fusermount", "-uz", volume.mountPointName)
		err = lazyUnmountCmd.Run()
		if nil != err {
			return
		}

		curRetryCount = 0

		for {
			time.Sleep(retryGap) // Try again in a bit
			missing, mountPointDevice, err = fetchInodeDevice(volume.mountPointName)
			if nil == err {
				if missing {
					err = fmt.Errorf("Race condition: %s.FUSEMountPoint == %s disappeared [case 1]", volume.volumeName, volume.mountPointName)
					return
				}
				if mountPointDevice == mountPointContainingDirDevice {
					break
				}
			}
			curRetryCount++
			if curRetryCount >= maxRetryCount {
				err = fmt.Errorf("MaxRetryCount exceeded for %s.FUSEMountPoint == %s [case 1]", volume.volumeName, volume.mountPointName)
				return
			}
		}
	}

	mountPointNameBase = path.Base(volume.mountPointName)

	conn, err = fuselib.Mount(
		volume.mountPointName,
		fuselib.FSName(mountPointNameBase),
		fuselib.AllowOther(),
		// OS X specific—
		fuselib.LocalVolume(),
		fuselib.VolumeName(mountPointNameBase),
		fuselib.NoAppleDouble(),
		fuselib.NoAppleXattr(),
	)

	if nil != err {
		logger.WarnfWithError(err, "Couldn't mount %s.FUSEMountPoint == %s", volume.volumeName, volume.mountPointName)
		err = nil
		return
	}

	volumeHandle, err = fs.FetchVolumeHandleByVolumeName(volume.volumeName)
	if nil != err {
		return
	}

	fs := &ProxyFUSE{volumeHandle: volumeHandle}

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
	}(volume.mountPointName, conn)

	// Wait for FUSE to mount the file system.   The "fs.wg.Done()" is in the
	// Root() routine.
	fs.wg.Wait()

	// If we made it to here, all was ok

	logger.Infof("Now serving %s.FUSEMountPoint == %s", volume.volumeName, volume.mountPointName)

	volume.mounted = true

	err = nil
	return
}

func openGate() {
	globals.gate.Unlock()
}

func closeGate() {
	globals.gate.Lock()
}

// Note: The following func's do nothing today. Thus, no "gate" is enforced in this package.
//       The reason is that as part of the fuselib.Unmount() in UnserveVolume(), a call to
//       Fsync() will be made. If the closeGate() were honored, the call to Fsync() would
//       indefinitely block.

func enterGate() {
	// globals.gate.RLock()
}

func leaveGate() {
	// globals.gate.RUnlock()
}
