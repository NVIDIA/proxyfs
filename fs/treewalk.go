package fs

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

type treeWalkStruct struct {
	volumeName       string
	volumeHandle     inode.VolumeHandle
	stopChan         chan bool
	inodeRefCountMap map[inode.InodeNumber]uint64
}

func validateVolume(volumeName string, stopChan chan bool) (err error) {
	tWS := &treeWalkStruct{
		volumeName:       volumeName,
		stopChan:         stopChan,
		inodeRefCountMap: make(map[inode.InodeNumber]uint64),
	}

	tWS.volumeHandle, err = inode.FetchVolumeHandle(volumeName)
	if nil != err {
		return
	}

	stopped, err := tWS.validateDirInode(inode.RootDirInodeNumber)
	if (nil != err) || stopped {
		return
	}

	linkCountsFixed := uint64(0)

	for inodeNumber, inodeRefCount := range tWS.inodeRefCountMap {
		select {
		case _ = <-tWS.stopChan:
			return
		default:
			inodeLinkCount, nonShadowingErr := tWS.volumeHandle.GetLinkCount(inodeNumber)
			if nil != nonShadowingErr {
				err = fmt.Errorf("%v.GetLinkCount(%v) failed: %v", tWS.volumeName, inodeNumber, nonShadowingErr)
				return
			}
			if inodeLinkCount != inodeRefCount {
				stats.IncrementOperations(&stats.FsValidateFixedLinkCountOps)
				logger.Errorf("fs.validateVolume(%v,): found link count discrepency on inode# 0x%016X", volumeName, inodeNumber)
				nonShadowingErr = tWS.volumeHandle.SetLinkCount(inodeNumber, inodeRefCount)
				if nil != nonShadowingErr {
					err = fmt.Errorf("%v.SetLinkCount(%v,) failed: %v", tWS.volumeName, inodeNumber, nonShadowingErr)
					return
				}
				linkCountsFixed++
			}
		}
	}

	switch linkCountsFixed {
	case uint64(0):
		err = nil
	case uint64(1):
		err = fmt.Errorf("1 inode needed its LinkCount corrected")
	default:
		err = fmt.Errorf("%v inodes needed their LinkCount corrected", linkCountsFixed)
	}

	return
}

func (tWS *treeWalkStruct) validateDirInode(dirInodeNumber inode.InodeNumber) (stopped bool, err error) {
	err = tWS.volumeHandle.Validate(dirInodeNumber)
	if nil != err {
		stopped = false
		stats.IncrementOperations(&stats.FsValidateDirectoryFailedOps)
		err = fmt.Errorf("%v.Validate(%v) failed: %v", tWS.volumeName, dirInodeNumber, err)
		logger.Errorf("fs.validateDirInode() error: %v", err)
		return
	}

	dirEntrySlice, _, err := tWS.volumeHandle.ReadDir(dirInodeNumber, 0, 0)
	if nil != err {
		stopped = false
		err = fmt.Errorf("%v.ReadDir(%v,,) failed: %v", tWS.volumeName, dirInodeNumber, err)
		return
	}

	for _, dirEntry := range dirEntrySlice {
		select {
		case _ = <-tWS.stopChan:
			stopped = true
			err = nil
			return
		default:
			prevInodeRefCount, ok := tWS.inodeRefCountMap[dirEntry.InodeNumber]
			if ok {
				tWS.inodeRefCountMap[dirEntry.InodeNumber] = 1 + prevInodeRefCount
			} else {
				tWS.inodeRefCountMap[dirEntry.InodeNumber] = 1
			}

			if ("." != dirEntry.Basename) && (".." != dirEntry.Basename) {
				inodeType, nonShadowingErr := tWS.volumeHandle.GetType(dirEntry.InodeNumber)
				if nil != nonShadowingErr {
					stopped = false
					err = fmt.Errorf("%v.GetType(%v) failed: %v", tWS.volumeName, dirEntry.InodeNumber, nonShadowingErr)
					return
				}

				if inode.DirType == inodeType {
					stopped, err = tWS.validateDirInode(dirEntry.InodeNumber)
					if stopped || (nil != err) {
						return
					}
				} else {
					err = tWS.validateNonDirInode(dirEntry.InodeNumber)
					if nil != err {
						stopped = false
						return
					}
				}
			}
		}
	}

	stopped = false
	err = nil
	return
}

func (tWS *treeWalkStruct) validateNonDirInode(nonDirInodeNumber inode.InodeNumber) (err error) {
	err = tWS.volumeHandle.Validate(nonDirInodeNumber)
	if nil != err {
		stats.IncrementOperations(&stats.FsValidateNonDirectoryFailedOps)
		err = fmt.Errorf("%v.Validate(%v) failed: %v", tWS.volumeName, nonDirInodeNumber, err)
		logger.Errorf("fs.validateNonDirInode() error: %v", err)
	}
	return
}
