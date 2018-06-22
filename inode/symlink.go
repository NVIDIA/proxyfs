package inode

import (
	"fmt"

	"github.com/swiftstack/ProxyFS/logger"
	"github.com/swiftstack/ProxyFS/stats"
)

func (vS *volumeStruct) CreateSymlink(target string, filePerm InodeMode, userID InodeUserID, groupID InodeGroupID) (symlinkInodeNumber InodeNumber, err error) {
	// Create file mode out of file permissions plus inode type
	fileMode, err := determineMode(filePerm, SymlinkType)
	if err != nil {
		return
	}

	symlinkInode, err := vS.makeInMemoryInode(SymlinkType, fileMode, userID, groupID)
	if err != nil {
		return
	}

	symlinkInode.dirty = true

	symlinkInode.SymlinkTarget = target
	symlinkInodeNumber = symlinkInode.InodeNumber

	ok, err := vS.inodeCacheInsert(symlinkInode)
	if nil != err {
		return
	}
	if !ok {
		err = fmt.Errorf("inodeCacheInsert(symlinkInode) failed")
		return
	}

	err = vS.flushInode(symlinkInode)
	if err != nil {
		logger.ErrorWithError(err)
		return
	}

	stats.IncrementOperations(&stats.SymlinkCreateOps)

	return
}

func (vS *volumeStruct) GetSymlink(symlinkInodeNumber InodeNumber) (target string, err error) {
	symlinkInode, err := vS.fetchInodeType(symlinkInodeNumber, SymlinkType)
	if err != nil {
		logger.ErrorWithError(err)
		return
	}

	target = symlinkInode.SymlinkTarget

	stats.IncrementOperations(&stats.SymlinkReadOps)

	return
}
