// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package fuse

import (
	"fmt"
	"os"
	"time"

	fuselib "bazil.org/fuse"
	"golang.org/x/net/context"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
)

type Symlink struct {
	volumeHandle fs.VolumeHandle
	inodeNumber  inode.InodeNumber
}

func (s Symlink) Attr(ctx context.Context, attr *fuselib.Attr) (err error) {
	var (
		stat fs.Stat
	)

	enterGate()
	defer leaveGate()

	stat, err = s.volumeHandle.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, s.inodeNumber)
	if nil != err {
		err = newFuseError(err)
		return
	}
	if uint64(inode.SymlinkType) != stat[fs.StatFType] {
		err = fmt.Errorf("[fuse]Dir.Attr() called on non-Symlink")
		err = blunder.AddError(err, blunder.InvalidInodeTypeError)
		err = newFuseError(err)
		return
	}

	attr.Valid = time.Duration(time.Microsecond) // TODO: Make this settable if FUSE inside ProxyFS endures
	attr.Inode = uint64(s.inodeNumber)           // or stat[fs.StatINum]
	attr.Size = stat[fs.StatSize]
	attr.Atime = time.Unix(0, int64(stat[fs.StatATime]))
	attr.Mtime = time.Unix(0, int64(stat[fs.StatMTime]))
	attr.Ctime = time.Unix(0, int64(stat[fs.StatCTime]))
	attr.Crtime = time.Unix(0, int64(stat[fs.StatCRTime]))
	attr.Mode = os.ModeSymlink | os.FileMode(stat[fs.StatMode]&0777)
	attr.Nlink = uint32(stat[fs.StatNLink])
	attr.Uid = uint32(stat[fs.StatUserID])
	attr.Gid = uint32(stat[fs.StatGroupID])

	return
}

func (s Symlink) Setattr(ctx context.Context, req *fuselib.SetattrRequest, resp *fuselib.SetattrResponse) (err error) {
	var (
		stat        fs.Stat
		statUpdates fs.Stat
	)

	enterGate()
	defer leaveGate()

	stat, err = s.volumeHandle.Getstat(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, s.inodeNumber)
	if nil != err {
		err = newFuseError(err)
		return
	}
	if uint64(inode.SymlinkType) != stat[fs.StatFType] {
		err = fmt.Errorf("[fuse]Dir.Attr() called on non-Symlink")
		err = blunder.AddError(err, blunder.InvalidInodeTypeError)
		err = newFuseError(err)
		return
	}

	statUpdates = make(fs.Stat)

	if 0 != (fuselib.SetattrMode & req.Valid) {
		statUpdates[fs.StatMode] = uint64(req.Mode & 0777)
	}
	if 0 != (fuselib.SetattrUid & req.Valid) {
		statUpdates[fs.StatUserID] = uint64(req.Uid)
	}
	if 0 != (fuselib.SetattrGid & req.Valid) {
		statUpdates[fs.StatGroupID] = uint64(req.Gid)
	}
	if 0 != (fuselib.SetattrAtime & req.Valid) {
		statUpdates[fs.StatATime] = uint64(req.Atime.UnixNano())
	}
	if 0 != (fuselib.SetattrMtime & req.Valid) {
		statUpdates[fs.StatMTime] = uint64(req.Mtime.UnixNano())
	}
	if 0 != (fuselib.SetattrAtimeNow & req.Valid) {
		statUpdates[fs.StatATime] = uint64(time.Now().UnixNano())
	}
	if 0 != (fuselib.SetattrMtimeNow & req.Valid) {
		statUpdates[fs.StatMTime] = uint64(time.Now().UnixNano())
	}
	if 0 != (fuselib.SetattrCrtime & req.Valid) {
		statUpdates[fs.StatCRTime] = uint64(req.Crtime.UnixNano())
	}

	err = s.volumeHandle.Setstat(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, s.inodeNumber, statUpdates)
	if nil != err {
		err = newFuseError(err)
	}

	return
}

func (s Symlink) Fsync(ctx context.Context, req *fuselib.FsyncRequest) error {
	return fuselib.ENOSYS
}

func (s Symlink) Readlink(ctx context.Context, req *fuselib.ReadlinkRequest) (string, error) {
	target, err := s.volumeHandle.Readsymlink(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, s.inodeNumber)
	if nil != err {
		err = newFuseError(err)
	}
	return target, err
}
