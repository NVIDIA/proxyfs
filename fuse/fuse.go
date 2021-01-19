// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package fuse

import (
	"fmt"
	"sync"

	fuselib "bazil.org/fuse"
	fusefslib "bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
)

type ProxyFUSE struct {
	volumeHandle fs.VolumeHandle
	wg           sync.WaitGroup // Used to synchronize mount
}

func (pfs *ProxyFUSE) Root() (fusefslib.Node, error) {
	root := Dir{volumeHandle: pfs.volumeHandle, inodeNumber: inode.RootDirInodeNumber}

	// Signal any waiters that we have completed mounting the volume.
	// We know this because this call is only made after the user level FUSE
	// library and the FUSE driver have agreed on the FUSE prototocol level.
	pfs.wg.Done()
	return root, nil
}

func (pfs *ProxyFUSE) Statfs(ctx context.Context, req *fuselib.StatfsRequest, resp *fuselib.StatfsResponse) error {
	enterGate()
	defer leaveGate()

	statvfs, err := pfs.volumeHandle.StatVfs()
	if err != nil {
		return newFuseError(err)
	}
	resp.Blocks = statvfs[fs.StatVFSTotalBlocks]
	resp.Bfree = statvfs[fs.StatVFSFreeBlocks]
	resp.Bavail = statvfs[fs.StatVFSAvailBlocks]
	resp.Files = statvfs[fs.StatVFSTotalInodes]
	resp.Ffree = statvfs[fs.StatVFSFreeInodes]
	resp.Bsize = uint32(statvfs[fs.StatVFSBlockSize])
	resp.Namelen = uint32(statvfs[fs.StatVFSMaxFilenameLen])
	resp.Frsize = uint32(statvfs[fs.StatVFSFragmentSize])
	return nil
}

type fuseError struct {
	str   string
	errno fuselib.Errno
}

func (fE *fuseError) Errno() fuselib.Errno {
	return fE.errno
}

func (fE *fuseError) Error() string {
	return fE.str
}

func newFuseError(err error) *fuseError {
	return &fuseError{
		str:   fmt.Sprintf("%v", err),
		errno: fuselib.Errno(blunder.Errno(err)),
	}
}
