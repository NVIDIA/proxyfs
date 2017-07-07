package fuse

import (
	"fmt"

	fuselib "bazil.org/fuse"
	fusefslib "bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
)

type ProxyFUSE struct {
	mountHandle fs.MountHandle
}

func (pfs ProxyFUSE) Root() (fusefslib.Node, error) {
	root := Dir{mountHandle: pfs.mountHandle, inodeNumber: inode.RootDirInodeNumber}
	return root, nil
}

func (pfs ProxyFUSE) Statfs(ctx context.Context, req *fuselib.StatfsRequest, resp *fuselib.StatfsResponse) error {
	statvfs, err := pfs.mountHandle.StatVfs()
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
