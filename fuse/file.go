package fuse

import (
	"fmt"
	"io"
	"os"
	"time"

	fuselib "bazil.org/fuse"
	"golang.org/x/net/context"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
)

type File struct {
	mountHandle fs.MountHandle
	inodeNumber inode.InodeNumber
}

func (f File) Access(ctx context.Context, req *fuselib.AccessRequest) error {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	if f.mountHandle.Access(inode.InodeUserID(req.Uid), inode.InodeGroupID(req.Gid), nil, f.inodeNumber, inode.InodeMode(req.Mask)) {
		return nil
	} else {
		return newFuseError(blunder.NewError(blunder.PermDeniedError, "EACCES"))
	}
}

func (f File) Attr(ctx context.Context, attr *fuselib.Attr) (err error) {
	var (
		stat fs.Stat
	)

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	stat, err = f.mountHandle.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, f.inodeNumber)
	if nil != err {
		err = newFuseError(err)
		return
	}
	if uint64(inode.FileType) != stat[fs.StatFType] {
		err = fmt.Errorf("[fuse]Dir.Attr() called on non-File")
		err = blunder.AddError(err, blunder.InvalidInodeTypeError)
		err = newFuseError(err)
		return
	}

	attr.Inode = uint64(f.inodeNumber) // or stat[fs.StatINum]
	attr.Size = stat[fs.StatSize]
	attr.Blocks = (stat[fs.StatSize] + 511) / 512
	attr.Atime = time.Unix(0, int64(stat[fs.StatATime]))
	attr.Mtime = time.Unix(0, int64(stat[fs.StatMTime]))
	attr.Ctime = time.Unix(0, int64(stat[fs.StatCTime]))
	attr.Crtime = time.Unix(0, int64(stat[fs.StatCRTime]))
	attr.Mode = os.FileMode(stat[fs.StatMode] & 0777)
	attr.Nlink = uint32(stat[fs.StatNLink])
	attr.Uid = uint32(stat[fs.StatUserID])
	attr.Gid = uint32(stat[fs.StatGroupID])
	attr.BlockSize = 4096 // Just a guess at a reasonable block size

	return
}

func (f File) Setattr(ctx context.Context, req *fuselib.SetattrRequest, resp *fuselib.SetattrResponse) (err error) {
	var (
		stat        fs.Stat
		statUpdates fs.Stat
	)

	globals.gate.RLock()
	defer globals.gate.RUnlock()

	stat, err = f.mountHandle.Getstat(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, f.inodeNumber)
	if nil != err {
		err = newFuseError(err)
		return
	}
	if uint64(inode.FileType) != stat[fs.StatFType] {
		err = fmt.Errorf("[fuse]Dir.Attr() called on non-File")
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

	err = f.mountHandle.Setstat(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, f.inodeNumber, statUpdates)
	if nil != err {
		err = newFuseError(err)
		return
	}

	if 0 != (fuselib.SetattrSize & req.Valid) {
		err = f.mountHandle.Resize(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, f.inodeNumber, req.Size)
		if nil != err {
			err = newFuseError(err)
			return
		}
	}

	return
}

func (f File) Flush(ctx context.Context, req *fuselib.FlushRequest) error {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	err := f.mountHandle.Flush(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, f.inodeNumber)
	if nil != err {
		err = newFuseError(err)
	}
	return err
}

func (f File) Fsync(ctx context.Context, req *fuselib.FsyncRequest) error {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	err := f.mountHandle.Flush(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil,
		f.inodeNumber)
	if nil != err {
		err = newFuseError(err)
	}
	return err
}

func (f File) Read(ctx context.Context, req *fuselib.ReadRequest, resp *fuselib.ReadResponse) (err error) {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	buf, err := f.mountHandle.Read(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, f.inodeNumber, uint64(req.Offset), uint64(req.Size), nil)
	if err != nil && err != io.EOF {
		err = newFuseError(err)
		return err
	}
	resp.Data = buf
	return nil
}

func (f File) Write(ctx context.Context, req *fuselib.WriteRequest, resp *fuselib.WriteResponse) error {
	globals.gate.RLock()
	defer globals.gate.RUnlock()

	// We need to buffer contents of req.Data because fs.Write() will likely retain a reference to it
	// (down in the Chunked PUT retry buffer) and Bazil FUSE will be reusing this WriteRequest (including
	// its .Data buf) for the next request.

	bufferedData := make([]byte, len(req.Data), len(req.Data))
	copy(bufferedData, req.Data)

	size, err := f.mountHandle.Write(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, f.inodeNumber, uint64(req.Offset), bufferedData, nil)
	if nil == err {
		resp.Size = int(size)
	} else {
		err = newFuseError(err)
	}
	return err
}
