package fuse

import (
	"fmt"
	"os"
	"time"

	fuselib "bazil.org/fuse"
	fusefslib "bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/swiftstack/ProxyFS/blunder"
	"github.com/swiftstack/ProxyFS/fs"
	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/logger"
)

type Dir struct {
	mountHandle fs.MountHandle
	inodeNumber inode.InodeNumber
}

func (d Dir) Access(ctx context.Context, req *fuselib.AccessRequest) error {
	enterGate()
	defer leaveGate()

	if d.mountHandle.Access(inode.InodeUserID(req.Uid), inode.InodeGroupID(req.Gid), nil, d.inodeNumber, inode.InodeMode(req.Mask)) {
		return nil
	} else {
		return newFuseError(blunder.NewError(blunder.PermDeniedError, "EACCES"))
	}
}

func (d Dir) Attr(ctx context.Context, attr *fuselib.Attr) (err error) {
	var (
		stat fs.Stat
	)

	enterGate()
	defer leaveGate()

	stat, err = d.mountHandle.Getstat(inode.InodeRootUserID, inode.InodeGroupID(0), nil, d.inodeNumber)
	if nil != err {
		err = newFuseError(err)
		return
	}
	if uint64(inode.DirType) != stat[fs.StatFType] {
		err = fmt.Errorf("[fuse]Dir.Attr() called on non-Dir")
		err = blunder.AddError(err, blunder.InvalidInodeTypeError)
		err = newFuseError(err)
		return
	}

	attr.Valid = time.Duration(time.Microsecond) // TODO: Make this settable if FUSE inside ProxyFS endures
	attr.Inode = uint64(d.inodeNumber)           // or stat[fs.StatINum]
	attr.Size = stat[fs.StatSize]
	attr.Atime = time.Unix(0, int64(stat[fs.StatATime]))
	attr.Mtime = time.Unix(0, int64(stat[fs.StatMTime]))
	attr.Ctime = time.Unix(0, int64(stat[fs.StatCTime]))
	attr.Crtime = time.Unix(0, int64(stat[fs.StatCRTime]))
	attr.Mode = os.ModeDir | os.FileMode(stat[fs.StatMode]&0777)
	attr.Nlink = uint32(stat[fs.StatNLink])
	attr.Uid = uint32(stat[fs.StatUserID])
	attr.Gid = uint32(stat[fs.StatGroupID])

	return
}

func (d Dir) Setattr(ctx context.Context, req *fuselib.SetattrRequest, resp *fuselib.SetattrResponse) (err error) {
	var (
		stat        fs.Stat
		statUpdates fs.Stat
	)

	enterGate()
	defer leaveGate()

	stat, err = d.mountHandle.Getstat(inode.InodeUserID(req.Uid), inode.InodeGroupID(req.Gid), nil, d.inodeNumber)
	if nil != err {
		err = newFuseError(err)
		return
	}
	if uint64(inode.DirType) != stat[fs.StatFType] {
		err = fmt.Errorf("[fuse]Dir.Attr() called on non-Dir")
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

	err = d.mountHandle.Setstat(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, statUpdates)
	if nil != err {
		err = newFuseError(err)
	}

	return
}

func (d Dir) Lookup(ctx context.Context, name string) (fusefslib.Node, error) {
	enterGate()
	defer leaveGate()

	childInodeNumber, err := d.mountHandle.Lookup(inode.InodeRootUserID, inode.InodeGroupID(0), nil, d.inodeNumber, name)
	if err != nil {
		return nil, fuselib.ENOENT
	}

	isDir, err := d.mountHandle.IsDir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, childInodeNumber)
	if isDir {
		return Dir{mountHandle: d.mountHandle, inodeNumber: childInodeNumber}, nil
	} else if err != nil {
		err = newFuseError(err)
		return nil, err
	}

	isFile, err := d.mountHandle.IsFile(inode.InodeRootUserID, inode.InodeGroupID(0), nil, childInodeNumber)
	if isFile {
		return File{mountHandle: d.mountHandle, inodeNumber: childInodeNumber}, nil
	} else if err != nil {
		err = newFuseError(err)
		return nil, err
	}

	isSymlink, err := d.mountHandle.IsSymlink(inode.InodeRootUserID, inode.InodeGroupID(0), nil, childInodeNumber)
	if isSymlink {
		return Symlink{mountHandle: d.mountHandle, inodeNumber: childInodeNumber}, nil
	} else if err != nil {
		err = newFuseError(err)
		return nil, err
	}

	actualType, err := d.mountHandle.GetType(inode.InodeRootUserID, inode.InodeGroupID(0), nil, childInodeNumber)
	if err != nil {
		err = newFuseError(err)
		return nil, err
	} else {
		err = fmt.Errorf("Unrecognized inode type %v", actualType)
		err = blunder.AddError(err, blunder.InvalidInodeTypeError)
		err = newFuseError(err)
		return nil, err
	}
}

func inodeTypeToDirentType(inodeType inode.InodeType) fuselib.DirentType {
	switch inodeType {
	case inode.FileType:
		return fuselib.DT_File
	case inode.DirType:
		return fuselib.DT_Dir
	case inode.SymlinkType:
		return fuselib.DT_Link
	default:
		return fuselib.DT_Unknown
	}
}

func (d Dir) ReadDirAll(ctx context.Context) ([]fuselib.Dirent, error) {
	enterGate()
	defer leaveGate()

	entries := make([]inode.DirEntry, 0)
	entryCount := uint64(0)
	lastEntryName := ""

	more := true
	for more {
		var readEntries []inode.DirEntry
		var readCount uint64
		var err error

		readEntries, readCount, more, err = d.mountHandle.Readdir(inode.InodeRootUserID, inode.InodeGroupID(0), nil, d.inodeNumber, 1024, lastEntryName)
		if err != nil {
			logger.ErrorfWithError(err, "Error in ReadDirAll")
			return nil, fuselib.EIO
		}
		entries = append(entries, readEntries...)
		entryCount += readCount
		lastEntryName = readEntries[len(readEntries)-1].Basename
	}

	fuseEntries := make([]fuselib.Dirent, entryCount)

	for i, entry := range entries {
		inodeType, _ := d.mountHandle.GetType(inode.InodeRootUserID, inode.InodeGroupID(0), nil, entry.InodeNumber)
		fuseEntries[i] = fuselib.Dirent{
			Inode: uint64(entry.InodeNumber),
			Type:  inodeTypeToDirentType(inodeType),
			Name:  entry.Basename,
		}
	}
	return fuseEntries, nil
}

func (d Dir) Remove(ctx context.Context, req *fuselib.RemoveRequest) (err error) {
	enterGate()
	defer leaveGate()

	if req.Dir {
		err = d.mountHandle.Rmdir(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, req.Name)
	} else {
		err = d.mountHandle.Unlink(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, req.Name)
	}
	if nil != err {
		err = newFuseError(err)
	}
	return
}

func (d Dir) Mknod(ctx context.Context, req *fuselib.MknodRequest) (fusefslib.Node, error) {
	enterGate()
	defer leaveGate()

	// Note: NFSd apparently prefers to use Mknod() instead of Create() when creating normal files...
	if 0 != (inode.InodeMode(req.Mode) & ^inode.PosixModePerm) {
		err := fmt.Errorf("Invalid Mode... only normal file creations supported")
		err = blunder.AddError(err, blunder.InvalidInodeTypeError)
		err = newFuseError(err)
		return nil, err
	}
	inodeNumber, err := d.mountHandle.Create(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, req.Name, inode.InodeMode(req.Mode))
	if err != nil {
		err = newFuseError(err)
		return nil, err
	}
	file := File{mountHandle: d.mountHandle, inodeNumber: inodeNumber}
	return file, nil
}

func (d Dir) Create(ctx context.Context, req *fuselib.CreateRequest, resp *fuselib.CreateResponse) (fusefslib.Node, fusefslib.Handle, error) {
	enterGate()
	defer leaveGate()

	if 0 != (inode.InodeMode(req.Mode) & ^inode.PosixModePerm) {
		err := fmt.Errorf("Invalid Mode... only normal file creations supported")
		err = blunder.AddError(err, blunder.InvalidInodeTypeError)
		err = newFuseError(err)
		return nil, nil, err
	}
	inodeNumber, err := d.mountHandle.Create(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, req.Name, inode.InodeMode(req.Mode))
	if err != nil {
		err = newFuseError(err)
		return nil, nil, err
	}
	file := File{mountHandle: d.mountHandle, inodeNumber: inodeNumber}
	return file, file, nil
}

func (d Dir) Flush(ctx context.Context, req *fuselib.FlushRequest) error {
	enterGate()
	defer leaveGate()

	err := d.mountHandle.Flush(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber)
	if err != nil {
		err = newFuseError(err)
	}
	return err
}

func (d Dir) Fsync(ctx context.Context, req *fuselib.FsyncRequest) error {
	enterGate()
	defer leaveGate()

	err := d.mountHandle.Flush(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil,
		d.inodeNumber)
	if err != nil {
		err = newFuseError(err)
	}
	return err
}

func (d Dir) Mkdir(ctx context.Context, req *fuselib.MkdirRequest) (fusefslib.Node, error) {
	enterGate()
	defer leaveGate()

	trimmedMode := inode.InodeMode(req.Mode) & inode.PosixModePerm
	newDirInodeNumber, err := d.mountHandle.Mkdir(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, req.Name, trimmedMode)
	if err != nil {
		err = newFuseError(err)
		return nil, err
	}
	return Dir{mountHandle: d.mountHandle, inodeNumber: newDirInodeNumber}, nil
}

func (d Dir) Rename(ctx context.Context, req *fuselib.RenameRequest, newDir fusefslib.Node) error {
	enterGate()
	defer leaveGate()

	dstDir, ok := newDir.(Dir)
	if !ok {
		return fuselib.EIO
	}
	err := d.mountHandle.Rename(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, req.OldName, dstDir.inodeNumber, req.NewName)
	if err != nil {
		err = newFuseError(err)
	}
	return err
}

func (d Dir) Symlink(ctx context.Context, req *fuselib.SymlinkRequest) (fusefslib.Node, error) {
	enterGate()
	defer leaveGate()

	symlinkInodeNumber, err := d.mountHandle.Symlink(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, req.NewName, req.Target)
	if err != nil {
		err = newFuseError(err)
		return nil, err
	}

	return Symlink{mountHandle: d.mountHandle, inodeNumber: symlinkInodeNumber}, nil
}

func (d Dir) Link(ctx context.Context, req *fuselib.LinkRequest, old fusefslib.Node) (fusefslib.Node, error) {
	enterGate()
	defer leaveGate()

	oldFile, ok := old.(File)
	if !ok {
		err := fmt.Errorf("old.(File) failed")
		return nil, err
	}

	err := d.mountHandle.Link(inode.InodeUserID(req.Header.Uid), inode.InodeGroupID(req.Header.Gid), nil, d.inodeNumber, req.NewName, oldFile.inodeNumber)
	if err != nil {
		err = newFuseError(err)
	}

	return old, err
}
