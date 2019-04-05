package main

import (
	"io"
	"os"
	"os/exec"
	"path"
	"reflect"
	"syscall"
	"time"

	"bazil.org/fuse"

	"github.com/swiftstack/ProxyFS/inode"
	"github.com/swiftstack/ProxyFS/jrpcfs"
	"github.com/swiftstack/ProxyFS/utils"
)

const (
	maxUnmountRetryCount uint32 = 100
	unmountRetryGap             = 100 * time.Millisecond
)

func performMountFUSE() {
	var (
		curRetryCount                 uint32
		err                           error
		lazyUnmountCmd                *exec.Cmd
		mountPointContainingDirDevice int64
		mountPointDevice              int64
	)

	err = fuse.Unmount(globals.config.FUSEMountPointPath)
	if nil != err {
		logTracef("pre-fuse.Unmount() in performMount() returned: %v", err)
	}

	mountPointContainingDirDevice = fetchInodeDevice("path.Dir([Agent]FUSEMountPointPath", path.Dir(globals.config.FUSEMountPointPath))
	mountPointDevice = fetchInodeDevice("[Agent]FUSEMountPointPath", globals.config.FUSEMountPointPath)

	if mountPointDevice != mountPointContainingDirDevice {
		// Presumably, the mount point is (still) currently mounted, so attempt to unmount it first

		lazyUnmountCmd = exec.Command("fusermount", "-uz", globals.config.FUSEMountPointPath)
		err = lazyUnmountCmd.Run()
		if nil != err {
			logFatal(err)
		}

		curRetryCount = 0

		for mountPointDevice != mountPointContainingDirDevice {
			time.Sleep(unmountRetryGap) // Try again in a bit
			curRetryCount++
			if curRetryCount >= maxUnmountRetryCount {
				logFatalf("mountPointDevice != mountPointContainingDirDevice MaxRetryCount exceeded")
			}
			mountPointDevice = fetchInodeDevice("[Agent]FUSEMountPointPath", globals.config.FUSEMountPointPath)
		}
	}

	globals.fuseConn, err = fuse.Mount(
		globals.config.FUSEMountPointPath,
		fuse.AllowOther(),
		fuse.AsyncRead(),
		fuse.DefaultPermissions(), // so handleAccessRequest() should not be called
		fuse.ExclCreate(),
		fuse.FSName(globals.config.FUSEVolumeName),
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.ReadOnly(),
		fuse.Subtype("ProxyFS"),
		fuse.VolumeName(globals.config.FUSEVolumeName),
	)
	if nil != err {
		logFatal(err)
	}

	go serveFuse()

	<-globals.fuseConn.Ready
	if nil != globals.fuseConn.MountError {
		logFatal(globals.fuseConn.MountError)
	}

	logInfof("Now serving %s on %s", globals.config.FUSEVolumeName, globals.config.FUSEMountPointPath)
}

func fetchInodeDevice(pathTitle string, path string) (inodeDevice int64) {
	var (
		err  error
		fi   os.FileInfo
		ok   bool
		stat *syscall.Stat_t
	)

	fi, err = os.Stat(path)
	if nil != err {
		if os.IsNotExist(err) {
			logFatalf("%s path (%s) not found", pathTitle, path)
		} else {
			logFatalf("%s path (%s) os.Stat() failed: %v", pathTitle, path, err)
		}
	}
	if nil == fi.Sys() {
		logFatalf("%s path (%s) had empty os.Stat()", pathTitle, path)
	}
	stat, ok = fi.Sys().(*syscall.Stat_t)
	if !ok {
		logFatalf("%s path (%s) fi.Sys().(*syscall.Stat_t) returned !ok", pathTitle, path)
	}

	inodeDevice = int64(stat.Dev)

	return
}

func performUnmountFUSE() {
	var (
		err error
	)

	err = fuse.Unmount(globals.config.FUSEMountPointPath)
	if nil != err {
		logFatal(err)
	}

	logInfof("%s unmounted", globals.config.FUSEMountPointPath)
}

func serveFuse() {
	var (
		err     error
		request fuse.Request
	)

	for {
		// Fetch next *fuse.Request... exiting on fuseConn error

		request, err = globals.fuseConn.ReadRequest()
		if nil != err {
			if io.EOF == err {
				logTracef("exiting serveFuse() due to io.EOF")
				return
			}
			logErrorf("serveFuse() exiting due to err: %v", err)
			return
		}
		logTracef("serveFuse() got %v", reflect.ValueOf(request).Type())
		switch request.(type) {
		case *fuse.AccessRequest:
			handleAccessRequest(request.(*fuse.AccessRequest))
		case *fuse.CreateRequest:
			handleCreateRequest(request.(*fuse.CreateRequest))
		case *fuse.DestroyRequest:
			handleDestroyRequest(request.(*fuse.DestroyRequest))
		case *fuse.ExchangeDataRequest:
			handleExchangeDataRequest(request.(*fuse.ExchangeDataRequest))
		case *fuse.FlushRequest:
			handleFlushRequest(request.(*fuse.FlushRequest))
		case *fuse.ForgetRequest:
			handleForgetRequest(request.(*fuse.ForgetRequest))
		case *fuse.FsyncRequest:
			handleFsyncRequest(request.(*fuse.FsyncRequest))
		case *fuse.GetattrRequest:
			handleGetattrRequest(request.(*fuse.GetattrRequest))
		case *fuse.GetxattrRequest:
			handleGetxattrRequest(request.(*fuse.GetxattrRequest))
		case *fuse.InitRequest:
			handleInitRequest(request.(*fuse.InitRequest))
		case *fuse.InterruptRequest:
			handleInterruptRequest(request.(*fuse.InterruptRequest))
		case *fuse.LinkRequest:
			handleLinkRequest(request.(*fuse.LinkRequest))
		case *fuse.ListxattrRequest:
			handleListxattrRequest(request.(*fuse.ListxattrRequest))
		case *fuse.LookupRequest:
			handleLookupRequest(request.(*fuse.LookupRequest))
		case *fuse.MkdirRequest:
			handleMkdirRequest(request.(*fuse.MkdirRequest))
		case *fuse.MknodRequest:
			handleMknodRequest(request.(*fuse.MknodRequest))
		case *fuse.OpenRequest:
			handleOpenRequest(request.(*fuse.OpenRequest))
		case *fuse.ReadRequest:
			handleReadRequest(request.(*fuse.ReadRequest))
		case *fuse.ReadlinkRequest:
			handleReadlinkRequest(request.(*fuse.ReadlinkRequest))
		case *fuse.ReleaseRequest:
			handleReleaseRequest(request.(*fuse.ReleaseRequest))
		case *fuse.RemoveRequest:
			handleRemoveRequest(request.(*fuse.RemoveRequest))
		case *fuse.RemovexattrRequest:
			handleRemovexattrRequest(request.(*fuse.RemovexattrRequest))
		case *fuse.RenameRequest:
			handleRenameRequest(request.(*fuse.RenameRequest))
		case *fuse.SetattrRequest:
			handleSetattrRequest(request.(*fuse.SetattrRequest))
		case *fuse.SetxattrRequest:
			handleSetxattrRequest(request.(*fuse.SetxattrRequest))
		case *fuse.StatfsRequest:
			handleStatfsRequest(request.(*fuse.StatfsRequest))
		case *fuse.SymlinkRequest:
			handleSymlinkRequest(request.(*fuse.SymlinkRequest))
		case *fuse.WriteRequest:
			handleWriteRequest(request.(*fuse.WriteRequest))
		default:
			logWarnf("received unserviced %v", reflect.ValueOf(request).Type())
			request.RespondError(fuse.ENOTSUP)
		}
	}
}

func handleAccessRequest(request *fuse.AccessRequest) {
	logFatalf("handleAccessRequest() should not have been called due to DefaultPermissions() passed to fuse.Mount()")
}

func handleCreateRequest(request *fuse.CreateRequest) {
	logInfof("TODO: handleCreateRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleDestroyRequest(request *fuse.DestroyRequest) {
	logInfof("TODO: handleDestroyRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleExchangeDataRequest(request *fuse.ExchangeDataRequest) {
	logInfof("TODO: handleExchangeDataRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleFlushRequest(request *fuse.FlushRequest) {
	logInfof("TODO: handleFlushRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleForgetRequest(request *fuse.ForgetRequest) {
	logInfof("TODO: handleForgetRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleFsyncRequest(request *fuse.FsyncRequest) {
	logInfof("TODO: handleFsyncRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleGetattrRequest(request *fuse.GetattrRequest) {
	var (
		err            error
		mode           os.FileMode
		getStatRequest *jrpcfs.GetStatRequest
		response       *fuse.GetattrResponse
		statStruct     *jrpcfs.StatStruct
	)

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
	}

	statStruct = &jrpcfs.StatStruct{}

	err = doJRPCRequest("Server.RpcGetStat", getStatRequest, statStruct)
	if nil != err {
		request.RespondError(err)
	}

	switch inode.InodeMode(statStruct.FileMode) & inode.PosixModeType {
	case inode.PosixModeDir:
		mode = os.ModeDir | os.FileMode(statStruct.FileMode&uint32(inode.PosixModeType))
	case inode.PosixModeFile:
		mode = os.FileMode(statStruct.FileMode & uint32(inode.PosixModeType))
	case inode.PosixModeSymlink:
		mode = os.ModeSymlink | os.FileMode(statStruct.FileMode&uint32(inode.PosixModeType))
	default:
	}

	response = &fuse.GetattrResponse{
		Attr: fuse.Attr{
			Valid:     globals.config.AttrDuration,
			Inode:     uint64(request.Header.Node),
			Size:      statStruct.Size,
			Blocks:    statStruct.Size / globals.config.AttrBlockSize,
			Atime:     time.Unix(0, int64(statStruct.ATimeNs)),
			Mtime:     time.Unix(0, int64(statStruct.MTimeNs)),
			Ctime:     time.Unix(0, int64(statStruct.CTimeNs)),
			Crtime:    time.Unix(0, int64(statStruct.CRTimeNs)),
			Mode:      mode,
			Nlink:     uint32(statStruct.NumLinks),
			Uid:       statStruct.UserID,
			Gid:       statStruct.GroupID,
			Rdev:      uint32(0),
			Flags:     uint32(0),
			BlockSize: uint32(globals.config.AttrBlockSize),
		},
	}

	request.Respond(response)
}

func handleGetxattrRequest(request *fuse.GetxattrRequest) {
	logInfof("TODO: handleGetxattrRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleInitRequest(request *fuse.InitRequest) {
	logFatalf("handleInitRequest() should not have been called... fuse.Mount() supposedly took care of it")
}

func handleInterruptRequest(request *fuse.InterruptRequest) {
	logInfof("TODO: handleInterruptRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleLinkRequest(request *fuse.LinkRequest) {
	logInfof("TODO: handleLinkRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleListxattrRequest(request *fuse.ListxattrRequest) {
	logInfof("TODO: handleListxattrRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleLookupRequest(request *fuse.LookupRequest) {
	logInfof("TODO: handleLookupRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleMkdirRequest(request *fuse.MkdirRequest) {
	logInfof("TODO: handleMkdirRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleMknodRequest(request *fuse.MknodRequest) {
	logInfof("TODO: handleMknodRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleOpenRequest(request *fuse.OpenRequest) {
	logInfof("TODO: handleOpenRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleReadRequest(request *fuse.ReadRequest) {
	logInfof("TODO: handleReadRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleReadlinkRequest(request *fuse.ReadlinkRequest) {
	logInfof("TODO: handleReadlinkRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleReleaseRequest(request *fuse.ReleaseRequest) {
	logInfof("TODO: handleReleaseRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleRemoveRequest(request *fuse.RemoveRequest) {
	logInfof("TODO: handleRemoveRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleRemovexattrRequest(request *fuse.RemovexattrRequest) {
	logInfof("TODO: handleRemovexattrRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleRenameRequest(request *fuse.RenameRequest) {
	logInfof("TODO: handleRenameRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleSetattrRequest(request *fuse.SetattrRequest) {
	logInfof("TODO: handleSetattrRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleSetxattrRequest(request *fuse.SetxattrRequest) {
	logInfof("TODO: handleSetxattrRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleStatfsRequest(request *fuse.StatfsRequest) {
	var (
		err            error
		response       *fuse.StatfsResponse
		statVFS        *jrpcfs.StatVFS
		statVFSRequest *jrpcfs.StatVFSRequest
	)

	statVFSRequest = &jrpcfs.StatVFSRequest{
		MountID: globals.mountID,
	}

	statVFS = &jrpcfs.StatVFS{}

	err = doJRPCRequest("Server.RpcStatVFS", statVFSRequest, statVFS)
	if nil != err {
		request.RespondError(err)
	}

	response = &fuse.StatfsResponse{
		Blocks:  statVFS.TotalBlocks,
		Bfree:   statVFS.FreeBlocks,
		Bavail:  statVFS.AvailBlocks,
		Files:   statVFS.TotalInodes,
		Ffree:   statVFS.FreeInodes,
		Bsize:   uint32(statVFS.BlockSize),
		Namelen: uint32(statVFS.MaxFilenameLen),
		Frsize:  uint32(statVFS.FragmentSize),
	}

	request.Respond(response)
}

func handleSymlinkRequest(request *fuse.SymlinkRequest) {
	logInfof("TODO: handleSymlinkRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

func handleWriteRequest(request *fuse.WriteRequest) {
	logInfof("TODO: handleWriteRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}
