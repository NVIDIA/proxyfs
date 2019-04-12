package main

// The following implements the Low Level FUSE upcalls for presenting a ProxyFS Volume locally.
//
// Useful URLs for understanding Bazil FUSE APIs and data structures:
//
//   https://gowalker.org/bazil.org/fuse          - specific to Bazil FUSE
//   https://docs.racket-lang.org/fuse/index.html - generic to FUSE
//
// Bazil FUSE source provides insite into the mapping from FUSE ops to Request structs:
//
//   vendor/bazil.org/fuse/fuse_kernel.go         - FUSE op names
//   vendor/bazil.org/fuse/fuse.go                - mapping from FUSE ops to Request structs
//
// JSON RPCs are implemented in packages fs, inode, & jrpcfs:
//
//   fs/api.go
//   inode/api.go
//   jrpcfs/api.go

import (
	"io"
	"os"
	"os/exec"
	"path"
	"reflect"
	"syscall"
	"time"

	"bazil.org/fuse"

	"github.com/swiftstack/ProxyFS/fs"
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

	if globals.config.ReadOnly {
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
	} else {
		globals.fuseConn, err = fuse.Mount(
			globals.config.FUSEMountPointPath,
			fuse.AllowOther(),
			fuse.AsyncRead(),
			fuse.DefaultPermissions(), // so handleAccessRequest() should not be called
			fuse.ExclCreate(),
			fuse.FSName(globals.config.FUSEVolumeName),
			fuse.NoAppleDouble(),
			fuse.NoAppleXattr(),
			fuse.Subtype("ProxyFS"),
			fuse.VolumeName(globals.config.FUSEVolumeName),
		)
	}
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
			// Bazil FUSE punted the not-understood opCode... so just reject it
			request.RespondError(fuse.ENOTSUP)
		}
	}
}

func handleAccessRequest(request *fuse.AccessRequest) {
	logFatalf("handleAccessRequest() should not have been called due to DefaultPermissions() passed to fuse.Mount()")
}

func handleCreateRequest(request *fuse.CreateRequest) {
	var (
		embeddedLookupResponse *fuse.LookupResponse
		embeddedOpenResponse   *fuse.OpenResponse
		err                    error
		createReply            *jrpcfs.Reply
		createRequest          *jrpcfs.CreateRequest
		response               *fuse.CreateResponse
	)

	createRequest = &jrpcfs.CreateRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
		Basename: request.Name,
		UserID:   int32(request.Uid),
		GroupID:  int32(request.Gid),
		FileMode: uint32(request.Mode & os.ModePerm),
	}

	createReply = &jrpcfs.Reply{}

	err = doJRPCRequest("Server.RpcCreate", createRequest, createReply)
	if nil != err {
		request.RespondError(err)
		return
	}

	embeddedLookupResponse, err = lookupRequestHelper(request.Node, request.Name)
	if nil != err {
		request.RespondError(err)
		return
	}

	embeddedOpenResponse = openRequestHelper(embeddedLookupResponse.Node, false)

	response = &fuse.CreateResponse{
		LookupResponse: *embeddedLookupResponse,
		OpenResponse:   *embeddedOpenResponse,
	}

	request.Respond(response)
}

// handleDestroyRequest is called just before an unmount. While we could enforce
// that no subsequent upcalls will be made, this is not necessary.
//
func handleDestroyRequest(request *fuse.DestroyRequest) {
	request.Respond()
}

func handleExchangeDataRequest(request *fuse.ExchangeDataRequest) {
	logInfof("TODO: handleExchangeDataRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}

// handleFlushRequest is called to sync/flush a previously opened FileInode or
// DirInode at time of close. This is not to be confused with handleFsyncRequest
// that is triggered at any time prior to close.
//
func handleFlushRequest(request *fuse.FlushRequest) {
	// TODO: For FileInode's, trigger necessary flushes
	request.Respond()
}

// handleForgetRequest provides a "hint" to no longer cache info about an Inode.
// As the InodeCache is managed with its own eviction logic, this becomes a no-op.
//
func handleForgetRequest(request *fuse.ForgetRequest) {
	request.Respond()
}

// handleFsyncRequest is called to sync/flush a previously opened FileInode or
// DirInode. This is not to be confused with handleFlushRequest that is actually
// the operation performced at time of close.
//
func handleFsyncRequest(request *fuse.FsyncRequest) {
	// TODO: For FileInode's, trigger necessary flushes
	request.Respond()
}

func getattrRequestHelper(node fuse.NodeID) (attr *fuse.Attr, err error) {
	var (
		mode           os.FileMode
		getStatRequest *jrpcfs.GetStatRequest
		statStruct     *jrpcfs.StatStruct
	)

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(node), // TODO: Check if SnapShot's work with this
		},
	}

	statStruct = &jrpcfs.StatStruct{}

	err = doJRPCRequest("Server.RpcGetStat", getStatRequest, statStruct)
	if nil != err {
		return
	}

	switch inode.InodeMode(statStruct.FileMode) & inode.PosixModeType {
	case inode.PosixModeDir:
		mode = os.ModeDir | os.FileMode(statStruct.FileMode&uint32(inode.PosixModePerm))
	case inode.PosixModeFile:
		mode = os.FileMode(statStruct.FileMode & uint32(inode.PosixModePerm))
	case inode.PosixModeSymlink:
		mode = os.ModeSymlink | os.FileMode(statStruct.FileMode&uint32(inode.PosixModePerm))
	default:
		logFatalf("Server.RpcGetStat returned unrecognized inode.InodeMode: 0x%08X", statStruct.FileMode)
	}

	attr = &fuse.Attr{
		Valid:     globals.config.AttrDuration,
		Inode:     uint64(node), // TODO: Check if SnapShot's work with this
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
	}

	return
}

func handleGetattrRequest(request *fuse.GetattrRequest) {
	var (
		attr     *fuse.Attr
		err      error
		response *fuse.GetattrResponse
	)

	attr, err = getattrRequestHelper(request.Header.Node)
	if nil != err {
		request.RespondError(fuse.ENOENT)
		return
	}

	response = &fuse.GetattrResponse{
		Attr: *attr,
	}

	request.Respond(response)
}

func handleGetxattrRequest(request *fuse.GetxattrRequest) {
	var (
		err             error
		getXAttrReply   *jrpcfs.GetXAttrReply
		getXAttrRequest *jrpcfs.GetXAttrRequest
		response        *fuse.GetxattrResponse
	)

	if (0 == request.Size) && (0 != request.Position) {
		request.RespondError(fuse.ERANGE)
		return
	}

	getXAttrRequest = &jrpcfs.GetXAttrRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
		AttrName: request.Name,
	}

	getXAttrReply = &jrpcfs.GetXAttrReply{}

	err = doJRPCRequest("Server.RpcGetXAttr", getXAttrRequest, getXAttrReply)
	if nil != err {
		request.RespondError(fuseMissingXAttrErrno)
		return
	}

	if 0 == request.Size {
		response = &fuse.GetxattrResponse{
			Xattr: getXAttrReply.AttrValue,
		}
	} else {
		if int(request.Position) >= len(getXAttrReply.AttrValue) {
			response = &fuse.GetxattrResponse{
				Xattr: make([]byte, 0),
			}
		} else if int(request.Position+request.Size) < len(getXAttrReply.AttrValue) {
			response = &fuse.GetxattrResponse{
				Xattr: getXAttrReply.AttrValue[request.Position:(request.Position + request.Size)],
			}
		} else {
			response = &fuse.GetxattrResponse{
				Xattr: getXAttrReply.AttrValue[request.Position:],
			}
		}
	}

	request.Respond(response)
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
	var (
		err         error
		linkReply   *jrpcfs.Reply
		linkRequest *jrpcfs.LinkRequest
		response    *fuse.LookupResponse
	)

	linkRequest = &jrpcfs.LinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
		Basename:          request.NewName,
		TargetInodeNumber: int64(request.OldNode), // TODO: Check if SnapShot's work with this
	}

	linkReply = &jrpcfs.Reply{}

	err = doJRPCRequest("Server.RpcLink", linkRequest, linkReply)
	if nil != err {
		request.RespondError(err)
		return
	}

	response, err = lookupRequestHelper(request.Node, request.NewName)
	if nil != err {
		request.RespondError(err)
		return
	}

	request.Respond(response)
}

// handleListxattrRequest makes the assumption that the Position parameter
// refers to the byte offset from the beginning of the first byte of the
// first attrName and that each attrName is followed by an ASCII NULL.
// As a consequence, it seems reasonable that attrName's are likely all
// ASCII characters. Alas, this does not explain the meaning of a value
// for Position that does not land at the beginning of an attrName.
//
func handleListxattrRequest(request *fuse.ListxattrRequest) {
	var (
		attrName         string
		attrNamesBuf     []byte
		err              error
		listXAttrReply   *jrpcfs.ListXAttrReply
		listXAttrRequest *jrpcfs.ListXAttrRequest
		response         *fuse.ListxattrResponse
	)

	if (0 == request.Size) && (0 != request.Position) {
		request.RespondError(fuse.ERANGE)
		return
	}

	listXAttrRequest = &jrpcfs.ListXAttrRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
	}

	listXAttrReply = &jrpcfs.ListXAttrReply{}

	err = doJRPCRequest("Server.RpcListXAttr", listXAttrRequest, listXAttrReply)

	if nil == err {
		attrNamesBuf = make([]byte, 0)

		for _, attrName = range listXAttrReply.AttrNames {
			attrNamesBuf = append(attrNamesBuf, []byte(attrName)...)
			attrNamesBuf = append(attrNamesBuf, byte(0))
		}

		if 0 == request.Size {
			response = &fuse.ListxattrResponse{
				Xattr: attrNamesBuf,
			}
		} else {
			if int(request.Position) >= len(attrNamesBuf) {
				response = &fuse.ListxattrResponse{
					Xattr: make([]byte, 0),
				}
			} else if int(request.Position+request.Size) < len(attrNamesBuf) {
				response = &fuse.ListxattrResponse{
					Xattr: attrNamesBuf[request.Position:(request.Position + request.Size)],
				}
			} else {
				response = &fuse.ListxattrResponse{
					Xattr: attrNamesBuf[request.Position:],
				}
			}
		}
	} else {
		response = &fuse.ListxattrResponse{
			Xattr: make([]byte, 0),
		}
	}

	request.Respond(response)
}

func lookupRequestHelper(node fuse.NodeID, name string) (response *fuse.LookupResponse, err error) {
	var (
		inodeReply     *jrpcfs.InodeReply
		getStatRequest *jrpcfs.GetStatRequest
		lookupRequest  *jrpcfs.LookupRequest
		mode           os.FileMode
		statStruct     *jrpcfs.StatStruct
	)

	lookupRequest = &jrpcfs.LookupRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(node), // TODO: Check if SnapShot's work with this
		},
		Basename: name,
	}

	inodeReply = &jrpcfs.InodeReply{}

	err = doJRPCRequest("Server.RpcLookup", lookupRequest, inodeReply)
	if nil != err {
		return
	}

	getStatRequest = &jrpcfs.GetStatRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: inodeReply.InodeNumber,
		},
	}

	statStruct = &jrpcfs.StatStruct{}

	err = doJRPCRequest("Server.RpcGetStat", getStatRequest, statStruct)
	if nil != err {
		return
	}

	switch inode.InodeMode(statStruct.FileMode) & inode.PosixModeType {
	case inode.PosixModeDir:
		mode = os.ModeDir | os.FileMode(statStruct.FileMode&uint32(inode.PosixModePerm))
	case inode.PosixModeFile:
		mode = os.FileMode(statStruct.FileMode & uint32(inode.PosixModePerm))
	case inode.PosixModeSymlink:
		mode = os.ModeSymlink | os.FileMode(statStruct.FileMode&uint32(inode.PosixModePerm))
	default:
		logFatalf("Server.RpcGetStat returned unrecognized inode.InodeMode: 0x%08X", statStruct.FileMode)
	}

	response = &fuse.LookupResponse{
		Node:       fuse.NodeID(inodeReply.InodeNumber), // TODO: Check if SnapShot's work with this
		Generation: 0,
		EntryValid: globals.config.LookupEntryDuration,
		Attr: fuse.Attr{
			Valid:     globals.config.AttrDuration,
			Inode:     uint64(inodeReply.InodeNumber), // TODO: Check if SnapShot's work with this
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

	return
}

func handleLookupRequest(request *fuse.LookupRequest) {
	var (
		err      error
		response *fuse.LookupResponse
	)

	response, err = lookupRequestHelper(request.Node, request.Name)
	if nil != err {
		request.RespondError(fuse.ENOENT)
		return
	}

	request.Respond(response)
}

func handleMkdirRequest(request *fuse.MkdirRequest) {
	var (
		embeddedLookupResponse *fuse.LookupResponse
		err                    error
		mkdirReply             *jrpcfs.Reply
		mkdirRequest           *jrpcfs.MkdirRequest
		response               *fuse.MkdirResponse
	)

	mkdirRequest = &jrpcfs.MkdirRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
		Basename: request.Name,
		UserID:   int32(request.Uid),
		GroupID:  int32(request.Gid),
		FileMode: uint32(request.Mode & os.ModePerm),
	}

	mkdirReply = &jrpcfs.Reply{}

	err = doJRPCRequest("Server.RpcMkdir", mkdirRequest, mkdirReply)
	if nil != err {
		request.RespondError(err)
		return
	}

	embeddedLookupResponse, err = lookupRequestHelper(request.Node, request.Name)
	if nil != err {
		request.RespondError(err)
		return
	}

	response = &fuse.MkdirResponse{
		LookupResponse: *embeddedLookupResponse,
	}

	request.Respond(response)
}

func handleMknodRequest(request *fuse.MknodRequest) {
	request.RespondError(fuse.ENOTSUP)
}

func openRequestHelper(node fuse.NodeID, dir bool) (response *fuse.OpenResponse) {
	var (
		flags    fuse.OpenResponseFlags
		handleID fuse.HandleID
	)

	globals.Lock()

	handleID = globals.lastHandleID + 1
	globals.lastHandleID = handleID

	globals.handleTable[handleID] = &handleStruct{
		InodeNumber:        inode.InodeNumber(node),
		prevDirEntLocation: -1,
	}

	globals.Unlock()

	if dir {
		flags = fuse.OpenResponseFlags(0)
	} else {
		flags = fuse.OpenDirectIO
	}

	response = &fuse.OpenResponse{
		Handle: handleID,
		Flags:  flags,
	}

	return
}

func handleOpenRequest(request *fuse.OpenRequest) {
	var (
		response *fuse.OpenResponse
	)

	response = openRequestHelper(request.Node, request.Dir)

	request.Respond(response)
}

func handleReadRequest(request *fuse.ReadRequest) {
	var (
		dirent                      fuse.Dirent
		direntType                  fuse.DirentType
		dirEntry                    jrpcfs.DirEntry
		err                         error
		handle                      *handleStruct
		ok                          bool
		readdirByLocRequest         *jrpcfs.ReaddirByLocRequest
		readdirReply                *jrpcfs.ReaddirReply
		response                    *fuse.ReadResponse
		responseDataLenBeforeAppend int
	)

	if request.Dir {
		handle, ok = globals.handleTable[request.Handle]
		if !ok {
			request.RespondError(fuse.ESTALE)
			return
		}

		if 0 == request.Offset {
			handle.prevDirEntLocation = -1
		}

		readdirByLocRequest = &jrpcfs.ReaddirByLocRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
			},
			MaxEntries:         globals.config.ReaddirMaxEntries,
			PrevDirEntLocation: handle.prevDirEntLocation,
		}

		readdirReply = &jrpcfs.ReaddirReply{}

		err = doJRPCRequest("Server.RpcReaddirByLoc", readdirByLocRequest, readdirReply)
		if nil != err {
			request.RespondError(err)
			return
		}

		response = &fuse.ReadResponse{
			Data: make([]byte, 0, request.Size),
		}

		for _, dirEntry = range readdirReply.DirEnts {
			switch inode.InodeType(dirEntry.FileType) {
			case inode.DirType:
				direntType = fuse.DT_Dir
			case inode.FileType:
				direntType = fuse.DT_File
			case inode.SymlinkType:
				direntType = fuse.DT_Link
			default:
				direntType = fuse.DT_Unknown
			}
			dirent.Inode = uint64(dirEntry.InodeNumber) // TODO: Check if SnapShot's work with this
			dirent.Type = direntType
			dirent.Name = dirEntry.Basename

			responseDataLenBeforeAppend = len(response.Data)

			response.Data = fuse.AppendDirent(response.Data, dirent)
			if len(response.Data) > request.Size {
				response.Data = response.Data[:responseDataLenBeforeAppend]
				break
			}

			handle.prevDirEntLocation++
		}

		request.Respond(response)
	} else {
		logInfof("TODO: handleReadRequest() for FileInode")
		logInfof("Header:\n%s", utils.JSONify(request.Header, true))
		logInfof("Payload\n%s", utils.JSONify(request, true))
		logInfof("Responding with fuse.ENOTSUP")
		request.RespondError(fuse.ENOTSUP)
	}
}

func handleReadlinkRequest(request *fuse.ReadlinkRequest) {
	var (
		err                error
		readSymlinkReply   *jrpcfs.ReadSymlinkReply
		readSymlinkRequest *jrpcfs.ReadSymlinkRequest
	)

	readSymlinkRequest = &jrpcfs.ReadSymlinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
	}

	readSymlinkReply = &jrpcfs.ReadSymlinkReply{}

	err = doJRPCRequest("Server.RpcReadSymlink", readSymlinkRequest, readSymlinkReply)
	if nil != err {
		request.RespondError(err)
		return
	}

	request.Respond(readSymlinkReply.Target)
}

func handleReleaseRequest(request *fuse.ReleaseRequest) {
	if !request.Dir && (0 != (request.ReleaseFlags & fuse.ReleaseFlush)) {
		// TODO: perform Flush on FileInode if necessary
	}

	globals.Lock()

	delete(globals.handleTable, request.Handle)

	globals.Unlock()

	request.Respond()
}

func handleRemoveRequest(request *fuse.RemoveRequest) {
	var (
		err           error
		unlinkReply   *jrpcfs.Reply
		unlinkRequest *jrpcfs.UnlinkRequest
	)

	unlinkRequest = &jrpcfs.UnlinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
		Basename: request.Name,
	}

	unlinkReply = &jrpcfs.Reply{}

	if request.Dir {
		err = doJRPCRequest("Server.RpcRmdir", unlinkRequest, unlinkReply)
	} else {
		err = doJRPCRequest("Server.RpcUnlink", unlinkRequest, unlinkReply)
	}

	if nil != err {
		request.RespondError(err)
		return
	}

	request.Respond()
}

func handleRemovexattrRequest(request *fuse.RemovexattrRequest) {
	var (
		err                error
		removeXAttrReply   *jrpcfs.Reply
		removeXAttrRequest *jrpcfs.RemoveXAttrRequest
	)

	removeXAttrRequest = &jrpcfs.RemoveXAttrRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
		AttrName: request.Name,
	}

	removeXAttrReply = &jrpcfs.Reply{}

	err = doJRPCRequest("Server.RpcRemoveXAttr", removeXAttrRequest, removeXAttrReply)
	if nil != err {
		request.RespondError(fuseMissingXAttrErrno)
		return
	}

	request.Respond()
}

func handleRenameRequest(request *fuse.RenameRequest) {
	var (
		err           error
		renameReply   *jrpcfs.Reply
		renameRequest *jrpcfs.RenameRequest
	)

	renameRequest = &jrpcfs.RenameRequest{
		MountID:           globals.mountID,
		SrcDirInodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		SrcBasename:       request.OldName,
		DstDirInodeNumber: int64(request.NewDir), // TODO: Check if SnapShot's work with this
		DstBasename:       request.NewName,
	}

	renameReply = &jrpcfs.Reply{}

	err = doJRPCRequest("Server.RpcRename", renameRequest, renameReply)
	if nil != err {
		request.RespondError(err)
		return
	}

	request.Respond()
}

func handleSetattrRequest(request *fuse.SetattrRequest) {
	var (
		attr                 *fuse.Attr
		chmodReply           *jrpcfs.Reply
		chmodRequest         *jrpcfs.ChmodRequest
		chownReply           *jrpcfs.Reply
		chownRequest         *jrpcfs.ChownRequest
		err                  error
		newAtime             *time.Time
		newMtime             *time.Time
		resizeReply          *jrpcfs.Reply
		resizeRequest        *jrpcfs.ResizeRequest
		response             *fuse.SetattrResponse
		setTimeReply         *jrpcfs.Reply
		setTimeRequest       *jrpcfs.SetTimeRequest
		settingAtimeAndMtime bool
		settingGid           bool
		settingMode          bool
		settingSize          bool
		settingUid           bool
		timeNow              time.Time
	)

	if (request.Valid & (fuse.SetattrMode | fuse.SetattrUid | fuse.SetattrGid | fuse.SetattrSize | fuse.SetattrAtime | fuse.SetattrMtime | fuse.SetattrAtimeNow | fuse.SetattrMtimeNow)) != request.Valid {
		// request.Valid contains non-supported bits
		request.RespondError(fuse.ENOTSUP)
		return
	}
	if (0 == (request.Valid & fuse.SetattrAtime)) && (0 != (request.Valid & fuse.SetattrAtimeNow)) {
		// request.Valid cannot contain SetattrAtimeNow but not SetattrAtime
		request.RespondError(fuse.ENOTSUP)
		return
	}
	if (0 == (request.Valid & fuse.SetattrMtime)) && (0 != (request.Valid & fuse.SetattrMtimeNow)) {
		// request.Valid cannot contain SetattrMtimeNow but not SetattrMtime
		request.RespondError(fuse.ENOTSUP)
		return
	}
	if (0 != (request.Valid & fuse.SetattrAtime)) &&
		(0 == (request.Valid & fuse.SetattrMtime)) {
		// request.Valid contains SetattrAtime{|Now} but not SetattrMtime{|Now}
		request.RespondError(fuse.ENOTSUP)
		return
	}
	if (0 == (request.Valid & fuse.SetattrAtime)) &&
		(0 != (request.Valid & fuse.SetattrMtime)) {
		// request.Valid contains SetattrMtime but not SetattrAtime
		request.RespondError(fuse.ENOTSUP)
		return
	}

	settingMode = (0 != (request.Valid & fuse.SetattrMode))

	settingUid = (0 != (request.Valid & fuse.SetattrUid))
	settingGid = (0 != (request.Valid & fuse.SetattrGid))

	settingSize = (0 != (request.Valid & fuse.SetattrSize))

	// Note: we already know we'd get the same result if we included SetattrMtime
	settingAtimeAndMtime = (0 != (request.Valid & fuse.SetattrAtime))

	if settingMode {
		if request.Mode != (request.Mode & os.FileMode(inode.PosixModePerm)) {
			request.RespondError(fuse.ENOTSUP)
			return
		}

		chmodRequest = &jrpcfs.ChmodRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
			},
			FileMode: uint32(request.Mode),
		}

		chmodReply = &jrpcfs.Reply{}

		err = doJRPCRequest("Server.RpcChmod", chmodRequest, chmodReply)
		if nil != err {
			request.RespondError(err)
			return
		}
	}

	if settingUid || settingGid {
		chownRequest = &jrpcfs.ChownRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
			},
		}

		if settingUid {
			chownRequest.UserID = int32(request.Uid)
		} else {
			chownRequest.UserID = -1
		}

		if settingGid {
			chownRequest.GroupID = int32(request.Gid)
		} else {
			chownRequest.GroupID = -1
		}

		chownReply = &jrpcfs.Reply{}

		err = doJRPCRequest("Server.RpcChown", chownRequest, chownReply)
		if nil != err {
			request.RespondError(err)
			return
		}
	}

	if settingSize {
		resizeRequest = &jrpcfs.ResizeRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
			},
			NewSize: request.Size,
		}

		resizeReply = &jrpcfs.Reply{}

		err = doJRPCRequest("Server.Resize", resizeRequest, resizeReply)
		if nil != err {
			request.RespondError(err)
			return
		}
	}

	if settingAtimeAndMtime {
		timeNow = time.Now()

		newAtime = nil
		newMtime = nil

		if 0 != (request.Valid & fuse.SetattrAtimeNow) {
			newAtime = &timeNow
		} else {
			newAtime = &request.Atime
		}

		if 0 != (request.Valid & fuse.SetattrMtimeNow) {
			newMtime = &timeNow
		} else {
			newMtime = &request.Mtime
		}

		setTimeRequest = &jrpcfs.SetTimeRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
			},
			StatStruct: jrpcfs.StatStruct{
				MTimeNs: uint64(newMtime.UnixNano()),
				ATimeNs: uint64(newAtime.UnixNano()),
			},
		}

		setTimeReply = &jrpcfs.Reply{}

		err = doJRPCRequest("Server.RpcSetTime", setTimeRequest, setTimeReply)
		if nil != err {
			request.RespondError(err)
			return
		}
	}

	attr, err = getattrRequestHelper(request.Header.Node)
	if nil != err {
		request.RespondError(err)
		return
	}

	response = &fuse.SetattrResponse{
		Attr: *attr,
	}

	request.Respond(response)
}

// handleSetxattrRequest supports creating and modifying an existing Extended Attribute.
// This is a bit limited w.r.t. what FUSE allows, but Bazil FUSE has left support for
// either requiring the Extended Attribute to previously not exist or to entirely replace
// it. In any case, due to the support for modifying an existing Extended Attribute, this
// func performs a Read-Modify-Write sequence. Note, also, that holes created from e.g.
// out-of-order Extended Attribute fragment writes are written as zeroes. Finally, as there
// is no way to discern between a modification starting at Position 0 and an entirely new
// value, we will adopt the convention that a Position 0 request is explicitly a new
// value (and, hence, avoid the Read-Modify-Write sequence).
//
func handleSetxattrRequest(request *fuse.SetxattrRequest) {
	var (
		attrValue             []byte
		err                   error
		getXAttrReply         *jrpcfs.GetXAttrReply
		getXAttrRequest       *jrpcfs.GetXAttrRequest
		paddingNeeded         int
		setXAttrReply         *jrpcfs.Reply
		setXAttrRequest       *jrpcfs.SetXAttrRequest
		xattrReplacementByte  byte
		xattrReplacementIndex int
	)

	if 0 == request.Position {
		attrValue = request.Xattr
	} else {
		getXAttrRequest = &jrpcfs.GetXAttrRequest{
			InodeHandle: jrpcfs.InodeHandle{
				MountID:     globals.mountID,
				InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
			},
			AttrName: request.Name,
		}

		getXAttrReply = &jrpcfs.GetXAttrReply{}

		err = doJRPCRequest("Server.RpcGetXAttr", getXAttrRequest, getXAttrReply)
		if nil == err {
			attrValue = getXAttrReply.AttrValue
			paddingNeeded = int(request.Position) + len(request.Xattr) - len(attrValue)
			if 0 < paddingNeeded {
				attrValue = append(attrValue, make([]byte, paddingNeeded)...)
			}
			for xattrReplacementIndex, xattrReplacementByte = range request.Xattr {
				attrValue[xattrReplacementIndex+int(request.Position)] = xattrReplacementByte
			}
		} else {
			attrValue = append(make([]byte, request.Position), request.Xattr...)
		}
	}

	setXAttrRequest = &jrpcfs.SetXAttrRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
		AttrName:  request.Name,
		AttrValue: attrValue,
		AttrFlags: fs.SetXAttrCreateOrReplace,
	}

	setXAttrReply = &jrpcfs.Reply{}

	err = doJRPCRequest("Server.RpcSetXAttr", setXAttrRequest, setXAttrReply)
	if nil != err {
		request.RespondError(fuseMissingXAttrErrno)
		return
	}

	request.Respond()
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
		return
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
	var (
		embeddedLookupResponse *fuse.LookupResponse
		err                    error
		symlinkReply           *jrpcfs.Reply
		symlinkRequest         *jrpcfs.SymlinkRequest
		response               *fuse.SymlinkResponse
	)

	symlinkRequest = &jrpcfs.SymlinkRequest{
		InodeHandle: jrpcfs.InodeHandle{
			MountID:     globals.mountID,
			InodeNumber: int64(request.Header.Node), // TODO: Check if SnapShot's work with this
		},
		Basename: request.NewName,
		Target:   request.Target,
		UserID:   int32(request.Uid),
		GroupID:  int32(request.Gid),
	}

	symlinkReply = &jrpcfs.Reply{}

	err = doJRPCRequest("Server.RpcSymlink", symlinkRequest, symlinkReply)
	if nil != err {
		request.RespondError(err)
		return
	}

	embeddedLookupResponse, err = lookupRequestHelper(request.Node, request.NewName)
	if nil != err {
		request.RespondError(err)
		return
	}

	response = &fuse.SymlinkResponse{
		LookupResponse: *embeddedLookupResponse,
	}

	request.Respond(response)
}

func handleWriteRequest(request *fuse.WriteRequest) {
	logInfof("TODO: handleWriteRequest()")
	logInfof("Header:\n%s", utils.JSONify(request.Header, true))
	logInfof("Payload\n%s", utils.JSONify(request, true))
	logInfof("Responding with fuse.ENOTSUP")
	request.RespondError(fuse.ENOTSUP)
}
